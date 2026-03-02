// memcached_fuzz_test.cpp — Fuzz tests for memcached protocol: spawn real server, send malformed data.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifndef DALAHASH_BINARY_PATH
#define DALAHASH_BINARY_PATH "dalahash"
#endif

namespace {

struct Xorshift64 {
    uint64_t state;
    explicit Xorshift64(uint64_t seed) : state(seed ? seed : 1) {}
    uint64_t next() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }
    void fill(uint8_t* buf, size_t len) {
        size_t i = 0;
        while (i + 8 <= len) {
            uint64_t v = next();
            std::memcpy(buf + i, &v, 8);
            i += 8;
        }
        if (i < len) {
            uint64_t v = next();
            std::memcpy(buf + i, &v, len - i);
        }
    }
    uint64_t range(uint64_t lo, uint64_t hi) { return lo + (next() % (hi - lo + 1)); }
};

static uint16_t pick_unused_port() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return 0;
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
        close(fd);
        return 0;
    }
    uint16_t port = ntohs(addr.sin_port);
    close(fd);
    return port;
}

static std::string make_log_path(uint16_t port) {
    return "/tmp/dalahash_mc_fuzz_test_" + std::to_string(getpid()) + "_" + std::to_string(port) + ".log";
}

static std::string read_text_file(const std::string& path) {
    FILE* f = std::fopen(path.c_str(), "r");
    if (!f)
        return {};
    std::string out;
    char buf[512];
    while (std::fgets(buf, static_cast<int>(sizeof(buf)), f))
        out += buf;
    std::fclose(f);
    return out;
}

static int connect_loopback(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static void set_socket_timeouts(int fd, int sec) {
    struct timeval tv = {};
    tv.tv_sec = sec;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

static bool send_all(int fd, const void* data, size_t len) {
    const auto* p = static_cast<const uint8_t*>(data);
    size_t off = 0;
    while (off < len) {
        ssize_t n = send(fd, p + off, len - off, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        if (n == 0)
            return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

static ssize_t recv_with_timeout(int fd, void* buf, size_t max_len) {
    ssize_t n = recv(fd, buf, max_len, 0);
    if (n < 0 && errno != EINTR)
        return -1;
    if (n < 0)
        return 0;
    return n;
}

static void drain_socket(int fd, std::string& out) {
    char buf[4096];
    for (;;) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n <= 0)
            break;
        out.append(buf, static_cast<size_t>(n));
    }
}

// Health check via "version\r\n".
static bool version_check(uint16_t port) {
    int fd = connect_loopback(port);
    if (fd < 0)
        return false;
    set_socket_timeouts(fd, 2);
    const char* cmd = "version\r\n";
    bool ok = false;
    if (send_all(fd, cmd, 9)) {
        char buf[128] = {};
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf) - 1);
        if (n > 0)
            ok = (std::string_view(buf, static_cast<size_t>(n)).starts_with("VERSION"));
    }
    close(fd);
    return ok;
}

static bool process_alive(pid_t pid) { return kill(pid, 0) == 0; }

class DalahashServerProcess {
  public:
    explicit DalahashServerProcess(uint16_t port) : port_(port), log_path_(make_log_path(port)) {}
    ~DalahashServerProcess() {
        stop();
        (void)std::remove(log_path_.c_str());
    }
    bool start() {
        if (pid_ > 0)
            return false;
        std::string port_arg = std::to_string(port_);
        pid_ = fork();
        if (pid_ < 0)
            return false;
        if (pid_ == 0) {
            int log_fd = open(log_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (log_fd >= 0) {
                (void)dup2(log_fd, STDOUT_FILENO);
                (void)dup2(log_fd, STDERR_FILENO);
                if (log_fd > STDERR_FILENO)
                    close(log_fd);
            }
            execl(DALAHASH_BINARY_PATH, DALAHASH_BINARY_PATH, "--port", port_arg.c_str(), "--workers", "1",
                  static_cast<char*>(nullptr));
            _exit(127);
        }
        return wait_until_ready();
    }
    void stop() {
        if (pid_ <= 0)
            return;
        (void)kill(pid_, SIGTERM);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            int status = 0;
            pid_t waited = waitpid(pid_, &status, WNOHANG);
            if (waited == pid_ || (waited == -1 && errno == ECHILD)) {
                pid_ = -1;
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        (void)kill(pid_, SIGKILL);
        (void)waitpid(pid_, nullptr, 0);
        pid_ = -1;
    }
    uint16_t port() const { return port_; }
    pid_t pid() const { return pid_; }
    std::string read_log() const { return read_text_file(log_path_); }

  private:
    bool wait_until_ready() {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            int status = 0;
            pid_t waited = waitpid(pid_, &status, WNOHANG);
            if (waited == pid_) {
                pid_ = -1;
                return false;
            }
            if (version_check(port_))
                return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
    }
    pid_t pid_ = -1;
    uint16_t port_;
    std::string log_path_;
};

class MemcachedFuzzTest : public ::testing::Test {
  protected:
    void SetUp() override {
        if (access(DALAHASH_BINARY_PATH, X_OK) != 0)
            GTEST_SKIP() << "dalahash binary not executable at " << DALAHASH_BINARY_PATH;
        uint16_t port = pick_unused_port();
        if (port == 0)
            GTEST_SKIP() << "failed to pick ephemeral TCP port";
        server_ = std::make_unique<DalahashServerProcess>(port);
        if (!server_->start()) {
            std::string log = server_->read_log();
            server_->stop();
            if (log.find("backend init failed") != std::string::npos ||
                log.find("io_uring_queue_init_params failed") != std::string::npos ||
                log.find("Operation not permitted") != std::string::npos ||
                log.find("Function not implemented") != std::string::npos) {
                GTEST_SKIP() << "io_uring backend unavailable.\n" << log;
            }
            FAIL() << "failed to start dalahash server.\n" << log;
        }
        std::random_device rd;
        seed_ = (static_cast<uint64_t>(rd()) << 32) | rd();
        rng_ = Xorshift64(seed_);
        std::fprintf(stderr, "[MemcachedFuzzTest] PRNG seed: %lu\n", seed_);
    }
    void TearDown() override { server_.reset(); }
    void assert_server_healthy() {
        ASSERT_TRUE(process_alive(server_->pid())) << "server died.\nLog:\n" << server_->read_log();
        ASSERT_TRUE(version_check(port())) << "server failed health check.\nLog:\n" << server_->read_log();
    }
    uint16_t port() const { return server_->port(); }

    std::unique_ptr<DalahashServerProcess> server_;
    Xorshift64 rng_{1};
    uint64_t seed_ = 0;
};

TEST_F(MemcachedFuzzTest, RandomBytes) {
    for (int i = 0; i < 20; i++) {
        size_t len = static_cast<size_t>(rng_.range(64, 8192));
        std::vector<uint8_t> data(len);
        rng_.fill(data.data(), len);
        int fd = connect_loopback(port());
        if (fd < 0)
            continue;
        set_socket_timeouts(fd, 1);
        (void)send_all(fd, data.data(), data.size());
        std::string resp;
        drain_socket(fd, resp);
        close(fd);
    }
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, TruncatedCommands) {
    std::string cmd = "set trunckey 0 0 8\r\ntruncval\r\n";
    for (size_t cut = 1; cut < cmd.size(); cut += 3) {
        int fd = connect_loopback(port());
        if (fd < 0)
            continue;
        set_socket_timeouts(fd, 1);
        (void)send_all(fd, cmd.data(), cut);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        close(fd);
    }
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, OversizedByteCount) {
    std::string payload = "set k 0 0 999999999\r\n";
    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);
    (void)send_all(fd, payload.data(), payload.size());
    std::string resp;
    drain_socket(fd, resp);
    close(fd);
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, MissingCRLF) {
    std::vector<std::string> payloads = {
        "get mykey\n",          // LF only
        "get mykey\r",          // CR only
        "set k 0 0 3\nfoo\n",   // LF only
        "set k 0 0 3\r\rfoo\r", // CR only
    };
    for (const auto& payload : payloads) {
        int fd = connect_loopback(port());
        if (fd < 0)
            continue;
        set_socket_timeouts(fd, 1);
        (void)send_all(fd, payload.data(), payload.size());
        std::string resp;
        drain_socket(fd, resp);
        close(fd);
    }
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, InvalidCommands) {
    std::vector<std::string> payloads = {
        "FOOBAR\r\n",
        "xyzzy\r\n",
        "\r\n",
        "set\r\n",                  // missing args
        "get\r\n",                  // missing key
        "delete\r\n",               // missing key
        "set k abc 0 3\r\nfoo\r\n", // non-numeric flags
    };
    for (const auto& payload : payloads) {
        int fd = connect_loopback(port());
        if (fd < 0)
            continue;
        set_socket_timeouts(fd, 1);
        (void)send_all(fd, payload.data(), payload.size());
        std::string resp;
        drain_socket(fd, resp);
        close(fd);
    }
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, MixedValidAndGarbage) {
    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);
    const char* cmd = "version\r\n";
    ASSERT_TRUE(send_all(fd, cmd, 9));
    char buf[128] = {};
    ssize_t n = recv_with_timeout(fd, buf, sizeof(buf) - 1);
    ASSERT_GT(n, 0);
    EXPECT_TRUE(std::string_view(buf, static_cast<size_t>(n)).starts_with("VERSION"));
    const char* garbage = "THIS IS NOT MEMCACHED\r\n";
    (void)send_all(fd, garbage, std::strlen(garbage));
    std::string resp;
    drain_socket(fd, resp);
    close(fd);
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, RapidConnectDisconnect) {
    for (int i = 0; i < 200; i++) {
        int fd = connect_loopback(port());
        if (fd >= 0)
            close(fd);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, BadDataBlockTerminator) {
    // Data block present but not terminated with \r\n
    std::string payload = "set k 0 0 3\r\nfooXX";
    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 1);
    (void)send_all(fd, payload.data(), payload.size());
    std::string resp;
    drain_socket(fd, resp);
    close(fd);
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, ManyConnectionsConcurrent) {
    constexpr int num_threads = 50;
    std::vector<std::thread> threads;
    std::atomic<int> ok_count{0};

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this, &ok_count]() {
            int fd = connect_loopback(port());
            if (fd < 0)
                return;
            set_socket_timeouts(fd, 3);
            const char* cmd = "version\r\n";
            if (send_all(fd, cmd, 9)) {
                char buf[128] = {};
                ssize_t n = recv_with_timeout(fd, buf, sizeof(buf) - 1);
                if (n > 0 && std::string_view(buf, static_cast<size_t>(n)).starts_with("VERSION"))
                    ok_count.fetch_add(1, std::memory_order_relaxed);
            }
            close(fd);
        });
    }

    for (auto& t : threads)
        t.join();

    EXPECT_EQ(ok_count.load(), num_threads);
    assert_server_healthy();
}

TEST_F(MemcachedFuzzTest, PipelineFlood) {
    constexpr int num_versions = 10000;
    std::string payload;
    payload.reserve(9 * num_versions);
    for (int i = 0; i < num_versions; i++)
        payload += "version\r\n";

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 10);
    ASSERT_TRUE(send_all(fd, payload.data(), payload.size()));

    std::string resp;
    char buf[65536];
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf));
        if (n <= 0)
            break;
        resp.append(buf, static_cast<size_t>(n));
        // Count VERSION responses
        int count = 0;
        size_t pos = 0;
        while ((pos = resp.find("VERSION", pos)) != std::string::npos) {
            count++;
            pos += 7;
        }
        if (count >= num_versions)
            break;
    }
    close(fd);

    int count = 0;
    size_t pos = 0;
    while ((pos = resp.find("VERSION", pos)) != std::string::npos) {
        count++;
        pos += 7;
    }
    EXPECT_EQ(count, num_versions);
    assert_server_healthy();
}

} // namespace
