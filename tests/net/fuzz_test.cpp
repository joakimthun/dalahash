// fuzz_test.cpp — Fuzz tests: spawn real dalahash server, send crafted/random/malformed data.

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

// --- PRNG ---

struct Xorshift64 {
    uint64_t state;

    explicit Xorshift64(uint64_t seed) : state(seed ? seed : 1) {}

    uint64_t next() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

    // Random bytes into buffer
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

    // Random integer in [lo, hi]
    uint64_t range(uint64_t lo, uint64_t hi) { return lo + (next() % (hi - lo + 1)); }
};

// --- Helpers ---

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
    return "/tmp/dalahash_fuzz_test_" + std::to_string(getpid()) + "_" + std::to_string(port) + ".log";
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
    tv.tv_usec = 0;
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

// Recv up to max_len bytes, returns bytes read or -1 on error/timeout.
static ssize_t recv_with_timeout(int fd, void* buf, size_t max_len) {
    ssize_t n = recv(fd, buf, max_len, 0);
    if (n < 0 && errno != EINTR)
        return -1;
    if (n < 0)
        return 0; // EINTR, treat as no data
    return n;
}

// Recv all available data until timeout or connection close, appending to out.
static void drain_socket(int fd, std::string& out) {
    char buf[4096];
    for (;;) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n <= 0)
            break;
        out.append(buf, static_cast<size_t>(n));
    }
}

static std::string make_bulk_string(std::string_view data) {
    return "$" + std::to_string(data.size()) + "\r\n" + std::string(data) + "\r\n";
}

static std::string make_resp_array(std::initializer_list<std::string_view> args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (auto arg : args)
        out += make_bulk_string(arg);
    return out;
}

static const std::string RESP_PING = "*1\r\n$4\r\nPING\r\n";
static const std::string RESP_PONG = "+PONG\r\n";

// Send RESP PING and verify PONG response.
static bool ping_check(uint16_t port) {
    int fd = connect_loopback(port);
    if (fd < 0)
        return false;
    set_socket_timeouts(fd, 2);

    bool ok = false;
    if (send_all(fd, RESP_PING.data(), RESP_PING.size())) {
        char buf[64] = {};
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf) - 1);
        if (n > 0) {
            ok = (std::string_view(buf, static_cast<size_t>(n)) == RESP_PONG);
        }
    }
    close(fd);
    return ok;
}

// Non-reaping liveness check. kill(pid, 0) tests whether the process exists
// without consuming its exit status, so the fixture's stop() can still reap it.
static bool process_alive(pid_t pid) { return kill(pid, 0) == 0; }

// --- Server process ---

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
            if (waited == pid_) {
                pid_ = -1;
                return;
            }
            if (waited == -1 && errno == ECHILD) {
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

            if (ping_check(port_))
                return true;

            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
    }

    pid_t pid_ = -1;
    uint16_t port_;
    std::string log_path_;
};

// --- Test fixture ---

class FuzzTest : public ::testing::Test {
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

        // Seed PRNG and log for reproducibility
        std::random_device rd;
        seed_ = (static_cast<uint64_t>(rd()) << 32) | rd();
        rng_ = Xorshift64(seed_);
        std::fprintf(stderr, "[FuzzTest] PRNG seed: %lu\n", seed_);
    }

    void TearDown() override { server_.reset(); }

    // Health check: verify server is alive and responds to PING
    void assert_server_healthy() {
        ASSERT_TRUE(process_alive(server_->pid())) << "server process died.\nLog:\n" << server_->read_log();
        ASSERT_TRUE(ping_check(port())) << "server failed PING health check.\nLog:\n" << server_->read_log();
    }

    uint16_t port() const { return server_->port(); }
    std::string server_log() const { return server_->read_log(); }

    std::unique_ptr<DalahashServerProcess> server_;
    Xorshift64 rng_{1};
    uint64_t seed_ = 0;
};

// ============================================================
// Protocol Fuzzing
// ============================================================

TEST_F(FuzzTest, RandomBytes) {
    constexpr int rounds = 20;
    for (int i = 0; i < rounds; i++) {
        size_t len = static_cast<size_t>(rng_.range(64, 8192));
        std::vector<uint8_t> data(len);
        rng_.fill(data.data(), len);

        int fd = connect_loopback(port());
        if (fd < 0)
            continue;
        set_socket_timeouts(fd, 1);

        // Send garbage - server may close or send error, that's fine
        (void)send_all(fd, data.data(), data.size());

        // Drain any response
        std::string resp;
        drain_socket(fd, resp);
        close(fd);
    }

    assert_server_healthy();
}

TEST_F(FuzzTest, TruncatedCommands) {
    // Full valid SET command
    std::string cmd = make_resp_array({"SET", "trunckey", "truncval"});

    // Truncate at various offsets
    for (size_t cut = 1; cut < cmd.size(); cut += 3) {
        int fd = connect_loopback(port());
        if (fd < 0)
            continue;
        set_socket_timeouts(fd, 1);

        (void)send_all(fd, cmd.data(), cut);
        // Server waits for more data or times out; either way close
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        close(fd);
    }

    assert_server_healthy();
}

TEST_F(FuzzTest, OversizedBulkStringLength) {
    // Huge length prefix: server should reject, not allocate unbounded memory
    std::string payload = "*1\r\n$999999999\r\n";

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    (void)send_all(fd, payload.data(), payload.size());

    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    // Server may have closed connection or sent error - both acceptable
    assert_server_healthy();
}

TEST_F(FuzzTest, InvalidTypeMarkers) {
    // Every byte that isn't a valid RESP type marker
    const char* valid_markers = "*$+:-";
    for (int c = 0; c < 256; c++) {
        if (std::strchr(valid_markers, c) != nullptr || c == 0)
            continue;

        // Only test a subset to keep runtime reasonable
        if (c % 17 != 0 && c != 'A' && c != 'Z' && c != '!' && c != '\t')
            continue;

        std::string payload;
        payload.push_back(static_cast<char>(c));
        payload += "1\r\n";

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

TEST_F(FuzzTest, ZeroArgArray) {
    std::string payload = "*0\r\n";

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    (void)send_all(fd, payload.data(), payload.size());

    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    // Parser requires argc >= 1, should return error or close
    assert_server_healthy();
}

TEST_F(FuzzTest, TooManyArgs) {
    // 99 args exceeds RESP_MAX_ARGS (8)
    std::string payload = "*99\r\n";
    for (int i = 0; i < 99; i++)
        payload += "$1\r\nX\r\n";

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    (void)send_all(fd, payload.data(), payload.size());

    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    assert_server_healthy();
}

TEST_F(FuzzTest, BinaryKeysAndValues) {
    // Keys/values with null bytes, control chars, embedded \r\n
    // Valid RESP using bulk strings — should round-trip correctly

    // Build binary value with nulls, control chars, \r\n embedded
    std::string bin_value;
    bin_value.push_back('\0');
    bin_value += "hello\r\nworld";
    bin_value.push_back('\x01');
    bin_value.push_back('\x7f');
    bin_value.push_back('\0');

    std::string set_cmd = make_resp_array({"SET", "binkey", std::string_view(bin_value)});
    std::string get_cmd = make_resp_array({"GET", "binkey"});

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    // Pipeline SET + GET
    std::string pipeline = set_cmd + get_cmd;
    ASSERT_TRUE(send_all(fd, pipeline.data(), pipeline.size()));

    // Read SET response (+OK\r\n) and GET response ($<len>\r\n<data>\r\n)
    std::string resp;
    char buf[4096];
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf));
        if (n <= 0)
            break;
        resp.append(buf, static_cast<size_t>(n));
        // We expect +OK\r\n$<len>\r\n<data>\r\n — check if we got enough
        std::string expected_get_prefix = "$" + std::to_string(bin_value.size()) + "\r\n";
        if (resp.find(expected_get_prefix) != std::string::npos &&
            resp.size() >= 5 + expected_get_prefix.size() + bin_value.size() + 2)
            break;
    }
    close(fd);

    // Verify SET response
    ASSERT_TRUE(resp.starts_with("+OK\r\n")) << "SET response: " << resp.substr(0, 20);

    // Verify GET response contains the binary value
    std::string expected_get = "$" + std::to_string(bin_value.size()) + "\r\n" + bin_value + "\r\n";
    EXPECT_NE(resp.find(expected_get), std::string::npos) << "Binary value round-trip failed";

    assert_server_healthy();
}

TEST_F(FuzzTest, MixedValidAndGarbage) {
    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    // Send PING first, wait for PONG, then send garbage
    ASSERT_TRUE(send_all(fd, RESP_PING.data(), RESP_PING.size()));

    // Read PONG response before sending garbage
    char buf[64] = {};
    ssize_t n = recv_with_timeout(fd, buf, sizeof(buf) - 1);
    ASSERT_GT(n, 0) << "No response to PING";
    EXPECT_EQ(std::string_view(buf, static_cast<size_t>(n)), RESP_PONG);

    // Now send garbage — server should error/close
    std::string garbage = "THIS IS NOT RESP\r\n";
    (void)send_all(fd, garbage.data(), garbage.size());

    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    // Server may have sent an error or just closed — both acceptable
    assert_server_healthy();
}

TEST_F(FuzzTest, NegativeBulkStringLength) {
    // $-5\r\n is invalid (parse_int only accepts non-negative)
    std::string payload = "*1\r\n$-5\r\n";

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    (void)send_all(fd, payload.data(), payload.size());

    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    assert_server_healthy();
}

TEST_F(FuzzTest, MissingCRLF) {
    // Wrong line terminators
    std::vector<std::string> payloads = {
        "*1\n$4\nPING\n",       // LF only
        "*1\r\r$4\r\rPING\r\r", // CR only
        "*1\n\r$4\n\rPING\n\r", // reversed CR/LF
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

TEST_F(FuzzTest, IntegerOverflowLength) {
    // INT_MAX+1 as bulk string length
    std::string payload = "*1\r\n$2147483648\r\n";

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    (void)send_all(fd, payload.data(), payload.size());

    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    assert_server_healthy();
}

// ============================================================
// Network-Level Fuzzing
// ============================================================

TEST_F(FuzzTest, RapidConnectDisconnect) {
    for (int i = 0; i < 200; i++) {
        int fd = connect_loopback(port());
        if (fd >= 0)
            close(fd);
    }

    // Small pause to let server process closes
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert_server_healthy();
}

TEST_F(FuzzTest, HalfCloseWriteSide) {
    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    // Send partial command then half-close write side
    std::string partial = "*1\r\n$4\r\nPI";
    (void)send_all(fd, partial.data(), partial.size());
    (void)shutdown(fd, SHUT_WR);

    // Try to read - server should handle gracefully
    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    assert_server_healthy();
}

TEST_F(FuzzTest, ByteAtATimeDelivery) {
    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 5);

    // Send PING one byte at a time with small delays
    for (size_t i = 0; i < RESP_PING.size(); i++) {
        ASSERT_TRUE(send_all(fd, &RESP_PING[i], 1)) << "failed at byte " << i;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Should get PONG back
    char buf[64] = {};
    ssize_t n = recv_with_timeout(fd, buf, sizeof(buf) - 1);
    close(fd);

    ASSERT_GT(n, 0) << "no response to byte-at-a-time PING";
    EXPECT_EQ(std::string_view(buf, static_cast<size_t>(n)), RESP_PONG);

    assert_server_healthy();
}

TEST_F(FuzzTest, LargePayloadExceedingBuffers) {
    // Value larger than BUF_SIZE (4096) but within CONN_BUF_SIZE (16384)
    std::string large_value(8000, 'V');
    std::string set_cmd = make_resp_array({"SET", "largekey", large_value});
    std::string get_cmd = make_resp_array({"GET", "largekey"});

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 3);

    std::string pipeline = set_cmd + get_cmd;
    ASSERT_TRUE(send_all(fd, pipeline.data(), pipeline.size()));

    std::string resp;
    char buf[16384];
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf));
        if (n <= 0)
            break;
        resp.append(buf, static_cast<size_t>(n));
        // Check if we have both responses
        if (resp.size() >= 5 + large_value.size() + 20)
            break;
    }
    close(fd);

    ASSERT_TRUE(resp.starts_with("+OK\r\n")) << "SET failed";

    // Verify GET returned the full value
    std::string expected_get = "$" + std::to_string(large_value.size()) + "\r\n" + large_value + "\r\n";
    EXPECT_NE(resp.find(expected_get), std::string::npos) << "Large value round-trip failed";

    assert_server_healthy();
}

TEST_F(FuzzTest, PayloadExceedingConnBufSize) {
    // Value > CONN_BUF_SIZE (16384): server should handle or close cleanly
    std::string huge_value(20000, 'X');
    std::string set_cmd = make_resp_array({"SET", "hugekey", huge_value});

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 3);

    (void)send_all(fd, set_cmd.data(), set_cmd.size());

    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    // Server may have closed the connection (exceeds CONN_BUF_SIZE) or handled it
    assert_server_healthy();
}

TEST_F(FuzzTest, ManyConnectionsConcurrent) {
    constexpr int num_threads = 50;
    std::vector<std::thread> threads;
    std::atomic<int> connect_ok{0};
    std::atomic<int> pong_ok{0};

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this, &connect_ok, &pong_ok]() {
            int fd = connect_loopback(port());
            if (fd < 0)
                return;
            connect_ok.fetch_add(1, std::memory_order_relaxed);
            set_socket_timeouts(fd, 3);

            if (!send_all(fd, RESP_PING.data(), RESP_PING.size())) {
                close(fd);
                return;
            }

            char buf[64] = {};
            ssize_t n = recv_with_timeout(fd, buf, sizeof(buf) - 1);
            if (n > 0 && std::string_view(buf, static_cast<size_t>(n)) == RESP_PONG)
                pong_ok.fetch_add(1, std::memory_order_relaxed);
            close(fd);
        });
    }

    for (auto& t : threads)
        t.join();

    int connected = connect_ok.load();
    int ponged = pong_ok.load();

    // All threads should connect and get PONG under normal conditions
    EXPECT_EQ(connected, num_threads) << "Only " << connected << "/" << num_threads << " threads connected";
    EXPECT_EQ(ponged, connected) << "Only " << ponged << "/" << connected << " connected threads got PONG";

    assert_server_healthy();
}

TEST_F(FuzzTest, ConnectionFloodWithGarbage) {
    constexpr int num_threads = 100;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this, i]() {
            Xorshift64 local_rng(seed_ + static_cast<uint64_t>(i) + 1);
            int fd = connect_loopback(port());
            if (fd < 0)
                return;
            set_socket_timeouts(fd, 1);

            size_t len = static_cast<size_t>(local_rng.range(64, 1024));
            std::vector<uint8_t> garbage(len);
            local_rng.fill(garbage.data(), len);

            (void)send_all(fd, garbage.data(), garbage.size());

            std::string resp;
            drain_socket(fd, resp);
            close(fd);
        });
    }

    for (auto& t : threads)
        t.join();

    // Let server process all the closes
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    assert_server_healthy();
}

TEST_F(FuzzTest, PartialCommandThenDisconnect) {
    // Various partial commands - server should clean up without leaking
    std::vector<std::string> partials = {
        "*", "*2\r\n", "*2\r\n$3\r\nSET", "*2\r\n$3\r\nSET\r\n$5\r\nhel", "*1\r\n$4\r\nPIN",
    };

    for (const auto& partial : partials) {
        int fd = connect_loopback(port());
        if (fd < 0)
            continue;
        set_socket_timeouts(fd, 1);

        (void)send_all(fd, partial.data(), partial.size());
        // Immediately disconnect
        close(fd);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    assert_server_healthy();
}

TEST_F(FuzzTest, PipelineFlood) {
    constexpr int num_pings = 10000;

    // Build a single buffer with 10000 PINGs
    std::string payload;
    payload.reserve(RESP_PING.size() * num_pings);
    for (int i = 0; i < num_pings; i++)
        payload += RESP_PING;

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 10);

    ASSERT_TRUE(send_all(fd, payload.data(), payload.size()));

    // Read all responses
    std::string resp;
    resp.reserve(RESP_PONG.size() * num_pings);
    size_t expected_len = RESP_PONG.size() * num_pings;
    char buf[65536];

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (resp.size() < expected_len && std::chrono::steady_clock::now() < deadline) {
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf));
        if (n <= 0)
            break;
        resp.append(buf, static_cast<size_t>(n));
    }
    close(fd);

    // Count PONG responses
    int pong_count = 0;
    size_t pos = 0;
    while ((pos = resp.find("+PONG\r\n", pos)) != std::string::npos) {
        pong_count++;
        pos += 7;
    }

    EXPECT_EQ(pong_count, num_pings) << "Expected " << num_pings << " PONGs, got " << pong_count;

    assert_server_healthy();
}

// ============================================================
// Boundary Testing
// ============================================================

TEST_F(FuzzTest, ExactBufSizeBoundary) {
    // Craft a pipeline whose total size is exactly 4096 bytes (BUF_SIZE).
    // RESP_PING = "*1\r\n$4\r\nPING\r\n" = 14 bytes.
    constexpr size_t BUF_SIZE = 4096;

    // Fill with PINGs, leaving room for a trailing SET to hit exactly BUF_SIZE.
    // We need at least ~30 bytes for the SET, so stop ~100 bytes early.
    std::string payload;
    while (payload.size() + RESP_PING.size() + 100 < BUF_SIZE)
        payload += RESP_PING;
    size_t ping_count = payload.size() / RESP_PING.size();

    // Build SET command to fill the remaining bytes exactly.
    // make_resp_array({"SET","K",val}) produces:
    //   *3\r\n  $3\r\nSET\r\n  $1\r\nK\r\n  $<digits>\r\n<val>\r\n
    //   = 4 + 8 + 8 + (1 + len_digits + 2 + val_len + 2) bytes
    //   = 20 + 5 + len_digits + val_len
    size_t remaining = BUF_SIZE - payload.size();
    // Solve: remaining = 20 + 5 + len_digits + val_len
    // Start with a guess for val_len, then adjust for digit-count stability.
    size_t fixed_overhead = 25;                      // 20 + 5 (the '$' + '\r\n' + trailing '\r\n')
    size_t val_len = remaining - fixed_overhead - 1; // assume 1 digit initially
    for (int iter = 0; iter < 3; iter++) {
        size_t digits = std::to_string(val_len).size();
        val_len = remaining - fixed_overhead - digits;
    }

    std::string val(val_len, 'B');
    std::string set_cmd = make_resp_array({"SET", "K", val});
    payload += set_cmd;

    ASSERT_EQ(payload.size(), BUF_SIZE) << "Payload size " << payload.size() << " != BUF_SIZE " << BUF_SIZE;

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 3);

    ASSERT_TRUE(send_all(fd, payload.data(), payload.size()));

    // Expect ping_count PONGs followed by +OK\r\n for the SET
    std::string resp;
    size_t expected_len = RESP_PONG.size() * ping_count + 5; // 5 = "+OK\r\n"
    char buf[8192];
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (resp.size() < expected_len && std::chrono::steady_clock::now() < deadline) {
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf));
        if (n <= 0)
            break;
        resp.append(buf, static_cast<size_t>(n));
    }
    close(fd);

    // Count PONGs
    size_t pongs = 0;
    size_t pos = 0;
    while ((pos = resp.find("+PONG\r\n", pos)) != std::string::npos) {
        pongs++;
        pos += RESP_PONG.size();
    }
    EXPECT_EQ(pongs, ping_count) << "Expected " << ping_count << " PONGs";

    // The SET at the boundary must have succeeded
    EXPECT_NE(resp.find("+OK\r\n"), std::string::npos) << "Boundary SET did not return +OK";

    assert_server_healthy();
}

TEST_F(FuzzTest, CrossBufSizeBoundary) {
    // A command that spans the 4096-byte boundary
    constexpr size_t BUF_SIZE = 4096;

    // Fill with PINGs up to just before 4096
    std::string prefix;
    while (prefix.size() + RESP_PING.size() < BUF_SIZE - 5)
        prefix += RESP_PING;

    // Now add a SET that will straddle the boundary
    std::string set_cmd = make_resp_array({"SET", "crosskey", "crossvalue"});
    std::string payload = prefix + set_cmd;

    // Verify the SET actually crosses the boundary
    ASSERT_GT(prefix.size(), BUF_SIZE - 20);
    ASSERT_GT(payload.size(), BUF_SIZE);

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 3);

    ASSERT_TRUE(send_all(fd, payload.data(), payload.size()));

    std::string resp;
    char buf[8192];
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf));
        if (n <= 0)
            break;
        resp.append(buf, static_cast<size_t>(n));
        // Check for SET's +OK response at end
        if (resp.find("+OK\r\n", resp.size() > 20 ? resp.size() - 20 : 0) != std::string::npos)
            break;
    }
    close(fd);

    EXPECT_NE(resp.find("+OK\r\n"), std::string::npos) << "Cross-boundary SET should succeed";

    // Verify the value was stored
    int fd2 = connect_loopback(port());
    ASSERT_GE(fd2, 0);
    set_socket_timeouts(fd2, 2);

    std::string get_cmd = make_resp_array({"GET", "crosskey"});
    ASSERT_TRUE(send_all(fd2, get_cmd.data(), get_cmd.size()));

    std::string get_resp;
    drain_socket(fd2, get_resp);
    close(fd2);

    EXPECT_NE(get_resp.find("crossvalue"), std::string::npos) << "Cross-boundary value not stored";

    assert_server_healthy();
}

TEST_F(FuzzTest, InputBufFullReassembly) {
    // Send data in two writes where the first is close to CONN_BUF_SIZE (16384),
    // forcing reassembly near the input buffer limit.
    constexpr size_t CONN_BUF_SIZE = 16384;

    // First: a large SET value that nearly fills CONN_BUF_SIZE
    size_t val_size = CONN_BUF_SIZE - 200; // leave room for RESP framing
    std::string large_val(val_size, 'R');
    std::string set_cmd = make_resp_array({"SET", "reassembly_key", large_val});

    // Send in two parts: split in the middle of the value
    size_t split = set_cmd.size() / 2;

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 3);

    ASSERT_TRUE(send_all(fd, set_cmd.data(), split));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(send_all(fd, set_cmd.data() + split, set_cmd.size() - split));

    // Read SET response
    char buf[256] = {};
    ssize_t n = recv_with_timeout(fd, buf, sizeof(buf) - 1);
    close(fd);

    if (n > 0) {
        // If server handled it, should be +OK
        EXPECT_TRUE(std::string_view(buf, static_cast<size_t>(n)).starts_with("+OK\r\n"))
            << "Reassembly SET response: " << std::string_view(buf, static_cast<size_t>(n));
    }
    // If server closed (buffer exceeded), that's also acceptable

    assert_server_healthy();
}

TEST_F(FuzzTest, SetexMalformedSeconds) {
    // Send SETEX with various malformed seconds fields; server must stay alive.
    std::vector<std::string> payloads = {
        make_resp_array({"SETEX", "k", "", "v"}),
        make_resp_array({"SETEX", "k", "-1", "v"}),
        make_resp_array({"SETEX", "k", "abc", "v"}),
        make_resp_array({"SETEX", "k", "0", "v"}),
        make_resp_array({"SETEX", "k", "99999999999999", "v"}),
    };

    // Also add some random-byte seconds fields
    for (int i = 0; i < 10; i++) {
        size_t len = static_cast<size_t>(rng_.range(1, 32));
        std::string sec(len, '\0');
        for (size_t j = 0; j < len; j++)
            sec[j] = static_cast<char>(rng_.next() & 0xFF);
        payloads.push_back(make_resp_array({"SETEX", "k", sec, "v"}));
    }

    // Send all payloads on a single connection to avoid per-payload SO_RCVTIMEO waits.
    // The server returns -ERR for each malformed SETEX but keeps the connection open.
    std::string combined;
    for (const auto& payload : payloads)
        combined += payload;

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);
    (void)send_all(fd, combined.data(), combined.size());
    std::string resp;
    drain_socket(fd, resp);
    close(fd);

    assert_server_healthy();
}

TEST_F(FuzzTest, SetexBinaryValues) {
    // SETEX with binary keys/values containing null bytes and \r\n.
    std::string bin_key;
    bin_key.push_back('\0');
    bin_key += "setex\r\nkey";
    bin_key.push_back('\x01');

    std::string bin_value;
    bin_value.push_back('\0');
    bin_value += "setex\r\nval";
    bin_value.push_back('\x7f');
    bin_value.push_back('\0');

    std::string set_cmd = make_resp_array({"SETEX", bin_key, "60", bin_value});
    std::string get_cmd = make_resp_array({"GET", bin_key});

    int fd = connect_loopback(port());
    ASSERT_GE(fd, 0);
    set_socket_timeouts(fd, 2);

    std::string pipeline = set_cmd + get_cmd;
    ASSERT_TRUE(send_all(fd, pipeline.data(), pipeline.size()));

    std::string resp;
    char buf[4096];
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        ssize_t n = recv_with_timeout(fd, buf, sizeof(buf));
        if (n <= 0)
            break;
        resp.append(buf, static_cast<size_t>(n));
        std::string expected_get_prefix = "$" + std::to_string(bin_value.size()) + "\r\n";
        if (resp.find(expected_get_prefix) != std::string::npos &&
            resp.size() >= 5 + expected_get_prefix.size() + bin_value.size() + 2)
            break;
    }
    close(fd);

    ASSERT_TRUE(resp.starts_with("+OK\r\n")) << "SETEX response: " << resp.substr(0, 20);

    std::string expected_get = "$" + std::to_string(bin_value.size()) + "\r\n" + bin_value + "\r\n";
    EXPECT_NE(resp.find(expected_get), std::string::npos) << "Binary SETEX value round-trip failed";

    assert_server_healthy();
}

} // namespace
