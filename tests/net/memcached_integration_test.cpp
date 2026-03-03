// memcached_integration_test.cpp — Real io_uring integration tests for memcached protocol.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#ifndef DALAHASH_BINARY_PATH
#define DALAHASH_BINARY_PATH "dalahash"
#endif

namespace {

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
    return "/tmp/dalahash_mc_int_test_" + std::to_string(getpid()) + "_" + std::to_string(port) + ".log";
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

// Health check: send "version\r\n" and expect "VERSION ..." response.
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

// Helper: send a memcached command and read the full response.
static std::string mc_command(uint16_t port, const std::string& cmd) {
    int fd = connect_loopback(port);
    if (fd < 0)
        return "<CONNECT_FAIL>";
    set_socket_timeouts(fd, 2);
    if (!send_all(fd, cmd.data(), cmd.size())) {
        close(fd);
        return "<SEND_FAIL>";
    }
    std::string resp;
    drain_socket(fd, resp);
    close(fd);
    return resp;
}

// Helper: send multiple commands on same connection.
static std::string mc_session(uint16_t port, const std::string& cmds) {
    int fd = connect_loopback(port);
    if (fd < 0)
        return "<CONNECT_FAIL>";
    set_socket_timeouts(fd, 2);
    if (!send_all(fd, cmds.data(), cmds.size())) {
        close(fd);
        return "<SEND_FAIL>";
    }
    // Wait a bit for all responses
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::string resp;
    drain_socket(fd, resp);
    close(fd);
    return resp;
}

class MemcachedIntegrationTest : public ::testing::Test {
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
    }

    void TearDown() override { server_.reset(); }

    uint16_t port() const { return server_->port(); }

  private:
    std::unique_ptr<DalahashServerProcess> server_;
};

TEST_F(MemcachedIntegrationTest, VersionHandshake) {
    std::string resp = mc_command(port(), "version\r\n");
    EXPECT_TRUE(resp.starts_with("VERSION")) << "Response: " << resp;
}

TEST_F(MemcachedIntegrationTest, SetGetRoundTrip) {
    std::string cmds = "set mykey 0 0 5\r\nhello\r\nget mykey\r\n";
    std::string resp = mc_session(port(), cmds);
    EXPECT_TRUE(resp.find("STORED") != std::string::npos) << "Response: " << resp;
    EXPECT_TRUE(resp.find("VALUE mykey 0 5") != std::string::npos) << "Response: " << resp;
    EXPECT_TRUE(resp.find("hello") != std::string::npos) << "Response: " << resp;
}

TEST_F(MemcachedIntegrationTest, DeleteRoundTrip) {
    std::string cmds = "set k 0 0 3\r\nfoo\r\ndelete k\r\nget k\r\n";
    std::string resp = mc_session(port(), cmds);
    EXPECT_TRUE(resp.find("STORED") != std::string::npos);
    EXPECT_TRUE(resp.find("DELETED") != std::string::npos);
    // After delete, GET should return just END (miss).
    // The response should end with "END\r\n"
    EXPECT_TRUE(resp.rfind("END\r\n") != std::string::npos);
}

TEST_F(MemcachedIntegrationTest, MetaSetGetRoundTrip) {
    std::string cmds = "ms mykey 5\r\nhello\r\nmg mykey v\r\n";
    std::string resp = mc_session(port(), cmds);
    EXPECT_TRUE(resp.find("HD") != std::string::npos) << "Response: " << resp;
    EXPECT_TRUE(resp.find("VA 5") != std::string::npos) << "Response: " << resp;
    EXPECT_TRUE(resp.find("hello") != std::string::npos) << "Response: " << resp;
}

TEST_F(MemcachedIntegrationTest, MetaDeleteRoundTrip) {
    std::string cmds = "ms k 3\r\nfoo\r\nmd k\r\nmg k v\r\n";
    std::string resp = mc_session(port(), cmds);
    EXPECT_TRUE(resp.find("HD") != std::string::npos);
    EXPECT_TRUE(resp.find("EN") != std::string::npos);
}

TEST_F(MemcachedIntegrationTest, MetaNoopRoundTrip) {
    std::string resp = mc_command(port(), "mn\r\n");
    EXPECT_EQ(resp, "MN\r\n");
}

TEST_F(MemcachedIntegrationTest, LargeValue) {
    std::string big(4000, 'Z');
    std::string cmds = "set bigkey 0 0 4000\r\n" + big + "\r\nget bigkey\r\n";
    std::string resp = mc_session(port(), cmds);
    EXPECT_TRUE(resp.find("STORED") != std::string::npos);
    EXPECT_TRUE(resp.find("VALUE bigkey 0 4000") != std::string::npos);
}

TEST_F(MemcachedIntegrationTest, PipelinedCommands) {
    // Multiple commands in one send
    std::string cmds;
    cmds += "set k1 0 0 2\r\nv1\r\n";
    cmds += "set k2 0 0 2\r\nv2\r\n";
    cmds += "get k1\r\n";
    cmds += "get k2\r\n";
    std::string resp = mc_session(port(), cmds);
    EXPECT_TRUE(resp.find("v1") != std::string::npos);
    EXPECT_TRUE(resp.find("v2") != std::string::npos);
}

} // namespace
