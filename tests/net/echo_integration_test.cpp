// echo_integration_test.cpp — Real io_uring backend integration tests for echo protocol.

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
#include <thread>
#include <vector>

#ifndef DALAHASH_BINARY_PATH
#define DALAHASH_BINARY_PATH "dalahash"
#endif

namespace {

static uint16_t pick_unused_port() {
    for (uint16_t port = 20000; port < 60000; port++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            return 0;

        int one = 1;
        (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        struct sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);

        if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
            close(fd);
            return port;
        }

        close(fd);
    }
    return 0;
}

static std::string make_log_path(uint16_t port) {
    return "/tmp/dalahash_echo_test_" + std::to_string(getpid()) + "_" + std::to_string(port) + ".log";
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

static bool send_all(int fd, const uint8_t* data, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = send(fd, data + off, len - off, MSG_NOSIGNAL);
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

static bool recv_exact(int fd, uint8_t* out, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = recv(fd, out + off, len - off, 0);
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

            int probe = connect_loopback(port_);
            if (probe >= 0) {
                close(probe);
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
    }

    uint16_t port_;
    std::string log_path_;
    pid_t pid_ = -1;
};

class EchoIntegrationTest : public ::testing::Test {
  protected:
    void SetUp() override {
        port_ = pick_unused_port();
        if (port_ == 0)
            GTEST_SKIP() << "unable to pick unused TCP port";

        server_ = std::make_unique<DalahashServerProcess>(port_);
        if (!server_->start())
            GTEST_SKIP() << "server did not start:\n" << server_->read_log();

        client_fd_ = connect_loopback(port_);
        if (client_fd_ < 0)
            GTEST_SKIP() << "failed to connect to echo server:\n" << server_->read_log();
        set_socket_timeouts(client_fd_, 2);
    }

    void TearDown() override {
        if (client_fd_ >= 0)
            close(client_fd_);
        if (server_)
            server_->stop();
    }

    void round_trip(const std::vector<uint8_t>& payload) {
        ASSERT_TRUE(send_all(client_fd_, payload.data(), payload.size()));
        std::vector<uint8_t> echoed(payload.size());
        ASSERT_TRUE(recv_exact(client_fd_, echoed.data(), echoed.size()));
        EXPECT_EQ(echoed, payload);
    }

    uint16_t port_ = 0;
    int client_fd_ = -1;
    std::unique_ptr<DalahashServerProcess> server_;
};

TEST_F(EchoIntegrationTest, TextRoundTrip) {
    const std::string text = "hello from echo\n";
    std::vector<uint8_t> payload(text.begin(), text.end());
    round_trip(payload);
}

TEST_F(EchoIntegrationTest, BinaryRoundTrip) {
    std::vector<uint8_t> payload = {0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF, 0x00, 0x2A};
    round_trip(payload);
}

TEST_F(EchoIntegrationTest, MultipleMessagesOnOneConnection) {
    const std::string a = "abc";
    const std::string b = "XYZ123";
    std::vector<uint8_t> p1(a.begin(), a.end());
    std::vector<uint8_t> p2(b.begin(), b.end());

    round_trip(p1);
    round_trip(p2);
}

} // namespace
