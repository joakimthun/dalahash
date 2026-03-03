// io_uring_integration_test.cpp — Real io_uring backend integration tests.

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

struct CommandResult {
    int exit_code = -1;
    std::string output;
};

static std::string trim_trailing_line_endings(std::string text) {
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r'))
        text.pop_back();
    return text;
}

static std::string shell_quote(std::string_view value) {
    std::string quoted;
    quoted.reserve(value.size() + 2);
    quoted.push_back('\'');
    for (char c : value) {
        if (c == '\'') {
            quoted += "'\\''";
        } else {
            quoted.push_back(c);
        }
    }
    quoted.push_back('\'');
    return quoted;
}

static CommandResult run_shell_capture(const std::string& cmd) {
    CommandResult result;

    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        result.output = "popen failed";
        return result;
    }

    char buf[256];
    while (std::fgets(buf, static_cast<int>(sizeof(buf)), pipe))
        result.output += buf;

    int status = pclose(pipe);
    if (status == -1) {
        result.exit_code = -1;
    } else if (WIFEXITED(status)) {
        result.exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        result.exit_code = 128 + WTERMSIG(status);
    }

    return result;
}

static bool command_exists(const char* name) {
    const char* path_env = std::getenv("PATH");
    if (!path_env)
        return false;

    std::string path(path_env);
    std::size_t start = 0;
    while (start <= path.size()) {
        std::size_t end = path.find(':', start);
        std::string dir;
        if (end == std::string::npos) {
            dir = path.substr(start);
        } else {
            dir = path.substr(start, end - start);
        }
        if (dir.empty())
            dir = ".";

        std::string candidate = dir + "/" + name;
        if (access(candidate.c_str(), X_OK) == 0)
            return true;

        if (end == std::string::npos)
            break;
        start = end + 1;
    }

    return false;
}

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
    return "/tmp/dalahash_io_uring_test_" + std::to_string(getpid()) + "_" + std::to_string(port) + ".log";
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

class RedisCliClient {
  public:
    explicit RedisCliClient(uint16_t port) : port_(port) {}

    CommandResult command(std::initializer_list<std::string_view> args) const {
        std::string cmd = base_command();
        for (std::string_view arg : args)
            cmd += " " + shell_quote(arg);
        cmd += " 2>&1";
        return run_shell_capture(cmd);
    }

    CommandResult session(std::string_view command_lines) const {
        std::string cmd = "printf %s " + shell_quote(command_lines);
        cmd += " | " + base_command() + " 2>&1";
        return run_shell_capture(cmd);
    }

  private:
    std::string base_command() const { return "redis-cli --raw -h 127.0.0.1 -p " + std::to_string(port_); }

    uint16_t port_;
};

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

    std::string read_log() const { return read_text_file(log_path_); }

  private:
    bool wait_until_ready() {
        RedisCliClient client(port_);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);

        while (std::chrono::steady_clock::now() < deadline) {
            int status = 0;
            pid_t waited = waitpid(pid_, &status, WNOHANG);
            if (waited == pid_) {
                pid_ = -1;
                return false;
            }

            CommandResult ping = client.command({"PING"});
            if (ping.exit_code == 0 && trim_trailing_line_endings(ping.output) == "PONG")
                return true;

            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        return false;
    }

    pid_t pid_ = -1;
    uint16_t port_;
    std::string log_path_;
};

class IoUringIntegrationTest : public ::testing::Test {
  protected:
    void SetUp() override {
        if (access(DALAHASH_BINARY_PATH, X_OK) != 0) {
            GTEST_SKIP() << "dalahash binary is not executable at " << DALAHASH_BINARY_PATH;
        }
        if (!command_exists("redis-cli")) {
            GTEST_SKIP() << "redis-cli not found in PATH";
        }

        uint16_t port = pick_unused_port();
        if (port == 0) {
            GTEST_SKIP() << "failed to pick an ephemeral TCP port";
        }

        server_ = std::make_unique<DalahashServerProcess>(port);
        if (!server_->start()) {
            std::string log = server_->read_log();
            server_->stop();
            if (log.find("backend init failed") != std::string::npos ||
                log.find("io_uring_queue_init_params failed") != std::string::npos ||
                log.find("Operation not permitted") != std::string::npos ||
                log.find("Function not implemented") != std::string::npos) {
                GTEST_SKIP() << "io_uring backend unavailable in this environment.\n" << log;
            }
            FAIL() << "failed to start dalahash server.\n" << log;
        }

        client_ = std::make_unique<RedisCliClient>(server_->port());
    }

    void TearDown() override {
        client_.reset();
        server_.reset();
    }

    const RedisCliClient& client() const { return *client_; }

  private:
    std::unique_ptr<DalahashServerProcess> server_;
    std::unique_ptr<RedisCliClient> client_;
};

TEST_F(IoUringIntegrationTest, PingRoundTrip) {
    CommandResult res = client().command({"PING"});
    ASSERT_EQ(res.exit_code, 0) << res.output;
    EXPECT_EQ(trim_trailing_line_endings(res.output), "PONG");
}

TEST_F(IoUringIntegrationTest, PingEchoesSingleArgument) {
    CommandResult res = client().command({"PING", "hello"});
    ASSERT_EQ(res.exit_code, 0) << res.output;
    EXPECT_EQ(trim_trailing_line_endings(res.output), "hello");
}

TEST_F(IoUringIntegrationTest, SetThenGetAcrossClientCalls) {
    CommandResult set = client().command({"SET", "io_uring:key", "value123"});
    ASSERT_EQ(set.exit_code, 0) << set.output;
    EXPECT_EQ(trim_trailing_line_endings(set.output), "OK");

    CommandResult get = client().command({"GET", "io_uring:key"});
    ASSERT_EQ(get.exit_code, 0) << get.output;
    EXPECT_EQ(trim_trailing_line_endings(get.output), "value123");
}

TEST_F(IoUringIntegrationTest, MissingKeyReturnsNilBulk) {
    CommandResult miss = client().command({"GET", "io_uring:missing"});
    ASSERT_EQ(miss.exit_code, 0) << miss.output;
    EXPECT_EQ(trim_trailing_line_endings(miss.output), "");
}

TEST_F(IoUringIntegrationTest, MultiCommandSingleConnectionSession) {
    CommandResult res = client().session("SET io_uring:session abc\n"
                                         "GET io_uring:session\n");
    ASSERT_EQ(res.exit_code, 0) << res.output;
    EXPECT_EQ(trim_trailing_line_endings(res.output), "OK\nabc");
}

TEST_F(IoUringIntegrationTest, ErrorResponsesSurfaceToClient) {
    CommandResult arity = client().command({"GET"});
    ASSERT_EQ(arity.exit_code, 0) << arity.output;
    EXPECT_TRUE(trim_trailing_line_endings(arity.output)
                    .starts_with("ERR wrong number of arguments for 'get' command"));

    CommandResult unknown = client().command({"NOT_A_REAL_COMMAND"});
    ASSERT_EQ(unknown.exit_code, 0) << unknown.output;
    EXPECT_EQ(trim_trailing_line_endings(unknown.output), "ERR unknown command");

    CommandResult ping_arity = client().command({"PING", "a", "b"});
    ASSERT_EQ(ping_arity.exit_code, 0) << ping_arity.output;
    EXPECT_TRUE(trim_trailing_line_endings(ping_arity.output)
                    .starts_with("ERR wrong number of arguments for 'ping' command"));
}

TEST_F(IoUringIntegrationTest, SetexThenGetRoundTrip) {
    CommandResult set = client().command({"SETEX", "setex:key", "60", "value456"});
    ASSERT_EQ(set.exit_code, 0) << set.output;
    EXPECT_EQ(trim_trailing_line_endings(set.output), "OK");

    CommandResult get = client().command({"GET", "setex:key"});
    ASSERT_EQ(get.exit_code, 0) << get.output;
    EXPECT_EQ(trim_trailing_line_endings(get.output), "value456");
}

TEST_F(IoUringIntegrationTest, SetexErrorResponses) {
    // Wrong arity
    CommandResult arity = client().command({"SETEX", "k"});
    ASSERT_EQ(arity.exit_code, 0) << arity.output;
    EXPECT_TRUE(trim_trailing_line_endings(arity.output)
                    .starts_with("ERR wrong number of arguments for 'setex' command"));

    // Non-numeric seconds
    CommandResult bad_sec = client().command({"SETEX", "k", "abc", "v"});
    ASSERT_EQ(bad_sec.exit_code, 0) << bad_sec.output;
    EXPECT_TRUE(
        trim_trailing_line_endings(bad_sec.output).starts_with("ERR invalid expire time in 'setex' command"));
}

} // namespace
