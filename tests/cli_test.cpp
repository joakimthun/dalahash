// cli_test.cpp — CLI parsing tests.

#include "cli.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

struct CliParseOutput {
    CliParseResult result = CliParseResult::ERROR;
    ServerConfig config = {};
    const char* error_message = nullptr;
    const char* error_arg = nullptr;
    // storage keeps the argv strings alive so error_arg pointers remain valid.
    std::vector<std::string> storage;
};

static CliParseOutput parse_args(std::initializer_list<const char*> args) {
    CliParseOutput out = {
        .result = CliParseResult::ERROR,
        .config = {.port = 6379, .num_workers = 0, .store_bytes = 256ull << 20},
        .error_message = nullptr,
        .error_arg = nullptr,
        .storage = {},
    };
    out.storage.reserve(args.size());
    for (const char* arg : args)
        out.storage.emplace_back(arg);

    std::vector<char*> argv;
    argv.reserve(out.storage.size());
    for (std::string& arg : out.storage)
        argv.push_back(arg.data());

    out.result = cli_parse_args(static_cast<int>(argv.size()), argv.data(), &out.config, &out.error_message,
                                &out.error_arg);
    return out;
}

TEST(Cli, DefaultsRemainWhenNoArgsProvided) {
    CliParseOutput out = parse_args({"dalahash"});
    EXPECT_EQ(out.result, CliParseResult::OK);
    EXPECT_EQ(out.config.port, 6379);
    EXPECT_EQ(out.config.num_workers, 0);
    EXPECT_EQ(out.config.store_bytes, 256ull << 20);
}

TEST(Cli, HelpReturnsHelpStatus) {
    CliParseOutput out = parse_args({"dalahash", "--help"});
    EXPECT_EQ(out.result, CliParseResult::HELP);
    EXPECT_STREQ(cli_usage(), "Usage: dalahash [--port PORT] [--workers N] [--store-bytes BYTES]");
}

TEST(Cli, ParsesValidWorkersZero) {
    CliParseOutput out = parse_args({"dalahash", "--workers", "0"});
    EXPECT_EQ(out.result, CliParseResult::OK);
    EXPECT_EQ(out.config.num_workers, 0);
}

TEST(Cli, RejectsNegativeWorkers) {
    CliParseOutput out = parse_args({"dalahash", "--workers", "-1"});
    EXPECT_EQ(out.result, CliParseResult::ERROR);
    ASSERT_NE(out.error_message, nullptr);
    EXPECT_STREQ(out.error_message, "Invalid --workers value");
}

TEST(Cli, RejectsNonNumericWorkers) {
    CliParseOutput out = parse_args({"dalahash", "--workers", "abc"});
    EXPECT_EQ(out.result, CliParseResult::ERROR);
    ASSERT_NE(out.error_message, nullptr);
    EXPECT_STREQ(out.error_message, "Invalid --workers value");
}

TEST(Cli, RejectsWorkersWithTrailingGarbage) {
    CliParseOutput out = parse_args({"dalahash", "--workers", "1x"});
    EXPECT_EQ(out.result, CliParseResult::ERROR);
    ASSERT_NE(out.error_message, nullptr);
    EXPECT_STREQ(out.error_message, "Invalid --workers value");
}

TEST(Cli, RejectsMissingWorkersValue) {
    CliParseOutput out = parse_args({"dalahash", "--workers"});
    EXPECT_EQ(out.result, CliParseResult::ERROR);
    ASSERT_NE(out.error_message, nullptr);
    EXPECT_STREQ(out.error_message, "Invalid --workers value");
}

TEST(Cli, RejectsZeroStoreBytes) {
    CliParseOutput out = parse_args({"dalahash", "--store-bytes", "0"});
    EXPECT_EQ(out.result, CliParseResult::ERROR);
    ASSERT_NE(out.error_message, nullptr);
    EXPECT_STREQ(out.error_message, "Invalid --store-bytes value");
}

TEST(Cli, RejectsNegativeStoreBytes) {
    CliParseOutput out = parse_args({"dalahash", "--store-bytes", "-1"});
    EXPECT_EQ(out.result, CliParseResult::ERROR);
    ASSERT_NE(out.error_message, nullptr);
    EXPECT_STREQ(out.error_message, "Invalid --store-bytes value");
}

TEST(Cli, RejectsZeroPort) {
    CliParseOutput out = parse_args({"dalahash", "--port", "0"});
    EXPECT_EQ(out.result, CliParseResult::ERROR);
    ASSERT_NE(out.error_message, nullptr);
    EXPECT_STREQ(out.error_message, "Invalid --port value");
}

TEST(Cli, ReportsUnknownArgument) {
    CliParseOutput out = parse_args({"dalahash", "--nope"});
    EXPECT_EQ(out.result, CliParseResult::ERROR);
    ASSERT_NE(out.error_message, nullptr);
    ASSERT_NE(out.error_arg, nullptr);
    EXPECT_STREQ(out.error_message, "Unknown argument");
    EXPECT_STREQ(out.error_arg, "--nope");
}

} // namespace
