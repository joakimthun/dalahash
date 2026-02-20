/* resp_test.cpp — RESP parser and response formatter tests. */

#include "resp.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

static bool arg_eq(const RespArg &arg, const char *expected) {
    return arg.len == std::strlen(expected) &&
           std::memcmp(arg.data, expected, arg.len) == 0;
}

TEST(RespParse, GetCommand) {
    std::string input = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    auto result = resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed);
    EXPECT_EQ(result, RespParseResult::OK);
    EXPECT_EQ(consumed, input.size());
    EXPECT_EQ(cmd.argc, 2);
    EXPECT_TRUE(arg_eq(cmd.args[0], "GET"));
    EXPECT_TRUE(arg_eq(cmd.args[1], "foo"));
}

TEST(RespParse, SetCommand) {
    std::string input = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    auto result = resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed);
    EXPECT_EQ(result, RespParseResult::OK);
    EXPECT_EQ(cmd.argc, 3);
    EXPECT_TRUE(arg_eq(cmd.args[0], "SET"));
    EXPECT_TRUE(arg_eq(cmd.args[1], "foo"));
    EXPECT_TRUE(arg_eq(cmd.args[2], "bar"));
}

TEST(RespParse, PingCommand) {
    std::string input = "*1\r\n$4\r\nPING\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    auto result = resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed);
    EXPECT_EQ(result, RespParseResult::OK);
    EXPECT_EQ(cmd.argc, 1);
    EXPECT_TRUE(arg_eq(cmd.args[0], "PING"));
}

TEST(RespParse, IncompleteEmpty) {
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(nullptr, 0, &cmd, &consumed), RespParseResult::INCOMPLETE);
}

TEST(RespParse, IncompletePartialArray) {
    std::string input = "*2\r\n$3\r\nGET\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed),
              RespParseResult::INCOMPLETE);
}

TEST(RespParse, IncompletePartialBulkString) {
    std::string input = "*1\r\n$3\r\nG";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed),
              RespParseResult::INCOMPLETE);
}

TEST(RespParse, PipelinedCommands) {
    std::string input =
        "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0, offset = 0;

    resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed);
    EXPECT_TRUE(arg_eq(cmd.args[0], "SET"));
    offset += consumed;

    resp_parse(reinterpret_cast<const uint8_t *>(input.data()) + offset, static_cast<uint32_t>(input.size() - offset), &cmd, &consumed);
    EXPECT_TRUE(arg_eq(cmd.args[0], "GET"));
    offset += consumed;
    EXPECT_EQ(offset, input.size());
}

TEST(RespParse, ErrorNotArray) {
    std::string input = "+OK\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, ErrorBadBulkType) {
    std::string input = "*1\r\n+OK\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, LargeValue) {
    std::string value(1000, 'x');
    std::string input = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1000\r\n" + value + "\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t *>(input.data()), static_cast<uint32_t>(input.size()), &cmd, &consumed),
              RespParseResult::OK);
    EXPECT_EQ(cmd.args[2].len, 1000u);
}

TEST(RespFormat, WriteOk) {
    uint8_t buf[16];
    EXPECT_EQ(std::string(reinterpret_cast<char *>(buf), resp_write_ok(buf)), "+OK\r\n");
}

TEST(RespFormat, WriteNull) {
    uint8_t buf[16];
    EXPECT_EQ(std::string(reinterpret_cast<char *>(buf), resp_write_null(buf)), "$-1\r\n");
}

TEST(RespFormat, WritePong) {
    uint8_t buf[16];
    EXPECT_EQ(std::string(reinterpret_cast<char *>(buf), resp_write_pong(buf)), "+PONG\r\n");
}

TEST(RespFormat, WriteBulk) {
    uint8_t buf[64];
    uint32_t n = resp_write_bulk(buf, reinterpret_cast<const uint8_t *>("hello"), 5);
    EXPECT_EQ(std::string(reinterpret_cast<char *>(buf), n), "$5\r\nhello\r\n");
}

TEST(RespFormat, WriteBulkEmpty) {
    uint8_t buf[64];
    uint32_t n = resp_write_bulk(buf, nullptr, 0);
    EXPECT_EQ(std::string(reinterpret_cast<char *>(buf), n), "$0\r\n\r\n");
}

TEST(RespFormat, WriteError) {
    uint8_t buf[128];
    uint32_t n = resp_write_error(buf, "unknown command");
    EXPECT_EQ(std::string(reinterpret_cast<char *>(buf), n), "-ERR unknown command\r\n");
}
