// resp_test.cpp — RESP parser and response formatter tests.

#include "resp.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

static bool arg_eq(const RespArg& arg, const char* expected) {
    return arg.len == std::strlen(expected) && std::memcmp(arg.data, expected, arg.len) == 0;
}

TEST(RespParse, GetCommand) {
    std::string input = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    auto result = resp_parse(reinterpret_cast<const uint8_t*>(input.data()),
                             static_cast<uint32_t>(input.size()), &cmd, &consumed);
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
    auto result = resp_parse(reinterpret_cast<const uint8_t*>(input.data()),
                             static_cast<uint32_t>(input.size()), &cmd, &consumed);
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
    auto result = resp_parse(reinterpret_cast<const uint8_t*>(input.data()),
                             static_cast<uint32_t>(input.size()), &cmd, &consumed);
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
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::INCOMPLETE);
}

TEST(RespParse, IncompletePartialBulkString) {
    std::string input = "*1\r\n$3\r\nG";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::INCOMPLETE);
}

TEST(RespParse, PipelinedCommands) {
    std::string input = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
                        "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0, offset = 0;

    resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()), &cmd,
               &consumed);
    EXPECT_TRUE(arg_eq(cmd.args[0], "SET"));
    offset += consumed;

    resp_parse(reinterpret_cast<const uint8_t*>(input.data()) + offset,
               static_cast<uint32_t>(input.size() - offset), &cmd, &consumed);
    EXPECT_TRUE(arg_eq(cmd.args[0], "GET"));
    offset += consumed;
    EXPECT_EQ(offset, input.size());
}

TEST(RespParse, ErrorNotArray) {
    std::string input = "+OK\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, ErrorBadBulkType) {
    std::string input = "*1\r\n+OK\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, LargeValue) {
    std::string value(1000, 'x');
    std::string input = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1000\r\n" + value + "\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::OK);
    EXPECT_EQ(cmd.args[2].len, 1000u);
}

TEST(RespFormat, WriteOk) {
    uint8_t buf[16];
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), resp_write_ok(buf)), "+OK\r\n");
}

TEST(RespFormat, WriteNull) {
    uint8_t buf[16];
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), resp_write_null(buf)), "$-1\r\n");
}

TEST(RespFormat, WritePong) {
    uint8_t buf[16];
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), resp_write_pong(buf)), "+PONG\r\n");
}

TEST(RespFormat, WriteBulk) {
    uint8_t buf[64];
    uint32_t n = resp_write_bulk(buf, reinterpret_cast<const uint8_t*>("hello"), 5);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), n), "$5\r\nhello\r\n");
}

TEST(RespFormat, WriteBulkEmpty) {
    uint8_t buf[64];
    uint32_t n = resp_write_bulk(buf, nullptr, 0);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), n), "$0\r\n\r\n");
}

TEST(RespFormat, WriteError) {
    uint8_t buf[128];
    uint32_t n = resp_write_error(buf, "unknown command");
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), n), "-ERR unknown command\r\n");
}

// --- Additional parse edge cases ---

TEST(RespParse, MaxArgsBoundary) {
    // RESP_MAX_ARGS=8: exactly 8 args should succeed.
    std::string input = "*8\r\n";
    for (int i = 0; i < 8; i++)
        input += "$1\r\nx\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::OK);
    EXPECT_EQ(cmd.argc, 8);
    EXPECT_EQ(consumed, input.size());
}

TEST(RespParse, TooManyArgs) {
    // 9 args exceeds RESP_MAX_ARGS=8 → ERROR.
    std::string input = "*9\r\n";
    for (int i = 0; i < 9; i++)
        input += "$1\r\nx\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, ZeroLengthBulkString) {
    std::string input = "*2\r\n$3\r\nSET\r\n$0\r\n\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::OK);
    EXPECT_EQ(cmd.args[1].len, 0u);
}

TEST(RespParse, ArgcZero) {
    std::string input = "*0\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, IncompleteArrayCount) {
    // Just '*' with no digits or CRLF.
    std::string input = "*";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::INCOMPLETE);
    EXPECT_EQ(consumed, 0u);
}

TEST(RespParse, IncompleteBulkLength) {
    // Array header complete, but bulk string length prefix cut short.
    std::string input = "*1\r\n$";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::INCOMPLETE);
    EXPECT_EQ(consumed, 0u);
}

TEST(RespParse, IncompleteBulkPayload) {
    // Only 2 of 5 payload bytes present.
    std::string input = "*1\r\n$5\r\nhe";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::INCOMPLETE);
    EXPECT_EQ(consumed, 0u);
}

TEST(RespParse, MultipleIncompleteResumes) {
    // Simulate 3 successive partial chunks: only the third completes the command.
    std::string full = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    size_t split1 = 3;  // "*2\r"
    size_t split2 = 12; // "*2\r\n$3\r\nGET\r"

    RespCommand cmd;
    uint32_t consumed = 0;

    // Chunk 1: incomplete
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(full.data()), static_cast<uint32_t>(split1), &cmd,
                         &consumed),
              RespParseResult::INCOMPLETE);
    EXPECT_EQ(consumed, 0u);

    // Chunk 1+2: still incomplete
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(full.data()), static_cast<uint32_t>(split2), &cmd,
                         &consumed),
              RespParseResult::INCOMPLETE);
    EXPECT_EQ(consumed, 0u);

    // Full message: OK
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(full.data()), static_cast<uint32_t>(full.size()),
                         &cmd, &consumed),
              RespParseResult::OK);
    EXPECT_EQ(consumed, full.size());
    EXPECT_TRUE(arg_eq(cmd.args[0], "GET"));
}

TEST(RespParse, ConsumedIsZeroOnIncomplete) {
    std::string input = "*2\r\n$3\r\nGET\r\n";
    RespCommand cmd;
    uint32_t consumed = 42; // pre-set to non-zero
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::INCOMPLETE);
    EXPECT_EQ(consumed, 0u);
}

TEST(RespParse, ConsumedMatchesFull) {
    std::string input = "*1\r\n$4\r\nPING\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::OK);
    EXPECT_EQ(consumed, static_cast<uint32_t>(input.size()));
}

TEST(RespParse, BinaryData) {
    // Bulk string containing \0, \r, \n embedded bytes.
    uint8_t payload[] = {'h', 0x00, '\r', '\n', 'o'};
    std::string input = "*1\r\n$5\r\n";
    input.append(reinterpret_cast<char*>(payload), 5);
    input += "\r\n";

    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::OK);
    EXPECT_EQ(cmd.args[0].len, 5u);
    EXPECT_EQ(std::memcmp(cmd.args[0].data, payload, 5), 0);
}

TEST(RespParse, SingleByteChunk) {
    // Only the first byte '*' → INCOMPLETE.
    std::string input = "*";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::INCOMPLETE);
}

// --- Additional format edge cases ---

TEST(RespFormat, WriteBulkLargeLen) {
    // Bulk string with multi-digit length prefix.
    std::string payload(12345, 'A');
    std::vector<uint8_t> buf(12345 + 32);
    uint32_t n = resp_write_bulk(buf.data(), reinterpret_cast<const uint8_t*>(payload.data()),
                                 static_cast<uint32_t>(payload.size()));
    std::string result(reinterpret_cast<char*>(buf.data()), n);
    EXPECT_TRUE(result.starts_with("$12345\r\n"));
    EXPECT_TRUE(result.ends_with("\r\n"));
    EXPECT_EQ(n, 8u + 12345u + 2u); // "$12345\r\n" + data + "\r\n"
}

TEST(RespFormat, WriteErrorLong) {
    // Error message near 512-byte truncation boundary.
    std::string long_msg(500, 'X');
    uint8_t buf[600];
    uint32_t n = resp_write_error(buf, long_msg.c_str());
    std::string result(reinterpret_cast<char*>(buf), n);
    EXPECT_TRUE(result.starts_with("-ERR "));
    // snprintf truncates at 512 total including null → 511 chars max
    EXPECT_LE(n, 511u);
}

TEST(RespFormat, WriteErrorEmpty) {
    uint8_t buf[128];
    uint32_t n = resp_write_error(buf, "");
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), n), "-ERR \r\n");
}

TEST(RespFormat, WriteErrorTruncation) {
    // 600-char message should be truncated; returned length must not exceed 511.
    std::string long_msg(600, 'Z');
    uint8_t buf[1024];
    uint32_t n = resp_write_error(buf, long_msg.c_str());
    EXPECT_LE(n, 511u);
    EXPECT_GT(n, 0u);
    // Output should start with the error prefix.
    std::string result(reinterpret_cast<char*>(buf), n);
    EXPECT_TRUE(result.starts_with("-ERR "));
}

// --- parse_int validation tests ---

TEST(RespParse, NonDigitInArrayCount) {
    // '*' followed by digits then a letter before \r\n → ERROR.
    std::string input = "*2A\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, NonDigitInBulkLength) {
    // Bulk string length with non-digit → ERROR.
    std::string input = "*1\r\n$3x\r\nGET\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, OverflowArrayCount) {
    // Huge number that overflows int → ERROR.
    std::string input = "*99999999999\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, OverflowBulkLength) {
    // Huge bulk string length that overflows int → ERROR.
    std::string input = "*1\r\n$99999999999\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, LetterOnlyCount) {
    // Array count with only letters → ERROR.
    std::string input = "*abc\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, EmptyDigitsInArrayCount) {
    // No digits between '*' and '\r\n' → ERROR.
    std::string input = "*\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, SpaceInCount) {
    // Space before digits → ERROR (space is not a digit).
    std::string input = "* 2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, NegativeArgcIsError) {
    std::string input = "*-1\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, NegativeBulkLenInRequestIsError) {
    std::string input = "*1\r\n$-1\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, InvalidCountTerminatorIsError) {
    // Array count line must end with CRLF.
    std::string input = "*2\rX$3\r\nGET\r\n$3\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

TEST(RespParse, InvalidBulkPayloadTrailerIsError) {
    // Bulk payload must be followed by CRLF.
    std::string input = "*1\r\n$3\r\nGETXX";
    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::ERROR);
}

// --- T10: Leading zeros in length prefix ---

TEST(RespParse, LeadingZerosInArrayCount) {
    // "*02\r\n..." — leading zeros are accepted since parse_int accepts any
    // sequence of digits. Verify defined behavior.
    std::string input = "*02\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    auto result = resp_parse(reinterpret_cast<const uint8_t*>(input.data()),
                             static_cast<uint32_t>(input.size()), &cmd, &consumed);
    EXPECT_EQ(result, RespParseResult::OK);
    EXPECT_EQ(cmd.argc, 2);
}

TEST(RespParse, LeadingZerosInBulkLength) {
    // "$0005\r\nhello\r\n" — 5 bytes with leading zeros.
    std::string input = "*1\r\n$0005\r\nhello\r\n";
    RespCommand cmd;
    uint32_t consumed = 0;
    auto result = resp_parse(reinterpret_cast<const uint8_t*>(input.data()),
                             static_cast<uint32_t>(input.size()), &cmd, &consumed);
    EXPECT_EQ(result, RespParseResult::OK);
    EXPECT_EQ(cmd.args[0].len, 5u);
    EXPECT_EQ(std::memcmp(cmd.args[0].data, "hello", 5), 0);
}

// --- T4: Binary-safe keys/values with embedded nulls ---

TEST(RespParse, BulkStringWithEmbeddedNulls) {
    // Bulk string containing multiple \0 bytes.
    uint8_t payload[] = {0x00, 0x00, 0x00, 'a', 0x00};
    std::string input = "*1\r\n$5\r\n";
    input.append(reinterpret_cast<char*>(payload), 5);
    input += "\r\n";

    RespCommand cmd;
    uint32_t consumed = 0;
    EXPECT_EQ(resp_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()),
                         &cmd, &consumed),
              RespParseResult::OK);
    EXPECT_EQ(cmd.args[0].len, 5u);
    EXPECT_EQ(std::memcmp(cmd.args[0].data, payload, 5), 0);
}
