// memcached_parse_test.cpp — Parser tests for memcached text protocol.

#include "memcached/memcached_parse.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

static McParseResult parse(const std::string& input, McCommand* cmd, uint32_t* consumed) {
    return mc_parse(reinterpret_cast<const uint8_t*>(input.data()), static_cast<uint32_t>(input.size()), cmd,
                    consumed);
}

// --- Legacy GET ---

TEST(McParse, LegacyGetComplete) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "get mykey\r\n";
    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::GET);
    EXPECT_EQ(consumed, 11u);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(cmd.key.data), cmd.key.len), "mykey");
}

TEST(McParse, LegacyGetIncomplete) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("get mykey", &cmd, &consumed), McParseResult::INCOMPLETE);
    EXPECT_EQ(consumed, 0u);
}

TEST(McParse, LegacyGetNoKey) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("get\r\n", &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, LegacyGetCaseInsensitive) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("GET mykey\r\n", &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::GET);
}

TEST(McParse, LegacyGetsRejected) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    // gets not supported (no CAS) — must return ERROR.
    EXPECT_EQ(parse("gets mykey\r\n", &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, LegacyGetMultiKeyRejected) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    // Multi-key get not supported — must return ERROR.
    EXPECT_EQ(parse("get k1 k2\r\n", &cmd, &consumed), McParseResult::ERROR);
}

// --- Legacy SET ---

TEST(McParse, LegacySetComplete) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "set mykey 0 60 5\r\nhello\r\n";
    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::SET);
    EXPECT_EQ(consumed, static_cast<uint32_t>(input.size()));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(cmd.key.data), cmd.key.len), "mykey");
    EXPECT_EQ(cmd.client_flags, 0u);
    EXPECT_EQ(cmd.exptime, 60u);
    EXPECT_EQ(cmd.bytes, 5u);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(cmd.value.data), cmd.value.len), "hello");
    EXPECT_FALSE(cmd.noreply);
}

TEST(McParse, LegacySetIncompleteCommandLine) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("set mykey 0 60 5", &cmd, &consumed), McParseResult::INCOMPLETE);
}

TEST(McParse, LegacySetIncompleteDataBlock) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("set mykey 0 60 5\r\nhel", &cmd, &consumed), McParseResult::INCOMPLETE);
}

TEST(McParse, LegacySetWithFlags) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("set k 42 0 3\r\nfoo\r\n", &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.client_flags, 42u);
}

TEST(McParse, LegacySetNoreply) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("set k 0 0 3 noreply\r\nfoo\r\n", &cmd, &consumed), McParseResult::OK);
    EXPECT_TRUE(cmd.noreply);
}

TEST(McParse, LegacySetZeroBytes) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("set k 0 0 0\r\n\r\n", &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.value.len, 0u);
}

// --- Legacy DELETE ---

TEST(McParse, LegacyDeleteComplete) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "delete mykey\r\n";
    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::DELETE);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(cmd.key.data), cmd.key.len), "mykey");
}

TEST(McParse, LegacyDeleteNoreply) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("delete mykey noreply\r\n", &cmd, &consumed), McParseResult::OK);
    EXPECT_TRUE(cmd.noreply);
}

// --- VERSION ---

TEST(McParse, VersionComplete) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("version\r\n", &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::VERSION);
    EXPECT_EQ(consumed, 9u);
}

TEST(McParse, VersionCaseInsensitive) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("VERSION\r\n", &cmd, &consumed), McParseResult::OK);
}

// --- Meta GET ---

TEST(McParse, MetaGetBasic) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "mg mykey\r\n";
    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::META_GET);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(cmd.key.data), cmd.key.len), "mykey");
    EXPECT_EQ(cmd.meta_flag_count, 0);
}

TEST(McParse, MetaGetWithFlags) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "mg mykey v f s\r\n";
    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::META_GET);
    EXPECT_EQ(cmd.meta_flag_count, 3);
    EXPECT_EQ(cmd.meta_flags[0].data[0], 'v');
    EXPECT_EQ(cmd.meta_flags[1].data[0], 'f');
    EXPECT_EQ(cmd.meta_flags[2].data[0], 's');
}

TEST(McParse, MetaGetMaxFlagsBoundary) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "mg mykey";
    for (int i = 0; i < MC_MAX_META_FLAGS; i++)
        input += " f";
    input += "\r\n";

    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.meta_flag_count, MC_MAX_META_FLAGS);
}

TEST(McParse, MetaGetTooManyFlagsRejected) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "mg mykey";
    for (int i = 0; i < MC_MAX_META_FLAGS + 1; i++)
        input += " f";
    input += "\r\n";

    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, MetaGetIncomplete) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("mg mykey v", &cmd, &consumed), McParseResult::INCOMPLETE);
}

// --- Meta SET ---

TEST(McParse, MetaSetBasic) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "ms mykey 5\r\nhello\r\n";
    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::META_SET);
    EXPECT_EQ(consumed, static_cast<uint32_t>(input.size()));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(cmd.value.data), cmd.value.len), "hello");
}

TEST(McParse, MetaSetWithFlags) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "ms mykey 5 T120 F42\r\nhello\r\n";
    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::META_SET);
    EXPECT_EQ(cmd.meta_flag_count, 2);
}

TEST(McParse, MetaSetTooManyFlagsRejected) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "ms mykey 5";
    for (int i = 0; i < MC_MAX_META_FLAGS; i++)
        input += " x";
    input += " q\r\nhello\r\n";

    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, MetaSetIncompleteData) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("ms mykey 5\r\nhel", &cmd, &consumed), McParseResult::INCOMPLETE);
}

// --- Meta DELETE ---

TEST(McParse, MetaDeleteBasic) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("md mykey\r\n", &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::META_DELETE);
}

TEST(McParse, MetaDeleteWithFlags) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "md mykey q\r\n";
    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.meta_flag_count, 1);
    EXPECT_EQ(cmd.meta_flags[0].data[0], 'q');
}

TEST(McParse, MetaDeleteTooManyFlagsRejected) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    std::string input = "md mykey";
    for (int i = 0; i < MC_MAX_META_FLAGS + 1; i++)
        input += " x";
    input += "\r\n";

    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::ERROR);
}

// --- Meta NOOP ---

TEST(McParse, MetaNoopBasic) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("mn\r\n", &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::META_NOOP);
    EXPECT_EQ(consumed, 4u);
}

// --- Error cases ---

TEST(McParse, EmptyInput) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(mc_parse(nullptr, 0, &cmd, &consumed), McParseResult::INCOMPLETE);
}

TEST(McParse, GarbageInput) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("FOOBAR\r\n", &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, QuitClosesConnection) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("quit\r\n", &cmd, &consumed), McParseResult::ERROR);
}

// --- Pipeline (multiple commands in one buffer) ---

TEST(McParse, PipelineTwoCommands) {
    std::string input = "version\r\nget k\r\n";
    McCommand cmd = {};
    uint32_t consumed = 0;

    EXPECT_EQ(parse(input, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::VERSION);
    EXPECT_EQ(consumed, 9u);

    // Parse second command from remaining bytes.
    std::string rest = input.substr(consumed);
    EXPECT_EQ(parse(rest, &cmd, &consumed), McParseResult::OK);
    EXPECT_EQ(cmd.type, McCommandType::GET);
}

TEST(McParse, LegacySetBadDataTerminator) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    // Data block not terminated with \r\n
    EXPECT_EQ(parse("set k 0 0 3\r\nfooXX", &cmd, &consumed), McParseResult::ERROR);
}

// --- Trailing garbage rejection ---

TEST(McParse, LegacySetTrailingGarbage) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    // Extra token after bytes that isn't noreply.
    EXPECT_EQ(parse("set k 0 0 3 junk\r\nfoo\r\n", &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, LegacySetTrailingAfterNoreply) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    // Extra token after noreply.
    EXPECT_EQ(parse("set k 0 0 3 noreply extra\r\nfoo\r\n", &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, LegacyDeleteTrailingGarbage) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    // Extra token after key that isn't noreply.
    EXPECT_EQ(parse("delete k junk\r\n", &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, LegacyDeleteTrailingAfterNoreply) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("delete k noreply extra\r\n", &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, VersionTrailingGarbage) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("version foo\r\n", &cmd, &consumed), McParseResult::ERROR);
}

TEST(McParse, MetaNoopTrailingGarbage) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    EXPECT_EQ(parse("mn foo\r\n", &cmd, &consumed), McParseResult::ERROR);
}
