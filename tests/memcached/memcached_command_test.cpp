// memcached_command_test.cpp — Command dispatch tests for memcached protocol.

#include "memcached/memcached_command.h"
#include "memcached/memcached_parse.h"
#include "store/store.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

// Helper: parse a raw memcached command string and execute it.
static std::string exec(const std::string& input, Store* store, uint64_t now_ms = 12345) {
    McCommand cmd = {};
    uint32_t consumed = 0;
    auto result = mc_parse(reinterpret_cast<const uint8_t*>(input.data()),
                           static_cast<uint32_t>(input.size()), &cmd, &consumed);
    if (result != McParseResult::OK)
        return "<PARSE_ERROR>";

    uint8_t buf[65536];
    uint32_t n = mc_command_execute(&cmd, store, now_ms, buf, sizeof(buf));
    return std::string(reinterpret_cast<char*>(buf), n);
}

// --- Legacy SET/GET round-trip ---

TEST(McCommand, SetThenGet) {
    Store store;
    EXPECT_EQ(exec("set k 0 0 5\r\nhello\r\n", &store), "STORED\r\n");
    EXPECT_EQ(exec("get k\r\n", &store), "VALUE k 0 5\r\nhello\r\nEND\r\n");
}

TEST(McCommand, GetMiss) {
    Store store;
    EXPECT_EQ(exec("get missing\r\n", &store), "END\r\n");
}

TEST(McCommand, ClientFlagsRoundTrip) {
    Store store;
    EXPECT_EQ(exec("set k 42 0 3\r\nfoo\r\n", &store), "STORED\r\n");
    std::string result = exec("get k\r\n", &store);
    EXPECT_TRUE(result.find("VALUE k 42 3") != std::string::npos);
}

TEST(McCommand, SetOverwrite) {
    Store store;
    exec("set k 0 0 5\r\nfirst\r\n", &store);
    exec("set k 0 0 6\r\nsecond\r\n", &store);
    std::string result = exec("get k\r\n", &store);
    EXPECT_TRUE(result.find("second") != std::string::npos);
}

// --- Legacy DELETE ---

TEST(McCommand, DeleteHit) {
    Store store;
    exec("set k 0 0 3\r\nfoo\r\n", &store);
    EXPECT_EQ(exec("delete k\r\n", &store), "DELETED\r\n");
    EXPECT_EQ(exec("get k\r\n", &store), "END\r\n");
}

TEST(McCommand, DeleteMiss) {
    Store store;
    EXPECT_EQ(exec("delete missing\r\n", &store), "NOT_FOUND\r\n");
}

TEST(McCommand, DeleteNoreply) {
    Store store;
    exec("set k 0 0 3\r\nfoo\r\n", &store);
    EXPECT_EQ(exec("delete k noreply\r\n", &store), "");
}

// --- VERSION ---

TEST(McCommand, Version) {
    Store store;
    std::string result = exec("version\r\n", &store);
    EXPECT_TRUE(result.starts_with("VERSION"));
}

// --- Legacy SET with expiration ---

TEST(McCommand, SetWithExptime) {
    Store store;
    EXPECT_EQ(exec("set k 0 1 3\r\nfoo\r\n", &store, 1000), "STORED\r\n");
    // Still alive at 1500ms.
    std::string alive = exec("get k\r\n", &store, 1500);
    EXPECT_TRUE(alive.find("foo") != std::string::npos);
    // Expired at 2001ms.
    EXPECT_EQ(exec("get k\r\n", &store, 2001), "END\r\n");
}

TEST(McCommand, SetNoreply) {
    Store store;
    EXPECT_EQ(exec("set k 0 0 3 noreply\r\nfoo\r\n", &store), "");
    // Value should still be stored.
    std::string result = exec("get k\r\n", &store);
    EXPECT_TRUE(result.find("foo") != std::string::npos);
}

// --- Meta SET/GET round-trip ---

TEST(McCommand, MetaSetThenGet) {
    Store store;
    EXPECT_EQ(exec("ms mykey 5\r\nhello\r\n", &store), "HD\r\n");
    EXPECT_EQ(exec("mg mykey v\r\n", &store), "VA 5\r\nhello\r\n");
}

TEST(McCommand, MetaGetMiss) {
    Store store;
    EXPECT_EQ(exec("mg missing v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaGetNoValueFlag) {
    Store store;
    exec("ms mykey 5\r\nhello\r\n", &store);
    // No 'v' flag — returns HD (hit, no data).
    EXPECT_EQ(exec("mg mykey\r\n", &store), "HD\r\n");
}

TEST(McCommand, MetaSetWithTTL) {
    Store store;
    EXPECT_EQ(exec("ms k 3 T1\r\nfoo\r\n", &store, 1000), "HD\r\n");
    EXPECT_EQ(exec("mg k v\r\n", &store, 1500), "VA 3\r\nfoo\r\n");
    EXPECT_EQ(exec("mg k v\r\n", &store, 2001), "EN\r\n");
}

TEST(McCommand, MetaSetWithFlags) {
    Store store;
    EXPECT_EQ(exec("ms k 3 F99\r\nbar\r\n", &store), "HD\r\n");
    // Verify flags come back via meta get with 'f' flag.
    std::string result = exec("mg k v f\r\n", &store);
    EXPECT_TRUE(result.find("f99") != std::string::npos);
}

TEST(McCommand, MetaSetQuiet) {
    Store store;
    EXPECT_EQ(exec("ms k 3 q\r\nfoo\r\n", &store), "");
}

TEST(McCommand, MetaSetTooManyFlagsRejected) {
    Store store;
    std::string cmd = "ms k 3";
    for (int i = 0; i < MC_MAX_META_FLAGS; i++)
        cmd += " x";
    cmd += " q\r\nfoo\r\n";

    EXPECT_EQ(exec(cmd, &store), "<PARSE_ERROR>");
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaSetMalformedFlags) {
    Store store;
    // Non-numeric F flag should fail.
    EXPECT_EQ(exec("ms k 3 Fabc\r\nfoo\r\n", &store), "ERROR\r\n");
    // Key should not have been stored.
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaSetMalformedTTL) {
    Store store;
    // Non-numeric T flag should fail.
    EXPECT_EQ(exec("ms k 3 Txyz\r\nfoo\r\n", &store), "ERROR\r\n");
    // Key should not have been stored.
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaSetBareF) {
    Store store;
    // Bare "F" with no digits should fail.
    EXPECT_EQ(exec("ms k 3 F\r\nfoo\r\n", &store), "ERROR\r\n");
}

TEST(McCommand, MetaSetDuplicateBadF) {
    Store store;
    // Bare "F" followed by valid "F42" — bare F is malformed, must reject.
    EXPECT_EQ(exec("ms k 3 F F42\r\nfoo\r\n", &store), "ERROR\r\n");
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaSetDuplicateBadT) {
    Store store;
    // Bare "T" followed by valid "T1" — bare T is malformed, must reject.
    EXPECT_EQ(exec("ms k 3 T T1\r\nfoo\r\n", &store), "ERROR\r\n");
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaSetGoodThenBadF) {
    Store store;
    // Valid "F42" followed by malformed "Fxyz" — must reject.
    EXPECT_EQ(exec("ms k 3 F42 Fxyz\r\nfoo\r\n", &store), "ERROR\r\n");
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaSetOverflowF) {
    Store store;
    // F value exceeds uint32_t max (4294967296 = 2^32).
    EXPECT_EQ(exec("ms k 3 F4294967296\r\nfoo\r\n", &store), "ERROR\r\n");
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaSetOverflowT) {
    Store store;
    // T value exceeds uint32_t max.
    EXPECT_EQ(exec("ms k 3 T4294967296\r\nfoo\r\n", &store), "ERROR\r\n");
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

// --- Meta DELETE ---

TEST(McCommand, MetaDeleteHit) {
    Store store;
    exec("ms k 3\r\nfoo\r\n", &store);
    EXPECT_EQ(exec("md k\r\n", &store), "HD\r\n");
    EXPECT_EQ(exec("mg k v\r\n", &store), "EN\r\n");
}

TEST(McCommand, MetaDeleteMiss) {
    Store store;
    EXPECT_EQ(exec("md missing\r\n", &store), "NF\r\n");
}

TEST(McCommand, MetaDeleteQuiet) {
    Store store;
    exec("ms k 3\r\nfoo\r\n", &store);
    EXPECT_EQ(exec("md k q\r\n", &store), "");
}

TEST(McCommand, MetaNoop) {
    Store store;
    EXPECT_EQ(exec("mn\r\n", &store), "MN\r\n");
}

// --- Absolute exptime (> 30 days = epoch timestamp) ---

TEST(McCommand, SetWithAbsoluteExptime) {
    Store store;
    // exptime = 3000000 (> 2592000) → absolute Unix epoch timestamp.
    // 3000000 seconds = 3000000000 ms. Key should be alive before that epoch and expired after.
    EXPECT_EQ(exec("set k 0 3000000 3\r\nfoo\r\n", &store, 2999999000ULL), "STORED\r\n");
    // Alive just before expiration epoch.
    std::string alive = exec("get k\r\n", &store, 2999999500ULL);
    EXPECT_TRUE(alive.find("foo") != std::string::npos);
    // Expired after the epoch timestamp.
    EXPECT_EQ(exec("get k\r\n", &store, 3000001000ULL), "END\r\n");
}

TEST(McCommand, SetWithRelativeExptimeBoundary) {
    Store store;
    // exptime = 2592000 (exactly 30 days) → relative TTL.
    EXPECT_EQ(exec("set k 0 2592000 3\r\nfoo\r\n", &store, 1000), "STORED\r\n");
    // Alive just before TTL expires.
    std::string alive = exec("get k\r\n", &store, 2592000000ULL);
    EXPECT_TRUE(alive.find("foo") != std::string::npos);
    // Expired after relative TTL.
    EXPECT_EQ(exec("get k\r\n", &store, 2592001001ULL), "END\r\n");
}

// --- Rejected commands ---

TEST(McCommand, GetsRejected) {
    Store store;
    exec("set k 0 0 3\r\nfoo\r\n", &store);
    EXPECT_EQ(exec("gets k\r\n", &store), "<PARSE_ERROR>");
}

TEST(McCommand, MultiKeyGetRejected) {
    Store store;
    exec("set k1 0 0 3\r\nfoo\r\n", &store);
    exec("set k2 0 0 3\r\nbar\r\n", &store);
    EXPECT_EQ(exec("get k1 k2\r\n", &store), "<PARSE_ERROR>");
}

// --- Meta GET with metadata flags but no value ---

TEST(McCommand, MetaGetFlagsWithoutValue) {
    Store store;
    exec("ms k 3 F42\r\nbar\r\n", &store);
    // Request flags and size but no value — should return HD with flag tokens.
    std::string result = exec("mg k f s\r\n", &store);
    EXPECT_TRUE(result.starts_with("HD"));
    EXPECT_TRUE(result.find("f42") != std::string::npos);
    EXPECT_TRUE(result.find("s3") != std::string::npos);
}

TEST(McCommand, MetaGetKeyFlagWithoutValue) {
    Store store;
    exec("ms mykey 3\r\nfoo\r\n", &store);
    std::string result = exec("mg mykey k\r\n", &store);
    EXPECT_TRUE(result.starts_with("HD"));
    EXPECT_TRUE(result.find("kmykey") != std::string::npos);
}

// --- Edge cases ---

TEST(McCommand, EmptyValue) {
    Store store;
    EXPECT_EQ(exec("set k 0 0 0\r\n\r\n", &store), "STORED\r\n");
    std::string result = exec("get k\r\n", &store);
    EXPECT_TRUE(result.find("VALUE k 0 0") != std::string::npos);
}

TEST(McCommand, LargeValue) {
    Store store;
    std::string big(4000, 'Z');
    std::string set_cmd = "set bigkey 0 0 4000\r\n" + big + "\r\n";
    EXPECT_EQ(exec(set_cmd, &store), "STORED\r\n");

    std::string result = exec("get bigkey\r\n", &store);
    EXPECT_TRUE(result.find("VALUE bigkey 0 4000") != std::string::npos);
    EXPECT_TRUE(result.find(big) != std::string::npos);
}

TEST(McCommand, OutputBufferOverflow) {
    Store store;
    McCommand cmd = {};
    cmd.type = McCommandType::VERSION;

    // Buffer too small for VERSION (22 bytes) and too small for ERROR (7 bytes).
    uint8_t buf[4] = {};
    uint32_t n = mc_command_execute(&cmd, &store, 12345, buf, sizeof(buf));
    // Should return 0 when buffer is too small even for error response.
    EXPECT_EQ(n, 0u);

    // Buffer large enough for ERROR but not VERSION.
    uint8_t buf2[8] = {};
    n = mc_command_execute(&cmd, &store, 12345, buf2, sizeof(buf2));
    EXPECT_EQ(n, 7u); // "ERROR\r\n"
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf2), n), "ERROR\r\n");
}
