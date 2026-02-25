// command_test.cpp — Command dispatch tests.

#include "command.h"
#include "resp.h"
#include "store.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

static RespCommand make_cmd(std::initializer_list<const char *> args) {
    RespCommand cmd = {};
    int i = 0;
    for (const char *arg : args) {
        cmd.args[i].data = reinterpret_cast<const uint8_t *>(arg);
        cmd.args[i].len = static_cast<uint32_t>(std::strlen(arg));
        i++;
    }
    cmd.argc = i;
    return cmd;
}

static std::string exec(const RespCommand &cmd, Store *store) {
    uint8_t buf[4096];
    uint32_t n = command_execute(&cmd, store, buf, sizeof(buf));
    return std::string(reinterpret_cast<char *>(buf), n);
}

TEST(Command, GetMiss) {
    Store store;
    EXPECT_EQ(exec(make_cmd({"GET", "x"}), &store), "$-1\r\n");
}

TEST(Command, SetThenGet) {
    Store store;
    EXPECT_EQ(exec(make_cmd({"SET", "k", "myvalue"}), &store), "+OK\r\n");
    EXPECT_EQ(exec(make_cmd({"GET", "k"}), &store), "$7\r\nmyvalue\r\n");
}

TEST(Command, SetOverwrite) {
    Store store;
    exec(make_cmd({"SET", "k", "first"}), &store);
    exec(make_cmd({"SET", "k", "second"}), &store);
    EXPECT_EQ(exec(make_cmd({"GET", "k"}), &store), "$6\r\nsecond\r\n");
}

TEST(Command, Ping) {
    Store store;
    EXPECT_EQ(exec(make_cmd({"PING"}), &store), "+PONG\r\n");
}

TEST(Command, CommandStub) {
    Store store;
    EXPECT_EQ(exec(make_cmd({"COMMAND", "DOCS"}), &store), "*0\r\n");
}

TEST(Command, UnknownCommand) {
    Store store;
    EXPECT_TRUE(exec(make_cmd({"FOOBAR"}), &store).starts_with("-ERR"));
}

TEST(Command, GetWrongArity) {
    Store store;
    EXPECT_TRUE(exec(make_cmd({"GET"}), &store).starts_with("-ERR"));
}

TEST(Command, SetWrongArity) {
    Store store;
    EXPECT_TRUE(exec(make_cmd({"SET", "k"}), &store).starts_with("-ERR"));
}

TEST(Command, CaseInsensitive) {
    Store store;
    EXPECT_EQ(exec(make_cmd({"set", "foo", "bar"}), &store), "+OK\r\n");
    EXPECT_EQ(exec(make_cmd({"get", "foo"}), &store), "$3\r\nbar\r\n");
}

TEST(Command, MixedCase) {
    Store store;
    EXPECT_EQ(exec(make_cmd({"SeT", "k", "v"}), &store), "+OK\r\n");
    EXPECT_EQ(exec(make_cmd({"gEt", "k"}), &store), "$1\r\nv\r\n");
}

// --- Additional command edge cases ---

TEST(Command, EmptyCommand) {
    Store store;
    RespCommand cmd = {};
    cmd.argc = 0;
    std::string result = exec(cmd, &store);
    EXPECT_TRUE(result.starts_with("-ERR"));
    EXPECT_NE(result.find("empty command"), std::string::npos);
}

TEST(Command, EmptyKey) {
    Store store;
    // SET with empty key, then GET with empty key.
    EXPECT_EQ(exec(make_cmd({"SET", "", "val"}), &store), "+OK\r\n");
    EXPECT_EQ(exec(make_cmd({"GET", ""}), &store), "$3\r\nval\r\n");
}

TEST(Command, EmptyValue) {
    Store store;
    EXPECT_EQ(exec(make_cmd({"SET", "k", ""}), &store), "+OK\r\n");
    EXPECT_EQ(exec(make_cmd({"GET", "k"}), &store), "$0\r\n\r\n");
}

TEST(Command, LargeValue) {
    Store store;
    std::string big(4000, 'Z');
    RespCommand cmd = {};
    const char *set_str = "SET";
    const char *key_str = "bigkey";
    cmd.args[0].data = reinterpret_cast<const uint8_t *>(set_str);
    cmd.args[0].len = 3;
    cmd.args[1].data = reinterpret_cast<const uint8_t *>(key_str);
    cmd.args[1].len = 6;
    cmd.args[2].data = reinterpret_cast<const uint8_t *>(big.data());
    cmd.args[2].len = static_cast<uint32_t>(big.size());
    cmd.argc = 3;
    EXPECT_EQ(exec(cmd, &store), "+OK\r\n");

    // GET it back.
    std::string get_result = exec(make_cmd({"GET", "bigkey"}), &store);
    EXPECT_TRUE(get_result.starts_with("$4000\r\n"));
    EXPECT_TRUE(get_result.ends_with("\r\n"));
    EXPECT_EQ(get_result.size(), 7u + 4000u + 2u); // "$4000\r\n" + data + "\r\n"
}

TEST(Command, BinaryKeyValue) {
    Store store;
    // Key and value containing null bytes.
    uint8_t key_data[] = {'k', 0x00, 'y'};
    uint8_t val_data[] = {'v', 0x00, '\n', 'l'};

    RespCommand set_cmd = {};
    const char *set_str = "SET";
    set_cmd.args[0].data = reinterpret_cast<const uint8_t *>(set_str);
    set_cmd.args[0].len = 3;
    set_cmd.args[1].data = key_data;
    set_cmd.args[1].len = 3;
    set_cmd.args[2].data = val_data;
    set_cmd.args[2].len = 4;
    set_cmd.argc = 3;

    uint8_t buf[4096];
    uint32_t n = command_execute(&set_cmd, &store, buf, sizeof(buf));
    EXPECT_EQ(std::string(reinterpret_cast<char *>(buf), n), "+OK\r\n");

    // GET with same binary key.
    RespCommand get_cmd = {};
    const char *get_str = "GET";
    get_cmd.args[0].data = reinterpret_cast<const uint8_t *>(get_str);
    get_cmd.args[0].len = 3;
    get_cmd.args[1].data = key_data;
    get_cmd.args[1].len = 3;
    get_cmd.argc = 2;

    n = command_execute(&get_cmd, &store, buf, sizeof(buf));
    std::string result(reinterpret_cast<char *>(buf), n);
    EXPECT_TRUE(result.starts_with("$4\r\n"));
}

TEST(Command, OverwriteMultipleTimes) {
    Store store;
    exec(make_cmd({"SET", "k", "first"}), &store);
    exec(make_cmd({"SET", "k", "second"}), &store);
    exec(make_cmd({"SET", "k", "third"}), &store);
    EXPECT_EQ(exec(make_cmd({"GET", "k"}), &store), "$5\r\nthird\r\n");
}

TEST(Command, GetTooManyArgs) {
    Store store;
    std::string result = exec(make_cmd({"GET", "a", "b", "c"}), &store);
    EXPECT_TRUE(result.starts_with("-ERR"));
}

TEST(Command, SetTooFewArgs) {
    Store store;
    std::string result = exec(make_cmd({"SET", "k"}), &store);
    EXPECT_TRUE(result.starts_with("-ERR"));
}

TEST(Command, SetTooManyArgs) {
    Store store;
    std::string result = exec(make_cmd({"SET", "k", "v", "extra"}), &store);
    EXPECT_TRUE(result.starts_with("-ERR"));
}

TEST(Command, PingExtraArgs) {
    // PING currently ignores argc — verify it still returns PONG.
    Store store;
    EXPECT_EQ(exec(make_cmd({"PING", "hello"}), &store), "+PONG\r\n");
}

TEST(Command, CommandWithArgs) {
    // COMMAND DOCS with extra args → still returns *0\r\n.
    Store store;
    EXPECT_EQ(exec(make_cmd({"COMMAND", "DOCS", "GET"}), &store), "*0\r\n");
}

static std::string exec_sized(const RespCommand &cmd, Store *store, uint32_t buf_size) {
    std::vector<uint8_t> buf(buf_size);
    uint32_t n = command_execute(&cmd, store, buf.data(), buf_size);
    return std::string(reinterpret_cast<char *>(buf.data()), n);
}

TEST(Command, OutputBufferTooSmall) {
    Store store;
    // buf_size=3 is less than the 5 bytes needed for "+OK\r\n".
    std::string result = exec_sized(make_cmd({"SET", "k", "v"}), &store, 3);
    // Should get a truncated error (write_error_bounded with size 3).
    EXPECT_LE(result.size(), 3u);
}

TEST(Command, OutputBufferExactFitSet) {
    Store store;
    // "+OK\r\n" is 5 bytes; buffer of exactly 5 should work.
    std::string result = exec_sized(make_cmd({"SET", "k", "v"}), &store, 5);
    EXPECT_EQ(result, "+OK\r\n");
}

TEST(Command, SetOOMReturnsErr) {
    KvStoreConfig cfg = {
        .capacity_bytes = 128,
        .shard_count = 1,
        .buckets_per_shard = 16,
        .worker_count = 1,
    };
    KvStore *shared = kv_store_create(&cfg);
    ASSERT_NE(shared, nullptr);
    ASSERT_EQ(kv_store_register_worker(shared, 0), 0);

    Store store;
    store_bind_shared(&store, shared, 0);

    std::string big(256, 'x');
    RespCommand cmd = {};
    const char *set_str = "SET";
    const char *key = "k";
    cmd.args[0].data = reinterpret_cast<const uint8_t *>(set_str);
    cmd.args[0].len = 3;
    cmd.args[1].data = reinterpret_cast<const uint8_t *>(key);
    cmd.args[1].len = 1;
    cmd.args[2].data = reinterpret_cast<const uint8_t *>(big.data());
    cmd.args[2].len = static_cast<uint32_t>(big.size());
    cmd.argc = 3;

    std::string out = exec(cmd, &store);
    EXPECT_TRUE(out.starts_with("-ERR"));
    EXPECT_NE(out.find("out of memory"), std::string::npos);

    kv_store_destroy(shared);
}
