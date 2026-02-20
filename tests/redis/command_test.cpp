/* command_test.cpp — Command dispatch tests. */

#include "command.h"
#include "resp.h"
#include "store.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

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
