/* dst_test.cpp — Deterministic simulation tests using SimIoBackend. */

#include "command.h"
#include "connection.h"
#include "resp.h"
#include "sim_io_backend.h"
#include "store.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

/* Mirrors worker's handle_recv: parse RESP, execute, return response. */
static std::string process_recv(Connection *conn, const uint8_t *data, uint32_t len,
                                 Store *store) {
    std::string response;
    uint8_t response_buf[65536];

    const uint8_t *parse_buf;
    uint32_t parse_len;
    uint8_t combined[16384 + 4096];

    if (conn->read_len > 0) {
        std::memcpy(combined, conn->read_buf, conn->read_len);
        uint32_t copy_len = len;
        if (conn->read_len + copy_len > sizeof(combined))
            copy_len = sizeof(combined) - conn->read_len;
        std::memcpy(combined + conn->read_len, data, copy_len);
        parse_buf = combined;
        parse_len = conn->read_len + copy_len;
        conn->read_len = 0;
    } else {
        parse_buf = data;
        parse_len = len;
    }

    uint32_t offset = 0;
    while (offset < parse_len) {
        RespCommand cmd;
        uint32_t consumed = 0;
        auto result = resp_parse(parse_buf + offset, parse_len - offset, &cmd, &consumed);
        if (result == RespParseResult::OK) {
            uint32_t n = command_execute(&cmd, store, response_buf, sizeof(response_buf));
            response.append(reinterpret_cast<char *>(response_buf), n);
            offset += consumed;
        } else if (result == RespParseResult::INCOMPLETE) {
            uint32_t remaining = parse_len - offset;
            std::memcpy(conn->read_buf, parse_buf + offset, remaining);
            conn->read_len = remaining;
            break;
        } else {
            conn->closing = true;
            break;
        }
    }
    return response;
}

TEST(DST, SetThenGet) {
    Store store;
    Connection *conn = connection_create(10);
    std::string set_data = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t *>(set_data.data()), static_cast<uint32_t>(set_data.size()), &store), "+OK\r\n");
    std::string get_data = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t *>(get_data.data()), static_cast<uint32_t>(get_data.size()), &store), "$3\r\nbar\r\n");
    connection_destroy(conn);
}

TEST(DST, GetMiss) {
    Store store;
    Connection *conn = connection_create(10);
    std::string data = "*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t *>(data.data()), static_cast<uint32_t>(data.size()), &store), "$-1\r\n");
    connection_destroy(conn);
}

TEST(DST, PipelinedCommands) {
    Store store;
    Connection *conn = connection_create(10);
    std::string data = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
                       "*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t *>(data.data()), static_cast<uint32_t>(data.size()), &store),
              "+OK\r\n$1\r\n1\r\n");
    connection_destroy(conn);
}

TEST(DST, PartialReassembly) {
    Store store;
    Connection *conn = connection_create(10);
    std::string full = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    size_t split = 15;

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t *>(full.data()), static_cast<uint32_t>(split), &store), "");
    EXPECT_GT(conn->read_len, 0u);

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t *>(full.data()) + split,
                           static_cast<uint32_t>(full.size() - split), &store), "+OK\r\n");
    connection_destroy(conn);
}

TEST(DST, ProtocolError) {
    Store store;
    Connection *conn = connection_create(10);
    std::string bad = "GARBAGE\r\n";
    process_recv(conn, reinterpret_cast<const uint8_t *>(bad.data()), static_cast<uint32_t>(bad.size()), &store);
    EXPECT_TRUE(conn->closing);
    connection_destroy(conn);
}

TEST(DST, SimBackendCapturesSend) {
    SimIoBackend sim;
    IoOps ops = sim_io_ops();
    ops.submit_send(reinterpret_cast<IoBackend *>(&sim), 42, reinterpret_cast<const uint8_t *>("+OK\r\n"), 5);
    EXPECT_EQ(sim.sent_data[42], "+OK\r\n");
}

TEST(DST, SimBackendTracksOperations) {
    SimIoBackend sim;
    IoOps ops = sim_io_ops();
    ops.submit_accept(reinterpret_cast<IoBackend *>(&sim), 5);
    EXPECT_TRUE(sim.accept_armed);
    ops.submit_recv(reinterpret_cast<IoBackend *>(&sim), 10);
    EXPECT_EQ(sim.recv_armed[0], 10);
    ops.submit_close(reinterpret_cast<IoBackend *>(&sim), 10);
    EXPECT_EQ(sim.closed_fds[0], 10);
}

TEST(DST, SimBackendWait) {
    SimIoBackend sim;
    IoOps ops = sim_io_ops();
    sim.pending.push_back(sim_accept(10));
    std::string data = "*1\r\n$4\r\nPING\r\n";
    sim.pending.push_back(sim_recv(&sim, 10, data.data(), static_cast<uint32_t>(data.size())));

    IoCompletion out[16];
    int n = ops.wait(reinterpret_cast<IoBackend *>(&sim), out, 16);
    EXPECT_EQ(n, 2);
    EXPECT_EQ(out[0].kind, IoCompletion::ACCEPT);
    EXPECT_EQ(out[1].kind, IoCompletion::RECV);
}

TEST(DST, MultipleConnections) {
    Store store;
    Connection *c1 = connection_create(10), *c2 = connection_create(11);
    std::string s1 = "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n";
    std::string s2 = "*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
    EXPECT_EQ(process_recv(c1, reinterpret_cast<const uint8_t *>(s1.data()), static_cast<uint32_t>(s1.size()), &store), "+OK\r\n");
    EXPECT_EQ(process_recv(c2, reinterpret_cast<const uint8_t *>(s2.data()), static_cast<uint32_t>(s2.size()), &store), "+OK\r\n");

    std::string g1 = "*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n";
    EXPECT_EQ(process_recv(c2, reinterpret_cast<const uint8_t *>(g1.data()), static_cast<uint32_t>(g1.size()), &store), "$2\r\nv1\r\n");
    connection_destroy(c1);
    connection_destroy(c2);
}
