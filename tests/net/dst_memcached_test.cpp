// dst_memcached_test.cpp — Deterministic simulation tests for memcached protocol.

#include "connection.h"
#include "protocol/protocol.h"
#include "sim_io_backend.h"
#include "worker.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

// Mirrors worker's handle_recv: parse selected protocol, execute, return response.
static std::string process_recv(Connection* conn, const uint8_t* data, uint32_t len,
                                ProtocolWorkerState* protocol_state) {
    std::string response;
    uint8_t response_buf[65536];

    const uint8_t* parse_buf;
    uint32_t parse_len;
    uint8_t combined[16384 + 4096];

    if (conn->input_len > 0) {
        std::memcpy(combined, conn->input_buf, conn->input_len);
        uint32_t copy_len = len;
        if (conn->input_len + copy_len > sizeof(combined))
            copy_len = sizeof(combined) - conn->input_len;
        std::memcpy(combined + conn->input_len, data, copy_len);
        parse_buf = combined;
        parse_len = conn->input_len + copy_len;
        conn->input_len = 0;
    } else {
        parse_buf = data;
        parse_len = len;
    }

    uint32_t offset = 0;
    while (offset < parse_len) {
        ProtocolCommand cmd;
        uint32_t consumed = 0;
        auto result = protocol_parse(parse_buf + offset, parse_len - offset, &cmd, &consumed);
        if (result == PROTOCOL_PARSE_OK) {
            uint32_t n =
                protocol_execute(&cmd, protocol_state, protocol_now_ms(), response_buf, sizeof(response_buf));
            response.append(reinterpret_cast<char*>(response_buf), n);
            offset += consumed;
        } else if (result == PROTOCOL_PARSE_INCOMPLETE) {
            uint32_t remaining = parse_len - offset;
            std::memcpy(conn->input_buf, parse_buf + offset, remaining);
            conn->input_len = remaining;
            break;
        } else {
            conn->closing = true;
            break;
        }
    }
    return response;
}

TEST(DSTMemcached, SetThenGet) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string set_data = "set foo 0 0 3\r\nbar\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(set_data.data()),
                           static_cast<uint32_t>(set_data.size()), &state),
              "STORED\r\n");
    std::string get_data = "get foo\r\n";
    std::string result = process_recv(conn, reinterpret_cast<const uint8_t*>(get_data.data()),
                                      static_cast<uint32_t>(get_data.size()), &state);
    EXPECT_TRUE(result.find("VALUE foo 0 3") != std::string::npos);
    EXPECT_TRUE(result.find("bar") != std::string::npos);
    EXPECT_TRUE(result.find("END") != std::string::npos);
    connection_destroy(conn);
}

TEST(DSTMemcached, GetMiss) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string data = "get missing\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(data.data()),
                           static_cast<uint32_t>(data.size()), &state),
              "END\r\n");
    connection_destroy(conn);
}

TEST(DSTMemcached, PipelinedCommands) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string data = "set a 0 0 1\r\n1\r\nget a\r\n";
    std::string result = process_recv(conn, reinterpret_cast<const uint8_t*>(data.data()),
                                      static_cast<uint32_t>(data.size()), &state);
    EXPECT_TRUE(result.find("STORED") != std::string::npos);
    EXPECT_TRUE(result.find("VALUE a 0 1") != std::string::npos);
    connection_destroy(conn);
}

TEST(DSTMemcached, PartialReassembly) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string full = "set foo 0 0 3\r\nbar\r\n";
    size_t split = 10;

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()), static_cast<uint32_t>(split),
                           &state),
              "");
    EXPECT_GT(conn->input_len, 0u);

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()) + split,
                           static_cast<uint32_t>(full.size() - split), &state),
              "STORED\r\n");
    connection_destroy(conn);
}

TEST(DSTMemcached, ProtocolError) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string bad = "GARBAGE\r\n";
    process_recv(conn, reinterpret_cast<const uint8_t*>(bad.data()), static_cast<uint32_t>(bad.size()),
                 &state);
    EXPECT_TRUE(conn->closing);
    connection_destroy(conn);
}

TEST(DSTMemcached, Version) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string data = "version\r\n";
    std::string result = process_recv(conn, reinterpret_cast<const uint8_t*>(data.data()),
                                      static_cast<uint32_t>(data.size()), &state);
    EXPECT_TRUE(result.starts_with("VERSION"));
    connection_destroy(conn);
}

TEST(DSTMemcached, Delete) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string set_data = "set k 0 0 3\r\nfoo\r\n";
    process_recv(conn, reinterpret_cast<const uint8_t*>(set_data.data()),
                 static_cast<uint32_t>(set_data.size()), &state);
    std::string del_data = "delete k\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(del_data.data()),
                           static_cast<uint32_t>(del_data.size()), &state),
              "DELETED\r\n");
    std::string get_data = "get k\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(get_data.data()),
                           static_cast<uint32_t>(get_data.size()), &state),
              "END\r\n");
    connection_destroy(conn);
}

TEST(DSTMemcached, MetaSetThenGet) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string set_data = "ms mykey 5\r\nhello\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(set_data.data()),
                           static_cast<uint32_t>(set_data.size()), &state),
              "HD\r\n");
    std::string get_data = "mg mykey v\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(get_data.data()),
                           static_cast<uint32_t>(get_data.size()), &state),
              "VA 5\r\nhello\r\n");
    connection_destroy(conn);
}

TEST(DSTMemcached, MetaDelete) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string set_data = "ms k 3\r\nfoo\r\n";
    process_recv(conn, reinterpret_cast<const uint8_t*>(set_data.data()),
                 static_cast<uint32_t>(set_data.size()), &state);
    std::string del_data = "md k\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(del_data.data()),
                           static_cast<uint32_t>(del_data.size()), &state),
              "HD\r\n");
    std::string get_data = "mg k v\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(get_data.data()),
                           static_cast<uint32_t>(get_data.size()), &state),
              "EN\r\n");
    connection_destroy(conn);
}

TEST(DSTMemcached, MetaNoop) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string data = "mn\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(data.data()),
                           static_cast<uint32_t>(data.size()), &state),
              "MN\r\n");
    connection_destroy(conn);
}

TEST(DSTMemcached, TooManyMetaFlagsIsProtocolError) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string data = "mg mykey";
    for (int i = 0; i < MC_MAX_META_FLAGS + 1; i++)
        data += " x";
    data += "\r\n";

    process_recv(conn, reinterpret_cast<const uint8_t*>(data.data()), static_cast<uint32_t>(data.size()),
                 &state);
    EXPECT_TRUE(conn->closing);
    connection_destroy(conn);
}

TEST(DSTMemcached, SimBackendCapturesSend) {
    SimIoBackend sim;
    IoOps ops = sim_io_ops();
    ops.submit_send(reinterpret_cast<IoBackend*>(&sim), 42, reinterpret_cast<const uint8_t*>("STORED\r\n"),
                    8);
    EXPECT_EQ(sim.sent_data[42], "STORED\r\n");
}
