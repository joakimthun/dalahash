// connection.h — Per-connection state for TCP clients.

#pragma once

#include "base/assert.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>

static constexpr uint32_t CONN_BUF_SIZE = 16384; // stream reassembly buffer
static constexpr int MAX_CONNECTIONS = 65536;    // flat fd-indexed table size

struct TxChunk;

struct Connection {
    int fd;
    uint8_t input_buf[CONN_BUF_SIZE]; // staged stream bytes across recv boundaries
    uint32_t input_len;               // valid bytes in input_buf
    bool closing;                     // stop accepting new app work on this conn
    bool close_submitted;             // close SQE already submitted to kernel
    bool close_in_retry_queue;        // queued for retry after -ENOSPC close
    bool send_inflight;               // exactly one async send in progress
    TxChunk* tx_head;                 // head of FIFO response queue
    TxChunk* tx_tail;                 // tail of FIFO response queue
    uint32_t tx_head_sent;            // bytes already sent from tx_head
    uint32_t tx_bytes_queued;         // total queued bytes (backpressure meter)
};

// Test hook: fail the next N connection_create() calls when > 0.
inline int g_connection_create_fail_count = 0;

inline Connection* connection_create(int fd) {
    ASSERT(fd >= 0 && fd < MAX_CONNECTIONS, "connection fd index out of range");
    if (g_connection_create_fail_count > 0) {
        g_connection_create_fail_count--;
        return nullptr;
    }
    auto* conn = static_cast<Connection*>(std::calloc(1, sizeof(Connection)));
    if (conn)
        conn->fd = fd;
    return conn;
}

inline void connection_destroy(Connection* conn) {
    ASSERT(conn != nullptr, "connection_destroy requires conn");
    if (!conn)
        return;
    std::free(conn);
}
