/* connection.h — Per-connection state for TCP clients. */

#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>

static constexpr uint32_t CONN_BUF_SIZE = 16384;   /* reassembly buffer for partial RESP */
static constexpr int MAX_CONNECTIONS = 65536;       /* flat fd-indexed table size */

struct Connection {
    int fd;
    uint8_t read_buf[CONN_BUF_SIZE]; /* partial RESP reassembly buffer */
    uint32_t read_len;
    bool closing;
};

inline Connection *connection_create(int fd) {
    auto *conn = static_cast<Connection *>(std::calloc(1, sizeof(Connection)));
    if (conn) conn->fd = fd;
    return conn;
}

inline void connection_destroy(Connection *conn) {
    std::free(conn);
}
