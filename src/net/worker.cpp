/* worker.cpp — Per-core worker event loop. */

#include "worker.h"
#include "connection.h"
#include "redis/command.h"
#include "redis/resp.h"
#include "redis/store.h"

#include <arpa/inet.h>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

static constexpr int MAX_COMPLETIONS = 256;
static constexpr uint32_t RESPONSE_BUF_SIZE = 65536;

/* pthread_setaffinity_np(3) — pins this thread to a specific CPU core.
 * Keeps the worker's cache lines hot and avoids cross-core migration, which
 * is critical for the thread-per-core model (each core has its own io_uring
 * ring, listen socket, and connection state — no sharing). */
static int pin_to_core(int cpu_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    if (ret != 0) {
        std::fprintf(stderr, "worker %d: failed to pin to core: %s\n", cpu_id, std::strerror(ret));
        return -ret;
    }
    return 0;
}

/* Each worker creates its own listen socket with SO_REUSEPORT — the kernel
 * distributes incoming connections across all workers via 4-tuple hash. */
static int create_listen_socket(uint16_t port) {
    /* socket(2) — AF_INET6 for IPv6 (with dual-stack for IPv4, see below).
     * SOCK_STREAM: TCP. SOCK_NONBLOCK: required for io_uring multishot accept
     * which needs a non-blocking listen fd. */
    int fd = socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) { std::perror("socket"); return -1; }

    int one = 1, zero = 0;
    /* SO_REUSEADDR (socket(7)): allows bind() even if the address was recently
     * used by a socket in TIME_WAIT state. Useful for fast server restarts. */
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    /* SO_REUSEPORT (socket(7)): allows multiple sockets to bind to the same
     * port. The kernel load-balances incoming connections across all sockets
     * bound to the port using a consistent 4-tuple hash. Each worker thread
     * gets its own listen socket — no shared accept queue, no lock contention. */
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
    /* IPV6_V6ONLY=0 (ipv6(7)): enables dual-stack — this IPv6 socket also
     * accepts IPv4 connections (mapped to ::ffff:x.x.x.x addresses). */
    setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero));

    struct sockaddr_in6 addr = {};
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(port);
    addr.sin6_addr = in6addr_any;

    /* bind(2) + listen(2) — backlog of 4096 pending connections. This is the
     * kernel's queue for connections that completed the TCP handshake but
     * haven't been accepted yet. Under heavy load, the kernel drops SYNs
     * when this queue is full (see tcp(7) tcp_max_syn_backlog). */
    if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::perror("bind"); close(fd); return -1;
    }
    if (listen(fd, 4096) < 0) {
        std::perror("listen"); close(fd); return -1;
    }
    return fd;
}

static void handle_accept(const IoCompletion *comp, Connection **conns,
                           IoOps *ops, IoBackend *backend, int listen_fd) {
    /* Multishot accept SQE no longer armed (kernel terminated it internally).
     * Rearm before processing so the gap in accept coverage is minimal.
     * Note: errors produce ERROR kind CQEs in uring_wait and never reach here. */
    if (!comp->more) ops->submit_accept(backend, listen_fd);

    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS) {
        std::fprintf(stderr, "accept: fd %d out of range\n", fd);
        return;
    }
    Connection *conn = connection_create(fd);
    if (!conn) {
        std::fprintf(stderr, "accept: alloc failed for fd %d\n", fd);
        ops->submit_close(backend, fd);
        return;
    }
    conns[fd] = conn;
    /* TCP_NODELAY submitted before recv. Both are async, but setsockopt is a
     * local kernel operation and will complete before the client's first data
     * arrives over the network, so NODELAY is in effect before any send. */
    ops->submit_nodelay(backend, fd);
    ops->submit_recv(backend, fd);
}

static void handle_recv(const IoCompletion *comp, Connection **conns,
                         Store *store, IoOps *ops, IoBackend *backend) {
    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) {
        if (comp->buf) ops->recycle_buffer(backend, comp->buf_id);
        return;
    }

    Connection *conn = conns[fd];
    const uint8_t *parse_buf; /* start of data to feed to the RESP parser */
    uint32_t parse_len;
    uint8_t combined[CONN_BUF_SIZE + 4096];

    /* Reassembly: a TCP recv boundary can split a RESP command anywhere. If
     * the previous recv left an incomplete command in conn->read_buf, prepend
     * it to the new data so the parser sees a contiguous byte sequence.
     *
     * combined[] is sized CONN_BUF_SIZE (max stored partial) + 4096 (headroom
     * for the continuation bytes arriving now). The +4096 is conservative —
     * only the bytes that complete the partial command are needed here; the
     * rest of the recv payload is parsed in subsequent loop iterations. */
    if (conn->read_len > 0) {
        std::memcpy(combined, conn->read_buf, conn->read_len);
        uint32_t copy_len = comp->buf_len;
        /* Cap copy_len to prevent combined[] from overflowing. If truncated,
         * the excess bytes are lost; the parser will return an error and the
         * connection will be closed cleanly on the next iteration. */
        if (conn->read_len + copy_len > sizeof(combined))
            copy_len = sizeof(combined) - conn->read_len;
        std::memcpy(combined + conn->read_len, comp->buf, copy_len);
        parse_buf = combined;
        parse_len = conn->read_len + copy_len;
        conn->read_len = 0; /* partial consumed into combined; reset for next recv */
    } else {
        /* No leftover — parse directly from the provided buffer ring slice.
         * parse_buf points into the kernel-managed buffer pool (zero-copy). */
        parse_buf = comp->buf;
        parse_len = comp->buf_len;
    }

    /* Pipeline loop: one TCP segment can carry multiple back-to-back RESP
     * commands (pipelining). We loop until all complete commands are consumed
     * or the data runs out. offset tracks how far into parse_buf we have read. */
    uint32_t offset = 0;

    /* Response buffer reused across all commands in this recv. Each call to
     * command_execute overwrites it with the next RESP reply. submit_send
     * queues an async SQE that references this pointer — the kernel reads the
     * bytes during the next io_uring_submit_and_wait_timeout call in uring_wait,
     * which happens at the top of the event loop after all completions in this
     * batch are processed. The buffer therefore remains valid long enough. */
    uint8_t response[RESPONSE_BUF_SIZE];

    while (offset < parse_len) {
        RespCommand cmd;
        uint32_t consumed = 0; /* bytes forming the parsed command (set by resp_parse on OK) */
        RespParseResult result = resp_parse(parse_buf + offset, parse_len - offset, &cmd, &consumed);

        if (result == RespParseResult::OK) {
            /* cmd.args[i].data points into parse_buf (zero-copy). command_execute
             * reads the args and writes the RESP reply into response[]. */
            uint32_t resp_len = command_execute(&cmd, store, response, sizeof(response));
            if (resp_len > 0) ops->submit_send(backend, fd, response, resp_len);
            offset += consumed; /* advance past this command and parse the next */
        } else if (result == RespParseResult::INCOMPLETE) {
            /* The recv boundary cut through a command. Save the remaining bytes
             * in conn->read_buf so they can be prepended on the next recv.
             * If remaining > CONN_BUF_SIZE the partial is too large to buffer
             * (e.g. a SET with a very large value split across many recvs that
             * individually exceed our reassembly limit) — close the connection. */
            uint32_t remaining = parse_len - offset;
            if (remaining > CONN_BUF_SIZE) {
                conn->closing = true;
                ops->submit_close(backend, fd);
                goto recycle; /* skip rearm: connection is being torn down */
            }
            std::memcpy(conn->read_buf, parse_buf + offset, remaining);
            conn->read_len = remaining;
            break; /* wait for the next recv to supply the rest */
        } else {
            /* Protocol error (wrong type byte, too many args, etc.). Close. */
            conn->closing = true;
            ops->submit_close(backend, fd);
            goto recycle; /* skip rearm: connection is being torn down */
        }
    }

    /* Multishot recv SQE no longer armed. Rearm if the connection is still live.
     * Note: errors (negative res) become ERROR kind in uring_wait before here,
     * so this path covers only the rare non-error kernel termination of the SQE. */
    if (!comp->more && !conn->closing)
        ops->submit_recv(backend, fd);

recycle:
    if (comp->buf) ops->recycle_buffer(backend, comp->buf_id);
}

static void handle_send(const IoCompletion *comp, Connection **conns,
                         IoOps *ops, IoBackend *backend) {
    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;
    if (comp->result < 0 && !conns[fd]->closing) {
        conns[fd]->closing = true;
        ops->submit_close(backend, fd);
    }
}

static void handle_close(const IoCompletion *comp, Connection **conns,
                          IoOps *ops, IoBackend *backend) {
    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;
    if (!conns[fd]->closing) {
        conns[fd]->closing = true;
        ops->submit_close(backend, fd);
        return;
    }
    connection_destroy(conns[fd]);
    conns[fd] = nullptr;
}

static void handle_error(const IoCompletion *comp, Connection **conns,
                          IoOps *ops, IoBackend *backend) {
    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;
    if (!conns[fd]->closing) {
        conns[fd]->closing = true;
        ops->submit_close(backend, fd);
    }
}

int worker_run(WorkerConfig *config) {
    pin_to_core(config->cpu_id);
    std::fprintf(stderr, "worker %d: starting on core %d, port %d\n",
                 config->cpu_id, config->cpu_id, config->port);

    int listen_fd = create_listen_socket(config->port);
    if (listen_fd < 0) return -1;

    /* Init must happen on this thread (SINGLE_ISSUER). */
    int ret = config->ops.init(config->backend);
    if (ret < 0) {
        std::fprintf(stderr, "worker %d: backend init failed: %s\n",
                     config->cpu_id, std::strerror(-ret));
        close(listen_fd);
        return ret;
    }

    config->ops.submit_accept(config->backend, listen_fd);

    Connection *conns[MAX_CONNECTIONS] = {};
    Store store = {};
    IoCompletion completions[MAX_COMPLETIONS];

    while (config->running->load(std::memory_order_relaxed)) {
        int n = config->ops.wait(config->backend, completions, MAX_COMPLETIONS);
        if (n < 0) {
            /* -EINTR: a signal (SIGINT/SIGTERM) interrupted the io_uring wait.
             * We loop back, re-check the running flag, and shut down if the
             * signal handler set it to false. This is the clean shutdown path. */
            if (n == -EINTR) continue;
            std::fprintf(stderr, "worker %d: wait failed: %s\n",
                         config->cpu_id, std::strerror(-n));
            break;
        }
        for (int i = 0; i < n; i++) {
            IoCompletion *c = &completions[i];
            switch (c->kind) {
            case IoCompletion::ACCEPT: handle_accept(c, conns, &config->ops, config->backend, listen_fd); break;
            case IoCompletion::RECV:   handle_recv(c, conns, &store, &config->ops, config->backend); break;
            case IoCompletion::SEND:   handle_send(c, conns, &config->ops, config->backend); break;
            case IoCompletion::CLOSE:  handle_close(c, conns, &config->ops, config->backend); break;
            case IoCompletion::ERROR:  handle_error(c, conns, &config->ops, config->backend); break;
            case IoCompletion::IGNORE: break; /* filtered in uring_wait; sim backend never emits these */
            }
        }
    }

    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (conns[i]) { connection_destroy(conns[i]); conns[i] = nullptr; }
    }
    close(listen_fd);
    config->ops.destroy(config->backend);
    std::fprintf(stderr, "worker %d: stopped\n", config->cpu_id);
    return 0;
}
