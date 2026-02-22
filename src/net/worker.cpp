// worker.cpp — Per-core worker event loop.

#include "worker.h"
#include "connection.h"
#include "redis/command.h"
#include "redis/resp.h"
#include "redis/store.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

static constexpr int MAX_COMPLETIONS = 256;
static constexpr int MAX_PENDING_CLOSE = 256;
static constexpr uint32_t RESPONSE_BUF_SIZE = 65536;
static constexpr uint32_t TX_HIGH_WATERMARK_BYTES = 1u << 20; // 1 MiB per connection
static constexpr uint32_t TX_POOL_GROW_BATCH = 64;

//  TX architecture
// --------------
// We must never hand io_uring a pointer to stack/transient memory, because
// SEND completion is asynchronous and the kernel can read the buffer later.
//
// Each connection therefore owns a FIFO queue of immutable TxChunk nodes.
// Chunks are allocated from a worker-local slab pool (single-threaded by
// design: one pool per worker event loop) and released on SEND completion.
//
// Why a slab pool here:
//   - avoids per-response malloc/free churn on hot path
//   - keeps similarly sized chunks cache-friendly
//   - makes allocation latency more predictable under pipeline load
//   - avoids cross-thread synchronization (no shared allocator state)
//
// Key invariants:
//   1) At most one SEND is in flight per connection (send_inflight=true).
//   2) Queue order equals wire order.
//   3) tx_head_sent tracks partial progress in the head chunk.
//   4) tx_bytes_queued enforces per-connection memory backpressure.
struct TxChunk {
    TxChunk *next;       // per-connection TX FIFO link
    TxChunk *alloc_next; // slab-owned allocation list link (for pool teardown)
    uint32_t len;        // bytes currently valid in payload area
    uint32_t cap;        // payload capacity for this chunk
    uint8_t class_idx;   // slab class index, or TX_CLASS_LARGE
};

static constexpr uint8_t TX_CLASS_LARGE = 0xFF;
static constexpr uint32_t TX_CLASS_COUNT = 4;
//  Slab classes are intentionally geometric-ish (x4/x4/x4) for low metadata
// complexity and good reuse across Redis-like response distributions:
//   256    -> tiny replies (+OK, +PONG, short errors)
//   1024   -> small bulk replies
//   4096   -> MSS-scale / medium payload
//   16384  -> larger but still common payloads
//
// Requests above 16 KiB use TX_CLASS_LARGE exact-size allocations to avoid
// excessive internal fragmentation in fixed classes.
static constexpr uint32_t TX_CLASS_CAPS[TX_CLASS_COUNT] = {256, 1024, 4096, 16384};

struct TxSlabPool {
        //  free_lists[i]:
    //   LIFO stack of currently unused chunks for class i.
    //   LIFO is intentional: recently used chunks are likely still hot in CPU
    //   cache and can be reused quickly in the next enqueue burst.
    TxChunk *free_lists[TX_CLASS_COUNT];
        //  alloc_lists[i]:
    //   Ownership list of every chunk ever allocated for class i.
    //   This is only for deterministic pool teardown; chunks move in/out of
    //   free_lists during runtime but remain reachable from alloc_lists.
    TxChunk *alloc_lists[TX_CLASS_COUNT];
};

//  Tracks deferred work when the SQ is full (-ENOSPC). Retried at the top of
// each event loop iteration before processing new completions.
struct WorkerState {
    bool accept_needs_rearm;
    int pending_close_fds[MAX_PENDING_CLOSE];
    int pending_close_count;
};

static inline uint8_t *tx_chunk_data(TxChunk *chunk) {
    return reinterpret_cast<uint8_t *>(chunk + 1);
}

//  Map requested payload length to smallest fitting slab class.
// Returns -1 when request exceeds all fixed classes and must use large path.
//
// First-fit is sufficient because class count is tiny and fixed (4), so this
// remains branch-light and predictable.
static int tx_select_class(uint32_t len) {
    for (uint32_t i = 0; i < TX_CLASS_COUNT; i++) {
        if (len <= TX_CLASS_CAPS[i]) return static_cast<int>(i);
    }
    return -1;
}

//  Refill one slab class in batches.
//
// Runtime behavior:
//   - allocate up to TX_POOL_GROW_BATCH chunks of identical capacity
//   - link each chunk into both:
//       alloc_lists[class] (permanent ownership)
//       free_lists[class]  (immediately reusable)
//   - return true if at least one allocation succeeded
//
// Partial success is valid: if allocator pressure appears mid-batch, we keep
// what was obtained and continue running instead of failing hard.
static bool tx_grow_class(TxSlabPool *pool, uint32_t class_idx) {
    uint32_t cap = TX_CLASS_CAPS[class_idx];
    uint32_t allocated = 0;
    for (uint32_t i = 0; i < TX_POOL_GROW_BATCH; i++) {
        auto *chunk = static_cast<TxChunk *>(std::malloc(sizeof(TxChunk) + cap));
        if (!chunk) break;
        chunk->class_idx = static_cast<uint8_t>(class_idx);
        chunk->cap = cap;
        chunk->len = 0;
        chunk->alloc_next = pool->alloc_lists[class_idx];
        pool->alloc_lists[class_idx] = chunk;
        chunk->next = pool->free_lists[class_idx];
        pool->free_lists[class_idx] = chunk;
        allocated++;
    }
    return allocated > 0;
}

//  Allocate one TX chunk.
//
// Fast path:
//   1) choose class for len
//   2) pop one chunk from class free list
//
// Slow path:
//   1) grow class in batch if free list empty
//   2) retry pop
//
// Large path:
//   - if len > max class cap, allocate exact-size chunk and mark as LARGE
//   - LARGE chunks are not tracked in alloc_lists and are freed immediately on
//     release (they are rare and intentionally bypass slab residency).
static TxChunk *tx_alloc(TxSlabPool *pool, uint32_t len) {
    int class_idx = tx_select_class(len);
    if (class_idx < 0) {
        auto *chunk = static_cast<TxChunk *>(std::malloc(sizeof(TxChunk) + len));
        if (!chunk) return nullptr;
        chunk->next = nullptr;
        chunk->alloc_next = nullptr;
        chunk->len = 0;
        chunk->cap = len;
        chunk->class_idx = TX_CLASS_LARGE;
        return chunk;
    }

    if (!pool->free_lists[class_idx] && !tx_grow_class(pool, static_cast<uint32_t>(class_idx)))
        return nullptr;

    TxChunk *chunk = pool->free_lists[class_idx];
    pool->free_lists[class_idx] = chunk->next;
    chunk->next = nullptr;
    chunk->len = 0;
    return chunk;
}

//  Return a chunk to allocator.
//
// Classed chunks:
//   recycled into free_lists for future reuse.
//
// LARGE chunks:
//   freed directly to keep slab footprint bounded and avoid pinning uncommon
//   huge allocations in resident pool memory.
static void tx_release_chunk(TxSlabPool *pool, TxChunk *chunk) {
    if (!chunk) return;
    if (chunk->class_idx == TX_CLASS_LARGE) {
        std::free(chunk);
        return;
    }
    uint32_t class_idx = chunk->class_idx;
    chunk->next = pool->free_lists[class_idx];
    pool->free_lists[class_idx] = chunk;
}

//  Drop and release all queued TX state for a connection. Used on final close
// and worker teardown.
static void tx_drop_queue(Connection *conn, TxSlabPool *pool) {
    TxChunk *chunk = conn->tx_head;
    while (chunk) {
        TxChunk *next = chunk->next;
        tx_release_chunk(pool, chunk);
        chunk = next;
    }
    conn->tx_head = nullptr;
    conn->tx_tail = nullptr;
    conn->tx_head_sent = 0;
    conn->tx_bytes_queued = 0;
    conn->send_inflight = false;
}

//  Drop unsent queued chunks but keep the in-flight head alive until its SEND
// completion arrives.
//
// This is critical when entering closing state while a SEND is already
// submitted: the kernel may still read from tx_head, so freeing it early would
// create a use-after-free.
static void tx_drop_after_head(Connection *conn, TxSlabPool *pool) {
    if (!conn->tx_head) {
        conn->tx_tail = nullptr;
        conn->tx_head_sent = 0;
        conn->tx_bytes_queued = 0;
        conn->send_inflight = false;
        return;
    }

    TxChunk *chunk = conn->tx_head->next;
    while (chunk) {
        TxChunk *next = chunk->next;
        tx_release_chunk(pool, chunk);
        chunk = next;
    }

    conn->tx_head->next = nullptr;
    conn->tx_tail = conn->tx_head;
    conn->tx_bytes_queued = conn->tx_head->len - conn->tx_head_sent;
}

//  Free all slab-managed allocations for this worker.
//
// We walk alloc_lists (ownership graph), not free_lists (state graph), so every
// classed chunk is freed exactly once regardless of whether it is currently
// queued, in free list, or recently popped.
static void tx_pool_destroy(TxSlabPool *pool) {
    for (uint32_t i = 0; i < TX_CLASS_COUNT; i++) {
        TxChunk *chunk = pool->alloc_lists[i];
        while (chunk) {
            TxChunk *next = chunk->alloc_next;
            std::free(chunk);
            chunk = next;
        }
        pool->alloc_lists[i] = nullptr;
        pool->free_lists[i] = nullptr;
    }
}

//  Queue one immutable payload for async send.
//
// Backpressure policy: if queued bytes would exceed TX_HIGH_WATERMARK_BYTES,
// reject enqueue so caller can close the connection rather than allowing
// unbounded memory growth.
static bool tx_enqueue(Connection *conn, TxSlabPool *pool,
                       const uint8_t *data, uint32_t len) {
    if (len == 0) return true;
    if (conn->tx_bytes_queued >= TX_HIGH_WATERMARK_BYTES) return false;
    if (len > TX_HIGH_WATERMARK_BYTES - conn->tx_bytes_queued) return false;

    TxChunk *chunk = tx_alloc(pool, len);
    if (!chunk) return false;

    chunk->len = len;
    chunk->next = nullptr;
    std::memcpy(tx_chunk_data(chunk), data, len);

    if (conn->tx_tail) conn->tx_tail->next = chunk;
    else conn->tx_head = chunk;
    conn->tx_tail = chunk;
    conn->tx_bytes_queued += len;
    return true;
}

//  Submit the current head chunk (or remaining tail of a partial head send).
// One in-flight send per connection keeps ordering straightforward and avoids
// multi-SQE ownership complexity.
static int submit_tx_head(Connection *conn, IoOps *ops, IoBackend *backend) {
    if (conn->closing || conn->send_inflight || !conn->tx_head) return 0;

    TxChunk *head = conn->tx_head;
    uint32_t remaining = head->len - conn->tx_head_sent;
    if (remaining == 0) return -EINVAL;

    int ret = ops->submit_send(backend, conn->fd,
                               tx_chunk_data(head) + conn->tx_head_sent,
                               remaining);
    if (ret == 0) conn->send_inflight = true;
    return ret;
}

// Destroy connection state without touching the kernel fd.
static void destroy_connection(int fd, Connection **conns, TxSlabPool *pool) {
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;
    tx_drop_queue(conns[fd], pool);
    connection_destroy(conns[fd]);
    conns[fd] = nullptr;
}

//  pthread_setaffinity_np(3) — pins this thread to a specific CPU core.
// Keeps the worker's cache lines hot and avoids cross-core migration, which
// is critical for the thread-per-core model (each core has its own io_uring
// ring, listen socket, and connection state — no sharing).
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

//  Each worker creates its own listen socket with SO_REUSEPORT — the kernel
// distributes incoming connections across all workers via 4-tuple hash.
static int create_listen_socket(uint16_t port) {
        //  socket(2) — AF_INET6 for IPv6 (with dual-stack for IPv4, see below).
    // SOCK_STREAM: TCP. SOCK_NONBLOCK: required for io_uring multishot accept
    // which needs a non-blocking listen fd.
    int fd = socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) { std::perror("socket"); return -1; }

    int one = 1, zero = 0;
        //  SO_REUSEADDR (socket(7)): allows bind() even if the address was recently
    // used by a socket in TIME_WAIT state. Useful for fast server restarts.
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        std::perror("setsockopt(SO_REUSEADDR)");
        close(fd);
        return -1;
    }
        //  SO_REUSEPORT (socket(7)): allows multiple sockets to bind to the same
    // port. The kernel load-balances incoming connections across all sockets
    // bound to the port using a consistent 4-tuple hash. Each worker thread
    // gets its own listen socket — no shared accept queue, no lock contention.
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) {
        std::perror("setsockopt(SO_REUSEPORT)");
        close(fd);
        return -1;
    }
        //  IPV6_V6ONLY=0 (ipv6(7)): enables dual-stack — this IPv6 socket also
    // accepts IPv4 connections (mapped to ::ffff:x.x.x.x addresses).
    if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero)) < 0) {
        std::perror("setsockopt(IPV6_V6ONLY)");
        close(fd);
        return -1;
    }

    struct sockaddr_in6 addr = {};
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(port);
    addr.sin6_addr = in6addr_any;

        //  bind(2) + listen(2) — backlog of 4096 pending connections. This is the
    // kernel's queue for connections that completed the TCP handshake but
    // haven't been accepted yet. Under heavy load, the kernel drops SYNs
    // when this queue is full (see tcp(7) tcp_max_syn_backlog).
    if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::perror("bind"); close(fd); return -1;
    }
    if (listen(fd, 4096) < 0) {
        std::perror("listen"); close(fd); return -1;
    }
    return fd;
}

static void submit_close_or_defer(int fd, Connection **conns, IoOps *ops,
                                  IoBackend *backend, WorkerState *state,
                                  TxSlabPool *pool) {
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;
    Connection *conn = conns[fd];

    if (conn->close_submitted || conn->close_in_retry_queue || conn->send_inflight)
        return;

        //  Only -ENOSPC is treated as a transient backpressure signal from SQ full.
    // Other errors are considered terminal for this connection state.
    int ret = ops->submit_close(backend, fd);
    if (ret == 0) {
        conn->close_submitted = true;
        return;
    }

    if (ret == -ENOSPC) {
        if (!conn->close_in_retry_queue && state->pending_close_count < MAX_PENDING_CLOSE) {
            state->pending_close_fds[state->pending_close_count++] = fd;
            conn->close_in_retry_queue = true;
        }
        return;
    }

    std::fprintf(stderr, "worker: submit_close(%d) failed: %s\n", fd, std::strerror(-ret));
    destroy_connection(fd, conns, pool);
}

//  TX / close state machine (per connection)
// -----------------------------------------
// This is the control-flow contract between handle_recv(), handle_send(),
// try_close(), and close submission/retry logic.
//
// States:
//   OPEN_IDLE:
//     closing=false, send_inflight=false, tx_head==nullptr
//   OPEN_QUEUED:
//     closing=false, tx_head!=nullptr
//   OPEN_SENDING:
//     closing=false, send_inflight=true, tx_head!=nullptr
//   CLOSING_DRAIN:
//     closing=true, send_inflight=true
//     (only tx_head is retained; tail is dropped)
//   CLOSING_WAIT_CLOSE_CQE:
//     closing=true, send_inflight=false, close_submitted=true
//   CLOSED:
//     conns[fd]==nullptr
//
// Transitions:
//   OPEN_IDLE --(enqueue response)--> OPEN_QUEUED
//   OPEN_QUEUED --(submit_tx_head)--> OPEN_SENDING
//   OPEN_SENDING --(SEND CQE short)--> OPEN_SENDING
//     tx_head_sent += cqe->res; resubmit remaining bytes of same chunk.
//   OPEN_SENDING --(SEND CQE full head; queue non-empty)--> OPEN_SENDING
//     pop head, submit next head.
//   OPEN_SENDING --(SEND CQE full head; queue empty)--> OPEN_IDLE
//   OPEN_* --(try_close)--> CLOSING_DRAIN or CLOSING_WAIT_CLOSE_CQE
//     if send_inflight: drop queued tail, wait for SEND CQE before close SQE.
//     else: drop queue and submit/defer close immediately.
//   CLOSING_DRAIN --(SEND CQE)--> CLOSING_WAIT_CLOSE_CQE
//     drop retained head, submit/defer close.
//   CLOSING_WAIT_CLOSE_CQE --(CLOSE CQE)--> CLOSED
//
// Safety goals guaranteed by this machine:
//   1) No kernel SEND references freed memory.
//   2) TCP write order follows queue order.
//   3) Short sends are retried from exact byte offset.
//   4) Close retries happen only on transient SQ saturation (-ENOSPC).
//  Attempt to close a connection. If a SEND is in flight, defer the close until
// that SEND completion arrives so no user buffer is freed early.
//
// Closing sequence:
//   1) mark closing=true to block new TX submissions
//   2) if send in flight, keep head chunk only and wait for SEND CQE
//   3) otherwise drop TX queue and submit close immediately/deferred
static void try_close(int fd, Connection **conns, IoOps *ops,
                      IoBackend *backend, WorkerState *state, TxSlabPool *pool) {
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;
    Connection *conn = conns[fd];

    conn->closing = true;

    if (conn->send_inflight) {
        tx_drop_after_head(conn, pool);
        if (conn->send_inflight) return;
    }

    tx_drop_queue(conn, pool);
    submit_close_or_defer(fd, conns, ops, backend, state, pool);
}

//  Per-iteration buffer recycle batch to reduce io_uring buf-ring tail updates.
// We gather buf_ids during completion processing and return them in one call.
struct RecycleBatch {
    uint16_t ids[MAX_COMPLETIONS];
    uint32_t count;
};

static void recycle_add(RecycleBatch *batch, uint16_t buf_id) {
    if (batch->count < MAX_COMPLETIONS)
        batch->ids[batch->count++] = buf_id;
}

static void recycle_flush(RecycleBatch *batch, IoOps *ops, IoBackend *backend) {
    if (batch->count == 0) return;

    if (ops->recycle_buffers) {
        ops->recycle_buffers(backend, batch->ids, batch->count);
    } else {
        for (uint32_t i = 0; i < batch->count; i++)
            ops->recycle_buffer(backend, batch->ids[i]);
    }
    batch->count = 0;
}

static void handle_accept(const IoCompletion *comp, Connection **conns,
                          IoOps *ops, IoBackend *backend, int listen_fd,
                          WorkerState *state, TxSlabPool *pool) {
    if (!comp->more) {
        if (ops->submit_accept(backend, listen_fd) < 0)
            state->accept_needs_rearm = true;
    }

    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS) {
        std::fprintf(stderr, "accept: fd %d out of range\n", fd);
        return;
    }

    // A stale entry here indicates a logic bug in close lifecycle handling.
    if (conns[fd]) {
        std::fprintf(stderr, "accept: replacing stale connection at fd %d\n", fd);
        destroy_connection(fd, conns, pool);
    }

    Connection *conn = connection_create(fd);
    if (!conn) {
        std::fprintf(stderr, "accept: alloc failed for fd %d\n", fd);
        return;
    }

    conns[fd] = conn;
    (void)ops->submit_nodelay(backend, fd); // non-critical
    if (ops->submit_recv(backend, fd) < 0)
        try_close(fd, conns, ops, backend, state, pool);
}

static void handle_recv(const IoCompletion *comp, Connection **conns,
                        Store *store, IoOps *ops, IoBackend *backend,
                        WorkerState *state, RecycleBatch *recycle,
                        TxSlabPool *pool) {
    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) {
        if (comp->buf) recycle_add(recycle, comp->buf_id);
        return;
    }

    Connection *conn = conns[fd];

        //  Buffer exhaustion: kernel terminated multishot recv with -ENOBUFS.
    // No data to parse — just rearm recv so it resumes after buffers recycle.
    if (!comp->buf && comp->buf_len == 0) {
        if (!comp->more && !conn->closing) {
            if (ops->submit_recv(backend, fd) < 0)
                try_close(fd, conns, ops, backend, state, pool);
        }
        return;
    }

    const uint8_t *parse_buf = nullptr; // points to comp->buf or conn->read_buf
    uint32_t parse_len = 0;
    bool parsing_from_conn_buf = false;
    uint32_t offset = 0;
    uint8_t response_tmp[RESPONSE_BUF_SIZE];   // one command response at a time
    uint8_t response_batch[RESPONSE_BUF_SIZE]; // coalesced pipeline send
    uint32_t response_batch_len = 0;

        //  Reassembly path: append new bytes directly into conn->read_buf so parser
    // sees one contiguous stream. If it would overflow the bounded buffer,
    // close the connection (no silent truncation).
    if (conn->read_len > 0) {
        if (conn->read_len + comp->buf_len > CONN_BUF_SIZE) {
            try_close(fd, conns, ops, backend, state, pool);
            goto recycle;
        }
        std::memcpy(conn->read_buf + conn->read_len, comp->buf, comp->buf_len);
        parse_buf = conn->read_buf;
        parse_len = conn->read_len + comp->buf_len;
        conn->read_len = 0;
        parsing_from_conn_buf = true;
    } else {
        parse_buf = comp->buf;
        parse_len = comp->buf_len;
    }

    while (offset < parse_len) {
        RespCommand cmd;
        uint32_t consumed = 0;
        RespParseResult result = resp_parse(parse_buf + offset, parse_len - offset, &cmd, &consumed);

        if (result == RespParseResult::OK) {
            uint32_t resp_len = command_execute(&cmd, store, response_tmp, RESPONSE_BUF_SIZE);
            if (resp_len > RESPONSE_BUF_SIZE) {
                try_close(fd, conns, ops, backend, state, pool);
                goto recycle;
            }

                        //  Keep one coalesced response batch per recv completion to reduce
            // SQE count under pipelines. Flush into TX queue when batch fills.
            if (response_batch_len > 0 &&
                resp_len > RESPONSE_BUF_SIZE - response_batch_len) {
                if (!tx_enqueue(conn, pool, response_batch, response_batch_len)) {
                    try_close(fd, conns, ops, backend, state, pool);
                    goto recycle;
                }
                response_batch_len = 0;
            }

            if (resp_len > 0) {
                std::memcpy(response_batch + response_batch_len, response_tmp, resp_len);
                response_batch_len += resp_len;
            }

            offset += consumed;
            continue;
        }

        if (result == RespParseResult::INCOMPLETE) {
            uint32_t remaining = parse_len - offset;
            if (remaining > CONN_BUF_SIZE) {
                try_close(fd, conns, ops, backend, state, pool);
                goto recycle;
            }

            // Preserve trailing partial command bytes for next recv.
            if (remaining > 0) {
                if (parsing_from_conn_buf) {
                    if (offset > 0)
                        std::memmove(conn->read_buf, conn->read_buf + offset, remaining);
                } else {
                    std::memcpy(conn->read_buf, parse_buf + offset, remaining);
                }
            }
            conn->read_len = remaining;
            break;
        }

        try_close(fd, conns, ops, backend, state, pool);
        goto recycle;
    }

    // Enqueue any remaining coalesced response bytes.
    if (response_batch_len > 0) {
        if (!tx_enqueue(conn, pool, response_batch, response_batch_len)) {
            try_close(fd, conns, ops, backend, state, pool);
            goto recycle;
        }
    }

    // Kick TX for this connection if idle.
    if (!conn->closing && !conn->send_inflight && conn->tx_head) {
        if (submit_tx_head(conn, ops, backend) < 0)
            try_close(fd, conns, ops, backend, state, pool);
    }

    if (!conn->closing && !comp->more) {
        if (ops->submit_recv(backend, fd) < 0)
            try_close(fd, conns, ops, backend, state, pool);
    }

recycle:
    if (comp->buf) recycle_add(recycle, comp->buf_id);
}

static void handle_send(const IoCompletion *comp, Connection **conns,
                        IoOps *ops, IoBackend *backend, WorkerState *state,
                        TxSlabPool *pool) {
    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;
    Connection *conn = conns[fd];

    // Guard against stale/late completions after connection state changes.
    if (!conn->send_inflight || !conn->tx_head) {
        if (comp->result < 0 && !conn->closing)
            try_close(fd, conns, ops, backend, state, pool);
        return;
    }

    TxChunk *head = conn->tx_head;
    uint32_t remaining = head->len - conn->tx_head_sent;

        //  SEND completion must be in (0, remaining]. Zero or overrun is treated as
    // a protocol/transport error for this connection state machine.
    if (comp->result <= 0 || static_cast<uint32_t>(comp->result) > remaining) {
        conn->send_inflight = false;
        if (!conn->closing) {
            try_close(fd, conns, ops, backend, state, pool);
        } else {
            tx_drop_queue(conn, pool);
            submit_close_or_defer(fd, conns, ops, backend, state, pool);
        }
        return;
    }

    uint32_t sent = static_cast<uint32_t>(comp->result);
    conn->tx_head_sent += sent;
    conn->tx_bytes_queued -= sent;
    conn->send_inflight = false;

        //  Full head drained: pop and recycle. Otherwise keep head and resubmit
    // remaining bytes from updated tx_head_sent.
    if (conn->tx_head_sent == head->len) {
        conn->tx_head = head->next;
        if (!conn->tx_head) conn->tx_tail = nullptr;
        conn->tx_head_sent = 0;
        tx_release_chunk(pool, head);
    }

    if (conn->closing) {
        tx_drop_queue(conn, pool);
        submit_close_or_defer(fd, conns, ops, backend, state, pool);
        return;
    }

    // Partial sends naturally continue from tx_head_sent.
    if (conn->tx_head) {
        if (submit_tx_head(conn, ops, backend) < 0)
            try_close(fd, conns, ops, backend, state, pool);
    }
}

static void handle_close(const IoCompletion *comp, Connection **conns,
                         IoOps *ops, IoBackend *backend, WorkerState *state,
                         TxSlabPool *pool) {
    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;

    Connection *conn = conns[fd];
    conn->close_submitted = false;
    conn->close_in_retry_queue = false;

    if (!conn->closing) {
        try_close(fd, conns, ops, backend, state, pool);
        return;
    }

    destroy_connection(fd, conns, pool);
}

static void handle_error(const IoCompletion *comp, Connection **conns,
                         IoOps *ops, IoBackend *backend, WorkerState *state,
                         TxSlabPool *pool) {
    int fd = comp->fd;
    if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) return;
    if (!conns[fd]->closing)
        try_close(fd, conns, ops, backend, state, pool);
}

int worker_run(WorkerConfig *config) {
    int listen_fd = -1;

    if (config->skip_setup) {
        listen_fd = config->listen_fd;
    } else {
        int pin_ret = pin_to_core(config->cpu_id);
        if (pin_ret < 0) {
            if (config->backend) config->ops.destroy(config->backend);
            return pin_ret;
        }
        std::fprintf(stderr, "worker %d: starting on core %d, port %d\n",
                     config->cpu_id, config->cpu_id, config->port);
        listen_fd = create_listen_socket(config->port);
        if (listen_fd < 0) {
            if (config->backend) config->ops.destroy(config->backend);
            return -1;
        }
    }

    int ret = config->ops.init(config->backend);
    if (ret < 0) {
        std::fprintf(stderr, "worker %d: backend init failed: %s\n",
                     config->cpu_id, std::strerror(-ret));
        if (!config->skip_setup && listen_fd >= 0) close(listen_fd);
        if (config->backend) config->ops.destroy(config->backend);
        return ret;
    }

    Connection *conns[MAX_CONNECTIONS] = {};
    Store store = {};
    IoCompletion completions[MAX_COMPLETIONS];
    WorkerState wstate = {};
    TxSlabPool tx_pool = {};

    if (config->ops.submit_accept(config->backend, listen_fd) < 0)
        wstate.accept_needs_rearm = true;

    while (config->running->load(std::memory_order_relaxed)) {
        // Drain deferred retries from the previous iteration.
        if (wstate.accept_needs_rearm) {
            if (config->ops.submit_accept(config->backend, listen_fd) == 0)
                wstate.accept_needs_rearm = false;
        }

        // Retry close submissions that previously failed with -ENOSPC.
        if (wstate.pending_close_count > 0) {
            int remaining = 0;
            for (int i = 0; i < wstate.pending_close_count; i++) {
                int fd = wstate.pending_close_fds[i];
                if (fd < 0 || fd >= MAX_CONNECTIONS || !conns[fd]) continue;

                Connection *conn = conns[fd];
                conn->close_in_retry_queue = false;

                if (!conn->closing || conn->close_submitted || conn->send_inflight)
                    continue;

                int close_ret = config->ops.submit_close(config->backend, fd);
                if (close_ret == 0) {
                    conn->close_submitted = true;
                } else if (close_ret == -ENOSPC) {
                    wstate.pending_close_fds[remaining++] = fd;
                    conn->close_in_retry_queue = true;
                } else {
                    std::fprintf(stderr, "worker: retry submit_close(%d) failed: %s\n",
                                 fd, std::strerror(-close_ret));
                    destroy_connection(fd, conns, &tx_pool);
                }
            }
            wstate.pending_close_count = remaining;
        }

        int n = config->ops.wait(config->backend, completions, MAX_COMPLETIONS);
        if (n < 0) {
            if (n == -EINTR) continue;
            std::fprintf(stderr, "worker %d: wait failed: %s\n",
                         config->cpu_id, std::strerror(-n));
            break;
        }

        RecycleBatch recycle = {};

        for (int i = 0; i < n; i++) {
            IoCompletion *c = &completions[i];
            switch (c->kind) {
            case IoCompletion::ACCEPT:
                handle_accept(c, conns, &config->ops, config->backend,
                              listen_fd, &wstate, &tx_pool);
                break;
            case IoCompletion::RECV:
                handle_recv(c, conns, &store, &config->ops, config->backend,
                            &wstate, &recycle, &tx_pool);
                break;
            case IoCompletion::SEND:
                handle_send(c, conns, &config->ops, config->backend,
                            &wstate, &tx_pool);
                break;
            case IoCompletion::CLOSE:
                handle_close(c, conns, &config->ops, config->backend,
                             &wstate, &tx_pool);
                break;
            case IoCompletion::ERROR:
                handle_error(c, conns, &config->ops, config->backend,
                             &wstate, &tx_pool);
                break;
            case IoCompletion::IGNORE:
                break;
            }
        }

        recycle_flush(&recycle, &config->ops, config->backend);
    }

    if (!config->skip_setup && listen_fd >= 0) close(listen_fd);
    if (config->backend) config->ops.destroy(config->backend);

    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (conns[i]) {
            tx_drop_queue(conns[i], &tx_pool);
            connection_destroy(conns[i]);
            conns[i] = nullptr;
        }
    }
    tx_pool_destroy(&tx_pool);

    if (!config->skip_setup)
        std::fprintf(stderr, "worker %d: stopped\n", config->cpu_id);
    return 0;
}
