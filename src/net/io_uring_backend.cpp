// io_uring_backend.cpp — Production I/O backend using Linux io_uring.
//
// Design overview
// ---------------
// Each worker thread owns one io_uring instance configured for single-issuer,
// deferred task-run mode (see uring_init for flag details). All I/O operations
// (accept, recv, send, close) are submitted as SQEs and completed
// asynchronously via CQEs — no blocking syscalls on the hot path.
//
// Multishot operations
//   Both accept and recv use multishot SQEs: a single submission generates
//   CQEs continuously (one per accepted connection / one per received chunk)
//   without needing to be resubmitted. This reduces SQE traffic to O(1) per
//   connection lifetime for accept and recv.
//
//   IORING_CQE_F_MORE signals that the multishot SQE is still armed — the
//   worker does nothing. When this flag is absent, the kernel terminated the
//   SQE (buffer exhaustion, error, etc.) and the worker must resubmit.
//
// Provided buffer rings (kernel-managed buffer pool)
//   Instead of supplying a user-space buffer with each recv SQE, we register
//   a ring of pre-allocated buffers with the kernel at init time via
//   io_uring_register_buf_ring(3). When a multishot recv completes, the kernel
//   automatically picks a free buffer from the ring, fills it, and reports the
//   buffer ID in the CQE flags. After the worker processes the data, it
//   recycles the buffer back into the ring. This eliminates per-recv buffer
//   management and keeps the hot path allocation-free.
//
//   Buffer lifecycle:
//     1. uring_init: allocate contiguous pool, register all buffers in ring
//     2. kernel: picks buffer from ring on recv completion
//     3. worker: reads data, then calls recycle_buffer to return it
//     4. goto 2
//
// Registered fixed files
//   io_uring_register_files_sparse(3) pre-registers MAX_CONNECTIONS empty
//   file slots in the kernel. accept_direct fills slots automatically on each
//   new connection; recv, send, and close use IOSQE_FIXED_FILE to reference
//   these slots directly. This eliminates the atomic fget/fput the kernel
//   otherwise performs for every I/O operation (~40ns per op).
//
//   close_direct (io_uring_prep_close_direct) releases both the slot and the
//   underlying socket — since accept_direct never creates an OS-level fd, this
//   is the only valid close path.
//
// Ring fd registration
//   io_uring_register_ring_fd(3) registers the ring's own file descriptor with
//   the kernel so subsequent io_uring_enter(2) calls resolve the ring without
//   an fget on the ring fd itself. Saves ~50ns per io_uring_enter syscall.
//
// User data encoding
//   Each SQE carries a 64-bit user_data field. We pack the operation kind
//   (ACCEPT/RECV/SEND/CLOSE/IGNORE) in the upper 32 bits and the fd in the
//   lower 32 bits. IGNORE is used for internal completions (e.g., TCP_NODELAY
//   setsockopt) that must be consumed from the CQ but never surfaced to the
//   worker. This avoids any per-SQE heap allocation — decoding is a shift+mask.

#include "io_uring_backend.h"
#include "connection.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <liburing.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

//  TCP_NODELAY setsockopt value — must be static (file-scope lifetime) because
// io_uring reads the optval pointer asynchronously after submit_nodelay returns.
// Stack-allocated values would be dangling by then.
static const int g_tcp_nodelay_on = 1;

//  Pack op kind + fd into SQE user_data to avoid per-op heap allocations.
// Layout: [bits 63..32: kind] [bits 31..0: fd]
static inline uint64_t encode_userdata(IoCompletion::Kind kind, int fd) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(kind)) << 32) | static_cast<uint64_t>(static_cast<uint32_t>(fd));
}
static inline IoCompletion::Kind decode_kind(uint64_t ud) {
    return static_cast<IoCompletion::Kind>(static_cast<uint32_t>(ud >> 32));
}
static inline int decode_fd(uint64_t ud) {
    return static_cast<int>(static_cast<uint32_t>(ud));
}

static constexpr int BUF_GROUP_ID = 0;

struct UringBackend {
    struct io_uring ring;
    struct io_uring_buf_ring *buf_ring;
    uint8_t *buf_pool;         // contiguous: buf_count * buf_size bytes
    uint32_t ring_size;
    uint32_t buf_count;
    uint32_t buf_size;
    bool ring_initialized;
        //  IORING_FEAT_NODROP: kernel queues overflowed CQEs in a backlog instead
    // of silently dropping them. We use this to detect CQ overflow at runtime.
    bool has_nodrop;
};

//  Must be called on the worker thread — IORING_SETUP_SINGLE_ISSUER (see
// io_uring_setup(2)) requires the thread that initializes the ring to be
// the only thread that ever submits SQEs to it.
static int uring_init(IoBackend *ctx) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);

        //  io_uring_setup(2) flags — each one tightens the kernel/userspace contract
    // for better performance in our thread-per-core model:
    //
    // IORING_SETUP_COOP_TASKRUN:
    //   Disables IPI (inter-processor interrupt) delivery for CQE task work.
    //   The kernel only runs io_uring task work when we explicitly enter the
    //   kernel (e.g. via io_uring_submit_and_wait_timeout), not on arbitrary
    //   context switches. Eliminates unexpected cross-CPU interrupts.
    //
    // IORING_SETUP_DEFER_TASKRUN:
    //   Defers all CQE delivery until the owning thread explicitly asks for
    //   completions. Without this, CQEs can appear during unrelated syscalls.
    //   Combined with COOP_TASKRUN, gives us full control over when completion
    //   work runs — critical for the thread-per-core model where we want zero
    //   surprise wakeups. Requires SINGLE_ISSUER.
    //
    // IORING_SETUP_SINGLE_ISSUER:
    //   Promises the kernel that only one thread will ever submit to this ring.
    //   Enables kernel-side optimizations: the SQ path skips locking entirely.
    //   Enforced at runtime — submitting from another thread will return -EEXIST.
    //
    // IORING_SETUP_SUBMIT_ALL:
    //   Without this flag, the kernel stops processing the SQ batch on the first
    //   error. With it, all SQEs in the batch are submitted regardless of
    //   individual failures. Prevents one bad SQE from blocking subsequent ones.
    //
    // IORING_SETUP_CQSIZE:
    //   Overrides the default CQ depth (2x SQ). We set 4x so the CQ can absorb
    //   large bursts of completions (accept + recv + send storms) without
    //   overflowing. params.cq_entries is read by the kernel when this flag is set.
    struct io_uring_params params = {};
    params.flags = IORING_SETUP_COOP_TASKRUN
                 | IORING_SETUP_DEFER_TASKRUN
                 | IORING_SETUP_SINGLE_ISSUER
                 | IORING_SETUP_SUBMIT_ALL
                 | IORING_SETUP_CQSIZE;
    params.cq_entries = be->ring_size * 4;

        //  io_uring_queue_init_params(3) — creates the SQ and CQ rings in the kernel.
    // ring_size is the SQ depth. On return, params.features is filled with the
    // kernel's capability bitmask (checked below for IORING_FEAT_NODROP).
    int ret = io_uring_queue_init_params(be->ring_size, &be->ring, &params);
    if (ret < 0) {
        std::fprintf(stderr, "io_uring_queue_init_params failed: %s\n", std::strerror(-ret));
        return ret;
    }
    be->ring_initialized = true;

        //  IORING_FEAT_NODROP (io_uring_setup(2) features): when set, the kernel
    // queues CQEs that don't fit in the CQ ring into an overflow backlog instead
    // of silently dropping them. We record this so uring_wait can check the
    // overflow flag and warn if the backlog is filling.
    be->has_nodrop = (params.features & IORING_FEAT_NODROP) != 0;

        //  --- Provided buffer ring setup ---
    // See io_uring_register_buf_ring(3) and io_uring_buf_ring_add(3).
    //
    // We allocate a ring control structure (io_uring_buf_ring) plus the buffer
    // entries. The kernel requires this memory to be page-aligned because it
    // maps it directly. Each entry in the ring points to a slice of our
    // contiguous buf_pool.
    size_t ring_entries_size = be->buf_count * sizeof(struct io_uring_buf);
    size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));

        //  posix_memalign(3) — page-aligned allocation required by the kernel for
    // the buf_ring shared mapping.
    void *ring_mem = nullptr;
    ret = posix_memalign(&ring_mem, page_size,
                         sizeof(struct io_uring_buf_ring) + ring_entries_size);
    if (ret != 0) {
        std::fprintf(stderr, "posix_memalign for buf_ring failed\n");
        return -ENOMEM;
    }
    be->buf_ring = static_cast<struct io_uring_buf_ring *>(ring_mem);

        //  io_uring_register_buf_ring(3) — registers the buffer ring with the kernel.
    // ring_addr: userspace address of the io_uring_buf_ring (must be page-aligned).
    // ring_entries: number of buffers (must be power of 2).
    // bgid: buffer group ID — referenced by SQEs via IOSQE_BUFFER_SELECT.
    struct io_uring_buf_reg reg = {};
    reg.ring_addr = (unsigned long)be->buf_ring;
    reg.ring_entries = be->buf_count;
    reg.bgid = BUF_GROUP_ID;

    ret = io_uring_register_buf_ring(&be->ring, &reg, 0);
    if (ret < 0) {
        std::fprintf(stderr, "io_uring_register_buf_ring failed: %s\n", std::strerror(-ret));
        std::free(ring_mem);
        be->buf_ring = nullptr;
        return ret;
    }

        //  Initialize the ring header, then populate it with all buffers.
    // io_uring_buf_ring_add(3) — adds one buffer to the ring:
    //   addr: pointer to the buffer in our pool (buf_pool + i * buf_size)
    //   len:  buffer capacity
    //   bid:  buffer ID (returned in CQE flags on recv completion)
    //   mask: power-of-2 mask for ring index wrapping
    //   offset: position in the ring for this add (we use i for initial fill)
    //
    // io_uring_buf_ring_advance(3) — commits all added buffers to the kernel
    // in one shot by advancing the ring tail. We call it once after the loop
    // rather than per-buffer to batch the tail update.
    io_uring_buf_ring_init(be->buf_ring);
    int mask = io_uring_buf_ring_mask(be->buf_count);
    for (uint32_t i = 0; i < be->buf_count; i++) {
        io_uring_buf_ring_add(be->buf_ring,
                              be->buf_pool + (i * be->buf_size),
                              be->buf_size, static_cast<unsigned short>(i), mask, static_cast<int>(i));
    }
    io_uring_buf_ring_advance(be->buf_ring, static_cast<int>(be->buf_count));

        //  io_uring_register_files_sparse(3) — pre-registers MAX_CONNECTIONS empty
    // slots in the kernel's fixed file table for this ring. Slots are populated
    // on-demand by accept_direct (IORING_FILE_INDEX_ALLOC). Once registered, all
    // recv/send/close SQEs reference these slots via IOSQE_FIXED_FILE, which
    // eliminates the atomic fget/fput the kernel performs for each I/O operation
    // on a normal (non-registered) fd.
    ret = io_uring_register_files_sparse(&be->ring, MAX_CONNECTIONS);
    if (ret < 0) {
        std::fprintf(stderr, "io_uring_register_files_sparse failed: %s\n", std::strerror(-ret));
        return ret;
    }

        //  io_uring_register_ring_fd(3) — registers the ring's own file descriptor
    // with the kernel so subsequent io_uring_enter(2) calls resolve it without
    // an fget on the ring fd (~50ns saved per syscall). Must be last in init
    // so the registered ring state reflects the final buf_ring and file table.
    ret = io_uring_register_ring_fd(&be->ring);
    if (ret < 0) {
        std::fprintf(stderr, "io_uring_register_ring_fd failed: %s\n", std::strerror(-ret));
        return ret;
    }

    return 0;
}

//  io_uring_prep_multishot_accept_direct(3) — like multishot_accept but places
// accepted sockets directly into the registered fixed file table rather than
// installing them as OS-level fds. The kernel auto-selects the next free slot
// (IORING_FILE_INDEX_ALLOC is baked into the liburing helper). On completion:
//   cqe->res = fixed file index (not an OS fd — only usable via IOSQE_FIXED_FILE)
//   IORING_CQE_F_MORE set = SQE still armed; absent = terminated, must resubmit.
//
// Using direct accept avoids one fd_install + fget/fput cycle per connection.
static int uring_submit_accept(IoBackend *ctx, int listen_fd) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&be->ring);
    if (!sqe) return -ENOSPC;
    io_uring_prep_multishot_accept_direct(sqe, listen_fd, nullptr, nullptr, 0);
    io_uring_sqe_set_data64(sqe, encode_userdata(IoCompletion::ACCEPT, listen_fd));
    return 0;
}

//  io_uring_prep_recv_multishot(3) — like multishot accept, one SQE generates a
// CQE for each chunk of data received on this fd, without resubmission.
//
// IOSQE_FIXED_FILE: fd is a registered file index (not an OS fd). Eliminates
// the atomic fget/fput that the kernel performs on every I/O operation.
//
// IOSQE_BUFFER_SELECT: tells the kernel to auto-select a buffer from the
// provided buffer group (buf_group = BUF_GROUP_ID) rather than using a
// user-supplied buffer. The buffer and length args are nullptr/0 because the
// kernel fills them from the pool. On completion, the buffer ID is encoded in
// the upper bits of cqe->flags (see IORING_CQE_BUFFER_SHIFT in uring_wait).
//
// If the buffer pool is exhausted (all buffers consumed, none recycled), the
// kernel terminates the multishot recv with -ENOBUFS — we'd need to resubmit
// after recycling. Currently we size the pool large enough to avoid this.
static int uring_submit_recv(IoBackend *ctx, int fd) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&be->ring);
    if (!sqe) return -ENOSPC;
    io_uring_prep_recv_multishot(sqe, fd, nullptr, 0, 0);
    sqe->flags |= IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;
    sqe->buf_group = BUF_GROUP_ID;
    io_uring_sqe_set_data64(sqe, encode_userdata(IoCompletion::RECV, fd));
    return 0;
}

//  io_uring_prep_send(3) — queues an async send. Unlike accept/recv, send is
// NOT multishot: each send requires its own SQE.
//
// IOSQE_FIXED_FILE: fd is a registered file index. See uring_submit_recv.
//
// MSG_NOSIGNAL (see send(2)): prevents the kernel from raising SIGPIPE when
// writing to a connection the peer has closed. Without this, a broken pipe
// would kill the entire process. With it, we get -EPIPE in cqe->res instead.
static int uring_submit_send(IoBackend *ctx, int fd, const uint8_t *data, uint32_t len) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&be->ring);
    if (!sqe) return -ENOSPC;
    io_uring_prep_send(sqe, fd, data, len, MSG_NOSIGNAL);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data64(sqe, encode_userdata(IoCompletion::SEND, fd));
    return 0;
}

//  io_uring_prep_close_direct(3) — closes a registered fixed file by its slot
// index, releasing both the kernel table slot and the underlying socket. Since
// accept_direct never installs an OS-level fd, close_direct is the only valid
// close path — there is no OS fd to pass to regular close(2) or prep_close.
static int uring_submit_close(IoBackend *ctx, int fd) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&be->ring);
    if (!sqe) return -ENOSPC;
    io_uring_prep_close_direct(sqe, static_cast<unsigned>(fd));
    io_uring_sqe_set_data64(sqe, encode_userdata(IoCompletion::CLOSE, fd));
    return 0;
}

//  io_uring_prep_cmd_sock(3) with SOCKET_URING_OP_SETSOCKOPT — sets a socket
// option asynchronously via io_uring, avoiding a synchronous setsockopt(2) on
// the accept hot path. Available since kernel 6.7 / liburing 2.7.
//
// IOSQE_FIXED_FILE: fd is the registered file index produced by accept_direct.
//
// TCP_NODELAY (tcp(7)): disables Nagle's algorithm. Without this, the kernel
// buffers small sends until a full MSS worth of data accumulates or an ACK
// arrives, adding up to 40ms latency for sub-MSS responses. For Redis-like
// request-response with small payloads, this must be disabled.
//
// The optval pointer (g_tcp_nodelay_on) must have static lifetime — the kernel
// reads it asynchronously after this function returns. Encoding IGNORE in
// user_data ensures the resulting CQE is consumed silently in uring_wait.
static int uring_submit_nodelay(IoBackend *ctx, int fd) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);
    struct io_uring_sqe *sqe = io_uring_get_sqe(&be->ring);
    if (!sqe) return -ENOSPC;
    io_uring_prep_cmd_sock(sqe, SOCKET_URING_OP_SETSOCKOPT, fd,
                           IPPROTO_TCP, TCP_NODELAY,
                           const_cast<int *>(&g_tcp_nodelay_on), sizeof(g_tcp_nodelay_on));
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data64(sqe, encode_userdata(IoCompletion::IGNORE, 0));
    return 0;
}

//  Returns a consumed buffer back to the provided buffer ring.
//
// After a recv completion, the kernel has removed this buffer from the ring.
// The worker processes the received data and then calls this to make the buffer
// available again. io_uring_buf_ring_add(3) re-inserts it at ring position 0
// (offset param), and io_uring_buf_ring_advance(3) commits the single entry
// by advancing the ring tail by 1.
//
// The mask (power-of-2 wrapping) ensures the ring index stays in bounds.
static void uring_recycle_buffer(IoBackend *ctx, uint16_t buf_id) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);
    int mask = io_uring_buf_ring_mask(be->buf_count);
    io_uring_buf_ring_add(be->buf_ring,
                          be->buf_pool + (static_cast<uint32_t>(buf_id) * be->buf_size),
                          be->buf_size, buf_id, mask, 0);
    io_uring_buf_ring_advance(be->buf_ring, 1);
}

//  Batched buffer recycle: one tail advance for all returned buffers.
//
// The provided-buf ring is append-only from userspace perspective: each call
// to io_uring_buf_ring_add writes entries at (tail + offset). We therefore use
// offset=added (0..N-1) for this batch and then advance tail once by N.
// This reduces atomic tail updates compared to per-buffer recycle.
static void uring_recycle_buffers(IoBackend *ctx, const uint16_t *buf_ids, uint32_t count) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);
    if (!buf_ids || count == 0) return;

    int mask = io_uring_buf_ring_mask(be->buf_count);
    uint32_t added = 0;
    for (uint32_t i = 0; i < count; i++) {
        uint16_t buf_id = buf_ids[i];
        if (static_cast<uint32_t>(buf_id) >= be->buf_count) continue;
        io_uring_buf_ring_add(be->buf_ring,
                              be->buf_pool + (static_cast<uint32_t>(buf_id) * be->buf_size),
                              be->buf_size, buf_id, mask, static_cast<int>(added));
        added++;
    }
    if (added > 0)
        io_uring_buf_ring_advance(be->buf_ring, static_cast<int>(added));
}

static int uring_wait(IoBackend *ctx, IoCompletion *out, int max_completions) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);

        //  __kernel_timespec is the kernel-native timespec (always 64-bit tv_sec),
    // required by io_uring timeout interfaces. Not the same as struct timespec.
    //
    // 100ms timeout: lets the worker check the shutdown flag periodically.
    // Under load, CQEs arrive before the timeout expires — zero overhead.
    struct __kernel_timespec ts = {.tv_sec = 0, .tv_nsec = 100'000'000};
    struct io_uring_cqe *cqe;

        //  io_uring_submit_and_wait_timeout(3) — submits any pending SQEs and waits
    // for at least 1 CQE, with a timeout. This is the single blocking point in
    // the event loop.
    //
    // We use this instead of io_uring_submit_and_wait(3) because with
    // DEFER_TASKRUN, the simpler wait variant doesn't process deferred task
    // work during the wait, making it uninterruptible by signals. The _timeout
    // variant internally uses IORING_ENTER_EXT_ARG which correctly handles
    // DEFER_TASKRUN and returns -EINTR when a signal arrives — essential for
    // clean shutdown via SIGINT/SIGTERM.
    //
    // Returns:
    //   >= 0:   success, CQEs are available
    //   -ETIME: no CQEs within timeout (not an error, just idle)
    //   -EINTR: interrupted by signal (worker checks shutdown flag and retries)
    int ret = io_uring_submit_and_wait_timeout(&be->ring, &cqe, 1, &ts, nullptr);
    if (ret < 0 && ret != -ETIME) return ret;
    if (ret == -ETIME) return 0;

        //  Two counters are needed:
    //   total_seen — all CQEs consumed from the ring (used for cq_advance)
    //   out_count  — CQEs emitted to caller (excludes IGNORE completions)
    //
    // io_uring_for_each_cqe — iterates over ready CQEs without advancing the
    // CQ head (peek semantics). We batch-advance once at the end.
    int out_count = 0;
    int total_seen = 0;
    unsigned head;
    io_uring_for_each_cqe(&be->ring, head, cqe) {
        IoCompletion::Kind kind = decode_kind(cqe->user_data);

                //  IGNORE completions are internal ops (e.g., TCP_NODELAY setsockopt).
        // Always consume them from the ring; log errors but never surface to
        // the worker — the worker has no action to take for these.
        if (kind == IoCompletion::IGNORE) {
            if (cqe->res < 0)
                std::fprintf(stderr, "io_uring: internal op failed: %s\n",
                             std::strerror(-cqe->res));
            total_seen++;
            continue;
        }

                //  Output array full: stop here so cq_advance only consumes a contiguous
        // prefix. This guarantees we never skip a data CQE and then advance
        // past it because of later IGNORE completions.
        //
        // Example bug avoided:
        //   [DATA][DATA][IGNORE]
        //   if max_completions reached before 2nd DATA and we still consumed
        //   IGNORE, total_seen would advance past skipped DATA -> drop CQE.
        if (out_count >= max_completions) break;

        IoCompletion *c = &out[out_count];
        c->kind = kind;
        c->fd = decode_fd(cqe->user_data);
        c->result = cqe->res;
        c->buf = nullptr;
        c->buf_len = 0;
        c->buf_id = 0;

                //  IORING_CQE_F_MORE: multishot SQE is still armed — no resubmission
        // needed. Absence means the kernel terminated it; the worker must call
        // submit_accept or submit_recv again. Always false for non-multishot.
        c->more = (cqe->flags & IORING_CQE_F_MORE) != 0;

        if (cqe->res < 0) {
                        //  ACCEPT failures carry listen fd in user_data and must
            // stay on ACCEPT path so worker can safely rearm accept without
            // touching unrelated connection slots.
            if (kind == IoCompletion::ACCEPT) {
                c->kind = IoCompletion::ACCEPT;
                c->more = false;
                out_count++;
                total_seen++;
                continue;
            }

                        //  -ENOBUFS on RECV: provided buffer pool exhausted. Not a real
            // error — the recv should be rearmed once buffers are recycled.
            // Surface as RECV with null buf so handle_recv can rearm.
            if (kind == IoCompletion::RECV && cqe->res == -ENOBUFS) {
                c->kind = IoCompletion::RECV;
                c->buf = nullptr;
                c->buf_len = 0;
                c->more = false;
            } else {
                c->kind = IoCompletion::ERROR;
                c->more = false;
            }
            out_count++;
            total_seen++;
            continue;
        }

        if (c->kind == IoCompletion::RECV) {
                        //  cqe->res == 0 on recv means EOF — peer closed the connection
            // (same semantics as recv(2) returning 0).
            if (cqe->res == 0) {
                c->kind = IoCompletion::CLOSE;
            } else if (cqe->flags & IORING_CQE_F_BUFFER) {
                                //  IORING_CQE_F_BUFFER: kernel attached a provided buffer to
                // this completion. The buffer ID is packed in the upper 16 bits
                // of cqe->flags (shifted by IORING_CQE_BUFFER_SHIFT = 16).
                // We use this ID to compute the buffer pointer in our pool and
                // to recycle the buffer after processing.
                uint16_t bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
                if (static_cast<uint32_t>(bid) >= be->buf_count) {
                    c->kind = IoCompletion::ERROR;
                    c->result = -EOVERFLOW;
                    c->more = false;
                } else {
                    c->buf = be->buf_pool + (static_cast<uint32_t>(bid) * be->buf_size);
                    c->buf_len = static_cast<uint32_t>(cqe->res);
                    c->buf_id = bid;
                }
            } else {
                                //  With IOSQE_BUFFER_SELECT we must always receive a selected
                // provided buffer when cqe->res > 0. Missing F_BUFFER indicates
                // inconsistent completion metadata, treat as fatal for op.
                c->kind = IoCompletion::ERROR;
                c->result = -EIO;
                c->more = false;
            }
        }

        if (c->kind == IoCompletion::ACCEPT) {
                        //  With accept_direct, cqe->res is the allocated fixed file index
            // (not an OS fd). This index is used for all subsequent recv/send/
            // close SQEs via IOSQE_FIXED_FILE, and as the conns[] table key.
            c->fd = cqe->res;
        }

        out_count++;
        total_seen++;
    }

        //  io_uring_cq_advance(3) — batch-advances the CQ head by total_seen,
    // marking all consumed CQEs (including IGNORE) as seen. More efficient
    // than calling io_uring_cqe_seen per CQE — single atomic store.
    io_uring_cq_advance(&be->ring, static_cast<unsigned>(total_seen));

        //  IORING_FEAT_NODROP: overflow means the kernel backlog is filling. This
    // indicates we are not draining the CQ fast enough — increase ring_size or
    // process fewer operations per event loop iteration.
    if (be->has_nodrop && io_uring_cq_has_overflow(&be->ring))
        std::fprintf(stderr, "warning: CQ overflow — increase ring_size (current: %u)\n",
                     be->ring_size);

    return out_count;
}

//  Teardown: io_uring_queue_exit(3) unmaps the SQ/CQ rings and releases kernel
// resources. We then free the userspace allocations in order: the buf_ring
// control structure, the contiguous buffer pool, and the backend struct.
static void uring_destroy(IoBackend *ctx) {
    auto *be = reinterpret_cast<UringBackend *>(ctx);
    if (be->ring_initialized) io_uring_queue_exit(&be->ring);
    if (be->buf_ring) std::free(be->buf_ring);
    if (be->buf_pool) std::free(be->buf_pool);
    std::free(be);
}

IoOps io_uring_ops() {
    return IoOps{
        .init = uring_init,
        .submit_accept = uring_submit_accept,
        .submit_recv = uring_submit_recv,
        .submit_send = uring_submit_send,
        .submit_close = uring_submit_close,
        .submit_nodelay = uring_submit_nodelay,
        .recycle_buffer = uring_recycle_buffer,
        .recycle_buffers = uring_recycle_buffers,
        .wait = uring_wait,
        .destroy = uring_destroy,
    };
}

//  Allocates the backend struct and the contiguous buffer pool, but does NOT
// initialize the io_uring ring. Ring init is deferred to uring_init() which
// must run on the worker thread (SINGLE_ISSUER constraint — the thread that
// calls io_uring_queue_init_params becomes the sole owner of the ring).
//
// buf_pool is one contiguous allocation of buf_count * buf_size bytes. During
// uring_init, it gets sliced into buf_count individual buffers and registered
// with the kernel's provided buffer ring.
IoBackend *io_uring_backend_create(uint32_t ring_size, uint32_t buf_count, uint32_t buf_size) {
    auto *be = static_cast<UringBackend *>(std::calloc(1, sizeof(UringBackend)));
    if (!be) return nullptr;
    be->ring_size = ring_size;
    be->buf_count = buf_count;
    be->buf_size = buf_size;
    be->buf_pool = static_cast<uint8_t *>(std::calloc(buf_count, buf_size));
    if (!be->buf_pool) { std::free(be); return nullptr; }
    return reinterpret_cast<IoBackend *>(be);
}
