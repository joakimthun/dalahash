/*
 * io.h — I/O abstraction layer (the DST seam).
 *
 * C-style function-pointer table separating the worker event loop from the
 * I/O mechanism. Production uses io_uring; tests swap in a simulated backend.
 */

#pragma once

#include <cstddef>
#include <cstdint>

struct IoCompletion {
    enum Kind : uint8_t { ACCEPT, RECV, SEND, CLOSE, ERROR, IGNORE };

    Kind kind;
    int fd;
    int result;        /* bytes transferred, or negative errno */
    uint8_t *buf;      /* RECV only: pointer to received data */
    uint32_t buf_len;  /* RECV only: length of received data */
    uint16_t buf_id;   /* RECV only: buffer ID for recycling */
    /* IORING_CQE_F_MORE: for multishot ops (ACCEPT/RECV), true while the SQE
     * remains armed in the kernel. False means the multishot terminated and
     * the caller must resubmit. Always false for non-multishot ops. */
    bool more;
};

struct IoBackend; /* opaque — each backend defines its own */

struct IoOps {
    int (*init)(IoBackend *ctx);
    int (*submit_accept)(IoBackend *ctx, int listen_fd);
    int (*submit_recv)(IoBackend *ctx, int fd);
    int (*submit_send)(IoBackend *ctx, int fd, const uint8_t *data, uint32_t len);
    int (*submit_close)(IoBackend *ctx, int fd);
    /* TCP_NODELAY via io_uring_prep_cmd_sock (kernel 6.7+, liburing 2.7+).
     * Disables Nagle's algorithm on newly accepted connections. Critical for
     * request-response workloads where small responses must be sent immediately
     * rather than coalesced. Async to keep the accept path off the hot path. */
    int (*submit_nodelay)(IoBackend *ctx, int fd);
    void (*recycle_buffer)(IoBackend *ctx, uint16_t buf_id);
    int (*wait)(IoBackend *ctx, IoCompletion *out, int max_completions);
    void (*destroy)(IoBackend *ctx);
};
