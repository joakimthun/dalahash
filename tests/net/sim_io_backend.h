/*
 * sim_io_backend.h — Simulated I/O backend for DST.
 *
 * In-process replacement for io_uring. Tests push scripted IoCompletion
 * events; wait() pops them; submit_send() captures outgoing data.
 */

#pragma once

#include "io.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

struct SimIoBackend {
    std::vector<IoCompletion> pending;
    size_t pending_index = 0;
    std::unordered_map<int, std::string> sent_data; /* captured per-fd responses */
    std::vector<int> recv_armed;
    std::vector<int> closed_fds;
    bool accept_armed = false;
    std::vector<std::vector<uint8_t>> buffers;      /* holds recv data allocations */
    bool inject_eintr = false;
    bool auto_stop = true;
};

static int sim_init(IoBackend *) { return 0; }

static int sim_submit_accept(IoBackend *ctx, int) {
    reinterpret_cast<SimIoBackend *>(ctx)->accept_armed = true;
    return 0;
}

static int sim_submit_recv(IoBackend *ctx, int fd) {
    reinterpret_cast<SimIoBackend *>(ctx)->recv_armed.push_back(fd);
    return 0;
}

static int sim_submit_send(IoBackend *ctx, int fd, const uint8_t *data, uint32_t len) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    be->sent_data[fd].append(reinterpret_cast<const char *>(data), len);
    IoCompletion comp = {};
    comp.kind = IoCompletion::SEND;
    comp.fd = fd;
    comp.result = static_cast<int>(len);
    be->pending.push_back(comp);
    return 0;
}

static int sim_submit_close(IoBackend *ctx, int fd) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    be->closed_fds.push_back(fd);
    IoCompletion comp = {};
    comp.kind = IoCompletion::CLOSE;
    comp.fd = fd;
    be->pending.push_back(comp);
    return 0;
}

static int sim_submit_nodelay(IoBackend *, int) { return 0; }

static void sim_recycle_buffer(IoBackend *, uint16_t) {}

static int sim_wait(IoBackend *ctx, IoCompletion *out, int max_completions) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    if (be->inject_eintr) { be->inject_eintr = false; return -EINTR; }
    int count = 0;
    while (be->pending_index < be->pending.size() && count < max_completions) {
        out[count++] = be->pending[be->pending_index++];
    }
    return count;
}

static void sim_destroy(IoBackend *) {} /* test owns the SimIoBackend */

static inline IoOps sim_io_ops() {
    return IoOps{
        .init = sim_init, .submit_accept = sim_submit_accept,
        .submit_recv = sim_submit_recv, .submit_send = sim_submit_send,
        .submit_close = sim_submit_close, .submit_nodelay = sim_submit_nodelay,
        .recycle_buffer = sim_recycle_buffer, .wait = sim_wait,
        .destroy = sim_destroy,
    };
}

static inline IoCompletion sim_accept(int new_fd) {
    /* more=true: multishot stays armed — worker won't try to resubmit. */
    return {.kind = IoCompletion::ACCEPT, .fd = new_fd, .result = new_fd,
            .buf = nullptr, .buf_len = 0, .buf_id = 0, .more = true};
}

static inline IoCompletion sim_recv(SimIoBackend *be, int fd, const void *data, uint32_t len) {
    be->buffers.emplace_back(reinterpret_cast<const uint8_t *>(data), reinterpret_cast<const uint8_t *>(data) + len);
    uint16_t buf_id = static_cast<uint16_t>(be->buffers.size() - 1);
    /* more=true: multishot stays armed — worker won't try to resubmit. */
    return {.kind = IoCompletion::RECV, .fd = fd, .result = static_cast<int>(len),
            .buf = be->buffers.back().data(), .buf_len = len, .buf_id = buf_id,
            .more = true};
}

static inline IoCompletion sim_close(int fd) {
    return {.kind = IoCompletion::CLOSE, .fd = fd, .result = 0,
            .buf = nullptr, .buf_len = 0, .buf_id = 0, .more = false};
}
