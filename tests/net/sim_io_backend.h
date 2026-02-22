// sim_io_backend.h — Simulated I/O backend for DST.
//
// In-process replacement for io_uring. Tests push scripted IoCompletion
// events; wait() pops them; submit_send() captures outgoing data.

#pragma once

#include "io.h"

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

struct SimIoBackend {
    struct PendingSendRef {
        int fd;
        const uint8_t *data;
        uint32_t len;
    };

    std::vector<IoCompletion> pending;
    size_t pending_index = 0;
    std::unordered_map<int, std::string> sent_data; // captured per-fd responses
    std::vector<int> recv_armed;
    std::vector<int> closed_fds;
    bool accept_armed = false;
    std::vector<std::vector<uint8_t>> buffers;      // holds recv data allocations
    bool inject_eintr = false;
    bool auto_stop = true;
    int send_call_count = 0;
    int submit_close_call_count = 0;
    std::atomic<bool> *running = nullptr; // set by integration tests for auto_stop
    bool copy_send_on_wait = false;       // emulate async user-buffer reads
    std::vector<PendingSendRef> pending_send_refs;
    size_t pending_send_index = 0;
    std::vector<int> scripted_send_results;
    size_t scripted_send_result_index = 0;
    std::vector<uint16_t> recycled_buf_ids;

        //  Failure injection: when > 0, the corresponding submit returns -ENOSPC
    // and the counter is decremented.
    int submit_accept_fail_count = 0;
    int submit_recv_fail_count = 0;
    int submit_send_fail_count = 0;
    int submit_close_fail_count = 0;
    int submit_close_fail_errno = -ENOSPC;
};

static int sim_init(IoBackend *) { return 0; }

static int sim_submit_accept(IoBackend *ctx, int) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    if (be->submit_accept_fail_count > 0) { be->submit_accept_fail_count--; return -ENOSPC; }
    be->accept_armed = true;
    return 0;
}

static int sim_submit_recv(IoBackend *ctx, int fd) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    if (be->submit_recv_fail_count > 0) { be->submit_recv_fail_count--; return -ENOSPC; }
    be->recv_armed.push_back(fd);
    return 0;
}

static int sim_submit_send(IoBackend *ctx, int fd, const uint8_t *data, uint32_t len) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    if (be->submit_send_fail_count > 0) { be->submit_send_fail_count--; return -ENOSPC; }
    be->send_call_count++;

    if (be->copy_send_on_wait) {
        be->pending_send_refs.push_back({.fd = fd, .data = data, .len = len});
    } else {
        be->sent_data[fd].append(reinterpret_cast<const char *>(data), len);
    }

    int result = static_cast<int>(len);
    if (be->scripted_send_result_index < be->scripted_send_results.size())
        result = be->scripted_send_results[be->scripted_send_result_index++];

    IoCompletion comp = {};
    comp.kind = IoCompletion::SEND;
    comp.fd = fd;
    comp.result = result;
    be->pending.push_back(comp);
    return 0;
}

static int sim_submit_close(IoBackend *ctx, int fd) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    be->submit_close_call_count++;
    if (be->submit_close_fail_count > 0) {
        be->submit_close_fail_count--;
        return be->submit_close_fail_errno;
    }
    be->closed_fds.push_back(fd);
    IoCompletion comp = {};
    comp.kind = IoCompletion::CLOSE;
    comp.fd = fd;
    be->pending.push_back(comp);
    return 0;
}

static int sim_submit_nodelay(IoBackend *, int) { return 0; }

static void sim_recycle_buffer(IoBackend *ctx, uint16_t buf_id) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    be->recycled_buf_ids.push_back(buf_id);
}

static void sim_recycle_buffers(IoBackend *ctx, const uint16_t *buf_ids, uint32_t count) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    for (uint32_t i = 0; i < count; i++)
        be->recycled_buf_ids.push_back(buf_ids[i]);
}

static int sim_wait(IoBackend *ctx, IoCompletion *out, int max_completions) {
    auto *be = reinterpret_cast<SimIoBackend *>(ctx);
    if (be->inject_eintr) { be->inject_eintr = false; return -EINTR; }
    int count = 0;
    while (be->pending_index < be->pending.size() && count < max_completions) {
        IoCompletion comp = be->pending[be->pending_index++];
        if (comp.kind == IoCompletion::SEND && be->copy_send_on_wait &&
            be->pending_send_index < be->pending_send_refs.size()) {
            SimIoBackend::PendingSendRef ref = be->pending_send_refs[be->pending_send_index++];
            if (comp.result > 0) {
                uint32_t copy_len = static_cast<uint32_t>(comp.result);
                if (copy_len > ref.len) copy_len = ref.len;
                be->sent_data[ref.fd].append(reinterpret_cast<const char *>(ref.data), copy_len);
            }
        }
        out[count++] = comp;
    }
        //  When all scripted events are drained and auto_stop is set, signal the
    // worker to exit its event loop.
    if (count == 0 && be->auto_stop && be->running)
        be->running->store(false, std::memory_order_relaxed);
    return count;
}

static void sim_destroy(IoBackend *) {} // test owns the SimIoBackend

static inline IoOps sim_io_ops() {
    return IoOps{
        .init = sim_init, .submit_accept = sim_submit_accept,
        .submit_recv = sim_submit_recv, .submit_send = sim_submit_send,
        .submit_close = sim_submit_close, .submit_nodelay = sim_submit_nodelay,
        .recycle_buffer = sim_recycle_buffer, .recycle_buffers = sim_recycle_buffers,
        .wait = sim_wait,
        .destroy = sim_destroy,
    };
}

static inline IoCompletion sim_accept(int new_fd) {
    // more=true: multishot stays armed — worker won't try to resubmit.
    return {.kind = IoCompletion::ACCEPT, .fd = new_fd, .result = new_fd,
            .buf = nullptr, .buf_len = 0, .buf_id = 0, .more = true};
}

static inline IoCompletion sim_recv(SimIoBackend *be, int fd, const void *data, uint32_t len) {
    be->buffers.emplace_back(reinterpret_cast<const uint8_t *>(data), reinterpret_cast<const uint8_t *>(data) + len);
    uint16_t buf_id = static_cast<uint16_t>(be->buffers.size() - 1);
    // more=true: multishot stays armed — worker won't try to resubmit.
    return {.kind = IoCompletion::RECV, .fd = fd, .result = static_cast<int>(len),
            .buf = be->buffers.back().data(), .buf_len = len, .buf_id = buf_id,
            .more = true};
}

static inline IoCompletion sim_close(int fd) {
    return {.kind = IoCompletion::CLOSE, .fd = fd, .result = 0,
            .buf = nullptr, .buf_len = 0, .buf_id = 0, .more = false};
}
