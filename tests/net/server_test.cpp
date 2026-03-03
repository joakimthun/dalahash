// server_test.cpp — Server startup error propagation tests.

#include "net/server.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <new>

namespace {

struct FakeBackend {
    int create_call;
};

struct FakeRuntime {
    std::atomic<int> create_calls{0};
    int fail_create_on_call = 0;
    int init_result = 0;
};

static FakeRuntime* g_fake_runtime = nullptr;

static IoBackend* fake_backend_create(uint32_t, uint32_t, uint32_t, uint32_t) {
    if (!g_fake_runtime)
        return nullptr;

    const int create_call = g_fake_runtime->create_calls.fetch_add(1, std::memory_order_relaxed) + 1;
    if (g_fake_runtime->fail_create_on_call == create_call)
        return nullptr;

    auto* backend = new (std::nothrow) FakeBackend{.create_call = create_call};
    return reinterpret_cast<IoBackend*>(backend);
}

static int fake_init(IoBackend*) { return g_fake_runtime ? g_fake_runtime->init_result : -EIO; }
static int fake_submit_accept(IoBackend*, int) { return 0; }
static int fake_submit_recv(IoBackend*, int) { return 0; }
static int fake_submit_send(IoBackend*, int, const uint8_t*, uint32_t) { return 0; }
static int fake_submit_close(IoBackend*, int) { return 0; }
static int fake_submit_nodelay(IoBackend*, int) { return 0; }
static void fake_recycle_buffer(IoBackend*, uint16_t) {}
static void fake_recycle_buffers(IoBackend*, const uint16_t*, uint32_t) {}
static int fake_wait(IoBackend*, IoCompletion*, int) { return 0; }

static void fake_destroy(IoBackend* ctx) { delete reinterpret_cast<FakeBackend*>(ctx); }

static IoOps fake_ops() {
    return IoOps{
        .init = fake_init,
        .submit_accept = fake_submit_accept,
        .submit_recv = fake_submit_recv,
        .submit_send = fake_submit_send,
        .submit_close = fake_submit_close,
        .submit_nodelay = fake_submit_nodelay,
        .recycle_buffer = fake_recycle_buffer,
        .recycle_buffers = fake_recycle_buffers,
        .wait = fake_wait,
        .destroy = fake_destroy,
    };
}

TEST(Server, ReturnsErrorWhenWorkerInitFails) {
    FakeRuntime runtime = {};
    runtime.init_result = -EIO;
    g_fake_runtime = &runtime;

    ServerConfig config = {.port = 0, .num_workers = 1, .store_bytes = 1ull << 20};
    IoOps ops = fake_ops();
    EXPECT_NE(server_start_with_runtime(&config, &ops, fake_backend_create, true), 0);

    g_fake_runtime = nullptr;
}

TEST(Server, ReturnsErrorWhenBackendCreateFailsAfterStartingWorker) {
    FakeRuntime runtime = {};
    runtime.fail_create_on_call = 2;
    runtime.init_result = -EIO;
    g_fake_runtime = &runtime;

    ServerConfig config = {.port = 0, .num_workers = 2, .store_bytes = 1ull << 20};
    IoOps ops = fake_ops();
    EXPECT_NE(server_start_with_runtime(&config, &ops, fake_backend_create, true), 0);

    g_fake_runtime = nullptr;
}

} // namespace
