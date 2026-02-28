// dst_echo_test.cpp — Deterministic simulation tests for echo protocol.

#include "sim_io_backend.h"
#include "worker.h"

#include <atomic>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace {

static SimIoBackend run_worker_sim(std::vector<IoCompletion> events) {
    SimIoBackend sim;
    sim.pending = std::move(events);
    std::atomic<bool> running{true};
    sim.running = &running;

    WorkerConfig config = {};
    config.cpu_id = 0;
    config.port = 0;
    config.ops = sim_io_ops();
    config.backend = reinterpret_cast<IoBackend*>(&sim);
    config.running = &running;
    config.skip_setup = true;
    config.listen_fd = 0;

    worker_run(&config);
    return sim;
}

} // namespace

TEST(EchoDST, SingleChunkEcho) {
    SimIoBackend sim_setup;
    std::string payload = "hello, echo";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, payload.data(), static_cast<uint32_t>(payload.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], payload);
    EXPECT_EQ(result.send_call_count, 1);
}

TEST(EchoDST, MultipleRecvsEchoedInOrder) {
    SimIoBackend sim_setup;
    std::string part1 = "abc";
    std::string part2 = "DEF123";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, part1.data(), static_cast<uint32_t>(part1.size())));
    events.push_back(sim_recv(&sim_setup, 10, part2.data(), static_cast<uint32_t>(part2.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], part1 + part2);
    EXPECT_EQ(result.send_call_count, 2);
}

TEST(EchoDST, BinaryPayload) {
    SimIoBackend sim_setup;
    const uint8_t payload[] = {0x00, 0x01, 0xFE, 0xFF, 0x41, 0x00};

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, payload, static_cast<uint32_t>(sizeof(payload))));

    SimIoBackend result = run_worker_sim(events);
    std::string expected(reinterpret_cast<const char*>(payload), sizeof(payload));
    EXPECT_EQ(result.sent_data[10], expected);
}

TEST(EchoDST, LargePayloadInOneRecv) {
    SimIoBackend sim_setup;
    std::string payload(8192, 'x');

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, payload.data(), static_cast<uint32_t>(payload.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], payload);
    EXPECT_EQ(result.send_call_count, 1);
}

// --- T8: Echo protocol additional stress tests ---

TEST(EchoDST, SendFailClosesConnection) {
    SimIoBackend sim_setup;
    std::string payload = "echo me";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, payload.data(), static_cast<uint32_t>(payload.size())));
    sim.submit_send_fail_count = 1;

    std::atomic<bool> running{true};
    sim.running = &running;

    WorkerConfig config = {};
    config.cpu_id = 0;
    config.ops = sim_io_ops();
    config.backend = reinterpret_cast<IoBackend*>(&sim);
    config.running = &running;
    config.skip_setup = true;
    config.listen_fd = 0;

    worker_run(&config);

    bool was_closed = false;
    for (int fd : sim.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
}

TEST(EchoDST, EmptyRecvDoesNotCrash) {
    // Recv with null buf and 0 len — should not crash or close.
    IoCompletion nobufs = {};
    nobufs.kind = IoCompletion::RECV;
    nobufs.fd = 10;
    nobufs.result = 0;
    nobufs.buf = nullptr;
    nobufs.buf_len = 0;
    nobufs.buf_id = 0;
    nobufs.more = false;

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(nobufs);

    SimIoBackend result = run_worker_sim(events);

    // Connection should NOT be closed — just rearm recv.
    EXPECT_TRUE(result.closed_fds.empty());
    bool rearmed = false;
    for (int fd : result.recv_armed) {
        if (fd == 10) {
            rearmed = true;
            break;
        }
    }
    EXPECT_TRUE(rearmed);
}

TEST(EchoDST, ExactBufferBoundary) {
    // Echo with payload exactly 4096 bytes (BUF_SIZE boundary).
    SimIoBackend sim_setup;
    std::string payload(4096, 'B');

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, payload.data(), static_cast<uint32_t>(payload.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], payload);
}

TEST(EchoDST, BackpressureClosesConnection) {
    // Fill TX queue past 1 MiB to trigger backpressure close.
    SimIoBackend sim_setup;

    std::string big(4096, 'X');

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.submit_send_fail_count = 999;
    // Send enough data to exceed 1 MiB
    for (int i = 0; i < 300; i++)
        sim.pending.push_back(sim_recv(&sim_setup, 10, big.data(), static_cast<uint32_t>(big.size())));

    std::atomic<bool> running{true};
    sim.running = &running;

    WorkerConfig config = {};
    config.cpu_id = 0;
    config.ops = sim_io_ops();
    config.backend = reinterpret_cast<IoBackend*>(&sim);
    config.running = &running;
    config.skip_setup = true;
    config.listen_fd = 0;

    worker_run(&config);

    bool was_closed = false;
    for (int fd : sim.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
}
