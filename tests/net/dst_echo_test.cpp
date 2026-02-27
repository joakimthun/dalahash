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
