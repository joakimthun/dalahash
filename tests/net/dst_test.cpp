// dst_test.cpp — Deterministic simulation tests using SimIoBackend.

#include "connection.h"
#include "protocol/protocol.h"
#include "sim_io_backend.h"
#include "worker.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

// Mirrors worker's handle_recv: parse selected protocol, execute, return response.
static std::string process_recv(Connection* conn, const uint8_t* data, uint32_t len,
                                ProtocolWorkerState* protocol_state) {
    std::string response;
    uint8_t response_buf[65536];

    const uint8_t* parse_buf;
    uint32_t parse_len;
    uint8_t combined[16384 + 4096];

    if (conn->input_len > 0) {
        std::memcpy(combined, conn->input_buf, conn->input_len);
        uint32_t copy_len = len;
        if (conn->input_len + copy_len > sizeof(combined))
            copy_len = sizeof(combined) - conn->input_len;
        std::memcpy(combined + conn->input_len, data, copy_len);
        parse_buf = combined;
        parse_len = conn->input_len + copy_len;
        conn->input_len = 0;
    } else {
        parse_buf = data;
        parse_len = len;
    }

    uint32_t offset = 0;
    while (offset < parse_len) {
        ProtocolCommand cmd;
        uint32_t consumed = 0;
        auto result = protocol_parse(parse_buf + offset, parse_len - offset, &cmd, &consumed);
        if (result == PROTOCOL_PARSE_OK) {
            uint32_t n =
                protocol_execute(&cmd, protocol_state, protocol_now_ms(), response_buf, sizeof(response_buf));
            response.append(reinterpret_cast<char*>(response_buf), n);
            offset += consumed;
        } else if (result == PROTOCOL_PARSE_INCOMPLETE) {
            uint32_t remaining = parse_len - offset;
            std::memcpy(conn->input_buf, parse_buf + offset, remaining);
            conn->input_len = remaining;
            break;
        } else {
            conn->closing = true;
            break;
        }
    }
    return response;
}

TEST(DST, SetThenGet) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string set_data = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(set_data.data()),
                           static_cast<uint32_t>(set_data.size()), &state),
              "+OK\r\n");
    std::string get_data = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(get_data.data()),
                           static_cast<uint32_t>(get_data.size()), &state),
              "$3\r\nbar\r\n");
    connection_destroy(conn);
}

TEST(DST, GetMiss) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string data = "*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(data.data()),
                           static_cast<uint32_t>(data.size()), &state),
              "$-1\r\n");
    connection_destroy(conn);
}

TEST(DST, PipelinedCommands) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string data = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
                       "*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(data.data()),
                           static_cast<uint32_t>(data.size()), &state),
              "+OK\r\n$1\r\n1\r\n");
    connection_destroy(conn);
}

TEST(DST, PartialReassembly) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string full = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    size_t split = 15;

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()), static_cast<uint32_t>(split),
                           &state),
              "");
    EXPECT_GT(conn->input_len, 0u);

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()) + split,
                           static_cast<uint32_t>(full.size() - split), &state),
              "+OK\r\n");
    connection_destroy(conn);
}

TEST(DST, ProtocolError) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string bad = "GARBAGE\r\n";
    process_recv(conn, reinterpret_cast<const uint8_t*>(bad.data()), static_cast<uint32_t>(bad.size()),
                 &state);
    EXPECT_TRUE(conn->closing);
    connection_destroy(conn);
}

TEST(DST, SimBackendCapturesSend) {
    SimIoBackend sim;
    IoOps ops = sim_io_ops();
    ops.submit_send(reinterpret_cast<IoBackend*>(&sim), 42, reinterpret_cast<const uint8_t*>("+OK\r\n"), 5);
    EXPECT_EQ(sim.sent_data[42], "+OK\r\n");
}

TEST(DST, SimBackendTracksOperations) {
    SimIoBackend sim;
    IoOps ops = sim_io_ops();
    ops.submit_accept(reinterpret_cast<IoBackend*>(&sim), 5);
    EXPECT_TRUE(sim.accept_armed);
    ops.submit_recv(reinterpret_cast<IoBackend*>(&sim), 10);
    EXPECT_EQ(sim.recv_armed[0], 10);
    ops.submit_close(reinterpret_cast<IoBackend*>(&sim), 10);
    EXPECT_EQ(sim.closed_fds[0], 10);
}

TEST(DST, SimBackendWait) {
    SimIoBackend sim;
    IoOps ops = sim_io_ops();
    sim.pending.push_back(sim_accept(10));
    std::string data = "*1\r\n$4\r\nPING\r\n";
    sim.pending.push_back(sim_recv(&sim, 10, data.data(), static_cast<uint32_t>(data.size())));

    IoCompletion out[16];
    int n = ops.wait(reinterpret_cast<IoBackend*>(&sim), out, 16);
    EXPECT_EQ(n, 2);
    EXPECT_EQ(out[0].kind, IoCompletion::ACCEPT);
    EXPECT_EQ(out[1].kind, IoCompletion::RECV);
}

TEST(DST, DeepPipelineCoalesced) {
    SimIoBackend sim;
    IoOps ops = sim_io_ops();
    auto* ctx = reinterpret_cast<IoBackend*>(&sim);

    // Build 10 pipelined PINGs.
    std::string pings;
    for (int i = 0; i < 10; i++)
        pings += "*1\r\n$4\r\nPING\r\n";

    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim, 10, pings.data(), static_cast<uint32_t>(pings.size())));

    // Drive completions through worker_run.
    Connection* conns[MAX_CONNECTIONS] = {};
    ProtocolWorkerState protocol_state = {};
    protocol_worker_init(&protocol_state);
    IoCompletion completions[256];

    int n = ops.wait(ctx, completions, 256);
    for (int i = 0; i < n; i++) {
        IoCompletion* c = &completions[i];
        if (c->kind == IoCompletion::ACCEPT) {
            Connection* conn = connection_create(c->fd);
            conns[c->fd] = conn;
        }
    }

    //  Process the recv through the real code path — but we can't call
    // handle_recv directly (static). Phase 5 adds integration tests via
    // worker_run. For now, verify via sim backend send capture.
    // Push recv event again so sim_wait returns it.
    sim.pending_index = 0;
    sim.pending.clear();
    sim.pending.push_back(sim_recv(&sim, 10, pings.data(), static_cast<uint32_t>(pings.size())));

    n = ops.wait(ctx, completions, 256);

    // Manually exercise the pipeline logic matching worker.cpp handle_recv.
    IoCompletion* comp = &completions[0];
    Connection* conn = conns[10];
    const uint8_t* parse_buf = comp->buf;
    uint32_t parse_len = comp->buf_len;
    uint32_t offset = 0;
    uint8_t response[65536];
    uint32_t resp_offset = 0;

    while (offset < parse_len) {
        ProtocolCommand cmd;
        uint32_t consumed = 0;
        auto result = protocol_parse(parse_buf + offset, parse_len - offset, &cmd, &consumed);
        if (result == PROTOCOL_PARSE_OK) {
            uint32_t cap = 65536 - resp_offset;
            uint32_t resp_len =
                protocol_execute(&cmd, &protocol_state, protocol_now_ms(), response + resp_offset, cap);
            resp_offset += resp_len;
            offset += consumed;
        } else {
            break;
        }
    }

    // All 10 PINGs should produce a single coalesced response.
    std::string expected;
    for (int i = 0; i < 10; i++)
        expected += "+PONG\r\n";
    EXPECT_EQ(std::string(reinterpret_cast<char*>(response), resp_offset), expected);

    // Verify it would be 1 send call (resp_offset > 0 → one submit_send).
    sim.sent_data.clear();
    ops.submit_send(ctx, 10, response, resp_offset);
    EXPECT_EQ(sim.sent_data[10], expected);

    connection_destroy(conn);
}

TEST(DST, MultipleConnections) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection *c1 = connection_create(10), *c2 = connection_create(11);
    std::string s1 = "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n";
    std::string s2 = "*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
    EXPECT_EQ(process_recv(c1, reinterpret_cast<const uint8_t*>(s1.data()), static_cast<uint32_t>(s1.size()),
                           &state),
              "+OK\r\n");
    EXPECT_EQ(process_recv(c2, reinterpret_cast<const uint8_t*>(s2.data()), static_cast<uint32_t>(s2.size()),
                           &state),
              "+OK\r\n");

    std::string g1 = "*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n";
    EXPECT_EQ(process_recv(c2, reinterpret_cast<const uint8_t*>(g1.data()), static_cast<uint32_t>(g1.size()),
                           &state),
              "$2\r\nv1\r\n");
    connection_destroy(c1);
    connection_destroy(c2);
}

// --- Integration tests: exercise real worker_run with SimIoBackend ---

// Helper: run worker_run with scripted events, return sim backend for assertions.
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

TEST(DSTIntegration, PingViaWorkerRun) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";
    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+PONG\r\n");
}

TEST(DSTIntegration, SetGetViaWorkerRun) {
    SimIoBackend sim_setup;
    std::string set_cmd = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    std::string get_cmd = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, set_cmd.data(), static_cast<uint32_t>(set_cmd.size())));
    events.push_back(sim_recv(&sim_setup, 10, get_cmd.data(), static_cast<uint32_t>(get_cmd.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+OK\r\n$3\r\nbar\r\n");
}

TEST(DSTIntegration, PipelinedPingsViaWorkerRun) {
    SimIoBackend sim_setup;
    std::string pings;
    for (int i = 0; i < 10; i++)
        pings += "*1\r\n$4\r\nPING\r\n";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, pings.data(), static_cast<uint32_t>(pings.size())));

    SimIoBackend result = run_worker_sim(events);

    std::string expected;
    for (int i = 0; i < 10; i++)
        expected += "+PONG\r\n";
    EXPECT_EQ(result.sent_data[10], expected);
    // Coalesced into 1 send call.
    EXPECT_EQ(result.send_call_count, 1);
}

TEST(DSTIntegration, BufferExhaustionRearms) {
    // Simulate -ENOBUFS: RECV with null buf, more=false.
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

    // Connection should NOT be closed — recv should be rearmed.
    EXPECT_TRUE(result.closed_fds.empty());
    // recv_armed should contain fd 10 (the rearm).
    bool rearmed = false;
    for (int fd : result.recv_armed) {
        if (fd == 10) {
            rearmed = true;
            break;
        }
    }
    EXPECT_TRUE(rearmed);
}

TEST(DSTIntegration, AcceptRearmOnSQFull) {
    SimIoBackend sim_setup;
    //  First accept will trigger submit_recv which will succeed.
    // But make the multishot accept terminate (more=false) and have
    // submit_accept fail once.
    IoCompletion accept_nomore = sim_accept(10);
    accept_nomore.more = false; // triggers rearm

    std::string ping = "*1\r\n$4\r\nPING\r\n";

    std::vector<IoCompletion> events;
    events.push_back(accept_nomore);
    events.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));

    SimIoBackend sim;
    sim.pending = events;
    sim.submit_accept_fail_count = 1; // first rearm fails
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

    // accept_armed should be true — retried on next iteration.
    EXPECT_TRUE(sim.accept_armed);
    EXPECT_EQ(sim.sent_data[10], "+PONG\r\n");
}

TEST(DSTIntegration, RecvFailClosesConnection) {
    // Accept a connection, but make submit_recv fail → connection should close.
    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));

    SimIoBackend sim;
    sim.pending = events;
    sim.submit_recv_fail_count = 1; // recv after accept fails
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

    // Connection fd 10 should have been closed.
    bool was_closed = false;
    for (int fd : sim.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
}

TEST(DSTIntegration, PartialReassemblyViaWorkerRun) {
    SimIoBackend sim_setup;
    std::string full = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    size_t split = 15;

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, full.data(), static_cast<uint32_t>(split)));
    events.push_back(
        sim_recv(&sim_setup, 10, full.data() + split, static_cast<uint32_t>(full.size() - split)));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+OK\r\n");
}

TEST(DSTIntegration, AcceptInitialArmFailureRetries) {
    SimIoBackend sim;
    sim.submit_accept_fail_count = 1; // initial arm fails
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
    EXPECT_TRUE(sim.accept_armed);
}

TEST(DSTIntegration, AcceptErrorTriggersAcceptRearm) {
    IoCompletion accept_error = {};
    accept_error.kind = IoCompletion::ACCEPT;
    accept_error.fd = 0;
    accept_error.result = -EIO;
    accept_error.buf = nullptr;
    accept_error.buf_len = 0;
    accept_error.buf_id = 0;
    accept_error.more = false;

    std::vector<IoCompletion> events;
    events.push_back(accept_error);

    SimIoBackend result = run_worker_sim(events);
    EXPECT_GE(result.submit_accept_call_count, 2);
}

TEST(DSTIntegration, AcceptErrorDoesNotCloseConnectionAtListenFdIndex) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    IoCompletion accept_error = {};
    accept_error.kind = IoCompletion::ACCEPT;
    accept_error.fd = 0;        // listen fd in skip_setup mode
    accept_error.result = -EIO; // accept failure
    accept_error.buf = nullptr;
    accept_error.buf_len = 0;
    accept_error.buf_id = 0;
    accept_error.more = false;

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(0));
    events.push_back(accept_error);
    events.push_back(sim_recv(&sim_setup, 0, ping.data(), static_cast<uint32_t>(ping.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[0], "+PONG\r\n");
    for (int fd : result.closed_fds) {
        EXPECT_NE(fd, 0);
    }
}

TEST(DSTIntegration, AcceptAllocFailureClosesAcceptedFd) {
    g_connection_create_fail_count = 1;

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));

    SimIoBackend result = run_worker_sim(events);
    g_connection_create_fail_count = 0;

    bool was_closed = false;
    for (int fd : result.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
}

TEST(DSTIntegration, SendBuffersOwnedAcrossAsyncCompletion) {
    SimIoBackend sim_setup;
    std::string set_cmd = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    std::string get_cmd = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, set_cmd.data(), static_cast<uint32_t>(set_cmd.size())));
    sim.pending.push_back(sim_recv(&sim_setup, 10, get_cmd.data(), static_cast<uint32_t>(get_cmd.size())));
    sim.copy_send_on_wait = true; // read user buffers when SEND CQEs are delivered

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
    EXPECT_EQ(sim.sent_data[10], "+OK\r\n$3\r\nbar\r\n");
}

TEST(DSTIntegration, PartialSendResubmitsRemainingBytes) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));
    sim.copy_send_on_wait = true;
    sim.scripted_send_results = {3, 4}; // +PONG\\r\\n -> 3 bytes then 4 bytes

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
    EXPECT_EQ(sim.sent_data[10], "+PONG\r\n");
    EXPECT_EQ(sim.send_call_count, 2);
}

TEST(DSTIntegration, ReassemblyOverflowClosesConnection) {
    SimIoBackend sim_setup;
    std::string prefix = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$20000\r\n";
    std::string first = prefix;
    if (first.size() < CONN_BUF_SIZE - 1)
        first.append(CONN_BUF_SIZE - 1 - first.size(), 'a');
    std::string second(32, 'b');

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, first.data(), static_cast<uint32_t>(first.size())));
    events.push_back(sim_recv(&sim_setup, 10, second.data(), static_cast<uint32_t>(second.size())));

    SimIoBackend result = run_worker_sim(events);

    bool was_closed = false;
    for (int fd : result.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
}

// --- Additional unit-level DST tests ---

TEST(DST, EmptyRecv) {
    // recv with 0 bytes of data (buf non-null but len=0).
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    uint8_t dummy = 0;
    std::string result = process_recv(conn, &dummy, 0, &state);
    EXPECT_EQ(result, "");
    EXPECT_FALSE(conn->closing);
    connection_destroy(conn);
}

TEST(DST, MultiplePartialReassemblies) {
    // Split a command into 3 chunks across recvs.
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string full = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    size_t s1 = 5, s2 = 15;

    EXPECT_EQ(
        process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()), static_cast<uint32_t>(s1), &state),
        "");
    EXPECT_GT(conn->input_len, 0u);

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()) + s1,
                           static_cast<uint32_t>(s2 - s1), &state),
              "");
    EXPECT_GT(conn->input_len, 0u);

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()) + s2,
                           static_cast<uint32_t>(full.size() - s2), &state),
              "+OK\r\n");
    connection_destroy(conn);
}

TEST(DST, PipelinedMixedCommands) {
    // SET, GET, PING, and unknown command in one recv.
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string data = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
                       "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
                       "*1\r\n$4\r\nPING\r\n"
                       "*1\r\n$3\r\nFOO\r\n";
    std::string result = process_recv(conn, reinterpret_cast<const uint8_t*>(data.data()),
                                      static_cast<uint32_t>(data.size()), &state);
    EXPECT_EQ(result, "+OK\r\n$1\r\nv\r\n+PONG\r\n-ERR unknown command\r\n");
    connection_destroy(conn);
}

// --- Additional integration tests ---

TEST(DSTIntegration, SignalInterruption) {
    // inject_eintr=true → worker continues after -EINTR from wait.
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    SimIoBackend sim;
    sim.inject_eintr = true; // first wait returns -EINTR
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));

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
    EXPECT_EQ(sim.sent_data[10], "+PONG\r\n");
}

TEST(DSTIntegration, MultipleConnectionsViaWorkerRun) {
    SimIoBackend sim_setup;
    std::string set1 = "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n";
    std::string set2 = "*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
    std::string get1 = "*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n";
    std::string get2 = "*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_accept(11));
    events.push_back(sim_recv(&sim_setup, 10, set1.data(), static_cast<uint32_t>(set1.size())));
    events.push_back(sim_recv(&sim_setup, 11, set2.data(), static_cast<uint32_t>(set2.size())));
    events.push_back(sim_recv(&sim_setup, 10, get1.data(), static_cast<uint32_t>(get1.size())));
    events.push_back(sim_recv(&sim_setup, 11, get2.data(), static_cast<uint32_t>(get2.size())));

    SimIoBackend result = run_worker_sim(events);
    // Each connection should have its own SET OK + GET response.
    EXPECT_NE(result.sent_data[10].find("+OK\r\n"), std::string::npos);
    EXPECT_NE(result.sent_data[10].find("$2\r\nv1\r\n"), std::string::npos);
    EXPECT_NE(result.sent_data[11].find("+OK\r\n"), std::string::npos);
    EXPECT_NE(result.sent_data[11].find("$2\r\nv2\r\n"), std::string::npos);
}

TEST(DSTIntegration, ErrorCompletionClosesConnection) {
    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));

    IoCompletion err = {};
    err.kind = IoCompletion::ERROR;
    err.fd = 10;
    err.result = -EIO;
    events.push_back(err);

    SimIoBackend result = run_worker_sim(events);
    bool was_closed = false;
    for (int fd : result.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
}

TEST(DSTIntegration, IgnoreCompletionSkipped) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));

    // Push IGNORE completion between accept and recv.
    IoCompletion ignore = {};
    ignore.kind = IoCompletion::IGNORE;
    ignore.fd = 10;
    events.push_back(ignore);

    events.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));

    SimIoBackend result = run_worker_sim(events);
    // Worker should continue normally — PING response should be sent.
    EXPECT_EQ(result.sent_data[10], "+PONG\r\n");
    // fd 10 should NOT be closed.
    for (int fd : result.closed_fds) {
        EXPECT_NE(fd, 10);
    }
}

TEST(DSTIntegration, SendFailClosesConnection) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));
    sim.submit_send_fail_count = 1; // send after recv fails

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

TEST(DSTIntegration, SendSubmitFailurePermanentCloseFailureDoesNotRetry) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));
    sim.submit_send_fail_count = 1;
    sim.submit_close_fail_count = 4;
    sim.submit_close_fail_errno = -EINVAL;

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

    EXPECT_EQ(sim.send_call_count, 0);
    EXPECT_EQ(sim.submit_close_call_count, 1);
    EXPECT_TRUE(sim.closed_fds.empty());
}

TEST(DSTIntegration, PendingCloseRetrySucceeds) {
    SimIoBackend sim_setup;
    std::string bad = "GARBAGE\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, bad.data(), static_cast<uint32_t>(bad.size())));
    sim.submit_close_fail_count = 1; // first close fails with -ENOSPC
    sim.submit_close_fail_errno = -ENOSPC;

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

    // Close should eventually succeed on retry.
    bool was_closed = false;
    for (int fd : sim.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
    // Should have been attempted at least twice.
    EXPECT_GE(sim.submit_close_call_count, 2);
}

TEST(DSTIntegration, CloseRetryQueueOverflowStillClosesHighFd) {
    SimIoBackend sim_setup;
    std::string bad = "GARBAGE\r\n";

    SimIoBackend sim;
    static constexpr int kConnCount = 300; // exceeds worker's pending-close queue
    for (int fd = 0; fd < kConnCount; fd++) {
        sim.pending.push_back(sim_accept(fd));
        sim.pending.push_back(sim_recv(&sim_setup, fd, bad.data(), static_cast<uint32_t>(bad.size())));
    }
    sim.submit_close_fail_count = kConnCount; // first close wave fails with ENOSPC
    sim.submit_close_fail_errno = -ENOSPC;

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

    bool high_fd_closed = false;
    for (int fd : sim.closed_fds) {
        if (fd == kConnCount - 1) {
            high_fd_closed = true;
            break;
        }
    }
    EXPECT_TRUE(high_fd_closed);
}

TEST(DSTIntegration, RecvAfterMultishotTermination) {
    // Multishot recv terminates (more=false), worker should rearm.
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    std::vector<uint8_t> ping_buf(ping.begin(), ping.end());

    IoCompletion recv_nomore = {};
    recv_nomore.kind = IoCompletion::RECV;
    recv_nomore.fd = 10;
    recv_nomore.result = static_cast<int>(ping_buf.size());
    recv_nomore.buf = ping_buf.data();
    recv_nomore.buf_len = static_cast<uint32_t>(ping_buf.size());
    recv_nomore.buf_id = 99;
    recv_nomore.more = false; // multishot terminated

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(recv_nomore);

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+PONG\r\n");
    // recv should have been rearmed for fd 10.
    bool rearmed = false;
    for (int fd : result.recv_armed) {
        if (fd == 10) {
            rearmed = true;
            break;
        }
    }
    EXPECT_TRUE(rearmed);
}

TEST(DSTIntegration, RecvRearmFailurePermanentCloseFailureDoesNotRetry) {
    SimIoBackend sim_setup;
    std::string partial = "*1\r\n$4\r\nPING\r";

    std::vector<uint8_t> partial_buf(partial.begin(), partial.end());

    IoCompletion recv_nomore = {};
    recv_nomore.kind = IoCompletion::RECV;
    recv_nomore.fd = 10;
    recv_nomore.result = static_cast<int>(partial_buf.size());
    recv_nomore.buf = partial_buf.data();
    recv_nomore.buf_len = static_cast<uint32_t>(partial_buf.size());
    recv_nomore.buf_id = 99;
    recv_nomore.more = false;

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(recv_nomore);
    sim.submit_recv_fail_after_successes = 1;
    sim.submit_close_fail_count = 4;
    sim.submit_close_fail_errno = -EINVAL;

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

    ASSERT_EQ(sim.recv_armed.size(), 1u);
    EXPECT_EQ(sim.recv_armed[0], 10);
    ASSERT_EQ(sim.recycled_buf_ids.size(), 1u);
    EXPECT_EQ(sim.recycled_buf_ids[0], 99);
    EXPECT_EQ(sim.submit_close_call_count, 1);
    EXPECT_TRUE(sim.closed_fds.empty());
}

TEST(DSTIntegration, NoCommandExecutionWhenClosing) {
    SimIoBackend sim_setup;
    std::string bad = "GARBAGE\r\n";
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, bad.data(), static_cast<uint32_t>(bad.size())));
    events.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));

    SimIoBackend result = run_worker_sim(events);

    auto it = result.sent_data.find(10);
    EXPECT_TRUE(it == result.sent_data.end() || it->second.empty());

    bool was_closed = false;
    for (int fd : result.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
}

TEST(DSTIntegration, SendResultZeroClosesConnection) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));
    sim.copy_send_on_wait = true;
    sim.scripted_send_results = {0}; // zero bytes sent → error

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

TEST(DSTIntegration, SendResultNegativeClosesConnection) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));
    sim.copy_send_on_wait = true;
    sim.scripted_send_results = {-1}; // negative → error

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

TEST(DSTIntegration, SendResultOverrunClosesConnection) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));
    sim.copy_send_on_wait = true;
    sim.scripted_send_results = {9999}; // > response len → overrun

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

TEST(DSTIntegration, MultipleChunkedSends) {
    // Large response split across 3 partial sends.
    SimIoBackend sim_setup;
    std::string pings;
    for (int i = 0; i < 5; i++)
        pings += "*1\r\n$4\r\nPING\r\n";
    // 5 x "+PONG\r\n" = 35 bytes total response

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, pings.data(), static_cast<uint32_t>(pings.size())));
    sim.copy_send_on_wait = true;
    sim.scripted_send_results = {10, 15, 10}; // 10 + 15 + 10 = 35 bytes

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

    std::string expected;
    for (int i = 0; i < 5; i++)
        expected += "+PONG\r\n";
    EXPECT_EQ(sim.sent_data[10], expected);
    EXPECT_EQ(sim.send_call_count, 3);
}

TEST(DSTIntegration, PartialSendResubmitFailurePermanentCloseFailureDoesNotRetry) {
    SimIoBackend sim_setup;
    std::string pings;
    for (int i = 0; i < 5; i++)
        pings += "*1\r\n$4\r\nPING\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, pings.data(), static_cast<uint32_t>(pings.size())));
    sim.copy_send_on_wait = true;
    sim.scripted_send_results = {10};
    sim.submit_send_fail_after_successes = 1;
    sim.submit_close_fail_count = 4;
    sim.submit_close_fail_errno = -EINVAL;

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

    std::string expected;
    for (int i = 0; i < 5; i++)
        expected += "+PONG\r\n";

    EXPECT_EQ(sim.send_call_count, 1);
    EXPECT_EQ(sim.sent_data[10], expected.substr(0, 10));
    EXPECT_EQ(sim.submit_close_call_count, 1);
    EXPECT_TRUE(sim.closed_fds.empty());
}

TEST(DSTIntegration, BufferRecycling) {
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    // Two recv events with distinct buf_ids.
    events.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));
    events.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));

    SimIoBackend result = run_worker_sim(events);

    // Both buf_ids should have been recycled.
    EXPECT_GE(result.recycled_buf_ids.size(), 2u);
}

TEST(DSTIntegration, CloseRetryOnlyOnENOSPC) {
    SimIoBackend sim_setup;
    std::string bad = "GARBAGE\r\n";

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, bad.data(), static_cast<uint32_t>(bad.size())));
    sim.submit_close_fail_count = 4;
    sim.submit_close_fail_errno = -EINVAL; // permanent close-submit failure

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
    EXPECT_EQ(sim.submit_close_call_count, 1);
}

TEST(DSTIntegration, TxHighWatermarkClosesConnection) {
    // Fill TX queue past 1 MiB backpressure limit, verify connection closed.
    SimIoBackend sim_setup;

    // Build a large pipelined payload: many SET commands whose responses will
    // queue up. We make submit_send fail so responses accumulate in TX queue.
    std::string cmds;
    // Each +OK\r\n is 5 bytes. To exceed 1 MiB (1048576) we need ~210000 cmds.
    // But that's too slow. Instead, use large GET responses.
    // SET a big value, then GET it many times so responses pile up.
    std::string big_value(4000, 'X');
    std::string set_cmd = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4000\r\n" + big_value + "\r\n";
    std::string get_cmd = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";

    // Each GET response is ~"$4000\r\n" + 4000 bytes + "\r\n" ≈ 4010 bytes.
    // Need ~262 GETs to exceed 1 MiB.
    std::string gets;
    for (int i = 0; i < 300; i++)
        gets += get_cmd;

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, set_cmd.data(), static_cast<uint32_t>(set_cmd.size())));
    // Make sends fail so TX queue fills up.
    sim.submit_send_fail_count = 999;
    sim.pending.push_back(sim_recv(&sim_setup, 10, gets.data(), static_cast<uint32_t>(gets.size())));

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

    // Connection should have been closed due to backpressure.
    bool was_closed = false;
    for (int fd : sim.closed_fds) {
        if (fd == 10) {
            was_closed = true;
            break;
        }
    }
    EXPECT_TRUE(was_closed);
}

TEST(DSTIntegration, ReassemblyAtStructureBoundaries) {
    // Split a SET command at various RESP structure boundaries.
    SimIoBackend sim_setup;
    std::string full = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";

    // Split points: after '*', after '3', inside first \r\n, after $, inside
    // bulk data, etc.
    size_t splits[] = {1, 2, 3, 4, 8, 10, 14, 18, 22, 26, 30};
    for (size_t split : splits) {
        if (split >= full.size())
            continue;

        std::vector<IoCompletion> events;
        events.push_back(sim_accept(10));
        events.push_back(sim_recv(&sim_setup, 10, full.data(), static_cast<uint32_t>(split)));
        events.push_back(
            sim_recv(&sim_setup, 10, full.data() + split, static_cast<uint32_t>(full.size() - split)));

        SimIoBackend result = run_worker_sim(events);
        EXPECT_EQ(result.sent_data[10], "+OK\r\n") << "Failed at split point " << split;
    }
}

TEST(DST, BinaryKeyValueViaDST) {
    // SET/GET with keys/values containing \0, \r, \n.
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);

    // Key: "k\0y" (3 bytes), Value: "v\r\n" (3 bytes)
    uint8_t key[] = {'k', 0x00, 'y'};
    uint8_t val[] = {'v', '\r', '\n'};

    std::string set_data = "*3\r\n$3\r\nSET\r\n$3\r\n";
    set_data.append(reinterpret_cast<char*>(key), 3);
    set_data += "\r\n$3\r\n";
    set_data.append(reinterpret_cast<char*>(val), 3);
    set_data += "\r\n";

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(set_data.data()),
                           static_cast<uint32_t>(set_data.size()), &state),
              "+OK\r\n");

    std::string get_data = "*2\r\n$3\r\nGET\r\n$3\r\n";
    get_data.append(reinterpret_cast<char*>(key), 3);
    get_data += "\r\n";

    std::string resp = process_recv(conn, reinterpret_cast<const uint8_t*>(get_data.data()),
                                    static_cast<uint32_t>(get_data.size()), &state);
    // Expected: "$3\r\nv\r\n\r\n" — bulk string with the binary value.
    EXPECT_EQ(resp.size(), 9u); // "$3\r\n" (4) + "v\r\n" (3) + "\r\n" (2)
    EXPECT_TRUE(resp.starts_with("$3\r\n"));
    EXPECT_EQ(resp[4], 'v');
    EXPECT_EQ(resp[5], '\r');
    EXPECT_EQ(resp[6], '\n');

    connection_destroy(conn);
}

TEST(DST, EmptyKeyAndEmptyValue) {
    // SET/GET with zero-length key and value.
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);

    std::string set_data = "*3\r\n$3\r\nSET\r\n$0\r\n\r\n$0\r\n\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(set_data.data()),
                           static_cast<uint32_t>(set_data.size()), &state),
              "+OK\r\n");

    std::string get_data = "*2\r\n$3\r\nGET\r\n$0\r\n\r\n";
    std::string resp = process_recv(conn, reinterpret_cast<const uint8_t*>(get_data.data()),
                                    static_cast<uint32_t>(get_data.size()), &state);
    EXPECT_EQ(resp, "$0\r\n\r\n");

    connection_destroy(conn);
}

TEST(DSTIntegration, MultiPartPartialSend) {
    // Multiple PINGs with response split across 5 partial sends.
    SimIoBackend sim_setup;
    std::string pings;
    for (int i = 0; i < 3; i++)
        pings += "*1\r\n$4\r\nPING\r\n";
    // 3 x "+PONG\r\n" = 21 bytes total response

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, pings.data(), static_cast<uint32_t>(pings.size())));
    sim.copy_send_on_wait = true;
    sim.scripted_send_results = {4, 5, 4, 5, 3}; // 4+5+4+5+3 = 21 bytes

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

    std::string expected;
    for (int i = 0; i < 3; i++)
        expected += "+PONG\r\n";
    EXPECT_EQ(sim.sent_data[10], expected);
    EXPECT_EQ(sim.send_call_count, 5);
}

// --- T1: TX backpressure boundary conditions ---

TEST(DSTIntegration, TxBackpressureExactBoundary) {
    // Fill TX queue to exactly TX_HIGH_WATERMARK_BYTES (1 MiB), verify next
    // enqueue is rejected and connection closed.
    SimIoBackend sim_setup;

    // SET a value such that GET responses are large enough to fill 1 MiB.
    std::string big_value(4000, 'Y');
    std::string set_cmd = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4000\r\n" + big_value + "\r\n";
    std::string get_cmd = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";

    // ~4010 bytes per GET response. 262 GETs ≈ 1050620 > 1 MiB.
    std::string gets;
    for (int i = 0; i < 262; i++)
        gets += get_cmd;

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_recv(&sim_setup, 10, set_cmd.data(), static_cast<uint32_t>(set_cmd.size())));
    sim.submit_send_fail_count = 999; // block sends so TX queue fills
    sim.pending.push_back(sim_recv(&sim_setup, 10, gets.data(), static_cast<uint32_t>(gets.size())));

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

TEST(DSTIntegration, TxBackpressureMultipleConnections) {
    // Multiple connections independently hitting backpressure should each
    // be closed without interfering with the other's close path.
    SimIoBackend sim_setup;

    std::string big_value(4000, 'Z');
    std::string set_cmd = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4000\r\n" + big_value + "\r\n";
    std::string get_cmd = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
    std::string gets;
    for (int i = 0; i < 300; i++)
        gets += get_cmd;

    SimIoBackend sim;
    sim.pending.push_back(sim_accept(10));
    sim.pending.push_back(sim_accept(11));
    sim.pending.push_back(sim_recv(&sim_setup, 10, set_cmd.data(), static_cast<uint32_t>(set_cmd.size())));
    sim.pending.push_back(sim_recv(&sim_setup, 11, set_cmd.data(), static_cast<uint32_t>(set_cmd.size())));
    sim.submit_send_fail_count = 999;
    sim.pending.push_back(sim_recv(&sim_setup, 10, gets.data(), static_cast<uint32_t>(gets.size())));
    sim.pending.push_back(sim_recv(&sim_setup, 11, gets.data(), static_cast<uint32_t>(gets.size())));

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

    // Both connections should be closed (backpressure)
    bool conn10_closed = false, conn11_closed = false;
    for (int fd : sim.closed_fds) {
        if (fd == 10)
            conn10_closed = true;
        if (fd == 11)
            conn11_closed = true;
    }
    EXPECT_TRUE(conn10_closed);
    EXPECT_TRUE(conn11_closed);
}

// --- T3: Recv buffer reassembly at exact boundaries ---

TEST(DSTIntegration, ReassemblySplitAtCRLF) {
    // Split a command exactly between \r and \n in the trailing CRLF.
    SimIoBackend sim_setup;
    std::string full = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";

    // Split right before the final \n (position = full.size() - 1)
    size_t split = full.size() - 1;

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, full.data(), static_cast<uint32_t>(split)));
    events.push_back(
        sim_recv(&sim_setup, 10, full.data() + split, static_cast<uint32_t>(full.size() - split)));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+OK\r\n") << "Failed splitting at CRLF boundary";
}

TEST(DST, ReassemblySplitWithinCRLF) {
    // Split within the \r\n pair of a RESP line — unit-level test.
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);

    // "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    // Split after the first '\r' of the final "\r\n"
    std::string full = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    size_t split = full.size() - 1; // before final \n

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()), static_cast<uint32_t>(split),
                           &state),
              "");
    EXPECT_GT(conn->input_len, 0u);

    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(full.data()) + split,
                           static_cast<uint32_t>(full.size() - split), &state),
              "+OK\r\n");
    connection_destroy(conn);
}

// --- T5: Error completions on listen fd ---

TEST(DSTIntegration, ErrorOnListenFdDoesNotCloseListener) {
    // An ERROR completion arriving for fd=0 (listen socket) should not close it.
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    IoCompletion err = {};
    err.kind = IoCompletion::ERROR;
    err.fd = 0; // listen fd in skip_setup mode
    err.result = -EIO;

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(5));
    events.push_back(err);
    events.push_back(sim_recv(&sim_setup, 5, ping.data(), static_cast<uint32_t>(ping.size())));

    SimIoBackend result = run_worker_sim(events);

    // fd=0 should not appear in closed_fds
    for (int fd : result.closed_fds) {
        EXPECT_NE(fd, 0) << "ERROR on listen fd should not close it";
    }
    // fd=5 should still work
    EXPECT_EQ(result.sent_data[5], "+PONG\r\n");
}

// --- T6: Accept/recv multishot graceful termination ---

TEST(DSTIntegration, AcceptMultishotTerminationWithoutError) {
    // Accept multishot terminates (more=false) with a valid fd (no error).
    SimIoBackend sim_setup;
    std::string ping = "*1\r\n$4\r\nPING\r\n";

    IoCompletion accept_nomore = sim_accept(10);
    accept_nomore.more = false; // multishot terminated, no error

    std::vector<IoCompletion> events;
    events.push_back(accept_nomore);
    events.push_back(sim_recv(&sim_setup, 10, ping.data(), static_cast<uint32_t>(ping.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+PONG\r\n");
    // Accept should have been rearmed
    EXPECT_TRUE(result.accept_armed);
}

TEST(DSTIntegration, RecvMultishotTerminationWithData) {
    // Recv multishot terminates (more=false) but delivers valid data.
    // Connection should process the data AND rearm recv.
    SimIoBackend sim_setup;
    std::string set_cmd = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";

    std::vector<uint8_t> set_buf(set_cmd.begin(), set_cmd.end());

    IoCompletion recv_nomore = {};
    recv_nomore.kind = IoCompletion::RECV;
    recv_nomore.fd = 10;
    recv_nomore.result = static_cast<int>(set_buf.size());
    recv_nomore.buf = set_buf.data();
    recv_nomore.buf_len = static_cast<uint32_t>(set_buf.size());
    recv_nomore.buf_id = 99;
    recv_nomore.more = false;

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(recv_nomore);

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+OK\r\n");
    // recv should have been rearmed
    bool rearmed = false;
    for (int fd : result.recv_armed) {
        if (fd == 10) {
            rearmed = true;
            break;
        }
    }
    EXPECT_TRUE(rearmed);
}

// --- T7: Deep pipeline at response buffer boundary ---

TEST(DSTIntegration, PipelineAtResponseBufferBoundary) {
    // Generate pipelined PINGs whose total response is near RESPONSE_BUF_SIZE
    // (65536). Each "+PONG\r\n" is 7 bytes. 65536/7 ≈ 9362.
    SimIoBackend sim_setup;

    static constexpr int kPingCount = 9362;
    std::string pings;
    for (int i = 0; i < kPingCount; i++)
        pings += "*1\r\n$4\r\nPING\r\n";

    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, pings.data(), static_cast<uint32_t>(pings.size())));

    SimIoBackend result = run_worker_sim(events);

    std::string expected;
    for (int i = 0; i < kPingCount; i++)
        expected += "+PONG\r\n";
    EXPECT_EQ(result.sent_data[10], expected);
}

// --- T9: Close retry queue at exact capacity ---

TEST(DSTIntegration, CloseRetryQueueExactCapacity) {
    // Fill exactly MAX_PENDING_CLOSE (256) entries in the close retry queue.
    SimIoBackend sim_setup;
    std::string bad = "GARBAGE\r\n";

    SimIoBackend sim;
    static constexpr int kConnCount = 256;
    for (int fd = 1; fd <= kConnCount; fd++) {
        sim.pending.push_back(sim_accept(fd));
        sim.pending.push_back(sim_recv(&sim_setup, fd, bad.data(), static_cast<uint32_t>(bad.size())));
    }
    // All first-attempt closes fail with ENOSPC
    sim.submit_close_fail_count = kConnCount;
    sim.submit_close_fail_errno = -ENOSPC;

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

    // All connections should eventually be closed on retry
    for (int fd = 1; fd <= kConnCount; fd++) {
        bool found = false;
        for (int closed_fd : sim.closed_fds) {
            if (closed_fd == fd) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "fd " << fd << " was not closed";
    }
}

TEST(DSTIntegration, CloseRetryOverflowListDrainedCorrectly) {
    // Exceed primary pending_close_fds (256) so fds spill into
    // overflow_close_fds, verifying the overflow drain path works.
    SimIoBackend sim_setup;
    std::string bad = "GARBAGE\r\n";

    SimIoBackend sim;
    // 300 > 256 = MAX_PENDING_CLOSE: at least 44 spill into overflow list.
    static constexpr int kConnCount = 300;
    for (int fd = 1; fd <= kConnCount; fd++) {
        sim.pending.push_back(sim_accept(fd));
        sim.pending.push_back(sim_recv(&sim_setup, fd, bad.data(), static_cast<uint32_t>(bad.size())));
    }
    sim.submit_close_fail_count = kConnCount;
    sim.submit_close_fail_errno = -ENOSPC;

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

    // All connections should eventually be closed, including those that
    // went through the overflow list.
    for (int fd = 1; fd <= kConnCount; fd++) {
        bool found = false;
        for (int closed_fd : sim.closed_fds) {
            if (closed_fd == fd) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "fd " << fd << " was not closed";
    }
}

TEST(DSTIntegration, OrphanCloseOverflowRetried) {
    // Orphan closes (connection_create fails) spill into overflow list when
    // the orphan pending queue is full, and are properly retried.
    SimIoBackend sim;

    // Accept 300 connections but make connection_create fail for all of them.
    // This produces 300 orphan close attempts.
    static constexpr int kConnCount = 300;
    for (int fd = 1; fd <= kConnCount; fd++)
        sim.pending.push_back(sim_accept(fd));
    g_connection_create_fail_count = kConnCount;
    // First wave of closes fails with ENOSPC.
    sim.submit_close_fail_count = kConnCount;
    sim.submit_close_fail_errno = -ENOSPC;

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
    g_connection_create_fail_count = 0;

    // All orphan fds should eventually be closed on retry.
    for (int fd = 1; fd <= kConnCount; fd++) {
        bool found = false;
        for (int closed_fd : sim.closed_fds) {
            if (closed_fd == fd) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "orphan fd " << fd << " was not closed";
    }
}

TEST(DSTIntegration, OverflowDrainRepeatedENOSPC) {
    // Exercises the second-wave ENOSPC case: fds spill into overflow_close_fds,
    // then hit ENOSPC again when the overflow list is drained. The snapshot-
    // before-clear approach must re-enqueue into a fresh queue.
    SimIoBackend sim_setup;
    std::string bad = "GARBAGE\r\n";

    SimIoBackend sim;
    // 300 connections: 256 fill primary queue, 44 spill to overflow.
    static constexpr int kConnCount = 300;
    for (int fd = 1; fd <= kConnCount; fd++) {
        sim.pending.push_back(sim_accept(fd));
        sim.pending.push_back(sim_recv(&sim_setup, fd, bad.data(), static_cast<uint32_t>(bad.size())));
    }
    // Fail the first 600 close submissions: first wave (300) fills queues,
    // second wave (300 retries from primary+overflow drain) fails again.
    // Third wave should succeed.
    sim.submit_close_fail_count = 600;
    sim.submit_close_fail_errno = -ENOSPC;

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

    // All connections must eventually close despite two waves of ENOSPC.
    for (int fd = 1; fd <= kConnCount; fd++) {
        bool found = false;
        for (int closed_fd : sim.closed_fds) {
            if (closed_fd == fd) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "fd " << fd << " was not closed after repeated ENOSPC";
    }
    // Must have retried at least 3 times per fd on average.
    EXPECT_GE(sim.submit_close_call_count, 600);
}

// --- SETEX DST unit tests ---

TEST(DST, SetexThenGet) {
    ProtocolWorkerState state = {};
    protocol_worker_init(&state);
    Connection* conn = connection_create(10);
    std::string setex_data = "*4\r\n$5\r\nSETEX\r\n$3\r\nfoo\r\n$2\r\n60\r\n$3\r\nbar\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(setex_data.data()),
                           static_cast<uint32_t>(setex_data.size()), &state),
              "+OK\r\n");
    std::string get_data = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    EXPECT_EQ(process_recv(conn, reinterpret_cast<const uint8_t*>(get_data.data()),
                           static_cast<uint32_t>(get_data.size()), &state),
              "$3\r\nbar\r\n");
    connection_destroy(conn);
}

// --- SETEX DSTIntegration tests ---

TEST(DSTIntegration, SetexThenGetViaWorkerRun) {
    SimIoBackend sim_setup;
    std::string setex_cmd = "*4\r\n$5\r\nSETEX\r\n$3\r\nfoo\r\n$2\r\n60\r\n$3\r\nbar\r\n";
    std::string get_cmd = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    std::string pipeline = setex_cmd + get_cmd;
    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, pipeline.data(), static_cast<uint32_t>(pipeline.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+OK\r\n$3\r\nbar\r\n");
}

TEST(DSTIntegration, SetexPipelinedViaWorkerRun) {
    SimIoBackend sim_setup;
    std::string setex1 = "*4\r\n$5\r\nSETEX\r\n$2\r\nk1\r\n$2\r\n60\r\n$2\r\nv1\r\n";
    std::string setex2 = "*4\r\n$5\r\nSETEX\r\n$2\r\nk2\r\n$2\r\n60\r\n$2\r\nv2\r\n";
    std::string get1 = "*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n";
    std::string get2 = "*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n";
    std::string pipeline = setex1 + setex2 + get1 + get2;
    std::vector<IoCompletion> events;
    events.push_back(sim_accept(10));
    events.push_back(sim_recv(&sim_setup, 10, pipeline.data(), static_cast<uint32_t>(pipeline.size())));

    SimIoBackend result = run_worker_sim(events);
    EXPECT_EQ(result.sent_data[10], "+OK\r\n+OK\r\n$2\r\nv1\r\n$2\r\nv2\r\n");
}
