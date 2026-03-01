// server.cpp — Spawns worker threads, handles shutdown signals.

#include "server.h"
#include "base/assert.h"
#include "connection.h"
#include "io_uring_backend.h"
#include "kv/shared_kv_store.h"
#include "worker.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <sys/resource.h>
#include <unistd.h>

static std::atomic<bool> g_running{true};

//  Signal handler for SIGINT/SIGTERM. Must only use async-signal-safe functions
// (see signal-safety(7)). write(2) is async-signal-safe; fprintf is NOT.
// The atomic store to g_running is safe because std::atomic<bool> with
// relaxed ordering is a single store instruction on x86/ARM.
static void signal_handler(int sig) {
    (void)sig;
    g_running.store(false, std::memory_order_relaxed);
    const char msg[] = "\ndalahash: shutting down...\n";
    (void)write(STDERR_FILENO, msg, sizeof(msg) - 1);
}

static void* worker_thread_fn(void* arg) {
    worker_run(static_cast<WorkerConfig*>(arg));
    return nullptr;
}

static constexpr uint32_t RING_SIZE = 4096;
static constexpr uint32_t BUF_COUNT = 1024; // provided buffers per worker
static constexpr uint32_t BUF_SIZE = 4096;
static constexpr uint64_t PROCESS_FD_RESERVE = 64;
static constexpr uint64_t PER_WORKER_FD_OVERHEAD = 4;

struct WorkerResourcePlan {
    int num_workers;
    uint32_t fixed_files_per_worker;
    unsigned long long nofile_soft_limit;
};

//  Derive a startup plan that fits inside the process RLIMIT_NOFILE soft limit.
//
// The hot-path requirement is driven by the io_uring fixed-file table:
// each worker registers one sparse table for accepted client sockets, plus a
// small number of always-open process/worker fds (listen sockets, ring fds,
// stdio, etc). We therefore:
//   1) reserve a small process-wide fd cushion first
//   2) ensure each worker gets at least one usable fixed-file slot
//   3) reduce worker count if the requested count cannot satisfy (2)
//   4) divide the remaining fd budget evenly across the final worker count
//
// This keeps startup deterministic on low-limit hosts: the process either
// starts with an explicit cap, or fails early when even one worker cannot fit.
static WorkerResourcePlan plan_worker_resources(int requested_workers) {
    ASSERT(requested_workers > 0, "requested_workers must be positive");

    WorkerResourcePlan plan = {
        .num_workers = requested_workers,
        .fixed_files_per_worker = static_cast<uint32_t>(MAX_CONNECTIONS),
        .nofile_soft_limit = 0,
    };

    struct rlimit nofile = {};
    if (getrlimit(RLIMIT_NOFILE, &nofile) != 0)
        return plan;
    if (nofile.rlim_cur == RLIM_INFINITY)
        return plan;

    uint64_t soft_limit = static_cast<uint64_t>(nofile.rlim_cur);
    plan.nofile_soft_limit = static_cast<unsigned long long>(soft_limit);

    // Keep a small process-wide reserve so startup and normal runtime fds
    // outside the fixed-file tables do not consume the full soft limit.
    uint64_t budget = soft_limit;
    if (budget > PROCESS_FD_RESERVE) {
        budget -= PROCESS_FD_RESERVE;
    } else {
        budget = 0;
    }

    // Every worker needs its non-client fd overhead plus at least one fixed
    // file slot. If we cannot afford that minimum, startup should fail early.
    const uint64_t min_worker_cost = PER_WORKER_FD_OVERHEAD + 1;
    if (budget < min_worker_cost) {
        plan.num_workers = 0;
        plan.fixed_files_per_worker = 0;
        return plan;
    }

    // Clamp the worker count before computing per-worker capacity. This favors
    // keeping all remaining workers identical rather than starting a subset
    // with oversized tables and failing later workers during backend init.
    uint64_t max_workers = budget / min_worker_cost;
    if (max_workers == 0)
        max_workers = 1;
    if (static_cast<uint64_t>(plan.num_workers) > max_workers)
        plan.num_workers = static_cast<int>(max_workers);

    // Split the remaining budget evenly across the final worker count, then
    // convert each worker's slice into the number of io_uring fixed-file slots
    // left after subtracting that worker's non-client fd overhead.
    uint64_t per_worker_budget = budget / static_cast<uint64_t>(plan.num_workers);
    uint64_t fixed_files = 1;
    if (per_worker_budget > PER_WORKER_FD_OVERHEAD)
        fixed_files = per_worker_budget - PER_WORKER_FD_OVERHEAD;
    if (fixed_files > static_cast<uint64_t>(MAX_CONNECTIONS))
        fixed_files = static_cast<uint64_t>(MAX_CONNECTIONS);
    plan.fixed_files_per_worker = static_cast<uint32_t>(fixed_files);
    return plan;
}

int server_start(const ServerConfig* config) {
    ASSERT(config != nullptr, "server_start requires config");
    if (!config)
        return 1;

    int num_workers = config->num_workers;
    if (num_workers <= 0) {
        //  sysconf(3) _SC_NPROCESSORS_ONLN — number of CPUs currently online.
        // We default to one worker per core for the thread-per-core model.
        num_workers = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
        if (num_workers <= 0)
            num_workers = 1;
    }
    ASSERT(num_workers > 0, "num_workers must be positive");

    WorkerResourcePlan resource_plan = plan_worker_resources(num_workers);
    if (resource_plan.num_workers <= 0 || resource_plan.fixed_files_per_worker == 0) {
        std::fprintf(stderr, "dalahash: RLIMIT_NOFILE soft limit %llu is too low to start any worker\n",
                     resource_plan.nofile_soft_limit);
        return 1;
    }
    if (resource_plan.num_workers != num_workers) {
        std::fprintf(stderr,
                     "dalahash: reducing workers from %d to %d to fit RLIMIT_NOFILE soft limit %llu\n",
                     num_workers, resource_plan.num_workers, resource_plan.nofile_soft_limit);
        num_workers = resource_plan.num_workers;
    }
    if (resource_plan.fixed_files_per_worker < static_cast<uint32_t>(MAX_CONNECTIONS)) {
        std::fprintf(stderr,
                     "dalahash: limiting fixed-file slots to %u per worker (RLIMIT_NOFILE soft limit %llu)\n",
                     resource_plan.fixed_files_per_worker, resource_plan.nofile_soft_limit);
    }

    std::fprintf(stderr, "dalahash: starting %d workers on port %d\n", num_workers, config->port);

    //  sigaction(2) — install signal handlers for clean shutdown.
    // sa_flags = 0: deliberately no SA_RESTART. Without SA_RESTART, blocked
    // syscalls return -EINTR when a signal arrives. This is critical for
    // io_uring: when io_uring_submit_and_wait_timeout is blocked waiting for
    // CQEs and SIGINT arrives, the -EINTR return lets the worker thread check
    // the g_running flag and exit cleanly. With SA_RESTART, the wait would
    // silently restart and the worker would never see the shutdown signal.
    struct sigaction sa = {};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    ASSERT(sigaction(SIGINT, &sa, nullptr) == 0, "sigaction(SIGINT) failed");
    ASSERT(sigaction(SIGTERM, &sa, nullptr) == 0, "sigaction(SIGTERM) failed");

    g_running.store(true, std::memory_order_relaxed);

    auto* configs =
        static_cast<WorkerConfig*>(std::calloc(static_cast<size_t>(num_workers), sizeof(WorkerConfig)));
    auto* threads = static_cast<pthread_t*>(std::calloc(static_cast<size_t>(num_workers), sizeof(pthread_t)));
    if (!configs || !threads) {
        std::fprintf(stderr, "dalahash: alloc failed\n");
        std::free(configs);
        std::free(threads);
        return 1;
    }

    KvStoreConfig store_cfg = {
        .capacity_bytes = config->store_bytes,
        .shard_count = 0,
        .buckets_per_shard = 0,
        .worker_count = static_cast<uint32_t>(num_workers),
    };
    KvStore* shared_store = kv_store_create(&store_cfg);
    if (!shared_store) {
        std::fprintf(stderr, "dalahash: shared store init failed\n");
        std::free(configs);
        std::free(threads);
        return 1;
    }

    for (int i = 0; i < num_workers; i++) {
        IoBackend* backend =
            io_uring_backend_create(RING_SIZE, BUF_COUNT, BUF_SIZE, resource_plan.fixed_files_per_worker);
        if (!backend) {
            std::fprintf(stderr, "dalahash: backend create failed for worker %d\n", i);
            g_running.store(false, std::memory_order_relaxed);
            break;
        }
        configs[i] = {.cpu_id = i,
                      .port = config->port,
                      .ops = io_uring_ops(),
                      .backend = backend,
                      .running = &g_running,
                      .shared_store = shared_store,
                      .worker_id = static_cast<uint32_t>(i),
                      .worker_count = static_cast<uint32_t>(num_workers)};

        int ret = pthread_create(&threads[i], nullptr, worker_thread_fn, &configs[i]);
        if (ret != 0) {
            std::fprintf(stderr, "dalahash: thread create failed for worker %d: %s\n", i, std::strerror(ret));
            configs[i].ops.destroy(configs[i].backend);
            configs[i].backend = nullptr;
            g_running.store(false, std::memory_order_relaxed);
            break;
        }
    }

    for (int i = 0; i < num_workers; i++) {
        if (threads[i]) {
            int join_ret = pthread_join(threads[i], nullptr);
            ASSERT(join_ret == 0, "pthread_join failed");
        }
    }

    kv_store_destroy(shared_store);
    std::free(configs);
    std::free(threads);
    std::fprintf(stderr, "dalahash: stopped\n");
    return 0;
}
