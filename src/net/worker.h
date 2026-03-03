// worker.h — Per-core worker event loop.

#pragma once

#include "io.h"
#include <atomic>
#include <cstdint>

struct KvStore;

struct WorkerConfig {
    int cpu_id;
    uint16_t port;
    IoOps ops;
    IoBackend* backend;
    std::atomic<bool>* running;
    KvStore* shared_store = nullptr;
    uint32_t worker_id = 0;
    uint32_t worker_count = 1;
    // When true, skip core pinning and listen socket creation (for DST).
    bool skip_setup = false;
    int listen_fd = -1; // pre-created listen fd when skip_setup is true
    int exit_code = 0;
};

int worker_run(WorkerConfig* config);
