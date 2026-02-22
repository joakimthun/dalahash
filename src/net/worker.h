// worker.h — Per-core worker event loop.

#pragma once

#include "io.h"
#include <atomic>
#include <cstdint>

struct WorkerConfig {
    int cpu_id;
    uint16_t port;
    IoOps ops;
    IoBackend *backend;
    std::atomic<bool> *running;
    // When true, skip core pinning and listen socket creation (for DST).
    bool skip_setup = false;
    int listen_fd = -1; // pre-created listen fd when skip_setup is true
};

int worker_run(WorkerConfig *config);
