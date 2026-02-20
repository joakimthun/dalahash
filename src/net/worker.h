/* worker.h — Per-core worker event loop. */

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
};

int worker_run(WorkerConfig *config);
