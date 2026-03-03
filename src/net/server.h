// server.h — Spawns one worker per CPU core.

#pragma once

#include "io.h"

#include <cstdint>

struct ServerConfig {
    uint16_t port;        // default: 6379
    int num_workers;      // 0 = auto-detect
    uint64_t store_bytes; // shared store memory cap
};

using ServerBackendCreateFn = IoBackend* (*)(uint32_t ring_size, uint32_t buf_count, uint32_t buf_size,
                                             uint32_t max_files);

int server_start(const ServerConfig* config);
int server_start_with_runtime(const ServerConfig* config, const IoOps* ops,
                              ServerBackendCreateFn backend_create, bool skip_worker_setup = false);
