// server.h — Spawns one worker per CPU core.

#pragma once

#include <cstdint>

struct ServerConfig {
    uint16_t port;      // default: 6379
    int num_workers;    // 0 = auto-detect
    uint64_t store_bytes; // shared store memory cap
};

int server_start(const ServerConfig *config);
