// main.cpp — Entry point.

#include "net/server.h"
#include "base/assert.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

int main(int argc, char **argv) {
    ASSERT(argc >= 0, "argc must be non-negative");
    ASSERT(argv != nullptr || argc == 0, "argv is null with non-zero argc");
    ServerConfig config = {.port = 6379, .num_workers = 0, .store_bytes = 256ull << 20};

    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            char *end = nullptr;
            unsigned long parsed = std::strtoul(argv[++i], &end, 10);
            if (!end || *end != '\0' || parsed == 0 || parsed > 65535ul) {
                std::fprintf(stderr, "Invalid --port value\n");
                return 1;
            }
            config.port = static_cast<uint16_t>(parsed);
        } else if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            config.num_workers = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--store-bytes") == 0 && i + 1 < argc) {
            char *end = nullptr;
            unsigned long long parsed = std::strtoull(argv[++i], &end, 10);
            if (!end || *end != '\0' || parsed == 0) {
                std::fprintf(stderr, "Invalid --store-bytes value\n");
                return 1;
            }
            config.store_bytes = static_cast<uint64_t>(parsed);
        } else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            std::printf("Usage: dalahash [--port PORT] [--workers N] [--store-bytes BYTES]\n");
            return 0;
        } else {
            std::fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            return 1;
        }
    }

    return server_start(&config);
}
