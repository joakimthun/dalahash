/* main.cpp — Entry point. Usage: dalahash [--port PORT] [--workers N] */

#include "net/server.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

int main(int argc, char **argv) {
    ServerConfig config = {.port = 6379, .num_workers = 0};

    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            config.num_workers = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            std::printf("Usage: dalahash [--port PORT] [--workers N]\n");
            return 0;
        } else {
            std::fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            return 1;
        }
    }

    return server_start(&config);
}
