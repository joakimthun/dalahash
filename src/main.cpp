// main.cpp — Entry point.

#include "base/assert.h"
#include "cli.h"
#include "net/server.h"

#include <cstdio>

int main(int argc, char** argv) {
    ASSERT(argc >= 0, "argc must be non-negative");
    ASSERT(argv != nullptr || argc == 0, "argv is null with non-zero argc");
    ServerConfig config = {.port = 6379, .num_workers = 0, .store_bytes = 256ull << 20, .store_max_items = 0};

    const char* error_message = nullptr;
    const char* error_arg = nullptr;
    CliParseResult parse_result = cli_parse_args(argc, argv, &config, &error_message, &error_arg);
    if (parse_result == CliParseResult::HELP) {
        std::printf("%s\n", cli_usage());
        return 0;
    }
    if (parse_result == CliParseResult::ERROR) {
        if (error_arg)
            std::fprintf(stderr, "%s: %s\n", error_message, error_arg);
        else
            std::fprintf(stderr, "%s\n", error_message ? error_message : "Invalid arguments");
        return 1;
    }

    return server_start(&config);
}
