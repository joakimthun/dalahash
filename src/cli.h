#pragma once

#include "base/assert.h"
#include "net/server.h"

#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>

enum class CliParseResult : uint8_t {
    OK = 0,
    HELP = 1,
    ERROR = 2,
};

inline const char* cli_usage() {
    return "Usage: dalahash [--port PORT] [--workers N] [--store-bytes BYTES] [--store-max-items N]";
}

inline bool cli_parse_port(const char* value, uint16_t* out) {
    if (!value || !out)
        return false;

    errno = 0;
    char* end = nullptr;
    unsigned long parsed = std::strtoul(value, &end, 10);
    if (errno != 0 || !end || *end != '\0' || parsed == 0 || parsed > 65535ul)
        return false;

    *out = static_cast<uint16_t>(parsed);
    return true;
}

inline bool cli_parse_workers(const char* value, int* out) {
    if (!value || !out)
        return false;

    errno = 0;
    char* end = nullptr;
    long parsed = std::strtol(value, &end, 10);
    if (errno != 0 || !end || *end != '\0' || parsed < 0 || parsed > INT_MAX)
        return false;

    *out = static_cast<int>(parsed);
    return true;
}

inline bool cli_parse_store_bytes(const char* value, uint64_t* out) {
    if (!value || !out)
        return false;
    if (value[0] == '-')
        return false;

    errno = 0;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (errno != 0 || !end || *end != '\0' || parsed == 0)
        return false;

    *out = static_cast<uint64_t>(parsed);
    return true;
}

inline bool cli_parse_store_max_items(const char* value, uint64_t* out) {
    if (!value || !out)
        return false;
    if (value[0] == '-')
        return false;

    errno = 0;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (errno != 0 || !end || *end != '\0' || parsed == 0)
        return false;

    *out = static_cast<uint64_t>(parsed);
    return true;
}

inline CliParseResult cli_parse_args(int argc, char** argv, ServerConfig* config, const char** error_message,
                                     const char** error_arg = nullptr) {
    ASSERT(argc >= 0, "argc must be non-negative");
    ASSERT(argv != nullptr || argc == 0, "argv is null with non-zero argc");
    ASSERT(config != nullptr, "cli_parse_args requires config");
    ASSERT(error_message != nullptr, "cli_parse_args requires error output");
    if (!config || !error_message)
        return CliParseResult::ERROR;

    *error_message = nullptr;
    if (error_arg)
        *error_arg = nullptr;

    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--port") == 0) {
            if (i + 1 >= argc) {
                *error_message = "Invalid --port value";
                return CliParseResult::ERROR;
            }
            if (!cli_parse_port(argv[++i], &config->port)) {
                *error_message = "Invalid --port value";
                return CliParseResult::ERROR;
            }
            continue;
        }

        if (std::strcmp(argv[i], "--workers") == 0) {
            if (i + 1 >= argc) {
                *error_message = "Invalid --workers value";
                return CliParseResult::ERROR;
            }
            if (!cli_parse_workers(argv[++i], &config->num_workers)) {
                *error_message = "Invalid --workers value";
                return CliParseResult::ERROR;
            }
            continue;
        }

        if (std::strcmp(argv[i], "--store-bytes") == 0) {
            if (i + 1 >= argc) {
                *error_message = "Invalid --store-bytes value";
                return CliParseResult::ERROR;
            }
            if (!cli_parse_store_bytes(argv[++i], &config->store_bytes)) {
                *error_message = "Invalid --store-bytes value";
                return CliParseResult::ERROR;
            }
            continue;
        }

        if (std::strcmp(argv[i], "--store-max-items") == 0) {
            if (i + 1 >= argc) {
                *error_message = "Invalid --store-max-items value";
                return CliParseResult::ERROR;
            }
            if (!cli_parse_store_max_items(argv[++i], &config->store_max_items)) {
                *error_message = "Invalid --store-max-items value";
                return CliParseResult::ERROR;
            }
            continue;
        }

        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0)
            return CliParseResult::HELP;

        *error_message = "Unknown argument";
        if (error_arg)
            *error_arg = argv[i];
        return CliParseResult::ERROR;
    }

    return CliParseResult::OK;
}
