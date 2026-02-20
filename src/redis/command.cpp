/* command.cpp — Command dispatch: verb lookup, store access, response encoding. */

#include "command.h"
#include <cstring>

/* Case-insensitive argument match via clearing bit 5 (0x20): 'a'→'A', 'g'→'G'.
 * Works for ASCII letters only, which covers all Redis command verbs. */
static bool arg_matches(const RespArg *arg, const char *expected, uint32_t expected_len) {
    if (arg->len != expected_len) return false;
    for (uint32_t i = 0; i < expected_len; i++) {
        if ((arg->data[i] & 0xDF) != static_cast<uint8_t>(expected[i])) return false;
    }
    return true;
}

/* Execute one parsed RESP command and write its RESP response into out_buf[].
 * Returns the number of bytes written. cmd->args[i].data points into the
 * receive buffer (zero-copy from the parser), so key/value byte ranges are
 * available without any additional allocation until this function returns. */
uint32_t command_execute(const RespCommand *cmd, Store *store,
                         uint8_t *out_buf, [[maybe_unused]] uint32_t out_buf_size) {
    if (cmd->argc < 1) return resp_write_error(out_buf, "empty command");

    const RespArg *verb = &cmd->args[0];

    if (arg_matches(verb, "GET", 3)) {
        if (cmd->argc != 2) return resp_write_error(out_buf, "wrong number of arguments for 'get' command");
        /* args[1] points into the receive buffer — construct string_view without copy. */
        std::string_view key(reinterpret_cast<const char *>(cmd->args[1].data), cmd->args[1].len);
        /* store_get returns a pointer into the map; resp_write_bulk copies the
         * value bytes into out_buf for the async send. */
        const std::string *val = store_get(store, key);
        if (!val) return resp_write_null(out_buf);
        return resp_write_bulk(out_buf, reinterpret_cast<const uint8_t *>(val->data()), static_cast<uint32_t>(val->size()));
    }

    if (arg_matches(verb, "SET", 3)) {
        if (cmd->argc != 3) return resp_write_error(out_buf, "wrong number of arguments for 'set' command");
        /* args[1] and args[2] point into the receive buffer. store_set copies
         * both key and value into std::string heap allocations in the map. */
        std::string_view key(reinterpret_cast<const char *>(cmd->args[1].data), cmd->args[1].len);
        std::string_view value(reinterpret_cast<const char *>(cmd->args[2].data), cmd->args[2].len);
        store_set(store, key, value);
        return resp_write_ok(out_buf);
    }

    if (arg_matches(verb, "PING", 4)) return resp_write_pong(out_buf);

    /* Stub for redis-cli handshake — returns empty array. */
    if (arg_matches(verb, "COMMAND", 7)) {
        std::memcpy(out_buf, "*0\r\n", 4);
        return 4;
    }

    return resp_write_error(out_buf, "unknown command");
}
