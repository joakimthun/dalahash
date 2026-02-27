// command.cpp — Command dispatch: verb lookup, store access, response encoding.

#include "command.h"
#include "base/assert.h"
#include <cstdio>
#include <cstring>

//  Case-insensitive argument match via clearing bit 5 (0x20): 'a'→'A', 'g'→'G'.
// Works for ASCII letters only, which covers all Redis command verbs.
static bool arg_matches(const RespArg* arg, const char* expected, uint32_t expected_len) {
    ASSERT(arg != nullptr, "arg_matches requires arg");
    ASSERT(expected != nullptr, "arg_matches requires expected literal");
    if (arg->len != expected_len)
        return false;
    for (uint32_t i = 0; i < expected_len; i++) {
        if ((arg->data[i] & 0xDF) != static_cast<uint8_t>(expected[i]))
            return false;
    }
    return true;
}

static uint32_t decimal_len_u32(uint32_t v) {
    uint32_t digits = 1;
    while (v >= 10) {
        v /= 10;
        digits++;
    }
    return digits;
}

static uint32_t write_error_bounded(uint8_t* out, uint32_t out_buf_size, const char* msg) {
    ASSERT(out != nullptr || out_buf_size == 0, "write_error_bounded null out with non-zero size");
    ASSERT(msg != nullptr, "write_error_bounded requires error message");
    if (!out || !msg)
        return 0;
    if (out_buf_size == 0)
        return 0;
    int n = std::snprintf(reinterpret_cast<char*>(out), out_buf_size, "-ERR %s\r\n", msg);
    if (n <= 0)
        return 0;
    if (static_cast<uint32_t>(n) >= out_buf_size)
        return out_buf_size - 1;
    return static_cast<uint32_t>(n);
}

//  Execute one parsed RESP command and write its RESP response into out_buf[].
// Returns the number of bytes written. cmd->args[i].data points into the
// receive buffer (zero-copy from the parser), so key/value byte ranges are
// available without any additional allocation until this function returns.
uint32_t command_execute(const RespCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                         uint32_t out_buf_size) {
    ASSERT(cmd != nullptr, "command_execute requires command");
    ASSERT(store != nullptr, "command_execute requires store");
    ASSERT(out_buf != nullptr || out_buf_size == 0, "command_execute null out with non-zero size");
    if (!cmd || !store || (!out_buf && out_buf_size > 0))
        return 0;
    ASSERT(cmd->argc >= 0, "command argc must be non-negative");
    ASSERT(cmd->argc <= RESP_MAX_ARGS, "command argc exceeds parser bound");

    if (cmd->argc < 1)
        return write_error_bounded(out_buf, out_buf_size, "empty command");

    const RespArg* verb = &cmd->args[0];

    if (arg_matches(verb, "GET", 3)) {
        if (cmd->argc != 2)
            return write_error_bounded(out_buf, out_buf_size, "wrong number of arguments for 'get' command");
        // args[1] points into the receive buffer — construct string_view without copy.
        std::string_view key(reinterpret_cast<const char*>(cmd->args[1].data), cmd->args[1].len);
        StoreValueView val = {};
        if (!store_get_at(store, key, now_ms, &val)) {
            if (out_buf_size < 5)
                return write_error_bounded(out_buf, out_buf_size, "output buffer too small");
            return resp_write_null(out_buf);
        }
        uint32_t val_len = val.len;
        ASSERT(val.data != nullptr || val_len == 0, "store returned null data for non-empty value");
        uint64_t needed = 1ull + decimal_len_u32(val_len) + 2ull + val_len + 2ull;
        if (needed > out_buf_size)
            return write_error_bounded(out_buf, out_buf_size, "response too large");
        return resp_write_bulk(out_buf, val.data, val.len);
    }

    if (arg_matches(verb, "SET", 3)) {
        if (cmd->argc != 3)
            return write_error_bounded(out_buf, out_buf_size, "wrong number of arguments for 'set' command");
        // args[1] and args[2] point into receive buffer; store_set copies bytes.
        std::string_view key(reinterpret_cast<const char*>(cmd->args[1].data), cmd->args[1].len);
        std::string_view value(reinterpret_cast<const char*>(cmd->args[2].data), cmd->args[2].len);
        StoreSetStatus set_status = store_set_at(store, key, value, now_ms);
        if (set_status == StoreSetStatus::OOM)
            return write_error_bounded(out_buf, out_buf_size, "out of memory");
        if (set_status != StoreSetStatus::OK)
            return write_error_bounded(out_buf, out_buf_size, "store failure");
        if (out_buf_size < 5)
            return write_error_bounded(out_buf, out_buf_size, "output buffer too small");
        return resp_write_ok(out_buf);
    }

    if (arg_matches(verb, "PING", 4)) {
        if (out_buf_size < 7)
            return write_error_bounded(out_buf, out_buf_size, "output buffer too small");
        return resp_write_pong(out_buf);
    }

    // Stub for redis-cli handshake — returns empty array.
    if (arg_matches(verb, "COMMAND", 7)) {
        if (out_buf_size < 4)
            return write_error_bounded(out_buf, out_buf_size, "output buffer too small");
        std::memcpy(out_buf, "*0\r\n", 4);
        return 4;
    }

    return write_error_bounded(out_buf, out_buf_size, "unknown command");
}
