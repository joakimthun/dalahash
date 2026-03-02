// memcached_parse.h — Zero-copy parser for memcached text protocol.
//
// Supports both legacy text commands (get/set/delete/version) and
// meta text commands (mg/ms/md). All McArg pointers reference the
// caller's receive buffer directly — no allocations on any path.

#pragma once

#include <cstdint>

static constexpr int MC_MAX_META_FLAGS = 16;

enum class McCommandType : uint8_t {
    GET,
    SET,
    DELETE,
    VERSION,
    META_GET,
    META_SET,
    META_DELETE,
    META_NOOP,
};

enum class McParseResult : uint8_t {
    OK,
    INCOMPLETE,
    ERROR,
};

struct McArg {
    const uint8_t* data; // zero-copy pointer into recv buffer
    uint32_t len;
};

struct McCommand {
    McCommandType type;
    McArg key;
    McArg value;                         // SET/META_SET data block
    uint32_t client_flags;               // Legacy SET flags field
    uint32_t exptime;                    // Legacy SET exptime (seconds)
    uint32_t bytes;                      // Legacy SET byte count
    McArg meta_flags[MC_MAX_META_FLAGS]; // Meta flag tokens
    int meta_flag_count;
    bool noreply;
};

// Parse one complete memcached command from data[0..len).
//
// On OK:         *cmd is populated, *consumed = bytes forming this command.
// On INCOMPLETE: *consumed = 0, caller should buffer and retry with more data.
// On ERROR:      malformed command.
McParseResult mc_parse(const uint8_t* data, uint32_t len, McCommand* cmd, uint32_t* consumed);
