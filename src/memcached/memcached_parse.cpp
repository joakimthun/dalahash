// memcached_parse.cpp — Zero-copy memcached text protocol parser.

#include "memcached_parse.h"
#include "base/assert.h"

#include <cstring>

// Scan for \r\n in data[start..len). Returns position of \r or len if not found.
static uint32_t find_crlf(const uint8_t* data, uint32_t start, uint32_t len) {
    for (uint32_t i = start; i + 1 < len; i++) {
        if (data[i] == '\r' && data[i + 1] == '\n')
            return i;
    }
    return len;
}

// Parse unsigned 32-bit integer from data[start..end). Returns false on error.
static bool parse_u32(const uint8_t* data, uint32_t start, uint32_t end, uint32_t* out) {
    if (start >= end)
        return false;
    uint64_t val = 0;
    for (uint32_t i = start; i < end; i++) {
        uint8_t ch = data[i];
        if (ch < '0' || ch > '9')
            return false;
        val = val * 10 + (ch - '0');
        if (val > UINT32_MAX)
            return false;
    }
    *out = static_cast<uint32_t>(val);
    return true;
}

// Skip spaces, return new position.
static uint32_t skip_spaces(const uint8_t* data, uint32_t pos, uint32_t end) {
    while (pos < end && data[pos] == ' ')
        pos++;
    return pos;
}

// Read a token (non-space bytes) starting at pos. Returns token as McArg.
static uint32_t read_token(const uint8_t* data, uint32_t pos, uint32_t end, McArg* out) {
    uint32_t start = pos;
    while (pos < end && data[pos] != ' ' && data[pos] != '\r')
        pos++;
    out->data = data + start;
    out->len = pos - start;
    return pos;
}

// Check if token matches expected string (case-insensitive for commands).
static bool token_matches_ci(const McArg* arg, const char* expected, uint32_t expected_len) {
    if (arg->len != expected_len)
        return false;
    for (uint32_t i = 0; i < expected_len; i++) {
        if ((arg->data[i] & 0xDF) != static_cast<uint8_t>(expected[i]))
            return false;
    }
    return true;
}

// Parse legacy "get <key>\r\n" — single key only; multi-key get rejected.
static McParseResult parse_legacy_get(const uint8_t* data, uint32_t line_end, McCommand* cmd, uint32_t pos) {
    pos = skip_spaces(data, pos, line_end);
    if (pos >= line_end)
        return McParseResult::ERROR;
    pos = read_token(data, pos, line_end, &cmd->key);
    if (cmd->key.len == 0)
        return McParseResult::ERROR;
    // Reject multi-key get — trailing tokens after key.
    pos = skip_spaces(data, pos, line_end);
    if (pos < line_end)
        return McParseResult::ERROR;
    cmd->type = McCommandType::GET;
    return McParseResult::OK;
}

// Parse legacy "set <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n"
static McParseResult parse_legacy_set(const uint8_t* data, uint32_t len, uint32_t line_end, McCommand* cmd,
                                      uint32_t pos, uint32_t* consumed) {
    // key
    pos = skip_spaces(data, pos, line_end);
    if (pos >= line_end)
        return McParseResult::ERROR;
    pos = read_token(data, pos, line_end, &cmd->key);
    if (cmd->key.len == 0)
        return McParseResult::ERROR;

    // flags
    pos = skip_spaces(data, pos, line_end);
    McArg flags_tok = {};
    pos = read_token(data, pos, line_end, &flags_tok);
    if (flags_tok.len == 0)
        return McParseResult::ERROR;
    if (!parse_u32(flags_tok.data, 0, flags_tok.len, &cmd->client_flags))
        return McParseResult::ERROR;

    // exptime
    pos = skip_spaces(data, pos, line_end);
    McArg exp_tok = {};
    pos = read_token(data, pos, line_end, &exp_tok);
    if (exp_tok.len == 0)
        return McParseResult::ERROR;
    if (!parse_u32(exp_tok.data, 0, exp_tok.len, &cmd->exptime))
        return McParseResult::ERROR;

    // bytes
    pos = skip_spaces(data, pos, line_end);
    McArg bytes_tok = {};
    pos = read_token(data, pos, line_end, &bytes_tok);
    if (bytes_tok.len == 0)
        return McParseResult::ERROR;
    if (!parse_u32(bytes_tok.data, 0, bytes_tok.len, &cmd->bytes))
        return McParseResult::ERROR;

    // optional noreply — reject any other trailing token
    pos = skip_spaces(data, pos, line_end);
    if (pos < line_end) {
        McArg nr_tok = {};
        pos = read_token(data, pos, line_end, &nr_tok);
        if (nr_tok.len == 7 && token_matches_ci(&nr_tok, "NOREPLY", 7))
            cmd->noreply = true;
        else
            return McParseResult::ERROR;
        // Reject trailing garbage after noreply.
        pos = skip_spaces(data, pos, line_end);
        if (pos < line_end)
            return McParseResult::ERROR;
    }

    // Data block: need line_end + 2 (skip \r\n) + bytes + 2 (\r\n)
    // Use 64-bit to prevent uint32_t overflow with large cmd->bytes.
    uint64_t data_start = static_cast<uint64_t>(line_end) + 2;
    uint64_t data_end = data_start + cmd->bytes;
    if (data_end + 2 > len)
        return McParseResult::INCOMPLETE;
    if (data[data_end] != '\r' || data[data_end + 1] != '\n')
        return McParseResult::ERROR;

    cmd->value.data = data + data_start;
    cmd->value.len = cmd->bytes;
    cmd->type = McCommandType::SET;
    *consumed = static_cast<uint32_t>(data_end + 2);
    return McParseResult::OK;
}

// Parse legacy "delete <key> [noreply]\r\n"
static McParseResult parse_legacy_delete(const uint8_t* data, uint32_t line_end, McCommand* cmd,
                                         uint32_t pos) {
    pos = skip_spaces(data, pos, line_end);
    if (pos >= line_end)
        return McParseResult::ERROR;
    pos = read_token(data, pos, line_end, &cmd->key);
    if (cmd->key.len == 0)
        return McParseResult::ERROR;

    // optional noreply — reject any other trailing token
    pos = skip_spaces(data, pos, line_end);
    if (pos < line_end) {
        McArg nr_tok = {};
        pos = read_token(data, pos, line_end, &nr_tok);
        if (nr_tok.len == 7 && token_matches_ci(&nr_tok, "NOREPLY", 7))
            cmd->noreply = true;
        else
            return McParseResult::ERROR;
        // Reject trailing garbage after noreply.
        pos = skip_spaces(data, pos, line_end);
        if (pos < line_end)
            return McParseResult::ERROR;
    }

    cmd->type = McCommandType::DELETE;
    return McParseResult::OK;
}

// Parse meta flags after key for mg/md commands (no data block).
static void parse_meta_flags(const uint8_t* data, uint32_t pos, uint32_t line_end, McCommand* cmd) {
    cmd->meta_flag_count = 0;
    while (pos < line_end && cmd->meta_flag_count < MC_MAX_META_FLAGS) {
        pos = skip_spaces(data, pos, line_end);
        if (pos >= line_end)
            break;
        McArg flag = {};
        pos = read_token(data, pos, line_end, &flag);
        if (flag.len > 0) {
            cmd->meta_flags[cmd->meta_flag_count] = flag;
            cmd->meta_flag_count++;
        }
    }
}

// Parse "mg <key> [flags...]\r\n"
static McParseResult parse_meta_get(const uint8_t* data, uint32_t line_end, McCommand* cmd, uint32_t pos) {
    pos = skip_spaces(data, pos, line_end);
    if (pos >= line_end)
        return McParseResult::ERROR;
    pos = read_token(data, pos, line_end, &cmd->key);
    if (cmd->key.len == 0)
        return McParseResult::ERROR;

    parse_meta_flags(data, pos, line_end, cmd);
    cmd->type = McCommandType::META_GET;
    return McParseResult::OK;
}

// Parse "ms <key> <datalen> [flags...]\r\n<data>\r\n"
static McParseResult parse_meta_set(const uint8_t* data, uint32_t len, uint32_t line_end, McCommand* cmd,
                                    uint32_t pos, uint32_t* consumed) {
    pos = skip_spaces(data, pos, line_end);
    if (pos >= line_end)
        return McParseResult::ERROR;
    pos = read_token(data, pos, line_end, &cmd->key);
    if (cmd->key.len == 0)
        return McParseResult::ERROR;

    // datalen
    pos = skip_spaces(data, pos, line_end);
    McArg len_tok = {};
    pos = read_token(data, pos, line_end, &len_tok);
    if (len_tok.len == 0)
        return McParseResult::ERROR;
    if (!parse_u32(len_tok.data, 0, len_tok.len, &cmd->bytes))
        return McParseResult::ERROR;

    // remaining tokens are meta flags
    parse_meta_flags(data, pos, line_end, cmd);

    // Data block — use 64-bit to prevent overflow with large cmd->bytes.
    uint64_t data_start = static_cast<uint64_t>(line_end) + 2;
    uint64_t data_end = data_start + cmd->bytes;
    if (data_end + 2 > len)
        return McParseResult::INCOMPLETE;
    if (data[data_end] != '\r' || data[data_end + 1] != '\n')
        return McParseResult::ERROR;

    cmd->value.data = data + data_start;
    cmd->value.len = cmd->bytes;
    cmd->type = McCommandType::META_SET;
    *consumed = static_cast<uint32_t>(data_end + 2);
    return McParseResult::OK;
}

// Parse "md <key> [flags...]\r\n"
static McParseResult parse_meta_delete(const uint8_t* data, uint32_t line_end, McCommand* cmd, uint32_t pos) {
    pos = skip_spaces(data, pos, line_end);
    if (pos >= line_end)
        return McParseResult::ERROR;
    pos = read_token(data, pos, line_end, &cmd->key);
    if (cmd->key.len == 0)
        return McParseResult::ERROR;

    parse_meta_flags(data, pos, line_end, cmd);
    cmd->type = McCommandType::META_DELETE;
    return McParseResult::OK;
}

McParseResult mc_parse(const uint8_t* data, uint32_t len, McCommand* cmd, uint32_t* consumed) {
    ASSERT(cmd != nullptr, "mc_parse requires cmd output");
    ASSERT(consumed != nullptr, "mc_parse requires consumed output");
    ASSERT(data != nullptr || len == 0, "mc_parse null data with non-zero length");
    if (!cmd || !consumed)
        return McParseResult::ERROR;
    if (!data && len > 0)
        return McParseResult::ERROR;

    // Zero-initialize output.
    *cmd = {};
    *consumed = 0;

    if (len == 0)
        return McParseResult::INCOMPLETE;

    // Find the first \r\n — the command line.
    uint32_t line_end = find_crlf(data, 0, len);
    if (line_end >= len)
        return McParseResult::INCOMPLETE;

    // Empty line.
    if (line_end == 0)
        return McParseResult::ERROR;

    // Default consumed is just the command line.
    uint32_t default_consumed = line_end + 2;

    // First-character dispatch for speed.
    uint8_t c0 = data[0];

    // Meta commands: mg, ms, md, mn
    // Require word boundary after 2-char prefix (space or end-of-line).
    if (c0 == 'm' && line_end >= 2 && (line_end == 2 || data[2] == ' ')) {
        if (data[1] == 'g') {
            uint32_t pos = 2;
            McParseResult r = parse_meta_get(data, line_end, cmd, pos);
            if (r == McParseResult::OK)
                *consumed = default_consumed;
            return r;
        }
        if (data[1] == 's') {
            uint32_t pos = 2;
            McParseResult r = parse_meta_set(data, len, line_end, cmd, pos, consumed);
            return r;
        }
        if (data[1] == 'd') {
            uint32_t pos = 2;
            McParseResult r = parse_meta_delete(data, line_end, cmd, pos);
            if (r == McParseResult::OK)
                *consumed = default_consumed;
            return r;
        }
        if (data[1] == 'n') {
            // mn takes no arguments.
            if (line_end != 2)
                return McParseResult::ERROR;
            cmd->type = McCommandType::META_NOOP;
            *consumed = default_consumed;
            return McParseResult::OK;
        }
    }

    // Legacy commands: read first token.
    McArg verb = {};
    uint32_t pos = read_token(data, 0, line_end, &verb);
    if (verb.len == 0)
        return McParseResult::ERROR;

    // "get" (single-key only; "gets" not supported — no CAS)
    if (verb.len == 3 && token_matches_ci(&verb, "GET", 3)) {
        McParseResult r = parse_legacy_get(data, line_end, cmd, pos);
        if (r == McParseResult::OK)
            *consumed = default_consumed;
        return r;
    }
    if (verb.len == 4 && token_matches_ci(&verb, "GETS", 4))
        return McParseResult::ERROR;

    // "set"
    if (verb.len == 3 && token_matches_ci(&verb, "SET", 3)) {
        McParseResult r = parse_legacy_set(data, len, line_end, cmd, pos, consumed);
        return r;
    }

    // "delete"
    if (verb.len == 6 && token_matches_ci(&verb, "DELETE", 6)) {
        McParseResult r = parse_legacy_delete(data, line_end, cmd, pos);
        if (r == McParseResult::OK)
            *consumed = default_consumed;
        return r;
    }

    // "version" — no arguments allowed
    if (verb.len == 7 && token_matches_ci(&verb, "VERSION", 7)) {
        uint32_t vp = skip_spaces(data, pos, line_end);
        if (vp < line_end)
            return McParseResult::ERROR;
        cmd->type = McCommandType::VERSION;
        *consumed = default_consumed;
        return McParseResult::OK;
    }

    // "quit"
    if (verb.len == 4 && token_matches_ci(&verb, "QUIT", 4)) {
        return McParseResult::ERROR; // close connection
    }

    return McParseResult::ERROR;
}
