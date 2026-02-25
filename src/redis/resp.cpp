// resp.cpp — RESP2 parser and response writer implementation.

#include "resp.h"

#include <climits>
#include <cstdio>
#include <cstring>

struct ParseIntResult {
    int value;
    bool error; // true = malformed input (non-digit, overflow, empty)
};

//  Reads a signed decimal integer starting at buf[*pos], then advances *pos
// past the mandatory \r\n terminator.
//
// Used for RESP count/length prefixes:
//   *3    — array of 3 elements
//   $5    — bulk string of 5 bytes
//   $-1   — null bulk string (negative, only valid length)
//
// Returns {value, false} on success, {-1, false} if \r\n hasn't arrived yet
// (INCOMPLETE), or {0, true} on malformed input (non-digit, overflow, empty).
static ParseIntResult parse_int(const uint8_t *buf, uint32_t len, uint32_t *pos) {
    int n = 0;
    bool negative = false;
    bool has_digits = false;
    uint32_t start = *pos;

    if (*pos < len && buf[*pos] == '-') { negative = true; (*pos)++; }

    while (*pos < len && buf[*pos] != '\r') {
        uint8_t c = buf[*pos];
        if (c < '0' || c > '9') { *pos = start; return {0, true}; }
        int digit = c - '0';
        // Overflow check: n * 10 + digit > INT_MAX
        if (n > INT_MAX / 10 || (n == INT_MAX / 10 && digit > 7)) {
            *pos = start; return {0, true};
        }
        n = n * 10 + digit;
        has_digits = true;
        (*pos)++;
    }

    // Need at least \r and \n still in the buffer.
    if (*pos + 1 >= len) { *pos = start; return {-1, false}; }

    // No digits between prefix and \r is malformed (e.g. "*\r\n").
    if (!has_digits) { *pos = start; return {0, true}; }

    *pos += 2; // skip \r\n
    return {negative ? -n : n, false};
}

//  Parse one complete RESP command from data[0..len).
//
// The expected wire layout is:
//
//   *<argc>\r\n
//   $<len0>\r\n<arg0 bytes>\r\n
//   $<len1>\r\n<arg1 bytes>\r\n
//   ...
//
// All cmd->args[i].data pointers point directly into data[] — no copying.
// On success, *consumed covers the entire command including all \r\n pairs.
RespParseResult resp_parse(const uint8_t *data, uint32_t len,
                           RespCommand *cmd, uint32_t *consumed) {
    uint32_t pos = 0;

    // Empty buffer — wait for data.
    if (pos >= len) { *consumed = 0; return RespParseResult::INCOMPLETE; }

    // RESP requests must start with '*' (array). Inline commands not supported.
    if (data[pos] != '*') return RespParseResult::ERROR;
    pos++;

    // Array element count.
    auto [argc, argc_err] = parse_int(data, len, &pos);
    if (argc_err) return RespParseResult::ERROR;
    if (argc < 0) { *consumed = 0; return RespParseResult::INCOMPLETE; }
    if (argc < 1 || argc > RESP_MAX_ARGS) return RespParseResult::ERROR;
    cmd->argc = argc;

    for (int i = 0; i < argc; i++) {
        if (pos >= len) { *consumed = 0; return RespParseResult::INCOMPLETE; }

        // Each element must be a bulk string ('$').
        if (data[pos] != '$') return RespParseResult::ERROR;
        pos++;

        // Bulk string byte length (may be -1 for null, but not in requests).
        auto [slen, slen_err] = parse_int(data, len, &pos);
        if (slen_err) return RespParseResult::ERROR;
        if (slen < 0) { *consumed = 0; return RespParseResult::INCOMPLETE; }

        // Check that the full payload + trailing \r\n is present.
        if (pos + static_cast<uint32_t>(slen) + 2 > len) { *consumed = 0; return RespParseResult::INCOMPLETE; }

        // Point directly into the receive buffer — zero-copy.
        cmd->args[i].data = data + pos;
        cmd->args[i].len = static_cast<uint32_t>(slen);
        pos += static_cast<uint32_t>(slen) + 2; // skip <bytes> + \r\n
    }

    *consumed = pos;
    return RespParseResult::OK;
}

// Emit "+OK\r\n" — RESP simple string, used as the SET reply.
uint32_t resp_write_ok(uint8_t *out) {
    std::memcpy(out, "+OK\r\n", 5);
    return 5;
}

// Emit "$-1\r\n" — RESP null bulk string, used when a key is not found.
uint32_t resp_write_null(uint8_t *out) {
    std::memcpy(out, "$-1\r\n", 5);
    return 5;
}

//  Emit a RESP bulk string: "$<len>\r\n<data>\r\n".
// Used for GET responses. data[] is copied into out[]; no pointer aliasing.
uint32_t resp_write_bulk(uint8_t *out, const uint8_t *data, uint32_t len) {
    uint32_t n = 0;
    out[n++] = '$';
    n += static_cast<uint32_t>(std::snprintf(reinterpret_cast<char *>(out) + n, 20, "%u", len));
    out[n++] = '\r'; out[n++] = '\n';
    if (len > 0) std::memcpy(out + n, data, len);
    n += len;
    out[n++] = '\r'; out[n++] = '\n';
    return n;
}

// Emit "-ERR <msg>\r\n" — RESP error, truncated to 512 bytes total.
uint32_t resp_write_error(uint8_t *out, const char *msg) {
    int n = std::snprintf(reinterpret_cast<char *>(out), 512, "-ERR %s\r\n", msg);
    if (n <= 0) return 0;
    return static_cast<uint32_t>(n >= 512 ? 511 : n);
}

// Emit "+PONG\r\n" — RESP simple string reply to PING.
uint32_t resp_write_pong(uint8_t *out) {
    std::memcpy(out, "+PONG\r\n", 7);
    return 7;
}
