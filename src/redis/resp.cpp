// resp.cpp — RESP2 parser and response writer implementation.

#include "resp.h"
#include "base/assert.h"

#include <climits>
#include <cstring>

// Fast decimal formatting for uint32_t. Returns number of bytes written.
// Caller must ensure out has at least 10 bytes of space.
static uint32_t uint_to_str(uint8_t* out, uint32_t val) {
    // Digit table avoids repeated division for two-digit groups.
    static constexpr char digits[] = "0123456789";
    uint8_t tmp[10];
    uint32_t n = 0;
    do {
        tmp[n++] = static_cast<uint8_t>(digits[val % 10]);
        val /= 10;
    } while (val > 0);
    // Reverse into output.
    for (uint32_t i = 0; i < n; i++)
        out[i] = tmp[n - 1 - i];
    return n;
}

enum class ParseIntStatus : uint8_t { OK, INCOMPLETE, ERROR };

struct ParseIntResult {
    ParseIntStatus status;
    int value;
};

static inline bool is_ascii_digit(uint8_t c) { return c >= '0' && c <= '9'; }

// Reads a non-negative decimal integer starting at buf[*pos], then advances
// *pos past the mandatory \r\n terminator.
//
// Used for RESP count/length prefixes:
//   *3    — array of 3 elements
//   $5    — bulk string of 5 bytes
//
// Request parsing rejects negative bulk lengths, so this only accepts digits.
// Returns ParseIntStatus::OK with parsed value on success, INCOMPLETE if the
// full line has not arrived, or ERROR on malformed input.
static ParseIntResult parse_int(const uint8_t* buf, uint32_t len, uint32_t* pos) {
    ASSERT(buf != nullptr || len == 0, "parse_int null buffer with non-zero length");
    ASSERT(pos != nullptr, "parse_int requires position pointer");
    ASSERT(*pos <= len, "parse_int position out of bounds");
    uint32_t p = *pos;
    int n = 0;
    bool has_digits = false;
    while (p < len) {
        const uint8_t c = buf[p];
        if (c == '\r')
            break;
        if (!is_ascii_digit(c))
            return {ParseIntStatus::ERROR, 0};
        const int digit = static_cast<int>(c - '0');
        // Overflow check: n * 10 + digit > INT_MAX
        if (n > INT_MAX / 10 || (n == INT_MAX / 10 && digit > 7))
            return {ParseIntStatus::ERROR, 0};
        n = n * 10 + digit;
        has_digits = true;
        p++;
    }

    if (p >= len)
        return {ParseIntStatus::INCOMPLETE, 0};

    // Need at least \r and \n still in the buffer.
    if (p + 1 >= len)
        return {ParseIntStatus::INCOMPLETE, 0};

    // No digits between prefix and \r is malformed (e.g. "*\r\n").
    if (!has_digits)
        return {ParseIntStatus::ERROR, 0};
    if (buf[p + 1] != '\n')
        return {ParseIntStatus::ERROR, 0};

    *pos = p + 2; // skip \r\n
    return {ParseIntStatus::OK, n};
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
RespParseResult resp_parse(const uint8_t* data, uint32_t len, RespCommand* cmd, uint32_t* consumed) {
    ASSERT(cmd != nullptr, "resp_parse requires cmd output");
    ASSERT(consumed != nullptr, "resp_parse requires consumed output");
    ASSERT(data != nullptr || len == 0, "resp_parse null data with non-zero length");
    if (!cmd || !consumed)
        return RespParseResult::ERROR;
    if (!data && len > 0)
        return RespParseResult::ERROR;

    uint32_t pos = 0;

    // Empty buffer — wait for data.
    if (pos >= len) {
        *consumed = 0;
        return RespParseResult::INCOMPLETE;
    }

    // RESP requests must start with '*' (array). Inline commands not supported.
    if (data[pos] != '*')
        return RespParseResult::ERROR;
    pos++;

    // Array element count.
    ParseIntResult argc_parse = parse_int(data, len, &pos);
    if (argc_parse.status == ParseIntStatus::INCOMPLETE) {
        *consumed = 0;
        return RespParseResult::INCOMPLETE;
    }
    if (argc_parse.status == ParseIntStatus::ERROR)
        return RespParseResult::ERROR;
    int argc = argc_parse.value;
    if (argc < 0)
        return RespParseResult::ERROR;
    if (argc < 1 || argc > RESP_MAX_ARGS)
        return RespParseResult::ERROR;
    cmd->argc = argc;

    for (int i = 0; i < argc; i++) {
        if (pos >= len) {
            *consumed = 0;
            return RespParseResult::INCOMPLETE;
        }

        // Each element must be a bulk string ('$').
        if (data[pos] != '$')
            return RespParseResult::ERROR;
        pos++;

        // Bulk string byte length (may be -1 for null, but not in requests).
        ParseIntResult slen_parse = parse_int(data, len, &pos);
        if (slen_parse.status == ParseIntStatus::INCOMPLETE) {
            *consumed = 0;
            return RespParseResult::INCOMPLETE;
        }
        if (slen_parse.status == ParseIntStatus::ERROR)
            return RespParseResult::ERROR;
        int slen = slen_parse.value;
        if (slen < 0)
            return RespParseResult::ERROR;

        // Check that the full payload + trailing \r\n is present.
        uint32_t slen_u32 = static_cast<uint32_t>(slen);
        if (pos + slen_u32 + 2 > len) {
            *consumed = 0;
            return RespParseResult::INCOMPLETE;
        }
        if (data[pos + slen_u32] != '\r' || data[pos + slen_u32 + 1] != '\n')
            return RespParseResult::ERROR;

        // Point directly into the receive buffer — zero-copy.
        cmd->args[i].data = data + pos;
        cmd->args[i].len = slen_u32;
        pos += slen_u32 + 2; // skip <bytes> + \r\n
    }

    *consumed = pos;
    ASSERT(*consumed > 0, "resp_parse OK must consume bytes");
    ASSERT(*consumed <= len, "resp_parse consumed beyond input length");
    return RespParseResult::OK;
}

// Emit "+OK\r\n" — RESP simple string, used as the SET reply.
uint32_t resp_write_ok(uint8_t* out) {
    ASSERT(out != nullptr, "resp_write_ok requires output buffer");
    std::memcpy(out, "+OK\r\n", 5);
    return 5;
}

// Emit "$-1\r\n" — RESP null bulk string, used when a key is not found.
uint32_t resp_write_null(uint8_t* out) {
    ASSERT(out != nullptr, "resp_write_null requires output buffer");
    std::memcpy(out, "$-1\r\n", 5);
    return 5;
}

//  Emit a RESP bulk string: "$<len>\r\n<data>\r\n".
// Used for GET responses. data[] is copied into out[]; no pointer aliasing.
uint32_t resp_write_bulk(uint8_t* out, const uint8_t* data, uint32_t len) {
    ASSERT(out != nullptr, "resp_write_bulk requires output buffer");
    ASSERT(data != nullptr || len == 0, "resp_write_bulk null data with non-zero length");
    uint32_t n = 0;
    out[n++] = '$';
    n += uint_to_str(out + n, len);
    out[n++] = '\r';
    out[n++] = '\n';
    if (len > 0)
        std::memcpy(out + n, data, len);
    n += len;
    out[n++] = '\r';
    out[n++] = '\n';
    return n;
}

// Emit "-ERR <msg>\r\n" — RESP error, truncated to 512 bytes total.
uint32_t resp_write_error(uint8_t* out, const char* msg) {
    ASSERT(out != nullptr, "resp_write_error requires output buffer");
    ASSERT(msg != nullptr, "resp_write_error requires message");
    if (!out || !msg)
        return 0;
    // Manual formatting: "-ERR <msg>\r\n", capped at 512 bytes total.
    std::memcpy(out, "-ERR ", 5);
    uint32_t n = 5;
    size_t msg_len = std::strlen(msg);
    // Cap so total output <= 511 bytes (matches snprintf(buf,512) which returns at most 511).
    if (msg_len > 511 - 5 - 2)
        msg_len = 511 - 5 - 2;
    std::memcpy(out + n, msg, msg_len);
    n += static_cast<uint32_t>(msg_len);
    out[n++] = '\r';
    out[n++] = '\n';
    return n;
}

// Emit "+PONG\r\n" — RESP simple string reply to PING.
uint32_t resp_write_pong(uint8_t* out) {
    ASSERT(out != nullptr, "resp_write_pong requires output buffer");
    std::memcpy(out, "+PONG\r\n", 7);
    return 7;
}
