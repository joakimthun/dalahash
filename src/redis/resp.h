/*
 * resp.h — RESP2 (REdis Serialization Protocol v2) parser and response writer.
 *
 * Wire format (all client requests are RESP arrays of bulk strings):
 *
 *   *<argc>\r\n                   array header (number of arguments)
 *   $<len>\r\n<bytes>\r\n         bulk string (one per argument)
 *
 *   Example — "SET mykey hello":
 *     *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n
 *
 * Response types written by resp_write_*:
 *   +<text>\r\n                   simple string  (OK, PONG)
 *   -ERR <msg>\r\n                error
 *   $<len>\r\n<data>\r\n          bulk string    (GET hit)
 *   $-1\r\n                       null bulk string (GET miss)
 *   *0\r\n                        empty array    (COMMAND stub)
 *
 * Zero-allocation design:
 *   RespArg pointers reference the caller's receive buffer directly — the
 *   parser never copies argument bytes. Responses are written into a
 *   caller-supplied output buffer. No heap allocation on any path.
 */

#pragma once

#include <cstddef>
#include <cstdint>

/* Maximum number of arguments in one RESP command.
 * Covers all current commands (GET=2, SET=3) with room to grow. */
static constexpr int RESP_MAX_ARGS = 8;

struct RespArg {
    const uint8_t *data; /* points into the receive buffer (zero-copy) */
    uint32_t len;        /* byte count, excludes trailing \r\n */
};

enum class RespParseResult : uint8_t { OK, INCOMPLETE, ERROR };

struct RespCommand {
    RespArg args[RESP_MAX_ARGS]; /* args[0] = command verb (e.g. "SET") */
    int argc;
};

/* Parse one complete RESP array-of-bulk-strings command from data[0..len).
 *
 * On OK:       *cmd is populated, *consumed = bytes forming this command.
 * On INCOMPLETE: *consumed = 0, caller should buffer and retry with more data.
 * On ERROR:    malformed RESP (wrong type byte, too many args, etc.). */
RespParseResult resp_parse(const uint8_t *data, uint32_t len,
                           RespCommand *cmd, uint32_t *consumed);

/* Response writers — each serialises one RESP value into out[] and returns
 * the number of bytes written. Callers must ensure out[] is large enough. */
uint32_t resp_write_ok(uint8_t *out);    /* +OK\r\n       (5 bytes) */
uint32_t resp_write_null(uint8_t *out);  /* $-1\r\n       (5 bytes) */
uint32_t resp_write_pong(uint8_t *out);  /* +PONG\r\n     (7 bytes) */
uint32_t resp_write_bulk(uint8_t *out, const uint8_t *data, uint32_t len);
uint32_t resp_write_error(uint8_t *out, const char *msg);
