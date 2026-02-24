// redis_protocol.h — Redis adapter that satisfies the protocol.h contract.
//
// Mapping summary
// ---------------
// This adapter keeps worker generic while reusing existing Redis components:
//   - protocol_parse    -> resp_parse
//   - protocol_execute  -> command_execute
//   - ProtocolWorkerState -> per-worker Store
//
// The worker's hot-path contract is preserved:
//   - parse result OK/INCOMPLETE/ERROR comes from RESP parser
//   - command data points into receive buffer (zero-copy parse)
//   - execute writes complete RESP reply into caller-provided output buffer

#pragma once

#include "redis/command.h"
#include "redis/resp.h"
#include "redis/store.h"

#include <cstdint>

// One parsed protocol command is exactly one parsed RESP command.
using ProtocolCommand = RespCommand;
// Parse status is directly the RESP parser result enum.
using ProtocolParseResult = RespParseResult;

// Constants used by worker hot path switch/if checks.
static constexpr ProtocolParseResult PROTOCOL_PARSE_OK = ProtocolParseResult::OK;
static constexpr ProtocolParseResult PROTOCOL_PARSE_INCOMPLETE = ProtocolParseResult::INCOMPLETE;
static constexpr ProtocolParseResult PROTOCOL_PARSE_ERROR = ProtocolParseResult::ERROR;

// Per-worker protocol state.
// Redis mode keeps a dedicated Store per worker thread (no shared locks).
struct ProtocolWorkerState {
    Store store;
};

// Initialize per-worker Redis state before event loop starts.
inline void protocol_worker_init(ProtocolWorkerState *state) {
    state->store = {};
}

// Parse one RESP command from [data, data + len).
// Contract delegated to resp_parse:
//   - OK: cmd populated, consumed > 0
//   - INCOMPLETE: consumed = 0
//   - ERROR: malformed request
inline ProtocolParseResult protocol_parse(const uint8_t *data, uint32_t len,
                                          ProtocolCommand *cmd, uint32_t *consumed) {
    return resp_parse(data, len, cmd, consumed);
}

// Execute one RESP command against per-worker Store and write full RESP reply.
// Return value is bytes written to out_buf.
// If command_execute reports a value > out_buf_size, worker treats this as
// overflow and closes the connection.
inline uint32_t protocol_execute(const ProtocolCommand *cmd,
                                 ProtocolWorkerState *state,
                                 uint8_t *out_buf, uint32_t out_buf_size) {
    return command_execute(cmd, &state->store, out_buf, out_buf_size);
}
