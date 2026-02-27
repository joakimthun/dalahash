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

#include "base/assert.h"
#include "redis/command.h"
#include "redis/resp.h"
#include "redis/store.h"

#include <cstdint>

struct ProtocolInitContext;

// One parsed protocol command is exactly one parsed RESP command.
using ProtocolCommand = RespCommand;
// Parse status is directly the RESP parser result enum.
using ProtocolParseResult = RespParseResult;

// Constants used by worker hot path switch/if checks.
static constexpr ProtocolParseResult PROTOCOL_PARSE_OK = ProtocolParseResult::OK;
static constexpr ProtocolParseResult PROTOCOL_PARSE_INCOMPLETE = ProtocolParseResult::INCOMPLETE;
static constexpr ProtocolParseResult PROTOCOL_PARSE_ERROR = ProtocolParseResult::ERROR;

// Per-worker protocol state.
struct ProtocolWorkerState {
    Store store;
};

// Initialize per-worker Redis state before event loop starts.
inline void protocol_worker_init(ProtocolWorkerState* state, const ProtocolInitContext* ctx = nullptr) {
    ASSERT(state != nullptr, "protocol_worker_init requires state");
    if (!state)
        return;
    store_reset(&state->store);
    if (ctx && ctx->shared_store) {
        store_bind_shared(&state->store, ctx->shared_store, ctx->worker_id);
        int ret = kv_store_register_worker(ctx->shared_store, ctx->worker_id);
        ASSERT(ret == 0, "shared store worker registration failed");
    }
}

inline void protocol_worker_quiescent(ProtocolWorkerState* state) {
    ASSERT(state != nullptr, "protocol_worker_quiescent requires state");
    if (!state)
        return;
    store_quiescent(&state->store);
}

inline uint64_t protocol_now_ms() { return kv_time_now_ms(); }

// Parse one RESP command from [data, data + len).
// Contract delegated to resp_parse:
//   - OK: cmd populated, consumed > 0
//   - INCOMPLETE: consumed = 0
//   - ERROR: malformed request
inline ProtocolParseResult protocol_parse(const uint8_t* data, uint32_t len, ProtocolCommand* cmd,
                                          uint32_t* consumed) {
    ASSERT(cmd != nullptr, "protocol_parse requires cmd output");
    ASSERT(consumed != nullptr, "protocol_parse requires consumed output");
    ASSERT(data != nullptr || len == 0, "protocol_parse null input with non-zero length");
    return resp_parse(data, len, cmd, consumed);
}

// Execute one RESP command against per-worker Store and write full RESP reply.
// Return value is bytes written to out_buf.
// If command_execute reports a value > out_buf_size, worker treats this as
// overflow and closes the connection.
inline uint32_t protocol_execute(const ProtocolCommand* cmd, ProtocolWorkerState* state, uint64_t now_ms,
                                 uint8_t* out_buf, uint32_t out_buf_size) {
    ASSERT(cmd != nullptr, "protocol_execute requires command");
    ASSERT(state != nullptr, "protocol_execute requires state");
    ASSERT(out_buf != nullptr || out_buf_size == 0, "protocol_execute null output with non-zero size");
    if (!cmd || !state || (!out_buf && out_buf_size > 0))
        return 0;
    return command_execute(cmd, &state->store, now_ms, out_buf, out_buf_size);
}
