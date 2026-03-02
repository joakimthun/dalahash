// memcached_protocol.h — Memcached adapter that satisfies the protocol.h contract.
//
// Mapping summary
// ---------------
// This adapter keeps worker generic while reusing memcached protocol components:
//   - protocol_parse    -> mc_parse
//   - protocol_execute  -> mc_command_execute
//   - ProtocolWorkerState -> per-worker Store

#pragma once

#include "base/assert.h"
#include "memcached/memcached_command.h"
#include "memcached/memcached_parse.h"
#include "store/store.h"

#include <cstdint>

struct ProtocolInitContext;

// One parsed protocol command is exactly one parsed memcached command.
using ProtocolCommand = McCommand;
// Parse status is directly the memcached parser result enum.
using ProtocolParseResult = McParseResult;

// Constants used by worker hot path switch/if checks.
static constexpr ProtocolParseResult PROTOCOL_PARSE_OK = ProtocolParseResult::OK;
static constexpr ProtocolParseResult PROTOCOL_PARSE_INCOMPLETE = ProtocolParseResult::INCOMPLETE;
static constexpr ProtocolParseResult PROTOCOL_PARSE_ERROR = ProtocolParseResult::ERROR;

// Per-worker protocol state.
struct ProtocolWorkerState {
    Store store;
};

// Initialize per-worker memcached state before event loop starts.
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

// Parse one memcached command from [data, data + len).
inline ProtocolParseResult protocol_parse(const uint8_t* data, uint32_t len, ProtocolCommand* cmd,
                                          uint32_t* consumed) {
    ASSERT(cmd != nullptr, "protocol_parse requires cmd output");
    ASSERT(consumed != nullptr, "protocol_parse requires consumed output");
    ASSERT(data != nullptr || len == 0, "protocol_parse null input with non-zero length");
    return mc_parse(data, len, cmd, consumed);
}

// Execute one memcached command against per-worker Store.
inline uint32_t protocol_execute(const ProtocolCommand* cmd, ProtocolWorkerState* state, uint64_t now_ms,
                                 uint8_t* out_buf, uint32_t out_buf_size) {
    ASSERT(cmd != nullptr, "protocol_execute requires command");
    ASSERT(state != nullptr, "protocol_execute requires state");
    ASSERT(out_buf != nullptr || out_buf_size == 0, "protocol_execute null output with non-zero size");
    if (!cmd || !state || (!out_buf && out_buf_size > 0))
        return 0;
    return mc_command_execute(cmd, &state->store, now_ms, out_buf, out_buf_size);
}
