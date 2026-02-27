// echo_protocol.h — Raw TCP echo adapter that satisfies protocol.h contract.
//
// Wire behavior
// -------------
// Every recv payload is treated as one protocol command and echoed back
// unchanged. There is no framing beyond what transport provides to worker.
//
// Implication:
//   - One recv completion -> one parse OK -> one execute -> echoed bytes.
//   - If a logical client message arrives in multiple TCP segments, each recv
//     segment is echoed independently (standard "stream echo" behavior).

#pragma once

#include "base/assert.h"
#include <cstdint>
#include <cstring>

struct ProtocolInitContext;

// Parsed command for echo: just a byte span.
// data points into worker-provided parse buffer and is consumed immediately
// by protocol_execute in the same loop iteration.
struct ProtocolCommand {
    const uint8_t* data;
    uint32_t len;
};

// Parse status required by protocol.h contract.
enum class ProtocolParseResult : uint8_t { OK, INCOMPLETE, ERROR };

// Constants referenced by worker hot path.
static constexpr ProtocolParseResult PROTOCOL_PARSE_OK = ProtocolParseResult::OK;
static constexpr ProtocolParseResult PROTOCOL_PARSE_INCOMPLETE = ProtocolParseResult::INCOMPLETE;
static constexpr ProtocolParseResult PROTOCOL_PARSE_ERROR = ProtocolParseResult::ERROR;

// Echo protocol has no per-worker mutable state.
struct ProtocolWorkerState {};

// No-op initializer required by protocol.h contract.
inline void protocol_worker_init(ProtocolWorkerState*, const ProtocolInitContext* = nullptr) {}

inline void protocol_worker_quiescent(ProtocolWorkerState*) {}

inline uint64_t protocol_now_ms() { return 0; }

// Parse one echo command from [data, data + len).
//
// Behavior:
//   - len > 0: return OK, command is the full span, consumed=len.
//   - len == 0: return INCOMPLETE so worker preserves state and waits for more.
//   - invalid args (null cmd/consumed or null data with len>0): ERROR.
inline ProtocolParseResult protocol_parse(const uint8_t* data, uint32_t len, ProtocolCommand* cmd,
                                          uint32_t* consumed) {
    ASSERT(cmd != nullptr, "echo protocol_parse requires cmd output");
    ASSERT(consumed != nullptr, "echo protocol_parse requires consumed output");
    ASSERT(data != nullptr || len == 0, "echo protocol_parse null data with non-zero length");
    if (!cmd || !consumed)
        return PROTOCOL_PARSE_ERROR;
    if (!data && len > 0)
        return PROTOCOL_PARSE_ERROR;
    if (len == 0) {
        *consumed = 0;
        return PROTOCOL_PARSE_INCOMPLETE;
    }

    cmd->data = data;
    cmd->len = len;
    *consumed = len;
    return PROTOCOL_PARSE_OK;
}

// Write echoed bytes to out_buf and return length.
//
// Overflow contract:
//   - If cmd->len exceeds out_buf_size, return out_buf_size + 1 as sentinel.
//   - worker.cpp treats any return > out_buf_size as fatal overflow and closes
//     the connection.
inline uint32_t protocol_execute(const ProtocolCommand* cmd, ProtocolWorkerState*, uint64_t, uint8_t* out_buf,
                                 uint32_t out_buf_size) {
    ASSERT(cmd != nullptr, "echo protocol_execute requires command");
    ASSERT(out_buf != nullptr || out_buf_size == 0, "echo protocol_execute null output with non-zero size");
    if (!cmd)
        return 0;
    if (cmd->len > out_buf_size)
        return out_buf_size + 1;
    if (cmd->len > 0)
        std::memcpy(out_buf, cmd->data, cmd->len);
    return cmd->len;
}
