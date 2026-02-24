// redis_protocol.h — Redis protocol adapter for the generic protocol interface.

#pragma once

#include "redis/command.h"
#include "redis/resp.h"
#include "redis/store.h"

#include <cstdint>

using ProtocolCommand = RespCommand;
using ProtocolParseResult = RespParseResult;

static constexpr ProtocolParseResult PROTOCOL_PARSE_OK = ProtocolParseResult::OK;
static constexpr ProtocolParseResult PROTOCOL_PARSE_INCOMPLETE = ProtocolParseResult::INCOMPLETE;
static constexpr ProtocolParseResult PROTOCOL_PARSE_ERROR = ProtocolParseResult::ERROR;

struct ProtocolWorkerState {
    Store store;
};

inline void protocol_worker_init(ProtocolWorkerState *state) {
    state->store = {};
}

inline ProtocolParseResult protocol_parse(const uint8_t *data, uint32_t len,
                                          ProtocolCommand *cmd, uint32_t *consumed) {
    return resp_parse(data, len, cmd, consumed);
}

inline uint32_t protocol_execute(const ProtocolCommand *cmd,
                                 ProtocolWorkerState *state,
                                 uint8_t *out_buf, uint32_t out_buf_size) {
    return command_execute(cmd, &state->store, out_buf, out_buf_size);
}
