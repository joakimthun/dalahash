// protocol.h — Compile-time selected protocol interface for net/worker.
//
// Overview
// --------
// The networking worker is protocol-agnostic. It includes this header and calls
// a small set of protocol symbols on the hot path:
//   - protocol_worker_init(...)
//   - protocol_parse(...)
//   - protocol_execute(...)
//
// A concrete protocol (Redis, Echo, future protocol) is selected at compile
// time via CMake definitions (DALAHASH_PROTOCOL_*). The selected header must
// provide the symbols documented below with exactly these names/signatures.
//
// Protocol implementation contract
// --------------------------------
// Every protocol header included via this file MUST define:
//
// 1) Types
//    - struct ProtocolCommand
//      Parsed command/message descriptor for one unit of work. It may contain
//      pointers into the caller-provided parse buffer.
//
//    - enum class ProtocolParseResult : uint8_t { OK, INCOMPLETE, ERROR }
//      Parse status returned by protocol_parse.
//
//    - struct ProtocolWorkerState
//      Per-worker protocol state (owned by worker thread; no cross-thread
//      sharing). For Redis this owns the per-worker Store.
//
// 2) Result constants used by worker hot path
//    - PROTOCOL_PARSE_OK
//    - PROTOCOL_PARSE_INCOMPLETE
//    - PROTOCOL_PARSE_ERROR
//
// 3) Functions
//    - inline void protocol_worker_init(ProtocolWorkerState *state)
//      Called once per worker before entering event loop.
//
//    - inline ProtocolParseResult protocol_parse(const uint8_t *data,
//                                                uint32_t len,
//                                                ProtocolCommand *cmd,
//                                                uint32_t *consumed)
//
//    - inline uint32_t protocol_execute(const ProtocolCommand *cmd,
//                                       ProtocolWorkerState *state,
//                                       uint8_t *out_buf,
//                                       uint32_t out_buf_size)
//
// Runtime semantics required by worker.cpp
// ----------------------------------------
// protocol_parse(...) semantics:
//   - On OK:
//       * Populate *cmd with one parsed unit.
//       * Set *consumed to bytes consumed from data[0..len).
//       * consumed MUST be > 0 to guarantee loop progress.
//
//   - On INCOMPLETE:
//       * Set *consumed to 0.
//       * Means parser needs more bytes to complete one unit.
//
//   - On ERROR:
//       * Malformed input or fatal parse state.
//       * Worker closes the connection.
//
// protocol_execute(...) semantics:
//   - Write full response bytes into out_buf.
//   - Return number of bytes written.
//   - Returning 0 is allowed (no response).
//   - If return value > out_buf_size, worker treats it as overflow and closes
//     the connection.
//
// Pointer lifetime:
//   - If ProtocolCommand stores pointers into the parse buffer, they only need
//     to remain valid until protocol_execute returns for that command.
//
// Performance guidance:
//   - These functions run on hot path. Prefer branch-light logic and avoid
//     allocations in parse/execute when possible.
//   - Keep this contract exception-free (-fno-exceptions project rule).

#pragma once

#if defined(DALAHASH_PROTOCOL_REDIS) && DALAHASH_PROTOCOL_REDIS
#include "protocol/redis/redis_protocol.h"
#elif defined(DALAHASH_PROTOCOL_ECHO) && DALAHASH_PROTOCOL_ECHO
#include "protocol/echo/echo_protocol.h"
#else
#error "No protocol selected. Set DALAHASH_PROTOCOL (redis/echo) in CMake."
#endif
