//  command.h — Dispatches a parsed RESP command and writes the RESP response.
//
// command_execute writes a complete RESP response into out_buf[] and returns
// the number of bytes written. Returns 0 only if no response should be sent
// (currently never happens — every command gets a response).

#pragma once

#include "resp.h"
#include "store.h"
#include <cstdint>

uint32_t command_execute(const RespCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                         uint32_t out_buf_size);
