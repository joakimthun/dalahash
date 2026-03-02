// memcached_command.h — Command dispatch for memcached protocol.

#pragma once

#include "memcached_parse.h"
#include "store/store.h"

#include <cstdint>

uint32_t mc_command_execute(const McCommand* cmd, Store* store, uint64_t now_ms, uint8_t* out_buf,
                            uint32_t out_buf_size);
