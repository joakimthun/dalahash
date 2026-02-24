// protocol.h — Compile-time selected protocol interface for net/worker.

#pragma once

#if defined(DALAHASH_PROTOCOL_REDIS) && DALAHASH_PROTOCOL_REDIS
#include "protocol/redis/redis_protocol.h"
#else
#error "No protocol selected. Set DALAHASH_PROTOCOL_REDIS=1 in the build."
#endif
