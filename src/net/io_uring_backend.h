// io_uring_backend.h — Production I/O backend using Linux io_uring.

#pragma once

#include "io.h"
#include <cstdint>

IoOps io_uring_ops();
IoBackend* io_uring_backend_create(uint32_t ring_size, uint32_t buf_count, uint32_t buf_size);
