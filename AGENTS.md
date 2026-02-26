# AGENTS.md

## Project (first version)

- The project is called dalahash - a C++ in-memory data store.
- It should be compatible with the basic Redis commands (using a standard Redis client) GET and SET.
- The emphasis is always on performance.
- The goal is to run dalahash at hardware capacity.

### Current implemented command surface

Redis mode:
- GET
- SET
- PING
- COMMAND (stub used by redis-cli handshake; returns `*0\r\n`)

Echo mode:
- Raw TCP echo (server writes back the exact bytes received).

## Rules & Standards

- Use cmake.
- Use clang-21 to compile.
- Tests should be written with gtest.
- Use C++23 (`CMAKE_CXX_STANDARD 23`).
- Do not use exceptions for error handling (`-fno-exceptions`).
- Prefer simple C-style code when possible. Avoid complex C++ features when not needed, and avoid templates as much as possible.
- Always use C++ style casts (`static_cast`, `reinterpret_cast`, `const_cast`) instead of C-style casts (`(Type)expr`).
- The system should be tested with deterministic simulation testing (DST).
- Use `ASSERT(...)` / `ASSERT_FMT(...)` for internal invariants that must always hold.
- When adding new invariants, add debug asserts at the point of assumption in `src/`.
- Keep external input/IO/runtime error handling as normal return/error paths; assertions are for logic/state invariants.

## Comments

- Keep comments concise.
- Only comment code that is not already obvious.
- Add inline comments only for non-obvious logic: bit tricks, io_uring behavior, syscalls, performance rationale, and architecture decisions.
- Prefer // over or /* */ whenever possible

## Prerequisites

```bash
# Core build/runtime deps (Ubuntu/Debian names)
sudo apt install cmake clang-21 liburing-dev

# Needed for real network integration tests and manual Redis-client checks
sudo apt install redis-tools
```

## Build

```bash
# Debug
cmake -B build -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21 -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j$(nproc)

# Release
cmake -B build -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21 -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

Protocol selection is compile-time via CMake cache var:
- `-DDALAHASH_PROTOCOL=redis` (default)
- `-DDALAHASH_PROTOCOL=echo`

## Run Server (all supported CLI modes)

```bash
# Default: port 6379, workers = online CPU count
./build/dalahash

# Custom port
./build/dalahash --port 6380

# Custom worker count
./build/dalahash --workers 1

# Custom port + workers
./build/dalahash --port 6380 --workers 1

# Help
./build/dalahash --help
```

Important: in Redis mode, each worker has its own in-memory `Store` in v1 (not shared). For predictable key visibility across clients, use `--workers 1`.

## Quick manual client check

```bash
redis-cli -h 127.0.0.1 -p 6379 PING
redis-cli -h 127.0.0.1 -p 6379 SET foo bar
redis-cli -h 127.0.0.1 -p 6379 GET foo
```

## AddressSanitizer (ASan)

```bash
# Build with ASan (use Debug for best stack traces)
cmake -B build -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21 \
      -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON
cmake --build build -j$(nproc)

# Run tests under ASan
ctest --test-dir build --output-on-failure

# Run server under ASan - LeakSanitizer reports on exit (Ctrl+C)
./build/dalahash --workers 1
```

## LeakSanitizer (LSan, standalone)

```bash
# Build with standalone LeakSanitizer
cmake -B build -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21 \
      -DCMAKE_BUILD_TYPE=Debug -DENABLE_LSAN=ON
cmake --build build -j$(nproc)

# Run tests with leak detection
ctest --test-dir build --output-on-failure

# Run server (leak report is emitted on clean exit)
./build/dalahash --workers 1
```

## Tests (all supported ways)

```bash
# Build tests
cmake --build build -j$(nproc)

# List discovered tests
ctest --test-dir build -N

# Run everything
ctest --test-dir build --output-on-failure
```

### Run test categories via CTest regex

```bash
# Smoke + RESP parser + command unit tests
ctest --test-dir build --output-on-failure -R '^(Smoke|Resp|Command)\.'

# DST unit tests (simulated backend)
ctest --test-dir build --output-on-failure -R '^DST\.'

# DST integration tests (worker_run + SimIoBackend)
ctest --test-dir build --output-on-failure -R '^DSTIntegration\.'

# Echo DST tests (echo protocol build)
ctest --test-dir build --output-on-failure -R '^EchoDST\.'

# Real io_uring Redis integration tests (redis protocol build; uses redis-cli)
ctest --test-dir build --output-on-failure -R '^IoUringIntegrationTest\.'

# Real echo integration tests (echo protocol build)
ctest --test-dir build --output-on-failure -R '^EchoIntegrationTest\.'

# Single test example
ctest --test-dir build --output-on-failure -R '^DSTIntegration\.SendFailClosesConnection$'
```

### Run test binaries directly

```bash
# Unit tests (smoke, RESP, command)
./build/tests/dalahash_tests

# DST tests (unit + integration against SimIoBackend)
./build/tests/dst_tests

# Real io_uring integration tests
./build/tests/io_uring_integration_tests

# Echo integration tests (echo protocol build)
./build/tests/echo_integration_tests

# Example gtest filter
./build/tests/dst_tests --gtest_filter='DSTIntegration.*'
```

Notes:
- `io_uring_integration_tests` require `redis-cli` in `PATH` and a compatible Linux/io_uring environment.
- Those tests self-skip when io_uring backend support is unavailable in the environment.

## Architecture

### Thread-per-core model

Each worker thread is pinned to one CPU core (`pthread_setaffinity_np`). Each worker owns:
- One `io_uring` ring (never shared - `IORING_SETUP_SINGLE_ISSUER`).
- One listen socket with `SO_REUSEPORT`.
- One flat `Connection *conns[MAX_CONNECTIONS]` table indexed by fixed file index.
- One protocol state object (`ProtocolWorkerState`).
  - In current Redis build, `ProtocolWorkerState` contains one per-thread `Store` (no locks in v1).

`server.cpp` spawns `N` worker threads (default: one per online CPU via `sysconf(_SC_NPROCESSORS_ONLN)`), each running `worker_run()`.

### io_uring setup flags

`IORING_SETUP_COOP_TASKRUN` - disables IPI delivery; task work only runs when we enter the kernel.
`IORING_SETUP_DEFER_TASKRUN` - defers CQE delivery until completions are explicitly requested.
`IORING_SETUP_SINGLE_ISSUER` - only the owning thread may submit SQEs.
`IORING_SETUP_SUBMIT_ALL` - continue SQ batch submission past individual SQE failures.
`IORING_SETUP_CQSIZE` - sets CQ depth to `4 * ring_size`.

Constants in `server.cpp`: `RING_SIZE=4096`, `BUF_COUNT=1024`, `BUF_SIZE=4096`.

### Multishot operations and rearm

Accept and recv use multishot SQEs (one submission -> many CQEs).
`IORING_CQE_F_MORE` in `cqe->flags` means the SQE is still armed; when absent, worker must resubmit. This is surfaced as `IoCompletion::more`.

### Fixed file table

`io_uring_register_files_sparse(MAX_CONNECTIONS)` pre-registers `MAX_CONNECTIONS=65536` empty slots.
`io_uring_prep_multishot_accept_direct` fills slots automatically (`IORING_FILE_INDEX_ALLOC`). On completion, `cqe->res` is the fixed file index (not an OS fd).
All recv/send SQEs use `IOSQE_FIXED_FILE`.
`io_uring_prep_close_direct` releases fixed-file slot + socket.

### Ring fd registration

`io_uring_register_ring_fd` registers the ring fd so later `io_uring_enter` calls avoid ring-fd `fget` overhead.

### TCP_NODELAY (async via io_uring)

`io_uring_prep_cmd_sock` with `SOCKET_URING_OP_SETSOCKOPT` sets `TCP_NODELAY` asynchronously using `IOSQE_FIXED_FILE`.
Requires kernel 6.7+ / liburing 2.7+.
The optval pointer (`g_tcp_nodelay_on`) must be `static`.
Completion is encoded as `IoCompletion::IGNORE` and consumed silently in `uring_wait`.

### CQ overflow detection

When `IORING_FEAT_NODROP` is available, overflow CQEs go to kernel backlog instead of dropping.
`io_uring_cq_has_overflow` is checked after waits to warn when CQ pressure is high.

### User data encoding

SQE `user_data` packs `(kind << 32) | fd`.
`IGNORE` is used for internal completions (e.g., async TCP_NODELAY).

### Wait loop: `io_uring_submit_and_wait_timeout`

Used instead of `io_uring_submit_and_wait` because `DEFER_TASKRUN` + timeout path (`IORING_ENTER_EXT_ARG`) is signal-interruptible (`-EINTR`).
Timeout is 100ms so shutdown flag is checked periodically.

### Provided buffer ring (kernel-managed pool)

`io_uring_register_buf_ring` registers a pre-allocated recv buffer pool.
Recv SQEs use `IOSQE_BUFFER_SELECT`; completion buffer ID is in `cqe->flags >> IORING_CQE_BUFFER_SHIFT`.
Worker recycles buffers (batched via `recycle_buffers` when backend supports it).

### DST seam: `IoOps` / `IoBackend`

`src/net/io.h` defines `IoOps` and opaque `IoBackend`.
For transport I/O, `worker.cpp` only calls through `IoOps` (`submit_*`, `wait`, `recycle_*`).
Production backend: `src/net/io_uring_backend.cpp`.
Simulation backend: `tests/net/sim_io_backend.h`.

`SimIoBackend` is an in-process stateful test backend:
- Tests push scripted `IoCompletion` events into `sim.pending`.
- `wait()` pops events.
- `submit_send` captures output per fd for assertions.
- Failure injection supports ENOSPC and other edge cases.

### Protocol seam (compile-time selected)

`src/protocol/protocol.h` is the protocol selection point used by `worker.cpp`.
- Selection is compile-time via `DALAHASH_PROTOCOL_*` defines from CMake.
- Supported selections:
  - `DALAHASH_PROTOCOL_REDIS=1`
  - `DALAHASH_PROTOCOL_ECHO=1`
- Worker hot path calls protocol adapter functions:
  - `protocol_parse(...)`
  - `protocol_execute(...)`
  - `protocol_worker_init(...)`

This keeps the transport layer generic while avoiding runtime virtual dispatch on the per-command path.

### RESP2 protocol (zero-allocation parser)

Redis protocol implementation lives in `src/protocol/redis/redis_protocol.h` and delegates to:
- `src/redis/resp.cpp` (`resp_parse`)
- `src/redis/command.cpp` (`command_execute`)

Echo protocol implementation lives in `src/protocol/echo/echo_protocol.h` and treats each recv payload as one message to echo back unchanged.

`src/redis/resp.cpp` keeps `RespArg.data` pointers into the recv buffer (no parse-time copying).
`resp_parse` returns `OK`, `INCOMPLETE`, or `ERROR`.
Response formatters write into caller-provided output buffers.

### TCP reassembly + pipeline execution (`handle_recv`)

`handle_recv` in `worker.cpp`:
1. If `conn->input_len > 0`, append new bytes into `conn->input_buf` (bounded by `CONN_BUF_SIZE`).
2. Parse loop: `protocol_parse` + `protocol_execute` until `INCOMPLETE` or `ERROR`.
3. Coalesce multiple command responses from one recv into `response_batch`.
4. On batch-full, enqueue current batch to TX queue.
5. On `INCOMPLETE`, preserve trailing bytes in `conn->input_buf`.
6. On parse error, close connection.
7. Rearm recv when multishot terminated.
8. Recycle provided buffers after processing.

### TX queue and async send ownership

Responses are never sent from stack memory.
Each connection owns a FIFO TX queue (`TxChunk`) with one send in flight at a time:
- `tx_enqueue` copies response bytes into owned chunk memory.
- `submit_tx_head` sends the remaining bytes of head chunk.
- Partial sends advance `tx_head_sent` and resubmit remainder.
- Completed head chunks are recycled to slab pool classes (`256`, `1024`, `4096`, `16384`) or freed (large allocations).
- Backpressure limit: `TX_HIGH_WATERMARK_BYTES` (1 MiB per connection). Exceeding it closes the connection.

### Close retry behavior

If `submit_close` returns `-ENOSPC`, fd is queued in pending-close retry list and retried in later loop iterations.
Only `-ENOSPC` is retried; other close submission errors destroy connection state.

### Key constants

| Constant | Value | Location |
|---|---|---|
| `MAX_CONNECTIONS` | 65536 | `src/net/connection.h` |
| `CONN_BUF_SIZE` | 16384 | `src/net/connection.h` |
| `RING_SIZE` | 4096 | `src/net/server.cpp` |
| `BUF_COUNT` | 1024 | `src/net/server.cpp` |
| `BUF_SIZE` | 4096 | `src/net/server.cpp` |
| `MAX_COMPLETIONS` | 256 | `src/net/worker.cpp` |
| `MAX_PENDING_CLOSE` | 256 | `src/net/worker.cpp` |
| `RESPONSE_BUF_SIZE` | 65536 | `src/net/worker.cpp` |
| `TX_HIGH_WATERMARK_BYTES` | 1048576 | `src/net/worker.cpp` |
| `TX_POOL_GROW_BATCH` | 64 | `src/net/worker.cpp` |

### Key files

| File | Purpose |
|---|---|
| `src/main.cpp` | CLI args (`--port`, `--workers`, `--help`) and server entrypoint |
| `src/net/io.h` | DST seam: `IoOps`, `IoBackend`, `IoCompletion` |
| `src/net/io_uring_backend.cpp` | Production io_uring backend |
| `src/net/worker.cpp` | Per-core event loop, reassembly, TX queue, close retry logic |
| `src/net/server.cpp` | Worker spawning, signal handling |
| `src/net/connection.h` | Per-connection state |
| `src/protocol/protocol.h` | Compile-time protocol selection used by worker |
| `src/protocol/redis/redis_protocol.h` | Redis protocol adapter used by the generic worker |
| `src/protocol/echo/echo_protocol.h` | Echo protocol adapter used by the generic worker |
| `src/redis/resp.h/.cpp` | RESP2 parser and response formatters |
| `src/redis/command.h/.cpp` | Command dispatch (GET, SET, PING, COMMAND stub) |
| `src/redis/store.h` | Per-thread key-value store (`std::unordered_map`) |
| `tests/net/sim_io_backend.h` | Simulated backend for DST |
| `tests/net/dst_test.cpp` | DST unit and DST integration tests |
| `tests/net/io_uring_integration_test.cpp` | Real network integration tests via redis-cli |
| `tests/net/dst_echo_test.cpp` | DST tests for echo protocol |
| `tests/net/echo_integration_test.cpp` | Real network integration tests for echo protocol |
| `tests/redis/resp_test.cpp` | RESP parser/formatter unit tests |
| `tests/redis/command_test.cpp` | Command dispatch unit tests |
| `tests/smoke_test.cpp` | Build sanity smoke test |

### Known gotchas

- `IORING_SETUP_DEFER_TASKRUN` requires `io_uring_submit_and_wait_timeout`; plain `io_uring_submit_and_wait` is not signal-interruptible in this mode.
- `accept_direct` completion returns fixed file index (`cqe->res`), not OS fd.
- `g_tcp_nodelay_on` must be `static` because kernel reads it asynchronously.
- `io_uring_register_ring_fd` should be called last in `uring_init`.
- `-fno-exceptions` is mandatory (no `try`/`catch`).
- Protocol is compile-time selected via `DALAHASH_PROTOCOL`; supported values are `redis` and `echo`.
- In Redis mode, store is per-worker (inside `ProtocolWorkerState`), not shared. With `--workers > 1`, key visibility depends on which worker accepted each connection.
- Only close-submit `-ENOSPC` is retried; other close-submit failures are treated as terminal.
- `store_get` uses `std::string(key)` lookup, which allocates on every GET (known inefficiency).
