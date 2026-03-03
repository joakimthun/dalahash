# AGENTS.md

## Project

- `dalahash` is a C++ in-memory data store.
- The project targets high-throughput, low-latency operation and is tuned to run close to hardware limits.
- Transport is a thread-per-core `io_uring` server.
- Protocol support is compile-time selected.

### Current command surface

Redis mode:

- `GET`
- `SET`
- `SETEX`
- `PING`
  - `PING` returns `PONG`
  - `PING <message>` echoes the message as a bulk string
- `COMMAND` (stub for `redis-cli`; returns `*0\r\n`)

Memcached mode (text protocol):

- Legacy: `get`, `set`, `delete`, `version`
- Meta: `mg`, `ms`, `md`, `mn`
- Client flags are preserved via a 4-byte big-endian value prefix
- `noreply` / quiet mode is supported where implemented

Echo mode:

- Raw TCP echo

## Rules And Standards

- Use CMake.
- Use `clang-21` / `clang++-21`.
- Use `clang-format-21` (or `clang-format` if it resolves to LLVM 21) with the repo-root `.clang-format`.
- Tests use gtest.
- Use C++23 (`CMAKE_CXX_STANDARD 23`).
- Do not use exceptions for error handling (`-fno-exceptions`).
- Prefer simple code and avoid unnecessary templates or complex abstractions.
- Use C++ casts (`static_cast`, `reinterpret_cast`, `const_cast`) instead of C-style casts.
- Use DST (deterministic simulation testing) for transport logic.
- Use `ASSERT(...)` / `ASSERT_FMT(...)` for internal invariants.
- Add invariants where assumptions are made inside `src/`.
- Keep external input, IO, and runtime failures as normal error-return paths.
- After any C++ code or comment change, run formatting before handing work off.

## Comments

- Keep comments concise.
- Comment only non-obvious logic, performance rationale, syscall behavior, or architecture constraints.
- Prefer `//` comments.

## Prerequisites

```bash
# Core build/runtime deps (Ubuntu/Debian names)
sudo apt install cmake clang-21 clang-format-21 liburing-dev

# Real Redis integration tests and manual Redis checks
sudo apt install redis-tools

# End-to-end benchmark helper
sudo apt install memtier

# Memcached benchmark readiness checks
sudo apt install netcat-openbsd
```

## Build

```bash
# Debug
cmake -B build \
  -DCMAKE_C_COMPILER=clang-21 \
  -DCMAKE_CXX_COMPILER=clang++-21 \
  -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j"$(nproc)"

# Release
cmake -B build \
  -DCMAKE_C_COMPILER=clang-21 \
  -DCMAKE_CXX_COMPILER=clang++-21 \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

Protocol selection is compile-time via CMake cache variable:

- `-DDALAHASH_PROTOCOL=redis` (default)
- `-DDALAHASH_PROTOCOL=memcached`
- `-DDALAHASH_PROTOCOL=echo`

Shared KV implementation selection is separate:

- `-DDALAHASH_KV_IMPL=v1`
- `-DDALAHASH_KV_IMPL=v2` (default)

Optional build toggles:

- `-DENABLE_ASAN=ON`
- `-DENABLE_LSAN=ON`
- `-DENABLE_BENCHMARKS=ON`

## Formatting

```bash
# Format all tracked C/C++ sources
cmake --build build --target format

# Format specific files directly
clang-format-21 -i src/foo.cpp src/foo.h
```

## Run Server

```bash
# Default: port 6379, workers = online CPU count, store = 256 MiB
./build/dalahash

# Custom port
./build/dalahash --port 6380

# Auto worker detection (same as default)
./build/dalahash --workers 0

# Custom worker count
./build/dalahash --workers 1

# Custom shared store capacity
./build/dalahash --store-bytes $((512 * 1024 * 1024))

# Custom port + workers + store capacity
./build/dalahash --port 6380 --workers 1 --store-bytes $((128 * 1024 * 1024))

# Help
./build/dalahash --help
```

CLI notes:

- `--workers 0` means auto-detect from `_SC_NPROCESSORS_ONLN`.
- Invalid `--workers`, `--port`, and `--store-bytes` values fail fast with exit code `1`.

## Quick Manual Client Checks

Redis mode:

```bash
redis-cli -h 127.0.0.1 -p 6379 PING
redis-cli -h 127.0.0.1 -p 6379 PING hello
redis-cli -h 127.0.0.1 -p 6379 SET foo bar
redis-cli -h 127.0.0.1 -p 6379 GET foo
```

Memcached mode:

```bash
printf "version\r\n" | nc -q 1 127.0.0.1 6379
printf "set k 0 0 3\r\nfoo\r\nget k\r\n" | nc -q 1 127.0.0.1 6379
printf "mn\r\n" | nc -q 1 127.0.0.1 6379
```

## AddressSanitizer

```bash
cmake -B build \
  -DCMAKE_C_COMPILER=clang-21 \
  -DCMAKE_CXX_COMPILER=clang++-21 \
  -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_ASAN=ON
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
./build/dalahash --workers 1
```

## LeakSanitizer

```bash
cmake -B build \
  -DCMAKE_C_COMPILER=clang-21 \
  -DCMAKE_CXX_COMPILER=clang++-21 \
  -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_LSAN=ON
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
./build/dalahash --workers 1
```

## Benchmarks

Build benchmark targets:

```bash
cmake -B build \
  -DCMAKE_C_COMPILER=clang-21 \
  -DCMAKE_CXX_COMPILER=clang++-21 \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_BENCHMARKS=ON
cmake --build build -j"$(nproc)" --target \
  shared_kv_single_thread_bench \
  shared_kv_multi_thread_bench \
  redis_resp_bench \
  memcached_protocol_bench
```

Run benchmark binaries:

```bash
./build/bench/shared_kv_single_thread_bench
./build/bench/shared_kv_multi_thread_bench
./build/bench/redis_resp_bench
./build/bench/memcached_protocol_bench
```

End-to-end benchmark helper:

```bash
# Redis mode
bash bench/run_benchmark.sh

# Memcached mode
bash bench/run_benchmark.sh --memcached

# CMake wrapper target (requires redis or memcached protocol build)
cmake --build build --target e2e_benchmark
```

## Tests

```bash
# Build tests
cmake --build build -j"$(nproc)"

# List tests
ctest --test-dir build -N

# Run everything
ctest --test-dir build --output-on-failure
```

### Useful CTest filters

```bash
# CLI + server + smoke + Redis unit tests
ctest --test-dir build --output-on-failure -R '^(Cli|Server|Smoke|Resp|Command)\.'

# Shared KV unit/concurrency tests
ctest --test-dir build --output-on-failure -R '^SharedKv\.'

# DST tests (Redis protocol build)
ctest --test-dir build --output-on-failure -R '^(DST|DSTIntegration)\.'

# Redis io_uring integration tests
ctest --test-dir build --output-on-failure -R '^IoUringIntegrationTest\.'

# Redis fuzz tests
ctest --test-dir build --output-on-failure -R '^Fuzz\.'

# Echo DST tests
ctest --test-dir build --output-on-failure -R '^EchoDST\.'

# Echo integration tests
ctest --test-dir build --output-on-failure -R '^EchoIntegrationTest\.'

# Memcached parser + command unit tests
ctest --test-dir build --output-on-failure -R '^(McParse|McCommand)\.'

# Memcached DST tests
ctest --test-dir build --output-on-failure -R '^DSTMemcached\.'

# Memcached integration tests
ctest --test-dir build --output-on-failure -R '^MemcachedIntegration\.'

# Memcached fuzz tests
ctest --test-dir build --output-on-failure -R '^MemcachedFuzz\.'
```

### Test binaries

```bash
# Always built
./build/tests/dalahash_tests

# Redis protocol build
./build/tests/dst_tests
./build/tests/io_uring_integration_tests
./build/tests/fuzz_tests

# Echo protocol build
./build/tests/dst_echo_tests
./build/tests/echo_integration_tests

# Memcached protocol build
./build/tests/dst_memcached_tests
./build/tests/memcached_integration_tests
./build/tests/memcached_fuzz_tests
```

Notes:

- `io_uring_integration_tests` require `redis-cli` and a compatible Linux `io_uring` environment.
- Integration tests self-skip when the runtime kernel/environment does not support the required backend.

## Architecture

### Thread-per-core model

Each worker thread is pinned to one CPU core (`pthread_setaffinity_np`). Each worker owns:

- One `io_uring` ring (`IORING_SETUP_SINGLE_ISSUER`)
- One listen socket with `SO_REUSEPORT`
- One `Connection* conns[MAX_CONNECTIONS]` table indexed by fixed-file index
- One protocol worker state object

`server.cpp` creates one shared `KvStore` in `server_start()` and passes that shared store to every worker. In Redis and memcached builds, `protocol_worker_init(...)` binds each worker's local `Store` wrapper to that shared `KvStore` and registers the worker id, so keys are visible across workers.

### Protocol seam

`src/protocol/protocol.h` is the compile-time selection point used by `worker.cpp`.

- `DALAHASH_PROTOCOL_REDIS=1`
- `DALAHASH_PROTOCOL_MEMCACHED=1`
- `DALAHASH_PROTOCOL_ECHO=1`

Worker hot paths call:

- `protocol_parse(...)`
- `protocol_execute(...)`
- `protocol_worker_init(...)`

### io_uring setup flags

- `IORING_SETUP_COOP_TASKRUN`
- `IORING_SETUP_DEFER_TASKRUN`
- `IORING_SETUP_SINGLE_ISSUER`
- `IORING_SETUP_SUBMIT_ALL`
- `IORING_SETUP_CQSIZE`

Current startup constants in `src/net/server.cpp`:

- `RING_SIZE = 4096`
- `BUF_COUNT = 1024`
- `BUF_SIZE = 4096`

### Fixed file table

- `io_uring_register_files_sparse(MAX_CONNECTIONS)` pre-registers the fixed file table.
- `io_uring_prep_multishot_accept_direct(..., IORING_FILE_INDEX_ALLOC)` allocates fixed-file slots directly.
- Accept completions return the fixed-file index, not the OS fd.
- Send/recv submissions use `IOSQE_FIXED_FILE`.
- `io_uring_prep_close_direct` closes the socket and releases the slot.

### Receive path

`handle_recv()` in `src/net/worker.cpp`:

1. Appends trailing bytes into `conn->input_buf` when a previous command was incomplete.
2. Repeatedly calls `protocol_parse()` and `protocol_execute()` until parse returns `INCOMPLETE` or `ERROR`.
3. Copies each response directly into the per-connection TX queue via `tx_enqueue(...)`.
4. Preserves incomplete trailing bytes for the next recv.
5. Closes the connection on parse or protocol error.
6. Rearms recv when multishot terminates.
7. Recycles provided buffers after processing.

### TX queue

- Responses are never sent from stack memory.
- Each connection owns a FIFO queue of `TxChunk` buffers.
- `tx_enqueue(...)` copies protocol output into owned memory.
- Only one send is in flight per connection.
- Partial sends advance `tx_head_sent` and resubmit the remaining bytes.
- `TX_HIGH_WATERMARK_BYTES` is 1 MiB per connection; exceeding it closes the connection.

### Shared store wrapper

`src/store/store.h` is the protocol-facing wrapper around `KvStore`.

- Workers bind to the shared `KvStore` with `store_bind_shared(...)`.
- Unit tests and protocol-only helpers can still lazily create a local fallback store via `store_ensure_local(...)`.
- `store_get(...)` / `store_get_at(...)` return `StoreValueView` views into store-owned memory and do not allocate on successful lookups.

## Key Files

| File | Purpose |
|---|---|
| `src/main.cpp` | Main entrypoint |
| `src/cli.h` | CLI parsing for `--port`, `--workers`, `--store-bytes`, `--help` |
| `src/net/server.cpp` | Worker startup, shared-store creation, signal handling |
| `src/net/server.h` | Server config and testable runtime seam |
| `src/net/worker.cpp` | Per-worker event loop, recv/parse/execute/send path |
| `src/net/io.h` | `IoOps`, `IoBackend`, `IoCompletion` seam |
| `src/net/io_uring_backend.cpp` | Production `io_uring` backend |
| `src/net/connection.h` | Per-connection state |
| `src/protocol/protocol.h` | Compile-time protocol selection |
| `src/protocol/redis/redis_protocol.h` | Redis adapter |
| `src/protocol/memcached/memcached_protocol.h` | Memcached adapter |
| `src/protocol/echo/echo_protocol.h` | Echo adapter |
| `src/redis/resp.h/.cpp` | RESP2 parser and formatters |
| `src/redis/command.h/.cpp` | Redis command execution |
| `src/memcached/memcached_parse.h/.cpp` | Memcached text parser |
| `src/memcached/memcached_response.h/.cpp` | Memcached response formatting |
| `src/memcached/memcached_command.h/.cpp` | Memcached command execution |
| `src/store/store.h` | Protocol-facing store wrapper |
| `src/kv/shared_kv_store.h` | Shared KV public API |
| `src/kv/shared_kv_store.cpp` | KV implementation v1 |
| `src/kv/shared_kv_store_v2.cpp` | KV implementation v2 |
| `src/kv/shared_kv_store_internal_stats.h` | Internal stats exposed to v2 benchmarks |
| `src/kv/DESIGN.md` | v1 KV design notes |
| `src/kv/DESIGN_V2.md` | v2 KV design notes |
| `tests/CMakeLists.txt` | Test target wiring |
| `tests/cli_test.cpp` | CLI parser unit tests |
| `tests/net/server_test.cpp` | Server startup failure tests |
| `tests/net/sim_io_backend.h` | Simulated backend for DST |
| `tests/net/dst_test.cpp` | Redis DST tests |
| `tests/net/io_uring_integration_test.cpp` | Redis real-network integration tests |
| `tests/net/fuzz_test.cpp` | Redis fuzz/integration stress |
| `tests/net/dst_echo_test.cpp` | Echo DST tests |
| `tests/net/echo_integration_test.cpp` | Echo integration tests |
| `tests/net/dst_memcached_test.cpp` | Memcached DST tests |
| `tests/net/memcached_integration_test.cpp` | Memcached integration tests |
| `tests/net/memcached_fuzz_test.cpp` | Memcached fuzz tests |
| `tests/redis/resp_test.cpp` | RESP unit tests |
| `tests/redis/command_test.cpp` | Redis command unit tests |
| `tests/memcached/memcached_parse_test.cpp` | Memcached parser unit tests |
| `tests/memcached/memcached_command_test.cpp` | Memcached command unit tests |
| `bench/CMakeLists.txt` | Benchmark target wiring |
| `bench/shared_kv_single_thread_bench.cpp` | Single-thread shared KV benchmarks |
| `bench/shared_kv_multi_thread_bench.cpp` | Multi-thread shared KV benchmarks |
| `bench/redis_resp_bench.cpp` | Redis parse/format/execute benchmarks |
| `bench/memcached_protocol_bench.cpp` | Memcached parse/format/execute benchmarks |
| `bench/run_benchmark.sh` | End-to-end memtier benchmark driver |

## Known Gotchas

- `IORING_SETUP_DEFER_TASKRUN` requires `io_uring_submit_and_wait_timeout`; plain `io_uring_submit_and_wait` is not used here.
- `accept_direct` completions return fixed-file indices, not OS fds.
- `g_tcp_nodelay_on` must stay `static` because the kernel reads it asynchronously.
- `io_uring_register_ring_fd` should be called last during ring setup.
- `-fno-exceptions` is mandatory.
- Protocol selection (`DALAHASH_PROTOCOL`) and KV implementation selection (`DALAHASH_KV_IMPL`) are separate build knobs.
- All workers share one `KvStore`; do not assume per-worker key isolation.
- Only close-submit `-ENOSPC` is retried; other close-submit failures are treated as terminal.
