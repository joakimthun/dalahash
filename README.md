<p align="center">
    <img src="/img/dalahash_logo_small.png" width="450" border="0" alt="dalahash logo">
</p>

`dalahash` (terrible pun on [Dalahäst](https://en.wikipedia.org/wiki/Dala_horse)) is an in-memory data store built for high-throughput, low-latency serving on Linux. It uses a thread-per-core `io_uring` server, shares one mostly lock-free key/value store across all workers, and selects its wire protocol at build time.

Current build-time protocol modes:

- `redis`: `GET`, `SET`, `SETEX`, `PING`, `COMMAND` (stub for `redis-cli`)
- `memcached`: text `get`, `set`, `delete`, `version`, plus meta `mg`, `ms`, `md`, `mn`
- `echo`: raw TCP echo

`dalahash` does **not** support (as of right now and probably never) the full set of redis and/or memcached operations/features.

## AI usage
About 90% of the actual code and 95% of the tests, benchmarks and docs is written by AI agents. I have defined the architecture, the design of the networking and shared_kv_store but most of the actual code is written by these agents.
I have used both `OpenAI GPT-5.3-Codex` (Plus subscription) and `Claude Opus 4.6` (Claude Pro subscription).

## Redis [memtier](https://github.com/redis/memtier_benchmark) benchmarks

The result are from running both the client(memtier_benchmark) and server(dalahash) on 2 seperate c8gn.16xlarge instances in the same AWS AZ (eu-north-1a) using a "spread" EC2 placement group.

Ping latency between the client and server:
```bash
64 bytes from 172.31.24.133: icmp_seq=3 ttl=127 time=0.077 ms
64 bytes from 172.31.24.133: icmp_seq=4 ttl=127 time=0.078 ms
...
```

### Summary
Using 30 threads and 30 clients per thread with a 256 byte data size a dalahash server is able to server **3.9M operations per second with an avg client latency of 230 µs and a p99.99 client latency of 823 µs** using a 90% get 10% set workload.

With Redis pipelining dalahash can reach 18.8M operations per second, but at the cost of latency and high cpu usage. 

**Note:** I did not spend a lot of time on these benchmarks, they are here to verify the design of the server and make sure the overall design can reach the performance I had in mind.

#### 90% Get, 10% Set data
```bash
memtier_benchmark -s <server_ip> -p 6379 --ratio 1:10 --threads=30 --clients=30 --distinct-client-seed --test-time=180 --data-size=256
```
| Type   | Ops/sec    | Avg. Latency | p50 Latency | p90 Latency | p99 Latency | p99.9 Latency | p99.990 Latency |
|--------|------------|--------------|-------------|-------------|-------------|---------------|-----------------|
| Sets   | 354601.51  | 0.23192      | 0.21500     | 0.31100     | 0.49500     | 0.59900       | 0.82300         |
| Gets   | 3545941.20 | 0.23037      | 0.21500     | 0.30300     | 0.49500     | 0.59100       | 0.82300         |
| Totals | 3900542.71 | 0.23051      | 0.21500     | 0.31100     | 0.49500     | 0.59100       | 0.82300         |

#### 90% Get, 10% Set with `--pipeline=30` data

| Type   | Ops/sec     | Avg. Latency | p50 Latency | p90 Latency | p99 Latency | p99.9 Latency | p99.990 Latency |
|--------|-------------|--------------|-------------|-------------|-------------|---------------|-----------------|
| Sets   | 1713983.55  | 1.42169      | 1.36700     | 1.91100     | 2.52700     | 2.99100       | 3.27900         |
| Gets   | 17139761.36 | 1.42957      | 1.37500     | 1.92700     | 2.52700     | 3.00700       | 3.27900         |
| Totals | 18853744.91 | 1.42885      | 1.37500     | 1.92700     | 2.52700     | 3.00700       | 3.27900         |

## Prerequisites

```bash
sudo apt install cmake clang-21 clang-format-21 liburing-dev     # build
sudo apt install redis-tools                                     # Redis integration tests
```

GTest and Google Benchmark are fetched automatically by CMake.

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

Protocol selection is compile-time:

```bash
-DDALAHASH_PROTOCOL=redis      # default
-DDALAHASH_PROTOCOL=memcached
-DDALAHASH_PROTOCOL=echo
```

Optional toggles: `-DENABLE_ASAN=ON`, `-DENABLE_LSAN=ON`, `-DENABLE_BENCHMARKS=ON`.

## Run

```bash
# Defaults: port 6379, workers = online CPUs, store = 256 MiB
./build/dalahash

# Custom configuration
./build/dalahash --port 6380 --workers 4 --store-bytes $((512 * 1024 * 1024))

# Help
./build/dalahash --help
```

`--workers 0` means auto-detect from online CPUs. Invalid values for `--workers`, `--port`, or `--store-bytes` fail fast with exit code `1`.

Quick manual checks (Redis mode):

```bash
redis-cli -h 127.0.0.1 -p 6379 SET foo bar
redis-cli -h 127.0.0.1 -p 6379 GET foo
```

## Architecture

At startup, `dalahash` creates one shared `KvStore`, then launches one worker thread per configured CPU (default: one per online core). Each worker owns its own listen socket, `io_uring` ring, connection table, RX/TX buffers, and protocol state. The `KvStore` is the intentional shared hot object across workers.

### Threading and networking

- Each worker is pinned with `pthread_setaffinity_np()`, so its ring, connection table, and caches stay on one CPU and do not migrate between cores.
- Each worker creates its own non-blocking dual-stack listen socket (`AF_INET6` with `IPV6_V6ONLY=0`, accepting both IPv4 and IPv6) and enables `SO_REUSEPORT`, so the kernel hashes new flows across workers instead of forcing a shared accept lock.
- `TCP_NODELAY` is applied asynchronously via `io_uring_prep_cmd_sock` (`SOCKET_URING_OP_SETSOCKOPT`) after each accepted connection, avoiding a synchronous `setsockopt` on the accept hot path. Requires kernel 6.7+ / liburing 2.7+.
- Each worker owns one `io_uring` instance (`RING_SIZE = 4096`) configured with `IORING_SETUP_COOP_TASKRUN`, `IORING_SETUP_DEFER_TASKRUN`, `IORING_SETUP_SINGLE_ISSUER`, `IORING_SETUP_SUBMIT_ALL`, and an oversized CQ. That keeps completions on the owning thread, avoids surprise task-work wakeups, and removes submission-side locking.
- Accept and recv use multishot SQEs. Accept uses `accept_direct`, so new sockets are placed directly into the ring's fixed-file table and completions return fixed-file slot indices rather than process fds.
- The backend pre-registers a provided-buffer ring (`BUF_COUNT = 1024`, `BUF_SIZE = 4096`), a sparse fixed-file table for client sockets, and the ring fd itself. Steady-state recv/send/close therefore run on pre-registered kernel objects and avoid per-op fd lookup churn.
- Worker startup is resource-aware: if the process `RLIMIT_NOFILE` is too small, startup reduces worker count or fixed-file slots per worker up front instead of failing later during ring initialization.

### RX/TX data path

- Accepted connections are tracked in a flat `Connection* conns[MAX_CONNECTIONS]` table indexed by fixed-file slot. That keeps lookups O(1) with no extra fd map.
- The RX hot path parses directly from the provided buffer when it can. If a command is incomplete, trailing bytes are copied into a bounded per-connection reassembly buffer (`CONN_BUF_SIZE = 16384`) and resumed on the next recv.
- The protocol seam is compile-time selected (`redis`, `memcached`, or `echo`). The generic worker only calls `protocol_parse()`, `protocol_execute()`, `protocol_worker_init()`, and `protocol_worker_quiescent()`.
- Protocol execution writes replies into a bounded stack buffer (`64 KiB`) and then copies them into the per-connection TX queue. This is deliberate: async sends must never point at stack or transient parser memory.
- Each connection owns a FIFO of immutable TX chunks. Only one send is in flight per connection, partial sends advance `tx_head_sent`, and wire order always follows queue order.
- TX memory is backpressured: if queued bytes exceed `1 MiB` for a connection, the worker closes that connection instead of allowing unbounded growth.
- Recv buffers are recycled back to the kernel in batches after each completion batch, which reduces buf-ring tail updates. Close retries are only deferred for transient SQ saturation (`-ENOSPC`).

### AWS `c8gn` queue affinity

- The current tuned deployment path in [AMAZON_LINUX_BUILD.md](AMAZON_LINUX_BUILD.md) targets Amazon Linux 2023 on `c8gn.16xlarge` with ENA using `16` combined queues and `16` dalahash workers pinned to CPUs `0-15`.
- The intended mapping is `queue N -> IRQ N -> worker N -> CPU N`, with CPU `N` also preferring TX queue `N`. This keeps packet processing, userspace work, and response transmission on the same core.
- The AWS launcher disables `irqbalance`, disables RPS, configures a 1:1 XPS mapping, and pins each `Tx-Rx-N` ENA IRQ to CPU `N`. The ENA management IRQ is moved to a housekeeping CPU outside the worker set.
- This tuning is not required for correctness, but it preserves cache locality, reduces cross-core interrupts, and avoids software steering work that can hurt tail latency.

### `shared_kv_store`

- The server creates one `KvStore` in `server_start()`. `protocol_worker_init()` binds each worker's protocol-local `Store` wrapper to that same `KvStore` plus a stable `worker_id`, so keys are visible across all workers.
- The store is a sharded hash table. Each key probes exactly two candidate buckets, four slots per bucket, for a maximum of eight slot probes on lookup or placement.
- Slots hold atomic `Node*` pointers. Published nodes are immutable: writes allocate a new node and replace the slot with CAS, while reads use acquire loads and can return a `KvValueView` directly into store-owned memory without allocating.
- Expiration is lazy on access, and capacity enforcement is approximate rather than a strict instantaneous hard cap. When over target, writers use bounded second-chance eviction (`refbit`) instead of global scans or hard locks.
- Reclamation is cooperative. Removed or replaced nodes are retired into per-worker batches, and each worker publishes quiescent points from its event loop so old batches can be freed safely after all workers have advanced.
- `kv_store_quiescent()` is designed to be cheap most of the time: it usually just publishes epoch progress and returns, and only runs bounded maintenance when retire pressure builds or on a periodic cadence.
- The store is fast because the hot GET/SET path avoids mutexes, keeps probing bounded, packs metadata + key + value into one allocation, uses copy-on-write publication, prefetches candidate buckets, samples `refbit` writes to reduce write amplification, and moves most reclamation work off the request path.

### shared_kv_store benchmark performance (bench/shared_kv_multi_thread_bench.cpp)

#### 100% get
<p align="center">
    <img src="/img/shared_kv_get_perf.svg" width="450" border="0" alt="get_100">
</p>

#### 95% get, 5% set
Scales well up to 24 threads.
<p align="center">
    <img src="/img/shared_kv_mixed_perf_95_5.svg" width="450" border="0" alt="get_100">
</p>

#### 80% get, 20% set
Scales well up to 8 threads.
<p align="center">
    <img src="/img/shared_kv_mixed_perf_80_20.svg" width="450" border="0" alt="get_100">
</p>

### Memory management

- `shared_kv_store` uses size-class pools for common node sizes (`64` through `32768` bytes). Each worker has small local caches in front of global lock-free class pools, which removes most allocator contention from the hot path. Oversized nodes fall back to exact-size `malloc()` / `free()`.
- Each stored item is one packed `Node` allocation containing metadata, key bytes, and value bytes. That improves locality and avoids extra pointer chasing once a slot is found.
- Retired nodes are grouped into bounded batches before entering the global retire queue, so reclamation work is amortized and maintenance latency stays bounded.
- In the `io_uring` layer, each worker allocates one contiguous RX buffer pool plus one page-aligned buf-ring control block at init time. The steady-state recv path reuses those buffers instead of allocating per packet.
- The TX path uses a worker-local slab pool with fixed chunk classes (`256`, `1024`, `4096`, `16384` bytes) for common replies. Large replies use exact-size allocations so rare big payloads do not permanently bloat the resident slab pool.
- `Connection` objects are allocated once on accept (`calloc`) and freed on final close. Outside of connection open/close and occasional pool growth, the network hot path is designed to avoid heap allocation.

## Testing

The repo includes several test layers:

- Unit tests (GTest) for CLI parsing, assertions, smoke coverage, server startup/error paths, Redis command/RESP logic, memcached parse/command logic, and `shared_kv_store` API, concurrency, and stress behavior.
- DST (deterministic simulation testing) for transport logic using the simulated I/O backend instead of the real kernel path. There are protocol-specific DST suites for Redis, memcached, and echo.
- Real integration tests that launch the `dalahash` binary and exercise it over TCP. Redis uses `redis-cli`; echo and memcached use raw socket clients. `io_uring` integration tests self-skip when the host kernel/runtime cannot support the required backend.
- Fuzz/stress tests for Redis and memcached that send malformed or random traffic to the real server process to catch parser and lifecycle bugs under hostile input.

```bash
# Run all tests
ctest --test-dir build --output-on-failure

# Useful filters
ctest --test-dir build --output-on-failure -R '^SharedKv\.'              # shared KV
ctest --test-dir build --output-on-failure -R '^(DST|DSTIntegration)\.'  # Redis DST
ctest --test-dir build --output-on-failure -R '^IoUringIntegrationTest\.'# Redis integration
ctest --test-dir build --output-on-failure -R '^DSTMemcached\.'          # memcached DST
ctest --test-dir build --output-on-failure -R '^MemcachedIntegration\.'  # memcached integration
```

## Benchmarks

Build micro-benchmark targets (requires `-DENABLE_BENCHMARKS=ON`):

```bash
cmake --build build -j"$(nproc)" --target \
  shared_kv_single_thread_bench \
  shared_kv_multi_thread_bench \
  redis_resp_bench \
  memcached_protocol_bench
```

Run them:

```bash
./build/bench/shared_kv_single_thread_bench
./build/bench/shared_kv_multi_thread_bench
./build/bench/redis_resp_bench
./build/bench/memcached_protocol_bench
```

End-to-end benchmark (requires [memtier](https://github.com/redis/memtier_benchmark) and a running server):

```bash
bash bench/run_benchmark.sh              # Redis mode
bash bench/run_benchmark.sh --memcached  # memcached mode
```
