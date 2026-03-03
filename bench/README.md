# benchmarks

This directory contains Google Benchmark microbenchmarks for:

- `src/kv/shared_kv_store.*`
- `src/redis/resp.*` and `src/redis/command.*`
- `src/memcached/memcached_parse.*`
- `src/memcached/memcached_response.*`
- `src/memcached/memcached_command.*`

Dependency resolution:

- CMake first tries `find_package(benchmark CONFIG)`.
- If not found, it falls back to `FetchContent` from GitHub.

## Build

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

## Run

Single-thread shared KV workloads:

```bash
./build/bench/shared_kv_single_thread_bench
```

Multi-thread shared KV workloads:

```bash
./build/bench/shared_kv_multi_thread_bench

# Override the default thread set (1,2,4,8,16)
./build/bench/shared_kv_multi_thread_bench --shared_kv_threads=3,5
```

`shared_kv_multi_thread_bench` accepts `--shared_kv_threads=<n[,m,...]>` to override the
default thread registrations. The spaced form also works:

```bash
./build/bench/shared_kv_multi_thread_bench --shared_kv_threads 8
```

Generate an SVG graph from the Google Benchmark JSON output:

```bash
./build/bench/shared_kv_multi_thread_bench \
  --benchmark_out=/tmp/shared_kv_multi_thread.json \
  --benchmark_out_format=json

python3 bench/plot_shared_kv_multi_thread.py \
  /tmp/shared_kv_multi_thread.json \
  /tmp/shared_kv_multi_thread.svg

# Narrow the graph to one workload/dataset when you want a cleaner chart.
python3 bench/plot_shared_kv_multi_thread.py \
  /tmp/shared_kv_multi_thread.json \
  /tmp/shared_kv_mixed_4096.svg \
  --workload Mixed80_20 \
  --dataset 4096/16/64
```

The SVG renderer creates one panel per workload, uses `threads` on the x-axis, and plots
Google Benchmark's `items_per_second` on the y-axis.

Redis RESP parse/format/execute microbenchmarks:

```bash
./build/bench/redis_resp_bench
```

Memcached parse/format/execute microbenchmarks:

```bash
./build/bench/memcached_protocol_bench
```

Current benchmark families:

- `shared_kv_single_thread_bench`: `Set`, `GetHit`, `GetMiss`, `DeleteHit`, `SetWithTtl`
- `shared_kv_multi_thread_bench`: `Mixed80_20`, `Get100`, `Get80Miss20Hit`, `Set100Overwrite`, `Delete50Set50`, `TtlChurn`
- `redis_resp_bench`: RESP parse (`GET`, `SET`, pipelined parse, mixed pipelined parse), RESP bulk formatting, Redis command execution (`SET`, `SETEX`)
- `memcached_protocol_bench`: memcached parse (legacy and meta), command execution (legacy and meta), response formatting (`VALUE`, `VA`, `HD`)

Run only one benchmark family or one argument case:

```bash
./build/bench/shared_kv_single_thread_bench \
  --benchmark_filter='BM_SharedKvSingleGetMiss/2000000/16/64/real_time$'

./build/bench/shared_kv_multi_thread_bench \
  --benchmark_filter='^SharedKvMultiFixture/(Mixed80_20|Get100)/4000000/128/256/' \
  --shared_kv_threads=3,5

./build/bench/memcached_protocol_bench \
  --benchmark_filter='BM_McCommandMetaGetValue/32/256$'
```

## Configurability

The shared KV benchmarks use these arguments:

1. `dataset_size`
2. `key_size`
3. `value_size`

Current shared-KV matrix:

- `4096, 16, 64`
- `4096, 16, 256`
- `16384, 24, 128`
- `65536, 32, 256`
- `2000000, 16, 64`
- `4000000, 16, 64`
- `8000000, 16, 64`
- `2000000, 24, 128`
- `4000000, 24, 128`
- `8000000, 24, 128`
- `4000000, 128, 256`

The Redis and memcached protocol benchmarks use smaller fixed key/value matrices tuned for parser and formatter microbenchmarks.

The generated keys and values are deterministic and use fixed seeds so results stay comparable across runs.
