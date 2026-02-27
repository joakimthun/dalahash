# benchmarks

This directory contains Google Benchmark workloads for:

- `src/kv/shared_kv_store.*`
- `src/redis/resp.*`

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
  redis_resp_bench
```

## Run

Single-thread set/get:

```bash
./build/bench/shared_kv_single_thread_bench
```

Multi-thread workloads:

```bash
./build/bench/shared_kv_multi_thread_bench
```

RESP parser/formatter microbenchmarks:

```bash
./build/bench/redis_resp_bench
```

- `Mixed80_20`: 80% GET / 20% SET
- `Get100`: 100% GET hit path

Run only one benchmark family or one argument case:

```bash
./build/bench/shared_kv_single_thread_bench \
  --benchmark_filter='BM_SharedKvSingleGetHit/2000000/16/64/real_time$'

./build/bench/shared_kv_multi_thread_bench \
  --benchmark_filter='SharedKvMultiFixture/Get100/4000000/24/128/real_time/threads:8$'

./build/bench/shared_kv_multi_thread_bench \
  --benchmark_filter='SharedKvMultiFixture/Mixed80_20/8000000/16/64/real_time/threads:8$'
```

## Configurability

Each benchmark case uses these arguments:

1. `dataset_size`
2. `key_size`
3. `value_size`

Current matrix:

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

The random keys and values are deterministic and generated from fixed seeds.
