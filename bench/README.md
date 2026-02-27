# shared_kv_store benchmarks

This directory contains Google Benchmark workloads for `src/kv/shared_kv_store.*`.

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
  shared_kv_multi_thread_bench
```

## Run

Single-thread set/get:

```bash
./build/bench/shared_kv_single_thread_bench
```

Multi-thread mixed scaling (80% GET / 20% SET):

```bash
./build/bench/shared_kv_multi_thread_bench
```

Run only one benchmark family or one argument case:

```bash
./build/bench/shared_kv_single_thread_bench \
  --benchmark_filter='BM_SharedKvSingle(Set|GetHit)/4096/16/64/real_time$'

./build/bench/shared_kv_multi_thread_bench \
  --benchmark_filter='SharedKvMultiFixture/Mixed80_20/16384/24/128/real_time/threads:8$'
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

The random keys and values are deterministic and generated from fixed seeds.
