# End-to-end Benchmark

Uses [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) to drive real Redis or memcached traffic through dalahash's full io_uring pipeline.

## Prerequisites

```bash
sudo apt install memtier redis-tools netcat-openbsd
```

Notes:

- `redis-cli` is used for Redis-mode readiness checks.
- `nc`/`netcat` is used for memcached-mode readiness checks.

## Usage

```bash
# Direct (auto-builds Release if no --binary)
bash bench/run_benchmark.sh

# Direct memcached mode
bash bench/run_benchmark.sh --memcached

# Via CMake target (builds then benchmarks)
cmake --build build-release --target e2e_benchmark

# Common overrides
bash bench/run_benchmark.sh --workers 1 --duration 10 --ratio 0:1   # all GETs
bash bench/run_benchmark.sh --pipeline 16 --clients 100              # high load
bash bench/run_benchmark.sh --memcached --workers 1 --ratio 1:0      # memcached writes
```

Run `bash bench/run_benchmark.sh --help` for all options.

## Output

Results are saved to `bench/results/<timestamp>/`:

- `summary.txt` — human-readable console output
- `results.json` — full memtier JSON for programmatic comparison
- `hdr*.hdr` — HDR latency histograms
- `config.txt` — parameters used for the run
