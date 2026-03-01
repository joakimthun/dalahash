# End-to-end Benchmark

Uses [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) to drive real Redis traffic through dalahash's full io_uring pipeline.

## Prerequisites

```bash
sudo apt install memtier redis-tools
```

## Usage

```bash
# Direct (auto-builds Release if no --binary)
bash bench/run_benchmark.sh

# Via CMake target (builds then benchmarks)
cmake --build build-release --target benchmark

# Common overrides
bash bench/run_benchmark.sh --workers 1 --duration 10 --ratio 0:1   # all GETs
bash bench/run_benchmark.sh --pipeline 16 --clients 100              # high load
```

Run `bash bench/run_benchmark.sh --help` for all options.

## Output

Results are saved to `bench/results/<timestamp>/`:
- `summary.txt` — human-readable console output
- `results.json` — full memtier JSON for programmatic comparison
- `hdr*.hdr` — HDR latency histograms
- `config.txt` — parameters used for the run
