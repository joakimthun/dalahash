#!/usr/bin/env bash
# bench/run_benchmark.sh — End-to-end benchmark for dalahash using memtier_benchmark.
# Automates: build (optional) → start server → seed keys → run benchmark → collect results → stop server.
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
WORKERS=4
PORT=6399
THREADS=4
CLIENTS=50
RATIO="1:1"
DURATION=30
DATA_SIZE=64
KEY_MAX=100000
PIPELINE=1
PERCENTILES="50,90,99,99.9,99.99"
OUTPUT_DIR="bench/results"
DISTINCT_CLIENT_SEED=""
BINARY=""
SETEX_MODE=""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

End-to-end benchmark for dalahash using memtier_benchmark.

Options:
  --workers N        dalahash worker threads         (default: $WORKERS)
  --port N           Server port                     (default: $PORT)
  --threads N        memtier client threads           (default: $THREADS)
  --clients N        Connections per memtier thread   (default: $CLIENTS)
  --ratio R          SET:GET ratio (memtier format)   (default: $RATIO)
  --duration N       Test duration in seconds         (default: $DURATION)
  --data-size N      Value payload size in bytes      (default: $DATA_SIZE)
  --key-max N        Key space size                   (default: $KEY_MAX)
  --pipeline N       Request pipeline depth           (default: $PIPELINE)
  --percentiles P    Latency percentiles to report    (default: $PERCENTILES)
  --output-dir DIR   Where to save results            (default: $OUTPUT_DIR)
  --distinct-client-seed  Use a different random seed per client (memtier flag)
  --setex              Run additional SETEX-only benchmark pass
  --binary PATH      Path to pre-built dalahash binary (default: auto-build Release)
  -h, --help         Show this help
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

# ── Parse CLI flags ───────────────────────────────────────────────────────────
require_arg() {
    if [[ $# -lt 2 || -z "${2:-}" ]]; then
        die "$1 requires a value"
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers)    require_arg "$@"; WORKERS="$2";    shift 2 ;;
        --port)       require_arg "$@"; PORT="$2";       shift 2 ;;
        --threads)    require_arg "$@"; THREADS="$2";    shift 2 ;;
        --clients)    require_arg "$@"; CLIENTS="$2";    shift 2 ;;
        --ratio)      require_arg "$@"; RATIO="$2";      shift 2 ;;
        --duration)   require_arg "$@"; DURATION="$2";   shift 2 ;;
        --data-size)  require_arg "$@"; DATA_SIZE="$2";  shift 2 ;;
        --key-max)    require_arg "$@"; KEY_MAX="$2";    shift 2 ;;
        --pipeline)   require_arg "$@"; PIPELINE="$2";   shift 2 ;;
        --percentiles) require_arg "$@"; PERCENTILES="$2"; shift 2 ;;
        --output-dir) require_arg "$@"; OUTPUT_DIR="$2"; shift 2 ;;
        --distinct-client-seed) DISTINCT_CLIENT_SEED=1; shift ;;
        --setex)      SETEX_MODE=1; shift ;;
        --binary)     require_arg "$@"; BINARY="$2";     shift 2 ;;
        -h|--help)    usage; exit 0 ;;
        *)            echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

# ── Prerequisite checks ──────────────────────────────────────────────────────
check_command() {
    if ! command -v "$1" &>/dev/null; then
        die "'$1' not found. $2"
    fi
}

check_command memtier_benchmark \
    "Install: sudo apt install memtier  OR  see https://github.com/RedisLabs/memtier_benchmark"
check_command redis-cli \
    "Install: sudo apt install redis-tools"

# ── Build if no binary provided ──────────────────────────────────────────────
# Pin Redis protocol, Release mode, no sanitizers. Uses a fresh build dir to
# guarantee no stale cache state leaks in from prior configurations.
if [[ -z "$BINARY" ]]; then
    BUILD_DIR="$PROJECT_DIR/build-release-bench"
    echo "==> Building dalahash (Release, Redis protocol) in $BUILD_DIR ..."
    cmake -B "$BUILD_DIR" \
        -DCMAKE_C_COMPILER=clang-21 \
        -DCMAKE_CXX_COMPILER=clang++-21 \
        -DCMAKE_BUILD_TYPE=Release \
        -DDALAHASH_PROTOCOL=redis \
        -DENABLE_ASAN=OFF \
        -DENABLE_LSAN=OFF \
        -DENABLE_BENCHMARKS=OFF \
        -S "$PROJECT_DIR" >/dev/null 2>&1
    cmake --build "$BUILD_DIR" -j"$(nproc)" --target dalahash >/dev/null 2>&1
    BINARY="$BUILD_DIR/dalahash"
    echo "    Built: $BINARY"
fi

if [[ ! -x "$BINARY" ]]; then
    die "binary not found or not executable: $BINARY"
fi

# ── Validate that the binary speaks Redis protocol ───────────────────────────
# The binary embeds the protocol as a compile-time define. We verify by checking
# for the DALAHASH_PROTOCOL_REDIS symbol in the binary, or fall back to a
# runtime PING check after startup.
validate_redis_protocol() {
    # Quick check: does the binary contain the Redis RESP "+PONG" response literal?
    if strings "$BINARY" 2>/dev/null | grep -q '+PONG'; then
        return 0
    fi
    echo "WARNING: could not confirm binary is built with Redis protocol." >&2
    echo "    Benchmark assumes Redis mode. If the server fails to respond," >&2
    echo "    rebuild with: -DDALAHASH_PROTOCOL=redis" >&2
}
validate_redis_protocol

# ── Prepare output directory ─────────────────────────────────────────────────
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$OUTPUT_DIR/$TIMESTAMP"
mkdir -p "$RUN_DIR"

# ── Start server ─────────────────────────────────────────────────────────────
SERVER_PID=""
cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "==> Stopping dalahash (PID $SERVER_PID) ..."
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "==> Starting dalahash on port $PORT with $WORKERS workers ..."
"$BINARY" --port "$PORT" --workers "$WORKERS" &
SERVER_PID=$!

# Wait for server to accept connections, checking for early exit
echo -n "    Waiting for server "
for i in $(seq 1 50); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo ""
        wait "$SERVER_PID" 2>/dev/null || true
        die "dalahash exited immediately (PID $SERVER_PID). Check binary, port, or environment."
    fi
    if redis-cli -h 127.0.0.1 -p "$PORT" PING 2>/dev/null | grep -q PONG; then
        echo " ready."
        break
    fi
    if [[ $i -eq 50 ]]; then
        echo ""
        die "server did not respond to PING after 5s. Is this a Redis-protocol binary?"
    fi
    echo -n "."
    sleep 0.1
done

# ── Save run configuration ───────────────────────────────────────────────────
cat > "$RUN_DIR/config.txt" <<EOF
timestamp:    $TIMESTAMP
binary:       $BINARY
workers:      $WORKERS
port:         $PORT
threads:      $THREADS
clients:      $CLIENTS
ratio:        $RATIO
duration:     ${DURATION}s
data_size:    ${DATA_SIZE}B
key_max:      $KEY_MAX
pipeline:     $PIPELINE
distinct_client_seed: ${DISTINCT_CLIENT_SEED:-0}
EOF

# ── Build optional memtier flags ──────────────────────────────────────────────
MEMTIER_EXTRA=()
if [[ -n "$DISTINCT_CLIENT_SEED" ]]; then
    MEMTIER_EXTRA+=(--distinct-client-seed)
fi

# ── Pre-seed keys ────────────────────────────────────────────────────────────
echo "==> Pre-seeding $KEY_MAX keys (${DATA_SIZE}B values) ..."
memtier_benchmark \
    -s 127.0.0.1 -p "$PORT" \
    --protocol=redis \
    --threads=2 --clients=10 \
    --ratio=1:0 \
    --key-minimum=1 --key-maximum="$KEY_MAX" \
    --key-pattern=P:P \
    -n allkeys \
    --data-size="$DATA_SIZE" \
    --hide-histogram \
    --print-percentiles="$PERCENTILES" \
    ${MEMTIER_EXTRA[@]+"${MEMTIER_EXTRA[@]}"} \
    2>&1 | tail -n 20
echo ""

# ── Main benchmark run ───────────────────────────────────────────────────────
echo "==> Running benchmark: ${DURATION}s, ratio=$RATIO, pipeline=$PIPELINE,"
echo "    ${THREADS}t × ${CLIENTS}c = $((THREADS * CLIENTS)) connections"
echo ""

memtier_benchmark \
    -s 127.0.0.1 -p "$PORT" \
    --protocol=redis \
    --threads="$THREADS" --clients="$CLIENTS" \
    --ratio="$RATIO" \
    --key-minimum=1 --key-maximum="$KEY_MAX" \
    --key-pattern=R:R \
    --test-time="$DURATION" \
    --data-size="$DATA_SIZE" \
    --pipeline="$PIPELINE" \
    --print-percentiles="$PERCENTILES" \
    --json-out-file="$RUN_DIR/results.json" \
    --hdr-file-prefix="$RUN_DIR/hdr" \
    ${MEMTIER_EXTRA[@]+"${MEMTIER_EXTRA[@]}"} \
    2>&1 | tee "$RUN_DIR/summary.txt"

# ── Optional SETEX benchmark ─────────────────────────────────────────────────
if [[ -n "$SETEX_MODE" ]]; then
    echo ""
    echo "==> Running SETEX benchmark: ${DURATION}s, pipeline=$PIPELINE,"
    echo "    ${THREADS}t × ${CLIENTS}c = $((THREADS * CLIENTS)) connections"
    echo ""

    memtier_benchmark \
        -s 127.0.0.1 -p "$PORT" \
        --protocol=redis \
        --threads="$THREADS" --clients="$CLIENTS" \
        --command="SETEX __key__ 60 __data__" \
        --command-key-pattern=R \
        --key-minimum=1 --key-maximum="$KEY_MAX" \
        --test-time="$DURATION" \
        --data-size="$DATA_SIZE" \
        --pipeline="$PIPELINE" \
        --print-percentiles="$PERCENTILES" \
        --json-out-file="$RUN_DIR/setex_results.json" \
        --hdr-file-prefix="$RUN_DIR/setex_hdr" \
        ${MEMTIER_EXTRA[@]+"${MEMTIER_EXTRA[@]}"} \
        2>&1 | tee "$RUN_DIR/setex_summary.txt"
fi

echo ""
echo "==> Results saved to $RUN_DIR/"
ls -lh "$RUN_DIR/"
