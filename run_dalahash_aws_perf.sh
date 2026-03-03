#!/usr/bin/env bash

set -euo pipefail

# This launcher tunes an AWS ENA-backed host so the current dalahash runtime
# model lines up cleanly with the NIC:
#   queue N -> IRQ N -> dalahash worker N -> CPU N
#
# dalahash currently pins worker N to CPU N in src/net/worker.cpp, so this
# script deliberately chooses a worker count that matches the number of active
# hardware queues and then configures the host around that fixed mapping.
#
# The script is intentionally verbose. Every major derived value and every
# host-level mutation is logged so operators can reconstruct what happened from
# the terminal, a redirected file, or the systemd journal.

# These constants mirror the server-side fd accounting in src/net/server.cpp.
# Keeping the same numbers here ensures the shell-side RLIMIT_NOFILE math
# matches the process's own startup expectations.
MAX_CONNECTIONS=65536
PROCESS_FD_RESERVE=64
PER_WORKER_FD_OVERHEAD=4

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
default_binary="${script_dir}/build/dalahash"

timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
    printf '%s [INFO] %s\n' "$(timestamp)" "$*"
}

warn() {
    printf '%s [WARN] %s\n' "$(timestamp)" "$*" >&2
}

die() {
    printf '%s [ERROR] %s\n' "$(timestamp)" "$*" >&2
    exit 1
}

print_usage() {
    cat <<'EOF'
Usage:
  sudo ./run_dalahash_aws_perf.sh [options] [-- extra dalahash args]

Examples:
  sudo ./run_dalahash_aws_perf.sh
  sudo ./run_dalahash_aws_perf.sh --store-bytes $((8 * 1024 * 1024 * 1024))
  sudo ./run_dalahash_aws_perf.sh --no-launch

Options:
  --binary PATH        Path to the dalahash binary (default: ./build/dalahash)
  --device IFACE       NIC to tune (default: interface for the default route)
  --port PORT          Server port (default: 6379)
  --store-bytes BYTES  Pass through to dalahash
  --user USER          Run dalahash as this user (default: SUDO_USER/current user)
  --workers N          Worker count (default: NIC combined queue count)
  --no-launch          Apply tuning only; do not start dalahash
  -h, --help           Show this help

What it does:
  - Detects host CPU topology, the current cpuset, and ENA queue capacity.
  - Disables irqbalance so manually pinned IRQs stay pinned.
  - Sets the NIC combined queue count to match the dalahash worker count.
  - Keeps RPS disabled and maps XPS queue N to CPU N.
  - Pins each <iface>-Tx-Rx-N IRQ to CPU N.
  - Pins the ENA management IRQ to the highest CPU outside the worker set.
  - Raises RLIMIT_NOFILE high enough to keep dalahash at MAX_CONNECTIONS per worker.

This script is tuned for dalahash's current thread model:
  - worker N pins itself to CPU N inside src/net/worker.cpp
  - exact queue<->IRQ<->worker alignment therefore requires CPUs 0..N-1
EOF
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

is_positive_int() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_non_negative_int() {
    [[ "$1" =~ ^(0|[1-9][0-9]*)$ ]]
}

format_command() {
    local rendered=""
    local arg

    for arg in "$@"; do
        printf -v rendered '%s %q' "$rendered" "$arg"
    done

    printf '%s\n' "${rendered# }"
}

log_multiline() {
    local prefix="$1"
    local payload="$2"

    if [[ -z "$payload" ]]; then
        log "${prefix}: (no output)"
        return
    fi

    while IFS= read -r line; do
        log "${prefix}: ${line}"
    done <<< "$payload"
}

log_command_output() {
    local label="$1"
    shift

    local rendered
    local output
    local rc

    rendered="$(format_command "$@")"
    log "${label}: running ${rendered}"

    if output="$("$@" 2>&1)"; then
        log_multiline "$label" "$output"
        return 0
    fi

    rc=$?
    log_multiline "$label" "$output"
    warn "${label} failed with exit code ${rc}"
    return "$rc"
}

read_file_or_na() {
    local path="$1"

    if [[ -r "$path" ]]; then
        cat "$path"
        return 0
    fi

    printf 'n/a\n'
}

# Linux cpulist values use forms like "0-15" or "0-7,16-23". These helpers
# turn that representation into counts or membership checks so the script can
# verify that dalahash's fixed CPU numbering still fits the current cgroup.
count_cpus_in_list() {
    local list="$1"
    local -a parts
    local count=0
    local part
    local start
    local end

    IFS=, read -r -a parts <<< "$list"
    for part in "${parts[@]}"; do
        if [[ "$part" == *-* ]]; then
            start="${part%-*}"
            end="${part#*-}"
            count=$((count + end - start + 1))
            continue
        fi
        count=$((count + 1))
    done

    printf '%s\n' "$count"
}

max_cpu_in_list() {
    local list="$1"
    local -a parts
    local max_cpu=-1
    local part
    local end

    IFS=, read -r -a parts <<< "$list"
    for part in "${parts[@]}"; do
        end="$part"
        if [[ "$part" == *-* ]]; then
            end="${part#*-}"
        fi
        if (( end > max_cpu )); then
            max_cpu="$end"
        fi
    done

    printf '%s\n' "$max_cpu"
}

cpu_in_cpulist() {
    local list="$1"
    local cpu="$2"
    local -a parts
    local part
    local start
    local end

    IFS=, read -r -a parts <<< "$list"
    for part in "${parts[@]}"; do
        if [[ "$part" == *-* ]]; then
            start="${part%-*}"
            end="${part#*-}"
            if (( cpu >= start && cpu <= end )); then
                return 0
            fi
            continue
        fi
        if (( cpu == part )); then
            return 0
        fi
    done

    return 1
}

require_prefix_cpus_in_list() {
    local list="$1"
    local worker_count="$2"
    local cpu

    for ((cpu = 0; cpu < worker_count; cpu++)); do
        cpu_in_cpulist "$list" "$cpu" || return 1
    done
}

get_allowed_cpulist() {
    awk '/^Cpus_allowed_list:/ { print $2; exit }' /proc/self/status
}

get_default_device() {
    ip -o route show to default | awk '{print $5; exit}'
}

get_channel_value() {
    local dev="$1"
    local section="$2"

    ethtool -l "$dev" 2>/dev/null | awk -v want="${section}:" '
        $0 == want { in_section = 1; next }
        in_section && $1 == "Combined:" { print $2; exit }
    '
}

count_rx_queues() {
    local dev="$1"
    local count=0
    local path

    for path in /sys/class/net/"$dev"/queues/rx-*; do
        [[ -d "$path" ]] || continue
        count=$((count + 1))
    done

    printf '%s\n' "$count"
}

# /proc and sysfs affinity files use comma-separated 32-bit hex groups, most
# significant group first. These helpers build masks that match that format so
# each queue or IRQ can be pinned to one exact CPU.
bitmap_for_index() {
    local index="$1"
    local bit_count="$2"
    local groups=$(((bit_count + 31) / 32))
    local target_group=$((index / 32))
    local bit=$((index % 32))
    local group
    local value
    local out=""

    for ((group = groups - 1; group >= 0; group--)); do
        value=0
        if (( group == target_group )); then
            value=$((1 << bit))
        fi
        if [[ -n "$out" ]]; then
            out+=","
        fi
        out+="$(printf '%08x' "$value")"
    done

    printf '%s\n' "$out"
}

zero_bitmap() {
    local bit_count="$1"
    local groups=$(((bit_count + 31) / 32))
    local group
    local out=""

    for ((group = groups - 1; group >= 0; group--)); do
        if [[ -n "$out" ]]; then
            out+=","
        fi
        out+="00000000"
    done

    printf '%s\n' "$out"
}

log_queue_affinity_state() {
    local dev="$1"
    local worker_count="$2"
    local phase="$3"
    local queue
    local rx_dir
    local tx_dir

    log "${phase}: active queue affinity state for ${dev}"
    for ((queue = 0; queue < worker_count; queue++)); do
        rx_dir="/sys/class/net/${dev}/queues/rx-${queue}"
        tx_dir="/sys/class/net/${dev}/queues/tx-${queue}"

        log "${phase}: queue=${queue} rps_cpus=$(read_file_or_na "${rx_dir}/rps_cpus") rps_flow_cnt=$(read_file_or_na "${rx_dir}/rps_flow_cnt") xps_cpus=$(read_file_or_na "${tx_dir}/xps_cpus") xps_rxqs=$(read_file_or_na "${tx_dir}/xps_rxqs")"
    done
}

log_irq_affinity_state() {
    local dev="$1"
    local phase="$2"
    local line
    local irq
    local name
    local affinity

    log "${phase}: IRQ affinity state"
    while IFS= read -r line; do
        [[ "$line" == *:* ]] || continue

        irq="${line%%:*}"
        irq="${irq//[[:space:]]/}"
        [[ -n "$irq" ]] || continue

        name="${line##* }"
        case "$name" in
        "${dev}-Tx-Rx-"*|ena-mgmnt*)
            affinity="$(read_file_or_na "/proc/irq/${irq}/smp_affinity_list")"
            log "${phase}: irq=${irq} name=${name} affinity=${affinity}"
            ;;
        esac
    done < /proc/interrupts
}

binary="$default_binary"
device=""
port="6379"
store_bytes=""
target_user=""
workers=""
no_launch=0

while (( $# > 0 )); do
    case "$1" in
    -h|--help)
        print_usage
        exit 0
        ;;
    --binary)
        shift
        (( $# > 0 )) || die "--binary requires a value"
        binary="$1"
        ;;
    --device)
        shift
        (( $# > 0 )) || die "--device requires a value"
        device="$1"
        ;;
    --port)
        shift
        (( $# > 0 )) || die "--port requires a value"
        port="$1"
        ;;
    --store-bytes)
        shift
        (( $# > 0 )) || die "--store-bytes requires a value"
        store_bytes="$1"
        ;;
    --user)
        shift
        (( $# > 0 )) || die "--user requires a value"
        target_user="$1"
        ;;
    --workers)
        shift
        (( $# > 0 )) || die "--workers requires a value"
        workers="$1"
        ;;
    --no-launch)
        no_launch=1
        ;;
    --)
        shift
        break
        ;;
    *)
        die "unknown option: $1"
        ;;
    esac
    shift
done

extra_dalahash_args=("$@")

log "Starting dalahash AWS performance launcher"
log "Initial options: binary=${binary} device=${device:-auto} port=${port} store_bytes=${store_bytes:-default} user=${target_user:-auto} workers=${workers:-auto} no_launch=${no_launch}"
if (( ${#extra_dalahash_args[@]} > 0 )); then
    log "Extra dalahash args: $(format_command "${extra_dalahash_args[@]}")"
fi

(( EUID == 0 )) || die "run this script as root (for example: sudo ./run_dalahash_aws_perf.sh)"

require_cmd awk
require_cmd ethtool
require_cmd ip

if [[ -z "$target_user" ]]; then
    if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
        target_user="$SUDO_USER"
    else
        target_user="${USER:-root}"
    fi
fi
log "Resolved target user: ${target_user}"

if [[ "$binary" != /* ]]; then
    binary="${PWD}/${binary}"
fi
[[ -x "$binary" ]] || die "dalahash binary not found or not executable: $binary"
log "Resolved dalahash binary: ${binary}"

if [[ -z "$device" ]]; then
    device="$(get_default_device)"
fi
[[ -n "$device" ]] || die "failed to detect the default-route network interface"
[[ -d "/sys/class/net/${device}" ]] || die "network interface not found: $device"
log "Selected network interface: ${device}"

host_cpulist="$(cat /sys/devices/system/cpu/online 2>/dev/null || true)"
if [[ -z "$host_cpulist" ]]; then
    require_cmd nproc
    host_cpu_count="$(nproc --all 2>/dev/null || nproc)"
    is_positive_int "$host_cpu_count" || die "failed to determine the host CPU count"
    host_cpulist="0-$((host_cpu_count - 1))"
fi

host_cpu_count="$(count_cpus_in_list "$host_cpulist")"
host_highest_cpu="$(max_cpu_in_list "$host_cpulist")"
is_positive_int "$host_cpu_count" || die "failed to determine the host CPU count"
is_non_negative_int "$host_highest_cpu" || die "failed to determine the highest host CPU id"

allowed_cpulist="$(get_allowed_cpulist)"
if [[ -z "$allowed_cpulist" ]]; then
    allowed_cpulist="$host_cpulist"
fi
allowed_cpu_count="$(count_cpus_in_list "$allowed_cpulist")"
is_positive_int "$allowed_cpu_count" || die "failed to determine the current allowed CPU set"

log "Host CPU topology: online=${host_cpulist} count=${host_cpu_count} highest_cpu=${host_highest_cpu}"
log "Current cpuset constraint: allowed=${allowed_cpulist} count=${allowed_cpu_count}"

max_combined="$(get_channel_value "$device" "Pre-set maximums")"
current_combined="$(get_channel_value "$device" "Current hardware settings")"
if ! is_positive_int "${max_combined:-}"; then
    max_combined="$(count_rx_queues "$device")"
fi
is_positive_int "${max_combined:-}" || die "failed to determine NIC queue count for ${device}"

if [[ -z "$workers" ]]; then
    workers="$max_combined"
    if (( workers > host_cpu_count )); then
        workers="$host_cpu_count"
    fi
fi

is_positive_int "$workers" || die "--workers must be a positive integer"
is_positive_int "$port" || die "--port must be a positive integer"
if (( port > 65535 )); then
    die "--port must be <= 65535"
fi
if [[ -n "$store_bytes" ]] && ! is_positive_int "$store_bytes"; then
    die "--store-bytes must be a positive integer"
fi
if (( workers > host_cpu_count )); then
    die "--workers (${workers}) exceeds host CPUs (${host_cpu_count})"
fi
if (( workers > allowed_cpu_count )); then
    die "--workers (${workers}) exceeds the current allowed CPU count (${allowed_cpu_count})"
fi
if (( workers > max_combined )); then
    die "--workers (${workers}) exceeds ${device} combined queue capacity (${max_combined})"
fi

local_cpulist="$(cat "/sys/class/net/${device}/device/local_cpulist" 2>/dev/null || true)"
if [[ -z "$local_cpulist" ]]; then
    local_cpulist="$host_cpulist"
fi
if ! require_prefix_cpus_in_list "$local_cpulist" "$workers"; then
    die "${device} local CPUs are ${local_cpulist}, but dalahash currently pins workers to CPUs 0-$((workers - 1)); exact IRQ<->worker alignment would require a CPU-affinity change in dalahash"
fi
if ! require_prefix_cpus_in_list "$allowed_cpulist" "$workers"; then
    die "current allowed CPUs are ${allowed_cpulist}, but dalahash currently pins workers to CPUs 0-$((workers - 1)); run this script from a cpuset that includes CPUs 0-$((workers - 1))"
fi

log "NIC topology: local_cpus=${local_cpulist} max_combined=${max_combined} current_combined=${current_combined:-unknown}"
log "Chosen dalahash worker count: ${workers}"
log "Expected mapping: queue N -> IRQ N -> worker N -> CPU N"

# Dump the current NIC state before any changes. This is noisy on purpose: it
# captures the exact starting point before the script rewrites queue masks.
log_command_output "NIC driver info" ethtool -i "$device" || true
log_command_output "NIC channel layout" ethtool -l "$device" || true
log_command_output "NIC interrupt coalescing" ethtool -c "$device" || true
log_command_output "NIC RSS indirection table" ethtool -x "$device" || true
log_queue_affinity_state "$device" "$workers" "Before queue tuning"
log_irq_affinity_state "$device" "Before IRQ tuning"

if command -v systemctl >/dev/null 2>&1; then
    if systemctl list-unit-files irqbalance.service >/dev/null 2>&1; then
        if systemctl is-active --quiet irqbalance || systemctl is-enabled --quiet irqbalance; then
            log "Disabling irqbalance so manual IRQ pinning remains stable"
            systemctl disable --now irqbalance >/dev/null 2>&1 || warn "failed to disable irqbalance; it may rewrite IRQ affinities"
        else
            log "irqbalance is already disabled"
        fi
    else
        log "irqbalance.service is not installed on this host"
    fi
else
    log "systemctl is not available; skipping irqbalance management"
fi

if is_positive_int "${current_combined:-}" && (( current_combined != workers )); then
    log "Changing ${device} combined queue count from ${current_combined} to ${workers}"
    ethtool -L "$device" combined "$workers"
else
    log "Leaving ${device} combined queue count unchanged"
fi

current_combined="$(get_channel_value "$device" "Current hardware settings")"
log "Effective ${device} combined queue count after update: ${current_combined:-unknown}"

log "Reprogramming ${device} RSS indirection table evenly across ${workers} queues"
if ! ethtool -X "$device" equal "$workers" >/dev/null 2>&1; then
    warn "failed to program RSS indirection table; ENA defaults may still be acceptable"
fi

cpu_bitmap_width=$((host_highest_cpu + 1))
queue_bitmap_width="$workers"
disabled_rps_mask="$(zero_bitmap "$cpu_bitmap_width")"
log "CPU mask width for affinity files: ${cpu_bitmap_width} bits"
log "Target disabled RPS mask: ${disabled_rps_mask}"

for ((queue = 0; queue < workers; queue++)); do
    rx_dir="/sys/class/net/${device}/queues/rx-${queue}"
    tx_dir="/sys/class/net/${device}/queues/tx-${queue}"
    target_xps_cpus="$(bitmap_for_index "$queue" "$cpu_bitmap_width")"
    target_xps_rxqs="$(bitmap_for_index "$queue" "$queue_bitmap_width")"

    [[ -d "$rx_dir" ]] || die "missing RX queue directory: ${rx_dir}"
    [[ -d "$tx_dir" ]] || die "missing TX queue directory: ${tx_dir}"

    log "Queue ${queue}: target rps_cpus=${disabled_rps_mask} rps_flow_cnt=0 xps_cpus=${target_xps_cpus} xps_rxqs=${target_xps_rxqs}"

    if [[ -w "${rx_dir}/rps_cpus" ]]; then
        printf '%s' "$disabled_rps_mask" > "${rx_dir}/rps_cpus"
    else
        warn "Queue ${queue}: ${rx_dir}/rps_cpus is not writable"
    fi

    if [[ -w "${rx_dir}/rps_flow_cnt" ]]; then
        printf '0' > "${rx_dir}/rps_flow_cnt"
    else
        warn "Queue ${queue}: ${rx_dir}/rps_flow_cnt is not writable"
    fi

    if [[ -w "${tx_dir}/xps_cpus" ]]; then
        printf '%s' "$target_xps_cpus" > "${tx_dir}/xps_cpus"
    else
        warn "Queue ${queue}: ${tx_dir}/xps_cpus is not writable"
    fi

    if [[ -w "${tx_dir}/xps_rxqs" ]]; then
        printf '%s' "$target_xps_rxqs" > "${tx_dir}/xps_rxqs"
    else
        warn "Queue ${queue}: ${tx_dir}/xps_rxqs is not writable"
    fi

    log "Queue ${queue}: applied rps_cpus=$(read_file_or_na "${rx_dir}/rps_cpus") rps_flow_cnt=$(read_file_or_na "${rx_dir}/rps_flow_cnt") xps_cpus=$(read_file_or_na "${tx_dir}/xps_cpus") xps_rxqs=$(read_file_or_na "${tx_dir}/xps_rxqs")"
done

housekeeping_cpu=-1
if (( host_highest_cpu >= workers )); then
    housekeeping_cpu="$host_highest_cpu"
fi
log "Housekeeping CPU for non-data ENA interrupts: ${housekeeping_cpu}"

data_irqs_pinned=0
mgmt_irqs_pinned=0

while IFS= read -r line; do
    [[ "$line" == *:* ]] || continue

    irq="${line%%:*}"
    irq="${irq//[[:space:]]/}"
    [[ -n "$irq" ]] || continue

    name="${line##* }"
    case "$name" in
    "${device}-Tx-Rx-"*)
        queue="${name##*-}"
        if ! is_non_negative_int "$queue"; then
            warn "Skipping IRQ ${irq}: unable to parse queue index from ${name}"
            continue
        fi
        if (( queue >= workers )); then
            log "Leaving IRQ ${irq} (${name}) untouched because queue ${queue} is outside the active worker set"
            continue
        fi

        log "Pinning IRQ ${irq} (${name}) to CPU ${queue}"
        printf '%s\n' "$queue" > "/proc/irq/${irq}/smp_affinity_list"
        data_irqs_pinned=$((data_irqs_pinned + 1))
        ;;
    ena-mgmnt*)
        if (( housekeeping_cpu >= 0 )); then
            log "Pinning IRQ ${irq} (${name}) to housekeeping CPU ${housekeeping_cpu}"
            printf '%s\n' "$housekeeping_cpu" > "/proc/irq/${irq}/smp_affinity_list"
            mgmt_irqs_pinned=$((mgmt_irqs_pinned + 1))
        else
            log "Leaving IRQ ${irq} (${name}) untouched because no housekeeping CPU exists outside the worker set"
        fi
        ;;
    esac
done < /proc/interrupts

if (( data_irqs_pinned < workers )); then
    warn "pinned ${data_irqs_pinned} data IRQs for ${device}, expected at least ${workers}"
fi

log_queue_affinity_state "$device" "$workers" "After queue tuning"
log_irq_affinity_state "$device" "After IRQ tuning"
log_command_output "NIC channel layout after tuning" ethtool -l "$device" || true
log_command_output "NIC RSS indirection table after tuning" ethtool -x "$device" || true

if (( no_launch != 0 )); then
    log "Pinned ${data_irqs_pinned} data IRQs and ${mgmt_irqs_pinned} management IRQs"
    log "Skipping RLIMIT_NOFILE changes in --no-launch mode because they would apply only to this short-lived shell"
    log "Tuning applied; not starting dalahash because --no-launch was requested"
    exit 0
fi

required_nofile=$((PROCESS_FD_RESERVE + workers * (MAX_CONNECTIONS + PER_WORKER_FD_OVERHEAD)))
current_soft_nofile="$(ulimit -Sn)"
current_hard_nofile="$(ulimit -Hn)"
log "Current RLIMIT_NOFILE: soft=${current_soft_nofile} hard=${current_hard_nofile}"
log "Target RLIMIT_NOFILE for ${workers} workers: ${required_nofile}"

ulimit -Hn "$required_nofile"
ulimit -Sn "$required_nofile"

log "Updated RLIMIT_NOFILE: soft=$(ulimit -Sn) hard=$(ulimit -Hn)"
log "Pinned ${data_irqs_pinned} data IRQs and ${mgmt_irqs_pinned} management IRQs"

cmd=("$binary" "--port" "$port" "--workers" "$workers")
if [[ -n "$store_bytes" ]]; then
    cmd+=("--store-bytes" "$store_bytes")
fi
cmd+=("${extra_dalahash_args[@]}")

log "Launch command: $(format_command "${cmd[@]}")"
log "Working directory: ${script_dir}"

cd "$script_dir"

if [[ "$target_user" == "root" ]]; then
    log "Launching dalahash directly as root"
    exec "${cmd[@]}"
fi

require_cmd sudo
log "Launching dalahash via sudo -u ${target_user}"
exec sudo -u "$target_user" -- "${cmd[@]}"
