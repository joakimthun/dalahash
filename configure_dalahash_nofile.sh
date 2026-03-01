#!/usr/bin/env bash

set -euo pipefail

MAX_CONNECTIONS=65536
PROCESS_FD_RESERVE=64
PER_WORKER_FD_OVERHEAD=4

print_usage() {
    cat <<'EOF'
Usage:
  sudo ./configure_dalahash_nofile.sh WORKERS [--user USER]

Examples:
  sudo ./configure_dalahash_nofile.sh 32
  sudo ./configure_dalahash_nofile.sh 32 --user ec2-user

What it does:
  - Calculates the per-process nofile limit needed to run WORKERS workers
    without dalahash reducing the per-worker io_uring fixed-file table size.
  - Writes /etc/sysctl.d/99-dalahash-fd.conf with fs.nr_open and fs.file-max.
  - Writes /etc/security/limits.d/99-dalahash.conf for the target user.

Notes:
  - Run this as root (for example via sudo).
  - PAM limits from /etc/security/limits.d apply on the next login.
EOF
}

die() {
    printf 'error: %s\n' "$1" >&2
    exit 1
}

is_positive_int() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

round_up_power_of_two() {
    local value="$1"
    local result=1

    while (( result < value )); do
        result=$((result << 1))
    done

    printf '%s\n' "$result"
}

workers=""
target_user=""

while (( $# > 0 )); do
    case "$1" in
    -h|--help)
        print_usage
        exit 0
        ;;
    --user)
        shift
        (( $# > 0 )) || die "--user requires a value"
        target_user="$1"
        ;;
    --*)
        die "unknown option: $1"
        ;;
    *)
        [[ -z "$workers" ]] || die "WORKERS was provided more than once"
        workers="$1"
        ;;
    esac
    shift
done

[[ -n "$workers" ]] || die "WORKERS is required"
is_positive_int "$workers" || die "WORKERS must be a positive integer"

if (( EUID != 0 )); then
    die "run this script as root (for example: sudo ./configure_dalahash_nofile.sh $workers)"
fi

if [[ -z "$target_user" ]]; then
    if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
        target_user="$SUDO_USER"
    else
        target_user="${USER:-root}"
    fi
fi

required_nofile=$((PROCESS_FD_RESERVE + workers * (MAX_CONNECTIONS + PER_WORKER_FD_OVERHEAD)))
kernel_limit="$(round_up_power_of_two "$required_nofile")"

sysctl_file="/etc/sysctl.d/99-dalahash-fd.conf"
limits_file="/etc/security/limits.d/99-dalahash.conf"

cat > "$sysctl_file" <<EOF
fs.nr_open = $kernel_limit
fs.file-max = $kernel_limit
EOF

sysctl --load "$sysctl_file" >/dev/null

cat > "$limits_file" <<EOF
$target_user soft nofile $required_nofile
$target_user hard nofile $required_nofile
EOF

cat <<EOF
Configured dalahash file descriptor limits.

Workers: $workers
Target user: $target_user
Required per-process nofile limit: $required_nofile
Kernel fs.nr_open / fs.file-max: $kernel_limit
sysctl config: $sysctl_file
PAM limits config: $limits_file
 
Next steps:
  - Log out completely, then log back in so the new PAM limits apply.
  - Verify in the new shell: ulimit -n && ulimit -Hn
EOF
