#!/usr/bin/env bash

set -euo pipefail

DEFAULT_KEY_PATH="/home/jthun/Downloads/dhash.pem"
DEFAULT_REMOTE_USER="ec2-user"

print_usage() {
    cat <<'EOF'
Usage:
  ./deploy_dalahash_to_aws.sh INSTANCE_DNS [--key PEM] [--user USER] [--remote-dir DIR]

Examples:
  ./deploy_dalahash_to_aws.sh ec2-16-170-227-15.eu-north-1.compute.amazonaws.com
  ./deploy_dalahash_to_aws.sh ec2-16-170-227-15.eu-north-1.compute.amazonaws.com \
    --key /path/to/key.pem

What it does:
  - Copies this repo to the remote host, excluding .git and any build* directories.
  - Installs the Amazon Linux build dependencies from AMAZON_LINUX_BUILD.md.
  - Builds a Release configuration with benchmarks enabled in /home/ec2-user/dalahash/build.
EOF
}

die() {
    printf 'error: %s\n' "$1" >&2
    exit 1
}

require_local_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required local command not found: $1"
}

stream_project_archive() {
    local project_dir="$1"

    (
        cd "$project_dir"
        find . -mindepth 1 \
            \( -type d \( -name '.git' -o -name 'build*' \) -prune \) -o \
            -print0 |
            tar --create --file=- --null --no-recursion --files-from=-
    )
}

instance_dns=""
key_path="$DEFAULT_KEY_PATH"
remote_user="$DEFAULT_REMOTE_USER"
remote_dir=""

while (( $# > 0 )); do
    case "$1" in
    -h|--help)
        print_usage
        exit 0
        ;;
    --key)
        shift
        (( $# > 0 )) || die "--key requires a value"
        key_path="$1"
        ;;
    --user)
        shift
        (( $# > 0 )) || die "--user requires a value"
        remote_user="$1"
        ;;
    --remote-dir)
        shift
        (( $# > 0 )) || die "--remote-dir requires a value"
        remote_dir="$1"
        ;;
    --*)
        die "unknown option: $1"
        ;;
    *)
        [[ -z "$instance_dns" ]] || die "INSTANCE_DNS was provided more than once"
        instance_dns="$1"
        ;;
    esac
    shift
done

[[ -n "$instance_dns" ]] || die "INSTANCE_DNS is required"
[[ -f "$key_path" ]] || die "PEM file not found: $key_path"

if [[ -z "$remote_dir" ]]; then
    remote_dir="/home/${remote_user}/dalahash"
fi

require_local_cmd find
require_local_cmd ssh
require_local_cmd tar

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ssh_target="${remote_user}@${instance_dns}"
ssh_args=(-i "$key_path" "$ssh_target")

printf '[1/3] Copying project to %s:%s\n' "$ssh_target" "$remote_dir"
stream_project_archive "$script_dir" |
    ssh "${ssh_args[@]}" "mkdir -p '$remote_dir' && tar -C '$remote_dir' --no-same-owner -xf -"

printf '[2/3] Removing remote build directories\n'
ssh "${ssh_args[@]}" "find '$remote_dir' -mindepth 1 -maxdepth 1 -type d -name 'build*' -exec rm -rf -- {} +"

printf '[3/3] Installing dependencies and building Release binaries\n'
ssh "${ssh_args[@]}" bash -s -- "$remote_dir" <<'EOF'
set -euo pipefail

remote_dir="$1"
cmake_bin="$HOME/.local/bin/cmake"

sudo dnf install -y \
    git \
    make \
    ninja-build \
    gcc-c++ \
    libstdc++-static \
    gcc14-libstdc++-static \
    clang20 \
    clang20-devel \
    clang20-libs \
    python3-pip \
    liburing-devel

if ! command -v ld.lld >/dev/null 2>&1; then
    if ! sudo dnf install -y lld20; then
        sudo dnf install -y lld
    fi
fi

python3 -m pip install --user --upgrade cmake

cd "$remote_dir"

"$cmake_bin" -B build \
    -DCMAKE_C_COMPILER=/usr/bin/clang-20 \
    -DCMAKE_CXX_COMPILER=/usr/bin/clang++-20 \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_BENCHMARKS=ON

"$cmake_bin" --build build -j"$(nproc)" --target \
    dalahash \
    shared_kv_single_thread_bench \
    shared_kv_multi_thread_bench \
    redis_resp_bench \
    memcached_protocol_bench
EOF

printf 'Remote build completed in %s/build\n' "$remote_dir"
