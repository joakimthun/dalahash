# Amazon Linux Build Notes

Verified on March 1, 2026 on Amazon Linux 2023 (`Amazon Linux 2023.10.20260216`, `aarch64`) against `/home/ec2-user/dalahash`.

## What Was Missing

The base EC2 image did not have the required build tools installed.

- `cmake` was missing, and the Amazon Linux repo version is only `3.22.2`, which is too old for this repo's `cmake_minimum_required(VERSION 3.25)`.
- `clang-21` is not available from the default Amazon Linux 2023 repos. The newest available packaged Clang is `clang-20`.
- `git`, `make`, `ninja`, and `liburing-devel` were also missing.

## Exact Steps Used

1. Install the base build dependencies:

```bash
sudo dnf install -y \
  git \
  make \
  ninja-build \
  gcc-c++ \
  libstdc++-static \
  clang20 \
  clang20-devel \
  clang20-libs \
  python3-pip \
  liburing-devel
```

2. Install a newer user-local CMake (this installed `cmake 4.2.3` in `~/.local/bin/cmake`):

```bash
python3 -m pip install --user --upgrade cmake
```

3. Configure and build the project with the packaged Clang 20 toolchain:

```bash
cd /home/ec2-user/dalahash
~/.local/bin/cmake -B build \
  -DCMAKE_C_COMPILER=/usr/bin/clang-20 \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++-20 \
  -DCMAKE_BUILD_TYPE=Debug
~/.local/bin/cmake --build build -j$(nproc)
```

4. The first build configured successfully, but the link step for `dalahash` failed with:

```text
/usr/bin/ld: cannot find -lstdc++: No such file or directory
```

5. Fix that linker error by installing the GCC 14 static libstdc++ package:

```bash
sudo dnf install -y gcc14-libstdc++-static
```

Clang 20 on Amazon Linux 2023 selects the GCC 14 toolchain by default. This project links `dalahash` with `-static-libstdc++ -static-libgcc`, so the matching GCC 14 static C++ runtime package is required.

6. Re-run the build:

```bash
cd /home/ec2-user/dalahash
~/.local/bin/cmake --build build -j$(nproc)
```

## Result

The build completed successfully after installing `gcc14-libstdc++-static`.

Quick checks that passed:

- `./build/dalahash --help`
- `ctest -N` from `/home/ec2-user/dalahash/build`

`./build/dalahash --help` printed:

```text
Usage: dalahash [--port PORT] [--workers N] [--store-bytes BYTES]
```

## Recommended Fresh Setup

If you are setting up a new Amazon Linux 2023 host from scratch, this one-shot package install avoids the linker issue up front:

```bash
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
python3 -m pip install --user --upgrade cmake
```

Then build with:

```bash
cd /home/ec2-user/dalahash
~/.local/bin/cmake -B build \
  -DCMAKE_C_COMPILER=/usr/bin/clang-20 \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++-20 \
  -DCMAKE_BUILD_TYPE=Debug
~/.local/bin/cmake --build build -j$(nproc)
```

## Configure FD Limits

This repo now includes `configure_dalahash_nofile.sh` in the same folder as this document. It calculates the required `nofile` limit from the worker count and updates:

- `/etc/sysctl.d/99-dalahash-fd.conf`
- `/etc/security/limits.d/99-dalahash.conf`

Run it as root from the repo root:

```bash
sudo ./configure_dalahash_nofile.sh 32
```

That configures the kernel caps plus the PAM limits for the current login user (when run through `sudo`, that is usually `ec2-user`).

You can target a different login user explicitly:

```bash
sudo ./configure_dalahash_nofile.sh 32 --user ec2-user
```

After running the script:

- Fully log out and log back in before checking `ulimit -n`.
- Verify in the new shell with `ulimit -n && ulimit -Hn`.

## Install The Performance Launcher And Managed Service

This repo now includes:

- `run_dalahash_aws_perf.sh`
- `systemd/dalahash.slice`
- `systemd/dalahash.service`

These files are the current "production" host-tuning path for the inspected AWS profile:

- instance class: `c8gn.16xlarge`
- OS: Amazon Linux 2023
- NIC: ENA with `16` combined queues
- dalahash workers: `16`

The design assumption is:

- `queue N -> IRQ N -> dalahash worker N -> CPU N`

That matches the current dalahash runtime, which pins worker `N` to CPU `N`.

### What The Files Do

- `run_dalahash_aws_perf.sh`
  - detects the default NIC and host CPU topology
  - disables `irqbalance`
  - keeps the ENA queue count aligned with the dalahash worker count
  - disables RPS, configures XPS, and pins each `Tx-Rx-N` IRQ to CPU `N`
  - pins the ENA management IRQ to a housekeeping CPU outside the worker set
  - emits detailed timestamped logs to stdout/stderr (and therefore to the systemd journal)
- `systemd/dalahash.slice`
  - reserves CPUs `0-15` for dalahash
- `systemd/dalahash.service`
  - runs the tuning script as `ExecStartPre`
  - launches dalahash directly inside `dalahash.slice`
  - keeps the process under systemd supervision

### Quick Glossary

- `RPS` = `Receive Packet Steering`
  - a Linux software path that can move receive-side packet processing from the CPU that took the NIC interrupt to some other CPU set via `rps_cpus`
  - this setup disables it because the ENA hardware queues are already pinned 1:1 to the intended worker CPUs, so extra software steering would add cross-core work and usually hurt locality
- `XPS` = `Transmit Packet Steering`
  - a Linux transmit-side mapping that tells the kernel which TX queue(s) a CPU should prefer via `xps_cpus` (and optionally `xps_rxqs`)
  - this setup enables a 1:1 mapping so CPU `N` prefers TX queue `N`, which keeps outbound traffic aligned with dalahash worker `N`

For this profile, the goal is:

- RX queue `N` -> IRQ `N` -> CPU `N` -> dalahash worker `N`
- TX from CPU `N` -> TX queue `N`

### Fresh Host Install (Same 16-Queue Profile)

If you are bringing up another host with the same profile, run these steps after copying the repo and building `./build/dalahash`.

1. Copy the repo to the target host.

You can use the existing helper:

```bash
./deploy_dalahash_to_aws.sh ec2-EXAMPLE.eu-north-1.compute.amazonaws.com
```

Or copy the repo manually so it lands at:

```text
/home/ec2-user/dalahash
```

2. Build the Release binary (or your preferred build) on the host:

```bash
cd /home/ec2-user/dalahash
~/.local/bin/cmake -B build \
  -DCMAKE_C_COMPILER=/usr/bin/clang-20 \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++-20 \
  -DCMAKE_BUILD_TYPE=Release
~/.local/bin/cmake --build build -j"$(nproc)"
```

3. Install the systemd units:

```bash
cd /home/ec2-user/dalahash
chmod 0755 ./run_dalahash_aws_perf.sh
sudo install -D -m 0644 ./systemd/dalahash.slice /etc/systemd/system/dalahash.slice
sudo install -D -m 0644 ./systemd/dalahash.service /etc/systemd/system/dalahash.service
sudo systemctl daemon-reload
```

4. Reserve non-hot-path CPUs for everything except dalahash:

```bash
sudo systemctl set-property system.slice AllowedCPUs=16-63
sudo systemctl set-property user.slice AllowedCPUs=16-63
```

This leaves:

- `0-15` for dalahash (via `dalahash.slice`)
- `16-63` for general system services and user sessions

5. Enable and start the managed service:

```bash
sudo systemctl enable --now dalahash.service
```

6. Verify the result:

```bash
sudo systemctl --no-pager --full status dalahash.service
systemctl show -p AllowedCPUs system.slice user.slice dalahash.slice
systemctl show -p MainPID -p ControlGroup dalahash.service
sudo journalctl -u dalahash.service --no-pager -n 120
```

On a correctly configured host you should see:

- `dalahash.service` running in `/dalahash.slice/dalahash.service`
- `system.slice` set to `AllowedCPUs=16-63`
- `user.slice` set to `AllowedCPUs=16-63`
- `dalahash.slice` set to `AllowedCPUs=0-15`
- detailed `run_dalahash_aws_perf.sh` logs in the journal

7. Optional direct verification:

```bash
pid="$(systemctl show -p MainPID --value dalahash.service)"
grep '^Cpus_allowed_list:' "/proc/${pid}/status"
grep -E 'ens50-Tx-Rx-|ena-mgmnt' /proc/interrupts
```

The dalahash process should be allowed on `0-15`, and the ENA IRQ affinity files should show:

- `ens50-Tx-Rx-0` on CPU `0`
- ...
- `ens50-Tx-Rx-15` on CPU `15`
- `ena-mgmnt` on CPU `63`

### If The New Host Is Not The Same Shape

Do not blindly install the current unit files on a host with a different queue count or CPU layout.

Before enabling the service, check:

```bash
ethtool -l ens50
cat /sys/class/net/ens50/device/local_cpulist
cat /sys/devices/system/cpu/online
```

If the host is not a 16-queue ENA system, update these together before starting the service:

- `systemd/dalahash.slice`
  - change `AllowedCPUs=` to the worker CPU range you want dalahash to own
- `systemd/dalahash.service`
  - change `ExecStart=... --workers N`
  - change `LimitNOFILE=` to match the new worker count

Current `LimitNOFILE` math is:

```text
64 + workers * (65536 + 4)
```

For example:

- `16` workers -> `1048704`
- `8` workers -> `524384`

The launcher script itself auto-detects the NIC and CPU topology, but the systemd unit and slice intentionally encode the current service profile so the CPU partition is explicit and predictable.

## Notes

- The repo built successfully with Clang 20.1.8 on Amazon Linux 2023.
- On multi-core hosts, `RLIMIT_NOFILE` can cap the per-worker io_uring fixed-file table size. Current builds log the applied per-worker slot limit at startup instead of failing worker initialization with `Too many open files`.
- The CMake `format` target was not generated in this environment because the repo currently looks for `clang-format-21` or `clang-format`, while Amazon Linux installed `clang-format-20`.
