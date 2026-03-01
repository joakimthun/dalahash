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

## Notes

- The repo built successfully with Clang 20.1.8 on Amazon Linux 2023.
- On multi-core hosts, `RLIMIT_NOFILE` can cap the per-worker io_uring fixed-file table size. Current builds log the applied per-worker slot limit at startup instead of failing worker initialization with `Too many open files`.
- The CMake `format` target was not generated in this environment because the repo currently looks for `clang-format-21` or `clang-format`, while Amazon Linux installed `clang-format-20`.
