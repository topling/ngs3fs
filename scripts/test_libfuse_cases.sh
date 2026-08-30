#!/usr/bin/env bash

set -euo pipefail

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
libfuse_dir=$(realpath "${1:?usage: $0 LIBFUSE_SOURCE_DIR [RUN_DIR]}")
if (( $# >= 2 )); then
  run_dir=$2
  if [[ -e "$run_dir" ]]; then
    echo "run directory already exists: $run_dir" >&2
    exit 1
  fi
  mkdir -p "$run_dir"
else
  run_dir=$(mktemp -d /tmp/ngs3fs-libfuse-cases.XXXXXX)
fi

versitygw=${VERSITYGW_BIN:-$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw}
ngs3fs=${NGS3FS_BIN:-$project_dir/build/dev/ngs3fs}
checks=$libfuse_dir/test/cases/lib/checks.py
test_syscalls_source=$libfuse_dir/test/test_syscalls.c
readdir_inode_source=$libfuse_dir/test/readdir_inode.c
port=${PORT:-17174}
stress_jobs=${NGS3FS_STRESS_JOBS:-8}
stress_iterations=${NGS3FS_STRESS_ITERATIONS:-20}
backend=$run_dir/backend
backend_prefix=$backend/ngs3fs-libfuse/data
mount_dir=$run_dir/mnt
test_bin_dir=$run_dir/libfuse-test-bin
access_key=ngs3fs-libfuse
secret_key=ngs3fs-libfuse-secret
endpoint=http://127.0.0.1:$port
server_pid=
ngs3fs_pid=

cleanup() {
  status=$?
  set +e
  if mountpoint -q "$mount_dir"; then
    fusermount3 -u "$mount_dir"
  fi
  if [[ -n "$ngs3fs_pid" ]]; then
    kill "$ngs3fs_pid" 2>/dev/null
    wait "$ngs3fs_pid" 2>/dev/null
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null
    wait "$server_pid" 2>/dev/null
  fi
  if (( status != 0 )); then
    echo "===== ngs3fs log =====" >&2
    sed -n '1,240p' "$run_dir/ngs3fs.log" >&2
    echo "===== VersityGW log =====" >&2
    sed -n '1,240p' "$run_dir/versitygw.log" >&2
  fi
  exit "$status"
}

trap cleanup EXIT

for file in "$versitygw" "$ngs3fs" "$checks" \
            "$test_syscalls_source" "$readdir_inode_source"; do
  if [[ ! -e "$file" ]]; then
    echo "missing libfuse test dependency: $file" >&2
    exit 1
  fi
done
if [[ ! -x "$versitygw" || ! -x "$ngs3fs" ]]; then
  echo "VersityGW and ngs3fs must be executable" >&2
  exit 1
fi
if (( stress_jobs <= 0 || stress_iterations <= 0 )); then
  echo "stress jobs and iterations must be positive" >&2
  exit 1
fi

mkdir -p "$backend_prefix" "$mount_dir" "$test_bin_dir"
: >"$run_dir/fuse_config.h"
cc -std=gnu11 -O2 -I"$run_dir" "$test_syscalls_source" \
  -o "$test_bin_dir/test_syscalls"
cc -std=gnu11 -O2 "$readdir_inode_source" \
  -o "$test_bin_dir/readdir_inode"

ROOT_ACCESS_KEY_ID=$access_key ROOT_SECRET_ACCESS_KEY=$secret_key \
  "$versitygw" --port "127.0.0.1:$port" --keep-alive --quiet \
    posix "$backend" >"$run_dir/versitygw.log" 2>&1 &
server_pid=$!

for ((attempt = 0; attempt != 100; ++attempt)); do
  if curl --silent --output /dev/null "$endpoint/"; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    echo "VersityGW exited before becoming ready" >&2
    exit 1
  fi
  sleep 0.1
done
if ! curl --silent --output /dev/null "$endpoint/"; then
  echo "VersityGW did not become ready" >&2
  exit 1
fi

AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
  "$ngs3fs" -f -e 127.0.0.1 -p "$port" -a "127.0.0.1:$port" \
    -b ngs3fs-libfuse -k data -T 0 -m 0644 -D 0755 "$mount_dir" \
    >"$run_dir/ngs3fs.log" 2>&1 &
ngs3fs_pid=$!

for ((attempt = 0; attempt != 100; ++attempt)); do
  if mountpoint -q "$mount_dir"; then
    break
  fi
  if ! kill -0 "$ngs3fs_pid" 2>/dev/null; then
    echo "ngs3fs exited before mounting" >&2
    exit 1
  fi
  sleep 0.1
done
if ! mountpoint -q "$mount_dir"; then
  echo "ngs3fs did not mount" >&2
  exit 1
fi

export FUSE_TEST_BIN_DIR=$test_bin_dir

run_check() {
  echo "libfuse check: $1"
  python3 "$checks" "$@"
}

# Keep the selection inside ngs3fs's contract: reads and writes use separate
# opens, writes are sequential, and links, special files, and truncate are not
# supported.
run_check fuse_test_unlink "$mount_dir"
run_check fuse_test_mkdir "$mount_dir"
run_check fuse_test_rmdir "$mount_dir"
run_check fuse_test_open_read "$backend_prefix" "$mount_dir"
run_check fuse_test_open_write "$backend_prefix" "$mount_dir"
run_check fuse_test_readdir "$backend_prefix" "$mount_dir" \
  --inode-check nonzero
run_check fuse_test_statvfs "$mount_dir"

while IFS=: read -r number name; do
  echo "libfuse test_syscalls: $number $name"
  "$test_bin_dir/test_syscalls" "$mount_dir" "$number"
done <<'EOF'
1:create
8:mkdir
9:rename-file
10:rename-directory
12:seekdir
EOF

stress_worker() {
  local worker=$1
  local iteration
  trap - EXIT
  for ((iteration = 1; iteration <= stress_iterations; ++iteration)); do
    python3 "$checks" fuse_test_unlink "$mount_dir"
    python3 "$checks" fuse_test_mkdir "$mount_dir"
    python3 "$checks" fuse_test_rmdir "$mount_dir"
    python3 "$checks" fuse_test_open_read "$backend_prefix" "$mount_dir"
    python3 "$checks" fuse_test_open_write "$backend_prefix" "$mount_dir"
    python3 "$checks" fuse_test_readdir "$backend_prefix" "$mount_dir" \
      --inode-check nonzero
  done
  printf 'libfuse stress worker %d passed %d iterations\n' \
    "$worker" "$stress_iterations"
}

echo "libfuse parallel stress: $stress_jobs workers x $stress_iterations iterations"
stress_start=$SECONDS
stress_pids=()
for ((worker = 1; worker <= stress_jobs; ++worker)); do
  stress_worker "$worker" &
  stress_pids+=("$!")
done
stress_failed=0
for pid in "${stress_pids[@]}"; do
  if ! wait "$pid"; then
    stress_failed=1
  fi
done
if (( stress_failed != 0 )); then
  echo "parallel libfuse stress failed" >&2
  exit 1
fi

echo "libfuse syscall stress: $stress_iterations iterations"
for ((iteration = 1; iteration <= stress_iterations; ++iteration)); do
  for number in 1 8 9 10 12; do
    "$test_bin_dir/test_syscalls" "$mount_dir" "$number"
  done
done

if ! kill -0 "$ngs3fs_pid" 2>/dev/null || ! mountpoint -q "$mount_dir"; then
  echo "ngs3fs did not survive the libfuse stress workload" >&2
  exit 1
fi
printf 'libfuse stress passed in %d seconds\n' "$((SECONDS - stress_start))"
printf 'libfuse cases passed; logs: %s\n' "$run_dir"
