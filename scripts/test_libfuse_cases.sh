#!/usr/bin/env bash

set -euo pipefail

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
libfuse_dir=$(realpath "${1:?usage: $0 LIBFUSE_SOURCE_DIR [RUN_DIR]}")
if (( $# >= 2 )); then
  run_dir=$(realpath -m "$2")
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
goofys=${GOOFYS_BIN:-}
client=${NGS3FS_TEST_CLIENT:-ngs3fs}
skip_cases=${NGS3FS_LIBFUSE_SKIP:-}
if [[ -z "$goofys" ]]; then
  goofys=$(command -v goofys || true)
fi
checks=$libfuse_dir/test/cases/lib/checks.py
test_syscalls_source=$libfuse_dir/test/test_syscalls.c
readdir_inode_source=$libfuse_dir/test/readdir_inode.c
port=${PORT:-17174}
stress_jobs=${NGS3FS_STRESS_JOBS:-8}
stress_iterations=${NGS3FS_STRESS_ITERATIONS:-20}
# Set NGS3FS_CACHE_DIR to exercise the persistent cache; unset keeps the
# original uncached test path. It only applies when NGS3FS_TEST_CLIENT=ngs3fs.
cache_dir=${NGS3FS_CACHE_DIR:-}
backend=$run_dir/backend
bucket=ngs3fs-libfuse
prefix=data
backend_prefix=$backend/$bucket/$prefix
mount_dir=$run_dir/mnt
test_bin_dir=$run_dir/libfuse-test-bin
access_key=ngs3fs-libfuse
secret_key=ngs3fs-libfuse-secret
endpoint=http://127.0.0.1:$port
server_pid=
client_pid=
client_log=$run_dir/$client.log

case $client in
  ngs3fs)
    client_binary=$ngs3fs
    ;;
  goofys)
    client_binary=$goofys
    ;;
  *)
    echo "NGS3FS_TEST_CLIENT must be ngs3fs or goofys" >&2
    exit 2
    ;;
esac
if [[ $client != ngs3fs && -n "$cache_dir" ]]; then
  echo "NGS3FS_CACHE_DIR ignored for test client: $client" >&2
fi

cleanup() {
  status=$?
  set +e
  if mountpoint -q "$mount_dir"; then
    fusermount3 -u "$mount_dir"
  fi
  if [[ -n "$client_pid" ]]; then
    kill "$client_pid" 2>/dev/null
    wait "$client_pid" 2>/dev/null
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null
    wait "$server_pid" 2>/dev/null
  fi
  if (( status != 0 )); then
    echo "===== $client log =====" >&2
    if [[ -f "$client_log" ]]; then
      sed -n '1,240p' "$client_log" >&2
    fi
    echo "===== VersityGW log =====" >&2
    if [[ -f "$run_dir/versitygw.log" ]]; then
      sed -n '1,240p' "$run_dir/versitygw.log" >&2
    fi
  fi
  exit "$status"
}

trap cleanup EXIT

for file in "$versitygw" "$client_binary" "$checks" \
            "$test_syscalls_source" "$readdir_inode_source"; do
  if [[ ! -e "$file" ]]; then
    echo "missing libfuse test dependency: $file" >&2
    exit 1
  fi
done
if [[ ! -x "$versitygw" || ! -x "$client_binary" ]]; then
  echo "VersityGW and $client must be executable" >&2
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

if [[ $client == ngs3fs ]]; then
  ngs3fs_mount_args=(-f -e 127.0.0.1 -p "$port" -a "127.0.0.1:$port"
    -b "$bucket" -k "$prefix" -T 0 -m 0644 -D 0755)
  if [[ -n "$cache_dir" ]]; then
    mkdir -p "$cache_dir"
    ngs3fs_mount_args+=(--cache-dir "$cache_dir")
    echo "ngs3fs local cache: $cache_dir"
  fi
  AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
    "$ngs3fs" "${ngs3fs_mount_args[@]}" "$mount_dir" \
      >"$client_log" 2>&1 &
else
  AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
    "$goofys" -f --endpoint "$endpoint/" --region us-east-1 \
      "$bucket:$prefix" "$mount_dir" >"$client_log" 2>&1 &
fi
client_pid=$!

for ((attempt = 0; attempt != 100; ++attempt)); do
  if mountpoint -q "$mount_dir"; then
    break
  fi
  if ! kill -0 "$client_pid" 2>/dev/null; then
    echo "$client exited before mounting" >&2
    exit 1
  fi
  sleep 0.1
done
if ! mountpoint -q "$mount_dir"; then
  echo "$client did not mount" >&2
  exit 1
fi

export FUSE_TEST_BIN_DIR=$test_bin_dir

case_skipped() {
  # ngs3fs deliberately reports EXDEV for a non-empty directory rename.  This
  # lets callers such as mv use their normal cross-filesystem copy/remove path
  # instead of pretending that an S3 prefix rename is atomic.
  if [[ $client = ngs3fs && $1 = rename-directory ]]; then
    return 0
  fi
  [[ " $skip_cases " == *" $1 "* ]]
}

run_check() {
  if case_skipped "$1"; then
    echo "libfuse check skipped: $1"
    return
  fi
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
  if case_skipped "$name"; then
    echo "libfuse test_syscalls skipped: $number $name"
    continue
  fi
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
    if ! case_skipped fuse_test_unlink; then
      python3 "$checks" fuse_test_unlink "$mount_dir"
    fi
    if ! case_skipped fuse_test_mkdir; then
      python3 "$checks" fuse_test_mkdir "$mount_dir"
    fi
    if ! case_skipped fuse_test_rmdir; then
      python3 "$checks" fuse_test_rmdir "$mount_dir"
    fi
    if ! case_skipped fuse_test_open_read; then
      python3 "$checks" fuse_test_open_read "$backend_prefix" "$mount_dir"
    fi
    if ! case_skipped fuse_test_open_write; then
      python3 "$checks" fuse_test_open_write "$backend_prefix" "$mount_dir"
    fi
    if ! case_skipped fuse_test_readdir; then
      python3 "$checks" fuse_test_readdir "$backend_prefix" "$mount_dir" \
        --inode-check nonzero
    fi
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
  while IFS=: read -r number name; do
    if case_skipped "$name"; then
      continue
    fi
    "$test_bin_dir/test_syscalls" "$mount_dir" "$number"
  done <<'EOF'
1:create
8:mkdir
9:rename-file
10:rename-directory
12:seekdir
EOF
done

if ! kill -0 "$client_pid" 2>/dev/null || ! mountpoint -q "$mount_dir"; then
  echo "$client did not survive the libfuse stress workload" >&2
  exit 1
fi
printf 'libfuse stress passed in %d seconds\n' "$((SECONDS - stress_start))"
printf 'libfuse cases passed for %s; logs: %s\n' "$client" "$run_dir"
