#!/usr/bin/env bash

set -euo pipefail

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
run_dir=${1:-$(mktemp -d /tmp/ngs3fs-multi-mount.XXXXXX)}
versitygw=${VERSITYGW_BIN:-$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw}
ngs3fs=${NGS3FS_BIN:-$project_dir/build/dev/ngs3fs}
race=${NGS3FS_RACE_BIN:-$project_dir/build/dev/cross_mount_race}
port=${PORT:-17176}
backend=$run_dir/backend
bucket=ngs3fs-multi-mount
prefix=data
first_mount=$run_dir/mnt-a
second_mount=$run_dir/mnt-b
# Set NGS3FS_CACHE_DIR to exercise the persistent cache. Each mount receives
# its own child directory so cache metadata and sparse data never collide.
cache_root=${NGS3FS_CACHE_DIR:-}
first_cache_dir=
second_cache_dir=
access_key=ngs3fs-multi-mount
secret_key=ngs3fs-multi-mount-secret
endpoint=http://127.0.0.1:$port
server_pid=
first_pid=
second_pid=

cleanup() {
  status=$?
  set +e
  for mount_dir in "$first_mount" "$second_mount"; do
    if mountpoint -q "$mount_dir"; then
      fusermount3 -u "$mount_dir"
    fi
  done
  for pid in "$first_pid" "$second_pid" "$server_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
    fi
  done
  if (( status != 0 )); then
    for log in "$run_dir"/*.log; do
      if [[ -f "$log" ]]; then
        echo "===== $log =====" >&2
        sed -n '1,240p' "$log" >&2
      fi
    done
  fi
  exit "$status"
}

trap cleanup EXIT

for binary in "$versitygw" "$ngs3fs" "$race"; do
  if [[ ! -x "$binary" ]]; then
    echo "missing executable: $binary" >&2
    exit 1
  fi
done

mkdir -p "$backend/$bucket/$prefix/large" \
         "$first_mount" "$second_mount"
if [[ -n "$cache_root" ]]; then
  mkdir -p "$cache_root"
  first_cache_dir=$cache_root/mnt-a
  second_cache_dir=$cache_root/mnt-b
  mkdir -p "$first_cache_dir" "$second_cache_dir"
  echo "ngs3fs local caches: $first_cache_dir, $second_cache_dir"
fi
printf 'old-object\n' >"$backend/$bucket/$prefix/race.bin"
printf 'visible-to-both\n' >"$backend/$bucket/$prefix/visible.bin"
large_entries=${NGS3FS_LARGE_DIRECTORY_ENTRIES:-5000}
for ((i = 0; i != large_entries; ++i)); do
  printf -v name 'entry-%08d' "$i"
  : >"$backend/$bucket/$prefix/large/$name"
done

ROOT_ACCESS_KEY_ID=$access_key ROOT_SECRET_ACCESS_KEY=$secret_key \
  "$versitygw" --port "127.0.0.1:$port" --keep-alive --quiet \
    posix "$backend" >"$run_dir/versity.log" 2>&1 &
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

mount_client() {
  local mount_dir=$1
  local log=$2
  local cache_dir=$3
  local mount_args=(-f -e 127.0.0.1 -p "$port" -a "127.0.0.1:$port"
    -b "$bucket" -k "$prefix" -T 0)
  if [[ -n "$cache_dir" ]]; then
    mount_args+=(--cache-dir "$cache_dir")
  fi
  AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
    "$ngs3fs" "${mount_args[@]}" "$mount_dir" >"$log" 2>&1 &
}

mount_client "$first_mount" "$run_dir/ngs3fs-a.log" "$first_cache_dir"
first_pid=$!
mount_client "$second_mount" "$run_dir/ngs3fs-b.log" "$second_cache_dir"
second_pid=$!
for mount_dir in "$first_mount" "$second_mount"; do
  for ((attempt = 0; attempt != 100; ++attempt)); do
    if mountpoint -q "$mount_dir"; then
      break
    fi
    sleep 0.1
  done
  if ! mountpoint -q "$mount_dir"; then
    echo "ngs3fs did not mount $mount_dir" >&2
    exit 1
  fi
done

cmp "$first_mount/visible.bin" "$second_mount/visible.bin"

first_count=$(find "$first_mount/large" -mindepth 1 -maxdepth 1 -type f | wc -l)
second_count=$(find "$second_mount/large" -mindepth 1 -maxdepth 1 -type f | wc -l)
if (( first_count != large_entries || second_count != large_entries )); then
  echo "large directory count mismatch: $first_count $second_count" >&2
  exit 1
fi

"$race" "$first_mount/race.bin" "$second_mount/race.bin"

rm "$second_mount/visible.bin" "$first_mount/race.bin"

printf 'multi-mount race and %d-entry directory test passed: %s\n' \
  "$large_entries" "$run_dir"
