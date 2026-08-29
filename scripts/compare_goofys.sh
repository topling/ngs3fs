#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
run_dir=${1:-"$project_dir/build/e2e/compare-$run_id"}
port=${PORT:-17071}
iterations=${ITERATIONS:-300}
object_mib=${OBJECT_MIB:-256}
order=${ORDER:-forward}
metrics=${METRICS:-0}
read_ahead=${READ_AHEAD:-256KiB}

versitygw="$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw"
ngs3fs=${NGS3FS_BIN:-"$project_dir/build/dev/ngs3fs"}
goofys=/home/leipeng/.cache/goofys-reference/bin/goofys
bench=${MMAP_BENCH_BIN:-"$project_dir/build/dev/mmap_fault_bench"}
backend="$run_dir/backend"
bucket=ngs3fs-bench
object=object.bin
object_path="$backend/$bucket/$object"
ngs3fs_mount="$run_dir/mnt-ngs3fs"
goofys_mount="$run_dir/mnt-goofys"
access_key=ngs3fs-bench
secret_key=ngs3fs-bench-secret
endpoint="http://127.0.0.1:$port"

server_pid=
ngs3fs_pid=
goofys_pid=

cleanup() {
  set +e
  if mountpoint -q "$goofys_mount"; then
    fusermount3 -u "$goofys_mount"
  fi
  if mountpoint -q "$ngs3fs_mount"; then
    fusermount3 -u "$ngs3fs_mount"
  fi
  if [[ -n "$goofys_pid" ]]; then
    kill "$goofys_pid" 2>/dev/null
    wait "$goofys_pid" 2>/dev/null
  fi
  if [[ -n "$ngs3fs_pid" ]]; then
    kill "$ngs3fs_pid" 2>/dev/null
    wait "$ngs3fs_pid" 2>/dev/null
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null
    wait "$server_pid" 2>/dev/null
  fi
}

wait_for_server() {
  for ((attempt = 0; attempt != 100; ++attempt)); do
    if curl --silent --output /dev/null "$endpoint/"; then
      return
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "versitygw exited before becoming ready" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for versitygw" >&2
  return 1
}

wait_for_mount() {
  local mount_dir=$1
  local daemon_pid=$2
  local name=$3
  for ((attempt = 0; attempt != 100; ++attempt)); do
    if mountpoint -q "$mount_dir"; then
      return
    fi
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
      echo "$name exited before mounting" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for $name mount" >&2
  return 1
}

process_cpu_ns() {
  local pid=$1
  local total=0
  local runtime
  local ignored
  local stat
  for stat in "/proc/$pid/task"/*/schedstat; do
    if read -r runtime ignored <"$stat"; then
      total=$((total + runtime))
    fi
  done
  printf '%s\n' "$total"
}

run_case() {
  local client=$1
  local mount_dir=$2
  local daemon_pid=$3
  local bytes=$4
  local stride=$5
  local daemon_log=$6
  local stem="$run_dir/$client-$bytes"
  local first_line
  local last_line
  local perf_pid
  local start_ns
  local end_ns
  local used_ns

  "$bench" "$mount_dir/$object" "$bytes" 20 "$stride" \
    >"$stem-warmup.jsonl"

  first_line=$(wc -l <"$daemon_log")
  start_ns=$(process_cpu_ns "$daemon_pid")
  if perf --version >/dev/null 2>&1; then
    perf stat --no-big-num -x, \
      -e task-clock,cycles:u,instructions:u,context-switches,cpu-migrations,page-faults \
      -p "$daemon_pid" -o "$stem-perf.csv" sleep 3600 &
    perf_pid=$!
    sleep 0.1
  else
    perf_pid=
    printf '%s\n' "perf unavailable for the running kernel" \
      >"$stem-perf.csv"
  fi
  "$bench" "$mount_dir/$object" "$bytes" "$iterations" "$stride" \
    >"$stem-wall.jsonl"
  end_ns=$(process_cpu_ns "$daemon_pid")
  used_ns=$((end_ns - start_ns))
  printf 'iterations,total_ns,ns_per_iteration\n' \
    >"$stem-cpu.csv"
  printf '%s,%s,%s\n' "$iterations" "$used_ns" \
    "$((used_ns / iterations))" \
    >>"$stem-cpu.csv"
  if [[ -n "$perf_pid" ]]; then
    kill -INT "$perf_pid" 2>/dev/null || true
    wait "$perf_pid" 2>/dev/null || true
  fi

  last_line=$(wc -l <"$daemon_log")
  if ((last_line > first_line)); then
    sed -n "$((first_line + 1)),${last_line}p" "$daemon_log" \
      >"$stem-daemon.jsonl"
  else
    : >"$stem-daemon.jsonl"
  fi
}

trap cleanup EXIT INT TERM

for binary in "$versitygw" "$ngs3fs" "$goofys" "$bench"; do
  if [[ ! -x "$binary" ]]; then
    echo "missing executable: $binary" >&2
    exit 1
  fi
done

mkdir -p "$backend/$bucket" "$ngs3fs_mount" "$goofys_mount"
dd if=/dev/urandom of="$object_path" bs=1M count="$object_mib" status=none
dd if="$object_path" of=/dev/null bs=8M status=none

ROOT_ACCESS_KEY_ID=$access_key ROOT_SECRET_ACCESS_KEY=$secret_key \
  "$versitygw" --port "127.0.0.1:$port" --keep-alive --quiet \
    --access-log "$run_dir/versity-access.log" posix "$backend" \
    >"$run_dir/versity.log" 2>&1 &
server_pid=$!
wait_for_server

metrics_args=()
if [[ "$metrics" != 0 ]]; then
  metrics_args=(-M)
fi

AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
  "$ngs3fs" -f "${metrics_args[@]}" -R "$read_ahead" \
    -e 127.0.0.1 -p "$port" \
    -a "127.0.0.1:$port" -b "$bucket" \
    "$ngs3fs_mount" >"$run_dir/ngs3fs.log" 2>&1 &
ngs3fs_pid=$!
wait_for_mount "$ngs3fs_mount" "$ngs3fs_pid" ngs3fs

AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
  "$goofys" -f --endpoint "$endpoint/" --region us-east-1 \
    "$bucket" "$goofys_mount" >"$run_dir/goofys.log" 2>&1 &
goofys_pid=$!
wait_for_mount "$goofys_mount" "$goofys_pid" goofys

test "$(stat -c %s "$ngs3fs_mount/$object")" = "$((object_mib * 1024 * 1024))"
test "$(stat -c %s "$goofys_mount/$object")" = "$((object_mib * 1024 * 1024))"

if [[ "$order" = reverse ]]; then
  run_case goofys "$goofys_mount" "$goofys_pid" 262144 17563648 \
    "$run_dir/goofys.log"
  run_case ngs3fs "$ngs3fs_mount" "$ngs3fs_pid" 262144 17563648 \
    "$run_dir/ngs3fs.log"
  run_case ngs3fs "$ngs3fs_mount" "$ngs3fs_pid" 1048576 17825792 \
    "$run_dir/ngs3fs.log"
  run_case goofys "$goofys_mount" "$goofys_pid" 1048576 17825792 \
    "$run_dir/goofys.log"
else
  run_case ngs3fs "$ngs3fs_mount" "$ngs3fs_pid" 262144 17563648 \
    "$run_dir/ngs3fs.log"
  run_case goofys "$goofys_mount" "$goofys_pid" 262144 17563648 \
    "$run_dir/goofys.log"
  run_case goofys "$goofys_mount" "$goofys_pid" 1048576 17825792 \
    "$run_dir/goofys.log"
  run_case ngs3fs "$ngs3fs_mount" "$ngs3fs_pid" 1048576 17825792 \
    "$run_dir/ngs3fs.log"
fi

printf '%s\n' "$run_dir"
