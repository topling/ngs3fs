#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
run_dir=${1:-"$project_dir/build/profiles/ngs3fs-$run_id"}
port=${PORT:-17072}
iterations=${ITERATIONS:-3000}
read_ahead=${READ_AHEAD:-256KiB}
bytes=${BYTES:-1048576}

versitygw="$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw"
ngs3fs=${NGS3FS_BIN:-"$project_dir/build/dev/ngs3fs"}
bench=${MMAP_BENCH_BIN:-"$project_dir/build/dev/mmap_fault_bench"}
perf_root="$project_dir/build/tools/perf-6.8/root"
perf="$perf_root/usr/lib/linux-tools-6.8.0-138/perf"
perf_lib="$perf_root/usr/lib/x86_64-linux-gnu"
flamegraph_dir="$project_dir/build/tools/FlameGraph"
backend="$run_dir/backend"
bucket=ngs3fs-profile
object=object.bin
mount_dir="$run_dir/mnt"
access_key=ngs3fs-profile
secret_key=ngs3fs-profile-secret
endpoint="http://127.0.0.1:$port"

server_pid=
ngs3fs_pid=
perf_pid=

cleanup() {
  set +e
  if [[ -n "$perf_pid" ]]; then
    kill -INT "$perf_pid" 2>/dev/null
    wait "$perf_pid" 2>/dev/null
  fi
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
  for ((attempt = 0; attempt != 100; ++attempt)); do
    if mountpoint -q "$mount_dir"; then
      return
    fi
    if ! kill -0 "$ngs3fs_pid" 2>/dev/null; then
      echo "ngs3fs exited before mounting" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for ngs3fs mount" >&2
  return 1
}

trap cleanup EXIT INT TERM

for binary in "$versitygw" "$ngs3fs" "$bench" "$perf" \
              "$flamegraph_dir/stackcollapse-perf.pl" \
              "$flamegraph_dir/flamegraph.pl"; do
  if [[ ! -x "$binary" ]]; then
    echo "missing executable: $binary" >&2
    exit 1
  fi
done

mkdir -p "$backend/$bucket" "$mount_dir"
dd if=/dev/urandom of="$backend/$bucket/$object" \
  bs=1M count=256 status=none
dd if="$backend/$bucket/$object" of=/dev/null bs=8M status=none

ROOT_ACCESS_KEY_ID=$access_key ROOT_SECRET_ACCESS_KEY=$secret_key \
  "$versitygw" --port "127.0.0.1:$port" --keep-alive --quiet \
    posix "$backend" >"$run_dir/versity.log" 2>&1 &
server_pid=$!
wait_for_server

AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
  "$ngs3fs" -f -R "$read_ahead" -e 127.0.0.1 -p "$port" \
    -a "127.0.0.1:$port" \
    -b "$bucket" "$mount_dir" \
    >"$run_dir/ngs3fs.log" 2>&1 &
ngs3fs_pid=$!
wait_for_mount

"$bench" "$mount_dir/$object" "$bytes" 50 17825792 \
  >"$run_dir/warmup.jsonl"

LD_LIBRARY_PATH=$perf_lib "$perf" record -F 499 -e cycles:u \
  --call-graph dwarf,16384 -p "$ngs3fs_pid" \
  -o "$run_dir/perf.data" -- sleep 3600 &
perf_pid=$!
sleep 0.2
"$bench" "$mount_dir/$object" "$bytes" "$iterations" 17825792 \
  >"$run_dir/mmap.jsonl"
kill -INT "$perf_pid" 2>/dev/null || true
wait "$perf_pid" 2>/dev/null || true
perf_pid=

LD_LIBRARY_PATH=$perf_lib "$perf" script -i "$run_dir/perf.data" \
  >"$run_dir/perf.script"
"$flamegraph_dir/stackcollapse-perf.pl" "$run_dir/perf.script" \
  >"$run_dir/perf.folded"
"$flamegraph_dir/flamegraph.pl" --width 1600 \
  --title "ngs3fs $bytes-byte cold mmap faults" \
  --subtitle "VersityGW v1.7.0, HTTP/1.1, 499 Hz userspace cycles" \
  --countname samples "$run_dir/perf.folded" \
  >"$run_dir/ngs3fs-$bytes.svg"
python3 "$project_dir/bench/build_interactive_flamegraph.py" \
  "$run_dir/perf.folded" "$run_dir/ngs3fs-$bytes-interactive.html" \
  --title "ngs3fs $bytes-byte cold mmap faults"

LD_LIBRARY_PATH=$perf_lib "$perf" report --stdio --no-children \
  --percent-limit 0.5 -i "$run_dir/perf.data" \
  >"$run_dir/perf-self.txt"
LD_LIBRARY_PATH=$perf_lib "$perf" report --stdio --children \
  --percent-limit 0.5 -i "$run_dir/perf.data" \
  >"$run_dir/perf-inclusive.txt"

printf '%s\n' "$run_dir"
