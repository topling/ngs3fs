#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
run_dir=${1:-"$project_dir/build/profiles/ngs3fs-$run_id"}
run_dir=$(realpath -m "$run_dir")
port=${PORT:-17072}
workload=${WORKLOAD:-mmap}
iterations=${ITERATIONS:-3000}
read_ahead=${READ_AHEAD:-256KiB}
max_connections=${MAX_CONNECTIONS:-8}
bytes=${BYTES:-1048576}
perf_event=${PERF_EVENT:-cycles:u}
perf_frequency=${PERF_FREQUENCY:-4000}
perf_stat_events=${PERF_STAT_EVENTS:-}
if [[ -z "$perf_stat_events" ]]; then
  perf_stat_events=task-clock,task-clock:u,task-clock:k
  perf_stat_events+=,context-switches,cpu-migrations,page-faults
  perf_stat_events+=,syscalls:sys_enter_splice
  perf_stat_events+=,syscalls:sys_enter_recvfrom
  perf_stat_events+=,syscalls:sys_enter_sendto
  perf_stat_events+=,syscalls:sys_enter_futex
  perf_stat_events+=,syscalls:sys_enter_writev
  perf_stat_events+=,syscalls:sys_enter_close
  perf_stat_events+=,syscalls:sys_enter_pipe2
  perf_stat_events+=,syscalls:sys_enter_fcntl
fi
random_files=${RANDOM_READ_FILES:-32}
random_threads=${RANDOM_READ_THREADS:-16}
random_operations=${RANDOM_READ_OPERATIONS:-128}
random_file_size=${RANDOM_READ_FILE_SIZE:-4194304}
random_maximum_read=${RANDOM_READ_MAXIMUM:-262144}
random_seed=${RANDOM_READ_SEED:-0x4e47533346535244}
random_advice=${RANDOM_READ_ADVICE:-random}

versitygw="$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw"
ngs3fs=${NGS3FS_BIN:-"$project_dir/build/dev/ngs3fs"}
bench=${MMAP_BENCH_BIN:-"$project_dir/build/dev/mmap_fault_bench"}
random_bench=${RANDOM_READ_BENCH_BIN:-"$project_dir/build/dev/random_read_stress"}
perf_root="$project_dir/build/tools/perf-6.8/root"
perf=${PERF_BIN:-"$perf_root/usr/lib/linux-tools-6.8.0-138/perf"}
perf_lib=${PERF_LIB_DIR:-"$perf_root/usr/lib/x86_64-linux-gnu"}
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
profile_first_line=0
profile_last_line=0
profile_get_requests=0

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

run_random_read() {
  local operations=$1
  local output=$2

  "$random_bench" -R "${random_advice_args[@]}" \
    -d "$mount_dir/random-read" \
    -f "$random_files" -t "$random_threads" -n "$operations" \
    -s "$random_file_size" -r "$random_maximum_read" -S "$random_seed" \
    >"$output"
}

trap cleanup EXIT INT TERM

required_binaries=("$versitygw" "$ngs3fs" "$perf")
random_advice_args=()
case "$random_advice" in
  random) ;;
  normal) random_advice_args=(-N) ;;
  *)
    echo "unsupported random-read advice: $random_advice" >&2
    exit 2
    ;;
esac
case "$workload" in
  mmap) required_binaries+=("$bench") ;;
  random-read) required_binaries+=("$random_bench") ;;
  *)
    echo "unsupported workload: $workload" >&2
    exit 2
    ;;
esac
for binary in "${required_binaries[@]}" \
              "$flamegraph_dir/stackcollapse-perf.pl" \
              "$flamegraph_dir/flamegraph.pl"; do
  if [[ ! -x "$binary" ]]; then
    echo "missing executable: $binary" >&2
    exit 1
  fi
done

mkdir -p "$backend/$bucket" "$mount_dir"
if [[ "$workload" = mmap ]]; then
  dd if=/dev/urandom of="$backend/$bucket/$object" \
    bs=1M count=256 status=none
  dd if="$backend/$bucket/$object" of=/dev/null bs=8M status=none
else
  "$random_bench" -p -d "$backend/$bucket/random-read" \
    -f "$random_files" -t "$random_threads" -n "$random_operations" \
    -s "$random_file_size" -r "$random_maximum_read" -S "$random_seed"
fi

ROOT_ACCESS_KEY_ID=$access_key ROOT_SECRET_ACCESS_KEY=$secret_key \
  "$versitygw" --port "127.0.0.1:$port" --keep-alive --quiet \
    --access-log "$run_dir/versity-access.log" \
    posix "$backend" >"$run_dir/versity.log" 2>&1 &
server_pid=$!
wait_for_server

AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
  "$ngs3fs" -f -R "$read_ahead" -e 127.0.0.1 -p "$port" \
    -C "$max_connections" \
    -a "127.0.0.1:$port" \
    -b "$bucket" "$mount_dir" \
    >"$run_dir/ngs3fs.log" 2>&1 &
ngs3fs_pid=$!
wait_for_mount

if [[ "$workload" = mmap ]]; then
  "$bench" "$mount_dir/$object" "$bytes" 50 17825792 \
    >"$run_dir/warmup.jsonl"
  title="ngs3fs $bytes-byte cold mmap faults"
  flame_svg="$run_dir/ngs3fs-$bytes.svg"
  flame_html="$run_dir/ngs3fs-$bytes-interactive.html"
else
  run_random_read 8 "$run_dir/warmup.txt"
  title="ngs3fs concurrent multi-file random reads ($random_advice advice)"
  flame_svg="$run_dir/ngs3fs-random-read.svg"
  flame_html="$run_dir/ngs3fs-random-read-interactive.html"
fi
profile_first_line=$(wc -l <"$run_dir/versity-access.log")

LD_LIBRARY_PATH=$perf_lib "$perf" record -F "$perf_frequency" \
  -e "$perf_event" \
  --call-graph dwarf,16384 -p "$ngs3fs_pid" \
  -o "$run_dir/perf.data" -- sleep 3600 &
perf_pid=$!
sleep 0.2
if [[ "$workload" = mmap ]]; then
  LD_LIBRARY_PATH=$perf_lib "$perf" stat --no-big-num -x, \
    -e "$perf_stat_events" -p "$ngs3fs_pid" \
    -o "$run_dir/perf-stat.csv" -- \
    "$bench" "$mount_dir/$object" "$bytes" "$iterations" 17825792 \
      >"$run_dir/mmap.jsonl"
else
  LD_LIBRARY_PATH=$perf_lib "$perf" stat --no-big-num -x, \
    -e "$perf_stat_events" -p "$ngs3fs_pid" \
    -o "$run_dir/perf-stat.csv" -- \
    "$random_bench" -R "${random_advice_args[@]}" \
      -d "$mount_dir/random-read" \
      -f "$random_files" -t "$random_threads" -n "$random_operations" \
      -s "$random_file_size" -r "$random_maximum_read" -S "$random_seed" \
      >"$run_dir/random-read.txt"
fi
profile_last_line=$(wc -l <"$run_dir/versity-access.log")
if ((profile_last_line > profile_first_line)); then
  profile_get_requests=$(
    sed -n "$((profile_first_line + 1)),${profile_last_line}p" \
      "$run_dir/versity-access.log" | grep -c 's3_GetObject' || true
  )
fi
kill -INT "$perf_pid" 2>/dev/null || true
wait "$perf_pid" 2>/dev/null || true
perf_pid=

LD_LIBRARY_PATH=$perf_lib "$perf" script -i "$run_dir/perf.data" \
  >"$run_dir/perf.script"
"$flamegraph_dir/stackcollapse-perf.pl" "$run_dir/perf.script" \
  >"$run_dir/perf.folded"
"$flamegraph_dir/flamegraph.pl" --width 1600 \
  --title "$title" \
  --subtitle "VersityGW v1.7.0, HTTP/1.1, $perf_frequency Hz $perf_event" \
  --countname samples "$run_dir/perf.folded" \
  >"$flame_svg"
python3 "$project_dir/bench/build_interactive_flamegraph.py" \
  "$run_dir/perf.folded" "$flame_html" \
  --title "$title"

LD_LIBRARY_PATH=$perf_lib "$perf" report --stdio --no-children \
  --percent-limit 0.5 -i "$run_dir/perf.data" \
  >"$run_dir/perf-self.txt"
LD_LIBRARY_PATH=$perf_lib "$perf" report --stdio --children \
  --percent-limit 0.5 -i "$run_dir/perf.data" \
  >"$run_dir/perf-inclusive.txt"
LD_LIBRARY_PATH=$perf_lib "$perf" report --stdio --no-children \
  --call-graph none --percent-limit 0.15 -i "$run_dir/perf.data" \
  >"$run_dir/perf-self-flat.txt"
LD_LIBRARY_PATH=$perf_lib "$perf" report --stdio --no-children \
  --call-graph none --sort dso --percent-limit 0.1 \
  -i "$run_dir/perf.data" >"$run_dir/perf-dso.txt"

if [[ "$workload" = random-read ]]; then
  bytes_read=
  elapsed_ns=
  pread_operations=
  mmap_operations=
  for token in $(<"$run_dir/random-read.txt"); do
    case "$token" in
      bytes=*) bytes_read=${token#bytes=} ;;
      elapsed_ns=*) elapsed_ns=${token#elapsed_ns=} ;;
      pread_operations=*) pread_operations=${token#pread_operations=} ;;
      mmap_operations=*) mmap_operations=${token#mmap_operations=} ;;
    esac
  done
  printf '%s\n' \
    'advice,files,threads,operations,pread_operations,mmap_operations,bytes,elapsed_ns,s3_get_requests,perf_event,perf_frequency' \
    >"$run_dir/profile-summary.csv"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$random_advice" "$random_files" "$random_threads" \
    "$((random_threads * random_operations))" "$pread_operations" \
    "$mmap_operations" "$bytes_read" "$elapsed_ns" \
    "$profile_get_requests" "$perf_event" "$perf_frequency" \
    >>"$run_dir/profile-summary.csv"
fi

printf '%s\n' "$run_dir"
