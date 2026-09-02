#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
run_dir=${1:-"$project_dir/build/e2e/compare-$run_id"}
run_dir=$(realpath -m "$run_dir")
port=${PORT:-17071}
workload=${WORKLOAD:-mmap}
client=${CLIENT:-both}
iterations=${ITERATIONS:-300}
object_mib=${OBJECT_MIB:-256}
order=${ORDER:-forward}
metrics=${METRICS:-0}
read_ahead=${READ_AHEAD:-256KiB}
socket_buffer_size=${SOCKET_BUFFER_SIZE:-2MiB}
max_connections=${MAX_CONNECTIONS:-8}
random_files=${RANDOM_READ_FILES:-32}
random_threads=${RANDOM_READ_THREADS:-16}
random_operations=${RANDOM_READ_OPERATIONS:-128}
random_file_size=${RANDOM_READ_FILE_SIZE:-4194304}
random_maximum_read=${RANDOM_READ_MAXIMUM:-262144}
random_seed=${RANDOM_READ_SEED:-0x4e47533346535244}
random_advice=${RANDOM_READ_ADVICE:-random}
perf=${PERF_BIN:-perf}

versitygw="$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw"
ngs3fs=${NGS3FS_BIN:-"$project_dir/build/dev/ngs3fs"}
goofys=${GOOFYS_BIN:-/home/leipeng/.cache/goofys-reference/bin/goofys}
bench=${MMAP_BENCH_BIN:-"$project_dir/build/dev/mmap_fault_bench"}
random_bench=${RANDOM_READ_BENCH_BIN:-"$project_dir/build/dev/random_read_stress"}
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

start_ngs3fs() {
  AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
    "$ngs3fs" -f "${metrics_args[@]}" -R "$read_ahead" \
      --socket-buffer-size "$socket_buffer_size" \
      -C "$max_connections" \
      -e 127.0.0.1 -p "$port" \
      -a "127.0.0.1:$port" -b "$bucket" \
      "$ngs3fs_mount" >"$run_dir/ngs3fs.log" 2>&1 &
  ngs3fs_pid=$!
  wait_for_mount "$ngs3fs_mount" "$ngs3fs_pid" ngs3fs
}

stop_ngs3fs() {
  fusermount3 -u "$ngs3fs_mount"
  kill "$ngs3fs_pid" 2>/dev/null || true
  wait "$ngs3fs_pid" 2>/dev/null || true
  ngs3fs_pid=
}

start_goofys() {
  AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
    "$goofys" -f --endpoint "$endpoint/" --region us-east-1 \
      "$bucket" "$goofys_mount" >"$run_dir/goofys.log" 2>&1 &
  goofys_pid=$!
  wait_for_mount "$goofys_mount" "$goofys_pid" goofys
}

stop_goofys() {
  fusermount3 -u "$goofys_mount"
  kill "$goofys_pid" 2>/dev/null || true
  wait "$goofys_pid" 2>/dev/null || true
  goofys_pid=
}

warm_random_backend() {
  find "$backend/$bucket/random-read" -type f \
    -exec dd if={} of=/dev/null bs=1M status=none \;
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
  if "$perf" --version >/dev/null 2>&1; then
    "$perf" stat --no-big-num -x, \
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

run_random_read_case() {
  local client=$1
  local mount_dir=$2
  local daemon_pid=$3
  local result
  local token
  local bytes=
  local elapsed_ns=
  local pread_operations=
  local mmap_operations=
  local start_ns
  local end_ns
  local used_ns
  local total_operations
  local first_line
  local last_line
  local get_requests
  local request_log="$run_dir/$client-random-read-requests.log"

  first_line=$(wc -l <"$run_dir/versity-access.log")
  start_ns=$(process_cpu_ns "$daemon_pid")
  result=$("$random_bench" -R "${random_advice_args[@]}" \
    -d "$mount_dir/random-read" \
    -f "$random_files" -t "$random_threads" -n "$random_operations" \
    -s "$random_file_size" -r "$random_maximum_read" -S "$random_seed")
  printf '%s\n' "$result" >"$run_dir/$client-random-read-result.txt"
  end_ns=$(process_cpu_ns "$daemon_pid")
  used_ns=$((end_ns - start_ns))
  last_line=$(wc -l <"$run_dir/versity-access.log")
  if ((last_line > first_line)); then
    sed -n "$((first_line + 1)),${last_line}p" \
      "$run_dir/versity-access.log" >"$request_log"
  else
    : >"$request_log"
  fi
  get_requests=$(grep -c 's3_GetObject' "$request_log" || true)

  for token in $result; do
    case "$token" in
      bytes=*) bytes=${token#bytes=} ;;
      elapsed_ns=*) elapsed_ns=${token#elapsed_ns=} ;;
      pread_operations=*) pread_operations=${token#pread_operations=} ;;
      mmap_operations=*) mmap_operations=${token#mmap_operations=} ;;
    esac
  done
  if [[ -z "$bytes" || -z "$elapsed_ns" ||
        -z "$pread_operations" || -z "$mmap_operations" ]]; then
    echo "unable to parse random-read result: $result" >&2
    return 1
  fi
  total_operations=$((pread_operations + mmap_operations))
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$client" "$random_advice" "$max_connections" \
    "$random_files" "$random_threads" "$total_operations" \
    "$pread_operations" "$mmap_operations" "$bytes" "$elapsed_ns" \
    "$used_ns" "$((used_ns / total_operations))" "$get_requests" \
    >>"$run_dir/random-read-summary.csv"
  printf '%s: wall_ns=%s daemon_cpu_ns=%s daemon_cpu_ns_per_operation=%s s3_get_requests=%s\n' \
    "$client" "$elapsed_ns" "$used_ns" "$((used_ns / total_operations))" \
    "$get_requests"
}

run_random_read_client() {
  local client=$1

  warm_random_backend
  if [[ "$client" = ngs3fs ]]; then
    start_ngs3fs
    test "$(stat -c %s "$ngs3fs_mount/random-read/file-0000.bin")" = \
      "$random_file_size"
    run_random_read_case ngs3fs "$ngs3fs_mount" "$ngs3fs_pid"
    stop_ngs3fs
  else
    start_goofys
    test "$(stat -c %s "$goofys_mount/random-read/file-0000.bin")" = \
      "$random_file_size"
    run_random_read_case goofys "$goofys_mount" "$goofys_pid"
    stop_goofys
  fi
}

trap cleanup EXIT INT TERM

required_binaries=("$versitygw" "$ngs3fs")
if [[ "$client" = goofys || "$client" = both ]]; then
  required_binaries+=("$goofys")
fi
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
for binary in "${required_binaries[@]}"; do
  if [[ ! -x "$binary" ]]; then
    echo "missing executable: $binary" >&2
    exit 1
  fi
done

mkdir -p "$backend/$bucket" "$ngs3fs_mount" "$goofys_mount"
if [[ "$workload" = mmap ]]; then
  dd if=/dev/urandom of="$object_path" bs=1M count="$object_mib" status=none
  dd if="$object_path" of=/dev/null bs=8M status=none
else
  "$random_bench" -p -d "$backend/$bucket/random-read" \
    -f "$random_files" -t "$random_threads" -n "$random_operations" \
    -s "$random_file_size" -r "$random_maximum_read" -S "$random_seed"
fi

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

if [[ "$workload" = random-read ]]; then
  printf '%s\n' \
    'client,advice,max_connections,files,threads,operations,pread_operations,mmap_operations,bytes,wall_ns,daemon_cpu_ns,daemon_cpu_ns_per_operation,s3_get_requests' \
    >"$run_dir/random-read-summary.csv"
  case "$client" in
    ngs3fs | goofys) run_random_read_client "$client" ;;
    both)
      if [[ "$order" = reverse ]]; then
        run_random_read_client goofys
        run_random_read_client ngs3fs
      else
        run_random_read_client ngs3fs
        run_random_read_client goofys
      fi
      ;;
    *)
      echo "unsupported client: $client" >&2
      exit 2
      ;;
  esac
else
  if [[ "$client" != both ]]; then
    echo "CLIENT is only supported by the random-read workload" >&2
    exit 2
  fi
  start_ngs3fs
  start_goofys
  test "$(stat -c %s "$ngs3fs_mount/$object")" = \
    "$((object_mib * 1024 * 1024))"
  test "$(stat -c %s "$goofys_mount/$object")" = \
    "$((object_mib * 1024 * 1024))"
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
fi

printf '%s\n' "$run_dir"
