#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
run_dir=${1:-"$project_dir/build/profiles/ngs3fs-$run_id"}
run_dir=$(realpath -m "$run_dir")
port=${PORT:-17072}
workload=${WORKLOAD:-mmap}
iterations=${ITERATIONS:-3000}
max_connections=${MAX_CONNECTIONS:-8}
io_engine=${NGS3FS_IO_ENGINE:-}
reactors=${NGS3FS_REACTORS:-}
bytes=${BYTES:-1048576}
perf_event=${PERF_EVENT:-cpu-clock}
perf_frequency=${PERF_FREQUENCY:-4000}
perf_mmap_size=${PERF_MMAP_SIZE:-4M}
io_args=()
memory_args=()
validate_memory=0
if [[ -n "$io_engine" ]]; then
  io_args+=(--io-engine "$io_engine")
fi
if [[ -n "$reactors" ]]; then
  io_args+=(--reactors "$reactors")
fi
perf_stat_events=${PERF_STAT_EVENTS:-}
if [[ -z "$perf_stat_events" ]]; then
  perf_stat_events=task-clock
  perf_stat_events+=,context-switches,cpu-migrations,page-faults
  if [[ -r /sys/kernel/tracing/events/syscalls/sys_enter_splice/id ]]; then
    perf_stat_events+=,syscalls:sys_enter_splice
    perf_stat_events+=,syscalls:sys_enter_recvfrom
    perf_stat_events+=,syscalls:sys_enter_sendto
    perf_stat_events+=,syscalls:sys_enter_futex
    perf_stat_events+=,syscalls:sys_enter_writev
    perf_stat_events+=,syscalls:sys_enter_close
    perf_stat_events+=,syscalls:sys_enter_pipe2
    perf_stat_events+=,syscalls:sys_enter_fcntl
    perf_stat_events+=,syscalls:sys_enter_poll
    perf_stat_events+=,syscalls:sys_enter_setsockopt
  else
    echo "warning: syscall tracepoints are not readable; profiling basic counters only" >&2
  fi
fi
random_files=${RANDOM_READ_FILES:-32}
random_threads=${RANDOM_READ_THREADS:-16}
random_operations=${RANDOM_READ_OPERATIONS:-128}
random_file_size=${RANDOM_READ_FILE_SIZE:-4194304}
random_maximum_read=${RANDOM_READ_MAXIMUM:-262144}
random_seed=${RANDOM_READ_SEED:-0x4e47533346535244}
random_advice=${RANDOM_READ_ADVICE:-random}
write_bytes=${WRITE_BYTES:-67108864}
write_block_size=${WRITE_BLOCK_SIZE:-262144}
write_files=${WRITE_FILES:-2}
cache_mode=${CACHE_MODE:-none}
drop_after_warmup=${DROP_CACHES_AFTER_WARMUP:-0}
cache_drop_status=skipped

if [[ "$drop_after_warmup" != 0 && "$drop_after_warmup" != 1 ]]; then
  echo "DROP_CACHES_AFTER_WARMUP must be 0 or 1" >&2
  exit 2
fi

versitygw="$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw"
ngs3fs=${NGS3FS_BIN:-"$project_dir/build/dev/ngs3fs"}
ngs3fs_source_dir=${NGS3FS_SOURCE_DIR:-$project_dir}
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
cache_dir="$run_dir/cache"
access_key=ngs3fs-profile
secret_key=ngs3fs-profile-secret
endpoint="http://127.0.0.1:$port"

case "$cache_mode" in
  none)
    cache_arg=()
    cache_path=-
    ;;
  cold | warm)
    cache_arg=(-L "$cache_dir" --cache-reserve 0)
    cache_path="$cache_dir"
    ;;
  *)
    echo "unsupported cache mode: $cache_mode (expected none, cold, or warm)" >&2
    exit 2
    ;;
esac
if [[ "$cache_mode" != none && -e "$cache_dir" ]]; then
  echo "cache directory already exists: $cache_dir" >&2
  exit 1
fi

server_pid=
ngs3fs_pid=
perf_pid=
perf_control_fd=
perf_ack_fd=
perf_control_fifo=
perf_ack_fifo=
perf_control_timeout_seconds=${PERF_CONTROL_TIMEOUT_SECONDS:-10}
perf_startup_timeout_seconds=${PERF_STARTUP_TIMEOUT_SECONDS:-30}
perf_record_log=
perf_diagnostic_log=
profile_first_line=0
profile_last_line=0
profile_get_requests=0
profile_head_requests=0
profile_put_requests=0
profile_upload_part_requests=0
profile_complete_requests=0
profile_elapsed_ns=0
profile_operations_unit=
profile_actual_bytes=

close_perf_control() {
  if [[ -n "$perf_control_fd" ]]; then
    exec {perf_control_fd}>&-
    perf_control_fd=
  fi
  if [[ -n "$perf_ack_fd" ]]; then
    exec {perf_ack_fd}>&-
    perf_ack_fd=
  fi
  if [[ -n "$perf_control_fifo" ]]; then
    rm -f "$perf_control_fifo"
    perf_control_fifo=
  fi
  if [[ -n "$perf_ack_fifo" ]]; then
    rm -f "$perf_ack_fifo"
    perf_ack_fifo=
  fi
}

control_perf_record() {
  local command=$1
  local timeout_seconds=${2:-$perf_control_timeout_seconds}
  local acknowledgement
  if ! printf '%s\n' "$command" >&"$perf_control_fd"; then
    echo "unable to send perf record $command command" >&2
    return 1
  fi
  if ! IFS= read -r -t "$timeout_seconds" acknowledgement \
      <&"$perf_ack_fd"; then
    {
      echo "timed out waiting for perf record $command acknowledgement"
      printf 'perf_record_pid=%s\n' "${perf_pid:-unavailable}"
      if [[ -n "$perf_pid" ]] && kill -0 "$perf_pid" 2>/dev/null; then
        printf 'perf_record_alive=yes\n'
        ps -o pid,stat,wchan,comm -p "$perf_pid" 2>&1 || true
      else
        printf 'perf_record_alive=no\n'
      fi
      printf 'perf_record_stderr=%s\n' \
        "${perf_record_log:-inherited standard error}"
      if [[ -n "$perf_record_log" && -s "$perf_record_log" ]]; then
        tail -n 20 "$perf_record_log" 2>&1 || true
      fi
    } >"$perf_diagnostic_log"
    cat "$perf_diagnostic_log" >&2
    return 1
  fi
  if [[ "$acknowledgement" != ack ]]; then
    echo "unexpected perf record $command acknowledgement: $acknowledgement" >&2
    return 1
  fi
}

stop_perf_record() {
  local report_status=${1:-0}
  local record_pid=${perf_pid:-}
  local interrupted=no
  local wait_status
  if [[ -z "$record_pid" ]]; then
    return
  fi
  if kill -0 "$record_pid" 2>/dev/null; then
    if kill -INT "$record_pid" 2>/dev/null; then
      interrupted=yes
    fi
  fi
  if wait "$record_pid" 2>/dev/null; then
    wait_status=0
  else
    wait_status=$?
  fi
  perf_pid=
  if [[ -n "$perf_diagnostic_log" ]]; then
    {
      printf 'perf_record_interrupted_by_harness=%s\n' "$interrupted"
      printf 'perf_record_exit_status=%s\n' "$wait_status"
    } >>"$perf_diagnostic_log"
  fi
  if ((report_status)); then
    printf 'perf_record_interrupted_by_harness=%s\n' "$interrupted" >&2
    printf 'perf_record_exit_status=%s\n' "$wait_status" >&2
  fi
  if ((wait_status != 0)) &&
      [[ "$interrupted:$wait_status" != yes:130 ]]; then
    printf 'perf record failed: exit_status=%s interrupted_by_harness=%s\n' \
      "$wait_status" "$interrupted" >&2
    return 1
  fi
  return 0
}

cleanup() {
  set +e
  if [[ -n "$perf_pid" ]]; then
    stop_perf_record 1
  fi
  close_perf_control
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

capture_thread_census() {
  local pid=$1
  local output=$2
  local task
  local tid
  local comm
  local kind
  local count=0
  local sqpoll_count=0
  local io_wq_count=0
  local status_key
  local status_value
  local status_unit
  local vm_rss_kib=
  local vm_hwm_kib=

  if ! {
    printf 'process_pid=%s\n' "$pid"
    printf 'source=/proc/%s/task\n' "$pid"
    printf 'tid\tkind\tcomm\n'
    for task in /proc/"$pid"/task/[0-9]*; do
      [[ -r "$task/comm" ]] || continue
      if ! IFS= read -r comm <"$task/comm"; then
        continue
      fi
      tid=${task##*/}
      kind=application
      if [[ "$comm" = iou-sqp-* ]]; then
        kind=kernel-sqpoll
        ((++sqpoll_count))
      elif [[ "$comm" = iou-wrk-* ]]; then
        kind=kernel-io-wq
        ((++io_wq_count))
      fi
      printf '%s\t%s\t%s\n' "$tid" "$kind" "$comm"
      ((++count))
    done
    printf 'task_count=%s\n' "$count"
    printf 'sqpoll_task_count=%s\n' "$sqpoll_count"
    printf 'io_wq_task_count=%s\n' "$io_wq_count"
    if [[ -r "/proc/$pid/status" ]]; then
      while read -r status_key status_value status_unit _; do
        case "$status_key" in
          VmRSS:) vm_rss_kib=$status_value ;;
          VmHWM:) vm_hwm_kib=$status_value ;;
        esac
      done <"/proc/$pid/status"
    fi
    printf 'memory_source=/proc/%s/status\n' "$pid"
    printf 'vm_rss_kib=%s\n' "${vm_rss_kib:-unavailable}"
    printf 'vm_hwm_kib=%s\n' "${vm_hwm_kib:-unavailable}"
  } >"$output" 2>/dev/null; then
    printf 'process_pid=%s\nthread_census=unavailable\n' "$pid" \
      >"$output" 2>/dev/null || true
  fi
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

drop_measurement_caches() {
  if [[ "$cache_mode" != warm || "$drop_after_warmup" != 1 ]]; then
    return
  fi
  if ! sync -f "$run_dir"; then
    cache_drop_status=failed
    echo "warning: unable to sync profile filesystem after warmup" >&2
    return
  fi
  if ! echo 3 2>/dev/null > /proc/sys/vm/drop_caches; then
    cache_drop_status=failed
    echo "warning: unable to drop kernel caches after warmup" >&2
  else
    cache_drop_status=success
  fi
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
  write)
    required_binaries+=("$random_bench")
    if ((write_bytes == 0 || write_block_size == 0 || write_files < 2 ||
         write_bytes % write_files != 0 ||
         (write_bytes / write_files) % write_block_size != 0)); then
      echo "WRITE_BYTES must divide evenly across at least two WRITE_FILES and WRITE_BLOCK_SIZE" >&2
      exit 2
    fi
    ;;
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
if [[ "$workload" = random-read && "$cache_mode" = none ]]; then
  memory_limits=$(python3 "$project_dir/scripts/random_read_memory.py" plan \
    --files "$random_files" --file-size "$random_file_size" \
    --threads "$random_threads" --maximum-read "$random_maximum_read" \
    --connections "$max_connections" --output "$run_dir/memory-plan.json")
  read -r mount_memory file_memory <<<"$memory_limits"
  memory_args=(--max-prefetch-memory "$mount_memory"
    --max-file-prefetch-memory "$file_memory" --stats-interval 1)
  validate_memory=1
fi
{
  printf 'ngs3fs_path=%s\n' "$(realpath "$ngs3fs")"
  printf 'ngs3fs_sha256=%s\n' "$(sha256sum "$ngs3fs" | cut -d' ' -f1)"
  printf 'git_commit=%s\n' "$(git -c safe.directory="$ngs3fs_source_dir" -C "$ngs3fs_source_dir" rev-parse HEAD)"
  printf 'git_dirty=%s\n' "$(git -c safe.directory="$ngs3fs_source_dir" -C "$ngs3fs_source_dir" status --porcelain | tr '\n' ' ')"
  printf 'git_tracked_dirty=%s\n' "$(git -c safe.directory="$ngs3fs_source_dir" -C "$ngs3fs_source_dir" status --porcelain --untracked-files=no | tr '\n' ' ')"
  printf 'cache_drop_requested=%s\n' "$drop_after_warmup"
  printf 'perf_event=%s\nperf_frequency=%s\n' "$perf_event" "$perf_frequency"
  printf 'perf_mmap_size=%s\n' "$perf_mmap_size"
  printf 'perf_record_verbosity=0\n'
  printf 'ngs3fs_io_engine=%s\nngs3fs_reactors=%s\n' \
    "${io_engine:-legacy}" "${reactors:-1}"
} >"$run_dir/system.txt"
if [[ "$workload" = mmap ]]; then
  dd if=/dev/urandom of="$backend/$bucket/$object" \
    bs=1M count=256 status=none
  dd if="$backend/$bucket/$object" of=/dev/null bs=8M status=none
  if [[ "$cache_mode" = cold ]]; then
    cp --reflink=auto "$backend/$bucket/$object" \
      "$backend/$bucket/$object-cold-warmup"
  fi
elif [[ "$workload" = random-read ]]; then
  "$random_bench" -p -d "$backend/$bucket/random-read" \
    -f "$random_files" -t "$random_threads" -n "$random_operations" \
    -s "$random_file_size" -r "$random_maximum_read" -S "$random_seed"
  if [[ "$cache_mode" = cold ]]; then
    cp -a "$backend/$bucket/random-read" \
      "$backend/$bucket/random-read-cold-warmup"
  fi
fi

ROOT_ACCESS_KEY_ID=$access_key ROOT_SECRET_ACCESS_KEY=$secret_key \
  "$versitygw" --port "127.0.0.1:$port" --keep-alive --quiet \
    --access-log "$run_dir/versity-access.log" \
    posix "$backend" >"$run_dir/versity.log" 2>&1 &
server_pid=$!
wait_for_server

AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
  "$ngs3fs" -f -e 127.0.0.1 -p "$port" \
    -C "$max_connections" \
    "${io_args[@]}" \
    "${memory_args[@]}" \
    -a "127.0.0.1:$port" \
    -b "$bucket" "${cache_arg[@]}" "$mount_dir" \
    >"$run_dir/ngs3fs.log" 2>&1 &
ngs3fs_pid=$!
wait_for_mount

if [[ "$workload" = mmap ]]; then
  warmup_object=$object
  warmup_iterations=50
  if [[ "$cache_mode" = warm ]]; then
    warmup_iterations=$iterations
  fi
  if [[ "$cache_mode" = cold ]]; then
    warmup_object="$object-cold-warmup"
  fi
  "$bench" "$mount_dir/$warmup_object" "$bytes" "$warmup_iterations" 17825792 \
    >"$run_dir/warmup.jsonl"
  title="ngs3fs $bytes-byte mmap faults ($cache_mode cache)"
  flame_svg="$run_dir/ngs3fs-$bytes.svg"
  flame_html="$run_dir/ngs3fs-$bytes-interactive.html"
elif [[ "$workload" = random-read ]]; then
  warmup_dir="$mount_dir/random-read"
  warmup_operations=8
  if [[ "$cache_mode" = warm ]]; then
    warmup_operations=$random_operations
  fi
  if [[ "$cache_mode" = cold ]]; then
    warmup_dir="$mount_dir/random-read-cold-warmup"
  fi
  "$random_bench" -R "${random_advice_args[@]}" \
    -d "$warmup_dir" \
    -f "$random_files" -t "$random_threads" -n "$warmup_operations" \
    -s "$random_file_size" -r "$random_maximum_read" -S "$random_seed" \
    >"$run_dir/warmup.txt"
  title="ngs3fs concurrent multi-file random reads ($random_advice advice, $cache_mode cache)"
  flame_svg="$run_dir/ngs3fs-random-read.svg"
  flame_html="$run_dir/ngs3fs-random-read-interactive.html"
else
  "$random_bench" -p -d "$mount_dir/write-warmup" \
    -f 2 -t 1 -n 1 -s 8388608 -r "$write_block_size" \
    >"$run_dir/warmup.txt"
  title="ngs3fs sequential cached write ($cache_mode cache)"
  flame_svg="$run_dir/ngs3fs-write.svg"
  flame_html="$run_dir/ngs3fs-write-interactive.html"
fi
ps -T -p "$ngs3fs_pid" -o pid,tid,comm,cls,ni,psr,stat \
  >"$run_dir/threads-before-perf.txt"
if [[ "$io_engine" = uring-sqpoll ]]; then
  if ! awk '$3 ~ /^iou-sqp-/ { found = 1 } END { exit !found }' \
      "$run_dir/threads-before-perf.txt"; then
    echo "SQPOLL selected but no kernel submission thread exists before perf attach" >&2
    exit 2
  fi
fi

run_profile_measurement() {
  local attempt=$1
  local multiplier=$2
  local control_failed=0
  drop_measurement_caches
  printf 'cache_drop_status_attempt_%s=%s\n' "$attempt" "$cache_drop_status" \
    >>"$run_dir/system.txt"
  profile_first_line=$(wc -l <"$run_dir/versity-access.log")
  perf_control_fifo="$run_dir/perf-control-$attempt.fifo"
  perf_ack_fifo="$run_dir/perf-ack-$attempt.fifo"
  perf_record_log="$run_dir/perf-record-$attempt.log"
  perf_diagnostic_log="$run_dir/perf-diagnostic-$attempt.txt"
  : >"$perf_record_log"
  : >"$perf_diagnostic_log"
  mkfifo "$perf_control_fifo" "$perf_ack_fifo"
  exec {perf_control_fd}<>"$perf_control_fifo"
  exec {perf_ack_fd}<>"$perf_ack_fifo"
  # Even verbosity one emits large per-frame unwind/symbol traces. Keep normal
  # stderr, control-timeout diagnostics and the recorder's exit status instead.
  # Attach only to ngs3fs. A dummy workload would replace perf's exit status
  # with the signal used to terminate that child during cleanup.
  LD_LIBRARY_PATH=$perf_lib "$perf" record -F "$perf_frequency" \
    -e "$perf_event" -m "$perf_mmap_size" \
    --call-graph dwarf,16384 --delay -1 \
    --control "fifo:$perf_control_fifo,$perf_ack_fifo" \
    -p "$ngs3fs_pid" \
    -o "$run_dir/perf.data" \
    2>"$perf_record_log" &
  perf_pid=$!
  if ! control_perf_record enable "$perf_startup_timeout_seconds"; then
    stop_perf_record 1 || true
    close_perf_control
    return 1
  fi
  profile_start_ns=$(date +%s%N)
  if [[ "$workload" = mmap ]]; then
    profile_operations_unit=iterations
    profile_actual_operations=$((iterations * multiplier))
    profile_actual_bytes=$((bytes * profile_actual_operations))
    LD_LIBRARY_PATH=$perf_lib "$perf" stat --no-big-num -x, \
      -e "$perf_stat_events" -p "$ngs3fs_pid" \
      -o "$run_dir/perf-stat.csv" -- \
      "$bench" "$mount_dir/$object" "$bytes" "$profile_actual_operations" 17825792 \
        >"$run_dir/mmap.jsonl"
  elif [[ "$workload" = random-read ]]; then
    profile_operations_unit=operations
    profile_actual_bytes=
    profile_actual_operations=$((random_threads * random_operations * multiplier))
    LD_LIBRARY_PATH=$perf_lib "$perf" stat --no-big-num -x, \
      -e "$perf_stat_events" -p "$ngs3fs_pid" \
      -o "$run_dir/perf-stat.csv" -- \
      "$random_bench" -R "${random_advice_args[@]}" \
        -d "$mount_dir/random-read" \
        -f "$random_files" -t "$random_threads" \
        -n "$((random_operations * multiplier))" \
        -s "$random_file_size" -r "$random_maximum_read" -S "$random_seed" \
        >"$run_dir/random-read.txt"
  else
    profile_operations_unit=files
    profile_actual_bytes=$((write_bytes * multiplier / write_files * write_files))
    profile_actual_operations=$write_files
    LD_LIBRARY_PATH=$perf_lib "$perf" stat --no-big-num -x, \
      -e "$perf_stat_events" -p "$ngs3fs_pid" \
      -o "$run_dir/perf-stat.csv" -- \
      "$random_bench" -p -d "$mount_dir/profile-write-attempt-$attempt" \
        -f "$write_files" -t 1 -n 1 \
        -s "$((write_bytes * multiplier / write_files))" -r "$write_block_size" \
        >"$run_dir/write.txt"
  fi
  profile_elapsed_ns=$(($(date +%s%N) - profile_start_ns))
  if ! control_perf_record disable; then
    control_failed=1
  fi
  profile_last_line=$(wc -l <"$run_dir/versity-access.log")
  profile_get_requests=0
  profile_head_requests=0
  profile_put_requests=0
  profile_upload_part_requests=0
  profile_complete_requests=0
  if ((profile_last_line > profile_first_line)); then
    profile_get_requests=$(sed -n "$((profile_first_line + 1)),${profile_last_line}p" \
      "$run_dir/versity-access.log" | grep -c 's3_GetObject' || true)
    profile_head_requests=$(sed -n "$((profile_first_line + 1)),${profile_last_line}p" \
      "$run_dir/versity-access.log" | grep -c 's3_HeadObject' || true)
    profile_put_requests=$(sed -n "$((profile_first_line + 1)),${profile_last_line}p" \
      "$run_dir/versity-access.log" | grep -c 's3_PutObject' || true)
    profile_upload_part_requests=$(sed -n "$((profile_first_line + 1)),${profile_last_line}p" \
      "$run_dir/versity-access.log" | grep -c 's3_UploadPart' || true)
    profile_complete_requests=$(sed -n "$((profile_first_line + 1)),${profile_last_line}p" \
      "$run_dir/versity-access.log" | grep -c 's3_CompleteMultipartUpload' || true)
  fi
  {
    printf 'cache_mode=%s\n' "$cache_mode"
    printf 'cache_dir=%s\n' "$cache_path"
    printf 'profile_attempt=%s\n' "$attempt"
    printf 'actual_operations=%s\n' "$profile_actual_operations"
    printf 'actual_operations_unit=%s\n' "$profile_operations_unit"
    if [[ -n "$profile_actual_bytes" ]]; then
      printf 'actual_bytes=%s\n' "$profile_actual_bytes"
    fi
    printf 's3_get_requests=%s\n' "$profile_get_requests"
    printf 's3_head_requests=%s\n' "$profile_head_requests"
    printf 's3_put_requests=%s\n' "$profile_put_requests"
    printf 's3_upload_part_requests=%s\n' "$profile_upload_part_requests"
    printf 's3_complete_requests=%s\n' "$profile_complete_requests"
    printf 'elapsed_ns=%s\n' "$profile_elapsed_ns"
  } >"$run_dir/profile-metadata.txt"
  if ! stop_perf_record; then
    control_failed=1
  fi
  close_perf_control
  if ((control_failed)); then
    return 1
  fi
}

profile_attempt=1
run_profile_measurement "$profile_attempt" 1
LD_LIBRARY_PATH=$perf_lib "$perf" script -i "$run_dir/perf.data" \
  >"$run_dir/perf.script"
"$flamegraph_dir/stackcollapse-perf.pl" "$run_dir/perf.script" \
  >"$run_dir/perf.folded"
if [[ ! -s "$run_dir/perf.folded" ]]; then
  if [[ "$cache_mode" = cold ]]; then
    echo "error: cold-cache profile captured no stacks; enlarge the workload and rerun" >&2
    exit 2
  fi
  echo "warning: perf captured no stacks; retrying a longer sampling pass" >&2
  rm -f "$run_dir/perf.data" "$run_dir/perf.script" \
    "$run_dir/perf.folded"
  profile_attempt=2
  run_profile_measurement "$profile_attempt" 4
  LD_LIBRARY_PATH=$perf_lib "$perf" script -i "$run_dir/perf.data" \
    >"$run_dir/perf.script"
  "$flamegraph_dir/stackcollapse-perf.pl" "$run_dir/perf.script" \
    >"$run_dir/perf.folded"
fi
if [[ ! -s "$run_dir/perf.folded" ]]; then
  echo "perf captured no stack samples after retry" >&2
  exit 2
fi
capture_thread_census "$ngs3fs_pid" \
  "$run_dir/threads-after-workload.txt"
if [[ "$io_engine" = uring-sqpoll ]]; then
  if awk '/^iou-sqp-/ { found = 1 } END { exit !found }' \
      "$run_dir/perf.folded"; then
    printf 'sqpoll_stacks_sampled=yes\n' >>"$run_dir/system.txt"
  else
    printf 'sqpoll_stacks_sampled=no\n' >>"$run_dir/system.txt"
    echo "warning: SQPOLL thread existed at perf attach but has no named stacks in this profile" >&2
  fi
fi
{
  printf 'profile_attempt=%s\n' "$profile_attempt"
  printf 'profile_actual_operations=%s\n' "$profile_actual_operations"
  printf 'profile_operations_unit=%s\n' "$profile_operations_unit"
  if [[ -n "$profile_actual_bytes" ]]; then
    printf 'profile_actual_bytes=%s\n' "$profile_actual_bytes"
  fi
  printf 'cache_drop_status=%s\n' "$cache_drop_status"
} >>"$run_dir/system.txt"
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
    'advice,cache_mode,cache_dir,files,threads,operations,pread_operations,mmap_operations,bytes,elapsed_ns,s3_get_requests,s3_head_requests,perf_event,perf_frequency' \
    >"$run_dir/profile-summary.csv"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$random_advice" "$cache_mode" "$cache_path" \
    "$random_files" "$random_threads" \
    "$profile_actual_operations" "$pread_operations" \
    "$mmap_operations" "$bytes_read" "$elapsed_ns" \
    "$profile_get_requests" "$profile_head_requests" \
    "$perf_event" "$perf_frequency" \
    >>"$run_dir/profile-summary.csv"
elif [[ "$workload" = write ]]; then
  printf '%s\n' \
    'cache_mode,cache_dir,bytes,block_size,elapsed_ns,s3_put_requests,s3_upload_part_requests,s3_complete_requests,perf_event,perf_frequency' \
    >"$run_dir/profile-summary.csv"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$cache_mode" "$cache_path" "$profile_actual_bytes" "$write_block_size" \
    "$profile_elapsed_ns" "$profile_put_requests" \
    "$profile_upload_part_requests" "$profile_complete_requests" \
    "$perf_event" "$perf_frequency" \
    >>"$run_dir/profile-summary.csv"
fi

if ((validate_memory)); then
  # Stop after all sampled/retry work, and collect final high-water counters.
  # Unmount triggers shutdown. A later SIGTERM could race signal-handler
  # removal and terminate the process before its final statistics are written.
  fusermount3 -u "$mount_dir"
  wait "$ngs3fs_pid" 2>/dev/null || true
  ngs3fs_pid=
  if ! python3 "$project_dir/scripts/random_read_memory.py" verify \
    --plan "$run_dir/memory-plan.json" --log "$run_dir/ngs3fs.log" \
    --output "$run_dir/memory-evidence.json"; then
    mv "$run_dir/profile-summary.csv" "$run_dir/profile-summary.invalid.csv"
    exit 1
  fi
fi

printf '%s\n' "$run_dir"
