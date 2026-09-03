#!/usr/bin/env bash

set -euo pipefail

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
run_dir=${1:-$(mktemp -d /tmp/ngs3fs-cache-recovery.XXXXXX)}
run_dir=$(realpath -m "$run_dir")
port=${PORT:-17680}
bytes=${RECOVERY_BYTES:-67108864}
block_size=${RECOVERY_BLOCK_SIZE:-262144}
timeout_seconds=${RECOVERY_TIMEOUT_SECONDS:-60}
versitygw=${VERSITYGW_BIN:-$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw}
ngs3fs=${NGS3FS_BIN:-$project_dir/build/dev/ngs3fs}
fixture=${CACHE_RECOVERY_FIXTURE_BIN:-$project_dir/build/dev/cache_recovery_fixture}
backend=$run_dir/backend
bucket=ngs3fs-cache-recovery
mount_dir=$run_dir/mnt
cache_dir=$run_dir/cache
ready=$run_dir/writer.ready
access_key=ngs3fs-cache-recovery
secret_key=ngs3fs-cache-recovery-secret
endpoint=http://127.0.0.1:$port
server_pid=
daemon_pid=
writer_pid=

cleanup() {
  status=$?
  set +e
  if [[ -n "$writer_pid" ]]; then
    kill -KILL "$writer_pid" 2>/dev/null
    wait "$writer_pid" 2>/dev/null
  fi
  if mountpoint -q "$mount_dir"; then
    fusermount3 -uz "$mount_dir"
  fi
  if [[ -n "$daemon_pid" ]]; then
    kill "$daemon_pid" 2>/dev/null
    wait "$daemon_pid" 2>/dev/null
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null
    wait "$server_pid" 2>/dev/null
  fi
  if ((status != 0)); then
    for log in "$run_dir"/*.log; do
      if [[ -f "$log" ]]; then
        printf '===== %s =====\n' "$log" >&2
        sed -n '1,240p' "$log" >&2
      fi
    done
  fi
  exit "$status"
}

wait_for_server() {
  for ((attempt = 0; attempt != 100; ++attempt)); do
    if curl --silent --output /dev/null "$endpoint/"; then
      return
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "VersityGW exited before becoming ready" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for VersityGW" >&2
  return 1
}

wait_for_mount() {
  for ((attempt = 0; attempt != 200; ++attempt)); do
    if mountpoint -q "$mount_dir"; then
      return
    fi
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
      echo "ngs3fs exited before mounting" >&2
      return 1
    fi
    sleep 0.05
  done
  echo "timed out waiting for ngs3fs" >&2
  return 1
}

start_ngs3fs() {
  local log=$1
  AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
    "$ngs3fs" -f -e 127.0.0.1 -p "$port" -a "127.0.0.1:$port" \
      -b "$bucket" -L "$cache_dir" --cache-reserve 0 \
      -P 8MiB -c 4 -C 8 "$mount_dir" >"$log" 2>&1 &
  daemon_pid=$!
  wait_for_mount
}

trap cleanup EXIT INT TERM

for binary in "$versitygw" "$ngs3fs" "$fixture"; do
  if [[ ! -x "$binary" ]]; then
    echo "missing executable: $binary" >&2
    exit 1
  fi
done
if ((bytes == 0 || block_size == 0)); then
  echo "recovery sizes must be nonzero" >&2
  exit 2
fi

mkdir -p "$backend/$bucket" "$mount_dir" "$cache_dir"
ROOT_ACCESS_KEY_ID=$access_key ROOT_SECRET_ACCESS_KEY=$secret_key \
  "$versitygw" --port "127.0.0.1:$port" --keep-alive --quiet \
    --access-log "$run_dir/versity-access.log" posix "$backend" \
    >"$run_dir/versity.log" 2>&1 &
server_pid=$!
wait_for_server

start_ngs3fs "$run_dir/ngs3fs-before-crash.log"
"$fixture" "$mount_dir/recovery.bin" "$bytes" "$block_size" "$ready" \
  >"$run_dir/writer.log" 2>&1 &
writer_pid=$!
for ((attempt = 0; attempt != 600; ++attempt)); do
  [[ -f "$ready" ]] && break
  kill -0 "$writer_pid" 2>/dev/null || {
    echo "recovery fixture exited before its write completed" >&2
    exit 1
  }
  sleep 0.05
done
[[ -f "$ready" ]] || {
  echo "timed out waiting for recovery fixture" >&2
  exit 1
}

# Crash only after the first multipart upload is observable. This exercises a
# recovery phase beyond merely discovering a dirty, never-started writer.
for ((attempt = 0; attempt != 200; ++attempt)); do
  grep -q 's3_UploadPart' "$run_dir/versity-access.log" && break
  sleep 0.05
done
grep -q 's3_UploadPart' "$run_dir/versity-access.log" || {
  echo "no multipart part reached S3 before the crash" >&2
  exit 1
}

kill -KILL "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
fusermount3 -uz "$mount_dir" 2>/dev/null || true
kill -KILL "$writer_pid" 2>/dev/null || true
wait "$writer_pid" 2>/dev/null || true
writer_pid=

recovery_first_line=$(wc -l <"$run_dir/versity-access.log")
start_ns=$(date +%s%N)
start_ngs3fs "$run_dir/ngs3fs-recovery.log"
deadline=$((SECONDS + timeout_seconds))
while ((SECONDS < deadline)); do
  recovered_size=$(stat -c %s "$backend/$bucket/recovery.bin" 2>/dev/null || true)
  if [[ "$recovered_size" = "$bytes" ]]; then
    break
  fi
  kill -0 "$daemon_pid" 2>/dev/null || {
    echo "ngs3fs exited during cache recovery" >&2
    exit 1
  }
  sleep 0.02
done
recovered_size=$(stat -c %s "$backend/$bucket/recovery.bin" 2>/dev/null || true)
[[ "$recovered_size" = "$bytes" ]] || {
  echo "cache recovery did not publish the expected object" >&2
  exit 1
}
elapsed_ns=$(($(date +%s%N) - start_ns))
cmp -n "$bytes" "$backend/$bucket/recovery.bin" /dev/zero
recovery_last_line=$(wc -l <"$run_dir/versity-access.log")
recovery_requests=0
if ((recovery_last_line > recovery_first_line)); then
  recovery_requests=$((recovery_last_line - recovery_first_line))
fi
upload_parts=$(grep -c 's3_UploadPart' "$run_dir/versity-access.log" || true)
complete_requests=$(grep -c 's3_CompleteMultipartUpload' \
  "$run_dir/versity-access.log" || true)

printf '%s\n' \
  'bytes,block_size,recovery_elapsed_ns,recovery_s3_requests,total_upload_parts,total_complete_requests' \
  >"$run_dir/summary.csv"
printf '%s,%s,%s,%s,%s,%s\n' \
  "$bytes" "$block_size" "$elapsed_ns" "$recovery_requests" \
  "$upload_parts" "$complete_requests" >>"$run_dir/summary.csv"
printf 'cache recovery passed: bytes=%s elapsed_ns=%s requests=%s\n' \
  "$bytes" "$elapsed_ns" "$recovery_requests"
printf '%s\n' "$run_dir"
