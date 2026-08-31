#!/usr/bin/env bash

set -euo pipefail

if (( EUID != 0 )); then
  echo "xfstests requires root; run this script through sudo" >&2
  exit 1
fi

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
xfstests_dir=$(realpath "${1:?usage: $0 XFSTESTS_SOURCE_DIR [RUN_DIR]}")
if (( $# >= 2 )); then
  run_dir=$(realpath -m "$2")
  if [[ -e "$run_dir" ]]; then
    echo "run directory already exists: $run_dir" >&2
    exit 1
  fi
  mkdir -p "$run_dir"
else
  run_dir=$(mktemp -d /tmp/ngs3fs-xfstests.XXXXXX)
fi

versitygw=${VERSITYGW_BIN:-$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw}
ngs3fs=${NGS3FS_BIN:-$project_dir/build/dev/ngs3fs}
goofys=${GOOFYS_BIN:-}
client=${NGS3FS_TEST_CLIENT:-ngs3fs}
if [[ -z "$goofys" ]]; then
  goofys=$(command -v goofys || true)
fi
check=$xfstests_dir/check
fsstress=$xfstests_dir/ltp/fsstress
random_read_stress=$project_dir/build/dev/random_read_stress
port=${PORT:-17175}
stress_jobs=${NGS3FS_XFSTESTS_STRESS_JOBS:-8}
stress_operations=${NGS3FS_XFSTESTS_STRESS_OPERATIONS:-500}
stress_seed=${NGS3FS_XFSTESTS_STRESS_SEED:-}
random_read_files=${NGS3FS_RANDOM_READ_FILES:-32}
random_read_threads=${NGS3FS_RANDOM_READ_THREADS:-16}
random_read_operations=${NGS3FS_RANDOM_READ_OPERATIONS:-128}
random_read_file_size=${NGS3FS_RANDOM_READ_FILE_SIZE:-4194304}
random_read_maximum=${NGS3FS_RANDOM_READ_MAXIMUM:-262144}
random_read_seed=${NGS3FS_RANDOM_READ_SEED:-0x4e47533346535244}
backend=$run_dir/backend
bucket=ngs3fs-xfstests
prefix=data
mount_dir=$run_dir/mnt
access_key=ngs3fs-xfstests
secret_key=ngs3fs-xfstests-secret
endpoint=http://127.0.0.1:$port
server_pid=
client_pid=
client_log=$run_dir/$client.log

case $client in
  ngs3fs)
    client_binary=$ngs3fs
    test_device=ngs3fs
    ;;
  goofys)
    client_binary=$goofys
    test_device=$bucket:$prefix
    ;;
  *)
    echo "NGS3FS_TEST_CLIENT must be ngs3fs or goofys" >&2
    exit 2
    ;;
esac

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
    echo "===== xfstests results =====" >&2
    find "$run_dir/results" -maxdepth 3 -type f -print -exec \
      sed -n '1,240p' {} \; 2>/dev/null >&2
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

for file in "$versitygw" "$client_binary" "$check" "$fsstress" \
            "$random_read_stress"; do
  if [[ ! -e "$file" ]]; then
    echo "missing xfstests dependency: $file" >&2
    exit 1
  fi
done
if [[ ! -x "$versitygw" || ! -x "$client_binary" || ! -x "$check" ||
      ! -x "$fsstress" || ! -x "$random_read_stress" ]]; then
  echo "all xfstests executables must be executable" >&2
  exit 1
fi
if (( stress_jobs <= 0 || stress_operations <= 0 )); then
  echo "stress jobs and operations must be positive" >&2
  exit 1
fi
if (( random_read_files < 2 || random_read_threads <= 0 ||
      random_read_operations <= 0 || random_read_file_size <= 0 ||
      random_read_maximum <= 0 )); then
  echo "random-read stress parameters are invalid" >&2
  exit 1
fi

mkdir -p "$backend/$bucket/$prefix" "$mount_dir" "$run_dir/results"

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
  AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
    "$ngs3fs" -f -e 127.0.0.1 -p "$port" -a "127.0.0.1:$port" \
      -b "$bucket" -k "$prefix" -m 0644 -D 0755 "$mount_dir" \
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

stress_dir=$mount_dir/fsstress
mkdir "$stress_dir"

# fsstress defaults include operations outside the ngs3fs contract. -z clears
# every default frequency, after which only supported namespace and metadata
# operations are enabled. Its write variants are deliberately excluded because
# they choose random offsets. Non-empty data reads are covered separately below.
echo "xfstests fsstress: $stress_jobs workers x $stress_operations operations"
stress_args=(-z -d "$stress_dir")
if [[ -n "$stress_seed" ]]; then
  stress_args+=(-s "$stress_seed")
fi
# fsstress terminates its worker process group with kill(-getpid(), SIGTERM).
# Give it a private session so that this cleanup can never reach the mounted
# filesystem process or the test harness when job control is unavailable.
setsid "$fsstress" "${stress_args[@]}" \
  -f creat=8 \
  -f getattr=8 \
  -f getdents=8 \
  -f mkdir=4 \
  -f rename=8 \
  -f rnoreplace=4 \
  -f rmdir=4 \
  -f stat=8 \
  -f unlink=8 \
  -p "$stress_jobs" \
  -n "$stress_operations"

if ! kill -0 "$client_pid" 2>/dev/null; then
  set +e
  wait "$client_pid"
  client_status=$?
  set -e
  client_pid=
  echo "$client exited during filtered xfstests: status=$client_status" >&2
  exit 1
fi
if ! mountpoint -q "$mount_dir"; then
  echo "$client did not survive the filtered xfstests workload" >&2
  exit 1
fi

printf 'concurrent random-read stress: %s threads, %s files, %s operations per thread\n' \
  "$random_read_threads" "$random_read_files" "$random_read_operations"
"$random_read_stress" \
  -d "$mount_dir/random-read" \
  -f "$random_read_files" \
  -t "$random_read_threads" \
  -n "$random_read_operations" \
  -s "$random_read_file_size" \
  -r "$random_read_maximum" \
  -S "$random_read_seed"

if ! kill -0 "$client_pid" 2>/dev/null || ! mountpoint -q "$mount_dir"; then
  echo "$client did not survive the concurrent random-read workload" >&2
  exit 1
fi

# This is an explicit contract allowlist, not an expected-failure list.
# generic/001 covers sequential writes, reads with content comparison, create,
# unlink, rename, and directories. generic/245 checks that rename cannot replace
# a non-empty directory. Tests requiring scratch devices, random/overwriting
# writes, O_RDWR, truncate, writable mmap, hard/symbolic links, xattrs, locks,
# special files, direct I/O, or persistence across fsync/remount stay disabled.
# The xfstests runner intentionally unmounts TEST_DIR after the final case, so
# run it after fsstress and require the client to finish successfully.
echo "xfstests correctness: generic/001 generic/245"
(
  cd "$xfstests_dir"
  setsid env \
    EMAIL=ngs3fs-ci \
    TEST_DEV="$test_device" \
    TEST_DIR="$mount_dir" \
    FSTYP=fuse \
    RESULT_BASE="$run_dir/results" \
      ./check generic/001 generic/245
)

set +e
wait "$client_pid"
client_status=$?
set -e
client_pid=
if (( client_status != 0 )); then
  echo "$client exited after xfstests: status=$client_status" >&2
  exit 1
fi
if mountpoint -q "$mount_dir"; then
  echo "xfstests did not unmount the test filesystem" >&2
  exit 1
fi

if [[ $client == goofys ]] && grep -q DirectoryObjectContainsData "$client_log"; then
  count=$(grep -c DirectoryObjectContainsData "$client_log")
  echo "goofys encountered $count DirectoryObjectContainsData failures" >&2
  exit 1
fi

printf 'filtered xfstests passed for %s; logs: %s\n' "$client" "$run_dir"
