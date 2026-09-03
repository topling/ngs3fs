#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_dir=${1:-$(mktemp -d /tmp/ngs3fs-versity-write.XXXXXX)}
port=${PORT:-17173}
versitygw="$project_dir/build/e2e/versitygw/versitygw_v1.7.0_Linux_x86_64/versitygw"
ngs3fs="$project_dir/build/dev/ngs3fs"
client="$project_dir/build/dev/mounted_e2e_client"
backend="$run_dir/backend"
mount_dir="$run_dir/mnt"
reference="$run_dir/reference.bin"
bucket=ngs3fs-write
prefix=data
object=object.bin
access_key=ngs3fs-write
secret_key=ngs3fs-write-secret
endpoint="http://127.0.0.1:$port"
# Set NGS3FS_CACHE_DIR to exercise the persistent cache; unset keeps the
# original uncached write test path.
cache_dir=${NGS3FS_CACHE_DIR:-}

server_pid=
ngs3fs_pid=

cleanup() {
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
}

trap cleanup EXIT INT TERM

for binary in "$versitygw" "$ngs3fs" "$client"; do
  if [[ ! -x "$binary" ]]; then
    echo "missing executable: $binary" >&2
    exit 1
  fi
done

mkdir -p "$backend/$bucket/$prefix" "$mount_dir"
dd if=/dev/urandom of="$reference" bs=1M count=12 status=none
cp "$reference" "$backend/$bucket/$prefix/$object"
mkdir -p "$backend/$bucket/$prefix/paged-dir"
for ((i = 0; i != 1001; ++i)); do
  : >"$backend/$bucket/$prefix/filler-$i"
  printf -v page_name 'page-%04d' "$i"
  : >"$backend/$bucket/$prefix/paged-dir/$page_name"
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
    echo "versitygw exited before becoming ready" >&2
    exit 1
  fi
  sleep 0.1
done

ngs3fs_mount_args=(-f -e 127.0.0.1 -p "$port" -a "127.0.0.1:$port"
  -b "$bucket" -k "$prefix" -D 0755)
if [[ -n "$cache_dir" ]]; then
  mkdir -p "$cache_dir"
  ngs3fs_mount_args+=(--cache-dir "$cache_dir")
  echo "ngs3fs local cache: $cache_dir"
fi
AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key \
  "$ngs3fs" "${ngs3fs_mount_args[@]}" "$mount_dir" \
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

"$client" "$mount_dir/$object" "$reference" "$mount_dir/renamed.bin"

if grep -q "copying memory-backed FUSE_WRITE" "$run_dir/ngs3fs.log"; then
  echo "Versity write unexpectedly used the copied FUSE fallback" >&2
  exit 1
fi

if [[ ! -f "$backend/$bucket/$prefix/renamed.bin" ||
      ! -f "$backend/$bucket/$prefix/paged-dir/page-1000" ||
      -e "$backend/$bucket/$prefix/paged-renamed" ||
      -e "$backend/$bucket/$prefix/parallel-a.bin" ||
      -e "$backend/$bucket/$prefix/parallel-b.bin" ||
      -e "$backend/$bucket/$prefix/subdir" ||
      -e "$backend/$bucket/$prefix/renamed-dir" ]]; then
  echo "Versity backend namespace does not match FUSE mutations" >&2
  exit 1
fi

printf '%s\n' "$run_dir"
