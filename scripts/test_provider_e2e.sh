#!/usr/bin/env bash

set -euo pipefail

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
ngs3fs=${NGS3FS_BIN:-$project_dir/build/dev/ngs3fs}
client=${NGS3FS_PROVIDER_CLIENT:-$project_dir/build/dev/provider_smoke}
endpoint_host=${NGS3FS_ENDPOINT_HOST:?NGS3FS_ENDPOINT_HOST is required}
endpoint_port=${NGS3FS_ENDPOINT_PORT:-443}
authority=${NGS3FS_AUTHORITY:-$endpoint_host:$endpoint_port}
bucket=${NGS3FS_BUCKET:?NGS3FS_BUCKET is required}
prefix=${NGS3FS_PREFIX:-ngs3fs-e2e-${GITHUB_RUN_ID:-$$}-${GITHUB_RUN_ATTEMPT:-0}}
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}
checksum=${NGS3FS_CHECKSUM:-auto}
run_dir=${1:-$(mktemp -d /tmp/ngs3fs-provider.XXXXXX)}
mount_dir=$run_dir/mnt
log=$run_dir/ngs3fs.log
pid=

cleanup() {
  status=$?
  set +e
  if mountpoint -q "$mount_dir"; then
    fusermount3 -u "$mount_dir"
  fi
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
  fi
  if (( status != 0 )) && [[ -f "$log" ]]; then
    sed -n '1,320p' "$log" >&2
  fi
  exit "$status"
}

trap cleanup EXIT

if [[ ! -x "$ngs3fs" || ! -x "$client" ]]; then
  echo "ngs3fs and provider_smoke must be built first" >&2
  exit 1
fi
mkdir -p "$mount_dir"

args=(
  -f
  --endpoint-host "$endpoint_host"
  --endpoint-port "$endpoint_port"
  --authority "$authority"
  --bucket "$bucket"
  --prefix "$prefix"
  --region "$region"
  --checksum "$checksum"
  --connect-timeout "${NGS3FS_CONNECT_TIMEOUT_MS:-10000}"
  --request-timeout "${NGS3FS_REQUEST_TIMEOUT_MS:-60000}"
)
if [[ ${NGS3FS_TLS:-1} == 1 ]]; then
  args+=(--tls)
fi
"$ngs3fs" "${args[@]}" "$mount_dir" >"$log" 2>&1 &
pid=$!

for ((attempt = 0; attempt != 200; ++attempt)); do
  if mountpoint -q "$mount_dir"; then
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "ngs3fs exited before mounting provider" >&2
    exit 1
  fi
  sleep 0.1
done
if ! mountpoint -q "$mount_dir"; then
  echo "ngs3fs did not mount provider" >&2
  exit 1
fi

"$client" "$mount_dir"
printf 'provider E2E passed: endpoint=%s bucket=%s prefix=%s\n' \
  "$endpoint_host" "$bucket" "$prefix"
