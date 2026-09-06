#!/usr/bin/env bash

# Correctness checks under controlled GET latency/rate, not a throughput score.
set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
output_dir=${1:-"$project_dir/build/e2e/read-latency-$run_id"}
output_dir=$(realpath -m "$output_dir")
ngs3fs=${NGS3FS_BIN:-"$project_dir/build/dev/ngs3fs"}
fixture=${FUSE_MMAP_TEST_BIN:-"$project_dir/build/dev/fuse_mmap_integration_test"}
reactors=${NGS3FS_REACTORS:-1}
read -r -a delays <<<"${READ_TEST_TTFB_MS:-0 10 100}"
read -r -a rates <<<"${READ_TEST_PAYLOAD_RATES:-0 268435456}"

if [[ -e "$output_dir" ]]; then
  echo "output directory already exists: $output_dir" >&2
  exit 2
fi
if [[ ! -x "$ngs3fs" || ! -x "$fixture" ]]; then
  echo "build ngs3fs and fuse_mmap_integration_test first" >&2
  exit 2
fi
for value in "${delays[@]}" "${rates[@]}" "$reactors"; do
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "latency, rates and reactor count must be nonnegative decimal integers" >&2
    exit 2
  fi
done
if ((10#$reactors == 0)); then
  echo "reactor count must be positive" >&2
  exit 2
fi
for delay in "${delays[@]}"; do
  if ((10#$delay > 1000)); then
    echo "TTFB must be at most 1000 ms for the fixture's bounded waits" >&2
    exit 2
  fi
done

mkdir -p "$output_dir"
printf '%s\n' 'ttfb_ms,payload_bytes_per_second,reactors,result,exit_code' \
  >"$output_dir/results.csv"
printf '%s\n' \
  'Controlled H2 whole-block read correctness checks.' \
  'Each GET delays its first body byte once, then independently paces payload bytes.' \
  'HTTP headers are not delayed; this is a first-body-byte delivery model.' \
  'Rate 0 means unlimited. These are per-GET rates, not a shared link shaper.' \
  'Fixture totals include setup, gating, byte validation and sequential reads.' \
  'Do not interpret whole-test wall time as FUSE latency or network throughput.' \
  >"$output_dir/README.txt"
failed=0
for delay in "${delays[@]}"; do
  for rate in "${rates[@]}"; do
    name="ttfb-$delay-rate-$rate"
    status=pass
    code=0
    if NGS3FS_TEST_TTFB_MS="$delay" \
       NGS3FS_TEST_PAYLOAD_BYTES_PER_SECOND="$rate" \
       timeout --signal=TERM --kill-after=10s 90s \
         "$fixture" "$ngs3fs" xxhash128 prefetch-pool uring "$reactors" \
         >"$output_dir/$name.log" 2>&1; then
      :
    else
      code=$?
      status=fail
      if ((code == 77)); then status=unsupported; fi
      failed=1
    fi
    printf '%s,%s,%s,%s,%s\n' "$delay" "$rate" "$reactors" "$status" "$code" \
      >>"$output_dir/results.csv"
    printf '%s: %s (exit %s)\n' "$name" "$status" "$code"
  done
done
printf '%s\n' "$output_dir"
exit "$failed"
