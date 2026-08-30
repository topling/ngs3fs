#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
output_dir=${1:-"$project_dir/build/benchmarks/random-read-$run_id"}
output_dir=$(realpath -m "$output_dir")
repetitions=${BENCHMARK_REPETITIONS:-3}
base_port=${PORT:-17500}

if [[ -e "$output_dir" ]]; then
  echo "benchmark output directory already exists: $output_dir" >&2
  exit 1
fi
if ((repetitions <= 0)); then
  echo "BENCHMARK_REPETITIONS must be positive" >&2
  exit 2
fi
mkdir -p "$output_dir"

{
  uname -a
  lscpu
  free -h
  printf 'ngs3fs_commit=%s\n' "$(git -C "$project_dir" rev-parse HEAD)"
  printf 'goofys_binary=%s\n' \
    "${GOOFYS_BIN:-/home/leipeng/.cache/goofys-reference/bin/goofys}"
  go version 2>/dev/null || true
} >"$output_dir/system.txt"

printf '%s\n' \
  'repetition,client,advice,max_connections,files,threads,operations,pread_operations,mmap_operations,bytes,wall_ns,daemon_cpu_ns,daemon_cpu_ns_per_operation,s3_get_requests' \
  >"$output_dir/samples.csv"

sample=0
for advice in random normal; do
  for ((repetition = 1; repetition <= repetitions; ++repetition)); do
    if ((repetition % 2 == 0)); then
      clients=(goofys ngs3fs)
    else
      clients=(ngs3fs goofys)
    fi
    for client in "${clients[@]}"; do
      ((++sample))
      run_dir="$output_dir/$advice-r$repetition-$client"
      sync
      if ! echo 3 >/proc/sys/vm/drop_caches 2>/dev/null; then
        echo "warning: unable to drop kernel caches before sample $sample" >&2
      fi
      WORKLOAD=random-read \
      CLIENT="$client" \
      RANDOM_READ_ADVICE="$advice" \
      PORT="$((base_port + sample))" \
        "$project_dir/scripts/compare_goofys.sh" "$run_dir"
      printf '%s,' "$repetition" >>"$output_dir/samples.csv"
      tail -n 1 "$run_dir/random-read-summary.csv" \
        >>"$output_dir/samples.csv"
      find "$run_dir/backend" -type f -delete
      find "$run_dir/backend" -depth -type d -empty -delete
    done
  done
done

printf '%s\n' "$output_dir"
