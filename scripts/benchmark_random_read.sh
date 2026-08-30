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

metric_stats() {
  local advice=$1
  local client=$2
  local field=$3
  local value
  local sum=0
  local median
  local last
  local -a values

  mapfile -t values < <(
    awk -F, -v advice="$advice" -v client="$client" -v field="$field" \
      'NR > 1 && $2 == client && $3 == advice { print $field }' \
      "$output_dir/samples.csv" | sort -n
  )
  if ((${#values[@]} == 0)); then
    echo "missing benchmark samples: advice=$advice client=$client" >&2
    return 1
  fi
  for value in "${values[@]}"; do
    sum=$((sum + value))
  done
  if ((${#values[@]} % 2 == 0)); then
    median=$(((values[${#values[@]} / 2 - 1] +
               values[${#values[@]} / 2]) / 2))
  else
    median=${values[${#values[@]} / 2]}
  fi
  last=$((${#values[@]} - 1))
  printf '%s,%s,%s,%s' "${values[0]}" "$median" \
    "$((sum / ${#values[@]}))" "${values[$last]}"
}

printf '%s\n' \
  'advice,client,samples,wall_min_ns,wall_median_ns,wall_mean_ns,wall_max_ns,cpu_min_ns,cpu_median_ns,cpu_mean_ns,cpu_max_ns,cpu_per_operation_min_ns,cpu_per_operation_median_ns,cpu_per_operation_mean_ns,cpu_per_operation_max_ns,s3_get_min,s3_get_median,s3_get_mean,s3_get_max' \
  >"$output_dir/summary.csv"
for advice in random normal; do
  for client in ngs3fs goofys; do
    wall=$(metric_stats "$advice" "$client" 11)
    cpu=$(metric_stats "$advice" "$client" 12)
    cpu_per_operation=$(metric_stats "$advice" "$client" 13)
    requests=$(metric_stats "$advice" "$client" 14)
    printf '%s,%s,%s,%s,%s,%s,%s\n' \
      "$advice" "$client" "$repetitions" "$wall" "$cpu" \
      "$cpu_per_operation" "$requests" >>"$output_dir/summary.csv"
  done
done

cat "$output_dir/summary.csv"
printf '%s\n' "$output_dir"
