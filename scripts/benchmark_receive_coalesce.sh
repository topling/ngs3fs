#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
output_dir=${1:-"$project_dir/build/benchmarks/receive-coalesce-$run_id"}
output_dir=$(realpath -m "$output_dir")
repetitions=${BENCHMARK_REPETITIONS:-4}
base_port=${PORT:-17700}
advice=${RANDOM_READ_ADVICE:-normal}
read -r -a thresholds <<<"${RECEIVE_COALESCE_THRESHOLDS:-0 64KiB}"

if [[ -e "$output_dir" ]]; then
  echo "benchmark output directory already exists: $output_dir" >&2
  exit 1
fi
if ((repetitions <= 0)) || ((${#thresholds[@]} < 2)); then
  echo "need positive repetitions and at least two thresholds" >&2
  exit 2
fi
mkdir -p "$output_dir"

{
  uname -a
  lscpu
  free -h
  printf 'ngs3fs_commit=%s\n' "$(git -C "$project_dir" rev-parse HEAD)"
  printf 'thresholds=%s\n' "${thresholds[*]}"
  printf 'advice=%s\n' "$advice"
} >"$output_dir/system.txt"

printf '%s\n' \
  'threshold,repetition,client,advice,max_connections,files,threads,operations,pread_operations,mmap_operations,bytes,wall_ns,daemon_cpu_ns,daemon_cpu_ns_per_operation,s3_get_requests' \
  >"$output_dir/samples.csv"

sample=0
for ((repetition = 1; repetition <= repetitions; ++repetition)); do
  if ((repetition % 2 == 0)); then
    order=()
    for ((i = ${#thresholds[@]} - 1; i >= 0; --i)); do
      order+=("${thresholds[i]}")
    done
  else
    order=("${thresholds[@]}")
  fi
  for threshold in "${order[@]}"; do
    ((++sample))
    label=${threshold//[^[:alnum:]]/_}
    run_dir="$output_dir/r$repetition-$label"
    sync
    if ! echo 3 >/proc/sys/vm/drop_caches 2>/dev/null; then
      echo "warning: unable to drop kernel caches before sample $sample" >&2
    fi
    WORKLOAD=random-read \
    CLIENT=ngs3fs \
    RANDOM_READ_ADVICE="$advice" \
    RECEIVE_COALESCE_THRESHOLD="$threshold" \
    PORT="$((base_port + sample))" \
      "$project_dir/scripts/compare_goofys.sh" "$run_dir"
    printf '%s,%s,' "$threshold" "$repetition" \
      >>"$output_dir/samples.csv"
    tail -n 1 "$run_dir/random-read-summary.csv" \
      >>"$output_dir/samples.csv"
    find "$run_dir/backend" -type f -delete
    find "$run_dir/backend" -depth -type d -empty -delete
  done
done

metric_stats() {
  local threshold=$1
  local field=$2
  local value
  local sum=0
  local median
  local last
  local -a values

  mapfile -t values < <(
    awk -F, -v threshold="$threshold" -v field="$field" \
      'NR > 1 && $1 == threshold { print $field }' \
      "$output_dir/samples.csv" | sort -n
  )
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
  'threshold,samples,wall_min_ns,wall_median_ns,wall_mean_ns,wall_max_ns,cpu_min_ns,cpu_median_ns,cpu_mean_ns,cpu_max_ns,cpu_per_operation_min_ns,cpu_per_operation_median_ns,cpu_per_operation_mean_ns,cpu_per_operation_max_ns,s3_get_min,s3_get_median,s3_get_mean,s3_get_max' \
  >"$output_dir/summary.csv"
for threshold in "${thresholds[@]}"; do
  wall=$(metric_stats "$threshold" 12)
  cpu=$(metric_stats "$threshold" 13)
  cpu_per_operation=$(metric_stats "$threshold" 14)
  requests=$(metric_stats "$threshold" 15)
  printf '%s,%s,%s,%s,%s,%s\n' \
    "$threshold" "$repetitions" "$wall" "$cpu" \
    "$cpu_per_operation" "$requests" >>"$output_dir/summary.csv"
done

cat "$output_dir/summary.csv"
printf '%s\n' "$output_dir"
