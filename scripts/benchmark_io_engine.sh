#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)
output_dir=${1:-"$project_dir/build/benchmarks/io-engine-$run_id"}
output_dir=$(realpath -m "$output_dir")
repetitions=${BENCHMARK_REPETITIONS:-3}
base_port=${PORT:-17800}
drop_caches=${BENCHMARK_DROP_CACHES:-1}
cache_drop_status=not-attempted

if [[ -e "$output_dir" ]]; then
  echo "benchmark output directory already exists: $output_dir" >&2
  exit 1
fi
if ((repetitions <= 0)); then
  echo "BENCHMARK_REPETITIONS must be positive" >&2
  exit 2
fi
if [[ "$drop_caches" != 0 && "$drop_caches" != 1 ]]; then
  echo "BENCHMARK_DROP_CACHES must be 0 or 1" >&2
  exit 2
fi
mkdir -p "$output_dir"

{
  uname -a
  lscpu
  free -h
  printf 'ngs3fs_commit=%s\n' "$(git -C "$project_dir" rev-parse HEAD)"
  printf 'repetitions=%s\n' "$repetitions"
  printf 'cache_drop_requested=%s\n' "$drop_caches"
  printf 'workload=random-read\n'
  printf 'advice=%s\n' "${RANDOM_READ_ADVICE:-random}"
  printf 'git_dirty=%s\n' "$(git -C "$project_dir" status --porcelain | tr '\n' ' ')"
  printf 'ngs3fs_path=%s\n' "$(realpath "${NGS3FS_BIN:-$project_dir/build/dev/ngs3fs}")"
  printf 'ngs3fs_sha256=%s\n' "$(sha256sum "${NGS3FS_BIN:-$project_dir/build/dev/ngs3fs}" 2>/dev/null | cut -d' ' -f1 || true)"
  printf 'goofys_path=%s\n' "$(realpath "${GOOFYS_BIN:-/home/leipeng/.cache/goofys-reference/bin/goofys}")"
  printf 'goofys_sha256=%s\n' "$(sha256sum "${GOOFYS_BIN:-/home/leipeng/.cache/goofys-reference/bin/goofys}" 2>/dev/null | cut -d' ' -f1 || true)"
  printf 'perf_event=process_cpu_time (/proc stat; no kernel sampling)\n'
} >"$output_dir/system.txt"

printf '%s\n' \
  'engine,reactors,repetition,client,advice,max_connections,files,threads,operations,pread_operations,mmap_operations,bytes,wall_ns,daemon_cpu_ns,daemon_cpu_ns_per_operation,s3_get_requests' \
  >"$output_dir/samples.csv"

engines=("legacy:1" "uring:1" "uring:4")
sample=0
for ((repetition = 1; repetition <= repetitions; ++repetition)); do
  # Rotate the first configuration so machine warm-up and temporal drift are
  # not consistently charged to the same engine.
  for ((configuration_index = 0;
        configuration_index < ${#engines[@]};
        ++configuration_index)); do
    configuration=${engines[(configuration_index + repetition - 1) % ${#engines[@]}]}
    engine=${configuration%%:*}
    reactors=${configuration##*:}
    ((++sample))
    run_dir="$output_dir/$engine-$reactors-r$repetition"
    if [[ "$drop_caches" == 1 ]]; then
      sync -f "$output_dir"
      if ! echo 3 2>/dev/null >/proc/sys/vm/drop_caches; then
        cache_drop_status=failed
        echo "warning: unable to drop kernel caches before sample $sample" >&2
      elif [[ "$cache_drop_status" != failed ]]; then
        cache_drop_status=success
      fi
    else
      cache_drop_status=skipped
    fi
    NGS3FS_IO_ENGINE="$engine" \
    NGS3FS_REACTORS="$reactors" \
    WORKLOAD=random-read \
    CLIENT=ngs3fs \
    RANDOM_READ_ADVICE="${RANDOM_READ_ADVICE:-random}" \
    CACHE_MODE=none \
    PORT="$((base_port + sample))" \
      "$project_dir/scripts/compare_goofys.sh" "$run_dir"
    sample_summary=$(tail -n 1 "$run_dir/random-read-summary.csv")
    printf '%s,%s,%s,%s\n' "$engine" "$reactors" "$repetition" \
      "$sample_summary" >>"$output_dir/samples.csv"
  done
done
printf 'cache_drop_status=%s\n' "$cache_drop_status" >>"$output_dir/system.txt"

metric_stats() {
  local engine=$1
  local reactors=$2
  local field=$3
  local value
  local sum=0
  local median
  local last
  local -a values
  mapfile -t values < <(
    awk -F, -v engine="$engine" -v reactors="$reactors" -v field="$field" \
      'NR > 1 && $1 == engine && $2 == reactors { print $field }' \
      "$output_dir/samples.csv" | sort -n
  )
  if ((${#values[@]} == 0)); then
    echo "missing benchmark samples: engine=$engine reactors=$reactors" >&2
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
  'engine,reactors,samples,wall_min_ns,wall_median_ns,wall_mean_ns,wall_max_ns,cpu_min_ns,cpu_median_ns,cpu_mean_ns,cpu_max_ns,cpu_per_operation_min_ns,cpu_per_operation_median_ns,cpu_per_operation_mean_ns,cpu_per_operation_max_ns,s3_get_min,s3_get_median,s3_get_mean,s3_get_max' \
  >"$output_dir/summary.csv"
for configuration in "${engines[@]}"; do
  engine=${configuration%%:*}
  reactors=${configuration##*:}
  wall=$(metric_stats "$engine" "$reactors" 13)
  cpu=$(metric_stats "$engine" "$reactors" 14)
  cpu_per_operation=$(metric_stats "$engine" "$reactors" 15)
  requests=$(metric_stats "$engine" "$reactors" 16)
  printf '%s,%s,%s,%s,%s,%s,%s\n' "$engine" "$reactors" "$repetitions" \
    "$wall" "$cpu" "$cpu_per_operation" "$requests" \
    >>"$output_dir/summary.csv"
done

awk -F, '
  NR == 1 { print "# ngs3fs io-engine random-read benchmark\n"; print "Median daemon CPU and wall time across repeated samples."; print ""; print "| Engine | Reactors | CPU/op (ms) | Wall (ms) | S3 GET |"; print "|---|---:|---:|---:|---:|"; next }
  { printf "| %s | %s | %.3f | %.3f | %s |\n", $1, $2, $13 / 1000000, $5 / 1000000, $17 }
' "$output_dir/summary.csv" >"$output_dir/summary.md"

cat "$output_dir/summary.md"
printf '%s\n' "$output_dir"
