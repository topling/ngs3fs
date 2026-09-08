#!/usr/bin/env python3
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


PROJECT_DIR = Path(__file__).resolve().parents[1]
PROFILE_SCRIPT = PROJECT_DIR / "scripts" / "profile_ngs3fs.sh"


def extract_measurement_function():
    lines = PROFILE_SCRIPT.read_text(encoding="utf-8").splitlines(keepends=True)
    start = lines.index("run_profile_measurement() {\n")
    for end in range(start + 1, len(lines)):
        if lines[end] == "}\n":
            return "".join(lines[start:end + 1])
    raise AssertionError("run_profile_measurement has no closing brace")


HARNESS = r"""
#!/usr/bin/env bash
set -euo pipefail

run_dir=$1
workload=$2
mount_dir="$run_dir/mnt"
object=object.bin
cache_mode=warm
cache_path="$run_dir/cache"
perf_lib=
perf=perf_stub
perf_frequency=4000
perf_event=stub
perf_mmap_size=1
perf_stat_events=stub
ngs3fs_pid=123
bench=mmap_bench_stub
random_bench=random_bench_stub
iterations=7
bytes=1024
random_advice_args=()
random_files=4
random_threads=3
random_operations=5
random_file_size=4096
random_maximum_read=512
random_seed=17
write_bytes=1000
write_block_size=256
write_files=3
cache_drop_status=skipped
perf_pid=
drop_calls=0
sleep_calls=0

mkdir -p "$run_dir"
printf 'old request outside measurement\n' >"$run_dir/versity-access.log"
: >"$run_dir/system.txt"
: >"$run_dir/bench-calls.txt"
: >"$run_dir/drop-calls.txt"

drop_measurement_caches() {
  drop_calls=$((drop_calls + 1))
  cache_drop_status="success-$drop_calls"
  printf '%s\n' "$attempt" >>"$run_dir/drop-calls.txt"
}

sleep() {
  sleep_calls=$((sleep_calls + 1))
}

kill() {
  return 0
}

wait() {
  return 0
}

emit_requests() {
  if [[ "$attempt" = 1 ]]; then
    printf '%s\n' s3_GetObject s3_GetObject s3_HeadObject \
      >>"$run_dir/versity-access.log"
  else
    printf '%s\n' s3_PutObject s3_PutObject s3_PutObject \
      s3_UploadPart s3_UploadPart s3_CompleteMultipartUpload \
      >>"$run_dir/versity-access.log"
  fi
}

record_call() {
  local name=$1
  shift
  printf '%s' "$name" >>"$run_dir/bench-calls.txt"
  printf '\t%s' "$@" >>"$run_dir/bench-calls.txt"
  printf '\n' >>"$run_dir/bench-calls.txt"
}

mmap_bench_stub() {
  record_call mmap "$@"
  emit_requests
  printf '{"result":"passed"}\n'
}

random_bench_stub() {
  record_call random "$@"
  emit_requests
  if [[ " $* " = *" -R "* ]]; then
    local count=0
    local previous=
    for value in "$@"; do
      if [[ "$previous" = -n ]]; then count=$value; fi
      previous=$value
    done
    local operations=$((random_threads * count))
    printf 'random-read stress passed: operations=%s pread_operations=%s mmap_operations=0 bytes=%s elapsed_ns=1 workload_cpu_ns=1\n' \
      "$count" "$operations" "$((operations * 128))"
  else
    printf 'random-read preparation passed: files=%s file_size=%s\n' \
      "$write_files" "$((write_bytes * multiplier / write_files))"
  fi
}

perf_stub() {
  local mode=$1
  shift
  if [[ "$mode" = record ]]; then
    return 0
  fi
  if [[ "$mode" != stat ]]; then
    return 2
  fi
  while (($#)); do
    if [[ "$1" = -- ]]; then
      shift
      "$@"
      return
    fi
    shift
  done
  return 2
}
"""


class ProfileMeasurementTest(unittest.TestCase):
    def run_workload(self, workload):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            harness = root / "measurement-harness.sh"
            harness.write_text(
                textwrap.dedent(HARNESS).lstrip()
                + "\n"
                + extract_measurement_function()
                + textwrap.dedent(
                    """

                    run_profile_measurement 1 1
                    cp "$run_dir/profile-metadata.txt" "$run_dir/metadata-1.txt"
                    run_profile_measurement 2 4
                    cp "$run_dir/profile-metadata.txt" "$run_dir/metadata-2.txt"
                    """
                ),
                encoding="utf-8",
            )
            subprocess.run(
                ["/usr/bin/bash", str(harness), str(root / "run"), workload],
                check=True,
                cwd=PROJECT_DIR,
                env={**os.environ, "PATH": "/usr/bin:/bin"},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            run_dir = root / "run"
            metadata = []
            for attempt in (1, 2):
                metadata.append(dict(
                    line.split("=", 1)
                    for line in (run_dir / f"metadata-{attempt}.txt")
                    .read_text(encoding="utf-8").splitlines()
                ))
            calls = [line.split("\t") for line in
                     (run_dir / "bench-calls.txt")
                     .read_text(encoding="utf-8").splitlines()]
            system = (run_dir / "system.txt").read_text(encoding="utf-8")
            drops = (run_dir / "drop-calls.txt").read_text(
                encoding="utf-8").splitlines()
            return metadata, calls, system, drops

    def assert_common_attempts(self, metadata, system, drops):
        self.assertEqual([row["profile_attempt"] for row in metadata], ["1", "2"])
        self.assertEqual(metadata[0]["s3_get_requests"], "2")
        self.assertEqual(metadata[0]["s3_head_requests"], "1")
        self.assertEqual(metadata[0]["s3_put_requests"], "0")
        self.assertEqual(metadata[1]["s3_get_requests"], "0")
        self.assertEqual(metadata[1]["s3_head_requests"], "0")
        self.assertEqual(metadata[1]["s3_put_requests"], "3")
        self.assertEqual(metadata[1]["s3_upload_part_requests"], "2")
        self.assertEqual(metadata[1]["s3_complete_requests"], "1")
        self.assertEqual(drops, ["1", "2"])
        self.assertIn("cache_drop_status_attempt_1=success-1\n", system)
        self.assertIn("cache_drop_status_attempt_2=success-2\n", system)

    def test_mmap_first_pass_and_retry_scale_operations(self):
        metadata, calls, system, drops = self.run_workload("mmap")
        self.assert_common_attempts(metadata, system, drops)
        self.assertEqual([row["actual_operations"] for row in metadata],
                         ["7", "28"])
        self.assertEqual([row["actual_operations_unit"] for row in metadata],
                         ["iterations", "iterations"])
        self.assertEqual([row["actual_bytes"] for row in metadata],
                         ["7168", "28672"])
        self.assertEqual([call[-2] for call in calls], ["7", "28"])

    def test_random_read_first_pass_and_retry_scale_each_thread(self):
        metadata, calls, system, drops = self.run_workload("random-read")
        self.assert_common_attempts(metadata, system, drops)
        self.assertEqual([row["actual_operations"] for row in metadata],
                         ["15", "60"])
        self.assertEqual([row["actual_operations_unit"] for row in metadata],
                         ["operations", "operations"])
        self.assertTrue(all("actual_bytes" not in row for row in metadata))
        counts = []
        for call in calls:
            counts.append(call[call.index("-n") + 1])
        self.assertEqual(counts, ["5", "20"])

    def test_write_reports_files_and_integer_rounded_bytes(self):
        metadata, calls, system, drops = self.run_workload("write")
        self.assert_common_attempts(metadata, system, drops)
        self.assertEqual([row["actual_operations"] for row in metadata],
                         ["3", "3"])
        self.assertEqual([row["actual_operations_unit"] for row in metadata],
                         ["files", "files"])
        self.assertEqual([row["actual_bytes"] for row in metadata],
                         ["999", "3999"])
        counts = []
        sizes = []
        for call in calls:
            counts.append(call[call.index("-n") + 1])
            sizes.append(call[call.index("-s") + 1])
        self.assertEqual(counts, ["1", "1"])
        self.assertEqual(sizes, ["333", "1333"])
        directories = [Path(call[call.index("-d") + 1]).name for call in calls]
        self.assertEqual(directories,
                         ["profile-write-attempt-1", "profile-write-attempt-2"])

    def test_cold_cache_without_samples_does_not_retry_warmed_target(self):
        source = PROFILE_SCRIPT.read_text(encoding="utf-8")
        marker = 'if [[ ! -s "$run_dir/perf.folded" ]]; then\n'
        start = source.index(marker)
        end = source.index(marker, start + len(marker))
        with tempfile.TemporaryDirectory() as temporary:
            harness = Path(temporary) / "cold-retry-harness.sh"
            harness.write_text(
                '#!/usr/bin/env bash\nset -euo pipefail\n'
                'run_dir=$1\ncache_mode=cold\n'
                'run_profile_measurement() { exit 99; }\n'
                + source[start:end], encoding="utf-8")
            result = subprocess.run(
                ["/usr/bin/bash", str(harness), temporary],
                capture_output=True, text=True,
                env={**os.environ, "PATH": "/usr/bin:/bin"})
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("enlarge the workload and rerun", result.stderr)


if __name__ == "__main__":
    unittest.main()
