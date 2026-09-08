#!/usr/bin/env python3
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


PROJECT_DIR = Path(__file__).resolve().parents[1]
PROFILE_SCRIPT = PROJECT_DIR / "scripts" / "profile_ngs3fs.sh"
COMPARE_SCRIPT = PROJECT_DIR / "scripts" / "compare_goofys.sh"
CI_WORKFLOW = PROJECT_DIR / ".github" / "workflows" / "ci.yml"


def extract_shell_function(name, script=PROFILE_SCRIPT):
    lines = script.read_text(encoding="utf-8").splitlines(keepends=True)
    start = lines.index(f"{name}() {{\n")
    for end in range(start + 1, len(lines)):
        if lines[end] == "}\n":
            return "".join(lines[start:end + 1])
    raise AssertionError(f"{name} has no closing brace")


MEASUREMENT_FUNCTIONS = "\n".join(
    extract_shell_function(name)
    for name in ("close_perf_control", "control_perf_record",
                 "stop_perf_record", "run_profile_measurement")
)


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
perf_control_fd=
perf_ack_fd=
perf_control_fifo=
perf_ack_fifo=
perf_control_timeout_seconds=${PERF_CONTROL_TIMEOUT_SECONDS:-0.2}
perf_startup_timeout_seconds=${PERF_STARTUP_TIMEOUT_SECONDS:-0.2}
perf_record_log=
perf_diagnostic_log=
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
: >"$run_dir/perf-control.txt"
: >"$run_dir/perf-control-timeouts.txt"

read() {
  local previous=
  local timeout=
  local value
  for value in "$@"; do
    if [[ "$previous" = -t ]]; then timeout=$value; fi
    previous=$value
  done
  if [[ -n "$timeout" ]]; then
    printf '%s\n' "$timeout" >>"$run_dir/perf-control-timeouts.txt"
  fi
  builtin read "$@"
}

drop_measurement_caches() {
  drop_calls=$((drop_calls + 1))
  cache_drop_status="success-$drop_calls"
  printf '%s\n' "$attempt" >>"$run_dir/drop-calls.txt"
}

sleep() {
  sleep_calls=$((sleep_calls + 1))
}

kill() {
  if [[ "$1" = -0 && "${PERF_STUB_NOT_ALIVE:-}" = 1 ]]; then
    return 1
  fi
  if [[ "$1" = -INT && "${PERF_STUB_INTERRUPT_FAIL:-}" = 1 ]]; then
    return 1
  fi
  return 0
}

wait() {
  return "${PERF_STUB_WAIT_STATUS:-0}"
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
    printf 'perf record stub started\n' >&2
    local control_spec=
    local delayed=0
    local verbose=0
    while (($#)); do
      case "$1" in
        -vvv)
          verbose=1
          shift
          ;;
        --control)
          control_spec=$2
          shift 2
          ;;
        --delay)
          [[ "$2" = -1 ]] && delayed=1
          shift 2
          ;;
        *) shift ;;
      esac
    done
    [[ "$verbose" = 1 && "$delayed" = 1 && "$control_spec" = fifo:*,* ]] || return 2
    local fifo_spec=${control_spec#fifo:}
    local control_fifo=${fifo_spec%%,*}
    local ack_fifo=${fifo_spec#*,}
    local command
    exec 7<>"$control_fifo"
    exec 8<>"$ack_fifo"
    while IFS= read -r command <&7; do
      printf '%s:%s\n' "$attempt" "$command" \
        >>"$run_dir/perf-control.txt"
      if [[ "${PERF_STUB_MISSING_ACK:-}" = "$command" ]]; then
        return 0
      fi
      # perf writes sizeof("ack\n"), including the trailing NUL.
      printf 'ack\n\0' >&8
      if [[ "$command" = disable ]]; then
        return 0
      fi
    done
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
    def test_thread_census_records_process_resident_memory(self):
        for script in (PROFILE_SCRIPT, COMPARE_SCRIPT):
            with self.subTest(script=script.name), \
                    tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "threads.txt"
                harness = Path(temporary) / "census-harness.sh"
                harness.write_text(
                    "#!/usr/bin/env bash\nset -euo pipefail\n"
                    + extract_shell_function("capture_thread_census", script)
                    + f'capture_thread_census $$ "{output}"\n',
                    encoding="utf-8")
                subprocess.run(
                    ["/usr/bin/bash", str(harness)], check=True,
                    cwd=PROJECT_DIR,
                    env={**os.environ, "PATH": "/usr/bin:/bin"})
                evidence = output.read_text(encoding="utf-8")
                self.assertIn("memory_source=/proc/", evidence)
                self.assertRegex(evidence, r"(?m)^vm_rss_kib=[0-9]+$")
                self.assertRegex(evidence, r"(?m)^vm_hwm_kib=[0-9]+$")

    def test_independent_benchmark_evidence_runs_after_profile_failure(self):
        source = CI_WORKFLOW.read_text(encoding="utf-8")
        for name in ("Compare Mountpoint concurrent random reads",
                     "A/B test socket receive buffer"):
            start = source.index(f"      - name: {name}\n")
            run = source.index("        run: |\n", start)
            step_header = source[start:run]
            self.assertIn("        if: always()\n", step_header)

    def test_stop_records_unsent_interrupt_and_cleanup_uses_it(self):
        cleanup = extract_shell_function("cleanup")
        self.assertIn("stop_perf_record 1", cleanup)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            harness = root / "stop-harness.sh"
            harness.write_text(
                textwrap.dedent(HARNESS).lstrip()
                + "\n"
                + extract_shell_function("stop_perf_record")
                + "\nperf_pid=123\n"
                + 'perf_diagnostic_log="$run_dir/perf-diagnostic.txt"\n'
                + "stop_perf_record 1\n",
                encoding="utf-8")
            result = subprocess.run(
                ["/usr/bin/bash", str(harness), str(root / "run"), "mmap"],
                cwd=PROJECT_DIR,
                env={**os.environ, "PATH": "/usr/bin:/bin",
                     "PERF_STUB_INTERRUPT_FAIL": "1",
                     "PERF_STUB_WAIT_STATUS": "42"},
                capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("perf_record_interrupted_by_harness=no",
                          result.stderr)
            self.assertIn("perf_record_exit_status=42", result.stderr)

    def test_unexpected_perf_exit_rejects_measurement_and_closes_fifos(self):
        cases = (("255", "1", False), ("130", "1", False),
                 ("130", "0", True), ("0", "1", True))
        for status, not_alive, success in cases:
            with self.subTest(status=status, not_alive=not_alive), \
                    tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                harness = root / "measurement-harness.sh"
                harness.write_text(
                    textwrap.dedent(HARNESS).lstrip() + "\n"
                    + MEASUREMENT_FUNCTIONS
                    + "\nrun_profile_measurement 1 1\n",
                    encoding="utf-8")
                run_dir = root / "run"
                result = subprocess.run(
                    ["/usr/bin/bash", str(harness), str(run_dir), "mmap"],
                    cwd=PROJECT_DIR,
                    env={**os.environ, "PATH": "/usr/bin:/bin",
                         "PERF_STUB_WAIT_STATUS": status,
                         "PERF_STUB_NOT_ALIVE": not_alive},
                    capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, success)
                self.assertEqual(list(run_dir.glob("perf-*.fifo")), [])
                diagnostic = (run_dir / "perf-diagnostic-1.txt").read_text(
                    encoding="utf-8")
                self.assertIn(f"perf_record_exit_status={status}", diagnostic)
                if not success:
                    self.assertIn("perf record failed:", result.stderr)

    def test_workflow_retains_perf_startup_diagnostics(self):
        source = CI_WORKFLOW.read_text(encoding="utf-8")
        for directory in ("github-io-engine-*", "github-*-advice",
                          "github-cached-write"):
            self.assertIn(
                f"build/profiles/{directory}/perf-record-*.log", source)
            self.assertIn(
                f"build/profiles/{directory}/perf-diagnostic-*.txt", source)

    def test_cached_profile_matrix_includes_owner_workers(self):
        source = CI_WORKFLOW.read_text(encoding="utf-8")
        names = (
            "Profile cached block reads on one and four reactors and SQPOLL",
            "Profile cached-source owner-ring worker model",
        )
        with tempfile.TemporaryDirectory() as temporary:
            harness = Path(temporary) / "profile-matrix.sh"
            bodies = []
            for name in names:
                start = source.index(f"      - name: {name}\n")
                start = source.index("        run: |\n", start)
                start += len("        run: |\n")
                end = source.index("\n      - name:", start)
                bodies.append(textwrap.dedent(source[start:end]))
            harness.write_text(
                '#!/usr/bin/env bash\nset -euo pipefail\n'
                'GITHUB_WORKSPACE=/unit-test\n'
                'perf() { :; }\n'
                'sudo() { printf "%s\\n" "$*"; }\n'
                + "\n".join(bodies), encoding="utf-8")
            result = subprocess.run(
                ["/usr/bin/bash", str(harness)], check=True,
                capture_output=True, text=True,
                env={**os.environ, "PATH": "/usr/bin:/bin"})
        rows = result.stdout.splitlines()
        self.assertEqual(len(rows), 10)
        for mode in ("cold", "warm"):
            normal = [row for row in rows if row.endswith(
                f"github-io-engine-cache-{mode}-uring-4")]
            self.assertEqual(len(normal), 1)
            self.assertIn("NGS3FS_REACTORS=4", normal[0])
            for variant in ("baseline", "current"):
                matched = [row for row in rows if row.endswith(
                    f"github-io-engine-affinity-cache-{mode}-{variant}")]
                self.assertEqual(len(matched), 1)
                row = matched[0]
                for setting in ("NGS3FS_IO_ENGINE=uring", "NGS3FS_REACTORS=4",
                                "MAX_CONNECTIONS=8", "RANDOM_READ_ADVICE=random",
                                "RANDOM_READ_OPERATIONS=512", f"CACHE_MODE={mode}"):
                    self.assertIn(setting, row)
                directory = "baseline" if variant == "baseline" else "dev"
                self.assertIn(f"NGS3FS_BIN=/unit-test/build/{directory}/ngs3fs", row)

    def run_workload(self, workload):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            harness = root / "measurement-harness.sh"
            harness.write_text(
                textwrap.dedent(HARNESS).lstrip()
                + "\n"
                + MEASUREMENT_FUNCTIONS
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
            controls = (run_dir / "perf-control.txt").read_text(
                encoding="utf-8").splitlines()
            fifos = list(run_dir.glob("perf-*.fifo"))
            return metadata, calls, system, drops, controls, fifos

    def assert_common_attempts(self, metadata, system, drops,
                               controls, fifos):
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
        self.assertEqual(controls,
                         ["1:enable", "1:disable", "2:enable", "2:disable"])
        self.assertEqual(fifos, [])

    def test_mmap_first_pass_and_retry_scale_operations(self):
        metadata, calls, system, drops, controls, fifos = \
            self.run_workload("mmap")
        self.assert_common_attempts(metadata, system, drops, controls, fifos)
        self.assertEqual([row["actual_operations"] for row in metadata],
                         ["7", "28"])
        self.assertEqual([row["actual_operations_unit"] for row in metadata],
                         ["iterations", "iterations"])
        self.assertEqual([row["actual_bytes"] for row in metadata],
                         ["7168", "28672"])
        self.assertEqual([call[-2] for call in calls], ["7", "28"])

    def test_random_read_first_pass_and_retry_scale_each_thread(self):
        metadata, calls, system, drops, controls, fifos = \
            self.run_workload("random-read")
        self.assert_common_attempts(metadata, system, drops, controls, fifos)
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
        metadata, calls, system, drops, controls, fifos = \
            self.run_workload("write")
        self.assert_common_attempts(metadata, system, drops, controls, fifos)
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

    def test_missing_perf_ack_times_out_and_removes_control_fifos(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            harness = root / "measurement-harness.sh"
            harness.write_text(
                textwrap.dedent(HARNESS).lstrip()
                + "\n"
                + MEASUREMENT_FUNCTIONS
                + "\nrun_profile_measurement 1 1\n",
                encoding="utf-8",
            )
            run_dir = root / "run"
            result = subprocess.run(
                ["/usr/bin/bash", str(harness), str(run_dir), "mmap"],
                cwd=PROJECT_DIR,
                env={**os.environ, "PATH": "/usr/bin:/bin",
                     "PERF_STUB_MISSING_ACK": "enable",
                     "PERF_STUB_NOT_ALIVE": "1",
                     "PERF_STUB_WAIT_STATUS": "42",
                     "PERF_STARTUP_TIMEOUT_SECONDS": "0.05",
                     "PERF_CONTROL_TIMEOUT_SECONDS": "0.05"},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("timed out waiting for perf record enable acknowledgement",
                          result.stderr)
            self.assertIn("perf_record_pid=", result.stderr)
            self.assertIn("perf_record_alive=", result.stderr)
            self.assertIn("perf_record_stderr=", result.stderr)
            self.assertIn("perf record stub started", result.stderr)
            self.assertIn("perf_record_interrupted_by_harness=no", result.stderr)
            self.assertIn("perf_record_exit_status=42", result.stderr)
            diagnostic = (run_dir / "perf-diagnostic-1.txt").read_text(
                encoding="utf-8")
            self.assertIn("timed out waiting for perf record enable", diagnostic)
            self.assertIn("perf_record_alive=no", diagnostic)
            self.assertIn("perf_record_exit_status=42", diagnostic)
            self.assertIn("perf record stub started",
                          (run_dir / "perf-record-1.log").read_text(
                              encoding="utf-8"))
            timeouts = (run_dir / "perf-control-timeouts.txt").read_text(
                encoding="utf-8").splitlines()
            self.assertEqual(timeouts, ["0.05"])
            self.assertEqual(list(run_dir.glob("perf-*.fifo")), [])

    def test_runtime_ack_uses_separate_timeout_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            harness = root / "measurement-harness.sh"
            harness.write_text(
                textwrap.dedent(HARNESS).lstrip()
                + "\n"
                + MEASUREMENT_FUNCTIONS
                + "\nrun_profile_measurement 1 1\n",
                encoding="utf-8",
            )
            run_dir = root / "run"
            result = subprocess.run(
                ["/usr/bin/bash", str(harness), str(run_dir), "mmap"],
                cwd=PROJECT_DIR,
                env={**os.environ, "PATH": "/usr/bin:/bin",
                     "PERF_STUB_MISSING_ACK": "disable",
                     "PERF_STARTUP_TIMEOUT_SECONDS": "2",
                     "PERF_CONTROL_TIMEOUT_SECONDS": "0.05"},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("timed out waiting for perf record disable acknowledgement",
                          result.stderr)
            controls = (run_dir / "perf-control.txt").read_text(
                encoding="utf-8").splitlines()
            self.assertEqual(controls, ["1:enable", "1:disable"])
            timeouts = (run_dir / "perf-control-timeouts.txt").read_text(
                encoding="utf-8").splitlines()
            self.assertEqual(timeouts, ["2", "0.05"])
            self.assertTrue((run_dir / "mmap.jsonl").is_file())
            self.assertEqual(list(run_dir.glob("perf-*.fifo")), [])

    def test_compare_cpu_window_starts_after_perf_attach_delay(self):
        source = COMPARE_SCRIPT.read_text(encoding="utf-8")
        start = source.index("run_case() {\n")
        end = source.index("\n}\n\nrun_random_read_case()", start)
        run_case = source[start:end]
        perf = run_case.index('    "$perf" stat')
        delay = run_case.index("    sleep 0.1", perf)
        cpu_start = run_case.index(
            '  start_ns=$(process_cpu_ns "$daemon_pid")', delay)
        workload = run_case.index('  "$bench" "$mount_dir/$object"', cpu_start)
        self.assertLess(perf, delay)
        self.assertLess(delay, cpu_start)
        self.assertLess(cpu_start, workload)

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
