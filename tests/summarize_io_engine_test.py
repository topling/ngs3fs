#!/usr/bin/env python3
import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "summarize_io_engine.py"
SPEC = importlib.util.spec_from_file_location("summarize_io_engine", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class SummarizeIoEngineTest(unittest.TestCase):
    def test_loads_medians_and_emits_sqpoll_deltas(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            suite = root / "github-io-engine-normal"
            suite.mkdir()
            configs = (("legacy", 1, (100, 120, 140), 200),
                       ("uring", 1, (80, 90, 100), 150),
                       ("uring", 4, (110, 120, 130), 220),
                       ("uring-sqpoll", 1, (70, 80, 90), 120))
            for engine, reactors, totals, _daemon in configs:
                for index, total in enumerate(totals, 1):
                    sample = suite / f"{engine}-{reactors}-r{index}"
                    sample.mkdir()
                    with (sample / "client-cpu.csv").open("w", newline="", encoding="utf-8") as stream:
                        writer = csv.writer(stream)
                        writer.writerow(("client", "daemon_cpu_ns", "workload_cpu_ns", "total_cpu_ns_per_operation"))
                        writer.writerow(("ngs3fs", total * 2, total, total))
            with (suite / "summary.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(("engine", "reactors", "samples", "cpu_per_operation_median_ns"))
                for engine, reactors, _totals, daemon in configs:
                    writer.writerow((engine, reactors, 3, daemon))
            data = {"normal": MODULE.load_suite_directory(suite)}
            text = MODULE.report(data)
            self.assertIn("uring-sqpoll:1", text)
            self.assertIn("daemon -20.00%", text)
            self.assertIn("total -11.11%", text)
            self.assertIn("| uring-sqpoll:1 | 3 | 120 ns | 80 ns |", text)
            rendered = MODULE.html(data)
            self.assertIn("<table>", rendered)
            self.assertIn("<td>uring-sqpoll:1</td>", rendered)

    def test_rejects_missing_summary_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary) / "github-io-engine-normal"
            suite.mkdir()
            (suite / "summary.csv").write_text(
                "engine,reactors,samples,cpu_per_operation_median_ns\nlegacy,1,1,10\nuring,1,1,20\n",
                encoding="utf-8")
            sample = suite / "legacy-1-r1"
            sample.mkdir()
            (sample / "client-cpu.csv").write_text(
                "client,total_cpu_ns_per_operation\nngs3fs,10\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing"):
                MODULE.load_suite_directory(suite)

    def test_zero_baseline_is_reported_as_not_available(self):
        self.assertEqual(MODULE.pct(10, 0), "n/a (zero baseline)")

    def test_emits_matched_four_reactor_delta(self):
        data = {"affinity-normal": {
            ("baseline", 4): (3, 200, 300),
            ("current", 4): (3, 150, 240),
        }}
        text = MODULE.report(data)
        self.assertIn("Matched four-reactor delta (current versus baseline)", text)
        self.assertIn("daemon -25.00%; total -20.00%", text)
        rendered = MODULE.html(data)
        self.assertIn("Matched four-reactor delta (current versus baseline)", rendered)
        self.assertIn("daemon -25.00%; total -20.00%", rendered)
        provenance = ("Baseline: owner-complete-unbatched. Current: "
                      "cached-source-owner-ring-unbatched.")
        titled = MODULE.html(data, title="Matched four-reactor CPU comparison",
                             provenance=provenance)
        self.assertIn("<h1>Matched four-reactor CPU comparison</h1>", titled)
        self.assertIn("Baseline: owner-complete-unbatched", titled)
        self.assertIn("Current: cached-source-owner-ring-unbatched", titled)

    def test_manifest_provenance_overrides_historical_descriptions(self):
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary) / "github-io-engine-affinity-normal"
            suite.mkdir()
            (suite / "system.txt").write_text(
                "benchmark_variant_revisions="
                "baseline=a3009ab3998e7cdad6e7c2321ecf2219dbade5e6 "
                "current=deadbeef\n"
                "benchmark_variant_descriptions="
                "baseline=owner-complete-unbatched "
                "current=cached-source-owner-ring-unbatched\n",
                encoding="utf-8")
            provenance = MODULE.owner_complete_provenance([suite])
            self.assertIn(
                "Baseline: owner-complete-unbatched "
                "(a3009ab3998e7cdad6e7c2321ecf2219dbade5e6)",
                provenance)
            self.assertIn(
                "Current: cached-source-owner-ring-unbatched (deadbeef)",
                provenance)
            self.assertNotIn("batched owner-complete execution", provenance)

    def test_rejects_inconsistent_manifest_provenance(self):
        with tempfile.TemporaryDirectory() as temporary:
            directories = []
            for index, current in enumerate(("first", "second")):
                suite = Path(temporary) / f"suite-{index}"
                suite.mkdir()
                (suite / "system.txt").write_text(
                    "benchmark_variant_revisions=baseline=base "
                    f"current={current}\n"
                    "benchmark_variant_descriptions=baseline=old "
                    "current=new\n",
                    encoding="utf-8")
                directories.append(suite)
            with self.assertRaisesRegex(ValueError, "inconsistent"):
                MODULE.owner_complete_provenance(directories)

    def test_even_number_of_samples_uses_median(self):
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary) / "github-io-engine-normal"
            suite.mkdir()
            for index, total in enumerate((100, 200), 1):
                sample = suite / f"legacy-1-r{index}"
                sample.mkdir()
                (sample / "client-cpu.csv").write_text(
                    "client,total_cpu_ns_per_operation\nngs3fs,%d\n" % total,
                    encoding="utf-8")
            (suite / "summary.csv").write_text(
                "engine,reactors,samples,cpu_per_operation_median_ns\nlegacy,1,2,50\n",
                encoding="utf-8")
            self.assertEqual(MODULE.load_suite_directory(suite)[("legacy", 1)][2], 150)

    def test_cached_reply_evidence_uses_actual_payload_counters(self):
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary) / "github-io-engine-affinity-cache-warm"
            suite.mkdir()
            for engine in ("baseline", "current"):
                for repetition in range(1, 4):
                    sample = suite / f"{engine}-4-r{repetition}"
                    sample.mkdir()
                    (sample / "client-cpu.csv").write_text(
                        "client,total_cpu_ns_per_operation\nngs3fs,1\n",
                        encoding="utf-8")
                    (sample / "ngs3fs-threads-after-workload.txt").write_text(
                        "vm_rss_kib=%d\nvm_hwm_kib=%d\n" % (
                            100 + repetition, 200 + repetition),
                        encoding="utf-8")
                    if engine == "current":
                        (sample / "ngs3fs.log").write_text(
                            "io_uring cached reply source_submissions: "
                            "mode=PREAD size=<=64KiB count=%d bytes=%d\n"
                            "io_uring cached reply source_submissions: "
                            "mode=SPLICE size=<=128KiB count=%d bytes=%d\n" % (
                                10 + repetition, 1000 + repetition,
                                20 + repetition, 2000 + repetition),
                            encoding="utf-8")
            partial = suite / "baseline-4-r4"
            partial.mkdir()
            (partial / "ngs3fs-threads-after-workload.txt").write_text(
                "vm_rss_kib=9999\nvm_hwm_kib=9999\n", encoding="utf-8")
            (partial / "ngs3fs.log").write_text(
                "io_uring cached reply source_submissions: "
                "mode=PREAD size=<=64KiB count=9999 bytes=9999\n",
                encoding="utf-8")
            directories = {"affinity-cache-warm": suite}
            text = MODULE.cached_reply_evidence_markdown(directories)
            self.assertIn("actual fd-backed FUSE reply payload", text)
            self.assertIn("not the application read size", text)
            self.assertIn("across the entire mount lifetime", text)
            self.assertIn("not the application read size or only the 8,192", text)
            self.assertIn("submitted/requested payload bytes", text)
            self.assertIn("Size bins are mutually exclusive", text)
            self.assertIn(
                "| affinity-cache-warm | baseline:4 | 0/3 | unavailable",
                text)
            self.assertIn(
                "| affinity-cache-warm | current:4 | 3/3 | PREAD | "
                "<=64KiB | 12 | 1,002 |", text)
            self.assertIn(
                "| affinity-cache-warm | current:4 | 3/3 | SPLICE | "
                "<=128KiB | 22 | 2,002 |", text)
            self.assertIn(
                "| affinity-cache-warm | current:4 | 3 | 102 KiB | "
                "202 KiB |", text)
            rendered = MODULE.cached_reply_evidence_html(directories)
            self.assertIn("Actual payload bin", rendered)
            self.assertIn("<td>PREAD</td><td>&lt;=64KiB</td>", rendered)


if __name__ == "__main__":
    unittest.main()
