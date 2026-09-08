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


if __name__ == "__main__":
    unittest.main()
