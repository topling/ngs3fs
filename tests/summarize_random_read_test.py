#!/usr/bin/env python3

import csv
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "summarize_random_read",
    Path(__file__).resolve().parents[1] / "scripts/summarize_random_read.py",
)
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


class RandomReadReportTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="ngs3fs-report-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.data = self.root / "data"
        self.data.mkdir()
        inputs = []
        for mode, engine in [("none", "uring"), ("warm", "legacy"), ("old", None)]:
            path = self.root / mode
            path.mkdir()
            inputs.append(path)
            rows = [
                dict(advice="normal", client=name, cache_mode=mode,
                     cpu_per_operation_median_ns=cpu, wall_median_ns=cpu * 100,
                     s3_get_median=12, samples=2)
                for name, cpu in [("ngs3fs", 1_000_000), ("mountpoint-s3", 2_000_000)]
            ]
            with (path / "summary.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=rows[0])
                writer.writeheader()
                writer.writerows(rows)
            if engine:
                (path / "system.txt").write_text(
                    f"ngs3fs_io_engine={engine}\nngs3fs_reactors=1\n",
                    encoding="utf-8",
                )
        self.inputs = inputs
        self.rows = summary.read_comparisons(inputs, self.data)
        self.modes = {row["suite"]: row for row in self.rows}

    def write_client_cpu_sample(self, suite, sample, client, advice,
                                daemon_cpu, workload_cpu, operations=10):
        path = self.root / suite / sample
        path.mkdir()
        total_cpu = daemon_cpu + workload_cpu
        with (path / "client-cpu.csv").open(
                "w", newline="", encoding="utf-8") as stream:
            fields = [
                "client", "daemon_cpu_ns", "workload_cpu_ns",
                "total_cpu_ns", "total_cpu_ns_per_operation",
            ]
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerow({
                "client": client,
                "daemon_cpu_ns": daemon_cpu,
                "workload_cpu_ns": workload_cpu,
                "total_cpu_ns": total_cpu,
                "total_cpu_ns_per_operation": total_cpu // operations,
            })
        with (path / "random-read-summary.csv").open(
                "w", newline="", encoding="utf-8") as stream:
            fields = ["client", "advice", "pread_operations", "mmap_operations"]
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerow({
                "client": client,
                "advice": advice,
                "pread_operations": operations // 2,
                "mmap_operations": operations - operations // 2,
            })

    def test_recorded_settings_and_missing_metadata(self):
        self.assertEqual(self.modes["none"]["ngs3fs_io_engine"], "uring")
        self.assertEqual(self.modes["none"]["ngs3fs_reactors"], "1")
        self.assertEqual(self.modes["warm"]["ngs3fs_io_engine"], "legacy")
        self.assertEqual(self.modes["warm"]["ngs3fs_reactors"], "1")
        self.assertEqual(self.modes["old"]["ngs3fs_io_engine"], "not recorded")
        self.assertEqual(self.modes["old"]["ngs3fs_reactors"], "not recorded")

    def test_markdown_and_pages_identify_engine_without_changing_ratio_header(self):
        markdown = self.root / "summary.md"
        page = self.root / "index.html"
        summary.write_markdown(markdown, self.rows, "test", "")
        summary.write_html(page, self.rows, "test", "")
        md = markdown.read_text(encoding="utf-8")
        html = page.read_text(encoding="utf-8")
        for report in (md, html):
            with self.subTest(report="Markdown" if report == md else "HTML"):
                self.assertIn("uring (1 reactor)", report)
                self.assertIn("legacy (threaded)", report)
                self.assertIn("not recorded", report)
        self.assertIn("| Reference / ngs3fs | CPU saved |", md)
        self.assertIn("Reference CPU ÷<br>ngs3fs CPU", html)
        self.assertIn("2.00x", md)
        self.assertIn("2.00×", html)

    def test_csv_keeps_settings_and_reference_over_ngs3fs_cpu(self):
        path = self.root / "combined.csv"
        summary.write_csv(path, self.rows)
        with path.open(newline="", encoding="utf-8") as stream:
            rows = {row["suite"]: row for row in csv.DictReader(stream)}
        self.assertEqual(rows["none"]["ngs3fs_io_engine"], "uring")
        self.assertEqual(rows["none"]["ngs3fs_reactors"], "1")
        self.assertEqual(rows["warm"]["ngs3fs_io_engine"], "legacy")
        self.assertEqual(rows["old"]["ngs3fs_io_engine"], "not recorded")
        for row in rows.values():
            with self.subTest(suite=row["suite"]):
                self.assertEqual(int(row["ngs3fs_cpu_per_operation_ns"]), 1_000_000)
                self.assertEqual(int(row["reference_cpu_per_operation_ns"]), 2_000_000)
                self.assertEqual(float(row["reference_cpu_over_ngs3fs"]), 2.0)
                self.assertEqual(float(row["ngs3fs_cpu_saving_percent"]), 50.0)

    def test_pages_links_dispatch_recycling_report_when_present(self):
        page = self.root / "index.html"
        (self.root / "inode-affinity-comparison.html").write_text(
            "test", encoding="utf-8")
        summary.write_html(page, self.rows, "test", "")
        rendered = page.read_text(encoding="utf-8")
        self.assertIn('href="inode-affinity-comparison.html"', rendered)
        self.assertIn("Dispatch return-pipe baseline vs direct shared lock-free recycling", rendered)

    def test_total_client_cpu_uses_daemon_plus_workload_and_links_samples(self):
        samples = [
            ("r1-ngs3fs", "ngs3fs", 8_000_000, 2_000_000),
            ("r2-ngs3fs", "ngs3fs", 16_000_000, 4_000_000),
            ("r1-reference", "mountpoint-s3", 12_000_000, 8_000_000),
            ("r2-reference", "mountpoint-s3", 24_000_000, 16_000_000),
        ]
        for sample, client, daemon, workload in samples:
            self.write_client_cpu_sample(
                "none", sample, client, "normal", daemon, workload)
        evidence = summary.read_client_cpu_evidence(
            self.inputs, self.rows, self.data)
        total = evidence[("none", "normal")]
        self.assertEqual(
            total["ngs3fs_total_cpu_per_operation_ns"], 1_500_000)
        self.assertEqual(
            total["reference_total_cpu_per_operation_ns"], 3_000_000)
        self.assertEqual(total["reference_over_ngs3fs"], 2.0)

        sample_path = self.data / "none-client-cpu.csv"
        with sample_path.open(newline="", encoding="utf-8") as stream:
            copied = list(csv.DictReader(stream))
        self.assertEqual(len(copied), 4)
        self.assertEqual(
            int(copied[0]["total_cpu_ns"]),
            int(copied[0]["daemon_cpu_ns"]) +
            int(copied[0]["workload_cpu_ns"]))

        markdown = self.root / "summary.md"
        page = self.root / "index.html"
        summary.write_markdown(markdown, self.rows, "test", "", evidence)
        summary.write_html(page, self.rows, "test", "", evidence)
        for report in (markdown.read_text(encoding="utf-8"),
                       page.read_text(encoding="utf-8")):
            self.assertIn("1.500", report)
            self.assertIn("3.000", report)
            self.assertIn("data/none-client-cpu.csv", report)

    def test_missing_old_client_cpu_is_unavailable_without_fabricated_file(self):
        evidence = summary.read_client_cpu_evidence(
            self.inputs, self.rows, self.data)
        self.assertNotIn(("old", "normal"), evidence)
        self.assertFalse((self.data / "old-client-cpu.csv").exists())
        markdown = self.root / "summary.md"
        summary.write_markdown(markdown, self.rows, "test", "", evidence)
        report = markdown.read_text(encoding="utf-8")
        self.assertIn(
            "| old | normal | unavailable | unavailable | "
            "client CPU unavailable |", report)


if __name__ == "__main__":
    unittest.main()
