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
        data = self.root / "data"
        data.mkdir()
        inputs = []
        for mode, engine in [("none", "uring"), ("warm", "legacy"), ("old", None)]:
            path = self.root / mode
            path.mkdir()
            inputs.append(path)
            rows = [
                dict(advice="normal", client=name, cache_mode=mode,
                     cpu_per_operation_median_ns=cpu, wall_median_ns=cpu * 100,
                     s3_get_median=12, samples=3)
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
        self.rows = summary.read_comparisons(inputs, data)
        self.modes = {row["suite"]: row for row in self.rows}

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


if __name__ == "__main__":
    unittest.main()
