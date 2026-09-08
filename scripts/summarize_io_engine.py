#!/usr/bin/env python3
"""Summarize I/O-engine daemon and total client CPU medians."""
from __future__ import annotations
import argparse
import csv
from html import escape
import re
from pathlib import Path
from statistics import median

SUITES = ("normal", "random", "cache-cold", "cache-warm", "cache-unlimited")
AFFINITY_SUITES = ("affinity-normal", "affinity-random",
                   "affinity-cache-cold", "affinity-cache-warm")
NAME = re.compile(r"^(?P<engine>.+)-(?P<reactors>[0-9]+)-r[0-9]+$")

def rows(path: Path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))

def load_suite_directory(directory: Path):
    suite = directory.name.removeprefix("github-io-engine-")
    summary = rows(directory / "summary.csv")
    values = {}
    for sample in sorted(directory.iterdir()):
        match = NAME.fullmatch(sample.name)
        path = sample / "client-cpu.csv"
        if not match or not path.is_file():
            continue
        sample_rows = rows(path)
        if len(sample_rows) != 1 or sample_rows[0].get("client") != "ngs3fs":
            raise ValueError(f"expected one ngs3fs row in {path}")
        key = (match["engine"], int(match["reactors"]))
        values.setdefault(key, []).append(int(sample_rows[0]["total_cpu_ns_per_operation"]))
    result = {}
    summary_keys = set()
    if not summary:
        raise ValueError(f"empty summary.csv in {directory}")
    for row in summary:
        key = (row["engine"], int(row["reactors"]))
        if key in summary_keys:
            raise ValueError(f"duplicate summary configuration for {suite} {key}")
        summary_keys.add(key)
    for key, totals in values.items():
        match = [row for row in summary if row["engine"] == key[0] and int(row["reactors"]) == key[1]
        ]
        if len(match) != 1 or int(match[0]["samples"]) != len(totals):
            raise ValueError(f"summary/sample mismatch for {suite} {key}")
        result[key] = (len(totals), int(match[0]["cpu_per_operation_median_ns"]), int(median(totals)))
    if set(values) != summary_keys:
        missing = sorted(summary_keys - set(values))
        extra = sorted(set(values) - summary_keys)
        raise ValueError(f"summary configurations without matching samples for {suite}: missing={missing} extra={extra}")
    if not result:
        raise ValueError(f"no valid client-cpu.csv samples in {directory}")
    return result

def pct(value, baseline):
    if baseline == 0:
        return "n/a (zero baseline)"
    return f"{(value / baseline - 1) * 100:+.2f}%"

def report(data_by_suite, title="I/O-engine CPU comparison"):
    out = [f"# {title}", "", "Medians over per-sample `client-cpu.csv` files. Daemon CPU/op is from `summary.csv`; total CPU/op is daemon plus workload from `client-cpu.csv`. Daemon CPU includes kernel SQPOLL threads. CPU excludes the S3 server and unattributed global kernel work; it is not wall time or perf-instrumented CPU. Negative percentages mean savings.", ""]
    for suite, data in data_by_suite.items():
        out += [f"## {suite}", "", "| Configuration | Samples | Daemon CPU/op | Total CPU/op |", "|---|---:|---:|---:|"]
        for (engine, reactors), (count, daemon, total) in sorted(data.items()):
            out.append(f"| {engine}:{reactors} | {count} | {daemon:,} ns | {total:,} ns |")
        sqpoll = data.get(("uring-sqpoll", 1))
        if sqpoll:
            out += ["", "SQPOLL deltas:", ""]
            for label, key in (("uring:1", ("uring", 1)), ("legacy:1", ("legacy", 1)), ("uring:4", ("uring", 4))):
                if key in data:
                    out.append(f"- versus {label}: daemon {pct(sqpoll[1], data[key][1])}; total {pct(sqpoll[2], data[key][2])}")
        baseline = data.get(("baseline", 4))
        current = data.get(("current", 4))
        if baseline and current:
            out += ["", "Inode-affinity delta (current versus baseline):", "",
                    f"- daemon {pct(current[1], baseline[1])}; total {pct(current[2], baseline[2])}"]
        out.append("")
    return "\n".join(out)

def html(data_by_suite, missing=(), title="I/O-engine CPU comparison"):
    out = [
        "<!doctype html><meta charset=\"utf-8\">",
        f"<title>{escape(title)}</title>",
        "<style>body{font:14px sans-serif;max-width:1100px;margin:2em auto}table{border-collapse:collapse;margin:1em 0 2em}th,td{border:1px solid #bbb;padding:.35em .6em;text-align:right}th:first-child,td:first-child{text-align:left}</style>",
        f"<h1>{escape(title)}</h1>",
        "<p>Medians over per-sample <code>client-cpu.csv</code>; daemon CPU/op is from <code>summary.csv</code> and includes kernel SQPOLL threads. Total CPU/op is daemon plus workload, excluding the S3 server and unattributed global kernel work. Negative percentages mean savings; this is not wall time or perf-instrumented CPU.</p>",
    ]
    for suite, data in data_by_suite.items():
        out.append(f"<h2>{escape(suite)}</h2><table><thead><tr><th>Configuration</th><th>Samples</th><th>Daemon CPU/op</th><th>Total CPU/op</th></tr></thead><tbody>")
        for (engine, reactors), (count, daemon, total) in sorted(data.items()):
            out.append(f"<tr><td>{escape(engine)}:{reactors}</td><td>{count}</td><td>{daemon:,} ns</td><td>{total:,} ns</td></tr>")
        out.append("</tbody></table>")
        sqpoll = data.get(("uring-sqpoll", 1))
        if sqpoll:
            out.append("<p>SQPOLL deltas:</p><ul>")
            for label, key in (("uring:1", ("uring", 1)), ("legacy:1", ("legacy", 1)), ("uring:4", ("uring", 4))):
                if key in data:
                    out.append(f"<li>versus {escape(label)}: daemon {pct(sqpoll[1], data[key][1])}; total {pct(sqpoll[2], data[key][2])}</li>")
            out.append("</ul>")
        baseline = data.get(("baseline", 4))
        current = data.get(("current", 4))
        if baseline and current:
            out.append("<p>Inode-affinity delta (current versus baseline): "
                       f"daemon {pct(current[1], baseline[1])}; "
                       f"total {pct(current[2], baseline[2])}</p>")
    if missing:
        out.append("<p>Missing suites: " + escape(", ".join(missing)) + "</p>")
    return "".join(out)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("suite_dirs", type=Path, nargs="*")
    args = parser.parse_args()
    if args.suite_dirs:
        directories = args.suite_dirs
    else:
        directories = [args.output_dir / f"github-io-engine-{suite}" for suite in SUITES]
    data = {}
    missing = []
    for directory in directories:
        suite = directory.name.removeprefix("github-io-engine-")
        if not (directory / "summary.csv").is_file():
            missing.append(suite)
            continue
        data[suite] = load_suite_directory(directory)
    if not data:
        raise SystemExit("no complete I/O-engine suites found")
    destination_dir = args.output_dir
    destination_dir.mkdir(parents=True, exist_ok=True)
    text = report(data)
    if missing:
        text += "\nMissing suites: " + ", ".join(missing) + "\n"
    (destination_dir / "sqpoll-comparison.md").write_text(text, encoding="utf-8")
    (destination_dir / "sqpoll-comparison.html").write_text(html(data, missing), encoding="utf-8")
    affinity = {suite: data[suite] for suite in AFFINITY_SUITES if suite in data}
    if affinity:
        affinity_missing = [suite for suite in AFFINITY_SUITES if suite not in affinity]
        affinity_title = "Inode-affinity reactor CPU comparison"
        affinity_text = report(affinity, affinity_title)
        if affinity_missing:
            affinity_text += "\nMissing suites: " + ", ".join(affinity_missing) + "\n"
        (destination_dir / "inode-affinity-comparison.md").write_text(
            affinity_text, encoding="utf-8")
        (destination_dir / "inode-affinity-comparison.html").write_text(
            html(affinity, affinity_missing, affinity_title), encoding="utf-8")

if __name__ == "__main__":
    main()
