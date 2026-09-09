#!/usr/bin/env python3
"""Summarize I/O-engine daemon and total client CPU medians."""
from __future__ import annotations
import argparse
import csv
from html import escape
import re
import shlex
from pathlib import Path
from statistics import median

SUITES = ("normal", "random", "cache-cold", "cache-warm", "cache-unlimited")
OWNER_COMPLETE_SUITES = ("affinity-normal", "affinity-random",
                         "affinity-cache-cold", "affinity-cache-warm")
NAME = re.compile(r"^(?P<engine>.+)-(?P<reactors>[0-9]+)-r[0-9]+$")
SOURCE_SUBMISSION = re.compile(
    r"io_uring cached reply source_submissions: "
    r"mode=(?P<mode>PREAD|SPLICE) "
    r"size=(?P<size><=4KiB|<=8KiB|<=16KiB|<=32KiB|<=64KiB|"
    r"<=128KiB|<=256KiB|<=1MiB|>1MiB) "
    r"count=(?P<count>[0-9]+) bytes=(?P<bytes>[0-9]+)")
SOURCE_SIZE_BINS = (
    "<=4KiB", "<=8KiB", "<=16KiB", "<=32KiB", "<=64KiB",
    "<=128KiB", "<=256KiB", "<=1MiB", ">1MiB")

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

def parse_variant_values(value):
    result = {}
    for field in shlex.split(value):
        if "=" not in field:
            raise ValueError(f"invalid benchmark variant field: {field}")
        label, item = field.split("=", 1)
        if not label or not item or label in result:
            raise ValueError(f"invalid benchmark variant field: {field}")
        result[label] = item
    return result

def load_variant_manifest(directory: Path):
    path = directory / "system.txt"
    if not path.is_file():
        return None
    fields = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key in ("benchmark_variant_revisions",
                                 "benchmark_variant_descriptions"):
            fields[key] = parse_variant_values(value)
    if not fields:
        return None
    revisions = fields.get("benchmark_variant_revisions", {})
    descriptions = fields.get("benchmark_variant_descriptions", {})
    if set(revisions) != {"baseline", "current"} or set(descriptions) != {
            "baseline", "current"}:
        raise ValueError(f"incomplete benchmark variant provenance in {path}")
    return revisions, descriptions

def owner_complete_provenance(directories):
    manifests = [manifest for directory in directories
                 if (manifest := load_variant_manifest(directory)) is not None]
    if not manifests:
        return ""
    if any(manifest != manifests[0] for manifest in manifests[1:]):
        raise ValueError("inconsistent benchmark variant provenance across suites")
    revisions, descriptions = manifests[0]
    return (
        f"Baseline: {descriptions['baseline']} ({revisions['baseline']}). "
        f"Current: {descriptions['current']} ({revisions['current']}). "
        "Both CI variants use four total reactors (one ingress and three "
        "workers), a mount-wide limit of eight HTTP connections, identical "
        "workloads, and alternating sample order on the same runner.")

def load_cached_reply_evidence(directory: Path):
    result = {}
    for sample in sorted(directory.iterdir()):
        match = NAME.fullmatch(sample.name)
        if not match or not (sample / "client-cpu.csv").is_file():
            continue
        key = (match["engine"], int(match["reactors"]))
        counters = {}
        log_path = sample / "ngs3fs.log"
        if log_path.is_file():
            for line in log_path.read_text(
                    encoding="utf-8", errors="replace").splitlines():
                parsed = SOURCE_SUBMISSION.fullmatch(line)
                if not parsed:
                    continue
                counter_key = (parsed["mode"], parsed["size"])
                if counter_key in counters:
                    raise ValueError(
                        f"duplicate cached reply counter {counter_key} in "
                        f"{log_path}")
                counters[counter_key] = (
                    int(parsed["count"]), int(parsed["bytes"]))
        memory = {}
        memory_path = sample / "ngs3fs-threads-after-workload.txt"
        if memory_path.is_file():
            for line in memory_path.read_text(encoding="utf-8").splitlines():
                name, separator, value = line.partition("=")
                if separator and name in ("vm_rss_kib", "vm_hwm_kib"):
                    memory[name] = int(value)
        result.setdefault(key, []).append((counters or None, memory))
    return result

def cached_reply_evidence_markdown(directories):
    out = [
        "## Cached reply source evidence",
        "",
        "Memory values and source-submission counts are medians across the "
        "same unsampled benchmark repetitions used by the CPU table above. "
        "Source size bins describe the actual fd-backed FUSE reply payload "
        "submitted to the owner reactor across the entire mount lifetime "
        "(preparation, warmup, timed workload, and teardown), not the "
        "application read size or only the 8,192 timed operations. A SPLICE "
        "attempt that falls back to PREAD contributes one logical attempt "
        "to each mode; exact short-I/O continuation does not add another "
        "attempt. Byte totals are submitted/requested payload bytes, not "
        "completed bytes. Size bins are mutually exclusive: after <=4KiB, "
        "each upper bound has an exclusive lower bound. RSS/HWM comes from "
        "the post-workload process snapshot in each sample.",
        "",
        "### Resident memory",
        "",
        "| Suite | Configuration | Samples | RSS median | HWM median |",
        "|---|---|---:|---:|---:|",
    ]
    evidence_by_suite = {}
    for suite, directory in directories.items():
        evidence = load_cached_reply_evidence(directory)
        evidence_by_suite[suite] = evidence
        for (engine, reactors), samples in sorted(evidence.items()):
            rss = [memory["vm_rss_kib"] for _counters, memory in samples
                   if "vm_rss_kib" in memory]
            hwm = [memory["vm_hwm_kib"] for _counters, memory in samples
                   if "vm_hwm_kib" in memory]
            rss_text = f"{int(median(rss)):,} KiB" if rss else "unavailable"
            hwm_text = f"{int(median(hwm)):,} KiB" if hwm else "unavailable"
            out.append(
                f"| {suite} | {engine}:{reactors} | {len(samples)} | "
                f"{rss_text} | {hwm_text} |")
    out += [
        "",
        "### Owner-ring source submissions",
        "",
        "Only non-zero bins are emitted by an instrumented binary. Missing "
        "bins within an instrumented sample are therefore counted as zero. "
        "A configuration with no counter lines is reported as unavailable, "
        "not as zero.",
        "",
        "| Suite | Configuration | Instrumented samples | Mode | Actual "
        "payload bin | Median attempts | Median requested bytes |",
        "|---|---|---:|---|---|---:|---:|",
    ]
    bin_order = {name: index for index, name in enumerate(SOURCE_SIZE_BINS)}
    for suite, evidence in evidence_by_suite.items():
        for (engine, reactors), samples in sorted(evidence.items()):
            instrumented = [counters for counters, _memory in samples
                            if counters is not None]
            if not instrumented:
                out.append(
                    f"| {suite} | {engine}:{reactors} | 0/{len(samples)} | "
                    "unavailable | unavailable | unavailable | unavailable |")
                continue
            keys = set().union(*(counters.keys()
                                 for counters in instrumented))
            for mode, size in sorted(
                    keys, key=lambda item: (
                        0 if item[0] == "PREAD" else 1,
                        bin_order[item[1]])):
                counts = [counters.get((mode, size), (0, 0))[0]
                          for counters in instrumented]
                byte_counts = [counters.get((mode, size), (0, 0))[1]
                               for counters in instrumented]
                out.append(
                    f"| {suite} | {engine}:{reactors} | "
                    f"{len(instrumented)}/{len(samples)} | {mode} | {size} | "
                    f"{int(median(counts)):,} | "
                    f"{int(median(byte_counts)):,} |")
    out.append("")
    return "\n".join(out)

def cached_reply_evidence_html(directories):
    evidence_by_suite = {
        suite: load_cached_reply_evidence(directory)
        for suite, directory in directories.items()}
    out = [
        "<h2>Cached reply source evidence</h2>",
        "<p>Memory values and source-submission counts are medians across "
        "the same unsampled benchmark repetitions used by the CPU table. "
        "Source size bins are actual fd-backed FUSE reply payloads submitted "
        "to the owner reactor across the entire mount lifetime (preparation, "
        "warmup, timed workload, and teardown), not application read sizes "
        "or only the 8,192 timed operations. SPLICE-to-PREAD fallback counts "
        "one logical attempt in each mode; exact short-I/O continuation does "
        "not add an attempt. Byte totals are submitted/requested payload "
        "bytes, not completed bytes. Bins are mutually exclusive: after "
        "&lt;=4KiB, every upper bound has an exclusive lower bound. RSS/HWM "
        "is the post-workload process snapshot.</p>",
        "<h3>Resident memory</h3>",
        "<table><thead><tr><th>Suite</th><th>Configuration</th>"
        "<th>Samples</th><th>RSS median</th><th>HWM median</th>"
        "</tr></thead><tbody>",
    ]
    for suite, evidence in evidence_by_suite.items():
        for (engine, reactors), samples in sorted(evidence.items()):
            rss = [memory["vm_rss_kib"] for _counters, memory in samples
                   if "vm_rss_kib" in memory]
            hwm = [memory["vm_hwm_kib"] for _counters, memory in samples
                   if "vm_hwm_kib" in memory]
            rss_text = f"{int(median(rss)):,} KiB" if rss else "unavailable"
            hwm_text = f"{int(median(hwm)):,} KiB" if hwm else "unavailable"
            out.append(
                f"<tr><td>{escape(suite)}</td>"
                f"<td>{escape(engine)}:{reactors}</td><td>{len(samples)}</td>"
                f"<td>{rss_text}</td><td>{hwm_text}</td></tr>")
    out += [
        "</tbody></table>",
        "<h3>Owner-ring source submissions</h3>",
        "<p>Only non-zero bins are emitted. Missing bins within an "
        "instrumented sample count as zero; no counter lines means "
        "unavailable, not zero.</p>",
        "<table><thead><tr><th>Suite</th><th>Configuration</th>"
        "<th>Instrumented samples</th><th>Mode</th>"
        "<th>Actual payload bin</th><th>Median attempts</th>"
        "<th>Median requested bytes</th></tr></thead><tbody>",
    ]
    bin_order = {name: index for index, name in enumerate(SOURCE_SIZE_BINS)}
    for suite, evidence in evidence_by_suite.items():
        for (engine, reactors), samples in sorted(evidence.items()):
            instrumented = [counters for counters, _memory in samples
                            if counters is not None]
            if not instrumented:
                out.append(
                    f"<tr><td>{escape(suite)}</td>"
                    f"<td>{escape(engine)}:{reactors}</td>"
                    f"<td>0/{len(samples)}</td>"
                    "<td>unavailable</td><td>unavailable</td>"
                    "<td>unavailable</td><td>unavailable</td></tr>")
                continue
            keys = set().union(*(counters.keys()
                                 for counters in instrumented))
            for mode, size in sorted(
                    keys, key=lambda item: (
                        0 if item[0] == "PREAD" else 1,
                        bin_order[item[1]])):
                counts = [counters.get((mode, size), (0, 0))[0]
                          for counters in instrumented]
                byte_counts = [counters.get((mode, size), (0, 0))[1]
                               for counters in instrumented]
                out.append(
                    f"<tr><td>{escape(suite)}</td>"
                    f"<td>{escape(engine)}:{reactors}</td>"
                    f"<td>{len(instrumented)}/{len(samples)}</td>"
                    f"<td>{mode}</td><td>{escape(size)}</td>"
                    f"<td>{int(median(counts)):,}</td>"
                    f"<td>{int(median(byte_counts)):,}</td></tr>")
    out.append("</tbody></table>")
    return "".join(out)

def report(data_by_suite, title="I/O-engine CPU comparison", provenance=""):
    out = [f"# {title}", ""]
    if provenance:
        out += [provenance, ""]
    out += ["Medians over per-sample `client-cpu.csv` files. Daemon CPU/op is from `summary.csv`; total CPU/op is daemon plus workload from `client-cpu.csv`. Daemon CPU includes kernel SQPOLL threads. CPU excludes the S3 server and unattributed global kernel work; it is not wall time or perf-instrumented CPU. Negative percentages mean savings.", ""]
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
            out += ["", "Matched four-reactor delta (current versus baseline):", "",
                    f"- daemon {pct(current[1], baseline[1])}; total {pct(current[2], baseline[2])}"]
        out.append("")
    return "\n".join(out)

def html(data_by_suite, missing=(), title="I/O-engine CPU comparison",
         provenance=""):
    out = [
        "<!doctype html><meta charset=\"utf-8\">",
        f"<title>{escape(title)}</title>",
        "<style>body{font:14px sans-serif;max-width:1100px;margin:2em auto}table{border-collapse:collapse;margin:1em 0 2em}th,td{border:1px solid #bbb;padding:.35em .6em;text-align:right}th:first-child,td:first-child{text-align:left}</style>",
        f"<h1>{escape(title)}</h1>",
    ]
    if provenance:
        out.append(f"<p>{escape(provenance)}</p>")
    out.append("<p>Medians over per-sample <code>client-cpu.csv</code>; daemon CPU/op is from <code>summary.csv</code> and includes kernel SQPOLL threads. Total CPU/op is daemon plus workload, excluding the S3 server and unattributed global kernel work. Negative percentages mean savings; this is not wall time or perf-instrumented CPU.</p>")
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
            out.append("<p>Matched four-reactor delta (current versus baseline): "
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
    suite_directories = {}
    missing = []
    for directory in directories:
        suite = directory.name.removeprefix("github-io-engine-")
        if not (directory / "summary.csv").is_file():
            missing.append(suite)
            continue
        data[suite] = load_suite_directory(directory)
        suite_directories[suite] = directory
    if not data:
        raise SystemExit("no complete I/O-engine suites found")
    destination_dir = args.output_dir
    destination_dir.mkdir(parents=True, exist_ok=True)
    text = report(data)
    if missing:
        text += "\nMissing suites: " + ", ".join(missing) + "\n"
    (destination_dir / "sqpoll-comparison.md").write_text(text, encoding="utf-8")
    (destination_dir / "sqpoll-comparison.html").write_text(html(data, missing), encoding="utf-8")
    owner_complete = {
        suite: data[suite] for suite in OWNER_COMPLETE_SUITES if suite in data}
    if owner_complete:
        owner_complete_missing = [
            suite for suite in OWNER_COMPLETE_SUITES
            if suite not in owner_complete]
        owner_complete_title = "Matched four-reactor CPU comparison"
        provenance = owner_complete_provenance([
            suite_directories[suite] for suite in owner_complete])
        owner_complete_text = report(
            owner_complete, owner_complete_title, provenance)
        if owner_complete_missing:
            owner_complete_text += (
                "\nMissing suites: " + ", ".join(owner_complete_missing) + "\n")
        owner_complete_html = html(
            owner_complete, owner_complete_missing, owner_complete_title,
            provenance)
        cached_directories = {
            suite: suite_directories[suite]
            for suite in ("affinity-cache-cold", "affinity-cache-warm")
            if suite in suite_directories}
        if cached_directories:
            owner_complete_text += (
                "\n" + cached_reply_evidence_markdown(cached_directories))
            owner_complete_html += cached_reply_evidence_html(
                cached_directories)
        for stem in ("owner-complete-comparison", "inode-affinity-comparison"):
            (destination_dir / f"{stem}.md").write_text(
                owner_complete_text, encoding="utf-8")
            (destination_dir / f"{stem}.html").write_text(
                owner_complete_html, encoding="utf-8")

if __name__ == "__main__":
    main()
