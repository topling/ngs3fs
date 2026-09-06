#!/usr/bin/env python3

import argparse
import csv
import datetime
import html
import os
import pathlib
import shutil


def number(row, name):
    return int(row[name])


def ratio(numerator, denominator):
    return numerator / denominator if denominator else float("inf")


def percent(value):
    return f"{value:.1f}%"


def milliseconds(nanoseconds):
    return f"{nanoseconds / 1_000_000:.3f}"


def integer_median(values):
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) // 2


def io_label(row):
    engine = row["ngs3fs_io_engine"]
    if engine == "legacy":
        return "legacy (threaded)"
    if engine == "uring":
        count = row["ngs3fs_reactors"]
        return f"uring ({count} reactor{'s' if count != '1' else ''})"
    return f"{engine} (requested reactors: {row['ngs3fs_reactors']})"


def read_comparisons(input_dirs, data_dir):
    comparisons = []
    for input_dir in sorted(set(input_dirs)):
        summary = input_dir / "summary.csv"
        if not summary.is_file():
            continue
        with summary.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
        system = input_dir / "system.txt"
        settings = {}
        if system.is_file():
            for line in system.read_text(encoding="utf-8").splitlines():
                name, separator, value = line.partition("=")
                if separator and name in ("ngs3fs_io_engine", "ngs3fs_reactors"):
                    settings[name] = value
        by_advice = {}
        for row in rows:
            by_advice.setdefault(row["advice"], {})[row["client"]] = row
        for advice, clients in by_advice.items():
            ngs3fs = clients.get("ngs3fs")
            references = [row for name, row in clients.items()
                          if name != "ngs3fs"]
            if ngs3fs is None or len(references) != 1:
                continue
            reference = references[0]
            ng_cpu = number(ngs3fs, "cpu_per_operation_median_ns")
            ref_cpu = number(reference, "cpu_per_operation_median_ns")
            ng_wall = number(ngs3fs, "wall_median_ns")
            ref_wall = number(reference, "wall_median_ns")
            ng_get = number(ngs3fs, "s3_get_median")
            ref_get = number(reference, "s3_get_median")
            comparisons.append({
                "suite": input_dir.name,
                "advice": advice,
                "ngs3fs_cache": ngs3fs["cache_mode"],
                "ngs3fs_io_engine": settings.get("ngs3fs_io_engine", "not recorded"),
                "ngs3fs_reactors": settings.get("ngs3fs_reactors", "not recorded"),
                "reference": reference["client"],
                "reference_cache": reference["cache_mode"],
                "samples": number(ngs3fs, "samples"),
                "ngs3fs_cpu_per_operation_ns": ng_cpu,
                "reference_cpu_per_operation_ns": ref_cpu,
                "reference_cpu_over_ngs3fs": ratio(ref_cpu, ng_cpu),
                "ngs3fs_cpu_saving_percent": 100 * (1 - ratio(ng_cpu, ref_cpu)),
                "ngs3fs_wall_median_ns": ng_wall,
                "reference_wall_median_ns": ref_wall,
                "reference_wall_over_ngs3fs": ratio(ref_wall, ng_wall),
                "ngs3fs_s3_get_median": ng_get,
                "reference_s3_get_median": ref_get,
                "ngs3fs_get_over_reference": ratio(ng_get, ref_get),
            })
        shutil.copyfile(summary, data_dir / f"{input_dir.name}-summary.csv")
        if system.is_file():
            shutil.copyfile(system, data_dir / f"{input_dir.name}-system.txt")
    comparisons.sort(key=lambda row: (
        row["reference"], row["ngs3fs_cache"], row["advice"]))
    return comparisons


def read_client_cpu_evidence(input_dirs, comparisons, data_dir):
    expected_by_suite = {}
    for row in comparisons:
        expected = expected_by_suite.setdefault(row["suite"], {})
        expected[(row["advice"], "ngs3fs")] = row["samples"]
        expected[(row["advice"], row["reference"])] = row["samples"]

    evidence = {}
    inputs_by_name = {path.name: path for path in input_dirs}
    fieldnames = [
        "sample", "advice", "client", "operations", "daemon_cpu_ns",
        "workload_cpu_ns", "total_cpu_ns", "total_cpu_ns_per_operation",
    ]
    for suite, expected in expected_by_suite.items():
        input_dir = inputs_by_name.get(suite)
        if input_dir is None:
            continue
        rows = []
        for cpu_path in sorted(input_dir.glob("*/client-cpu.csv")):
            result_path = cpu_path.parent / "random-read-summary.csv"
            if not result_path.is_file():
                raise RuntimeError(
                    f"client CPU sample has no random-read summary: {cpu_path}")
            with result_path.open(newline="", encoding="utf-8") as source:
                results = {row["client"]: row for row in csv.DictReader(source)}
            with cpu_path.open(newline="", encoding="utf-8") as source:
                for cpu in csv.DictReader(source):
                    result = results.get(cpu["client"])
                    if result is None:
                        raise RuntimeError(
                            f"client CPU sample has no matching result: {cpu_path}")
                    key = (result["advice"], cpu["client"])
                    if key not in expected:
                        continue
                    operations = (number(result, "pread_operations") +
                                  number(result, "mmap_operations"))
                    if operations == 0:
                        raise RuntimeError(
                            f"client CPU sample has no operations: {cpu_path}")
                    daemon_cpu = number(cpu, "daemon_cpu_ns")
                    workload_cpu = number(cpu, "workload_cpu_ns")
                    total_cpu = daemon_cpu + workload_cpu
                    total_per_operation = total_cpu // operations
                    if (number(cpu, "total_cpu_ns") != total_cpu or
                            number(cpu, "total_cpu_ns_per_operation") !=
                            total_per_operation):
                        raise RuntimeError(
                            f"inconsistent client CPU totals: {cpu_path}")
                    rows.append({
                        "sample": cpu_path.parent.name,
                        "advice": result["advice"],
                        "client": cpu["client"],
                        "operations": operations,
                        "daemon_cpu_ns": daemon_cpu,
                        "workload_cpu_ns": workload_cpu,
                        "total_cpu_ns": total_cpu,
                        "total_cpu_ns_per_operation": total_per_operation,
                    })

        grouped = {}
        for row in rows:
            grouped.setdefault((row["advice"], row["client"]), []).append(row)
        complete = rows and all(
            len(grouped.get(key, [])) == count
            for key, count in expected.items())
        output = data_dir / f"{suite}-client-cpu.csv"
        if not complete:
            output.unlink(missing_ok=True)
            continue
        rows.sort(key=lambda row: (row["advice"], row["client"], row["sample"]))
        with output.open("w", newline="", encoding="utf-8") as destination:
            writer = csv.DictWriter(destination, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        for comparison in (row for row in comparisons if row["suite"] == suite):
            ngs3fs = grouped[(comparison["advice"], "ngs3fs")]
            reference = grouped[(comparison["advice"], comparison["reference"])]
            ngs3fs_total = integer_median(
                [row["total_cpu_ns_per_operation"] for row in ngs3fs])
            reference_total = integer_median(
                [row["total_cpu_ns_per_operation"] for row in reference])
            evidence[(suite, comparison["advice"])] = {
                "ngs3fs_total_cpu_per_operation_ns": ngs3fs_total,
                "reference_total_cpu_per_operation_ns": reference_total,
                "reference_over_ngs3fs": ratio(reference_total, ngs3fs_total),
                "source": f"data/{output.name}",
            }
    return evidence


def write_csv(path, comparisons):
    if not comparisons:
        raise RuntimeError("no complete benchmark comparisons found")
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=comparisons[0].keys())
        writer.writeheader()
        writer.writerows(comparisons)


def run_url():
    server = os.environ.get("GITHUB_SERVER_URL")
    repository = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    if server and repository and run_id:
        return f"{server}/{repository}/actions/runs/{run_id}"
    return ""


def write_markdown(path, comparisons, generated, source_run,
                   client_cpu_evidence=None):
    client_cpu_evidence = client_cpu_evidence or {}
    lines = [
        "# ngs3fs random-read CPU comparison",
        "",
        f"Generated at {generated} from the median of runner samples.",
        "CPU is aggregate daemon CPU time divided by completed read operations.",
        "The S3 endpoint is loopback VersityGW, so network first-byte latency is negligible.",
        "",
        "The ngs3fs I/O column comes from each suite's recorded settings; legacy uses its threaded loop, independently of reactor count.",
        "",
        "| Advice | Cache (ngs3fs / reference) | ngs3fs I/O | Reference | CPU/op (ngs3fs / reference) | Reference / ngs3fs | CPU saved | S3 GET (ngs3fs / reference) |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for row in comparisons:
        lines.append(
            f"| {row['advice']} | {row['ngs3fs_cache']} / "
            f"{row['reference_cache']} | {io_label(row)} | {row['reference']} | "
            f"{milliseconds(row['ngs3fs_cpu_per_operation_ns'])} / "
            f"{milliseconds(row['reference_cpu_per_operation_ns'])} ms | "
            f"{row['reference_cpu_over_ngs3fs']:.2f}x | "
            f"{percent(row['ngs3fs_cpu_saving_percent'])} | "
            f"{row['ngs3fs_s3_get_median']} / "
            f"{row['reference_s3_get_median']} |"
        )
    lines.extend([
        "",
        "## Total client CPU evidence",
        "",
        "The primary table above remains daemon CPU only. Total client CPU "
        "adds workload-process CPU, including system-call time charged to the "
        "workload, to daemon CPU. Independent S3-server CPU and unattributed "
        "global kernel work are excluded.",
        "",
        "| Suite | Advice | Total CPU/op (ngs3fs / reference) | Reference / ngs3fs | Evidence |",
        "|---|---|---:|---:|---|",
    ])
    for row in comparisons:
        total = client_cpu_evidence.get((row["suite"], row["advice"]))
        if total is None:
            values = "unavailable | unavailable | client CPU unavailable"
        else:
            values = (
                f"{milliseconds(total['ngs3fs_total_cpu_per_operation_ns'])} / "
                f"{milliseconds(total['reference_total_cpu_per_operation_ns'])} ms | "
                f"{total['reference_over_ngs3fs']:.2f}x | "
                f"[per-sample CSV]({total['source']})")
        lines.append(
            f"| {row['suite']} | {row['advice']} | {values} |")
    if source_run:
        lines.extend(["", f"Source workflow: {source_run}"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_html(path, comparisons, generated, source_run,
               client_cpu_evidence=None):
    client_cpu_evidence = client_cpu_evidence or {}
    rows = []
    for row in comparisons:
        saving = row["ngs3fs_cpu_saving_percent"]
        saving_class = "better" if saving >= 0 else "worse"
        source = f"data/{row['suite']}-summary.csv"
        rows.append(f"""
          <tr>
            <td>{html.escape(row['advice'])}</td>
            <td><span class="mode">{html.escape(row['ngs3fs_cache'])}</span> / <span class="mode">{html.escape(row['reference_cache'])}</span></td>
            <td>{html.escape(io_label(row))}</td>
            <td>{html.escape(row['reference'])}</td>
            <td class="number">{milliseconds(row['ngs3fs_cpu_per_operation_ns'])}</td>
            <td class="number">{milliseconds(row['reference_cpu_per_operation_ns'])}</td>
            <td class="number"><strong>{row['reference_cpu_over_ngs3fs']:.2f}×</strong></td>
            <td class="number {saving_class}">{percent(saving)}</td>
            <td class="number">{row['ngs3fs_s3_get_median']:,} / {row['reference_s3_get_median']:,}</td>
            <td><a href="{html.escape(source)}">CSV</a></td>
          </tr>""")
    total_rows = []
    for row in comparisons:
        total = client_cpu_evidence.get((row["suite"], row["advice"]))
        if total is None:
            total_values = """
            <td class="number">unavailable</td>
            <td class="number">unavailable</td>
            <td>client CPU unavailable</td>"""
        else:
            source = html.escape(total["source"])
            total_values = f"""
            <td class="number">{milliseconds(total['ngs3fs_total_cpu_per_operation_ns'])} / {milliseconds(total['reference_total_cpu_per_operation_ns'])}</td>
            <td class="number">{total['reference_over_ngs3fs']:.2f}×</td>
            <td><a href="{source}">per-sample CSV</a></td>"""
        total_rows.append(f"""
          <tr>
            <td>{html.escape(row['suite'])}</td>
            <td>{html.escape(row['advice'])}</td>{total_values}
          </tr>""")
    workflow = ""
    if source_run:
        safe_run = html.escape(source_run)
        workflow = f' · <a href="{safe_run}">source workflow</a>'
    document = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ngs3fs CPU benchmark</title>
  <style>
    :root {{ color-scheme: light dark; font-family: ui-sans-serif, system-ui, sans-serif; }}
    body {{ margin: 0 auto; max-width: 1180px; padding: 2rem 1rem 4rem; }}
    h1 {{ margin-bottom: .35rem; }}
    .subtitle {{ color: #777; margin: 0 0 1.5rem; }}
    .card {{ border: 1px solid #8885; border-radius: 12px; overflow-x: auto; }}
    table {{ border-collapse: collapse; width: 100%; min-width: 1020px; }}
    th, td {{ border-bottom: 1px solid #8884; padding: .7rem .65rem; text-align: left; }}
    th {{ background: #8881; font-size: .82rem; }}
    tr:last-child td {{ border-bottom: 0; }}
    .number {{ font-variant-numeric: tabular-nums; text-align: right; }}
    .better {{ color: #159447; font-weight: 700; }}
    .worse {{ color: #c43d3d; font-weight: 700; }}
    .mode {{ font-family: ui-monospace, monospace; }}
    .notes {{ line-height: 1.55; margin-top: 1.5rem; }}
    code {{ background: #8882; border-radius: 4px; padding: .1rem .3rem; }}
  </style>
</head>
<body>
  <h1>ngs3fs random-read CPU comparison</h1>
  <p class="subtitle">Generated {html.escape(generated)} from GitHub-hosted runner measurements{workflow}.</p>
  <div class="card">
    <table>
      <thead><tr>
        <th>Advice</th><th>Cache<br>ngs3fs / reference</th>
        <th>ngs3fs I/O</th>
        <th>Reference</th>
        <th class="number">ngs3fs<br>CPU/op (ms)</th>
        <th class="number">Reference<br>CPU/op (ms)</th>
        <th class="number">Reference CPU ÷<br>ngs3fs CPU</th>
        <th class="number">ngs3fs CPU saved</th>
        <th class="number">Median S3 GET<br>ngs3fs / reference</th><th>Evidence</th>
      </tr></thead>
      <tbody>{''.join(rows)}
      </tbody>
    </table>
  </div>
  <h2>Total client CPU evidence</h2>
  <p>Total client CPU adds workload-process CPU, including system-call time
  charged to the workload, to daemon CPU. Independent S3-server CPU and
  unattributed global kernel work are excluded.</p>
  <div class="card">
    <table>
      <thead><tr>
        <th>Suite</th><th>Advice</th>
        <th class="number">Total CPU/op<br>ngs3fs / reference (ms)</th>
        <th class="number">Reference CPU ÷<br>ngs3fs CPU</th><th>Evidence</th>
      </tr></thead>
      <tbody>{''.join(total_rows)}
      </tbody>
    </table>
  </div>
  <div class="notes">
    <p><strong>CPU/op</strong> is aggregate daemon CPU time divided by completed
    <code>pread</code> and mmap-fault operations. Values are medians, not wall time.</p>
    <p>The S3 endpoint is loopback VersityGW, so network first-byte latency is
    negligible. Results must not be extrapolated to remote S3 latency.</p>
    <p><strong>cold</strong> and <strong>warm</strong> mean disk data caching is enabled
    independently for both ngs3fs and Mountpoint. The cache column makes asymmetric
    comparisons explicit. Each Evidence link opens its raw summary CSV.</p>
    <p>The ngs3fs I/O column comes from each suite's recorded engine and reactor
    settings. Legacy uses its threaded loop, independently of reactor count;
    missing historical settings are not inferred from current defaults.</p>
    <p><a href="random-read-cpu-comparison.csv">Combined CSV</a> ·
    <a href="random-read-cpu-comparison.md">Markdown report</a></p>
  </div>
</body>
</html>
"""
    path.write_text(document, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", type=pathlib.Path)
    parser.add_argument("input_dirs", nargs="+", type=pathlib.Path)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = args.output_dir / "data"
    data_dir.mkdir(exist_ok=True)
    generated = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC")
    comparisons = read_comparisons(args.input_dirs, data_dir)
    client_cpu = read_client_cpu_evidence(
        args.input_dirs, comparisons, data_dir)
    source_run = run_url()
    write_csv(args.output_dir / "random-read-cpu-comparison.csv", comparisons)
    write_markdown(args.output_dir / "random-read-cpu-comparison.md",
                   comparisons, generated, source_run, client_cpu)
    write_html(args.output_dir / "index.html", comparisons, generated,
               source_run, client_cpu)


if __name__ == "__main__":
    main()
