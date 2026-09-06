#!/usr/bin/env python3
"""Size and validate non-pressure uncached random-read benchmark samples."""

import argparse
import json
from pathlib import Path
import sys


BLOCK = 2 * 1024 * 1024


def rounded(size):
    return (size + BLOCK - 1) // BLOCK * BLOCK


def memory_snapshot():
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, value, *_ = line.split()
        if name in ("MemTotal:", "MemAvailable:"):
            values[name[:-1]] = int(value) * 1024
    capacity = values["MemTotal"]
    available = values["MemAvailable"]
    # Container limits may be lower than the host's /proc/meminfo totals.
    for line in Path("/proc/self/cgroup").read_text().splitlines():
        if not line.startswith("0::"):
            continue
        relative = Path(line[3:].lstrip("/"))
        base = Path("/sys/fs/cgroup")
        current = base / relative
        if not current.exists():
            current = base
        while current == base or base in current.parents:
            maximum = current / "memory.max"
            usage = current / "memory.current"
            if maximum.is_file() and usage.is_file():
                limit = maximum.read_text().strip()
                if limit != "max":
                    limit = int(limit)
                    capacity = min(capacity, limit)
                    available = min(available, max(0, limit - int(usage.read_text())))
            if current == base:
                break
            current = current.parent
    return {
        "physical_bytes": values["MemTotal"],
        "mem_available_bytes": values["MemAvailable"],
        "effective_capacity_bytes": capacity,
        "effective_available_bytes": available,
    }


def make_plan(files, file_size, threads, maximum_read, connections, memory):
    if min(files, file_size, threads, maximum_read, connections) <= 0:
        raise ValueError("file count/size, threads, maximum read and connections must be positive")
    per_file = rounded(file_size)
    working_set = files * per_file
    # An unaligned request may straddle an additional block. Reserve one such
    # span per concurrent reader/connection beyond the entire retained data set.
    request_span = max(2 * BLOCK, rounded(min(maximum_read, file_size) + BLOCK - 1))
    inflight = max(threads, connections) * request_span
    mount_budget = working_set + inflight
    file_budget = per_file + inflight
    # Keep room for both the local server and FUSE page caches, plus daemons.
    # This is deliberately a baseline admission check, not a mount default.
    headroom = 256 * 1024 * 1024
    required = mount_budget + 2 * working_set + headroom
    errors = []
    if required > memory["effective_capacity_bytes"]:
        errors.append("working set, receive buffers and headroom exceed physical/cgroup capacity")
    if required > memory["effective_available_bytes"]:
        errors.append("insufficient currently available memory for a non-pressure baseline")
    return {
        "policy": "full-working-set-plus-concurrent-inflight",
        "block_bytes": BLOCK,
        "files": files,
        "file_size_bytes": file_size,
        "threads": threads,
        "connections": connections,
        "maximum_read_bytes": maximum_read,
        "rounded_file_bytes": per_file,
        "working_set_bytes": working_set,
        "request_span_bytes": request_span,
        "inflight_allowance_bytes": inflight,
        "max_prefetch_memory_bytes": mount_budget,
        "max_file_prefetch_memory_bytes": file_budget,
        "host_headroom_bytes": headroom,
        "required_available_bytes": required,
        "memory": memory,
        "admitted": not errors,
        "errors": errors,
    }


STAT_FIELDS = (
    "prefetch_budget_exhaustions",
    "prefetch_evicted_bytes",
    "prefetch_peak_bytes",
    "prefetch_file_peak_bytes",
    "receive_pool_mapped_bytes",
    "receive_pool_idle_bytes",
    "receive_pool_capacity_bytes",
)


def validate_sample(plan, log):
    errors = list(plan.get("errors", []))
    if not plan.get("admitted") and not errors:
        errors.append("memory plan was not admitted")
    samples = []
    warnings = []
    for line in log.splitlines():
        lower = line.lower()
        if "budget exhausted" in lower or "cannot allocate memory" in lower:
            warnings.append(line)
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if value.get("event") in ("stats", "shutdown_stats"):
            samples.append(value)
    final = next((row for row in reversed(samples) if row["event"] == "shutdown_stats"), None)
    if final is None:
        errors.append("missing graceful shutdown_stats; sample cannot prove absence of pressure")
    else:
        for name in STAT_FIELDS:
            if type(final.get(name)) is not int or final[name] < 0:
                errors.append(f"missing or invalid shutdown counter: {name}")
        for name in ("prefetch_budget_exhaustions", "prefetch_evicted_bytes"):
            if final.get(name, 0) != 0:
                errors.append(f"memory-pressure sample: {name}={final[name]}")
        for name, cap in (
            ("prefetch_peak_bytes", "max_prefetch_memory_bytes"),
            ("prefetch_file_peak_bytes", "max_file_prefetch_memory_bytes"),
            ("receive_pool_mapped_bytes", "max_prefetch_memory_bytes"),
            ("receive_pool_capacity_bytes", "max_prefetch_memory_bytes"),
        ):
            if type(final.get(name)) is int and final[name] > plan[cap]:
                errors.append(f"{name} exceeds the explicit planned budget")
    if warnings:
        errors.append("allocation/budget warning in daemon log")
    peaks = {
        name: max((row[name] for row in samples if type(row.get(name)) is int), default=0)
        for name in STAT_FIELDS
    }
    return {
        "valid_non_pressure_sample": not errors,
        "errors": errors,
        "planned_mount_budget_bytes": plan["max_prefetch_memory_bytes"],
        "planned_file_budget_bytes": plan["max_file_prefetch_memory_bytes"],
        "observed_maxima": peaks,
        "shutdown_stats": final,
        "allocation_warnings": warnings,
        "stats_samples": len(samples),
        "interpretation": "Zero exhaustion/pressure-eviction counters are required, not inferred from a large budget. Mapped maxima are sampled; prefetch peaks are daemon high-water counters.",
    }


def write_report(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan")
    for name in ("files", "file-size", "threads", "maximum-read", "connections"):
        plan.add_argument("--" + name, type=int, required=True)
    plan.add_argument("--output", required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--plan", required=True)
    verify.add_argument("--log", required=True)
    verify.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        result = make_plan(args.files, args.file_size, args.threads,
                           args.maximum_read, args.connections, memory_snapshot())
        write_report(args.output, result)
        if result["admitted"]:
            print(result["max_prefetch_memory_bytes"], result["max_file_prefetch_memory_bytes"])
            return 0
    else:
        result = validate_sample(json.loads(Path(args.plan).read_text()), Path(args.log).read_text())
        write_report(args.output, result)
        if result["valid_non_pressure_sample"]:
            print("uncached random-read memory validation: no budget exhaustion or pressure eviction", file=sys.stderr)
            return 0
    for error in result["errors"]:
        print("invalid uncached random-read baseline: " + error, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
