#!/usr/bin/env python3

import argparse
import json
import math
from pathlib import Path


def load_values(path: Path, field: str) -> list[int]:
    values: list[int] = []
    with path.open("r", encoding="utf-8") as source:
        for line in source:
            line = line.strip()
            if not line.startswith("{"):
                continue
            record = json.loads(line)
            if field == "wall_residual_ns" and "total_ns" in record:
                values.append(max(
                    0,
                    int(record["total_ns"])
                    - int(record.get("transport_span_ns", 0)),
                ))
            elif field in record:
                values.append(int(record[field]))
    if not values:
        raise ValueError(f"{path} contains no {field} samples")
    return sorted(values)


def percentile(values: list[int], fraction: float) -> int:
    index = max(0, math.ceil(fraction * len(values)) - 1)
    return values[index]


def summary(values: list[int]) -> dict[str, float | int]:
    return {
        "samples": len(values),
        "total": sum(values),
        "mean": sum(values) / len(values),
        "p50": percentile(values, 0.50),
        "p99": percentile(values, 0.99),
        "p999": percentile(values, 0.999),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--field", default="residual_ns")
    parser.add_argument("--max-ratio", type=float, default=0.20)
    parser.add_argument("--statistic", choices=("mean", "p50", "p99", "p999"),
                        default="p99")
    arguments = parser.parse_args()

    candidate = summary(load_values(arguments.candidate, arguments.field))
    output: dict[str, object] = {"candidate": candidate}
    exit_code = 0
    if arguments.baseline:
        baseline = summary(load_values(arguments.baseline, arguments.field))
        ratio = float(candidate[arguments.statistic]) / float(
            baseline[arguments.statistic]
        )
        passed = ratio <= arguments.max_ratio
        output.update({"baseline": baseline, "ratio": ratio, "passed": passed})
        exit_code = 0 if passed else 1
    print(json.dumps(output, separators=(",", ":")))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
