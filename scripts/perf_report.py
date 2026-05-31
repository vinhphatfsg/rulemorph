#!/usr/bin/env python3
import argparse
import json
import math
import os
import stat
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CRITERION = ROOT / "target" / "criterion"
WARN_THRESHOLD = 10.0
REGRESSION_THRESHOLD = 20.0
IMPROVEMENT_THRESHOLD = -5.0
MAX_CRITERION_RESULT_FILES = 200
MAX_JSON_BYTES = 1_048_576
MAX_BENCHMARK_ID_CHARS = 300
MAX_THROUGHPUT_VALUE = 1_000_000_000_000


def load_estimates(criterion_dir):
    rows = []
    for estimates_path in estimate_paths(criterion_dir):
        data = load_json_limited(estimates_path)
        mean = data.get("mean", {})
        raw_point = mean.get("point_estimate")
        if raw_point is None:
            continue
        point = finite_number(raw_point)
        if point is None or point <= 0:
            raise ValueError(f"invalid Criterion mean point estimate: {estimates_path}")
        new_dir = estimates_path.parent
        benchmark = load_benchmark(new_dir / "benchmark.json")
        throughput = benchmark.get("throughput")
        name = benchmark_id(benchmark, estimates_path, criterion_dir)
        if len(name) > MAX_BENCHMARK_ID_CHARS:
            raise ValueError(f"benchmark id is too long: {estimates_path}")
        rows.append(
            {
                "benchmark": name,
                "mean_ns": point,
                "records_sec": records_per_sec(point, throughput),
                "mb_sec": mb_per_sec(point, throughput),
            }
        )
    return sorted(rows, key=lambda row: row["benchmark"])


def estimate_paths(criterion_dir):
    paths = []
    for path in criterion_dir.glob("**/new/estimates.json"):
        paths.append(path)
        if len(paths) > MAX_CRITERION_RESULT_FILES:
            raise ValueError(
                f"too many Criterion result files under {criterion_dir}: "
                f"limit is {MAX_CRITERION_RESULT_FILES}"
            )
    return sorted(paths)


def load_json_limited(path):
    path_stat = path.lstat()
    if stat.S_ISLNK(path_stat.st_mode):
        raise ValueError(f"refusing to read symlinked JSON file: {path}")
    if not stat.S_ISREG(path_stat.st_mode):
        raise ValueError(f"refusing to read non-file JSON path: {path}")
    if path_stat.st_size > MAX_JSON_BYTES:
        raise ValueError(f"JSON file is too large: {path} ({path_stat.st_size} bytes)")

    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        fd_stat = os.fstat(fd)
        if not stat.S_ISREG(fd_stat.st_mode):
            raise ValueError(f"refusing to read non-file JSON path: {path}")
        if fd_stat.st_size > MAX_JSON_BYTES:
            raise ValueError(f"JSON file is too large: {path} ({fd_stat.st_size} bytes)")
        with os.fdopen(fd, "r", encoding="utf-8") as f:
            fd = None
            return json.load(f)
    finally:
        if fd is not None:
            os.close(fd)


def load_benchmark(path):
    if not path.exists():
        return {}
    return load_json_limited(path)


def benchmark_id(benchmark, estimates_path, criterion_dir):
    group = benchmark.get("group_id")
    if isinstance(group, str) and group:
        parts = [group]
        function_id = benchmark.get("function_id")
        value_str = benchmark.get("value_str")
        if isinstance(function_id, str) and function_id:
            parts.append(function_id)
        if isinstance(value_str, str) and value_str:
            parts.append(value_str)
        return "/".join(parts)
    return estimates_path.parents[1].relative_to(criterion_dir).as_posix()


def records_per_sec(mean_ns, throughput):
    if not isinstance(throughput, dict) or "Elements" not in throughput:
        return None
    elements = throughput_value(throughput.get("Elements"), "Elements")
    seconds = mean_ns / 1_000_000_000
    return elements / seconds


def mb_per_sec(mean_ns, throughput):
    if not isinstance(throughput, dict) or "Bytes" not in throughput:
        return None
    bytes_value = throughput_value(throughput.get("Bytes"), "Bytes")
    seconds = mean_ns / 1_000_000_000
    return (bytes_value / 1_048_576) / seconds


def finite_number(value):
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def throughput_value(value, name):
    number = finite_number(value)
    if number is None or number < 0 or number > MAX_THROUGHPUT_VALUE:
        raise ValueError(f"invalid Criterion throughput {name}: {value!r}")
    return number


def load_json_baseline(path):
    if path is None or not path.exists():
        return {}
    data = load_json_limited(path)
    baseline = {}
    for name, values in data.get("benchmarks", {}).items():
        if isinstance(values, dict):
            baseline[name] = values
    return baseline


def baseline_mean(entry):
    if not isinstance(entry, dict):
        return None
    mean_ns = entry.get("mean_ns")
    if isinstance(mean_ns, (int, float)):
        return float(mean_ns)
    return None


def delta_value(current, baseline):
    if baseline is None:
        return None
    return ((current - baseline) / baseline) * 100


def classify_delta(delta):
    if delta is None:
        return "new"
    if delta <= IMPROVEMENT_THRESHOLD:
        return "improvement"
    if delta >= REGRESSION_THRESHOLD:
        return "regression"
    if delta >= WARN_THRESHOLD:
        return "warn"
    return "ok"


def with_comparison(rows, baseline):
    compared = []
    for row in rows:
        delta = delta_value(row["mean_ns"], baseline_mean(baseline.get(row["benchmark"])))
        compared.append(
            {
                **row,
                "delta_percent": delta,
                "status": classify_delta(delta),
            }
        )
    return compared


def format_optional(value, suffix=""):
    if value is None:
        return "-"
    return f"{value:.2f}{suffix}"


def format_delta(delta):
    if delta is None:
        return "-"
    return f"{delta:+.2f}%"


def markdown_cell(value):
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def write_json_snapshot(rows, missing, path):
    if path is None:
        return
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "thresholds": {
            "improvement_percent": IMPROVEMENT_THRESHOLD,
            "warn_percent": WARN_THRESHOLD,
            "regression_percent": REGRESSION_THRESHOLD,
        },
        "benchmarks": {
            row["benchmark"]: {
                "mean_ns": row["mean_ns"],
                "records_sec": row["records_sec"],
                "mb_sec": row["mb_sec"],
                "delta_percent": row["delta_percent"],
                "status": row["status"],
            }
            for row in rows
        },
        "missing_from_current": missing,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def print_markdown(rows, missing):
    print("# Rulemorph Core Performance Snapshot")
    print()
    print("| benchmark | mean ns/iter | records/sec | MB/sec | baseline delta | status |")
    print("| --- | ---: | ---: | ---: | ---: | --- |")
    for row in rows:
        print(
            f"| {markdown_cell(row['benchmark'])} | {row['mean_ns']:.0f} | "
            f"{format_optional(row['records_sec'])} | "
            f"{format_optional(row['mb_sec'])} | "
            f"{format_delta(row['delta_percent'])} | "
            f"{markdown_cell(row['status'])} |"
        )
    for name in missing:
        print(f"| {markdown_cell(name)} | - | - | - | - | missing |")
    if not rows and not missing:
        print("| `_no_criterion_results_found_` | 0 | - | - | - | new |")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--criterion-dir", type=Path, default=DEFAULT_CRITERION)
    parser.add_argument("--baseline-json", type=Path)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()

    rows = load_estimates(args.criterion_dir)
    baseline = load_json_baseline(args.baseline_json)
    compared = with_comparison(rows, baseline)
    current_names = {row["benchmark"] for row in compared}
    missing = sorted(name for name in baseline if name not in current_names)

    write_json_snapshot(compared, missing, args.json_output)
    print_markdown(compared, missing)


if __name__ == "__main__":
    main()
