#!/usr/bin/env python3
"""
Small vs large parquet performance benchmark for parq reader operations.
"""

from __future__ import annotations

import argparse
import statistics
import time
from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from parq.reader import ParquetReader


OPS = ("meta", "count", "head", "tail")


def _write_dataset(path: Path, rows: int, batch_size: int = 100_000) -> None:
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.float64()),
            ("flag", pa.bool_()),
        ]
    )

    with pq.ParquetWriter(path, schema=schema, compression="snappy") as writer:
        for start in range(0, rows, batch_size):
            end = min(start + batch_size, rows)
            ids = list(range(start, end))
            values = [float(i % 10_000) / 100.0 for i in range(start, end)]
            flags = [(i % 2) == 0 for i in range(start, end)]
            batch = pa.record_batch(
                [
                    pa.array(ids, type=pa.int64()),
                    pa.array(values, type=pa.float64()),
                    pa.array(flags, type=pa.bool_()),
                ],
                schema=schema,
            )
            writer.write_batch(batch)


def create_benchmark_datasets(
    base_dir: Path,
    *,
    small_rows: int = 10_000,
    large_rows: int = 2_000_000,
) -> Tuple[Path, Path, Dict[str, int]]:
    base_dir.mkdir(parents=True, exist_ok=True)
    small_path = base_dir / "small.parquet"
    large_path = base_dir / "large.parquet"

    _write_dataset(small_path, rows=small_rows)
    _write_dataset(large_path, rows=large_rows)

    dataset_info = {
        "small_rows": small_rows,
        "large_rows": large_rows,
        "small_bytes": small_path.stat().st_size,
        "large_bytes": large_path.stat().st_size,
    }
    return small_path, large_path, dataset_info


def _bench_operation(path: Path, op: str, repeats: int) -> Tuple[float, float]:
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        reader = ParquetReader(str(path))
        if op == "meta":
            reader.get_metadata_dict()
        elif op == "count":
            _ = reader.num_rows
        elif op == "head":
            reader.read_head(10)
        elif op == "tail":
            reader.read_tail(10)
        else:
            raise ValueError(f"Unsupported op: {op}")
        samples.append((time.perf_counter() - t0) * 1000.0)

    return statistics.median(samples), statistics.mean(samples)


def compare_reader_performance(
    small_path: Path,
    large_path: Path,
    *,
    repeats: int = 7,
) -> Dict[str, Dict[str, float]]:
    result: Dict[str, Dict[str, float]] = {}
    for op in OPS:
        small_median, small_mean = _bench_operation(small_path, op, repeats)
        large_median, large_mean = _bench_operation(large_path, op, repeats)
        ratio = large_median / small_median if small_median else float("inf")
        result[op] = {
            "small_median_ms": small_median,
            "small_mean_ms": small_mean,
            "large_median_ms": large_median,
            "large_mean_ms": large_mean,
            "ratio": ratio,
        }
    return result


def render_markdown_report(
    perf_result: Dict[str, Dict[str, float]],
    dataset_info: Dict[str, int],
) -> str:
    lines = [
        "## Parquet Performance Comparison",
        "",
        f"- small: {dataset_info['small_rows']} rows, {dataset_info['small_bytes']} bytes",
        f"- large: {dataset_info['large_rows']} rows, {dataset_info['large_bytes']} bytes",
        "",
        "| op | small median (ms) | large median (ms) | ratio |",
        "|---|---:|---:|---:|",
    ]
    for op in OPS:
        data = perf_result[op]
        lines.append(
            f"| {op} | {data['small_median_ms']:.3f} | "
            f"{data['large_median_ms']:.3f} | {data['ratio']:.2f}x |"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark small vs large parquet performance.")
    parser.add_argument(
        "--output-dir",
        default="/tmp/parq-bench",
        help="Directory for generated benchmark parquet files.",
    )
    parser.add_argument("--small-rows", type=int, default=10_000)
    parser.add_argument("--large-rows", type=int, default=2_000_000)
    parser.add_argument("--repeats", type=int, default=7)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    small_path, large_path, dataset_info = create_benchmark_datasets(
        output_dir,
        small_rows=args.small_rows,
        large_rows=args.large_rows,
    )
    result = compare_reader_performance(small_path, large_path, repeats=args.repeats)
    print(render_markdown_report(result, dataset_info))


if __name__ == "__main__":
    main()
