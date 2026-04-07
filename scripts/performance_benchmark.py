#!/usr/bin/env python3
"""
Small vs large multi-format performance benchmark for parq reader operations.
"""

from __future__ import annotations

import argparse
import shutil
import statistics
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from parq.reader import MultiFormatReader


READ_OPS = ("meta", "count", "head", "tail")
SPLIT_SCENARIOS = {
    "parquet_to_csv": ("parquet", ".csv"),
    "csv_to_parquet": ("csv", ".parquet"),
    "xlsx_to_parquet": ("xlsx", ".parquet"),
}


def _dataset_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.float64()),
            ("flag", pa.bool_()),
            ("label", pa.string()),
        ]
    )


def _build_batch(schema: pa.Schema, start: int, end: int) -> pa.RecordBatch:
    ids = list(range(start, end))
    values = [float(i % 10_000) / 100.0 for i in range(start, end)]
    flags = [(i % 2) == 0 for i in range(start, end)]
    labels = [f"row-{i}" for i in range(start, end)]
    return pa.record_batch(
        [
            pa.array(ids, type=pa.int64()),
            pa.array(values, type=pa.float64()),
            pa.array(flags, type=pa.bool_()),
            pa.array(labels, type=pa.string()),
        ],
        schema=schema,
    )


def _iter_batches(rows: int, batch_size: int = 100_000) -> Iterable[pa.RecordBatch]:
    schema = _dataset_schema()
    for start in range(0, rows, batch_size):
        end = min(start + batch_size, rows)
        yield _build_batch(schema, start, end)


def _write_parquet_dataset(path: Path, rows: int, batch_size: int = 100_000) -> None:
    schema = _dataset_schema()
    with pq.ParquetWriter(path, schema=schema, compression="snappy") as writer:
        for batch in _iter_batches(rows, batch_size=batch_size):
            writer.write_batch(batch)


def _write_csv_dataset(path: Path, rows: int, batch_size: int = 100_000) -> None:
    schema = _dataset_schema()
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = pacsv.CSVWriter(path, schema)
    try:
        for batch in _iter_batches(rows, batch_size=batch_size):
            writer.write(batch)
    finally:
        writer.close()


def _write_xlsx_dataset(path: Path, rows: int, batch_size: int = 2_000) -> None:
    import openpyxl

    workbook = openpyxl.Workbook(write_only=True)
    worksheet = workbook.create_sheet()
    column_names = _dataset_schema().names
    worksheet.append(column_names)
    for batch in _iter_batches(rows, batch_size=batch_size):
        batch_dict = batch.to_pydict()
        for row_idx in range(len(batch)):
            worksheet.append([batch_dict[column][row_idx] for column in column_names])
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    workbook.close()


def _write_dataset(path: Path, rows: int) -> None:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        _write_parquet_dataset(path, rows)
        return
    if suffix == ".csv":
        _write_csv_dataset(path, rows)
        return
    if suffix == ".xlsx":
        _write_xlsx_dataset(path, rows)
        return
    raise ValueError(f"Unsupported dataset suffix: {suffix}")


def create_benchmark_datasets(
    base_dir: Path,
    *,
    parquet_small_rows: int = 10_000,
    parquet_large_rows: int = 2_000_000,
    csv_small_rows: int = 10_000,
    csv_large_rows: int = 2_000_000,
    xlsx_small_rows: int = 2_000,
    xlsx_large_rows: int = 20_000,
) -> Tuple[Dict[str, Dict[str, Path]], Dict[str, Dict[str, int]]]:
    base_dir.mkdir(parents=True, exist_ok=True)
    spec = {
        "parquet": {
            "small_rows": parquet_small_rows,
            "large_rows": parquet_large_rows,
            "suffix": ".parquet",
        },
        "csv": {
            "small_rows": csv_small_rows,
            "large_rows": csv_large_rows,
            "suffix": ".csv",
        },
        "xlsx": {
            "small_rows": xlsx_small_rows,
            "large_rows": xlsx_large_rows,
            "suffix": ".xlsx",
        },
    }

    datasets: Dict[str, Dict[str, Path]] = {}
    dataset_info: Dict[str, Dict[str, int]] = {}
    for fmt, fmt_spec in spec.items():
        small_path = base_dir / f"{fmt}-small{fmt_spec['suffix']}"
        large_path = base_dir / f"{fmt}-large{fmt_spec['suffix']}"
        _write_dataset(small_path, rows=fmt_spec["small_rows"])
        _write_dataset(large_path, rows=fmt_spec["large_rows"])
        datasets[fmt] = {"small": small_path, "large": large_path}
        dataset_info[fmt] = {
            "small_rows": fmt_spec["small_rows"],
            "large_rows": fmt_spec["large_rows"],
            "small_bytes": small_path.stat().st_size,
            "large_bytes": large_path.stat().st_size,
        }
    return datasets, dataset_info


def _bench_read_operation(path: Path, op: str, repeats: int) -> Tuple[float, float]:
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        reader = MultiFormatReader(str(path))
        if op == "meta":
            reader.get_metadata_dict()
        elif op == "count":
            _ = reader.num_rows
        elif op == "head":
            reader.read_head(10)
        elif op == "tail":
            reader.read_tail(10)
        else:
            raise ValueError(f"Unsupported read op: {op}")
        samples.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(samples), statistics.mean(samples)


def _bench_split_operation(
    path: Path,
    output_suffix: str,
    repeats: int,
    base_dir: Path,
) -> Tuple[float, float]:
    samples = []
    for repeat in range(repeats):
        split_dir = base_dir / f"{path.stem}-{output_suffix.lstrip('.')}-{repeat}"
        if split_dir.exists():
            shutil.rmtree(split_dir)
        split_dir.mkdir(parents=True, exist_ok=True)

        t0 = time.perf_counter()
        reader = MultiFormatReader(str(path))
        reader.split_file(
            output_pattern=str(split_dir / f"chunk-%02d{output_suffix}"),
            record_count=max(1, reader.num_rows // 4),
        )
        samples.append((time.perf_counter() - t0) * 1000.0)

        shutil.rmtree(split_dir, ignore_errors=True)
    return statistics.median(samples), statistics.mean(samples)


def compare_reader_performance(
    datasets: Dict[str, Dict[str, Path]],
    *,
    repeats: int = 7,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {"reads": {}, "splits": {}}

    for fmt, fmt_paths in datasets.items():
        read_result: Dict[str, Dict[str, float]] = {}
        for op in READ_OPS:
            small_median, small_mean = _bench_read_operation(fmt_paths["small"], op, repeats)
            large_median, large_mean = _bench_read_operation(fmt_paths["large"], op, repeats)
            ratio = large_median / small_median if small_median else float("inf")
            read_result[op] = {
                "small_median_ms": small_median,
                "small_mean_ms": small_mean,
                "large_median_ms": large_median,
                "large_mean_ms": large_mean,
                "ratio": ratio,
            }
        result["reads"][fmt] = read_result

    split_base_dir = next(iter(datasets.values()))["small"].parent / "split-bench"
    split_base_dir.mkdir(parents=True, exist_ok=True)
    for scenario, (source_format, output_suffix) in SPLIT_SCENARIOS.items():
        small_median, small_mean = _bench_split_operation(
            datasets[source_format]["small"], output_suffix, repeats, split_base_dir
        )
        large_median, large_mean = _bench_split_operation(
            datasets[source_format]["large"], output_suffix, repeats, split_base_dir
        )
        ratio = large_median / small_median if small_median else float("inf")
        result["splits"][scenario] = {
            "small_median_ms": small_median,
            "small_mean_ms": small_mean,
            "large_median_ms": large_median,
            "large_mean_ms": large_mean,
            "ratio": ratio,
        }
    shutil.rmtree(split_base_dir, ignore_errors=True)
    return result


def render_markdown_report(
    perf_result: Dict[str, Dict[str, Any]],
    dataset_info: Dict[str, Dict[str, int]],
) -> str:
    lines = ["## Read Performance", ""]
    for fmt, fmt_info in dataset_info.items():
        lines.extend(
            [
                f"### {fmt}",
                "",
                f"- small: {fmt_info['small_rows']} rows, {fmt_info['small_bytes']} bytes",
                f"- large: {fmt_info['large_rows']} rows, {fmt_info['large_bytes']} bytes",
                "",
                "| op | small median (ms) | large median (ms) | ratio |",
                "|---|---:|---:|---:|",
            ]
        )
        for op in READ_OPS:
            data = perf_result["reads"][fmt][op]
            lines.append(
                f"| {op} | {data['small_median_ms']:.3f} | "
                f"{data['large_median_ms']:.3f} | {data['ratio']:.2f}x |"
            )
        lines.append("")

    lines.extend(
        [
            "## Split Performance",
            "",
            "| scenario | small median (ms) | large median (ms) | ratio |",
            "|---|---:|---:|---:|",
        ]
    )
    for scenario in SPLIT_SCENARIOS:
        data = perf_result["splits"][scenario]
        lines.append(
            f"| {scenario} | {data['small_median_ms']:.3f} | "
            f"{data['large_median_ms']:.3f} | {data['ratio']:.2f}x |"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark small vs large multi-format performance.")
    parser.add_argument(
        "--output-dir",
        default="/tmp/parq-bench",
        help="Directory for generated benchmark datasets.",
    )
    parser.add_argument("--parquet-small-rows", type=int, default=10_000)
    parser.add_argument("--parquet-large-rows", type=int, default=2_000_000)
    parser.add_argument("--csv-small-rows", type=int, default=10_000)
    parser.add_argument("--csv-large-rows", type=int, default=2_000_000)
    parser.add_argument("--xlsx-small-rows", type=int, default=2_000)
    parser.add_argument("--xlsx-large-rows", type=int, default=20_000)
    parser.add_argument("--repeats", type=int, default=7)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    datasets, dataset_info = create_benchmark_datasets(
        output_dir,
        parquet_small_rows=args.parquet_small_rows,
        parquet_large_rows=args.parquet_large_rows,
        csv_small_rows=args.csv_small_rows,
        csv_large_rows=args.csv_large_rows,
        xlsx_small_rows=args.xlsx_small_rows,
        xlsx_large_rows=args.xlsx_large_rows,
    )
    result = compare_reader_performance(datasets, repeats=args.repeats)
    print(render_markdown_report(result, dataset_info))


if __name__ == "__main__":
    main()
