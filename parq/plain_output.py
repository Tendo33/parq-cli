"""Lightweight output formatters that bypass Rich for fast CLI output."""

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa


class PlainOutputFormatter:
    """Tab-separated plain text output with minimal overhead."""

    @staticmethod
    def _writer() -> csv.writer:
        return csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, str):
            return (
                value.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
            )
        return value

    @staticmethod
    def print_metadata(metadata_dict: Dict[str, Any]) -> None:
        writer = PlainOutputFormatter._writer()
        for key, value in metadata_dict.items():
            writer.writerow([key, PlainOutputFormatter._normalize_value(value)])

    @staticmethod
    def print_schema(schema_info: List[Dict[str, Any]]) -> None:
        writer = PlainOutputFormatter._writer()
        writer.writerow(["name", "type", "nullable"])
        for col in schema_info:
            writer.writerow(
                [
                    PlainOutputFormatter._normalize_value(col["name"]),
                    PlainOutputFormatter._normalize_value(col["type"]),
                    col["nullable"],
                ]
            )

    @staticmethod
    def print_table(arrow_table: pa.Table, title: str = "") -> None:
        del title
        writer = PlainOutputFormatter._writer()
        writer.writerow(arrow_table.column_names)
        for batch in arrow_table.to_batches():
            batch_dict = batch.to_pydict()
            for row_idx in range(len(batch)):
                writer.writerow(
                    [
                        PlainOutputFormatter._normalize_value(batch_dict[column][row_idx])
                        for column in arrow_table.column_names
                    ]
                )

    @staticmethod
    def print_count(count: int) -> None:
        print(count)

    @staticmethod
    def print_error(message: str) -> None:
        print(f"Error: {message}", file=sys.stderr)

    @staticmethod
    def print_success(message: str) -> None:
        print(message)

    @staticmethod
    def print_stats(stats_rows: List[Dict[str, Any]]) -> None:
        writer = PlainOutputFormatter._writer()
        writer.writerow(["name", "type", "count", "null_count", "min", "max", "mean", "cardinality", "top_values"])
        for row in stats_rows:
            import json as _json
            top_values = row.get("top_values")
            top_str = _json.dumps(top_values) if top_values else ""
            writer.writerow(
                [
                    PlainOutputFormatter._normalize_value(row["name"]),
                    PlainOutputFormatter._normalize_value(row["type"]),
                    row["count"],
                    row["null_count"],
                    PlainOutputFormatter._normalize_value(row["min"]),
                    PlainOutputFormatter._normalize_value(row["max"]),
                    PlainOutputFormatter._normalize_value(row["mean"]),
                    PlainOutputFormatter._normalize_value(row.get("cardinality")),
                    top_str,
                ]
            )

    @staticmethod
    def print_diff_result(diff_result: Dict[str, Any]) -> None:
        writer = PlainOutputFormatter._writer()
        for key in [
            "row_count_delta",
            "only_left_count",
            "only_right_count",
            "changed_count",
        ]:
            writer.writerow([key, PlainOutputFormatter._normalize_value(diff_result[key])])

    @staticmethod
    def print_convert_result(
        source_file: Path,
        output_file: Path,
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        writer = PlainOutputFormatter._writer()
        writer.writerow(["source", PlainOutputFormatter._normalize_value(str(source_file))])
        writer.writerow(["output", PlainOutputFormatter._normalize_value(str(output_file))])
        writer.writerow(["total_rows", total_rows])
        writer.writerow(["elapsed", f"{elapsed_time:.2f}s"])

    @staticmethod
    def print_merge_result(
        input_files: List[Path],
        output_file: Path,
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        writer = PlainOutputFormatter._writer()
        writer.writerow(["inputs", len(input_files)])
        writer.writerow(["output", PlainOutputFormatter._normalize_value(str(output_file))])
        writer.writerow(["total_rows", total_rows])
        writer.writerow(["elapsed", f"{elapsed_time:.2f}s"])

    @staticmethod
    def print_split_result(
        source_file: Path,
        output_files: List[Path],
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        writer = PlainOutputFormatter._writer()
        writer.writerow(["source", PlainOutputFormatter._normalize_value(str(source_file))])
        writer.writerow(["total_rows", total_rows])
        writer.writerow(["output_files", len(output_files)])
        writer.writerow(["elapsed", f"{elapsed_time:.2f}s"])
        for output_file in output_files:
            writer.writerow(["file", PlainOutputFormatter._normalize_value(str(output_file))])


class JsonOutputFormatter:
    """JSON output with minimal formatting overhead."""

    @staticmethod
    def print_metadata(metadata_dict: Dict[str, Any]) -> None:
        print(json.dumps(metadata_dict, default=str))

    @staticmethod
    def print_schema(schema_info: List[Dict[str, Any]]) -> None:
        print(json.dumps({"columns": schema_info}, default=str))

    @staticmethod
    def print_table(arrow_table: pa.Table, title: str = "") -> None:
        del title
        rows = []
        for batch in arrow_table.to_batches():
            batch_dict = batch.to_pydict()
            for row_idx in range(len(batch)):
                rows.append({column: batch_dict[column][row_idx] for column in arrow_table.column_names})
        print(json.dumps({"rows": rows}, default=str))

    @staticmethod
    def print_count(count: int) -> None:
        print(json.dumps({"count": count}))

    @staticmethod
    def print_error(message: str) -> None:
        print(json.dumps({"error": message}), file=sys.stderr)

    @staticmethod
    def print_success(message: str) -> None:
        print(json.dumps({"message": message}))

    @staticmethod
    def print_stats(stats_rows: List[Dict[str, Any]]) -> None:
        print(json.dumps({"columns": stats_rows}, default=str))

    @staticmethod
    def print_diff_result(diff_result: Dict[str, Any]) -> None:
        print(json.dumps(diff_result, default=str))

    @staticmethod
    def print_convert_result(
        source_file: Path,
        output_file: Path,
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        print(
            json.dumps(
                {
                    "source": str(source_file),
                    "output": str(output_file),
                    "total_rows": total_rows,
                    "elapsed_seconds": round(elapsed_time, 2),
                }
            )
        )

    @staticmethod
    def print_merge_result(
        input_files: List[Path],
        output_file: Path,
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        print(
            json.dumps(
                {
                    "inputs": [str(path) for path in input_files],
                    "output": str(output_file),
                    "total_rows": total_rows,
                    "elapsed_seconds": round(elapsed_time, 2),
                }
            )
        )

    @staticmethod
    def print_split_result(
        source_file: Path,
        output_files: List[Path],
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        print(
            json.dumps(
                {
                    "source": str(source_file),
                    "total_rows": total_rows,
                    "output_files": [str(path) for path in output_files],
                    "elapsed_seconds": round(elapsed_time, 2),
                }
            )
        )
