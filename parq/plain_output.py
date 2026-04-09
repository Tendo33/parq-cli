"""Lightweight output formatters that bypass Rich for fast CLI output."""

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa


class PlainOutputFormatter:
    """Tab-separated plain text output — zero Rich overhead."""

    @staticmethod
    def _writer() -> csv.writer:
        """Return a TSV writer that safely escapes embedded tabs and newlines."""
        return csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        """Keep plain output physically single-line per row for shell pipelines."""
        if value is None:
            return ""
        if isinstance(value, str):
            return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
        return value

    @staticmethod
    def print_metadata(metadata_dict: Dict[str, Any]) -> None:
        writer = PlainOutputFormatter._writer()
        for k, v in metadata_dict.items():
            writer.writerow([k, PlainOutputFormatter._normalize_value(v)])

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
            d = batch.to_pydict()
            for i in range(len(batch)):
                writer.writerow(
                    [PlainOutputFormatter._normalize_value(d[c][i]) for c in arrow_table.column_names]
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
        for f in output_files:
            writer.writerow(["file", PlainOutputFormatter._normalize_value(str(f))])


class JsonOutputFormatter:
    """JSON output — zero Rich overhead, machine-readable."""

    @staticmethod
    def print_metadata(metadata_dict: Dict[str, Any]) -> None:
        print(json.dumps(metadata_dict, default=str))

    @staticmethod
    def print_schema(schema_info: List[Dict[str, Any]]) -> None:
        print(json.dumps({"columns": schema_info}))

    @staticmethod
    def print_table(arrow_table: pa.Table, title: str = "") -> None:
        rows = []
        for batch in arrow_table.to_batches():
            d = batch.to_pydict()
            for i in range(len(batch)):
                rows.append({c: d[c][i] for c in arrow_table.column_names})
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
    def print_split_result(
        source_file: Path,
        output_files: List[Path],
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        print(json.dumps({
            "source": str(source_file),
            "total_rows": total_rows,
            "output_files": [str(f) for f in output_files],
            "elapsed_seconds": round(elapsed_time, 2),
        }))
