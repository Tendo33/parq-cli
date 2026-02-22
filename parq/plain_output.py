"""Lightweight output formatters that bypass Rich for fast CLI output."""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa


class PlainOutputFormatter:
    """Tab-separated plain text output — zero Rich overhead."""

    @staticmethod
    def print_metadata(metadata_dict: Dict[str, Any]) -> None:
        for k, v in metadata_dict.items():
            print(f"{k}\t{v}")

    @staticmethod
    def print_schema(schema_info: List[Dict[str, Any]]) -> None:
        print("name\ttype\tnullable")
        for col in schema_info:
            print(f"{col['name']}\t{col['type']}\t{col['nullable']}")

    @staticmethod
    def print_table(arrow_table: pa.Table, title: str = "") -> None:
        print("\t".join(arrow_table.column_names))
        for batch in arrow_table.to_batches():
            d = batch.to_pydict()
            for i in range(len(batch)):
                print("\t".join(str(d[c][i]) for c in arrow_table.column_names))

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
        print(f"source\t{source_file}")
        print(f"total_rows\t{total_rows}")
        print(f"output_files\t{len(output_files)}")
        print(f"elapsed\t{elapsed_time:.2f}s")
        for f in output_files:
            print(f"  {f}")


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

