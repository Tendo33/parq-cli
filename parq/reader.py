"""
Tabular file reader module.
Provides the unified MultiFormatReader, diff_files, and merge_files API.

Internal format-specific logic lives in parq/formats/:
  _common.py        — shared utilities and statistics
  _csv.py           — CSV reader functions
  _xlsx.py          — XLSX reader functions
  _chunk_writers.py — chunk writers and streaming split/write helpers
  _parquet.py       — ParquetReader class
"""

from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, List, Optional, Union

import pyarrow as pa

from parq.formats import SUPPORTED_INPUT_FORMATS
from parq.formats._chunk_writers import (
    _open_chunk_writer,
    _resolve_output_files,
    _stream_split_by_record_count,
    _split_batches_to_files,
    _validate_output_pattern,
    _write_batches_to_output,
)
from parq.formats._common import (
    _InputMetadata,
    _collect_preview_from_batches,
    _compute_stats_from_batches,
    _create_empty_table,
    _resolve_split_shape,
    _select_schema,
    _table_schema_info,
    _validate_preview_params,
)
from parq.formats._csv import _iter_csv_batches, _scan_csv_metadata
from parq.formats._parquet import ParquetReader
from parq.formats._xlsx import (
    _count_xlsx_rows,
    _iter_xlsx_batches,
    _scan_xlsx_structure,
)

__all__ = [
    "SUPPORTED_INPUT_FORMATS",
    "ParquetReader",
    "MultiFormatReader",
    "diff_files",
    "merge_files",
]


class MultiFormatReader:
    """Reader that supports parquet/csv/tsv/xlsx with a unified CLI-facing interface."""

    def __init__(
        self,
        file_path: str,
        delimiter: str = ",",
        sheet: Optional[str] = None,
    ):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        suffix = self.file_path.suffix.lower()
        if suffix not in SUPPORTED_INPUT_FORMATS:
            supported = ", ".join(sorted(SUPPORTED_INPUT_FORMATS))
            raise ValueError(
                f"Unsupported file format: {suffix or '<none>'}. Supported formats: {supported}"
            )

        # Auto-detect tab delimiter for .tsv files when no explicit delimiter was given
        if suffix == ".tsv" and delimiter == ",":
            delimiter = "\t"

        self._delimiter = delimiter
        self._sheet = sheet

        # Normalise input_format: tsv is handled like csv internally
        self.input_format = "csv" if suffix in {".csv", ".tsv"} else suffix.lstrip(".")

        self._parquet_reader: Optional[ParquetReader] = None
        self._metadata_cache: Optional[_InputMetadata] = None
        self.last_split_total_rows: Optional[int] = None
        self.last_write_total_rows: Optional[int] = None

        if suffix == ".parquet":
            self._parquet_reader = ParquetReader(file_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_metadata(self, *, include_row_count: bool = False) -> _InputMetadata:
        """Load and cache metadata without materializing the full input as a table."""
        if self._parquet_reader is not None:
            raise RuntimeError("Parquet metadata should be read from ParquetReader")

        if self.input_format == "xlsx":
            if self._metadata_cache is None:
                self._metadata_cache = _scan_xlsx_structure(self.file_path, sheet=self._sheet)
            if include_row_count and self._metadata_cache.num_rows is None:
                self._metadata_cache = _InputMetadata(
                    headers=self._metadata_cache.headers,
                    schema=self._metadata_cache.schema,
                    num_rows=_count_xlsx_rows(self.file_path, sheet=self._sheet),
                )
            return self._metadata_cache

        if self._metadata_cache is None or (
            include_row_count and self._metadata_cache.num_rows is None
        ):
            self._metadata_cache = _scan_csv_metadata(
                self.file_path, include_row_count=include_row_count, delimiter=self._delimiter
            )
        return self._metadata_cache

    def _iter_input_batches(self, columns: Optional[List[str]] = None) -> Iterator[pa.RecordBatch]:
        """Iterate non-parquet input in batches with format branching in one place."""
        metadata = self._load_metadata()
        if self.input_format == "csv":
            yield from _iter_csv_batches(self.file_path, columns=columns, delimiter=self._delimiter)
            return
        yield from _iter_xlsx_batches(self.file_path, metadata, columns=columns, sheet=self._sheet)

    def _selected_schema(self, columns: Optional[List[str]] = None) -> pa.Schema:
        """Return full or selected schema after central column validation."""
        metadata = self._load_metadata()
        _validate_preview_params(0, metadata.schema, columns)
        return _select_schema(metadata.schema, columns)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def schema(self) -> pa.Schema:
        if self._parquet_reader is not None:
            return self._parquet_reader.schema
        return self._load_metadata().schema

    @property
    def num_rows(self) -> int:
        if self._parquet_reader is not None:
            return self._parquet_reader.num_rows
        return self._load_metadata(include_row_count=True).num_rows or 0

    @property
    def num_columns(self) -> int:
        if self._parquet_reader is not None:
            return self._parquet_reader.num_columns
        return len(self.schema)

    @property
    def num_physical_columns(self) -> int:
        if self._parquet_reader is not None:
            return self._parquet_reader.num_physical_columns
        return self.num_columns

    @property
    def num_row_groups(self) -> int:
        if self._parquet_reader is not None:
            return self._parquet_reader.num_row_groups
        return 1

    # ------------------------------------------------------------------
    # Metadata / schema
    # ------------------------------------------------------------------

    def get_metadata_dict(self, fast: bool = False) -> dict:
        if self._parquet_reader is not None:
            return self._parquet_reader.get_metadata_dict(fast=fast)

        metadata_dict = {
            "file_path": str(self.file_path),
            "input_format": self.input_format,
            "num_columns": self.num_columns,
            "file_size": self.file_path.stat().st_size,
            "num_row_groups": self.num_row_groups,
        }
        if not fast:
            metadata_dict["num_rows"] = self.num_rows
        return metadata_dict

    def get_schema_info(self) -> List[dict]:
        if self._parquet_reader is not None:
            return self._parquet_reader.get_schema_info()
        return _table_schema_info(self.schema)

    # ------------------------------------------------------------------
    # Preview
    # ------------------------------------------------------------------

    def read_head(self, n: int = 5, columns: Optional[List[str]] = None) -> pa.Table:
        if self._parquet_reader is not None:
            return self._parquet_reader.read_head(n=n, columns=columns)
        _validate_preview_params(n, self.schema, columns)
        if n == 0:
            return _create_empty_table(self.schema, columns=columns)
        return _collect_preview_from_batches(
            self._iter_input_batches(columns=columns),
            n,
            self._selected_schema(columns),
        )

    def read_tail(self, n: int = 5, columns: Optional[List[str]] = None) -> pa.Table:
        if self._parquet_reader is not None:
            return self._parquet_reader.read_tail(n=n, columns=columns)
        _validate_preview_params(n, self.schema, columns)
        if n == 0:
            return _create_empty_table(self.schema, columns=columns)
        return _collect_preview_from_batches(
            self._iter_input_batches(columns=columns),
            n,
            self._selected_schema(columns),
            from_tail=True,
        )

    def read_columns(self, columns: Optional[List[str]] = None) -> pa.Table:
        if self._parquet_reader is not None:
            return self._parquet_reader.read_columns(columns=columns)
        selected_schema = self._selected_schema(columns)
        batches = list(self._iter_input_batches(columns=columns))
        if not batches:
            return _create_empty_table(selected_schema)
        return pa.Table.from_batches(batches, schema=selected_schema)

    # ------------------------------------------------------------------
    # Split helpers (B4 decomposition)
    # ------------------------------------------------------------------

    def _split_parquet_to_parquet(
        self,
        output_pattern: str,
        file_count: Optional[int],
        record_count: Optional[int],
        progress_callback: Optional[Callable[[int, int], None]],
        force: bool,
    ) -> List[Path]:
        """Fast path: parquet input → parquet output via ParquetReader."""
        assert self._parquet_reader is not None
        output_files = self._parquet_reader.split_file(
            output_pattern=output_pattern,
            file_count=file_count,
            record_count=record_count,
            progress_callback=progress_callback,
            force=force,
        )
        self.last_split_total_rows = self._parquet_reader.last_split_total_rows
        return output_files

    def _split_streaming_record_count(
        self,
        schema: pa.Schema,
        batches: Iterable[pa.RecordBatch],
        output_pattern: str,
        record_count: int,
        progress_callback: Optional[Callable[[int, int], None]],
        compression: Optional[str],
        force: bool,
    ) -> List[Path]:
        """Single-pass streaming split by record count."""
        output_files, total_rows = _stream_split_by_record_count(
            batches,
            schema,
            output_pattern,
            record_count,
            progress_callback=progress_callback,
            compression=compression,
            force=force,
        )
        self.last_split_total_rows = total_rows
        return output_files

    def _split_generic_file_count(
        self,
        schema: pa.Schema,
        batches: Iterable[pa.RecordBatch],
        total_rows: int,
        output_pattern: str,
        file_count: Optional[int],
        record_count: Optional[int],
        progress_callback: Optional[Callable[[int, int], None]],
        compression: Optional[str],
        force: bool,
    ) -> List[Path]:
        """Two-pass split by file count (or record count when total is known upfront)."""
        chunk_sizes = _resolve_split_shape(
            total_rows, file_count=file_count, record_count=record_count
        )
        output_files = _resolve_output_files(output_pattern, len(chunk_sizes), force=force)
        result = _split_batches_to_files(
            batches,
            schema,
            output_files,
            chunk_sizes,
            total_rows,
            progress_callback=progress_callback,
            compression=compression,
        )
        self.last_split_total_rows = total_rows
        return result

    # ------------------------------------------------------------------
    # Split / convert / stats
    # ------------------------------------------------------------------

    def split_file(
        self,
        output_pattern: str,
        file_count: Optional[int] = None,
        record_count: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        force: bool = False,
    ) -> List[Path]:
        """Split the input file into multiple output files."""
        _validate_output_pattern(output_pattern)

        sample_output = Path(output_pattern % 0)
        output_suffix = sample_output.suffix.lower()

        # Fast path: parquet → parquet
        if self._parquet_reader is not None and output_suffix == ".parquet":
            return self._split_parquet_to_parquet(
                output_pattern, file_count, record_count, progress_callback, force
            )

        # Determine schema and compression
        if self._parquet_reader is not None:
            schema = self._parquet_reader.schema
            compression = (
                self._parquet_reader._get_compression_type()
                if output_suffix == ".parquet"
                else None
            )
        else:
            schema = self.schema
            compression = None

        # Single-pass streaming path when chunking by record count
        if record_count is not None and file_count is None:
            batches: Iterable[pa.RecordBatch] = (
                self._iter_input_batches()
                if self._parquet_reader is None
                else self._parquet_reader._parquet_file.iter_batches(
                    batch_size=min(10000, record_count)
                )
            )
            return self._split_streaming_record_count(
                schema, batches, output_pattern, record_count, progress_callback, compression, force
            )

        # Two-pass path: count rows first, then split
        total_rows = self.num_rows
        chunk_sizes = _resolve_split_shape(
            total_rows, file_count=file_count, record_count=record_count
        )
        batches = (
            self._iter_input_batches()
            if self._parquet_reader is None
            else self._parquet_reader._parquet_file.iter_batches(
                batch_size=min(10000, max(chunk_sizes))
            )
        )
        return self._split_generic_file_count(
            schema,
            batches,
            total_rows,
            output_pattern,
            file_count,
            record_count,
            progress_callback,
            compression,
            force,
        )

    def convert_file(
        self,
        output_path: Union[str, Path],
        columns: Optional[List[str]] = None,
        force: bool = False,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Convert the input file to a single supported output file."""
        output_path = Path(output_path)
        if self._parquet_reader is not None:
            rows_written = self._parquet_reader.convert_file(
                output_path, columns=columns, force=force, progress_callback=progress_callback
            )
            self.last_write_total_rows = rows_written
            return rows_written

        total_rows = self.num_rows if progress_callback is not None else 0
        rows_written = _write_batches_to_output(
            self._iter_input_batches(columns=columns),
            self._selected_schema(columns),
            output_path,
            force=force,
            progress_callback=progress_callback,
            total_rows=total_rows,
        )
        self.last_write_total_rows = rows_written
        return rows_written

    def get_stats(
        self, columns: Optional[List[str]] = None, limit: int = 50, top_n: int = 5
    ) -> List[dict]:
        """Return simple column statistics for the current file."""
        if self._parquet_reader is not None:
            return self._parquet_reader.get_stats(columns=columns, limit=limit, top_n=top_n)
        selected_schema = self._selected_schema(columns)
        return _compute_stats_from_batches(
            self._iter_input_batches(columns=columns),
            selected_schema,
            limit=limit,
            top_n=top_n,
        )


# ------------------------------------------------------------------
# Module-level utilities used by diff and merge
# ------------------------------------------------------------------


def _reader_batch_iterator(
    reader: MultiFormatReader, columns: Optional[List[str]] = None
) -> Iterator[pa.RecordBatch]:
    """Return a batch iterator for any supported reader."""
    if reader._parquet_reader is not None:
        yield from reader._parquet_reader._parquet_file.iter_batches(columns=columns)
        return
    yield from reader._iter_input_batches(columns=columns)


def _iter_row_dicts(
    reader: MultiFormatReader,
    columns: List[str],
) -> Iterator[dict[str, Any]]:
    """Iterate selected rows as lightweight dicts without materializing full tables."""
    for batch in _reader_batch_iterator(reader, columns=columns):
        batch_dict = batch.to_pydict()
        for row_idx in range(len(batch)):
            yield {column: batch_dict[column][row_idx] for column in columns}


def _build_diff_index(
    reader: MultiFormatReader,
    key_columns: List[str],
    selected_columns: List[str],
    comparable_columns: List[str],
    side_name: str,
) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Build a keyed row index for diffing and reject duplicate keys."""
    index: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in _iter_row_dicts(reader, selected_columns):
        key = tuple(row[column] for column in key_columns)
        if key in index:
            rendered_key = key if len(key) > 1 else key[0]
            raise ValueError(f"Duplicate key found in {side_name} input: {rendered_key}")
        index[key] = {column: row[column] for column in comparable_columns}
    return index


def diff_files(
    left_path: Union[str, Path],
    right_path: Union[str, Path],
    key_columns: List[str],
    columns: Optional[List[str]] = None,
    limit: int = 20,
    summary_only: bool = False,
) -> dict:
    """Compute a keyed diff summary for two parquet/csv files."""
    left_reader = MultiFormatReader(str(left_path))
    right_reader = MultiFormatReader(str(right_path))

    if "xlsx" in {left_reader.input_format, right_reader.input_format}:
        raise ValueError(
            "diff currently supports parquet/csv inputs; convert xlsx first with:\n"
            "  parq convert input.xlsx output.parquet"
        )

    _validate_preview_params(0, left_reader.schema, key_columns)
    _validate_preview_params(0, right_reader.schema, key_columns)
    if columns is not None:
        _validate_preview_params(0, left_reader.schema, columns)
        _validate_preview_params(0, right_reader.schema, columns)

    left_names = left_reader.schema.names
    right_names = right_reader.schema.names
    schema_only_left = [name for name in left_names if name not in right_names]
    schema_only_right = [name for name in right_names if name not in left_names]
    shared_columns = [name for name in left_names if name in right_names]
    schema_type_mismatches = []
    for name in shared_columns:
        left_field = left_reader.schema.field(name)
        right_field = right_reader.schema.field(name)
        if str(left_field.type) != str(right_field.type):
            schema_type_mismatches.append(
                {
                    "column": name,
                    "left_type": str(left_field.type),
                    "right_type": str(right_field.type),
                }
            )

    comparable_columns = columns or [
        name for name in left_names if name in right_names and name not in key_columns
    ]
    selected_columns = list(dict.fromkeys(key_columns + comparable_columns))
    index_left = left_reader.num_rows <= right_reader.num_rows
    indexed_reader = left_reader if index_left else right_reader
    streamed_reader = right_reader if index_left else left_reader
    indexed_side_name = "left" if index_left else "right"
    streamed_side_name = "right" if index_left else "left"

    indexed_rows = _build_diff_index(
        indexed_reader,
        key_columns,
        selected_columns,
        comparable_columns,
        indexed_side_name,
    )
    missing_from_indexed: List[tuple[Any, ...]] = []
    missing_from_indexed_count = 0
    changed_rows = []
    changed_count = 0
    seen_streamed_keys: set[tuple[Any, ...]] = set()

    for row in _iter_row_dicts(streamed_reader, selected_columns):
        key = tuple(row[column] for column in key_columns)
        if key in seen_streamed_keys:
            rendered_key = key if len(key) > 1 else key[0]
            raise ValueError(f"Duplicate key found in {streamed_side_name} input: {rendered_key}")
        seen_streamed_keys.add(key)

        streamed_values = {column: row[column] for column in comparable_columns}
        if key not in indexed_rows:
            missing_from_indexed_count += 1
            if not summary_only and len(missing_from_indexed) < limit:
                missing_from_indexed.append(key)
            continue

        indexed_values = indexed_rows.pop(key)
        if any(streamed_values[column] != indexed_values[column] for column in comparable_columns):
            changed_count += 1
            if index_left:
                left_values = indexed_values
                right_values = streamed_values
            else:
                left_values = streamed_values
                right_values = indexed_values
            if not summary_only and len(changed_rows) < limit:
                changed_rows.append(
                    {
                        "key": key if len(key) > 1 else key[0],
                        "left": left_values,
                        "right": right_values,
                    }
                )

    remaining_indexed_count = len(indexed_rows)
    if not summary_only:
        remaining_indexed_keys = sorted(indexed_rows.keys())[:limit]
        missing_from_indexed.sort()
        only_left_keys = remaining_indexed_keys if index_left else missing_from_indexed
        only_right_keys = missing_from_indexed if index_left else remaining_indexed_keys
    else:
        only_left_keys = []
        only_right_keys = []

    only_left_count = remaining_indexed_count if index_left else missing_from_indexed_count
    only_right_count = missing_from_indexed_count if index_left else remaining_indexed_count

    return {
        "row_count_delta": left_reader.num_rows - right_reader.num_rows,
        "only_left_count": only_left_count,
        "only_right_count": only_right_count,
        "changed_count": changed_count,
        "schema_only_left": (
            [] if summary_only else [{"column": name} for name in schema_only_left[:limit]]
        ),
        "schema_only_right": (
            [] if summary_only else [{"column": name} for name in schema_only_right[:limit]]
        ),
        "schema_type_mismatches": [] if summary_only else schema_type_mismatches[:limit],
        "only_left": [{"key": key if len(key) > 1 else key[0]} for key in only_left_keys[:limit]],
        "only_right": [{"key": key if len(key) > 1 else key[0]} for key in only_right_keys[:limit]],
        "changed_rows": [] if summary_only else changed_rows[:limit],
    }


def merge_files(
    input_paths: List[Union[str, Path]],
    output_path: Union[str, Path],
    force: bool = False,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Merge multiple compatible inputs into a single output file."""
    if len(input_paths) == 0:
        raise ValueError("At least one input file must be provided")

    output_path = Path(output_path)
    if output_path.exists():
        if force:
            output_path.unlink()
        else:
            raise FileExistsError(f"Output file already exists: {output_path}")

    readers = [MultiFormatReader(str(path)) for path in input_paths]
    schemas = [reader.schema for reader in readers]
    try:
        unified_schema = pa.unify_schemas(schemas)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        raise ValueError(f"Incompatible schemas: {e}") from e

    # Compute total rows for progress reporting (0 = unknown)
    total_rows = 0
    if progress_callback is not None:
        try:
            total_rows = sum(r.num_rows for r in readers)
        except Exception:
            total_rows = 0

    writer = _open_chunk_writer(
        output_path,
        unified_schema,
        compression="SNAPPY" if output_path.suffix.lower() == ".parquet" else None,
    )
    total_written = 0
    try:
        for reader in readers:
            for batch in _reader_batch_iterator(reader):
                batch_table = pa.Table.from_batches([batch], schema=batch.schema)
                if batch_table.schema != unified_schema:
                    batch = batch_table.cast(unified_schema).to_batches()[0]
                writer.write_batch(batch)
                total_written += len(batch)
                if progress_callback is not None:
                    progress_callback(total_written, total_rows)
    except Exception:
        try:
            writer.close()
        except Exception:
            pass
        try:
            output_path.unlink()
        except (FileNotFoundError, OSError):
            pass
        raise

    writer.close()
    return total_written
