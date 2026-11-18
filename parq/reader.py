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

import heapq
import math
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, List, Optional, Union

import pyarrow as pa

from parq.formats import SUPPORTED_INPUT_FORMATS
from parq.formats._chunk_writers import (
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
    _validate_encoding,
    _validate_preview_params,
)
from parq.formats._csv import _iter_csv_batches, _scan_csv_metadata
from parq.formats._parquet import ParquetReader
from parq.formats._xlsx import (
    XLSX_TYPE_SAMPLE_ROWS,
    _count_xlsx_rows,
    _iter_cached_xlsx_rows,
    _iter_xlsx_batches,
    _load_xlsx_sheet,
    _scan_xlsx_structure,
)

__all__ = [
    "SUPPORTED_INPUT_FORMATS",
    "ParquetReader",
    "MultiFormatReader",
    "diff_files",
    "merge_files",
]

# Distinct keys kept in one in-memory diff index. Larger inputs are
# hash-partitioned into temporary Parquet files.
DIFF_MEMORY_ROW_LIMIT = 100_000
# Rows aimed for per partition. Hash partitioning is uneven, so aiming at the
# limit itself leaves most buckets a little over it and every one of them
# partitions again. Half the limit keeps each bucket clear of the threshold.
DIFF_PARTITION_TARGET_ROWS = DIFF_MEMORY_ROW_LIMIT // 2


class MultiFormatReader:
    """Reader that supports parquet/csv/tsv/xlsx with a unified CLI-facing interface."""

    def __init__(
        self,
        file_path: str,
        delimiter: str = ",",
        sheet: Optional[str] = None,
        encoding: str = "utf8",
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

        # --encoding only describes CSV/TSV bytes. Validating it for parquet or
        # xlsx would fail a script that passes it globally.
        self._encoding = _validate_encoding(encoding) if self.input_format == "csv" else encoding

        self._parquet_reader: Optional[ParquetReader] = None
        self._metadata_cache: Optional[_InputMetadata] = None
        self._xlsx_cached_rows: Optional[List[List[Any]]] = None
        self.last_split_total_rows: Optional[int] = None
        self.last_write_total_rows: Optional[int] = None

        if suffix == ".parquet":
            self._parquet_reader = ParquetReader(file_path)

    def close(self) -> None:
        """Release file handles owned by this reader."""
        if self._parquet_reader is not None:
            self._parquet_reader.close()

    def __enter__(self) -> "MultiFormatReader":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

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
                    schema_complete=self._metadata_cache.schema_complete,
                )
            return self._metadata_cache

        if self._metadata_cache is None or (
            include_row_count and self._metadata_cache.num_rows is None
        ):
            self._metadata_cache = _scan_csv_metadata(
                self.file_path,
                include_row_count=include_row_count,
                delimiter=self._delimiter,
                encoding=self._encoding,
            )
        return self._metadata_cache

    def _ensure_xlsx_types_for_full_read(self) -> None:
        """Parse an XLSX sheet once and widen column types before a full read.

        Rows are cached only up to ``DIFF_MEMORY_ROW_LIMIT``. A larger sheet
        keeps the widened schema and is parsed again in batches, so a diff does
        not hold every row of both sides as Python lists.
        """
        if self.input_format != "xlsx":
            return
        if self._xlsx_cached_rows is not None:
            return
        if self._metadata_cache is not None and self._metadata_cache.schema_complete:
            self._xlsx_cached_rows = []
            return
        metadata, rows = _load_xlsx_sheet(
            self.file_path,
            sheet=self._sheet,
            sample_size=None,
            retain_rows_limit=DIFF_MEMORY_ROW_LIMIT,
        )
        self._metadata_cache = metadata
        self._xlsx_cached_rows = rows if rows is not None else []

    def _iter_input_batches(self, columns: Optional[List[str]] = None) -> Iterator[pa.RecordBatch]:
        """Iterate non-parquet input in batches with format branching in one place."""
        metadata = self._load_metadata()
        if self.input_format == "csv":
            yield from _iter_csv_batches(
                self.file_path,
                columns=columns,
                delimiter=self._delimiter,
                encoding=self._encoding,
            )
            return
        if self._xlsx_cached_rows:
            yield from _iter_cached_xlsx_rows(self._xlsx_cached_rows, metadata, columns=columns)
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
        if n > XLSX_TYPE_SAMPLE_ROWS:
            self._ensure_xlsx_types_for_full_read()
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
        if n > 0:
            self._ensure_xlsx_types_for_full_read()
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
        self._ensure_xlsx_types_for_full_read()
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
        self._ensure_xlsx_types_for_full_read()
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
        _reject_output_matching_input(output_path, [self.file_path])
        self._ensure_xlsx_types_for_full_read()
        if self._parquet_reader is not None:
            rows_written = self._parquet_reader.convert_file(
                output_path, columns=columns, force=force, progress_callback=progress_callback
            )
            self.last_write_total_rows = rows_written
            return rows_written

        # CSV row counts require their own scan. Leave the total unknown so
        # convert stays single-pass. XLSX already learned the count while
        # loading the sheet above.
        if progress_callback is not None and self.input_format != "csv":
            total_rows = self.num_rows
        else:
            total_rows = 0
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
        self._ensure_xlsx_types_for_full_read()
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


def _iter_row_tuples(
    reader: MultiFormatReader,
    columns: List[str],
) -> Iterator[tuple[Any, ...]]:
    """Iterate selected rows as tuples in ``columns`` order.

    ``zip`` splits the columns into rows in C, which is far cheaper than a
    dictionary comprehension per row. Dict-based callers convert from this.
    """
    for batch in _reader_batch_iterator(reader, columns=columns):
        batch_dict = batch.to_pydict()
        column_values = [batch_dict[column] for column in columns]
        if not column_values:
            yield from (() for _ in range(len(batch)))
            continue
        yield from zip(*column_values)


def _build_diff_index(
    reader: MultiFormatReader,
    key_columns: List[str],
    selected_columns: List[str],
    comparable_columns: List[str],
    side_name: str,
) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Build a keyed row index for diffing and reject duplicate keys."""
    key_positions = [selected_columns.index(column) for column in key_columns]
    comparable_positions = [selected_columns.index(column) for column in comparable_columns]
    index: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    for row in _iter_row_tuples(reader, selected_columns):
        key = _row_key_at(row, key_positions, key_columns)
        if key in index:
            raise ValueError(
                f"Duplicate key found in {side_name} input: {_render_key(key)}"
            )
        index[key] = tuple(row[position] for position in comparable_positions)
    return index


def _empty_row_diff() -> dict:
    """Return a zeroed row-diff payload without schema fields."""
    return {
        "only_left_count": 0,
        "only_right_count": 0,
        "changed_count": 0,
        "only_left": [],
        "only_right": [],
        "changed_rows": [],
    }


def _render_key(key: tuple[Any, ...]) -> Any:
    """Render a one-column key as a scalar and a composite key as a tuple."""
    rendered = tuple(_render_key_part(part) for part in key)
    return rendered if len(rendered) > 1 else rendered[0]


def _render_key_part(part: Any) -> Any:
    """Show a Decimal NaN sentinel as Decimal NaN in diff output."""
    if isinstance(part, _DecimalNaNKey):
        return Decimal("NaN")
    return part


class _NaNKey(float):
    """Float NaN that compares equal to itself so it can be a diff key.

    ``hash(float("nan"))`` is not stable across temporary objects, so this
    sentinel uses a constant hash and only compares equal to other sentinels.
    """

    def __new__(cls) -> "_NaNKey":
        return float.__new__(cls, "nan")

    def __hash__(self) -> int:
        return 0

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _NaNKey)


_NAN_KEY = _NaNKey()


class _DecimalNaNKey:
    """Decimal NaN that compares equal to itself so it can be a diff key.

    ``Decimal('NaN')`` is not equal to itself, and ``Decimal('sNaN')`` cannot
    be hashed. One sentinel covers both.
    """

    def __hash__(self) -> int:
        return 0

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _DecimalNaNKey)

    def __repr__(self) -> str:
        return "Decimal('NaN')"


_DECIMAL_NAN_KEY = _DecimalNaNKey()


def _normalize_key_part(part: Any) -> Any:
    """Map values that should match as one key onto a stable, hashable form."""
    # Keys are overwhelmingly strings, ints, or None. Testing the exact type
    # first keeps that case to one check instead of a chain of isinstance
    # calls, which cost more than everything else in a large diff.
    kind = type(part)
    if kind is float:
        return _normalize_float_key(part)
    if kind is Decimal:
        return _DECIMAL_NAN_KEY if part.is_nan() else part
    if isinstance(part, float):
        return _normalize_float_key(part)
    if isinstance(part, Decimal):
        return _DECIMAL_NAN_KEY if part.is_nan() else part
    return part


def _normalize_float_key(part: float) -> Any:
    """Fold NaN onto one sentinel and -0.0 onto 0.0."""
    if part != part:
        return _NAN_KEY
    return 0.0 if part == 0 else part


def _row_key(row: dict[str, Any], key_columns: List[str]) -> tuple[Any, ...]:
    """Build a hashable key, treating NaN and signed zero as one value."""
    parts = []
    for column in key_columns:
        part = _normalize_key_part(row[column])
        if isinstance(part, (list, dict)):
            kind = "list" if isinstance(part, list) else "struct"
            raise ValueError(f"Key column '{column}' contains a {kind}, which cannot be a key")
        parts.append(part)
    return tuple(parts)


def _row_key_at(
    row: tuple[Any, ...],
    key_positions: List[int],
    key_columns: List[str],
) -> tuple[Any, ...]:
    """Build a hashable key from a row tuple, given the key positions."""
    parts = []
    for position, column in zip(key_positions, key_columns, strict=True):
        part = _normalize_key_part(row[position])
        if part.__class__ is list or part.__class__ is dict:
            kind = "list" if part.__class__ is list else "struct"
            raise ValueError(f"Key column '{column}' contains a {kind}, which cannot be a key")
        parts.append(part)
    return tuple(parts)


def _key_part_sort_token(part: Any) -> tuple[Any, ...]:
    """Order one key part without asking Python to compare unrelated types.

    Null sorts first. Decimal NaN is kept out of Decimal comparison, which
    raises ``InvalidOperation``. ``datetime`` is checked before ``date``
    because a datetime is a date.
    """
    if part is None:
        return (0,)
    if isinstance(part, float) and math.isnan(part):
        return (1,)
    if isinstance(part, bool):
        return (2, part)
    if isinstance(part, int):
        return (3, part)
    if isinstance(part, float):
        return (4, part)
    if isinstance(part, _DecimalNaNKey) or (isinstance(part, Decimal) and part.is_nan()):
        return (5,)
    if isinstance(part, Decimal):
        return (6, part)
    if isinstance(part, str):
        return (7, part)
    if isinstance(part, datetime):
        return (8, part)
    if isinstance(part, date):
        return (9, part)
    if isinstance(part, time):
        return (10, part)
    if isinstance(part, (bytes, bytearray)):
        return (11, bytes(part))
    return (12, type(part).__name__, repr(part))


def _sorted_diff_keys(
    keys: Iterable[tuple[Any, ...]], limit: Optional[int] = None
) -> list[tuple[Any, ...]]:
    """Return diff keys in a stable order, including nulls and Decimal NaN.

    A limit uses ``heapq.nsmallest``, so an in-memory index of 100,000 keys is
    not sorted in full to show the first few.
    """

    def sort_token(part_key: tuple[Any, ...]) -> tuple[Any, ...]:
        return tuple(_key_part_sort_token(part) for part in part_key)

    if limit is not None:
        return heapq.nsmallest(limit, keys, key=sort_token)
    return sorted(keys, key=sort_token)


def _same_value(left: Any, right: Any) -> bool:
    """Compare two cell values, treating NaN as equal to NaN."""
    if left is right:
        return True
    if isinstance(left, float) and isinstance(right, float):
        if math.isnan(left) and math.isnan(right):
            return True
        return left == right
    if isinstance(left, Decimal) and isinstance(right, Decimal):
        if left.is_nan() and right.is_nan():
            return True
        return left == right
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return False
        return all(_same_value(left[key], right[key]) for key in left)
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return False
        return all(_same_value(item, other) for item, other in zip(left, right))
    if isinstance(left, tuple) and isinstance(right, tuple):
        if len(left) != len(right):
            return False
        return all(_same_value(item, other) for item, other in zip(left, right))
    if _is_widened_text(left, right) or _is_widened_text(right, left):
        return True
    return left == right


def _row_dict(columns: List[str], values: tuple[Any, ...]) -> dict[str, Any]:
    """Render a comparable-values tuple as the dict the diff payload reports."""
    return dict(zip(columns, values))


def _same_row_values(left: tuple[Any, ...], right: tuple[Any, ...]) -> bool:
    """Compare two comparable-value tuples with NaN treated as equal."""
    return all(_same_value(a, b) for a, b in zip(left, right))


def _is_widened_text(value: Any, text: Any) -> bool:
    """Return whether text is the string XLSX stores when it widens this value.

    Widening calls ``str(value)``. That text is the same cell. ``"01"``,
    ``"1.0"`` for an integer, and ``"note"`` stay different. This does not
    affect keys; those still use ``_normalize_key_part``.
    """
    if not isinstance(text, str) or isinstance(value, str) or value is None:
        return False
    if isinstance(value, bool):
        return text == str(value)
    if isinstance(value, Decimal):
        if value.is_nan():
            return False
        return text == str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return False
        return text == str(value)
    if isinstance(value, int) or isinstance(value, (datetime, date, time)):
        return text == str(value)
    return False


def _stable_bucket(key: tuple[Any, ...], num_buckets: int, salt: int) -> int:
    """Hash a key into a bucket without depending on PYTHONHASHSEED."""
    import zlib

    # ``key`` already came from ``_row_key``, so its parts are normalized and
    # normalizing again is pure work.
    payload = repr((salt, key)).encode("utf-8", errors="backslashreplace")
    return zlib.crc32(payload) % num_buckets


def _diff_rows_in_memory(
    left_reader: Optional[MultiFormatReader],
    right_reader: Optional[MultiFormatReader],
    key_columns: List[str],
    selected_columns: List[str],
    comparable_columns: List[str],
    limit: int,
    summary_only: bool,
) -> dict:
    """Diff two already-bounded inputs by indexing the smaller side."""
    if left_reader is None and right_reader is None:
        return _empty_row_diff()
    if left_reader is None:
        index_left = True
        indexed_reader = None
        streamed_reader = right_reader
    elif right_reader is None:
        index_left = False
        indexed_reader = None
        streamed_reader = left_reader
    else:
        index_left = left_reader.num_rows <= right_reader.num_rows
        indexed_reader = left_reader if index_left else right_reader
        streamed_reader = right_reader if index_left else left_reader

    assert streamed_reader is not None
    indexed_side_name = "left" if index_left else "right"
    streamed_side_name = "right" if index_left else "left"
    if indexed_reader is None:
        indexed_rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    else:
        indexed_rows = _build_diff_index(
            indexed_reader,
            key_columns,
            selected_columns,
            comparable_columns,
            indexed_side_name,
        )

    key_positions = [selected_columns.index(column) for column in key_columns]
    comparable_positions = [selected_columns.index(column) for column in comparable_columns]
    missing_from_indexed: List[tuple[Any, ...]] = []
    missing_from_indexed_count = 0
    changed_rows = []
    changed_count = 0
    seen_streamed_keys: set[tuple[Any, ...]] = set()

    for row in _iter_row_tuples(streamed_reader, selected_columns):
        key = _row_key_at(row, key_positions, key_columns)
        if key in seen_streamed_keys:
            raise ValueError(
                f"Duplicate key found in {streamed_side_name} input: {_render_key(key)}"
            )
        seen_streamed_keys.add(key)

        streamed_values = tuple(row[position] for position in comparable_positions)
        if key not in indexed_rows:
            missing_from_indexed_count += 1
            if not summary_only and len(missing_from_indexed) < limit:
                missing_from_indexed.append(key)
            continue

        indexed_values = indexed_rows.pop(key)
        # Tuple equality settles the common case where every compared cell is
        # identical; the per-cell comparison only runs when something differs,
        # where it still has to treat NaN as equal to NaN.
        if streamed_values != indexed_values and not _same_row_values(
            streamed_values, indexed_values
        ):
            changed_count += 1
            if index_left:
                left_values = _row_dict(comparable_columns, indexed_values)
                right_values = _row_dict(comparable_columns, streamed_values)
            else:
                left_values = _row_dict(comparable_columns, streamed_values)
                right_values = _row_dict(comparable_columns, indexed_values)
            if not summary_only and len(changed_rows) < limit:
                changed_rows.append(
                    {
                        "key": _render_key(key),
                        "left": left_values,
                        "right": right_values,
                    }
                )

    remaining_indexed_count = len(indexed_rows)
    if not summary_only:
        remaining_indexed_keys = _sorted_diff_keys(indexed_rows, limit=limit)
        missing_from_indexed = _sorted_diff_keys(missing_from_indexed)
        only_left_keys = remaining_indexed_keys if index_left else missing_from_indexed
        only_right_keys = missing_from_indexed if index_left else remaining_indexed_keys
    else:
        only_left_keys = []
        only_right_keys = []

    return {
        "only_left_count": (
            remaining_indexed_count if index_left else missing_from_indexed_count
        ),
        "only_right_count": (
            missing_from_indexed_count if index_left else remaining_indexed_count
        ),
        "changed_count": changed_count,
        "only_left": [{"key": _render_key(key)} for key in only_left_keys[:limit]],
        "only_right": [{"key": _render_key(key)} for key in only_right_keys[:limit]],
        "changed_rows": [] if summary_only else changed_rows[:limit],
    }


def _spill_reader_to_buckets(
    reader: MultiFormatReader,
    selected_columns: List[str],
    key_columns: List[str],
    directory: Path,
    side: str,
    num_buckets: int,
    salt: int,
) -> List[Optional[Path]]:
    """Write selected rows into hash buckets without holding every key."""
    import pyarrow.parquet as pq

    paths: List[Optional[Path]] = [None] * num_buckets
    writers: List[Any] = []
    writer_for_bucket: List[Any] = [None] * num_buckets
    try:
        key_positions = [selected_columns.index(column) for column in key_columns]
        for batch in _reader_batch_iterator(reader, columns=selected_columns):
            if len(batch) == 0:
                continue
            batch_dict = batch.to_pydict()
            key_values = [batch_dict[column] for column in selected_columns]
            bucket_indices: List[List[int]] = [[] for _ in range(num_buckets)]
            append = [indices.append for indices in bucket_indices]
            if len(key_positions) == 1:
                position = key_positions[0]
                normalize = _normalize_key_part
                for row_idx, value in enumerate(key_values[position]):
                    part = normalize(value)
                    if part.__class__ is list or part.__class__ is dict:
                        kind = "list" if part.__class__ is list else "struct"
                        raise ValueError(
                            f"Key column '{key_columns[0]}' contains a {kind}, "
                            "which cannot be a key"
                        )
                    append[_stable_bucket((part,), num_buckets, salt)](row_idx)
            else:
                for row_idx in range(len(batch)):
                    key = _row_key_at(
                        tuple(values[row_idx] for values in key_values),
                        key_positions,
                        key_columns,
                    )
                    append[_stable_bucket(key, num_buckets, salt)](row_idx)
            for bucket, indices in enumerate(bucket_indices):
                if not indices:
                    continue
                taken = batch.take(pa.array(indices, type=pa.int64()))
                writer = writer_for_bucket[bucket]
                if writer is None:
                    output_path = directory / f"{side}-{bucket}.parquet"
                    writer = pq.ParquetWriter(output_path, taken.schema)
                    writer_for_bucket[bucket] = writer
                    writers.append(writer)
                    paths[bucket] = output_path
                writer.write_batch(taken)
        for writer in writers:
            writer.close()
        writers.clear()
        return paths
    finally:
        for writer in writers:
            try:
                writer.close()
            except Exception:
                pass


def _merge_row_diffs(parts: List[dict], limit: int, summary_only: bool) -> dict:
    """Sum partitioned diff counts and keep a bounded sample."""
    merged = _empty_row_diff()
    for part in parts:
        merged["only_left_count"] += part["only_left_count"]
        merged["only_right_count"] += part["only_right_count"]
        merged["changed_count"] += part["changed_count"]
        if summary_only:
            continue
        merged["only_left"].extend(part["only_left"])
        merged["only_right"].extend(part["only_right"])
        merged["changed_rows"].extend(part["changed_rows"])
    if summary_only:
        return merged
    # Each bucket sampled its own smallest `limit` keys, so the merge kept one
    # bucket's whole sample and dropped the rest. Re-sort the union to keep the
    # smallest keys overall, which is what an unpartitioned diff reports.
    merged["only_left"] = _sort_rendered_keys(merged["only_left"], limit)
    merged["only_right"] = _sort_rendered_keys(merged["only_right"], limit)
    merged["changed_rows"] = merged["changed_rows"][:limit]
    return merged


def _sort_rendered_keys(rows: List[dict[str, Any]], limit: int) -> List[dict[str, Any]]:
    """Order ``{"key": ...}`` rows by the diff's key ordering and trim to ``limit``."""
    if len(rows) <= 1:
        return rows[:limit]

    def sort_token(row: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
        key = row["key"]
        parts = key if isinstance(key, tuple) else (key,)
        return tuple(_key_part_sort_token(part) for part in parts)

    if len(rows) > limit:
        return heapq.nsmallest(limit, rows, key=sort_token)
    return sorted(rows, key=sort_token)


def _diff_rows_partitioned(
    left_reader: Optional[MultiFormatReader],
    right_reader: Optional[MultiFormatReader],
    key_columns: List[str],
    selected_columns: List[str],
    comparable_columns: List[str],
    limit: int,
    summary_only: bool,
    salt: int,
    depth: int,
    parent_rows: int,
) -> dict:
    """Split both sides by key hash and diff one bucket at a time."""
    import math
    import tempfile

    num_buckets = min(32, max(2, math.ceil(parent_rows / DIFF_PARTITION_TARGET_ROWS)))
    with tempfile.TemporaryDirectory(prefix="parq-diff-") as tmp_dir:
        directory = Path(tmp_dir)
        if left_reader is None:
            left_paths: List[Optional[Path]] = [None] * num_buckets
        else:
            left_paths = _spill_reader_to_buckets(
                left_reader,
                selected_columns,
                key_columns,
                directory,
                "left",
                num_buckets,
                salt,
            )
        if right_reader is None:
            right_paths: List[Optional[Path]] = [None] * num_buckets
        else:
            right_paths = _spill_reader_to_buckets(
                right_reader,
                selected_columns,
                key_columns,
                directory,
                "right",
                num_buckets,
                salt,
            )

        parts = []
        open_readers: List[MultiFormatReader] = []
        try:
            for bucket in range(num_buckets):
                left_path = left_paths[bucket]
                right_path = right_paths[bucket]
                if left_path is None and right_path is None:
                    continue
                bucket_left = MultiFormatReader(str(left_path)) if left_path is not None else None
                bucket_right = (
                    MultiFormatReader(str(right_path)) if right_path is not None else None
                )
                if bucket_left is not None:
                    open_readers.append(bucket_left)
                if bucket_right is not None:
                    open_readers.append(bucket_right)
                bucket_rows = max(
                    0 if bucket_left is None else bucket_left.num_rows,
                    0 if bucket_right is None else bucket_right.num_rows,
                )
                if bucket_rows >= parent_rows and bucket_rows > DIFF_MEMORY_ROW_LIMIT:
                    part = _diff_rows_in_memory(
                        bucket_left,
                        bucket_right,
                        key_columns,
                        selected_columns,
                        comparable_columns,
                        limit,
                        summary_only,
                    )
                else:
                    part = _diff_rows_bounded(
                        bucket_left,
                        bucket_right,
                        key_columns,
                        selected_columns,
                        comparable_columns,
                        limit,
                        summary_only,
                        salt=salt + 1 + bucket,
                        depth=depth + 1,
                    )
                parts.append(part)
            return _merge_row_diffs(parts, limit, summary_only)
        finally:
            for open_reader in open_readers:
                open_reader.close()


def _diff_rows_bounded(
    left_reader: Optional[MultiFormatReader],
    right_reader: Optional[MultiFormatReader],
    key_columns: List[str],
    selected_columns: List[str],
    comparable_columns: List[str],
    limit: int,
    summary_only: bool,
    salt: int = 0,
    depth: int = 0,
) -> dict:
    """Diff rows in memory, or partition when one side exceeds the row limit."""
    left_rows = 0 if left_reader is None else left_reader.num_rows
    right_rows = 0 if right_reader is None else right_reader.num_rows
    largest = max(left_rows, right_rows)
    if largest <= DIFF_MEMORY_ROW_LIMIT or depth >= 8:
        return _diff_rows_in_memory(
            left_reader,
            right_reader,
            key_columns,
            selected_columns,
            comparable_columns,
            limit,
            summary_only,
        )
    return _diff_rows_partitioned(
        left_reader,
        right_reader,
        key_columns,
        selected_columns,
        comparable_columns,
        limit,
        summary_only,
        salt=salt,
        depth=depth,
        parent_rows=largest,
    )


def _duplicated_column_names(names: List[str]) -> set[str]:
    """Return names that occur more than once."""
    return {name for name in names if names.count(name) > 1}


def _schema_type_mismatches(
    left_schema: pa.Schema, right_schema: pa.Schema, shared_columns: List[str]
) -> List[dict]:
    """Pair shared columns in order so a repeated name is not looked up with schema.field."""
    mismatches = []
    for name in dict.fromkeys(shared_columns):
        left_fields = [field for field in left_schema if field.name == name]
        right_fields = [field for field in right_schema if field.name == name]
        multiple = len(left_fields) > 1 or len(right_fields) > 1
        for index, (left_field, right_field) in enumerate(zip(left_fields, right_fields)):
            if str(left_field.type) == str(right_field.type):
                continue
            mismatch = {
                "column": name,
                "left_type": str(left_field.type),
                "right_type": str(right_field.type),
            }
            if multiple:
                mismatch["occurrence"] = index
            mismatches.append(mismatch)
        if len(left_fields) != len(right_fields):
            mismatches.append(
                {
                    "column": name,
                    "left_type": f"{len(left_fields)} occurrences",
                    "right_type": f"{len(right_fields)} occurrences",
                }
            )
    return mismatches


def diff_files(
    left_path: Union[str, Path],
    right_path: Union[str, Path],
    key_columns: List[str],
    columns: Optional[List[str]] = None,
    limit: int = 20,
    summary_only: bool = False,
    delimiter: str = ",",
    sheet: Optional[str] = None,
    encoding: str = "utf8",
) -> dict:
    """Compute a keyed diff summary for two supported tabular files."""
    if limit < 0:
        raise ValueError(f"limit must be non-negative, got {limit}")
    with (
        MultiFormatReader(
            str(left_path), delimiter=delimiter, sheet=sheet, encoding=encoding
        ) as left_reader,
        MultiFormatReader(
            str(right_path), delimiter=delimiter, sheet=sheet, encoding=encoding
        ) as right_reader,
    ):
        # XLSX schema inspection samples the first rows. A diff needs the full
        # sheet so a later value can widen a column before types are compared.
        left_reader._ensure_xlsx_types_for_full_read()
        right_reader._ensure_xlsx_types_for_full_read()

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
        schema_type_mismatches = _schema_type_mismatches(
            left_reader.schema, right_reader.schema, shared_columns
        )
        duplicated_names = _duplicated_column_names(left_names) | _duplicated_column_names(
            right_names
        )
        if columns is None:
            ambiguous = [
                name
                for name in dict.fromkeys(shared_columns)
                if name not in key_columns and name in duplicated_names
            ]
            if ambiguous:
                raise ValueError(
                    "Column names are duplicated and cannot be compared by name: "
                    f"{ambiguous}. Pass --columns with the unique columns to compare."
                )

        comparable_columns = columns or [
            name for name in left_names if name in right_names and name not in key_columns
        ]
        selected_columns = list(dict.fromkeys(key_columns + comparable_columns))
        row_diff = _diff_rows_bounded(
            left_reader,
            right_reader,
            key_columns,
            selected_columns,
            comparable_columns,
            limit,
            summary_only,
        )
        left_rows = left_reader.num_rows
        right_rows = right_reader.num_rows

    return {
        "row_count_delta": left_rows - right_rows,
        "only_left_count": row_diff["only_left_count"],
        "only_right_count": row_diff["only_right_count"],
        "changed_count": row_diff["changed_count"],
        "schema_only_left": (
            [] if summary_only else [{"column": name} for name in schema_only_left[:limit]]
        ),
        "schema_only_right": (
            [] if summary_only else [{"column": name} for name in schema_only_right[:limit]]
        ),
        "schema_type_mismatches": schema_type_mismatches[:limit],
        "only_left": [] if summary_only else row_diff["only_left"],
        "only_right": [] if summary_only else row_diff["only_right"],
        "changed_rows": [] if summary_only else row_diff["changed_rows"],
    }


def _reject_output_matching_input(
    output_path: Path, input_paths: List[Union[str, Path]]
) -> None:
    """Refuse to write onto an input file (``--force`` would delete it first)."""
    if not output_path.exists():
        return
    for input_path in input_paths:
        try:
            same = output_path.samefile(input_path)
        except OSError:
            continue
        if same:
            raise ValueError(f"Output file must not be one of the inputs: {output_path}")


def merge_files(
    input_paths: List[Union[str, Path]],
    output_path: Union[str, Path],
    force: bool = False,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    delimiter: str = ",",
    sheet: Optional[str] = None,
    encoding: str = "utf8",
) -> int:
    """Merge multiple compatible inputs into a single output file."""
    if len(input_paths) == 0:
        raise ValueError("At least one input file must be provided")

    output_path = Path(output_path)
    _reject_output_matching_input(output_path, input_paths)
    if output_path.exists() and not force:
        raise FileExistsError(f"Output file already exists: {output_path}")

    readers: List[MultiFormatReader] = []
    try:
        for path in input_paths:
            readers.append(
                MultiFormatReader(str(path), delimiter=delimiter, sheet=sheet, encoding=encoding)
            )
        for reader in readers:
            reader._ensure_xlsx_types_for_full_read()
        schemas = [reader.schema for reader in readers]
        try:
            unified_schema = pa.unify_schemas(schemas)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            raise ValueError(f"Incompatible schemas: {e}") from e

        # Compute total rows for progress reporting (0 = unknown)
        total_rows = 0
        if progress_callback is not None:
            try:
                total_rows = sum(reader.num_rows for reader in readers)
            except Exception:
                total_rows = 0

        def batches() -> Iterator[pa.RecordBatch]:
            for reader in readers:
                for batch in _reader_batch_iterator(reader):
                    if batch.schema != unified_schema:
                        batch = (
                            pa.Table.from_batches([batch], schema=batch.schema)
                            .cast(unified_schema)
                            .to_batches()[0]
                        )
                    yield batch

        return _write_batches_to_output(
            batches(),
            unified_schema,
            output_path,
            compression="SNAPPY" if output_path.suffix.lower() == ".parquet" else None,
            force=force,
            progress_callback=progress_callback,
            total_rows=total_rows,
        )
    finally:
        for reader in readers:
            reader.close()
