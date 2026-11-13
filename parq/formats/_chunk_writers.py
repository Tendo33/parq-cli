"""
Chunk writers and streaming split/write utilities for all output formats.
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from parq.formats._common import _validate_delimiter
from parq.formats._xlsx import _require_openpyxl


def _coerce_output_value(value: Any) -> Any:
    """Normalize nested values before writing them to text-oriented output formats."""
    if isinstance(value, (dict, list, set, tuple)):
        return str(value)
    return value


def _cleanup_split_files(writer: Any, created_paths: List[Path]) -> None:
    """Close a writer (if open) and delete staged temp files on failure."""
    if writer is not None:
        try:
            writer.close()
        except Exception:
            pass
    for path in created_paths:
        try:
            path.unlink()
        except (FileNotFoundError, OSError):
            pass


def _publish_staged_outputs(staged: List[tuple[Path, Path]]) -> None:
    """Move staged split parts onto their final paths after every part is complete."""
    for final_path, temp_path in staged:
        os.replace(temp_path, final_path)


class _CsvChunkWriter:
    """Incremental CSV/TSV writer backed by PyArrow CSVWriter."""

    def __init__(self, output_path: Path, schema: pa.Schema, delimiter: str = ","):
        _validate_delimiter(delimiter)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = pacsv.CSVWriter(
            output_path,
            schema,
            write_options=pacsv.WriteOptions(delimiter=delimiter),
        )

    def write_batch(self, batch: pa.RecordBatch) -> None:
        self._writer.write(batch)

    def close(self) -> None:
        self._writer.close()


class _XlsxChunkWriter:
    """Incremental XLSX writer using openpyxl write-only mode."""

    def __init__(self, output_path: Path, schema: pa.Schema):
        openpyxl = _require_openpyxl()
        self._output_path = output_path
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._workbook = openpyxl.Workbook(write_only=True)
        self._worksheet = self._workbook.create_sheet()
        self._column_names = schema.names
        self._worksheet.append(self._column_names)

    def write_batch(self, batch: pa.RecordBatch) -> None:
        batch_dict = batch.to_pydict()
        for row_idx in range(len(batch)):
            row = [
                _coerce_output_value(batch_dict[col_name][row_idx])
                for col_name in self._column_names
            ]
            self._worksheet.append(row)

    def close(self) -> None:
        self._workbook.save(self._output_path)
        self._workbook.close()


def _open_chunk_writer(
    output_path: Path, schema: pa.Schema, compression: Optional[str] = None
) -> Any:
    """Open incremental writer for supported output suffixes."""
    suffix = output_path.suffix.lower()
    if suffix == ".parquet":
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return pq.ParquetWriter(output_path, schema, compression=compression)
    if suffix == ".csv":
        return _CsvChunkWriter(output_path, schema, delimiter=",")
    if suffix == ".tsv":
        return _CsvChunkWriter(output_path, schema, delimiter="\t")
    if suffix == ".xlsx":
        return _XlsxChunkWriter(output_path, schema)
    raise ValueError(f"Unsupported output file format: {suffix or '<none>'}")


def _resolve_output_files(output_pattern: str, num_files: int, force: bool = False) -> List[Path]:
    """Validate output pattern and preflight all output file paths."""
    try:
        output_pattern % 0
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid output pattern format: {e}") from e

    output_files = [Path(output_pattern % i) for i in range(num_files)]
    if not force:
        for output_path in output_files:
            if output_path.exists():
                raise FileExistsError(f"Output file already exists: {output_path}")
    return output_files


def _validate_output_pattern(output_pattern: str) -> None:
    """Validate a printf-style output pattern."""
    try:
        output_pattern % 0
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid output pattern format: {e}") from e


def _open_validated_output_path(output_pattern: str, index: int, force: bool = False) -> Path:
    """Resolve one output path from a pattern and reject existing targets unless force."""
    output_path = Path(output_pattern % index)
    if output_path.exists() and not force:
        raise FileExistsError(f"Output file already exists: {output_path}")
    return output_path


def _split_batches_to_files(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    output_files: List[Path],
    chunk_sizes: List[int],
    total_rows: int,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    compression: Optional[str] = None,
) -> List[Path]:
    """Stream record batches into split output files with cleanup on failure.

    Parts are written beside the destination and moved into place only after every
    part has been closed, so a failed ``--force`` split leaves existing files alone.
    """
    staged_outputs: List[tuple[Path, Path]] = []
    temp_paths: List[Path] = []
    current_writer: Optional[Any] = None

    def open_staged_writer(final_path: Path) -> Any:
        temp_path = _temporary_output_path(final_path)
        temp_paths.append(temp_path)
        staged_outputs.append((final_path, temp_path))
        return _open_chunk_writer(temp_path, schema, compression=compression)

    try:
        current_file_idx = 0
        current_file_rows = 0
        target_rows_for_current_file = chunk_sizes[current_file_idx]
        rows_processed = 0
        current_writer = open_staged_writer(output_files[current_file_idx])

        for batch in batches:
            batch_offset = 0
            while batch_offset < len(batch):
                rows_remaining_in_file = target_rows_for_current_file - current_file_rows
                rows_to_write = min(rows_remaining_in_file, len(batch) - batch_offset)
                slice_batch = (
                    batch
                    if rows_to_write == len(batch) and batch_offset == 0
                    else batch.slice(batch_offset, rows_to_write)
                )
                current_writer.write_batch(slice_batch)

                batch_offset += rows_to_write
                current_file_rows += rows_to_write
                rows_processed += rows_to_write

                if progress_callback:
                    progress_callback(rows_processed, total_rows)

                if (
                    current_file_rows >= target_rows_for_current_file
                    and current_file_idx < len(output_files) - 1
                ):
                    current_writer.close()
                    current_file_idx += 1
                    current_file_rows = 0
                    target_rows_for_current_file = chunk_sizes[current_file_idx]
                    current_writer = open_staged_writer(output_files[current_file_idx])

        if current_writer is not None:
            current_writer.close()
            current_writer = None
        _publish_staged_outputs(staged_outputs)
    except Exception:
        _cleanup_split_files(current_writer, temp_paths)
        raise

    return output_files


def _stream_split_by_record_count(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    output_pattern: str,
    record_count: int,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    compression: Optional[str] = None,
    force: bool = False,
) -> tuple[List[Path], int]:
    """Split batches in a single pass when chunking by record count."""
    if record_count <= 0:
        raise ValueError("record_count must be positive")

    _validate_output_pattern(output_pattern)
    staged_outputs: List[tuple[Path, Path]] = []
    temp_paths: List[Path] = []
    current_writer: Optional[Any] = None
    current_rows = 0
    rows_processed = 0
    current_file_idx = 0

    try:
        for batch in batches:
            batch_offset = 0
            while batch_offset < len(batch):
                if current_writer is None:
                    output_path = _open_validated_output_path(
                        output_pattern, current_file_idx, force=force
                    )
                    temp_path = _temporary_output_path(output_path)
                    temp_paths.append(temp_path)
                    staged_outputs.append((output_path, temp_path))
                    current_writer = _open_chunk_writer(
                        temp_path, schema, compression=compression
                    )
                    current_rows = 0

                rows_remaining = record_count - current_rows
                rows_to_write = min(rows_remaining, len(batch) - batch_offset)
                slice_batch = (
                    batch
                    if rows_to_write == len(batch) and batch_offset == 0
                    else batch.slice(batch_offset, rows_to_write)
                )
                current_writer.write_batch(slice_batch)
                batch_offset += rows_to_write
                current_rows += rows_to_write
                rows_processed += rows_to_write

                if progress_callback is not None:
                    # Total row count is unknown in this single pass. 0 keeps the
                    # progress bar indeterminate instead of pinning it at 100%.
                    progress_callback(rows_processed, 0)

                if current_rows >= record_count:
                    current_writer.close()
                    current_writer = None
                    current_file_idx += 1

        if current_writer is not None:
            current_writer.close()
            current_writer = None
        if rows_processed == 0:
            raise ValueError("Cannot split empty file")
        _publish_staged_outputs(staged_outputs)
    except Exception:
        _cleanup_split_files(current_writer, temp_paths)
        raise

    return [final_path for final_path, _temp_path in staged_outputs], rows_processed


def _temporary_output_path(output_path: Path) -> Path:
    """Create an empty temp path next to the destination so replace stays on one filesystem.

    The temp file keeps the destination suffix. Writers choose a format from that suffix.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{output_path.name}.", dir=output_path.parent)
    os.close(fd)
    raw_path = Path(tmp_name)
    if not output_path.suffix:
        raw_path.unlink()
        return raw_path
    temporary_path = raw_path.with_name(raw_path.name + output_path.suffix)
    raw_path.rename(temporary_path)
    temporary_path.unlink()
    return temporary_path


def _write_batches_to_output(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    output_path: Path,
    compression: Optional[str] = None,
    force: bool = False,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    total_rows: int = 0,
) -> int:
    """Write a batch iterator to a single output file.

    The destination is replaced only after the temp file is complete, so a failed
    ``--force`` write leaves the previous file in place.
    """
    if output_path.exists() and not force:
        raise FileExistsError(f"Output file already exists: {output_path}")

    tmp_path = _temporary_output_path(output_path)
    writer: Any = None
    try:
        writer = _open_chunk_writer(tmp_path, schema, compression=compression)
        rows_written = 0
        for batch in batches:
            writer.write_batch(batch)
            rows_written += len(batch)
            if progress_callback is not None:
                progress_callback(rows_written, total_rows)
        writer.close()
        writer = None
        os.replace(tmp_path, output_path)
        return rows_written
    except Exception:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        try:
            tmp_path.unlink()
        except (FileNotFoundError, OSError):
            pass
        raise
