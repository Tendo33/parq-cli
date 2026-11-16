"""
CSV format reader functions.
"""

from pathlib import Path
from typing import Iterator, List, Optional

import pyarrow as pa
import pyarrow.csv as pacsv

from parq.formats._common import _InputMetadata, _validate_delimiter, _validate_encoding


def _csv_read_options(encoding: str) -> pacsv.ReadOptions:
    """Build CSV read options after the encoding name has been checked.

    Block size stays at PyArrow's 1 MiB default. The streaming reader infers
    column types from the first block, so a larger block changes ``schema``
    and can turn a later conversion error into a successful string column.
    """
    return pacsv.ReadOptions(encoding=_validate_encoding(encoding))


def _iter_csv_batches(
    file_path: Path,
    columns: Optional[List[str]] = None,
    delimiter: str = ",",
    encoding: str = "utf8",
) -> Iterator[pa.RecordBatch]:
    """Stream CSV input as Arrow record batches."""
    _validate_delimiter(delimiter)
    parse_options = pacsv.ParseOptions(delimiter=delimiter)
    read_options = _csv_read_options(encoding)
    convert_options = None
    if columns is not None:
        convert_options = pacsv.ConvertOptions(include_columns=columns)
    yield from pacsv.open_csv(
        file_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )


def _scan_csv_metadata(
    file_path: Path,
    include_row_count: bool = False,
    delimiter: str = ",",
    encoding: str = "utf8",
) -> _InputMetadata:
    """Scan CSV schema eagerly and count rows only when required."""
    _validate_delimiter(delimiter)
    parse_options = pacsv.ParseOptions(delimiter=delimiter)
    reader = pacsv.open_csv(
        file_path, read_options=_csv_read_options(encoding), parse_options=parse_options
    )
    schema = reader.schema
    num_rows = sum(len(batch) for batch in reader) if include_row_count else None
    return _InputMetadata(headers=tuple(schema.names), schema=schema, num_rows=num_rows)
