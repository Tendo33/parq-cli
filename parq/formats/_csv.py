"""
CSV format reader functions.
"""

from pathlib import Path
from typing import Iterator, List, Optional

import pyarrow as pa
import pyarrow.csv as pacsv

from parq.formats._common import _InputMetadata


def _iter_csv_batches(
    file_path: Path,
    columns: Optional[List[str]] = None,
    delimiter: str = ",",
) -> Iterator[pa.RecordBatch]:
    """Stream CSV input as Arrow record batches."""
    parse_options = pacsv.ParseOptions(delimiter=delimiter)
    convert_options = None
    if columns is not None:
        convert_options = pacsv.ConvertOptions(include_columns=columns)
    yield from pacsv.open_csv(
        file_path, parse_options=parse_options, convert_options=convert_options
    )


def _scan_csv_metadata(
    file_path: Path, include_row_count: bool = False, delimiter: str = ","
) -> _InputMetadata:
    """Scan CSV schema eagerly and count rows only when required."""
    parse_options = pacsv.ParseOptions(delimiter=delimiter)
    reader = pacsv.open_csv(file_path, parse_options=parse_options)
    schema = reader.schema
    num_rows = sum(len(batch) for batch in reader) if include_row_count else None
    return _InputMetadata(headers=tuple(schema.names), schema=schema, num_rows=num_rows)
