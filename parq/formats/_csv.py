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


def _encoding_error(detail: str, encoding: str) -> ValueError:
    """Build the error for a CSV column the chosen encoding cannot read."""
    return ValueError(
        f"Could not decode {detail} as {encoding}. "
        "Pass --encoding with the file's text encoding."
    )


def _nul_encoding_error(encoding: str) -> ValueError:
    """Build the error for a text file whose bytes are interleaved with NUL."""
    return ValueError(
        f"Could not decode the file as {encoding}. "
        "The input contains NUL bytes, which usually means UTF-16. "
        "Pass --encoding utf-16, utf-16-le, or utf-16-be."
    )


def _csv_column_detail(message: str, schema: Optional[pa.Schema]) -> str:
    """Map PyArrow's ``column #N`` marker to a column name when one is known."""
    marker = "column #"
    start = message.find(marker)
    if start == -1 or schema is None:
        return "a column"
    digits: list[str] = []
    for char in message[start + len(marker) :]:
        if not char.isdigit():
            break
        digits.append(char)
    if not digits:
        return "a column"
    index = int("".join(digits))
    if index >= len(schema.names):
        return f"column {index + 1}"
    return f"column {schema.names[index]!r}"


def _parse_error_tail(message: str) -> Optional[str]:
    """Return the record PyArrow echoes after a column-count error.

    PyArrow prints ``CSV parse error: Expected N columns, got M in
    column name: <record>``. Only the echoed record is needed, so the last
    ``": "`` is the split point. Reworded messages only drop the UTF-16
    hint; the parse error itself still surfaces.
    """
    if "CSV parse error:" not in message or "columns" not in message:
        return None
    _prefix, separator, tail = message.rpartition(": ")
    if separator == "":
        return None
    return tail


def _translate_csv_arrow_error(
    exc: pa.ArrowInvalid, schema: Optional[pa.Schema], encoding: str
) -> None:
    """Re-raise decode failures as encoding errors and leave parse errors alone.

    The conversion error is the phrase PyArrow uses for a bad byte in a
    string column. A column-count error only means UTF-16 when the echoed
    record is entirely NUL. A short row keeps the parse error when its text
    contains ``invalid UTF8`` or a NUL next to other bytes.

    Both phrases are PyArrow 18-23 message text. A future reword drops the
    hint and re-raises the original ``ArrowInvalid``; it never turns a parse
    error into a wrong encoding claim.
    """
    message = str(exc)
    if "CSV conversion error to string: invalid UTF8 data" in message:
        raise _encoding_error(_csv_column_detail(message, schema), encoding) from exc
    tail = _parse_error_tail(message)
    if tail is not None and tail != "" and all(char == "\x00" for char in tail):
        raise _nul_encoding_error(encoding) from exc
    raise exc


def _reject_undecoded_csv_columns(schema: pa.Schema, encoding: str) -> None:
    """Fail when a CSV column came back as bytes or its header is not text.

    PyArrow keeps a column it cannot decode as binary and still returns the
    row. That hides a wrong ``--encoding``. A header PyArrow stored as raw
    bytes raises UnicodeDecodeError when its name is read. UTF-16 without a
    BOM can come back as a string name full of NUL bytes.
    """
    names: list[str] = []
    seen: set[str] = set()
    for index, field in enumerate(schema):
        try:
            name = field.name
        except UnicodeDecodeError as exc:
            raise _encoding_error(f"column {index + 1} header", encoding) from exc
        if "\x00" in name:
            raise _nul_encoding_error(encoding)
        if not (pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type)):
            continue
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    if not names:
        return
    label = "column" if len(names) == 1 else "columns"
    shown = ", ".join(repr(name) for name in names)
    raise _encoding_error(f"{label} {shown}", encoding)


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
    try:
        reader = pacsv.open_csv(
            file_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )
    except pa.ArrowInvalid as exc:
        _translate_csv_arrow_error(exc, None, read_options.encoding)
    _reject_undecoded_csv_columns(reader.schema, read_options.encoding)
    try:
        yield from reader
    except pa.ArrowInvalid as exc:
        _translate_csv_arrow_error(exc, reader.schema, read_options.encoding)


def _scan_csv_metadata(
    file_path: Path,
    include_row_count: bool = False,
    delimiter: str = ",",
    encoding: str = "utf8",
) -> _InputMetadata:
    """Scan CSV schema eagerly and count rows only when required."""
    _validate_delimiter(delimiter)
    parse_options = pacsv.ParseOptions(delimiter=delimiter)
    read_options = _csv_read_options(encoding)
    try:
        reader = pacsv.open_csv(
            file_path, read_options=read_options, parse_options=parse_options
        )
    except pa.ArrowInvalid as exc:
        _translate_csv_arrow_error(exc, None, read_options.encoding)
    schema = reader.schema
    _reject_undecoded_csv_columns(schema, read_options.encoding)
    num_rows = None
    if include_row_count:
        try:
            num_rows = sum(len(batch) for batch in reader)
        except pa.ArrowInvalid as exc:
            _translate_csv_arrow_error(exc, schema, read_options.encoding)
    return _InputMetadata(headers=tuple(schema.names), schema=schema, num_rows=num_rows)
