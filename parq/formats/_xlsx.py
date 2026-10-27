"""
XLSX format reader functions.

Reading uses python-calamine. Writing stays on openpyxl in the chunk writer.
"""

import math
import zipfile
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterator, List, Optional

import pyarrow as pa

from parq.formats._common import _InputMetadata, _select_schema

# Schema preview samples this many data rows. Full reads widen the schema when
# a later row does not fit the sample.
XLSX_TYPE_SAMPLE_ROWS = 1000

_XLSX_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63


def _require_openpyxl() -> Any:
    """Import openpyxl lazily and raise actionable error when unavailable."""
    try:
        import openpyxl
    except ImportError as e:
        raise ValueError(
            "XLSX support requires 'openpyxl'. Install it with: pip install 'parq-cli[xlsx]'"
        ) from e
    return openpyxl


def _require_calamine() -> Any:
    """Import python-calamine lazily and raise an actionable error when unavailable."""
    try:
        import python_calamine
    except ImportError as e:
        raise ValueError(
            "XLSX support requires 'python-calamine'. Install it with: pip install 'parq-cli[xlsx]'"
        ) from e
    return python_calamine


def _require_defused_element_tree() -> Any:
    """Import defusedxml lazily; workbook XML is untrusted input."""
    try:
        import defusedxml.ElementTree as defused_et
    except ImportError as e:
        raise ValueError(
            "XLSX support requires 'defusedxml'. Install it with: pip install 'parq-cli[xlsx]'"
        ) from e
    return defused_et


def _active_sheet_index(file_path: Path) -> int:
    """Return the workbook's active sheet index without parsing every cell."""
    ET = _require_defused_element_tree()
    from defusedxml import DefusedXmlException

    try:
        with zipfile.ZipFile(file_path) as workbook_zip:
            root = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
        view = root.find("m:bookViews/m:workbookView", _XLSX_NS)
        if view is None:
            return 0
        return int(view.attrib.get("activeTab", "0"))
    except (
        KeyError,
        ValueError,
        ET.ParseError,
        DefusedXmlException,
        zipfile.BadZipFile,
        OSError,
    ):
        return 0


def _open_xlsx_sheet(file_path: Path, sheet: Optional[str]) -> tuple[Any, Any]:
    """Open one worksheet and return ``(sheet, workbook)``."""
    calamine = _require_calamine()
    workbook = calamine.load_workbook(str(file_path))
    try:
        names = list(workbook.sheet_names)
        if sheet is None:
            index = _active_sheet_index(file_path)
            if index < 0 or index >= len(names):
                index = 0
            return workbook.get_sheet_by_index(index), workbook
        if sheet.isdigit():
            index = int(sheet)
            if index >= len(names):
                raise ValueError(
                    f"Sheet index {index} out of range — workbook has {len(names)} sheet(s)"
                )
            return workbook.get_sheet_by_index(index), workbook
        if sheet not in names:
            raise ValueError(f"Sheet '{sheet}' not found — available sheets: {names}")
        return workbook.get_sheet_by_name(sheet), workbook
    except Exception:
        workbook.close()
        raise


def _normalize_cell(value: Any) -> Any:
    """Match openpyxl empty cells and whole-number floats to the previous types."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        as_int = int(value)
        if _INT64_MIN <= as_int < _INT64_MAX:
            return as_int
    return value


def _normalize_row(row: List[Any], width: int) -> List[Any]:
    """Pad a worksheet row to the header width and normalize each cell."""
    return [_normalize_cell(row[idx]) if idx < len(row) else None for idx in range(width)]


def _normalize_excel_headers(header_row: List[Any]) -> List[str]:
    """Normalize empty/duplicate xlsx header names for Arrow table construction."""
    seen = set()
    normalized = []
    for idx, value in enumerate(header_row, start=1):
        value = _normalize_cell(value)
        base_name = (
            str(value).strip() if value is not None and str(value).strip() else f"column_{idx}"
        )
        name = base_name
        suffix = 2
        while name in seen:
            name = f"{base_name}_{suffix}"
            suffix += 1
        seen.add(name)
        normalized.append(name)
    return normalized


def _infer_arrow_type(value: Any) -> Optional[pa.DataType]:
    """Infer an Arrow type from one already-normalized spreadsheet value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, datetime):
        return pa.timestamp("us")
    if isinstance(value, date):
        return pa.date32()
    if isinstance(value, time):
        return pa.time64("us")
    return pa.string()


def _merge_arrow_types(
    existing_type: Optional[pa.DataType], new_type: Optional[pa.DataType]
) -> Optional[pa.DataType]:
    """Merge two inferred Arrow types conservatively for spreadsheet scanning."""
    if new_type is None:
        return existing_type
    if existing_type is None or existing_type == pa.null():
        return new_type
    if new_type == pa.null() or existing_type == new_type:
        return existing_type
    if pa.types.is_integer(existing_type) and pa.types.is_floating(new_type):
        return new_type
    if pa.types.is_floating(existing_type) and pa.types.is_integer(new_type):
        return existing_type
    return pa.string()


def _coerce_value_for_field(value: Any, field: pa.Field) -> Any:
    """Coerce row values to match the selected Arrow field when needed."""
    if value is None:
        return None
    if pa.types.is_string(field.type) and not isinstance(value, str):
        return str(value)
    return value


def _schema_from_types(headers: List[str], inferred_types: List[Optional[pa.DataType]]) -> pa.Schema:
    """Build a nullable Arrow schema from inferred column types."""
    return pa.schema(
        [
            pa.field(name, inferred_type or pa.null(), nullable=True)
            for name, inferred_type in zip(headers, inferred_types, strict=True)
        ]
    )


def _load_xlsx_sheet(
    file_path: Path, sheet: Optional[str] = None, sample_size: Optional[int] = XLSX_TYPE_SAMPLE_ROWS
) -> tuple[_InputMetadata, Optional[List[List[Any]]]]:
    """Read headers and column types, retaining rows only for a full scan.

    A finite ``sample_size`` stops early and returns no cached rows. ``None``
    reads the sheet once and returns every data row so later iteration does
    not parse the workbook again.
    """
    worksheet, workbook = _open_xlsx_sheet(file_path, sheet)
    try:
        row_iter = worksheet.iter_rows()
        header = next(row_iter, None)
        if header is None:
            return (
                _InputMetadata(
                    headers=tuple(), schema=pa.schema([]), num_rows=0, schema_complete=True
                ),
                [] if sample_size is None else None,
            )

        headers = _normalize_excel_headers(list(header))
        inferred_types: list[Optional[pa.DataType]] = [None] * len(headers)
        sampled_rows = 0
        cached_rows: Optional[List[List[Any]]] = [] if sample_size is None else None

        for row in row_iter:
            values = _normalize_row(list(row), len(headers))
            sampled_rows += 1
            for idx, value in enumerate(values):
                inferred_types[idx] = _merge_arrow_types(
                    inferred_types[idx], _infer_arrow_type(value)
                )
            if cached_rows is not None:
                cached_rows.append(values)
            if sample_size is not None and sampled_rows >= sample_size:
                break

        schema_complete = sample_size is None or sampled_rows < sample_size
        metadata = _InputMetadata(
            headers=tuple(headers),
            schema=_schema_from_types(headers, inferred_types),
            num_rows=sampled_rows if schema_complete else None,
            schema_complete=schema_complete,
        )
        return metadata, cached_rows
    finally:
        workbook.close()


def _scan_xlsx_structure(
    file_path: Path, sample_size: Optional[int] = XLSX_TYPE_SAMPLE_ROWS, sheet: Optional[str] = None
) -> _InputMetadata:
    """Scan xlsx headers and infer column types.

    ``sample_size=None`` reads every data row. A finite sample leaves
    ``schema_complete`` false when the sheet may continue past the window.
    """
    metadata, _rows = _load_xlsx_sheet(file_path, sheet=sheet, sample_size=sample_size)
    return metadata


def _count_xlsx_rows(file_path: Path, sheet: Optional[str] = None) -> int:
    """Count xlsx data rows exactly, excluding the header row."""
    worksheet, workbook = _open_xlsx_sheet(file_path, sheet)
    try:
        row_iter = worksheet.iter_rows()
        header = next(row_iter, None)
        if header is None:
            return 0
        return sum(1 for _ in row_iter)
    finally:
        workbook.close()


def _iter_row_batches(
    rows: Iterator[List[Any]],
    metadata: _InputMetadata,
    columns: Optional[List[str]] = None,
    batch_size: int = 1000,
) -> Iterator[pa.RecordBatch]:
    """Yield Arrow batches from normalized data rows."""
    if not metadata.headers:
        return

    selected_columns = list(metadata.headers) if columns is None else columns
    selected_indices = [metadata.headers.index(column) for column in selected_columns]
    selected_schema = _select_schema(metadata.schema, selected_columns)
    selected_fields = [selected_schema.field(column) for column in selected_columns]
    buffered_rows: list[dict[str, Any]] = []

    for row in rows:
        buffered_rows.append(
            {
                column: _coerce_value_for_field(
                    row[idx] if idx < len(row) else None,
                    field,
                )
                for column, idx, field in zip(selected_columns, selected_indices, selected_fields, strict=True)
            }
        )
        if len(buffered_rows) >= batch_size:
            yield pa.Table.from_pylist(buffered_rows, schema=selected_schema).to_batches()[0]
            buffered_rows = []

    if buffered_rows:
        yield pa.Table.from_pylist(buffered_rows, schema=selected_schema).to_batches()[0]


def _iter_xlsx_batches(
    file_path: Path,
    metadata: _InputMetadata,
    columns: Optional[List[str]] = None,
    batch_size: int = 1000,
    sheet: Optional[str] = None,
) -> Iterator[pa.RecordBatch]:
    """Stream xlsx input as Arrow record batches."""
    worksheet, workbook = _open_xlsx_sheet(file_path, sheet)
    try:
        row_iter = worksheet.iter_rows()
        header = next(row_iter, None)
        if header is None:
            return
        width = len(metadata.headers)
        normalized = (_normalize_row(list(row), width) for row in row_iter)
        yield from _iter_row_batches(normalized, metadata, columns=columns, batch_size=batch_size)
    finally:
        workbook.close()


def _iter_cached_xlsx_rows(
    rows: List[List[Any]],
    metadata: _InputMetadata,
    columns: Optional[List[str]] = None,
    batch_size: int = 1000,
) -> Iterator[pa.RecordBatch]:
    """Yield batches from rows already parsed during a full schema read."""
    yield from _iter_row_batches(iter(rows), metadata, columns=columns, batch_size=batch_size)
