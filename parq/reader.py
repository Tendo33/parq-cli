"""
Tabular file reader module.
Provides functionality to read and inspect parquet/csv/xlsx files.
"""

from pathlib import Path
from typing import Any, Callable, List, Optional

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

# Heuristic multiplier for determining when to use fast path vs optimized path
# Files with <= n * FAST_PATH_ROWS_MULTIPLIER rows use full read + slice (simpler)
# Larger files use row group optimization to reduce I/O
FAST_PATH_ROWS_MULTIPLIER = 10
SUPPORTED_INPUT_FORMATS = {".parquet", ".csv", ".xlsx"}


def _validate_preview_params(n: int, schema: pa.Schema, columns: Optional[List[str]]) -> None:
    """Validate preview parameters shared by multiple readers."""
    if n < 0:
        raise ValueError(f"n must be non-negative, got {n}")

    if columns is not None:
        if len(columns) == 0:
            raise ValueError("columns cannot be empty")
        missing = [c for c in columns if c not in schema.names]
        if missing:
            raise ValueError(f"Columns not found in schema: {missing}")


def _create_empty_table(schema: pa.Schema, columns: Optional[List[str]] = None) -> pa.Table:
    """Create an empty table with source schema or selected columns."""
    fields = schema if columns is None else [schema.field(c) for c in columns]
    arrays = [pa.array([], type=field.type) for field in fields]
    names = [field.name for field in fields]
    return pa.Table.from_arrays(arrays, names=names)


def _table_schema_info(schema: pa.Schema) -> List[dict]:
    """Convert Arrow schema to formatter-compatible schema info structure."""
    return [
        {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        }
        for field in schema
    ]


def _require_openpyxl() -> Any:
    """Import openpyxl lazily and raise actionable error when unavailable."""
    try:
        import openpyxl
    except ImportError as e:
        raise ValueError(
            "XLSX support requires 'openpyxl'. Install it with: pip install 'parq-cli[xlsx]'"
        ) from e
    return openpyxl


def _normalize_excel_headers(header_row: List[Any]) -> List[str]:
    """Normalize empty/duplicate xlsx header names for Arrow table construction."""
    seen = set()
    normalized = []
    for idx, value in enumerate(header_row, start=1):
        base_name = str(value).strip() if value is not None and str(value).strip() else f"column_{idx}"
        name = base_name
        suffix = 2
        while name in seen:
            name = f"{base_name}_{suffix}"
            suffix += 1
        seen.add(name)
        normalized.append(name)
    return normalized


def _to_arrow_array(values: List[Any]) -> pa.Array:
    """Build Arrow array with graceful fallback for mixed-type spreadsheet columns."""
    try:
        return pa.array(values)
    except (pa.ArrowTypeError, pa.ArrowInvalid):
        normalized = [None if v is None else str(v) for v in values]
        return pa.array(normalized, type=pa.string())


def _read_xlsx_table(file_path: Path) -> pa.Table:
    """Read active sheet from xlsx into Arrow table."""
    openpyxl = _require_openpyxl()
    workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    try:
        worksheet = workbook.active
        rows = worksheet.iter_rows(values_only=True)
        header = next(rows, None)
        if header is None:
            return pa.table({})

        headers = _normalize_excel_headers(list(header))
        columns = {name: [] for name in headers}

        for row in rows:
            row_values = list(row) if row is not None else []
            for idx, name in enumerate(headers):
                columns[name].append(row_values[idx] if idx < len(row_values) else None)

        arrays = [_to_arrow_array(columns[name]) for name in headers]
        return pa.Table.from_arrays(arrays, names=headers)
    finally:
        workbook.close()


def _write_xlsx_table(table: pa.Table, output_path: Path) -> None:
    """Write Arrow table to xlsx using openpyxl."""
    openpyxl = _require_openpyxl()
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.append(table.column_names)

    for batch in table.to_batches():
        batch_dict = batch.to_pydict()
        for row_idx in range(len(batch)):
            row = []
            for col_name in table.column_names:
                value = batch_dict[col_name][row_idx]
                if isinstance(value, (dict, list, set, tuple)):
                    row.append(str(value))
                else:
                    row.append(value)
            worksheet.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output_path)


def _resolve_split_shape(
    total_rows: int,
    file_count: Optional[int] = None,
    record_count: Optional[int] = None,
) -> List[int]:
    """Resolve split chunk sizes with existing validation semantics."""
    if file_count is None and record_count is None:
        raise ValueError("Either file_count or record_count must be specified")
    if file_count is not None and record_count is not None:
        raise ValueError("file_count and record_count are mutually exclusive")
    if total_rows == 0:
        raise ValueError("Cannot split empty file")

    if file_count is not None:
        if file_count <= 0:
            raise ValueError("file_count must be positive")
        if file_count > total_rows:
            raise ValueError("file_count cannot exceed total rows")
        base_rows = total_rows // file_count
        remainder = total_rows % file_count
        return [base_rows + (1 if i < remainder else 0) for i in range(file_count)]

    assert record_count is not None
    if record_count <= 0:
        raise ValueError("record_count must be positive")
    full_chunks, remainder = divmod(total_rows, record_count)
    chunk_sizes = [record_count] * full_chunks
    if remainder:
        chunk_sizes.append(remainder)
    return chunk_sizes


def _write_table_by_suffix(table: pa.Table, output_path: Path) -> None:
    """Write Arrow table chunk using extension-specific writer."""
    suffix = output_path.suffix.lower()
    if suffix == ".parquet":
        pq.write_table(table, output_path)
        return
    if suffix == ".csv":
        pacsv.write_csv(table, output_path)
        return
    if suffix == ".xlsx":
        _write_xlsx_table(table, output_path)
        return
    raise ValueError(f"Unsupported output file format: {suffix or '<none>'}")


class ParquetReader:
    """Parquet file reader with metadata inspection capabilities."""

    def __init__(self, file_path: str):
        """
        Initialize ParquetReader with a file path.

        Args:
            file_path: Path to the Parquet file
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        self._parquet_file = pq.ParquetFile(self.file_path)

    @property
    def metadata(self) -> pq.FileMetaData:
        """Get file metadata."""
        return self._parquet_file.metadata

    @property
    def schema(self) -> pa.Schema:
        """Get file schema."""
        return self._parquet_file.schema_arrow

    @property
    def num_rows(self) -> int:
        """Get total number of rows."""
        return self.metadata.num_rows

    @property
    def num_columns(self) -> int:
        """Get total number of columns (logical schema columns)."""
        return len(self.schema)

    @property
    def num_physical_columns(self) -> int:
        """Get total number of physical columns (from metadata)."""
        return self.metadata.num_columns

    @property
    def num_row_groups(self) -> int:
        """Get number of row groups."""
        return self.metadata.num_row_groups

    def get_metadata_dict(self) -> dict:
        """
        Get metadata as a dictionary.

        Returns:
            Dictionary containing file metadata
        """
        metadata_dict = {
            "file_path": str(self.file_path),
            "num_rows": self.num_rows,
            "num_columns": self.num_columns,
        }

        # Add physical columns right after logical columns if different
        if self.num_physical_columns != self.num_columns:
            metadata_dict["num_physical_columns"] = self.num_physical_columns

        # Add file size
        file_size = self.file_path.stat().st_size
        metadata_dict["file_size"] = file_size

        # Add compression information
        compression = self._get_compression_summary()
        if compression is not None:
            metadata_dict["compression"] = compression

        # Add remaining metadata
        metadata_dict.update(
            {
                "num_row_groups": self.num_row_groups,
                "format_version": self.metadata.format_version,
                "serialized_size": self.metadata.serialized_size,
                "created_by": self.metadata.created_by,
            }
        )

        return metadata_dict

    def get_schema_info(self) -> List[dict]:
        """
        Get schema information as a list of column details.

        Returns:
            List of dictionaries with column information
        """
        schema_info = []
        for field in self.schema:
            schema_info.append(
                {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable,
                }
            )
        return schema_info

    def read_head(self, n: int = 5, columns: Optional[List[str]] = None) -> pa.Table:
        """
        Read first n rows.

        Optimized to only read necessary row groups for large files,
        significantly reducing memory usage and improving performance.

        Args:
            n: Number of rows to read (must be >= 0)
            columns: Optional list of column names to read. If None, read all columns.

        Returns:
            PyArrow table with first n rows

        Raises:
            ValueError: If n < 0, or columns is empty, or columns contains invalid names
        """
        # Input validation
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")

        if columns is not None:
            if len(columns) == 0:
                raise ValueError("columns cannot be empty")
            missing = [c for c in columns if c not in self.schema.names]
            if missing:
                raise ValueError(f"Columns not found in schema: {missing}")

        # Guard: zero rows requested - return empty table with correct schema
        if n == 0:
            return self._create_empty_table(columns=columns)

        # Fast path: small files or single row group
        # Avoids overhead of row group calculation
        if self.num_rows <= n * FAST_PATH_ROWS_MULTIPLIER or self.num_row_groups == 1:
            table = self._parquet_file.read(columns=columns)
            return table.slice(0, min(n, self.num_rows))

        # Optimized path: only read necessary row groups
        # For large files, this can reduce I/O by 10-100x
        rows_read = 0
        row_groups = []
        for i in range(self.num_row_groups):
            row_groups.append(i)
            rows_read += self.metadata.row_group(i).num_rows
            if rows_read >= n:
                break

        table = self._parquet_file.read_row_groups(row_groups, columns=columns)
        return table.slice(0, n)

    def read_tail(self, n: int = 5, columns: Optional[List[str]] = None) -> pa.Table:
        """
        Read last n rows.

        Optimized to only read necessary row groups from the end of the file,
        significantly reducing memory usage and improving performance for large files.

        Args:
            n: Number of rows to read (must be >= 0)
            columns: Optional list of column names to read. If None, read all columns.

        Returns:
            PyArrow table with last n rows

        Raises:
            ValueError: If n < 0, or columns is empty, or columns contains invalid names
        """
        # Input validation
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")

        if columns is not None:
            if len(columns) == 0:
                raise ValueError("columns cannot be empty")
            missing = [c for c in columns if c not in self.schema.names]
            if missing:
                raise ValueError(f"Columns not found in schema: {missing}")

        # Guard: zero rows requested - return empty table with correct schema
        if n == 0:
            return self._create_empty_table(columns=columns)

        # Fast path: small files or single row group
        if self.num_rows <= n * FAST_PATH_ROWS_MULTIPLIER or self.num_row_groups == 1:
            table = self._parquet_file.read(columns=columns)
            start = max(0, self.num_rows - n)
            return table.slice(start, n)

        # Optimized path: read from end, only necessary row groups
        # Start from last row group and work backwards
        rows_needed = n
        row_groups = []
        for i in range(self.num_row_groups - 1, -1, -1):
            row_groups.append(i)
            rows_needed -= self.metadata.row_group(i).num_rows
            if rows_needed <= 0:
                break

        row_groups.reverse()
        table = self._parquet_file.read_row_groups(row_groups, columns=columns)
        start = max(0, len(table) - n)
        return table.slice(start, n)

    def read_columns(self, columns: Optional[List[str]] = None) -> pa.Table:
        """
        Read specific columns.

        Args:
            columns: List of column names to read. If None, read all columns.

        Returns:
            PyArrow table with selected columns
        """
        return self._parquet_file.read(columns=columns)

    def split_file(
        self,
        output_pattern: str,
        file_count: Optional[int] = None,
        record_count: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> List[Path]:
        """
        Split parquet file into multiple files.

        Args:
            output_pattern: Output file name pattern (e.g., 'result-%06d.parquet')
            file_count: Number of output files (mutually exclusive with record_count)
            record_count: Number of records per file (mutually exclusive with file_count)
            progress_callback: Optional callback function(current, total) for progress updates

        Returns:
            List of created file paths

        Raises:
            ValueError: If both or neither of file_count/record_count are provided
            IOError: If file write fails
        """

        total_rows = self.num_rows
        chunk_sizes = _resolve_split_shape(total_rows, file_count=file_count, record_count=record_count)
        num_files = len(chunk_sizes)

        # Validate output pattern
        try:
            # Test format string with a sample index
            output_pattern % 0
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid output pattern format: {e}")

        # Prepare output file paths
        output_files = []
        for i in range(num_files):
            output_path = Path(output_pattern % i)
            output_files.append(output_path)

            # Check if file already exists
            if output_path.exists():
                raise FileExistsError(f"Output file already exists: {output_path}")

        compression = self._get_compression_type()
        created_output_paths: List[Path] = []
        current_writer: Optional[pq.ParquetWriter] = None

        def _open_writer(idx: int) -> pq.ParquetWriter:
            path = output_files[idx]
            path.parent.mkdir(parents=True, exist_ok=True)
            created_output_paths.append(path)
            return pq.ParquetWriter(path, self.schema, compression=compression)

        try:
            current_file_idx = 0
            current_file_rows = 0
            target_rows_for_current_file = chunk_sizes[current_file_idx]
            rows_processed = 0
            current_writer = _open_writer(0)

            batch_size = min(10000, max(chunk_sizes))
            for batch in self._parquet_file.iter_batches(batch_size=batch_size):
                batch_rows = len(batch)
                batch_offset = 0

                while batch_offset < batch_rows:
                    rows_remaining_in_file = target_rows_for_current_file - current_file_rows
                    rows_to_write = min(rows_remaining_in_file, batch_rows - batch_offset)

                    if rows_to_write == batch_rows and batch_offset == 0:
                        current_writer.write_batch(batch)
                    else:
                        slice_batch = batch.slice(batch_offset, rows_to_write)
                        current_writer.write_batch(slice_batch)

                    batch_offset += rows_to_write
                    current_file_rows += rows_to_write
                    rows_processed += rows_to_write

                    if progress_callback:
                        progress_callback(rows_processed, total_rows)

                    if (
                        current_file_rows >= target_rows_for_current_file
                        and current_file_idx < num_files - 1
                    ):
                        current_writer.close()
                        current_file_idx += 1
                        current_file_rows = 0
                        target_rows_for_current_file = chunk_sizes[current_file_idx]
                        current_writer = _open_writer(current_file_idx)

            current_writer.close()
            current_writer = None

        except Exception:
            if current_writer is not None:
                try:
                    current_writer.close()
                except Exception:
                    pass

            for output_path in created_output_paths:
                try:
                    output_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    pass

            raise

        return output_files

    def _get_compression_type(self) -> str:
        """
        Get compression type from source file.

        Returns:
            Compression type string (e.g., 'SNAPPY', 'GZIP', 'NONE')
        """
        if self.num_row_groups > 0:
            compression = self.metadata.row_group(0).column(0).compression
            return compression
        return "SNAPPY"  # Default compression

    def _get_compression_summary(self) -> Optional[str]:
        """Get compression summary from all row groups and columns.

        Optimized to scan the first row group fully, then only continue
        scanning subsequent row groups if heterogeneous compression is
        detected. Homogeneous files (the common case) exit after one
        row group, making this O(num_columns) instead of
        O(num_row_groups * num_columns).
        """
        if self.num_row_groups == 0:
            return None

        compressions: set[str] = set()
        first_rg = self.metadata.row_group(0)
        for col_idx in range(first_rg.num_columns):
            compressions.add(first_rg.column(col_idx).compression)

        if len(compressions) <= 1 and self.num_row_groups > 1:
            first_rg_types = compressions.copy()
            for rg_idx in range(1, self.num_row_groups):
                rg = self.metadata.row_group(rg_idx)
                for col_idx in range(rg.num_columns):
                    comp = rg.column(col_idx).compression
                    if comp not in first_rg_types:
                        compressions.add(comp)
                if compressions != first_rg_types:
                    break

        if not compressions:
            return None

        if len(compressions) == 1:
            return compressions.pop()

        return ", ".join(sorted(compressions))

    def _create_empty_table(self, columns: Optional[List[str]] = None) -> pa.Table:
        """Create an empty table with the same schema as the source file."""
        fields = self.schema if columns is None else [self.schema.field(c) for c in columns]
        arrays = [pa.array([], type=field.type) for field in fields]
        names = [field.name for field in fields]
        return pa.Table.from_arrays(arrays, names=names)


class MultiFormatReader:
    """Reader that supports parquet/csv/xlsx with a unified CLI-facing interface."""

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        suffix = self.file_path.suffix.lower()
        if suffix not in SUPPORTED_INPUT_FORMATS:
            supported = ", ".join(sorted(SUPPORTED_INPUT_FORMATS))
            raise ValueError(
                f"Unsupported file format: {suffix or '<none>'}. Supported formats: {supported}"
            )

        self.input_format = suffix.lstrip(".")
        self._parquet_reader: Optional[ParquetReader] = None
        self._table: Optional[pa.Table] = None

        if suffix == ".parquet":
            self._parquet_reader = ParquetReader(file_path)
        elif suffix == ".csv":
            self._table = pacsv.read_csv(self.file_path)
        else:
            self._table = _read_xlsx_table(self.file_path)

    @property
    def schema(self) -> pa.Schema:
        if self._parquet_reader is not None:
            return self._parquet_reader.schema
        assert self._table is not None
        return self._table.schema

    @property
    def num_rows(self) -> int:
        if self._parquet_reader is not None:
            return self._parquet_reader.num_rows
        assert self._table is not None
        return len(self._table)

    @property
    def num_columns(self) -> int:
        if self._parquet_reader is not None:
            return self._parquet_reader.num_columns
        assert self._table is not None
        return self._table.num_columns

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

    def get_metadata_dict(self) -> dict:
        if self._parquet_reader is not None:
            return self._parquet_reader.get_metadata_dict()

        metadata_dict = {
            "file_path": str(self.file_path),
            "input_format": self.input_format,
            "num_rows": self.num_rows,
            "num_columns": self.num_columns,
            "file_size": self.file_path.stat().st_size,
            "num_row_groups": self.num_row_groups,
        }
        return metadata_dict

    def get_schema_info(self) -> List[dict]:
        if self._parquet_reader is not None:
            return self._parquet_reader.get_schema_info()
        return _table_schema_info(self.schema)

    def read_head(self, n: int = 5, columns: Optional[List[str]] = None) -> pa.Table:
        if self._parquet_reader is not None:
            return self._parquet_reader.read_head(n=n, columns=columns)

        _validate_preview_params(n, self.schema, columns)
        if n == 0:
            return _create_empty_table(self.schema, columns=columns)

        table = self.read_columns(columns=columns)
        return table.slice(0, min(n, self.num_rows))

    def read_tail(self, n: int = 5, columns: Optional[List[str]] = None) -> pa.Table:
        if self._parquet_reader is not None:
            return self._parquet_reader.read_tail(n=n, columns=columns)

        _validate_preview_params(n, self.schema, columns)
        if n == 0:
            return _create_empty_table(self.schema, columns=columns)

        table = self.read_columns(columns=columns)
        start = max(0, self.num_rows - n)
        return table.slice(start, n)

    def read_columns(self, columns: Optional[List[str]] = None) -> pa.Table:
        if self._parquet_reader is not None:
            return self._parquet_reader.read_columns(columns=columns)

        assert self._table is not None
        if columns is None:
            return self._table

        missing = [c for c in columns if c not in self.schema.names]
        if missing:
            raise ValueError(f"Columns not found in schema: {missing}")
        return self._table.select(columns)

    def split_file(
        self,
        output_pattern: str,
        file_count: Optional[int] = None,
        record_count: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> List[Path]:
        try:
            output_pattern % 0
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid output pattern format: {e}") from e

        sample_output = Path(output_pattern % 0)
        output_suffix = sample_output.suffix.lower()

        if self._parquet_reader is not None and output_suffix == ".parquet":
            return self._parquet_reader.split_file(
                output_pattern=output_pattern,
                file_count=file_count,
                record_count=record_count,
                progress_callback=progress_callback,
            )

        total_rows = self.num_rows
        chunk_sizes = _resolve_split_shape(
            total_rows, file_count=file_count, record_count=record_count
        )
        num_files = len(chunk_sizes)

        output_files = []
        for i in range(num_files):
            output_path = Path(output_pattern % i)
            output_files.append(output_path)
            if output_path.exists():
                raise FileExistsError(f"Output file already exists: {output_path}")

        source_table = (
            self._table
            if self._table is not None
            else self._parquet_reader.read_columns() if self._parquet_reader is not None else None
        )
        assert source_table is not None

        created_output_paths: List[Path] = []
        rows_processed = 0
        row_offset = 0
        try:
            for output_path, chunk_size in zip(output_files, chunk_sizes):
                chunk = source_table.slice(row_offset, chunk_size)
                row_offset += chunk_size
                output_path.parent.mkdir(parents=True, exist_ok=True)
                created_output_paths.append(output_path)
                _write_table_by_suffix(chunk, output_path)

                rows_processed += len(chunk)
                if progress_callback:
                    progress_callback(rows_processed, total_rows)
        except Exception:
            for created_path in created_output_paths:
                try:
                    created_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
            raise

        return output_files
