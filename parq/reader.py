"""
Parquet file reader module.
Provides functionality to read and inspect Parquet files.
"""

from pathlib import Path
from typing import Callable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

# Heuristic multiplier for determining when to use fast path vs optimized path
# Files with <= n * FAST_PATH_ROWS_MULTIPLIER rows use full read + slice (simpler)
# Larger files use row group optimization to reduce I/O
FAST_PATH_ROWS_MULTIPLIER = 10


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

        # Validate parameters
        if file_count is None and record_count is None:
            raise ValueError("Either file_count or record_count must be specified")
        if file_count is not None and record_count is not None:
            raise ValueError("file_count and record_count are mutually exclusive")

        total_rows = self.num_rows
        if total_rows == 0:
            raise ValueError("Cannot split empty file")

        # Calculate number of files and rows per file
        if file_count is not None:
            if file_count <= 0:
                raise ValueError("file_count must be positive")
            if file_count > total_rows:
                raise ValueError("file_count cannot exceed total rows")
            num_files = file_count
            rows_per_file = (total_rows + num_files - 1) // num_files  # Ceiling division
        else:
            if record_count <= 0:
                raise ValueError("record_count must be positive")
            rows_per_file = record_count
            num_files = (total_rows + rows_per_file - 1) // rows_per_file

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
            rows_processed = 0
            current_writer = _open_writer(0)

            batch_size = min(10000, rows_per_file)
            for batch in self._parquet_file.iter_batches(batch_size=batch_size):
                batch_rows = len(batch)
                batch_offset = 0

                while batch_offset < batch_rows:
                    rows_remaining_in_file = rows_per_file - current_file_rows
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

                    if current_file_rows >= rows_per_file and current_file_idx < num_files - 1:
                        current_writer.close()
                        current_file_idx += 1
                        current_file_rows = 0
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
