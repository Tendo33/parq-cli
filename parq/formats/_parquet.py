"""
Parquet-specific reader using PyArrow.
"""

from pathlib import Path
from typing import Callable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from parq.formats._common import (
    FAST_PATH_ROWS_MULTIPLIER,
    _compute_stats_from_batches,
    _create_empty_table,
    _select_schema,
    _validate_preview_params,
)
from parq.formats._chunk_writers import (
    _resolve_output_files,
    _split_batches_to_files,
    _write_batches_to_output,
)


class ParquetReader:
    """Parquet file reader with metadata inspection capabilities."""

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        self._parquet_file = pq.ParquetFile(self.file_path)
        self.last_split_total_rows: Optional[int] = None
        self.last_write_total_rows: Optional[int] = None

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

    def get_metadata_dict(self, fast: bool = False) -> dict:
        """Get metadata as a dictionary."""
        metadata_dict = {
            "file_path": str(self.file_path),
            "num_columns": self.num_columns,
            "file_size": self.file_path.stat().st_size,
            "num_row_groups": self.num_row_groups,
            "format_version": self.metadata.format_version,
        }

        if not fast:
            metadata_dict["num_rows"] = self.num_rows

        if self.num_physical_columns != self.num_columns:
            metadata_dict["num_physical_columns"] = self.num_physical_columns

        if not fast:
            compression = self._get_compression_summary()
            if compression is not None:
                metadata_dict["compression"] = compression
            metadata_dict.update(
                {
                    "serialized_size": self.metadata.serialized_size,
                    "created_by": self.metadata.created_by,
                }
            )

        return metadata_dict

    def get_schema_info(self) -> List[dict]:
        """Get schema information as a list of column details."""
        schema_info = []
        for field in self.schema:
            schema_info.append(
                {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            )
        return schema_info

    def _read_preview(
        self, n: int, columns: Optional[List[str]] = None, from_tail: bool = False
    ) -> pa.Table:
        """Read a parquet preview window using shared validation and row-group planning."""
        _validate_preview_params(n, self.schema, columns)
        if n == 0:
            return _create_empty_table(self.schema, columns=columns)

        if self.num_rows <= n * FAST_PATH_ROWS_MULTIPLIER or self.num_row_groups == 1:
            table = self._parquet_file.read(columns=columns)
            start = max(0, self.num_rows - n) if from_tail else 0
            length = n if from_tail else min(n, self.num_rows)
            return table.slice(start, length)

        rows_remaining = n
        row_groups = []
        indices = (
            range(self.num_row_groups - 1, -1, -1) if from_tail else range(self.num_row_groups)
        )
        for row_group_index in indices:
            row_groups.append(row_group_index)
            rows_remaining -= self.metadata.row_group(row_group_index).num_rows
            if rows_remaining <= 0:
                break

        if from_tail:
            row_groups.reverse()
        table = self._parquet_file.read_row_groups(row_groups, columns=columns)
        start = max(0, len(table) - n) if from_tail else 0
        return table.slice(start, n)

    def read_head(self, n: int = 5, columns: Optional[List[str]] = None) -> pa.Table:
        """Read first n rows."""
        return self._read_preview(n, columns=columns)

    def read_tail(self, n: int = 5, columns: Optional[List[str]] = None) -> pa.Table:
        """Read last n rows."""
        return self._read_preview(n, columns=columns, from_tail=True)

    def read_columns(self, columns: Optional[List[str]] = None) -> pa.Table:
        """Read specific columns."""
        return self._parquet_file.read(columns=columns)

    def split_file(
        self,
        output_pattern: str,
        file_count: Optional[int] = None,
        record_count: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        force: bool = False,
    ) -> List[Path]:
        """Split parquet file into multiple files."""
        from parq.formats._common import _resolve_split_shape

        total_rows = self.num_rows
        chunk_sizes = _resolve_split_shape(
            total_rows, file_count=file_count, record_count=record_count
        )
        output_files = _resolve_output_files(output_pattern, len(chunk_sizes), force=force)
        result = _split_batches_to_files(
            self._parquet_file.iter_batches(batch_size=min(10000, max(chunk_sizes))),
            self.schema,
            output_files,
            chunk_sizes,
            total_rows,
            progress_callback=progress_callback,
            compression=self._get_compression_type(),
        )
        self.last_split_total_rows = total_rows
        return result

    def convert_file(
        self,
        output_path: Path,
        columns: Optional[List[str]] = None,
        force: bool = False,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Convert parquet input to one supported output file."""
        rows_written = _write_batches_to_output(
            self._parquet_file.iter_batches(columns=columns),
            _select_schema(self.schema, columns),
            output_path,
            compression=(
                self._get_compression_type() if output_path.suffix.lower() == ".parquet" else None
            ),
            force=force,
            progress_callback=progress_callback,
            total_rows=self.num_rows,
        )
        self.last_write_total_rows = rows_written
        return rows_written

    def get_stats(
        self, columns: Optional[List[str]] = None, limit: int = 50, top_n: int = 5
    ) -> List[dict]:
        """Return simple column statistics."""
        selected_schema = _select_schema(self.schema, columns)
        return _compute_stats_from_batches(
            self._parquet_file.iter_batches(columns=columns),
            selected_schema,
            limit=limit,
            top_n=top_n,
        )

    def _get_compression_type(self) -> str:
        """Get compression type from source file."""
        if self.num_row_groups > 0:
            return self.metadata.row_group(0).column(0).compression
        return "SNAPPY"

    def _get_compression_summary(self) -> Optional[str]:
        """Get compression summary from all row groups and columns."""
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
