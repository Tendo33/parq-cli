"""
Parquet file reader module.
Provides functionality to read and inspect Parquet files.
"""

from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq


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
        """Get total number of columns."""
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
        return {
            "file_path": str(self.file_path),
            "num_rows": self.num_rows,
            "num_columns": self.num_columns,
            "num_row_groups": self.num_row_groups,
            "format_version": self.metadata.format_version,
            "serialized_size": self.metadata.serialized_size,
            "created_by": self.metadata.created_by,
        }

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

    def read_head(self, n: int = 5) -> pa.Table:
        """
        Read first n rows.

        Args:
            n: Number of rows to read

        Returns:
            PyArrow table with first n rows
        """
        table = self._parquet_file.read()
        return table.slice(0, min(n, self.num_rows))

    def read_tail(self, n: int = 5) -> pa.Table:
        """
        Read last n rows.

        Args:
            n: Number of rows to read

        Returns:
            PyArrow table with last n rows
        """
        table = self._parquet_file.read()
        start = max(0, self.num_rows - n)
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


# {{CHENGQI:
# Action: Created; Timestamp: 2025-10-14 16:14:00 +08:00;
# Reason: Core reader implementation with metadata and data reading capabilities;
# Principle_Applied: SOLID-S (Single Responsibility), DRY, High Cohesion
# }}
# {{START MODIFICATIONS}}
# - Implemented ParquetReader class with PyArrow backend
# - Added metadata inspection methods
# - Added head/tail reading functionality
# - Proper error handling for file not found
# {{END MODIFICATIONS}}
