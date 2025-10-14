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

        # Add compression type (from first row group, first column)
        if self.num_row_groups > 0:
            compression = self.metadata.row_group(0).column(0).compression
            metadata_dict["compression_types"] = compression

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

    def split_file(
        self,
        output_pattern: str,
        file_count: Optional[int] = None,
        record_count: Optional[int] = None,
    ) -> List[Path]:
        """
        Split parquet file into multiple files.

        Args:
            output_pattern: Output file name pattern (e.g., 'result-%06d.parquet')
            file_count: Number of output files (mutually exclusive with record_count)
            record_count: Number of records per file (mutually exclusive with file_count)

        Returns:
            List of created file paths

        Raises:
            ValueError: If both or neither of file_count/record_count are provided
            IOError: If file write fails
        """
        # {{CHENGQI:
        # Action: Added; Timestamp: 2025-10-14 21:30:00 +08:00;
        # Reason: Implement split command functionality;
        # Principle_Applied: SOLID-S (Single Responsibility), DRY, Error handling
        # }}
        # {{START MODIFICATIONS}}

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

        # Create writers for each output file
        writers = []
        try:
            for output_path in output_files:
                # Create parent directories if needed
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Create writer with same schema and compression as source
                writer = pq.ParquetWriter(
                    output_path,
                    self.schema,
                    compression=self._get_compression_type(),
                )
                writers.append(writer)

            # Read and distribute data in batches
            current_file_idx = 0
            current_file_rows = 0

            # Use batch reader for memory efficiency
            batch_size = min(10000, rows_per_file)  # Read in chunks
            for batch in self._parquet_file.iter_batches(batch_size=batch_size):
                batch_rows = len(batch)
                batch_offset = 0

                while batch_offset < batch_rows:
                    # Calculate how many rows to write to current file
                    rows_remaining_in_file = rows_per_file - current_file_rows
                    rows_to_write = min(rows_remaining_in_file, batch_rows - batch_offset)

                    # Extract slice from batch
                    if rows_to_write == batch_rows and batch_offset == 0:
                        # Write entire batch
                        writers[current_file_idx].write_batch(batch)
                    else:
                        # Write partial batch
                        slice_batch = batch.slice(batch_offset, rows_to_write)
                        writers[current_file_idx].write_batch(slice_batch)

                    batch_offset += rows_to_write
                    current_file_rows += rows_to_write

                    # Move to next file if current is full
                    if current_file_rows >= rows_per_file and current_file_idx < num_files - 1:
                        current_file_idx += 1
                        current_file_rows = 0

        finally:
            # Always close writers
            for writer in writers:
                if writer:
                    writer.close()

        # {{END MODIFICATIONS}}

        return output_files

    def _get_compression_type(self) -> str:
        """
        Get compression type from source file.

        Returns:
            Compression type string (e.g., 'SNAPPY', 'GZIP', 'NONE')
        """
        # {{CHENGUI:
        # Action: Added; Timestamp: 2025-10-14 21:30:00 +08:00;
        # Reason: Helper method to extract compression type for split files;
        # Principle_Applied: DRY, Encapsulation
        # }}
        if self.num_row_groups > 0:
            compression = self.metadata.row_group(0).column(0).compression
            return compression
        return "SNAPPY"  # Default compression
