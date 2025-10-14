"""
Tests for ParquetReader.
"""

import pytest

from parq.reader import ParquetReader


class TestParquetReader:
    """Test ParquetReader functionality."""

    def test_reader_initialization(self, sample_parquet_file):
        """Test reader can be initialized with valid file."""
        reader = ParquetReader(str(sample_parquet_file))
        assert reader.file_path.exists()

    def test_file_not_found(self):
        """Test reader raises error for non-existent file."""
        with pytest.raises(FileNotFoundError):
            ParquetReader("nonexistent.parquet")

    def test_get_metadata(self, sample_parquet_file):
        """Test metadata retrieval."""
        reader = ParquetReader(str(sample_parquet_file))
        metadata = reader.get_metadata_dict()

        assert metadata["num_rows"] == 5
        assert metadata["num_columns"] == 5
        assert "file_path" in metadata
        assert "created_by" in metadata

    def test_get_schema(self, sample_parquet_file):
        """Test schema retrieval."""
        reader = ParquetReader(str(sample_parquet_file))
        schema_info = reader.get_schema_info()

        assert len(schema_info) == 5

        # Check first column
        assert schema_info[0]["name"] == "id"
        assert "int" in schema_info[0]["type"].lower()

    def test_num_rows(self, sample_parquet_file):
        """Test row count."""
        reader = ParquetReader(str(sample_parquet_file))
        assert reader.num_rows == 5

    def test_num_columns(self, sample_parquet_file):
        """Test column count."""
        reader = ParquetReader(str(sample_parquet_file))
        assert reader.num_columns == 5

    def test_read_head(self, sample_parquet_file):
        """Test reading first N rows."""
        reader = ParquetReader(str(sample_parquet_file))

        # Read first 3 rows
        table = reader.read_head(3)
        assert len(table) == 3

        # Read more than available
        table = reader.read_head(10)
        assert len(table) == 5

    def test_read_tail(self, sample_parquet_file):
        """Test reading last N rows."""
        reader = ParquetReader(str(sample_parquet_file))

        # Read last 2 rows
        table = reader.read_tail(2)
        assert len(table) == 2

        # Verify it's the last rows
        df = table.to_pandas()
        assert df.iloc[0]["id"] == 4
        assert df.iloc[1]["id"] == 5

    def test_read_columns(self, sample_parquet_file):
        """Test reading specific columns."""
        reader = ParquetReader(str(sample_parquet_file))

        # Read specific columns
        table = reader.read_columns(columns=["id", "name"])
        assert table.num_columns == 2
        assert "id" in table.column_names
        assert "name" in table.column_names
