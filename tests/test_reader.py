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

    def test_split_with_file_count(self, sample_parquet_file, tmp_path):
        """Test splitting file by file count."""
        reader = ParquetReader(str(sample_parquet_file))

        # Split into 2 files
        output_pattern = str(tmp_path / "split-%03d.parquet")
        output_files = reader.split_file(output_pattern=output_pattern, file_count=2)

        assert len(output_files) == 2

        # Verify all files exist
        for file_path in output_files:
            assert file_path.exists()

        # Verify total rows match
        total_rows = sum(ParquetReader(str(f)).num_rows for f in output_files)
        assert total_rows == reader.num_rows

        # Verify schema consistency
        for file_path in output_files:
            split_reader = ParquetReader(str(file_path))
            assert split_reader.num_columns == reader.num_columns

    def test_split_with_record_count(self, sample_parquet_file, tmp_path):
        """Test splitting file by record count."""
        reader = ParquetReader(str(sample_parquet_file))

        # Split with 2 records per file (should create 3 files: 2+2+1)
        output_pattern = str(tmp_path / "split-%03d.parquet")
        output_files = reader.split_file(output_pattern=output_pattern, record_count=2)

        assert len(output_files) == 3  # 5 rows / 2 = 3 files

        # Verify row counts
        row_counts = [ParquetReader(str(f)).num_rows for f in output_files]
        assert row_counts == [2, 2, 1]  # First two files have 2 rows, last has 1

    def test_split_parameter_validation(self, sample_parquet_file, tmp_path):
        """Test parameter validation for split."""
        reader = ParquetReader(str(sample_parquet_file))
        output_pattern = str(tmp_path / "split-%03d.parquet")

        # Test: both parameters None
        with pytest.raises(ValueError, match="Either file_count or record_count must be specified"):
            reader.split_file(output_pattern=output_pattern)

        # Test: both parameters provided
        with pytest.raises(ValueError, match="mutually exclusive"):
            reader.split_file(output_pattern=output_pattern, file_count=2, record_count=100)

        # Test: negative file_count
        with pytest.raises(ValueError, match="must be positive"):
            reader.split_file(output_pattern=output_pattern, file_count=-1)

        # Test: negative record_count
        with pytest.raises(ValueError, match="must be positive"):
            reader.split_file(output_pattern=output_pattern, record_count=-1)

    def test_split_invalid_pattern(self, sample_parquet_file, tmp_path):
        """Test split with invalid output pattern."""
        reader = ParquetReader(str(sample_parquet_file))

        # Invalid format string (missing format specifier)
        with pytest.raises(ValueError, match="Invalid output pattern"):
            reader.split_file(output_pattern="no-format-specifier.parquet", file_count=2)

    def test_split_file_exists_error(self, sample_parquet_file, tmp_path):
        """Test split fails when output file already exists."""
        reader = ParquetReader(str(sample_parquet_file))

        # Create a file that would conflict
        existing_file = tmp_path / "split-000.parquet"
        existing_file.touch()

        output_pattern = str(tmp_path / "split-%03d.parquet")

        with pytest.raises(FileExistsError, match="already exists"):
            reader.split_file(output_pattern=output_pattern, file_count=2)

    def test_split_creates_subdirectories(self, sample_parquet_file, tmp_path):
        """Test split creates subdirectories if needed."""
        reader = ParquetReader(str(sample_parquet_file))

        # Output pattern with subdirectory
        output_pattern = str(tmp_path / "output" / "split-%03d.parquet")
        output_files = reader.split_file(output_pattern=output_pattern, file_count=2)

        # Verify subdirectory was created
        assert (tmp_path / "output").exists()
        assert (tmp_path / "output").is_dir()

        # Verify files exist in subdirectory
        for file_path in output_files:
            assert file_path.exists()
            assert file_path.parent == tmp_path / "output"
