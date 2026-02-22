"""
Tests for ParquetReader.
"""

from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

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
        assert metadata["compression"] == reader.metadata.row_group(0).column(0).compression
        assert "file_path" in metadata
        assert "created_by" in metadata

    def test_get_metadata_includes_physical_columns_for_nested_schema(self, tmp_path):
        """Test metadata includes physical column count when different from logical count."""
        file_path = tmp_path / "nested.parquet"
        table = pa.table(
            {
                "id": [1, 2, 3],
                "meta": [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}],
            }
        )
        pq.write_table(table, file_path)

        reader = ParquetReader(str(file_path))
        metadata = reader.get_metadata_dict()

        assert metadata["num_columns"] == 2
        assert metadata["num_physical_columns"] > metadata["num_columns"]

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

    def test_read_head_edge_cases(self, sample_parquet_file):
        """Test read_head with edge cases."""
        reader = ParquetReader(str(sample_parquet_file))

        # Test n=0 returns empty table with correct schema
        table = reader.read_head(0)
        assert len(table) == 0
        assert table.num_columns == reader.num_columns
        assert table.column_names == reader.schema.names
        assert table.schema == reader.schema

        # Test negative n raises ValueError
        with pytest.raises(ValueError, match="must be non-negative"):
            reader.read_head(-1)

    def test_read_head_with_columns(self, sample_parquet_file):
        """Test read_head returns only selected columns."""
        reader = ParquetReader(str(sample_parquet_file))
        table = reader.read_head(3, columns=["id", "name"])
        assert len(table) == 3
        assert table.column_names == ["id", "name"]

    def test_read_head_with_columns_invalid(self, sample_parquet_file):
        """Test read_head raises on invalid column names."""
        reader = ParquetReader(str(sample_parquet_file))
        with pytest.raises(ValueError, match="not found in schema"):
            reader.read_head(3, columns=["id", "nonexistent"])

    def test_read_head_with_columns_optimized_path(self, tmp_path):
        """Test column pruning works with optimized row-group path."""
        file_path = tmp_path / "multi_rg.parquet"
        table = pa.table({
            "id": list(range(40)),
            "value": [f"v{i}" for i in range(40)],
            "extra": [float(i) for i in range(40)],
        })
        pq.write_table(table, file_path, row_group_size=5)
        reader = ParquetReader(str(file_path))
        result = reader.read_head(2, columns=["id"])
        assert result.column_names == ["id"]
        assert result["id"].to_pylist() == [0, 1]

    def test_read_head_with_columns_empty(self, sample_parquet_file):
        """Test read_head with empty columns list raises ValueError."""
        reader = ParquetReader(str(sample_parquet_file))
        with pytest.raises(ValueError, match="cannot be empty"):
            reader.read_head(3, columns=[])

    def test_read_head_optimized_path_with_multiple_row_groups(self, tmp_path):
        """Test read_head optimized path reads only needed row groups."""
        file_path = tmp_path / "multi_rg.parquet"
        table = pa.table({"id": list(range(40)), "value": [f"v{i}" for i in range(40)]})
        pq.write_table(table, file_path, row_group_size=5)

        reader = ParquetReader(str(file_path))
        assert reader.num_row_groups > 1

        result = reader.read_head(2)
        assert result["id"].to_pylist() == [0, 1]

    def test_read_tail(self, sample_parquet_file):
        """Test reading last N rows."""
        reader = ParquetReader(str(sample_parquet_file))

        # Read last 2 rows
        table = reader.read_tail(2)
        assert len(table) == 2

        # Verify it's the last rows (using PyArrow's idiomatic to_pylist method)
        id_values = table["id"].to_pylist()
        assert id_values[0] == 4
        assert id_values[1] == 5

    def test_read_tail_edge_cases(self, sample_parquet_file):
        """Test read_tail with edge cases."""
        reader = ParquetReader(str(sample_parquet_file))

        # Test n=0 returns empty table with correct schema
        table = reader.read_tail(0)
        assert len(table) == 0
        assert table.num_columns == reader.num_columns
        assert table.column_names == reader.schema.names
        assert table.schema == reader.schema

        # Test negative n raises ValueError
        with pytest.raises(ValueError, match="must be non-negative"):
            reader.read_tail(-5)

    def test_read_tail_optimized_path_with_multiple_row_groups(self, tmp_path):
        """Test read_tail optimized path keeps output order correct."""
        file_path = tmp_path / "multi_rg_tail.parquet"
        table = pa.table({"id": list(range(40)), "value": [f"v{i}" for i in range(40)]})
        pq.write_table(table, file_path, row_group_size=5)

        reader = ParquetReader(str(file_path))
        assert reader.num_row_groups > 1

        result = reader.read_tail(3)
        assert result["id"].to_pylist() == [37, 38, 39]

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

    def test_split_rejects_file_count_larger_than_total_rows(self, sample_parquet_file, tmp_path):
        """Test split by file count rejects creating guaranteed empty output files."""
        reader = ParquetReader(str(sample_parquet_file))
        output_pattern = str(tmp_path / "split-%03d.parquet")

        with pytest.raises(ValueError, match="cannot exceed total rows"):
            reader.split_file(output_pattern=output_pattern, file_count=reader.num_rows + 1)

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

    def test_split_cleans_up_outputs_on_write_error(self, sample_parquet_file, tmp_path, monkeypatch):
        """Test split removes created outputs if writing fails partway through."""
        reader = ParquetReader(str(sample_parquet_file))
        output_pattern = str(tmp_path / "split-%03d.parquet")

        class FailingWriter:
            def __init__(self, where, schema, compression=None):
                del schema, compression
                self.path = Path(where)
                self.path.touch()

            def write_batch(self, batch):
                del batch
                if self.path.name.endswith("001.parquet"):
                    raise OSError("simulated write failure")

            def close(self):
                return None

        monkeypatch.setattr("parq.reader.pq.ParquetWriter", FailingWriter)

        with pytest.raises(OSError, match="simulated write failure"):
            reader.split_file(output_pattern=output_pattern, file_count=2)

        assert not (tmp_path / "split-000.parquet").exists()
        assert not (tmp_path / "split-001.parquet").exists()
