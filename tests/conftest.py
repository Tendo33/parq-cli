"""
Pytest configuration and fixtures.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def sample_parquet_file(tmp_path):
    """Create a sample Parquet file for testing."""
    # Create sample data
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "city": ["New York", "London", "Paris", "Tokyo", "Sydney"],
        "salary": [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
    }

    table = pa.table(data)

    # Write to temporary file
    file_path = tmp_path / "sample.parquet"
    pq.write_table(table, file_path)

    return file_path
