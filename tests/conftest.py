"""
Pytest configuration and fixtures.
"""

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def sample_data_dict():
    """Shared sample tabular data for parquet/csv/xlsx fixtures."""
    return {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "city": ["New York", "London", "Paris", "Tokyo", "Sydney"],
        "salary": [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
    }


@pytest.fixture
def sample_parquet_file(tmp_path, sample_data_dict):
    """Create a sample Parquet file for testing."""
    table = pa.table(sample_data_dict)

    # Write to temporary file
    file_path = tmp_path / "sample.parquet"
    pq.write_table(table, file_path)

    return file_path


@pytest.fixture
def sample_csv_file(tmp_path, sample_data_dict):
    """Create a sample CSV file for testing."""
    table = pa.table(sample_data_dict)
    file_path = tmp_path / "sample.csv"
    pcsv.write_csv(table, file_path)
    return file_path


@pytest.fixture
def sample_xlsx_file(tmp_path, sample_data_dict):
    """Create a sample XLSX file for testing (requires openpyxl)."""
    openpyxl = pytest.importorskip("openpyxl")
    file_path = tmp_path / "sample.xlsx"

    wb = openpyxl.Workbook()
    ws = wb.active
    headers = list(sample_data_dict.keys())
    ws.append(headers)

    row_count = len(sample_data_dict["id"])
    for i in range(row_count):
        ws.append([sample_data_dict[h][i] for h in headers])

    wb.save(file_path)
    return file_path
