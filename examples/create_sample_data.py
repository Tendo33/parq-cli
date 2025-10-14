"""
Generate sample Parquet files for testing parq-cli.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Create examples directory if not exists
examples_dir = Path(__file__).parent
examples_dir.mkdir(exist_ok=True)


def create_simple_sample():
    """Create a simple sample Parquet file."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "city": ["New York", "London", "Paris", "Tokyo", "Sydney"],
        "salary": [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
    }

    table = pa.table(data)
    output_file = examples_dir / "simple.parquet"
    pq.write_table(table, output_file)
    print(f"✓ Created: {output_file}")


def create_large_sample():
    """Create a larger sample with more rows."""
    import random

    n_rows = 1000

    data = {
        "id": list(range(1, n_rows + 1)),
        "value": [random.random() * 100 for _ in range(n_rows)],
        "category": [random.choice(["A", "B", "C", "D"]) for _ in range(n_rows)],
        "timestamp": pa.array(
            [f"2024-01-{(i % 30) + 1:02d}" for i in range(n_rows)], type=pa.string()
        ),
    }

    table = pa.table(data)
    output_file = examples_dir / "large.parquet"
    pq.write_table(table, output_file, compression="snappy")
    print(f"✓ Created: {output_file}")


def create_types_sample():
    """Create a sample with various data types."""
    data = {
        "int32_col": pa.array([1, 2, 3], type=pa.int32()),
        "int64_col": pa.array([100, 200, 300], type=pa.int64()),
        "float_col": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
        "string_col": pa.array(["foo", "bar", "baz"], type=pa.string()),
        "bool_col": pa.array([True, False, True], type=pa.bool_()),
        "date_col": pa.array(["2024-01-01", "2024-01-02", "2024-01-03"], type=pa.date32()),
        "nullable_col": pa.array([1, None, 3], type=pa.int32()),
    }

    table = pa.table(data)
    output_file = examples_dir / "types.parquet"
    pq.write_table(table, output_file)
    print(f"✓ Created: {output_file}")


if __name__ == "__main__":
    print("Creating sample Parquet files...\n")

    create_simple_sample()
    create_large_sample()
    create_types_sample()

    print("\n✅ All sample files created successfully!")
    print("\nTry these commands:")
    print("  parq examples/simple.parquet")
    print("  parq examples/simple.parquet --schema")
    print("  parq examples/large.parquet --head 10")
    print("  parq examples/types.parquet --count")
