"""
Performance comparison tests for small vs large Parquet files.
"""

import importlib.util
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "performance_benchmark.py"


def _load_performance_module():
    """Load scripts/performance_benchmark.py as a module."""
    spec = importlib.util.spec_from_file_location("performance_benchmark_module", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.performance
def test_small_vs_large_multi_format_performance_report(tmp_path):
    """Benchmark report should cover parquet/csv/xlsx reads and cross-format splits."""
    module = _load_performance_module()

    datasets, dataset_info = module.create_benchmark_datasets(
        tmp_path,
        parquet_small_rows=10_000,
        parquet_large_rows=200_000,
        csv_small_rows=10_000,
        csv_large_rows=200_000,
        xlsx_small_rows=500,
        xlsx_large_rows=5_000,
    )
    result = module.compare_reader_performance(datasets, repeats=3)
    report = module.render_markdown_report(result, dataset_info)

    print(report)

    assert "## Read Performance" in report
    assert "## Split Performance" in report
    assert "parquet" in result["reads"]
    assert "csv" in result["reads"]
    assert "xlsx" in result["reads"]
    assert "parquet_to_csv" in result["splits"]
    assert "csv_to_parquet" in result["splits"]
    assert "xlsx_to_parquet" in result["splits"]

    assert result["reads"]["parquet"]["head"]["ratio"] > 1.05
    assert result["reads"]["csv"]["tail"]["ratio"] > 1.05
    assert result["reads"]["xlsx"]["count"]["ratio"] > 1.05
    assert result["splits"]["parquet_to_csv"]["ratio"] > 1.05
    assert result["splits"]["csv_to_parquet"]["ratio"] > 1.05
