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
def test_small_vs_large_parquet_performance_report(tmp_path):
    """Large parquet should be measurably slower than small parquet for head/tail reads."""
    module = _load_performance_module()

    small_path, large_path, dataset_info = module.create_benchmark_datasets(
        tmp_path,
        small_rows=10_000,
        large_rows=2_000_000,
    )
    result = module.compare_reader_performance(small_path, large_path, repeats=7)
    report = module.render_markdown_report(result, dataset_info)

    print(report)

    assert result["head"]["ratio"] > 1.2
    assert result["tail"]["ratio"] > 1.2
