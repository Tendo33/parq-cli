"""
Tests for GitHub workflow configuration.
"""

from pathlib import Path


def test_publish_workflow_requires_multi_os_tests_before_publish():
    """Publish workflow should gate release on matrix tests across all OSes."""
    workflow_path = Path(".github/workflows/publish.yml")
    content = workflow_path.read_text(encoding="utf-8")

    assert "pre-publish-tests:" in content
    assert "build-and-publish:" in content
    assert "needs: pre-publish-tests" in content
    assert "os: [ubuntu-latest, windows-latest, macos-latest]" in content
    assert "fail-fast: false" in content
    assert "pytest" in content


def test_test_workflow_includes_performance_comparison_step():
    """Main test workflow should run small-vs-large parquet perf comparison in CI."""
    workflow_path = Path(".github/workflows/test.yml")
    content = workflow_path.read_text(encoding="utf-8")

    assert 'pytest --cov=parq --cov-report=xml -m "not performance"' in content
    assert "Run performance comparison (small vs large parquet)" in content
    assert "pytest tests/test_performance.py -m performance -q -s" in content
    assert "matrix.os == 'ubuntu-latest' && matrix.python-version == '3.11'" in content
