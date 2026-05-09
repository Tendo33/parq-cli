"""
Tests for release helper scripts.
"""

from pathlib import Path


def test_publish_script_uses_project_virtualenv_tools():
    """Manual release script should prefer the project virtualenv over global PATH."""
    content = Path("scripts/publish.sh").read_text(encoding="utf-8")

    assert 'PYTHON_BIN="./.venv/bin/python"' in content
    assert 'PYTEST_BIN="./.venv/bin/pytest"' in content
    assert 'TWINE_BIN="./.venv/bin/twine"' in content
    assert "$PYTHON_BIN -m build" in content
    assert '$PYTEST_BIN -m "not performance"' in content


def test_publish_script_uses_tomllib_with_tomli_fallback():
    """Version extraction should work on modern Python without requiring tomli."""
    content = Path("scripts/publish.sh").read_text(encoding="utf-8")

    assert "import tomllib" in content
    assert "import tomli as tomllib" in content


def test_publish_script_excludes_performance_tests_by_default():
    """Manual release flow should match the repo's default non-performance test policy."""
    content = Path("scripts/publish.sh").read_text(encoding="utf-8")

    assert '$PYTEST_BIN -m "not performance"' in content


def test_pyproject_description_mentions_multi_format_tabular_support():
    """Published package metadata should reflect csv/xlsx support, not parquet-only wording."""
    content = Path("pyproject.toml").read_text(encoding="utf-8")

    assert "tabular" in content.lower() or ".csv" in content.lower()


def test_dev_dependencies_include_manual_release_tooling():
    """The documented virtualenv workflow should install build/twine for publish.sh."""
    content = Path("pyproject.toml").read_text(encoding="utf-8")

    assert '"build' in content
    assert '"twine' in content


def test_readme_documents_empty_file_and_machine_output_behavior():
    """README should explain empty-file handling and structured output choices."""
    content = Path("README.md").read_text(encoding="utf-8").lower()

    assert "empty csv" in content
    assert "plain" in content and "json" in content
