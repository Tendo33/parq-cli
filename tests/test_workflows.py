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
