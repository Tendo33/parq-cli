"""
Tests for scripts/check_version.py.
"""

import importlib.util
import urllib.error
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_version.py"


def load_check_version_module():
    """Load check_version.py as a module for testing."""
    spec = importlib.util.spec_from_file_location("check_version_module", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_get_pypi_versions_returns_empty_on_404(monkeypatch):
    """Package-not-found should be treated as first release."""
    module = load_check_version_module()

    def raise_404(_url, timeout=10):
        raise urllib.error.HTTPError(_url, 404, "Not Found", hdrs=None, fp=None)

    monkeypatch.setattr(module.urllib.request, "urlopen", raise_404)
    assert module.get_pypi_versions("parq-cli") == []


def test_get_pypi_versions_raises_on_network_error(monkeypatch):
    """Network errors should fail fast instead of pretending first release."""
    module = load_check_version_module()

    def raise_network_error(_url, timeout=10):
        raise urllib.error.URLError("timeout")

    monkeypatch.setattr(module.urllib.request, "urlopen", raise_network_error)

    with pytest.raises(RuntimeError, match="Unable to fetch"):
        module.get_pypi_versions("parq-cli")


def test_get_pypi_versions_raises_on_non_404_http_error(monkeypatch):
    """Non-404 HTTP errors should be normalized into RuntimeError."""
    module = load_check_version_module()

    def raise_503(_url, timeout=10):
        raise urllib.error.HTTPError(_url, 503, "Service Unavailable", hdrs=None, fp=None)

    monkeypatch.setattr(module.urllib.request, "urlopen", raise_503)

    with pytest.raises(RuntimeError, match="Unable to fetch"):
        module.get_pypi_versions("parq-cli")


def test_get_latest_version_uses_pep440_order():
    """Latest version should be computed using robust PEP 440 parsing."""
    module = load_check_version_module()
    versions = ["1.0.0", "1.0.0rc1", "1.10.0", "1.2.0", "1.10.0.post1"]
    assert module.get_latest_version(versions) == "1.10.0.post1"


def test_main_exits_with_code_1_when_fetch_fails(monkeypatch):
    """Main should stop with non-zero exit on network failures."""
    module = load_check_version_module()

    def raise_fetch_error(_package_name):
        raise RuntimeError("Unable to fetch versions from PyPI: timeout")

    monkeypatch.setattr(module, "get_pypi_versions", raise_fetch_error)

    with pytest.raises(SystemExit) as excinfo:
        module.main()

    assert excinfo.value.code == 1
