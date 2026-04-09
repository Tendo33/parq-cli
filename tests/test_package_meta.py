"""
Tests for package metadata helpers in parq/__init__.py.
"""

from pathlib import Path

import parq


def test_get_version_prefers_installed_package_version(monkeypatch, tmp_path):
    """Test installed package metadata is preferred when available."""
    fake_init = tmp_path / "_fake_pkg" / "__init__.py"
    fake_init.parent.mkdir(parents=True, exist_ok=True)
    fake_init.write_text("# fake", encoding="utf-8")

    def fake_version(_package_name):
        return "9.9.9"

    monkeypatch.setattr("importlib.metadata.version", fake_version)
    monkeypatch.setattr(parq, "__file__", str(fake_init), raising=False)
    assert parq._get_version() == "9.9.9"


def test_get_version_prefers_repo_pyproject_when_running_from_source(monkeypatch, tmp_path):
    """Repository pyproject should win over stale installed metadata in source checkouts."""
    fake_init = tmp_path / "parq" / "__init__.py"
    fake_init.parent.mkdir(parents=True)
    fake_init.write_text("# fake", encoding="utf-8")

    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text('version = "2.3.4"\n', encoding="utf-8")
    (tmp_path / ".git").mkdir()

    monkeypatch.setattr("importlib.metadata.version", lambda _package_name: "9.9.9")
    monkeypatch.setattr(parq, "__file__", str(fake_init), raising=False)

    assert parq._get_version() == "2.3.4"


def test_get_version_falls_back_to_pyproject(monkeypatch, tmp_path):
    """Test pyproject.toml is used when metadata lookup fails."""
    fake_init = tmp_path / "parq" / "__init__.py"
    fake_init.parent.mkdir(parents=True)
    fake_init.write_text("# fake", encoding="utf-8")

    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text('version = "2.3.4"\n', encoding="utf-8")

    def raise_metadata_error(_package_name):
        raise RuntimeError("not installed")

    monkeypatch.setattr("importlib.metadata.version", raise_metadata_error)
    monkeypatch.setattr(parq, "__file__", str(fake_init), raising=False)

    assert parq._get_version() == "2.3.4"


def test_get_version_returns_unknown_when_all_sources_fail(monkeypatch, tmp_path):
    """Test unknown fallback when metadata and pyproject are unavailable."""
    fake_init = tmp_path / "parq" / "__init__.py"
    fake_init.parent.mkdir(parents=True)
    fake_init.write_text("# fake", encoding="utf-8")

    def raise_metadata_error(_package_name):
        raise RuntimeError("not installed")

    monkeypatch.setattr("importlib.metadata.version", raise_metadata_error)
    monkeypatch.setattr(parq, "__file__", str(fake_init), raising=False)

    # Ensure no pyproject.toml exists in fallback lookup path.
    pyproject_path = Path(fake_init).parent.parent / "pyproject.toml"
    if pyproject_path.exists():
        pyproject_path.unlink()

    assert parq._get_version() == "unknown"
