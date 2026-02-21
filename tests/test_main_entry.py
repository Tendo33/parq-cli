"""
Tests for module entry point behavior.
"""

import ast
import runpy
from pathlib import Path

import parq.cli


def test_module_entry_point_invokes_app(monkeypatch):
    """Test running parq as a module dispatches to CLI app."""
    called = {"value": False}

    def fake_app():
        called["value"] = True

    monkeypatch.setattr(parq.cli, "app", fake_app)

    runpy.run_module("parq.__main__", run_name="__main__")

    assert called["value"] is True


def test_cli_module_avoids_typing_extensions_dependency():
    """CLI should use stdlib typing.Annotated instead of typing_extensions."""
    cli_path = Path(__file__).resolve().parents[1] / "parq" / "cli.py"
    tree = ast.parse(cli_path.read_text(encoding="utf-8"))

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "typing_extensions":
            raise AssertionError("parq.cli should not import typing_extensions")
