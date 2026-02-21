"""
Tests for module entry point behavior.
"""

import runpy

import parq.cli


def test_module_entry_point_invokes_app(monkeypatch):
    """Test running parq as a module dispatches to CLI app."""
    called = {"value": False}

    def fake_app():
        called["value"] = True

    monkeypatch.setattr(parq.cli, "app", fake_app)

    runpy.run_module("parq.__main__", run_name="__main__")

    assert called["value"] is True
