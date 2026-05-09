"""Tests for plain and JSON output formatters."""

import json

import pyarrow as pa

from parq.plain_output import JsonOutputFormatter, PlainOutputFormatter


class TestPlainOutputFormatter:
    def test_format_count(self, capsys):
        PlainOutputFormatter.print_count(42)
        assert capsys.readouterr().out.strip() == "42"

    def test_format_metadata(self, capsys):
        PlainOutputFormatter.print_metadata({"num_rows": 10, "file_size": 1024})
        out = capsys.readouterr().out
        assert "num_rows" in out
        assert "10" in out

    def test_format_schema(self, capsys):
        PlainOutputFormatter.print_schema([{"name": "id", "type": "int64", "nullable": True}])
        out = capsys.readouterr().out
        assert "id" in out
        assert "int64" in out

    def test_format_table(self, capsys):
        t = pa.table({"id": [1, 2], "name": ["a", "b"]})
        PlainOutputFormatter.print_table(t, "Test")
        out = capsys.readouterr().out
        assert "1" in out
        assert "a" in out

    def test_format_table_preserves_single_row_for_control_characters(self, capsys):
        t = pa.table({"id": [1], "text": ["a\tb\nnext"]})
        PlainOutputFormatter.print_table(t, "Test")
        out_lines = capsys.readouterr().out.splitlines()
        assert len(out_lines) == 2
        assert out_lines[0] == "id\ttext"


class TestJsonOutputFormatter:
    def test_format_count(self, capsys):
        JsonOutputFormatter.print_count(42)
        data = json.loads(capsys.readouterr().out)
        assert data["count"] == 42

    def test_format_metadata(self, capsys):
        JsonOutputFormatter.print_metadata({"num_rows": 10})
        data = json.loads(capsys.readouterr().out)
        assert data["num_rows"] == 10

    def test_format_table(self, capsys):
        t = pa.table({"id": [1, 2], "name": ["a", "b"]})
        JsonOutputFormatter.print_table(t, "Test")
        data = json.loads(capsys.readouterr().out)
        assert len(data["rows"]) == 2
        assert data["rows"][0]["id"] == 1
