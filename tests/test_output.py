"""
Tests for output formatting utilities.
"""

from pathlib import Path

from rich.console import Console

from parq.output import OutputFormatter
import parq.output as output_module


def _recording_console():
    return Console(record=True, force_terminal=False, width=120)


def test_format_file_size_boundaries():
    """Test file size formatter across B/KB/MB/GB boundaries."""
    assert OutputFormatter._format_file_size(512) == "512 B"
    assert OutputFormatter._format_file_size(2048) == "2.00 KB"
    assert OutputFormatter._format_file_size(5 * 1024 * 1024) == "5.00 MB"
    assert OutputFormatter._format_file_size(3 * 1024 * 1024 * 1024) == "3.00 GB"


def test_print_metadata_includes_logical_and_storage_labels(monkeypatch):
    """Test metadata rendering for logical/physical columns and file size."""
    console = _recording_console()
    monkeypatch.setattr(output_module, "console", console)

    OutputFormatter.print_metadata(
        {
            "file_path": "data.parquet",
            "num_columns": 2,
            "num_physical_columns": 3,
            "file_size": 2048,
        }
    )

    text = console.export_text()
    assert "(logical)" in text
    assert "(storage)" in text
    assert "2.00 KB" in text


def test_print_success_outputs_message(monkeypatch):
    """Test success helper writes a success marker and message."""
    console = _recording_console()
    monkeypatch.setattr(output_module, "console", console)

    OutputFormatter.print_success("done")
    text = console.export_text()
    assert "done" in text


def test_print_split_result_renders_existing_files(monkeypatch, tmp_path):
    """Test split result summary renders successfully with mixed file existence."""
    console = _recording_console()
    monkeypatch.setattr(output_module, "console", console)

    existing_file = tmp_path / "part-000.parquet"
    existing_file.write_bytes(b"hello")
    missing_file = tmp_path / "part-001.parquet"

    OutputFormatter.print_split_result(
        source_file=Path("input.parquet"),
        output_files=[existing_file, missing_file],
        total_rows=10,
        elapsed_time=0.5,
    )

    text = console.export_text()
    assert "Split Complete" in text
    assert "Output Files" in text
    assert "5 B" in text
    assert "part-001.parquet" not in text
