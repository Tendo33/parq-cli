"""
Tests for CLI commands.
"""

from typer.testing import CliRunner

from parq.cli import app

runner = CliRunner()


class TestCLI:
    """Test CLI commands."""

    def test_cli_help(self):
        """Test --help option."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "parq" in result.output.lower()

    def test_cli_version(self):
        """Test version option."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_cli_file_not_found(self):
        """Test error handling for non-existent file."""
        result = runner.invoke(app, ["nonexistent.parquet"])
        assert result.exit_code != 0  # Should fail with any non-zero exit code

    def test_cli_metadata(self, sample_parquet_file):
        """Test displaying metadata (default behavior)."""
        result = runner.invoke(app, [str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "num_rows" in result.output.lower() or "5" in result.output

    def test_cli_schema(self, sample_parquet_file):
        """Test --schema option."""
        result = runner.invoke(app, [str(sample_parquet_file), "--schema"])
        assert result.exit_code == 0
        assert "schema" in result.output.lower() or "column" in result.output.lower()

    def test_cli_head(self, sample_parquet_file):
        """Test --head option."""
        result = runner.invoke(app, [str(sample_parquet_file), "--head", "3"])
        assert result.exit_code == 0

    def test_cli_tail(self, sample_parquet_file):
        """Test --tail option."""
        result = runner.invoke(app, [str(sample_parquet_file), "--tail", "2"])
        assert result.exit_code == 0

    def test_cli_count(self, sample_parquet_file):
        """Test --count option."""
        result = runner.invoke(app, [str(sample_parquet_file), "--count"])
        assert result.exit_code == 0
        assert "5" in result.output


# {{CHENGQI:
# Action: Created; Timestamp: 2025-10-14 16:24:00 +08:00;
# Reason: CLI integration tests using Typer CliRunner;
# Principle_Applied: Testability, End-to-end testing
# }}
