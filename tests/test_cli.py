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
        assert "0.0.3" in result.output

    def test_cli_file_not_found(self):
        """Test error handling for non-existent file."""
        result = runner.invoke(app, ["nonexistent.parquet"])
        assert result.exit_code != 0  # Should fail with any non-zero exit code

    def test_cli_meta_command(self, sample_parquet_file):
        """Test meta command: parq meta FILE."""
        result = runner.invoke(app, ["meta", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "num_rows" in result.output.lower() or "5" in result.output

    def test_cli_schema_command(self, sample_parquet_file):
        """Test schema subcommand: parq schema FILE."""
        result = runner.invoke(app, ["schema", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "schema" in result.output.lower() or "column" in result.output.lower()

    def test_cli_head_default(self, sample_parquet_file):
        """Test head command with default 5 rows: parq head FILE."""
        result = runner.invoke(app, ["head", str(sample_parquet_file)])
        assert result.exit_code == 0
        # Should show "First 5 Rows" in output
        assert "first" in result.output.lower() and "5" in result.output.lower()

    def test_cli_head_with_n_option(self, sample_parquet_file):
        """Test head command with -n option: parq head -n 3 FILE."""
        result = runner.invoke(app, ["head", "-n", "3", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "first" in result.output.lower() and "3" in result.output.lower()

    def test_cli_tail_default(self, sample_parquet_file):
        """Test tail command with default 5 rows: parq tail FILE."""
        result = runner.invoke(app, ["tail", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "last" in result.output.lower() and "5" in result.output.lower()

    def test_cli_tail_with_n_option(self, sample_parquet_file):
        """Test tail command with -n option: parq tail -n 2 FILE."""
        result = runner.invoke(app, ["tail", "-n", "2", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "last" in result.output.lower() and "2" in result.output.lower()

    def test_cli_count_command(self, sample_parquet_file):
        """Test count subcommand: parq count FILE."""
        result = runner.invoke(app, ["count", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "5" in result.output

    def test_schema_file_not_found(self):
        """Test schema command with non-existent file."""
        result = runner.invoke(app, ["schema", "nonexistent.parquet"])
        assert result.exit_code != 0

    def test_head_file_not_found(self):
        """Test head command with non-existent file."""
        result = runner.invoke(app, ["head", "nonexistent.parquet"])
        assert result.exit_code != 0

    def test_tail_file_not_found(self):
        """Test tail command with non-existent file."""
        result = runner.invoke(app, ["tail", "nonexistent.parquet"])
        assert result.exit_code != 0

    def test_count_file_not_found(self):
        """Test count command with non-existent file."""
        result = runner.invoke(app, ["count", "nonexistent.parquet"])
        assert result.exit_code != 0

    def test_meta_file_not_found(self):
        """Test meta command with non-existent file."""
        result = runner.invoke(app, ["meta", "nonexistent.parquet"])
        assert result.exit_code != 0
