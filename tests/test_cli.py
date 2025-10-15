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

    def test_split_with_file_count(self, sample_parquet_file, tmp_path):
        """Test split command with --file-count option."""
        output_pattern = str(tmp_path / "output-%03d.parquet")
        result = runner.invoke(
            app,
            [
                "split",
                str(sample_parquet_file),
                "--file-count",
                "2",
                "--name-format",
                output_pattern,
            ],
        )
        assert result.exit_code == 0
        assert "Split Complete" in result.output or "split" in result.output.lower()

        # Verify files were created
        assert (tmp_path / "output-000.parquet").exists()
        assert (tmp_path / "output-001.parquet").exists()

    def test_split_with_record_count(self, sample_parquet_file, tmp_path):
        """Test split command with --record-count option."""
        output_pattern = str(tmp_path / "part-%02d.parquet")
        result = runner.invoke(
            app, ["split", str(sample_parquet_file), "--record-count", "2", "-n", output_pattern]
        )
        assert result.exit_code == 0

        # Verify 3 files were created (5 rows / 2 = 3 files)
        assert (tmp_path / "part-00.parquet").exists()
        assert (tmp_path / "part-01.parquet").exists()
        assert (tmp_path / "part-02.parquet").exists()

    def test_split_missing_parameters(self, sample_parquet_file):
        """Test split command fails without file-count or record-count."""
        result = runner.invoke(app, ["split", str(sample_parquet_file)])
        assert result.exit_code != 0
        assert "must be specified" in result.output

    def test_split_mutually_exclusive_params(self, sample_parquet_file, tmp_path):
        """Test split command fails with both file-count and record-count."""
        output_pattern = str(tmp_path / "output-%03d.parquet")
        result = runner.invoke(
            app,
            [
                "split",
                str(sample_parquet_file),
                "--file-count",
                "2",
                "--record-count",
                "100",
                "-n",
                output_pattern,
            ],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_split_file_not_found(self, tmp_path):
        """Test split command with non-existent source file."""
        output_pattern = str(tmp_path / "output-%03d.parquet")
        result = runner.invoke(
            app, ["split", "nonexistent.parquet", "--file-count", "2", "-n", output_pattern]
        )
        assert result.exit_code != 0

    def test_split_custom_format(self, sample_parquet_file, tmp_path):
        """Test split with custom name format."""
        output_pattern = str(tmp_path / "custom_name_%06d.parquet")
        result = runner.invoke(
            app, ["split", str(sample_parquet_file), "-f", "3", "-n", output_pattern]
        )
        assert result.exit_code == 0

        # Verify custom format was used
        assert (tmp_path / "custom_name_000000.parquet").exists()
        assert (tmp_path / "custom_name_000001.parquet").exists()
        assert (tmp_path / "custom_name_000002.parquet").exists()

    def test_split_short_options(self, sample_parquet_file, tmp_path):
        """Test split command with short option flags."""
        output_pattern = str(tmp_path / "out-%d.parquet")
        result = runner.invoke(
            app, ["split", str(sample_parquet_file), "-f", "2", "-n", output_pattern]
        )
        assert result.exit_code == 0
