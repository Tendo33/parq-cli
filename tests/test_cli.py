"""
Tests for CLI commands.
"""

import os
import subprocess
import sys

import pytest
import pyarrow as pa
import pyarrow.csv as pacsv
from typer.testing import CliRunner

from parq import __version__
from parq.cli import app

runner = CliRunner()


class TestCLI:
    """Test CLI commands."""

    def test_cli_help(self):
        """Test --help option."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "parq" in result.output.lower()
        assert "tabular files" in result.output.lower()

    def test_cli_help_with_gbk_encoding(self):
        """Test --help works under GBK-style console encoding."""
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "gbk"
        result = subprocess.run(
            [sys.executable, "-m", "parq", "--help"],
            capture_output=True,
            text=True,
            encoding="gbk",
            errors="replace",
            env=env,
            cwd=os.getcwd(),
        )

        assert result.returncode == 0
        assert "Usage:" in result.stdout
        assert "UnicodeEncodeError" not in result.stderr

    def test_cli_version(self):
        """Test version option."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output

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

    def test_cli_head_with_columns(self, sample_parquet_file):
        """Test head command with --columns option."""
        result = runner.invoke(app, ["head", "-c", "id,name", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "id" in result.output
        assert "name" in result.output
        assert "salary" not in result.output

    def test_cli_head_with_columns_invalid(self, sample_parquet_file):
        """Test head command with invalid column name."""
        result = runner.invoke(app, ["head", "-c", "id,nonexistent", str(sample_parquet_file)])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_cli_tail_with_columns(self, sample_parquet_file):
        """Test tail command with --columns option."""
        result = runner.invoke(app, ["tail", "-c", "id,city", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "id" in result.output
        assert "city" in result.output
        assert "salary" not in result.output

    def test_cli_count_command(self, sample_parquet_file):
        """Test count subcommand: parq count FILE."""
        result = runner.invoke(app, ["count", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "5" in result.output

    def test_cli_count_plain(self, sample_parquet_file):
        """Test count with --output plain."""
        result = runner.invoke(app, ["--output", "plain", "count", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert result.output.strip() == "5"

    def test_cli_count_json(self, sample_parquet_file):
        """Test count with --output json."""
        import json
        result = runner.invoke(app, ["--output", "json", "count", str(sample_parquet_file)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["count"] == 5

    def test_cli_rejects_invalid_output_format(self, sample_parquet_file):
        """Test --output rejects unsupported values instead of silently falling back."""
        result = runner.invoke(app, ["--output", "xml", "count", str(sample_parquet_file)])
        assert result.exit_code != 0
        assert "invalid value" in result.output.lower()

    def test_cli_meta_plain(self, sample_parquet_file):
        """Test meta with --output plain."""
        result = runner.invoke(app, ["--output", "plain", "meta", str(sample_parquet_file)])
        assert result.exit_code == 0
        assert "num_rows\t5" in result.output

    def test_cli_head_json(self, sample_parquet_file):
        """Test head with --output json."""
        import json
        result = runner.invoke(app, ["-o", "json", "head", "-n", "2", str(sample_parquet_file)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data["rows"]) == 2

    def test_cli_head_plain_with_columns(self, sample_parquet_file):
        """Test head --output plain combined with --columns."""
        result = runner.invoke(
            app, ["--output", "plain", "head", "-c", "id,name", str(sample_parquet_file)]
        )
        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        assert lines[0] == "id\tname"

    def test_cli_head_plain_preserves_single_row_for_control_chars(self, tmp_path):
        """Plain output should remain row-safe when cells contain tabs or newlines."""
        table = pa.table({"id": [1], "text": ["a\tb\nnext"]})
        file_path = tmp_path / "weird.csv"
        pacsv.write_csv(table, file_path)

        result = runner.invoke(app, ["--output", "plain", "head", str(file_path)])

        assert result.exit_code == 0
        assert len(result.output.splitlines()) == 2
        assert result.output.splitlines()[0] == "id\ttext"

    @pytest.mark.parametrize("fixture_name", ["sample_csv_file", "sample_xlsx_file"])
    def test_read_commands_support_non_parquet_inputs(self, fixture_name, request):
        """Test read commands support csv/xlsx inputs directly."""
        input_file = request.getfixturevalue(fixture_name)
        commands = [
            ["meta", str(input_file)],
            ["schema", str(input_file)],
            ["head", "-n", "2", str(input_file)],
            ["tail", "-n", "2", str(input_file)],
            ["count", str(input_file)],
        ]

        for cmd in commands:
            result = runner.invoke(app, cmd)
            assert result.exit_code == 0

    def test_split_supports_csv_input(self, sample_csv_file, tmp_path):
        """Test split command accepts csv input and writes parquet outputs."""
        output_pattern = str(tmp_path / "csv-split-%02d.parquet")
        result = runner.invoke(
            app,
            ["split", str(sample_csv_file), "--record-count", "2", "--name-format", output_pattern],
        )
        assert result.exit_code == 0
        assert (tmp_path / "csv-split-00.parquet").exists()
        assert (tmp_path / "csv-split-01.parquet").exists()

    def test_split_supports_xlsx_input(self, sample_xlsx_file, tmp_path):
        """Test split command accepts xlsx input and writes parquet outputs."""
        output_pattern = str(tmp_path / "xlsx-split-%02d.parquet")
        result = runner.invoke(
            app,
            ["split", str(sample_xlsx_file), "--record-count", "2", "--name-format", output_pattern],
        )
        assert result.exit_code == 0
        assert (tmp_path / "xlsx-split-00.parquet").exists()
        assert (tmp_path / "xlsx-split-01.parquet").exists()

    def test_split_parquet_to_csv_output(self, sample_parquet_file, tmp_path):
        """Test split command writes CSV content when output suffix is .csv."""
        output_pattern = str(tmp_path / "parquet-split-%02d.csv")
        result = runner.invoke(
            app,
            ["split", str(sample_parquet_file), "--record-count", "2", "--name-format", output_pattern],
        )
        assert result.exit_code == 0
        row_counts = [pacsv.read_csv(tmp_path / f"parquet-split-{i:02d}.csv").num_rows for i in range(3)]
        assert row_counts == [2, 2, 1]

    @pytest.mark.parametrize("command", ["meta", "schema", "head", "tail", "count"])
    def test_read_commands_file_not_found(self, command):
        """Test read commands handle non-existent file gracefully."""
        result = runner.invoke(app, [command, "nonexistent.parquet"])
        assert result.exit_code == 1
        assert "File not found" in result.output
        assert "Traceback" not in result.output
        assert "UnboundLocalError" not in result.output

    @pytest.mark.parametrize("command", ["head", "tail"])
    def test_preview_commands_negative_n(self, command, sample_parquet_file):
        """Test head/tail reject negative row count gracefully."""
        result = runner.invoke(app, [command, "-n", "-1", str(sample_parquet_file)])
        assert result.exit_code == 1
        assert "non-negative" in result.output
        assert "Traceback" not in result.output
        assert "UnboundLocalError" not in result.output

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
        assert result.exit_code == 1
        assert "File not found" in result.output
        assert "Traceback" not in result.output

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

    def test_split_unexpected_error_message_is_format_agnostic(self, sample_csv_file, monkeypatch):
        """Test unexpected split errors use generic file wording for csv/xlsx support."""

        class BoomReader:
            num_rows = 1

            @staticmethod
            def split_file(**kwargs):
                del kwargs
                raise RuntimeError("simulated split boom")

        monkeypatch.setattr("parq.cli._get_reader", lambda _path: BoomReader())

        result = runner.invoke(app, ["split", str(sample_csv_file), "--record-count", "1"])
        assert result.exit_code == 1
        assert "Failed to split file" in result.output
