"""
CLI application module.
Command-line interface for parq-cli tool.
"""

from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Callable, List, Optional

import typer


class OutputFormat(str, Enum):
    """Supported CLI output formats."""

    RICH = "rich"
    PLAIN = "plain"
    JSON = "json"


app = typer.Typer(
    name="parq",
    help="A powerful command-line tool for inspecting tabular files",
    add_completion=False,
)

_output_format: OutputFormat = OutputFormat.RICH


def _get_formatter():
    """Lazy load formatter based on output format setting."""
    if _output_format == OutputFormat.PLAIN:
        from parq.plain_output import PlainOutputFormatter

        return PlainOutputFormatter()
    if _output_format == OutputFormat.JSON:
        from parq.plain_output import JsonOutputFormatter

        return JsonOutputFormatter()
    from parq.output import OutputFormatter

    return OutputFormatter()


def _get_reader(file_path: str):
    """Lazy load reader to improve CLI startup time."""
    from parq.reader import MultiFormatReader

    return MultiFormatReader(file_path)


def _parse_column_list(columns: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated column lists into normalized names."""
    if columns is None:
        return None
    parsed = [column.strip() for column in columns.split(",") if column.strip()]
    return parsed or []


def _run_with_error_handling(
    operation: Callable[[Any], None],
    *,
    generic_error_prefix: str,
) -> None:
    """Execute a CLI operation with consistent error handling."""
    formatter = _get_formatter()
    try:
        operation(formatter)
    except typer.Exit:
        raise
    except (FileNotFoundError, ValueError, FileExistsError) as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1) from e
    except Exception as e:
        formatter.print_error(f"{generic_error_prefix}: {e}")
        raise typer.Exit(code=1) from e


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: Annotated[
        bool, typer.Option("--version", "-v", help="Show version information")
    ] = False,
    output: Annotated[
        OutputFormat, typer.Option("--output", "-o", help="Output format")
    ] = OutputFormat.RICH,
) -> None:
    """A powerful command-line tool for inspecting tabular files."""
    global _output_format
    _output_format = output

    if version:
        from parq import __version__

        typer.echo(f"parq-cli version {__version__}")
        raise typer.Exit()

    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


@app.command()
def meta(
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .xlsx)")],
    fast: Annotated[
        bool,
        typer.Option("--fast", help="Skip expensive fields such as full row counts where possible"),
    ] = False,
) -> None:
    """Display metadata information of a tabular file."""

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        formatter.print_metadata(reader.get_metadata_dict(fast=fast))

    _run_with_error_handling(operation, generic_error_prefix="Failed to read file")


@app.command()
def schema(
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .xlsx)")],
) -> None:
    """Display the schema of a tabular file."""

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        formatter.print_schema(reader.get_schema_info())

    _run_with_error_handling(operation, generic_error_prefix="Failed to read file")


@app.command()
def head(
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .xlsx)")],
    n: Annotated[int, typer.Option("-n", help="Number of rows to display")] = 5,
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to display"),
    ] = None,
) -> None:
    """Display the first N rows of a tabular file."""

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        table = reader.read_head(n, columns=_parse_column_list(columns))
        formatter.print_table(table, f"First {n} Rows")

    _run_with_error_handling(operation, generic_error_prefix="Failed to read file")


@app.command()
def tail(
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .xlsx)")],
    n: Annotated[int, typer.Option("-n", help="Number of rows to display")] = 5,
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to display"),
    ] = None,
) -> None:
    """Display the last N rows of a tabular file."""

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        table = reader.read_tail(n, columns=_parse_column_list(columns))
        formatter.print_table(table, f"Last {n} Rows")

    _run_with_error_handling(operation, generic_error_prefix="Failed to read file")


@app.command()
def count(
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .xlsx)")],
) -> None:
    """Display the total row count of a tabular file."""

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        formatter.print_count(reader.num_rows)

    _run_with_error_handling(operation, generic_error_prefix="Failed to read file")


@app.command()
def split(
    file: Annotated[Path, typer.Argument(help="Path to source file (.parquet, .csv, .xlsx)")],
    file_count: Annotated[
        Optional[int],
        typer.Option("--file-count", "-f", help="Number of output files"),
    ] = None,
    record_count: Annotated[
        Optional[int],
        typer.Option("--record-count", "-r", help="Number of records per file"),
    ] = None,
    name_format: Annotated[
        str,
        typer.Option("--name-format", "-n", help="Output file name format"),
    ] = "result-%06d.parquet",
) -> None:
    """Split a source file into multiple files."""

    formatter = _get_formatter()
    try:
        if file_count is None and record_count is None:
            formatter.print_error(
                "Either --file-count or --record-count must be specified.\n"
                "Use 'parq split --help' for usage information."
            )
            raise typer.Exit(code=1)

        if file_count is not None and record_count is not None:
            formatter.print_error(
                "--file-count and --record-count are mutually exclusive.\nPlease specify only one."
            )
            raise typer.Exit(code=1)

        import time

        start_time = time.time()
        reader = _get_reader(str(file))

        if _output_format == OutputFormat.RICH:
            from rich.progress import (
                BarColumn,
                Progress,
                SpinnerColumn,
                TextColumn,
                TimeRemainingColumn,
            )
            from parq.output import console

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(f"[cyan]Splitting {file.name}...", total=None)

                def update_progress(current: int, total: int):
                    if progress.tasks[task].total is None and total:
                        progress.update(task, total=total)
                    progress.update(task, completed=current)

                output_files = reader.split_file(
                    output_pattern=name_format,
                    file_count=file_count,
                    record_count=record_count,
                    progress_callback=update_progress,
                )
        else:
            output_files = reader.split_file(
                output_pattern=name_format,
                file_count=file_count,
                record_count=record_count,
            )

        elapsed_time = time.time() - start_time
        total_rows = reader.last_split_total_rows if reader.last_split_total_rows is not None else reader.num_rows
        formatter.print_split_result(
            source_file=file,
            output_files=output_files,
            total_rows=total_rows,
            elapsed_time=elapsed_time,
        )
    except typer.Exit:
        raise
    except (FileNotFoundError, ValueError, FileExistsError) as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1) from e
    except Exception as e:
        formatter.print_error(f"Failed to split file: {e}")
        raise typer.Exit(code=1) from e


@app.command()
def stats(
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .xlsx)")],
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to summarize"),
    ] = None,
    limit: Annotated[int, typer.Option("--limit", help="Maximum columns to display")] = 50,
) -> None:
    """Display simple per-column statistics."""

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        formatter.print_stats(reader.get_stats(columns=_parse_column_list(columns), limit=limit))

    _run_with_error_handling(operation, generic_error_prefix="Failed to compute stats")


@app.command()
def convert(
    source: Annotated[Path, typer.Argument(help="Path to source file (.parquet, .csv, .xlsx)")],
    output: Annotated[Path, typer.Argument(help="Destination path; suffix controls output format")],
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to include"),
    ] = None,
) -> None:
    """Convert one supported input file to another supported output format."""

    def operation(formatter: Any) -> None:
        import time

        start_time = time.time()
        reader = _get_reader(str(source))
        total_rows = reader.convert_file(output, columns=_parse_column_list(columns))
        formatter.print_convert_result(source, output, total_rows, time.time() - start_time)

    _run_with_error_handling(operation, generic_error_prefix="Failed to convert file")


@app.command()
def diff(
    left: Annotated[Path, typer.Argument(help="Left input file (.parquet or .csv)")],
    right: Annotated[Path, typer.Argument(help="Right input file (.parquet or .csv)")],
    key: Annotated[str, typer.Option("--key", help="Comma-separated key columns for row matching")],
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated subset of columns to compare"),
    ] = None,
    limit: Annotated[int, typer.Option("--limit", help="Maximum sample rows to print")] = 20,
    summary_only: Annotated[
        bool,
        typer.Option("--summary-only", help="Return counts only without sample payloads"),
    ] = False,
) -> None:
    """Compare two tabular files using one or more key columns."""

    def operation(formatter: Any) -> None:
        from parq.reader import diff_files

        result = diff_files(
            left,
            right,
            key_columns=_parse_column_list(key) or [],
            columns=_parse_column_list(columns),
            limit=limit,
            summary_only=summary_only,
        )
        formatter.print_diff_result(result)

    _run_with_error_handling(operation, generic_error_prefix="Failed to diff files")


@app.command()
def merge(
    paths: Annotated[
        List[Path],
        typer.Argument(help="Input files followed by the output file path"),
    ],
) -> None:
    """Merge multiple compatible input files into one output file."""

    def operation(formatter: Any) -> None:
        import time
        from parq.reader import merge_files

        if len(paths) < 2:
            raise ValueError("Provide at least one input file followed by an output path")
        input_files = paths[:-1]
        output = paths[-1]
        start_time = time.time()
        total_rows = merge_files(input_files, output)
        formatter.print_merge_result(input_files, output, total_rows, time.time() - start_time)

    _run_with_error_handling(operation, generic_error_prefix="Failed to merge files")


if __name__ == "__main__":
    app()
