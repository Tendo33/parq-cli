"""
CLI application module.
Command-line interface for parq-cli tool.
"""

from pathlib import Path
from typing import Annotated, Any, Callable, Optional

import typer

app = typer.Typer(
    name="parq",
    help="A powerful command-line tool for inspecting Apache Parquet files 🚀",
    add_completion=False,
)

_output_format: str = "rich"


def _get_formatter():
    """Lazy load formatter based on output format setting."""
    if _output_format == "plain":
        from parq.plain_output import PlainOutputFormatter

        return PlainOutputFormatter()
    elif _output_format == "json":
        from parq.plain_output import JsonOutputFormatter

        return JsonOutputFormatter()
    from parq.output import OutputFormatter

    return OutputFormatter()


def _get_reader(file_path: str):
    """Lazy load reader to improve CLI startup time."""
    from parq.reader import ParquetReader

    return ParquetReader(file_path)


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
    except (FileNotFoundError, ValueError) as e:
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
        str, typer.Option("--output", "-o", help="Output format: rich, plain, json")
    ] = "rich",
) -> None:
    """A powerful command-line tool for inspecting Apache Parquet files 🚀"""
    global _output_format
    _output_format = output

    if version:
        from parq import __version__

        typer.echo(f"parq-cli version {__version__}")
        raise typer.Exit()

    # If no subcommand and no version flag, show help
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


@app.command()
def meta(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
) -> None:
    """
    Display metadata information of a Parquet file.

    Example:

        parq meta data.parquet
    """
    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        metadata = reader.get_metadata_dict()
        formatter.print_metadata(metadata)

    _run_with_error_handling(operation, generic_error_prefix="Failed to read Parquet file")


@app.command()
def schema(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
) -> None:
    """
    Display the schema of a Parquet file.

    Example:

        parq schema data.parquet
    """
    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        schema_info = reader.get_schema_info()
        formatter.print_schema(schema_info)

    _run_with_error_handling(operation, generic_error_prefix="Failed to read Parquet file")


@app.command()
def head(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
    n: Annotated[int, typer.Option("-n", help="Number of rows to display")] = 5,
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to display"),
    ] = None,
) -> None:
    """
    Display the first N rows of a Parquet file (default: 5).

    Examples:

        # Show first 5 rows (default)
        parq head data.parquet

        # Show first 10 rows
        parq head -n 10 data.parquet
    """
    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        col_list = [c.strip() for c in columns.split(",")] if columns else None
        table = reader.read_head(n, columns=col_list)
        formatter.print_table(table, f"First {n} Rows")

    _run_with_error_handling(operation, generic_error_prefix="Failed to read Parquet file")


@app.command()
def tail(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
    n: Annotated[int, typer.Option("-n", help="Number of rows to display")] = 5,
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to display"),
    ] = None,
) -> None:
    """
    Display the last N rows of a Parquet file (default: 5).

    Examples:

        # Show last 5 rows (default)
        parq tail data.parquet

        # Show last 10 rows
        parq tail -n 10 data.parquet
    """
    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        col_list = [c.strip() for c in columns.split(",")] if columns else None
        table = reader.read_tail(n, columns=col_list)
        formatter.print_table(table, f"Last {n} Rows")

    _run_with_error_handling(operation, generic_error_prefix="Failed to read Parquet file")


@app.command()
def count(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
) -> None:
    """
    Display the total row count of a Parquet file.

    Example:

        parq count data.parquet
    """
    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file))
        formatter.print_count(reader.num_rows)

    _run_with_error_handling(operation, generic_error_prefix="Failed to read Parquet file")


@app.command()
def split(
    file: Annotated[Path, typer.Argument(help="Path to source Parquet file")],
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
    """
    Split a Parquet file into multiple files.

    The output file count is determined by either --file-count or --record-count parameter.
    File names are formatted according to --name-format (default: result-%06d.parquet).

    Examples:

        # Split into 3 files
        parq split data.parquet --file-count 3

        # Split with 1000 records per file
        parq split data.parquet --record-count 1000

        # Custom output format
        parq split data.parquet -f 5 -n "output-%03d.parquet"

        # Split into subdirectory
        parq split data.parquet -f 3 -n "output/part-%02d.parquet"
    """
    # Initialize formatter early for error messages
    formatter = _get_formatter()

    try:
        # Validate mutually exclusive parameters
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

        # Start timer
        import time

        start_time = time.time()

        # Create reader
        reader = _get_reader(str(file))

        # Setup progress bar (skip for plain/json modes)
        if _output_format == "rich":
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
                task = progress.add_task(
                    f"[cyan]Splitting {file.name}...", total=reader.num_rows
                )

                def update_progress(current: int, _total: int):
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

        # Calculate elapsed time
        elapsed_time = time.time() - start_time

        # Display results
        formatter.print_split_result(
            source_file=file,
            output_files=output_files,
            total_rows=reader.num_rows,
            elapsed_time=elapsed_time,
        )

    except FileNotFoundError as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1)
    except (ValueError, FileExistsError) as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        formatter.print_error(f"Failed to split Parquet file: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
