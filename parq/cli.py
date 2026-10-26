"""
CLI application module.
Command-line interface for parq-cli tool.
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Callable, List, Optional

import typer


class OutputFormat(str, Enum):
    """Supported CLI output formats."""

    RICH = "rich"
    PLAIN = "plain"
    JSON = "json"


@dataclass
class _AppState:
    """Per-invocation state threaded through Typer's context object."""

    output_format: OutputFormat = OutputFormat.RICH
    delimiter: str = ","
    sheet: Optional[str] = None


app = typer.Typer(
    name="parq",
    help="A powerful command-line tool for inspecting tabular files",
    epilog=(
        "--output, --delimiter, and --sheet work before a subcommand and on the subcommand. "
        "When both are set, the value on the subcommand wins."
    ),
    add_completion=False,
)


def _get_formatter(ctx: typer.Context) -> Any:
    """Lazy load formatter based on output format stored in context state."""
    state: _AppState = ctx.find_root().obj or _AppState()
    if state.output_format == OutputFormat.PLAIN:
        from parq.plain_output import PlainOutputFormatter

        return PlainOutputFormatter()
    if state.output_format == OutputFormat.JSON:
        from parq.plain_output import JsonOutputFormatter

        return JsonOutputFormatter()
    from parq.output import OutputFormatter

    return OutputFormatter()


def _get_state(ctx: typer.Context) -> _AppState:
    """Return the _AppState stored in the root context."""
    return ctx.find_root().obj or _AppState()


# Command-level copies of the root options. Defaults stay None so an omitted
# flag does not clobber a value passed before the subcommand.
_CommandOutputFormat = Annotated[
    Optional[OutputFormat],
    typer.Option(
        "--output",
        "-o",
        help="Output format: rich, plain, or json",
        show_default=False,
    ),
]
_CommandDelimiter = Annotated[
    Optional[str],
    typer.Option(
        "--delimiter",
        "-d",
        help="Field delimiter for CSV/TSV input",
        show_default=False,
    ),
]
_CommandSheet = Annotated[
    Optional[str],
    typer.Option(
        "--sheet",
        help="XLSX sheet name or 0-based index. A value of only digits is an index.",
        show_default=False,
    ),
]


def _apply_command_options(
    ctx: typer.Context,
    output_format: Optional[OutputFormat],
    delimiter: Optional[str],
    sheet: Optional[str],
) -> _AppState:
    """Let flags after the subcommand override the same flags placed before it."""
    state = _get_state(ctx)
    if output_format is not None:
        state.output_format = output_format
    if delimiter is not None:
        state.delimiter = delimiter
    if sheet is not None:
        state.sheet = sheet
    return state


def _get_reader(file_path: str, state: Optional[_AppState] = None) -> Any:
    """Lazy load reader to improve CLI startup time."""
    from parq.reader import MultiFormatReader

    if state is not None:
        return MultiFormatReader(file_path, delimiter=state.delimiter, sheet=state.sheet)
    return MultiFormatReader(file_path)


def _parse_column_list(columns: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated column lists into normalized names."""
    if columns is None:
        return None
    parsed = [column.strip() for column in columns.split(",") if column.strip()]
    return parsed or []


def _run_with_error_handling(
    ctx: typer.Context,
    operation: Callable[[Any], None],
    *,
    generic_error_prefix: str,
    file_path: Optional[Path] = None,
) -> None:
    """Execute a CLI operation with consistent error handling."""
    path_text = str(file_path) if file_path is not None else None
    formatter = _get_formatter(ctx)
    try:
        operation(formatter)
    except typer.Exit:
        raise
    except (FileNotFoundError, ValueError, FileExistsError) as e:
        formatter.print_error(str(e), file_path=path_text)
        raise typer.Exit(code=1) from e
    except Exception as e:
        formatter.print_error(f"{generic_error_prefix}: {e}", file_path=path_text)
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
    delimiter: Annotated[
        str,
        typer.Option(
            "--delimiter",
            "-d",
            help="Field delimiter for CSV/TSV input (default: ',')",
        ),
    ] = ",",
    sheet: Annotated[
        Optional[str],
        typer.Option(
            "--sheet",
            help=(
                "XLSX sheet name or 0-based index to read (default: active sheet). "
                "A value of only digits is an index."
            ),
        ),
    ] = None,
) -> None:
    """A powerful command-line tool for inspecting tabular files."""
    state = ctx.ensure_object(_AppState)
    state.output_format = output
    state.delimiter = delimiter
    state.sheet = sheet

    if version:
        from parq import __version__

        typer.echo(f"parq-cli version {__version__}")
        raise typer.Exit()

    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


@app.command()
def meta(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .tsv, .xlsx)")],
    fast: Annotated[
        bool,
        typer.Option("--fast", help="Skip expensive fields such as full row counts where possible"),
    ] = False,
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Display metadata information of a tabular file."""
    _apply_command_options(ctx, output_format, delimiter, sheet)

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file), _get_state(ctx))
        formatter.print_metadata(reader.get_metadata_dict(fast=fast))

    _run_with_error_handling(
        ctx, operation, generic_error_prefix="Failed to read file", file_path=file
    )


@app.command()
def schema(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .tsv, .xlsx)")],
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Display the schema of a tabular file."""
    _apply_command_options(ctx, output_format, delimiter, sheet)

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file), _get_state(ctx))
        formatter.print_schema(reader.get_schema_info())

    _run_with_error_handling(
        ctx, operation, generic_error_prefix="Failed to read file", file_path=file
    )


@app.command()
def head(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .tsv, .xlsx)")],
    n: Annotated[int, typer.Option("-n", help="Number of rows to display")] = 5,
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to display"),
    ] = None,
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Display the first N rows of a tabular file."""
    _apply_command_options(ctx, output_format, delimiter, sheet)

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file), _get_state(ctx))
        table = reader.read_head(n, columns=_parse_column_list(columns))
        formatter.print_table(table, f"First {n} Rows")

    _run_with_error_handling(
        ctx, operation, generic_error_prefix="Failed to read file", file_path=file
    )


@app.command()
def tail(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .tsv, .xlsx)")],
    n: Annotated[int, typer.Option("-n", help="Number of rows to display")] = 5,
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to display"),
    ] = None,
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Display the last N rows of a tabular file."""
    _apply_command_options(ctx, output_format, delimiter, sheet)

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file), _get_state(ctx))
        table = reader.read_tail(n, columns=_parse_column_list(columns))
        formatter.print_table(table, f"Last {n} Rows")

    _run_with_error_handling(
        ctx, operation, generic_error_prefix="Failed to read file", file_path=file
    )


@app.command()
def count(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .tsv, .xlsx)")],
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Display the total row count of a tabular file."""
    _apply_command_options(ctx, output_format, delimiter, sheet)

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file), _get_state(ctx))
        formatter.print_count(reader.num_rows)

    _run_with_error_handling(
        ctx, operation, generic_error_prefix="Failed to read file", file_path=file
    )


@app.command()
def split(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="Path to source file (.parquet, .csv, .tsv, .xlsx)")],
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
    force: Annotated[
        bool,
        typer.Option("--force", "-F", help="Overwrite existing output files"),
    ] = False,
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Split a source file into multiple files."""

    state = _apply_command_options(ctx, output_format, delimiter, sheet)
    formatter = _get_formatter(ctx)
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
        reader = _get_reader(str(file), state)

        if state.output_format == OutputFormat.RICH:
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
                    force=force,
                )
        else:
            output_files = reader.split_file(
                output_pattern=name_format,
                file_count=file_count,
                record_count=record_count,
                force=force,
            )

        elapsed_time = time.time() - start_time
        total_rows = (
            reader.last_split_total_rows
            if reader.last_split_total_rows is not None
            else reader.num_rows
        )
        formatter.print_split_result(
            source_file=file,
            output_files=output_files,
            total_rows=total_rows,
            elapsed_time=elapsed_time,
        )
    except typer.Exit:
        raise
    except (FileNotFoundError, ValueError, FileExistsError) as e:
        formatter.print_error(str(e), file_path=str(file))
        raise typer.Exit(code=1) from e
    except Exception as e:
        formatter.print_error(f"Failed to split file: {e}", file_path=str(file))
        raise typer.Exit(code=1) from e


@app.command()
def stats(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="Path to data file (.parquet, .csv, .tsv, .xlsx)")],
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to summarize"),
    ] = None,
    limit: Annotated[int, typer.Option("--limit", help="Maximum columns to display")] = 50,
    top_n: Annotated[
        int,
        typer.Option("--top-n", help="Top N most frequent values to show for string columns"),
    ] = 5,
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Display simple per-column statistics."""
    _apply_command_options(ctx, output_format, delimiter, sheet)

    def operation(formatter: Any) -> None:
        reader = _get_reader(str(file), _get_state(ctx))
        formatter.print_stats(
            reader.get_stats(columns=_parse_column_list(columns), limit=limit, top_n=top_n)
        )

    _run_with_error_handling(
        ctx, operation, generic_error_prefix="Failed to compute stats", file_path=file
    )


@app.command()
def convert(
    ctx: typer.Context,
    source: Annotated[Path, typer.Argument(help="Path to source file (.parquet, .csv, .tsv, .xlsx)")],
    output: Annotated[Path, typer.Argument(help="Destination path; suffix controls output format")],
    columns: Annotated[
        Optional[str],
        typer.Option("--columns", "-c", help="Comma-separated list of columns to include"),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", "-F", help="Overwrite existing output file"),
    ] = False,
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Convert one supported input file to another supported output format.

    --output / -o selects rich, plain, or json. The destination file is the OUTPUT argument.
    """

    state = _apply_command_options(ctx, output_format, delimiter, sheet)
    formatter = _get_formatter(ctx)

    try:
        import time

        start_time = time.time()
        reader = _get_reader(str(source), state)

        if state.output_format == OutputFormat.RICH:
            from rich.progress import (
                Progress,
                SpinnerColumn,
                TextColumn,
                BarColumn,
                TimeRemainingColumn,
            )
            from parq.output import console

            total_rows_hint = reader.num_rows if reader.input_format == "parquet" else 0

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"[cyan]Converting {source.name}...",
                    total=total_rows_hint if total_rows_hint > 0 else None,
                )

                def update_progress(current: int, total: int) -> None:
                    if progress.tasks[task].total is None and total:
                        progress.update(task, total=total)
                    progress.update(task, completed=current)

                total_rows = reader.convert_file(
                    output,
                    columns=_parse_column_list(columns),
                    force=force,
                    progress_callback=update_progress,
                )
        else:
            total_rows = reader.convert_file(
                output,
                columns=_parse_column_list(columns),
                force=force,
            )

        formatter.print_convert_result(source, output, total_rows, time.time() - start_time)
    except typer.Exit:
        raise
    except (FileNotFoundError, ValueError, FileExistsError) as e:
        formatter.print_error(str(e), file_path=str(source))
        raise typer.Exit(code=1) from e
    except Exception as e:
        formatter.print_error(f"Failed to convert file: {e}", file_path=str(source))
        raise typer.Exit(code=1) from e


@app.command()
def diff(
    ctx: typer.Context,
    left: Annotated[Path, typer.Argument(help="Left input file (.parquet, .csv, or .tsv)")],
    right: Annotated[Path, typer.Argument(help="Right input file (.parquet, .csv, or .tsv)")],
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
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Compare two tabular files using one or more key columns."""
    _apply_command_options(ctx, output_format, delimiter, sheet)

    def operation(formatter: Any) -> None:
        from parq.reader import diff_files

        state = _get_state(ctx)
        result = diff_files(
            left,
            right,
            key_columns=_parse_column_list(key) or [],
            columns=_parse_column_list(columns),
            limit=limit,
            summary_only=summary_only,
            delimiter=state.delimiter,
            sheet=state.sheet,
        )
        formatter.print_diff_result(result)

    _run_with_error_handling(ctx, operation, generic_error_prefix="Failed to diff files")


@app.command()
def merge(
    ctx: typer.Context,
    paths: Annotated[
        List[Path],
        typer.Argument(help="Input files followed by the output file path"),
    ],
    force: Annotated[
        bool,
        typer.Option("--force", "-F", help="Overwrite existing output file"),
    ] = False,
    output_format: _CommandOutputFormat = None,
    delimiter: _CommandDelimiter = None,
    sheet: _CommandSheet = None,
) -> None:
    """Merge multiple compatible input files into one output file."""

    _apply_command_options(ctx, output_format, delimiter, sheet)
    formatter = _get_formatter(ctx)
    try:
        import time
        from parq.reader import merge_files

        if len(paths) < 2:
            formatter.print_error("Provide at least one input file followed by an output path")
            raise typer.Exit(code=1)

        input_files = paths[:-1]
        output = paths[-1]
        start_time = time.time()

        state = _get_state(ctx)
        if state.output_format == OutputFormat.RICH:
            from rich.progress import (
                Progress,
                SpinnerColumn,
                TextColumn,
                BarColumn,
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
                task = progress.add_task(f"[cyan]Merging {len(input_files)} file(s)...", total=None)

                def update_progress(current: int, total: int) -> None:
                    if progress.tasks[task].total is None and total:
                        progress.update(task, total=total)
                    progress.update(task, completed=current)

                total_rows = merge_files(
                    input_files,
                    output,
                    force=force,
                    progress_callback=update_progress,
                    delimiter=state.delimiter,
                    sheet=state.sheet,
                )
        else:
            total_rows = merge_files(
                input_files,
                output,
                force=force,
                delimiter=state.delimiter,
                sheet=state.sheet,
            )

        formatter.print_merge_result(input_files, output, total_rows, time.time() - start_time)
    except typer.Exit:
        raise
    except (FileNotFoundError, ValueError, FileExistsError) as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1) from e
    except Exception as e:
        formatter.print_error(f"Failed to merge files: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    app()
