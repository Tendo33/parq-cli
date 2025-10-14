"""
CLI application module.
Command-line interface for parq-cli tool.
"""

import time
from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from parq.output import OutputFormatter
from parq.reader import ParquetReader

app = typer.Typer(
    name="parq",
    help="A powerful command-line tool for inspecting Apache Parquet files 🚀",
    add_completion=False,
)

formatter = OutputFormatter()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: Annotated[
        bool, typer.Option("--version", "-v", help="Show version information")
    ] = False,
) -> None:
    """A powerful command-line tool for inspecting Apache Parquet files 🚀"""
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
    try:
        reader = ParquetReader(str(file))
        metadata = reader.get_metadata_dict()
        formatter.print_metadata(metadata)
    except FileNotFoundError as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        formatter.print_error(f"Failed to read Parquet file: {e}")
        raise typer.Exit(code=1)


@app.command()
def schema(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
) -> None:
    """
    Display the schema of a Parquet file.

    Example:

        parq schema data.parquet
    """
    try:
        reader = ParquetReader(str(file))
        schema_info = reader.get_schema_info()
        formatter.print_schema(schema_info)
    except FileNotFoundError as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        formatter.print_error(f"Failed to read Parquet file: {e}")
        raise typer.Exit(code=1)


@app.command()
def head(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
    n: Annotated[int, typer.Option("-n", help="Number of rows to display")] = 5,
) -> None:
    """
    Display the first N rows of a Parquet file (default: 5).

    Examples:

        # Show first 5 rows (default)
        parq head data.parquet

        # Show first 10 rows
        parq head -n 10 data.parquet
    """
    try:
        reader = ParquetReader(str(file))
        table = reader.read_head(n)
        formatter.print_table(table, f"First {n} Rows")
    except FileNotFoundError as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        formatter.print_error(f"Failed to read Parquet file: {e}")
        raise typer.Exit(code=1)


@app.command()
def tail(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
    n: Annotated[int, typer.Option("-n", help="Number of rows to display")] = 5,
) -> None:
    """
    Display the last N rows of a Parquet file (default: 5).

    Examples:

        # Show last 5 rows (default)
        parq tail data.parquet

        # Show last 10 rows
        parq tail -n 10 data.parquet
    """
    try:
        reader = ParquetReader(str(file))
        table = reader.read_tail(n)
        formatter.print_table(table, f"Last {n} Rows")
    except FileNotFoundError as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        formatter.print_error(f"Failed to read Parquet file: {e}")
        raise typer.Exit(code=1)


@app.command()
def count(
    file: Annotated[Path, typer.Argument(help="Path to Parquet file")],
) -> None:
    """
    Display the total row count of a Parquet file.

    Example:

        parq count data.parquet
    """
    try:
        reader = ParquetReader(str(file))
        formatter.print_count(reader.num_rows)
    except FileNotFoundError as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        formatter.print_error(f"Failed to read Parquet file: {e}")
        raise typer.Exit(code=1)


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
    # {{CHENGQI:
    # Action: Added; Timestamp: 2025-10-14 21:32:00 +08:00;
    # Reason: Add CLI command for split functionality;
    # Principle_Applied: Consistent error handling pattern, User-friendly output
    # }}
    # {{START MODIFICATIONS}}
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
        start_time = time.time()

        # Create reader and perform split
        reader = ParquetReader(str(file))
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

    # {{END MODIFICATIONS}}


if __name__ == "__main__":
    app()
