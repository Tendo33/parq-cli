"""
CLI application module.
Command-line interface for parq-cli tool.
"""

from pathlib import Path

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


if __name__ == "__main__":
    app()
