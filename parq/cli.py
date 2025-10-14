"""
CLI application module.
Command-line interface for parq-cli tool.
"""

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
    no_args_is_help=False,
)

formatter = OutputFormatter()


@app.command()
def main(
    file: Annotated[
        Optional[Path],
        typer.Argument(
            help="Path to Parquet file",
        ),
    ] = None,
    schema: Annotated[
        bool, typer.Option("--schema", "-s", help="Display schema information")
    ] = False,
    head: Annotated[Optional[int], typer.Option("--head", help="Display first N rows")] = None,
    tail: Annotated[Optional[int], typer.Option("--tail", help="Display last N rows")] = None,
    count: Annotated[bool, typer.Option("--count", "-c", help="Display total row count")] = False,
    version: Annotated[
        bool, typer.Option("--version", "-v", help="Show version information")
    ] = False,
) -> None:
    """
    A powerful command-line tool for inspecting Apache Parquet files 🚀

    Examples:

        # Show file metadata
        parq data.parquet

        # Show schema
        parq data.parquet --schema

        # Show first 10 rows
        parq data.parquet --head 10

        # Show last 5 rows
        parq data.parquet --tail 5

        # Show row count
        parq data.parquet --count

        # Show version
        parq --version
    """

    # Handle version flag
    if version:
        from parq import __version__

        typer.echo(f"parq-cli version {__version__}")
        return

    # File is required if not showing version
    if file is None:
        typer.echo("Error: Missing argument 'FILE'.")
        typer.echo("Try 'parq --help' for help.")
        raise typer.Exit(code=1)

    try:
        reader = ParquetReader(str(file))

        # If no options specified, show metadata
        if not any([schema, head is not None, tail is not None, count]):
            metadata = reader.get_metadata_dict()
            formatter.print_metadata(metadata)
            return

        # Show schema
        if schema:
            schema_info = reader.get_schema_info()
            formatter.print_schema(schema_info)

        # Show head
        if head is not None:
            table = reader.read_head(head)
            formatter.print_table(table, f"First {head} Rows")

        # Show tail
        if tail is not None:
            table = reader.read_tail(tail)
            formatter.print_table(table, f"Last {tail} Rows")

        # Show count
        if count:
            formatter.print_count(reader.num_rows)

    except FileNotFoundError as e:
        formatter.print_error(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        formatter.print_error(f"Failed to read Parquet file: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()


# {{CHENGQI:
# Action: Modified; Timestamp: 2025-10-14 18:07:04 +08:00;
# Reason: Fixed CLI options parsing by using @app.command() instead of @app.callback();
# Principle_Applied: KISS, Typer best practices - single command app should use @app.command()
# }}
# {{START MODIFICATIONS}}
# - Changed @app.callback(invoke_without_command=True) to @app.command()
# - Removed ctx parameter and subcommand checking logic (lines 67-69)
# - This fixes the issue where options like --schema were incorrectly parsed as subcommands
# - Now 'parq file.parquet --schema' works correctly as expected
# {{END MODIFICATIONS}}
