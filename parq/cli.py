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
)

formatter = OutputFormatter()


@app.command()
def main(
    file: Annotated[
        Path,
        typer.Argument(
            help="Path to Parquet file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    schema: Annotated[
        bool, typer.Option("--schema", "-s", help="Display schema information")
    ] = False,
    head: Annotated[Optional[int], typer.Option("--head", help="Display first N rows")] = None,
    tail: Annotated[Optional[int], typer.Option("--tail", help="Display last N rows")] = None,
    count: Annotated[bool, typer.Option("--count", "-c", help="Display total row count")] = False,
) -> None:
    """
    Inspect Apache Parquet files.

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
    """
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


@app.command()
def version() -> None:
    """Show version information."""
    from parq import __version__

    typer.echo(f"parq-cli version {__version__}")


if __name__ == "__main__":
    app()


# {{CHENGQI:
# Action: Created; Timestamp: 2025-10-14 16:18:00 +08:00;
# Reason: CLI application using Typer framework with all core commands;
# Principle_Applied: KISS, User-centric design, Clear documentation
# }}
# {{START MODIFICATIONS}}
# - Implemented Typer CLI application
# - Added main command with file, schema, head, tail, count options
# - Proper error handling and user-friendly messages
# - Version command
# - Comprehensive help text and examples
# {{END MODIFICATIONS}}
