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


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
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
    version: Annotated[bool, typer.Option("--version", "-v", help="Show version information")] = False,
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
    # If a subcommand was invoked, don't run this callback logic
    if ctx.invoked_subcommand is not None:
        return
    
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
# Action: Modified; Timestamp: 2025-10-14 HH:MM:SS +08:00;
# Reason: Use @app.callback with invoke_without_command=True to make main function work without subcommands;
# Principle_Applied: KISS, User-centric design - users can now use 'parq file.parquet' directly
# }}
# {{START MODIFICATIONS}}
# - Changed @app.command() to @app.callback(invoke_without_command=True)
# - Set no_args_is_help=False to allow file argument processing
# - Added ctx parameter to check for subcommands
# - Integrated version as --version flag instead of separate command
# - Simplified user experience: parq train.parquet now works directly
# {{END MODIFICATIONS}}
