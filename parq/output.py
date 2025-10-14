"""
Output formatting module.
Handles pretty-printing of Parquet data and metadata.
"""

from typing import Any, Dict, List

import pyarrow as pa
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


class OutputFormatter:
    """Formatter for displaying Parquet data and metadata."""

    @staticmethod
    def print_metadata(metadata_dict: Dict[str, Any]) -> None:
        """
        Print file metadata in a formatted panel.

        Args:
            metadata_dict: Dictionary containing metadata
        """
        content = "\n".join(
            [
                f"[cyan]{key}:[/cyan] [yellow]{value}[/yellow]"
                for key, value in metadata_dict.items()
            ]
        )
        panel = Panel(
            content,
            title="[bold green]📊 Parquet File Metadata[/bold green]",
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(panel)

    @staticmethod
    def print_schema(schema_info: List[Dict[str, Any]]) -> None:
        """
        Print schema information as a table.

        Args:
            schema_info: List of column information dictionaries
        """
        table = Table(
            title="[bold blue]📋 Schema Information[/bold blue]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
        )

        table.add_column("Column Name", style="cyan", no_wrap=True)
        table.add_column("Data Type", style="green")
        table.add_column("Nullable", style="yellow")

        for col in schema_info:
            table.add_row(col["name"], col["type"], "✓" if col["nullable"] else "✗")

        console.print(table)

    @staticmethod
    def print_table(arrow_table: pa.Table, title: str = "Data Preview") -> None:
        """
        Print PyArrow table as a Rich table.

        Args:
            arrow_table: PyArrow table to display
            title: Title for the table
        """
        # Convert to pandas for easier display
        df = arrow_table.to_pandas()

        table = Table(
            title=f"[bold blue]📄 {title}[/bold blue]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
        )

        # Add columns
        for col in df.columns:
            table.add_column(str(col), style="cyan")

        # Add rows
        for _, row in df.iterrows():
            table.add_row(*[str(val) for val in row])

        console.print(table)

    @staticmethod
    def print_count(count: int) -> None:
        """
        Print row count.

        Args:
            count: Number of rows
        """
        panel = Panel(
            f"[bold yellow]{count:,}[/bold yellow] rows",
            title="[bold green]📊 Total Rows[/bold green]",
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(panel)

    @staticmethod
    def print_error(message: str) -> None:
        """
        Print error message.

        Args:
            message: Error message to display
        """
        console.print(f"[bold red]❌ Error:[/bold red] {message}")

    @staticmethod
    def print_success(message: str) -> None:
        """
        Print success message.

        Args:
            message: Success message to display
        """
        console.print(f"[bold green]✓[/bold green] {message}")


# {{CHENGQI:
# Action: Created; Timestamp: 2025-10-14 16:16:00 +08:00;
# Reason: Output formatting module using Rich library for beautiful terminal output;
# Principle_Applied: SOLID-S (Single Responsibility), DRY, User-centric design
# }}
# {{START MODIFICATIONS}}
# - Implemented OutputFormatter with Rich library
# - Added methods for metadata, schema, table, and count display
# - Beautiful colored output with panels and tables
# - Error and success message formatting
# {{END MODIFICATIONS}}
