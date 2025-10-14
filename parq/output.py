"""
Output formatting module.
Handles pretty-printing of Parquet data and metadata.
"""

from pathlib import Path
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
    def _format_file_size(size_bytes: int) -> str:
        """
        Format file size in human-readable format.

        Args:
            size_bytes: Size in bytes

        Returns:
            Formatted string like "1.23 MB"
        """
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    @staticmethod
    def print_metadata(metadata_dict: Dict[str, Any]) -> None:
        """
        Print file metadata in a formatted panel.

        Args:
            metadata_dict: Dictionary containing metadata
        """
        # Special handling for specific fields
        content_lines = []
        for key, value in metadata_dict.items():
            if key == "num_columns":
                content_lines.append(
                    f"[cyan]{key}:[/cyan] [yellow]{value}[/yellow] [dim](logical)[/dim]"
                )
            elif key == "num_physical_columns":
                content_lines.append(
                    f"[cyan]{key}:[/cyan] [yellow]{value}[/yellow] [dim](storage)[/dim]"
                )
            elif key == "file_size":
                # Format file size in human-readable format
                formatted_size = OutputFormatter._format_file_size(value)
                content_lines.append(f"[cyan]{key}:[/cyan] [yellow]{formatted_size}[/yellow]")
            else:
                content_lines.append(f"[cyan]{key}:[/cyan] [yellow]{value}[/yellow]")

        content = "\n".join(content_lines)

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

    @staticmethod
    def print_split_result(
        source_file: Path,
        output_files: List[Path],
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        """
        Print split operation results.

        Args:
            source_file: Path to source file
            output_files: List of output file paths
            total_rows: Total number of rows in source file
            elapsed_time: Time taken to split in seconds
        """
        # {{CHENGQI:
        # Action: Added; Timestamp: 2025-10-14 21:35:00 +08:00;
        # Reason: Add output formatter for split command results;
        # Principle_Applied: Consistent output formatting, User-friendly display
        # }}
        # {{START MODIFICATIONS}}

        # Calculate statistics
        num_files = len(output_files)
        rows_per_file = total_rows // num_files

        # Get total output size
        total_size = sum(f.stat().st_size for f in output_files if f.exists())
        formatted_total_size = OutputFormatter._format_file_size(total_size)

        # Create summary content
        summary_lines = [
            f"[cyan]Source file:[/cyan] [yellow]{source_file}[/yellow]",
            f"[cyan]Total rows:[/cyan] [yellow]{total_rows:,}[/yellow]",
            f"[cyan]Output files:[/cyan] [yellow]{num_files}[/yellow]",
            f"[cyan]Rows per file:[/cyan] [yellow]~{rows_per_file:,}[/yellow]",
            f"[cyan]Total output size:[/cyan] [yellow]{formatted_total_size}[/yellow]",
            f"[cyan]Time elapsed:[/cyan] [yellow]{elapsed_time:.2f}s[/yellow]",
        ]

        summary_content = "\n".join(summary_lines)

        # Print summary panel
        panel = Panel(
            summary_content,
            title="[bold green]✂️  Split Complete[/bold green]",
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(panel)

        # Print output files table
        table = Table(
            title="[bold blue]📁 Output Files[/bold blue]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
        )

        table.add_column("#", style="cyan", no_wrap=True)
        table.add_column("File Name", style="green")
        table.add_column("Size", style="yellow", justify="right")

        for idx, file_path in enumerate(output_files):
            if file_path.exists():
                size = OutputFormatter._format_file_size(file_path.stat().st_size)
                table.add_row(str(idx), str(file_path), size)

        console.print(table)

        # {{END MODIFICATIONS}}
