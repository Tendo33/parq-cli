"""
Output formatting module.
Handles pretty-printing of Parquet data and metadata.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def _console_encoding() -> str:
    """Best-effort console encoding lookup for safe rich output."""
    stream = getattr(console, "file", None)
    encoding = getattr(stream, "encoding", None) or getattr(console, "encoding", None)
    return encoding or sys.stdout.encoding or "utf-8"


def _supports_text(text: str) -> bool:
    """Return whether the current console encoding can represent the given text."""
    try:
        text.encode(_console_encoding())
        return True
    except UnicodeEncodeError:
        return False


def _safe_text(text: str, fallback: str) -> str:
    """Prefer richer labels when encodable, otherwise use ASCII-safe fallbacks."""
    return text if _supports_text(text) else fallback


class OutputFormatter:
    """Formatter for displaying tabular data and metadata."""

    @staticmethod
    def _format_file_size(size_bytes: int) -> str:
        """Format file size in human-readable form."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        if size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        if size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    @staticmethod
    def print_metadata(metadata_dict: Dict[str, Any]) -> None:
        """Print file metadata in a formatted panel."""
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
                content_lines.append(
                    f"[cyan]{key}:[/cyan] [yellow]{OutputFormatter._format_file_size(value)}[/yellow]"
                )
            else:
                content_lines.append(f"[cyan]{key}:[/cyan] [yellow]{value}[/yellow]")

        panel = Panel(
            "\n".join(content_lines),
            title=(
                f"[bold green]{_safe_text(chr(0x1F4C4) + ' File Metadata', 'File Metadata')}"
                "[/bold green]"
            ),
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(panel)

    @staticmethod
    def print_schema(schema_info: List[Dict[str, Any]]) -> None:
        """Print schema information as a table."""
        table = Table(
            title=(
                f"[bold blue]"
                f"{_safe_text(chr(0x1F4CB) + ' Schema Information', 'Schema Information')}"
                f"[/bold blue]"
            ),
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
        )
        table.add_column("Column Name", style="cyan", no_wrap=True)
        table.add_column("Data Type", style="green")
        table.add_column("Nullable", style="yellow")

        for col in schema_info:
            table.add_row(col["name"], col["type"], "yes" if col["nullable"] else "no")

        console.print(table)

    @staticmethod
    def print_table(arrow_table: pa.Table, title: str = "Data Preview") -> None:
        """Print a PyArrow table as a Rich table."""
        table = Table(
            title=f"[bold blue]{_safe_text(chr(0x1F4CA) + ' ' + title, title)}[/bold blue]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
            padding=(0, 1),
            show_lines=True,
        )

        for col_name in arrow_table.column_names:
            table.add_column(str(col_name), style="cyan")

        for batch in arrow_table.to_batches():
            batch_dict = batch.to_pydict()
            for row_idx in range(len(batch)):
                table.add_row(*[str(batch_dict[col_name][row_idx]) for col_name in arrow_table.column_names])

        console.print(table)

    @staticmethod
    def print_count(count: int) -> None:
        """Print row count."""
        panel = Panel(
            f"[bold yellow]{count:,}[/bold yellow] rows",
            title=f"[bold green]{_safe_text(chr(0x1F4CA) + ' Total Rows', 'Total Rows')}[/bold green]",
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(panel)

    @staticmethod
    def print_error(message: str) -> None:
        """Print error message."""
        console.print(f"[bold red]{_safe_text(chr(0x274C) + ' Error:', 'Error:')}[/bold red] {message}")

    @staticmethod
    def print_success(message: str) -> None:
        """Print success message."""
        console.print(f"[bold green]{_safe_text(chr(0x2705), 'OK')}[/bold green] {message}")

    @staticmethod
    def print_stats(stats_rows: List[Dict[str, Any]]) -> None:
        """Print column statistics."""
        table = Table(
            title=f"[bold blue]{_safe_text(chr(0x1F4CA) + ' Column Statistics', 'Column Statistics')}[/bold blue]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
        )
        for column in ["name", "type", "count", "null_count", "min", "max", "mean"]:
            table.add_column(column, style="cyan" if column == "name" else None)

        for row in stats_rows:
            table.add_row(
                str(row["name"]),
                str(row["type"]),
                str(row["count"]),
                str(row["null_count"]),
                "" if row["min"] is None else str(row["min"]),
                "" if row["max"] is None else str(row["max"]),
                "" if row["mean"] is None else str(row["mean"]),
            )

        console.print(table)

    @staticmethod
    def print_diff_result(diff_result: Dict[str, Any]) -> None:
        """Print diff summary plus small row samples."""
        summary = Panel(
            "\n".join(
                [
                    f"[cyan]row_count_delta:[/cyan] [yellow]{diff_result['row_count_delta']}[/yellow]",
                    f"[cyan]only_left_count:[/cyan] [yellow]{diff_result['only_left_count']}[/yellow]",
                    f"[cyan]only_right_count:[/cyan] [yellow]{diff_result['only_right_count']}[/yellow]",
                    f"[cyan]changed_count:[/cyan] [yellow]{diff_result['changed_count']}[/yellow]",
                ]
            ),
            title=f"[bold green]{_safe_text(chr(0x1F50D) + ' Diff Summary', 'Diff Summary')}[/bold green]",
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(summary)

        for section in [
            "schema_only_left",
            "schema_only_right",
            "schema_type_mismatches",
            "changed_rows",
            "only_left",
            "only_right",
        ]:
            rows = diff_result.get(section) or []
            if not rows:
                continue
            table = Table(
                title=f"[bold blue]{section}[/bold blue]",
                box=box.ROUNDED,
                show_header=True,
                header_style="bold magenta",
            )
            columns = list(rows[0].keys())
            for column in columns:
                table.add_column(str(column), style="cyan")
            for row in rows:
                table.add_row(*[str(row.get(column, "")) for column in columns])
            console.print(table)

    @staticmethod
    def print_convert_result(
        source_file: Path,
        output_file: Path,
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        """Print convert result summary."""
        panel = Panel(
            "\n".join(
                [
                    f"[cyan]Source file:[/cyan] [yellow]{source_file}[/yellow]",
                    f"[cyan]Output file:[/cyan] [yellow]{output_file}[/yellow]",
                    f"[cyan]Total rows:[/cyan] [yellow]{total_rows:,}[/yellow]",
                    f"[cyan]Time elapsed:[/cyan] [yellow]{elapsed_time:.2f}s[/yellow]",
                ]
            ),
            title=f"[bold green]{_safe_text(chr(0x2705) + ' Convert Complete', 'Convert Complete')}[/bold green]",
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(panel)

    @staticmethod
    def print_merge_result(
        input_files: List[Path],
        output_file: Path,
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        """Print merge result summary."""
        panel = Panel(
            "\n".join(
                [
                    f"[cyan]Input files:[/cyan] [yellow]{len(input_files)}[/yellow]",
                    f"[cyan]Output file:[/cyan] [yellow]{output_file}[/yellow]",
                    f"[cyan]Total rows:[/cyan] [yellow]{total_rows:,}[/yellow]",
                    f"[cyan]Time elapsed:[/cyan] [yellow]{elapsed_time:.2f}s[/yellow]",
                ]
            ),
            title=f"[bold green]{_safe_text(chr(0x2705) + ' Merge Complete', 'Merge Complete')}[/bold green]",
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(panel)

    @staticmethod
    def print_split_result(
        source_file: Path,
        output_files: List[Path],
        total_rows: int,
        elapsed_time: float,
    ) -> None:
        """Print split operation results."""
        num_files = len(output_files)
        rows_per_file = total_rows // num_files if num_files else 0
        total_size = sum(f.stat().st_size for f in output_files if f.exists())
        formatted_total_size = OutputFormatter._format_file_size(total_size)

        summary_content = "\n".join(
            [
                f"[cyan]Source file:[/cyan] [yellow]{source_file}[/yellow]",
                f"[cyan]Total rows:[/cyan] [yellow]{total_rows:,}[/yellow]",
                f"[cyan]Output files:[/cyan] [yellow]{num_files}[/yellow]",
                f"[cyan]Rows per file:[/cyan] [yellow]~{rows_per_file:,}[/yellow]",
                f"[cyan]Total output size:[/cyan] [yellow]{formatted_total_size}[/yellow]",
                f"[cyan]Time elapsed:[/cyan] [yellow]{elapsed_time:.2f}s[/yellow]",
            ]
        )

        panel = Panel(
            summary_content,
            title=f"[bold green]{_safe_text(chr(0x2705) + ' Split Complete', 'Split Complete')}[/bold green]",
            border_style="green",
            box=box.ROUNDED,
        )
        console.print(panel)

        table = Table(
            title=f"[bold blue]{_safe_text(chr(0x1F4C1) + ' Output Files', 'Output Files')}[/bold blue]",
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
