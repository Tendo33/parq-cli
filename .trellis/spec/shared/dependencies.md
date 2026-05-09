# Dependencies

## Runtime

- Python `>=3.10`
- Typer for CLI commands
- Rich for terminal output
- PyArrow for Parquet/CSV/TSV data access and writing
- Optional `openpyxl` for XLSX support via `parq-cli[xlsx]`

## Development

- pytest and pytest-cov
- Ruff
- build, twine
- vulture

Use `uv sync --extra dev` for local development. Use `pip install -e ".[dev,xlsx]"` when matching CI/publish workflows.
