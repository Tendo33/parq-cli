# Directory Structure

- `parq/cli.py`: Typer app, command definitions, global options, error handling, lazy reader/formatter loading.
- `parq/reader.py`: `MultiFormatReader`, Parquet reader re-export, diff and merge public functions.
- `parq/output.py`: Rich terminal formatting.
- `parq/plain_output.py`: plain and JSON formatters for automation.
- `parq/formats/_parquet.py`: Parquet-specific reader implementation.
- `parq/formats/_csv.py`: CSV/TSV batch iteration and metadata scanning.
- `parq/formats/_xlsx.py`: XLSX scanning, sheet resolution, and lazy `openpyxl` handling.
- `parq/formats/_chunk_writers.py`: streaming split/write helpers.
- `parq/formats/_common.py`: shared validation, stats, schema, and preview utilities.
- `scripts/`: version, release, and benchmark helpers.
- `tests/`: CLI, reader, output, workflow, performance, and release script tests.
