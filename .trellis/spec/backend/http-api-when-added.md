# CLI Command Contracts

parq-cli has no HTTP API. Treat Typer commands as the public interface.

## Global Options

- `--version` / `-v`
- `--output` / `-o`: `rich`, `plain`, or `json`
- `--delimiter` / `-d`: CSV/TSV delimiter
- `--sheet`: XLSX sheet name or zero-based index

## Command Rules

- Missing files, invalid formats, invalid columns, existing output targets, and invalid split shapes should produce friendly errors and exit code `1`.
- `--force` controls overwriting output files.
- `--columns` accepts comma-separated names and rejects empty/missing selections.
- TSV is auto-detected by `.tsv` suffix unless an explicit delimiter is provided.
- XLSX support requires `openpyxl`; missing optional dependency must tell users how to install `parq-cli[xlsx]`.
