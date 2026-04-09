# parq-cli

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A command-line tool for inspecting, transforming, and comparing tabular files.

[Chinese README](https://github.com/Tendo33/parq-cli/blob/main/README_CN.md)

## Overview

`parq` focuses on the workflows that come up most often when working with `.parquet`, `.csv`, and `.xlsx` files:

- inspect metadata and schema
- preview the first or last rows
- count rows
- split large files
- compute lightweight column stats
- convert between supported formats
- diff two datasets by key
- merge compatible files

The CLI keeps startup light with lazy imports, preserves `plain` and `json` output modes for automation, and avoids unnecessary full-table materialization for large CSV/XLSX workflows where possible.

## Installation

```bash
pip install parq-cli
```

Enable `.xlsx` support with the optional dependency:

```bash
pip install "parq-cli[xlsx]"
```

## Quick Start

```bash
# Inspect metadata
parq meta data.parquet
parq meta --fast data.csv

# Show schema
parq schema data.xlsx

# Preview rows
parq head data.parquet
parq head -n 10 --columns id,name data.csv
parq tail -n 20 data.csv

# Count rows
parq count data.parquet

# Split files
parq split data.csv --record-count 100000 -n "chunks/part-%03d.csv"
parq split data.parquet --file-count 4 -n "chunks/part-%02d.parquet"

# Column statistics
parq stats sales.parquet --columns amount,discount --limit 10

# Format conversion
parq convert raw.xlsx cleaned.parquet
parq convert source.parquet export.csv --columns id,name,status

# Dataset diff
parq diff old.parquet new.parquet --key id --columns status,amount
parq diff left.csv right.csv --key id --summary-only

# Merge compatible inputs
parq merge part-001.parquet part-002.parquet merged.parquet
```

## Supported Formats

| Command | Parquet | CSV | XLSX |
| --- | --- | --- | --- |
| `meta` | yes | yes | yes |
| `schema` | yes | yes | yes |
| `head` / `tail` | yes | yes | yes |
| `count` | yes | yes | yes |
| `split` | yes | yes | yes |
| `stats` | yes | yes | yes |
| `convert` | yes | yes | yes |
| `diff` | yes | yes | no, convert first |
| `merge` | yes | yes | yes |

`XLSX` support requires `openpyxl`.

## Command Reference

### `meta`

```bash
parq meta FILE
parq meta --fast FILE
```

Shows file-level metadata such as path, format, column count, file size, row-group count, and when available, row count and Parquet-specific metadata.

Use `--fast` when you want a cheap metadata pass on CSV/XLSX files. In fast mode, expensive fields such as full row counts are skipped.

### `schema`

```bash
parq schema FILE
```

Shows column names, types, and nullable information.

### `head` and `tail`

```bash
parq head FILE
parq head -n 20 FILE
parq head -n 20 --columns id,name FILE

parq tail FILE
parq tail -n 20 FILE
parq tail -n 20 --columns id,name FILE
```

Notes:

- default preview size is `5`
- `--columns` accepts a comma-separated list
- missing files return a friendly error with exit code `1`
- empty header-only CSV/XLSX files return an empty preview with detected columns
- an empty csv with no header raises a friendly `Empty CSV file` error

### `count`

```bash
parq count FILE
```

Returns the total row count.

### `split`

```bash
parq split FILE --file-count N
parq split FILE --record-count N
parq split FILE --record-count 100000 -n "chunks/part-%03d.parquet"
```

Splits one input file into multiple output files.

Rules:

- specify exactly one of `--file-count` or `--record-count`
- output format is inferred from `--name-format`
- existing target files are not overwritten
- in `--record-count` mode, CSV/XLSX now stream in a single pass instead of pre-counting the entire file

### `stats`

```bash
parq stats FILE
parq stats FILE --columns amount,discount
parq stats FILE --limit 20
```

Computes simple per-column statistics.

- numeric columns include `count`, `null_count`, `min`, `max`, `mean`
- non-numeric columns include `count` and `null_count`
- default `--limit` is `50` to avoid flooding the terminal on very wide tables

### `convert`

```bash
parq convert SOURCE OUTPUT
parq convert SOURCE OUTPUT --columns id,name,status
```

Converts a supported input file to another supported output format. The output format is determined by the `OUTPUT` suffix.

Notes:

- current targets are `.parquet`, `.csv`, and `.xlsx`
- conversion is streaming-based where possible
- existing output files raise an error instead of being overwritten

### `diff`

```bash
parq diff LEFT RIGHT --key id
parq diff LEFT RIGHT --key id1,id2 --columns status,amount
parq diff LEFT RIGHT --key id --summary-only
```

Compares two datasets by key and reports:

- row count delta
- rows only present on the left
- rows only present on the right
- changed rows for the selected columns
- schema-only columns and same-name type mismatches

Notes:

- `--key` is required
- `diff` currently supports Parquet and CSV inputs
- XLSX files should be converted first
- duplicate keys on either side are treated as an error
- `--summary-only` keeps the counts and omits sample payloads

### `merge`

```bash
parq merge INPUT1 INPUT2 OUTPUT
parq merge chunks/*.parquet merged.parquet
```

Merges multiple compatible input files into a single output file. The last positional argument is the output path.

Notes:

- schemas must be identical or safely unifiable by Arrow
- existing output files are not overwritten
- output format is inferred from the output suffix

## Output Modes

Global options:

- `--version`, `-v`: show version information
- `--output`, `-o`: select output format
- `--help`: show command help

Available output modes:

- `rich`: human-friendly terminal rendering
- `plain`: low-overhead tabular output for shell pipelines
- `json`: machine-readable structured output

Examples:

```bash
parq meta data.parquet --output json
parq stats data.csv --output plain
parq diff left.parquet right.parquet --key id --summary-only --output json
```

On Windows terminals that cannot safely render emoji or extended characters, Rich headings automatically fall back to a safe plain style instead of crashing.

## Large File Notes

- Parquet metadata, row counts, and previews use Arrow metadata and row-group shortcuts where available.
- CSV `tail` uses a fixed-size column window instead of materializing every row as Python dicts.
- CSV/XLSX `split --record-count` streams in one pass.
- `meta --fast` is the best option when you need quick metadata from large CSV/XLSX inputs.
- XLSX schema inference samples the first 1000 rows instead of scanning the entire sheet up front.

For repeated heavy workflows, converting large CSV/XLSX files to Parquet is still the best path for throughput.

## Development

Install development dependencies:

```bash
uv sync --extra dev
```

or:

```bash
pip install -e ".[dev]"
```

Useful commands:

```bash
python -m parq --help
pytest -m "not performance"
pytest tests/test_performance.py -m performance -q -s
ruff check parq tests
ruff check --fix parq tests
pytest --cov=parq --cov-report=html
```

## Status

Implemented:

- metadata and schema inspection
- head and tail preview
- row counting
- file splitting
- column statistics
- format conversion
- keyed dataset diff
- compatible file merge

Planned improvements are now centered on deeper performance tuning, richer diff workflows, and broader reporting capabilities rather than adding the core commands from scratch.

## License

[MIT](LICENSE)
