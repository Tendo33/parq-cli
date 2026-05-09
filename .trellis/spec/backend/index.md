# CLI/Core Spec Index

## Current Core

The `parq` CLI is implemented with Typer. The core data access layer is `MultiFormatReader`, backed by PyArrow and format-specific helpers.

Primary commands:

- `meta`
- `schema`
- `head`
- `tail`
- `count`
- `split`
- `stats`
- `convert`
- `diff`
- `merge`

## Pre-Development Checklist

- Read [directory-structure.md](directory-structure.md) before moving modules.
- Read [http-api-when-added.md](http-api-when-added.md) for CLI command contracts.
- Read [python-package.md](python-package.md) before changing package metadata, entrypoints, release scripts, or versioning.
- Read [config-logging.md](config-logging.md) before changing output/error behavior.
- Read [database-when-added.md](database-when-added.md) before changing file IO or persistence assumptions.
- Read [type-safety.md](type-safety.md) before typing/schema changes.
- Read [testing.md](testing.md) before verification.

## Quality Check

- CLI flags and README examples remain accurate.
- `plain` and `json` output modes remain machine-usable.
- CSV/XLSX large-file paths stay streaming where existing code streams.
- Optional XLSX dependency errors remain actionable.
