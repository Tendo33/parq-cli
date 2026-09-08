# Project Agent Entrypoint

This file is the cross-tool entrypoint for parq-cli.

## Working Rules

- parq-cli is a pure Python CLI package, not a full-stack or frontend project.
- Keep Python on `>=3.10`.
- Preserve Typer command contracts, rich/plain/json output modes, and streaming behavior for large CSV/XLSX workflows.

## Execution Style

- Read `parq/cli.py`, `parq/reader.py`, relevant `parq/formats/*`, output formatters, and tests before editing.
- Keep changes small and covered by CLI/reader/output tests.
- Run focused tests first, then the repository verification gate.
