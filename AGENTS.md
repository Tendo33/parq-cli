# Project Agent Entrypoint

This file is the cross-tool entrypoint for parq-cli.

## Read Order

1. Start at [.trellis/spec/README.md](.trellis/spec/README.md)
2. Use [.trellis/spec/backend/index.md](.trellis/spec/backend/index.md) before changing CLI, reader, format, output, or release code
3. Use [.trellis/spec/shared/verification.md](.trellis/spec/shared/verification.md) before claiming completion

## Working Rules

- Treat `.trellis/spec/` as the detailed source of truth for AI-assisted work.
- parq-cli is a pure Python CLI package, not a full-stack or frontend project.
- Keep Python on `>=3.10`.
- Preserve Typer command contracts, rich/plain/json output modes, and streaming behavior for large CSV/XLSX workflows.
- Update Trellis specs whenever commands, formats, output contracts, scripts, release flow, or verification commands change.

## Execution Style

- Read `parq/cli.py`, `parq/reader.py`, relevant `parq/formats/*`, output formatters, and tests before editing.
- Keep changes small and covered by CLI/reader/output tests.
- Run focused tests first, then the repository verification gate.
