# parq-cli Trellis Spec

parq-cli is a Python command-line tool for inspecting, transforming, converting, diffing, and merging tabular files. It supports Parquet, CSV, TSV, and XLSX workflows through a Typer CLI and PyArrow-backed reader utilities.

This repository is not a Python/Vite full-stack project. There is no frontend layer.

## Source Order

1. `README.md`, `README_CN.md`, `docs/`, `examples/`, and `scripts/README.md`
2. `pyproject.toml`, `uv.lock`, `.github/workflows/*`, and release scripts
3. `parq/` and `tests/`
4. `.trellis/spec/`

## Spec Layers

- [backend](backend/index.md): CLI commands, reader, format modules, output contracts, and release scripts
- [shared](shared/index.md): dependencies, docs, code quality, and verification
- [guides](guides/index.md): task flow and review checklist
- [big questions](big-question/index.md): large-file and release boundaries that need care

## Non-Negotiables

- Python baseline is `>=3.10`.
- CLI entrypoint is `parq = "parq.cli:app"`.
- `plain` and `json` output modes are automation-facing contracts.
- Do not add a frontend, API server, or database to this project without a separate product scope.
