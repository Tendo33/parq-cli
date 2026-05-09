# Repository Guidelines

## Project Structure & Module Organization
Core package code lives in `parq/`:
- `parq/cli.py` defines the Typer CLI entrypoint (`parq`).
- `parq/reader.py` handles Parquet IO and data extraction.
- `parq/output.py` and `parq/plain_output.py` format terminal/plain outputs.

Tests are in `tests/` (for example `tests/test_cli.py`, `tests/test_reader.py`, `tests/test_workflows.py`).  
Supporting assets and utilities:
- `examples/` for sample-data scripts and usage examples
- `data/` for local sample parquet files
- `scripts/` for release/version automation
- `.github/workflows/` for CI test and publish pipelines

## Build, Test, and Development Commands
- `uv sync --extra dev` or `pip install -e ".[dev]"`: install dev dependencies.
- `parq --help`: verify CLI entrypoint works.
- `pytest -m "not performance"`: run default CI-style test set.
- `pytest tests/test_performance.py -m performance -q -s`: run performance-only tests.
- `pytest --cov=parq --cov-report=html`: generate coverage report in `htmlcov/`.
- `ruff check parq tests`: lint checks.
- `ruff check --fix parq tests`: auto-fix lint issues where possible.

## Coding Style & Naming Conventions
Target Python is `>=3.10`. Use 4-space indentation and keep lines within 100 chars (`[tool.ruff] line-length = 100`).  
Follow Python naming conventions:
- modules/functions/variables: `snake_case`
- classes: `PascalCase`
- constants: `UPPER_SNAKE_CASE`

Keep CLI option names explicit and consistent with existing commands (for example `--file-count`, `--record-count`, `--columns`).

## Testing Guidelines
Use `pytest` for all tests. Name test files `test_*.py` and test functions `test_*`.  
Mark performance scenarios with `@pytest.mark.performance`; they are excluded from standard CI runs.  
When changing CLI behavior, add or update tests in `tests/test_cli.py` and output-focused tests (`tests/test_output.py`, `tests/test_plain_output.py`).

## Commit & Pull Request Guidelines
Use Conventional Commit-style messages seen in history: `feat:`, `fix:`, `perf:`, `chore:` (example: `feat: add --columns option to head and tail`).  
Before opening a PR, run lint + tests locally and include:
- what changed and why
- affected commands/examples (copy-pasteable)
- any compatibility or performance impact

For releases, prefer `python scripts/bump_version.py <patch|minor|major> --yes` and push the generated version tag (`v*`) to trigger publish workflow.
