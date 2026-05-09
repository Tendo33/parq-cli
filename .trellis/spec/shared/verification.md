# Verification

## General Checks

```bash
git status --short --branch
rg -n "ai_docs|START_HERE|sync_ai_adapters|check_ai_docs" . \
  -g "!node_modules" -g "!.git" -g "!dist" -g "!build" -g "!htmlcov" -g "!.coverage"
git diff --check
```

## Quality Gate

```bash
uv sync --extra dev --extra xlsx
uv run ruff check parq tests scripts
uv run ruff format --check parq tests scripts
uv run pytest -m "not performance"
```

## Optional Checks

```bash
uv run pytest tests/test_performance.py -m performance -q -s
uv run pytest --cov=parq --cov-report=html -m "not performance"
uv run python scripts/check_version.py
```
