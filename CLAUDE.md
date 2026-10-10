# Claude Project Entrypoint


## Guardrails

- parq-cli is a Python command-line tool for Parquet, CSV, TSV, and XLSX files.
- There is no frontend layer.
- Package baseline is Python `>=3.10`.
- Default tests exclude performance scenarios; run performance tests only when relevant.
- Keep `parq` CLI output stable for automation, especially `plain` and `json` output modes.

## Commands

```bash
pytest -m "not performance"
ruff check parq tests
pytest tests/test_performance.py -m performance -q -s
```

## Gotchas

- `parq schema` on XLSX samples the first 1000 rows. Full reads parse the sheet once with python-calamine and widen a column when a later value does not fit. Writing XLSX still uses openpyxl.
- `diff` and `merge` use the global `--delimiter` and `--sheet`. `diff` keeps at most 100,000 rows per side in memory, then spills to temporary files.
- `.tsv` is valid for both input and output. Output uses a tab delimiter.
