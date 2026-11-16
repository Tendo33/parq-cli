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
- Parquet `head` stops inside a large row group. `tail` still reads the whole group when the last rows share one group. CSV type inference still uses PyArrow's 1 MiB block.
- `diff` and `merge` use the global `--delimiter`, `--sheet`, and `--encoding`. `diff` reads XLSX after a full sheet parse. It keeps at most 100,000 rows per side in memory, then spills to temporary files. CSV/TSV `--encoding` applies to reads; a column or header that does not decode fails the read. Writes stay UTF-8.
- `.tsv` is valid for both input and output. Output uses a tab delimiter.
