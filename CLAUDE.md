# Claude Project Entrypoint


## Guardrails

- parq-cli is a Python command-line tool for Parquet, CSV, TSV, and XLSX files.
- There is no frontend layer.
- Package baseline is Python `>=3.10`.
- Default tests exclude performance scenarios; run performance tests only when relevant.
- Keep `parq` CLI output stable for automation, especially `plain` and `json` output modes.
