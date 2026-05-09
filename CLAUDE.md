# Claude Project Entrypoint

Use Trellis as the working memory for this repository:

1. Read [.trellis/spec/README.md](.trellis/spec/README.md)
2. Read [.trellis/spec/backend/index.md](.trellis/spec/backend/index.md)
3. Read [.trellis/spec/shared/verification.md](.trellis/spec/shared/verification.md)

Keep this file thin. Long-lived CLI, reader, format, output, release, and verification facts belong in `.trellis/spec/`.

## Guardrails

- parq-cli is a Python command-line tool for Parquet, CSV, TSV, and XLSX files.
- There is no frontend layer.
- Package baseline is Python `>=3.10`.
- Default tests exclude performance scenarios; run performance tests only when relevant.
- Keep `parq` CLI output stable for automation, especially `plain` and `json` output modes.
