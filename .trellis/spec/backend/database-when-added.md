# File IO And Persistence

parq-cli has no database or service runtime. It reads user-supplied tabular files and writes user-requested output files.

Rules:

- Never overwrite output files unless `--force` is set.
- Validate split output patterns before writing.
- Clean up partial split files when a writer fails.
- Stream CSV/XLSX split/convert paths where possible to avoid full materialization.
- Preserve input files unless a command explicitly writes to a separate output path.
