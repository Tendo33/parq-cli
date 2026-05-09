# Cross-Layer Thinking Guide

For parq-cli, "cross-layer" means CLI flags, reader behavior, output formatters, docs, tests, and release metadata must agree.

Before changing behavior, trace:

```text
README/docs example
  -> Typer command/option in parq/cli.py
  -> MultiFormatReader or format helper
  -> OutputFormatter / PlainOutputFormatter / JsonOutputFormatter
  -> tests
```

Ask:

- Does this alter a public command, flag, or output shape?
- Does the change affect rich, plain, and json output modes?
- Does CSV/TSV/XLSX behavior still handle large files without avoidable full materialization?
- Does optional XLSX support still fail with an actionable install message?
- Do release/version docs or workflows need updates?
