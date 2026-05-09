# Type Safety

- Keep public reader methods typed with concrete return expectations.
- Use `Path` for file-system command arguments.
- Keep output mode as the `OutputFormat` enum.
- Validate external strings such as column lists, sheet identifiers, output patterns, and file suffixes before using them.
- Avoid leaking PyArrow-specific exceptions directly through CLI commands when a friendly `ValueError` fits.
