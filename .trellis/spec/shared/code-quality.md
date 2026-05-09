# Code Quality

- Keep CLI command implementations thin; put reusable data logic in `parq/reader.py` or `parq/formats/*`.
- Preserve lazy imports in CLI paths to keep startup fast.
- Keep line length at 100 characters.
- Use Python 3.10+ syntax, but avoid unnecessary cleverness in CLI-facing error handling.
- Convert file/format errors into friendly CLI messages with exit code `1`.
- Add or update tests for CLI flags, output shapes, reader behavior, and release scripts.
