# Testing

Important suites:

- `tests/test_cli.py`: command behavior and errors
- `tests/test_reader.py`: reader and format support
- `tests/test_output.py` and `tests/test_plain_output.py`: output modes
- `tests/test_workflows.py`: end-to-end CLI workflows
- `tests/test_performance.py`: performance-marked scenarios
- `tests/test_release_scripts.py`: version/release helpers
- `tests/test_package_meta.py`: package metadata

Default verification excludes performance tests. Run performance tests when changing split, convert, stats, preview, or large-file paths.
