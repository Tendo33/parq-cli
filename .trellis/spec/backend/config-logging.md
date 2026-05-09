# Output And Errors

parq-cli does not have a runtime config file or logging subsystem. Its user-visible behavior is terminal output.

## Output Modes

- `rich`: human-friendly tables and progress display
- `plain`: stable plain text for shell pipelines
- `json`: machine-readable JSON for automation

## Error Handling

Use `_run_with_error_handling` for CLI operations. Convert known file/value/overwrite errors to formatter errors and exit code `1`. Keep unexpected errors readable without dumping implementation internals by default.
