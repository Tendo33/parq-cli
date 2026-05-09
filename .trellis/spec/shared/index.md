# Shared Spec Index

## Current Product

parq-cli is a local CLI package. It prioritizes fast startup, lazy optional imports, streaming data paths where possible, and stable terminal/plain/json output for automation.

## Pre-Development Checklist

- Read [dependencies.md](dependencies.md) before changing package metadata or optional dependencies.
- Read [project-docs.md](project-docs.md) before changing README/docs/examples.
- Read [code-quality.md](code-quality.md) before code changes.
- Read [verification.md](verification.md) before claiming completion.

## Quality Check

- Python baseline and CI matrix agree with `requires-python`.
- CLI help, README examples, and tests agree.
- Large-file code paths avoid unnecessary full-table materialization.
- Release scripts and version metadata stay aligned.
