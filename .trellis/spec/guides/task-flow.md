# Task Flow

1. Identify the affected surface: CLI command, reader, format helper, output formatter, docs, scripts, or CI.
2. Read the relevant Trellis spec index and source files.
3. Make the smallest change that preserves command and output contracts.
4. Add or update focused tests for changed behavior.
5. Run focused checks, then the shared verification gate.
6. Update `.trellis/spec/` when the change alters long-lived behavior or conventions.
