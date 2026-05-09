# Large File Streaming

parq-cli should avoid full-table materialization for large CSV/XLSX paths where existing code streams batches.

Before changing split, convert, stats, head/tail, diff, or merge behavior, inspect whether the current implementation intentionally streams. Add performance or workflow coverage when changing those paths.
