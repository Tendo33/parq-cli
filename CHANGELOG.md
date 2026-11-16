# Changelog

## Unreleased

## 0.2.4

- CSV and TSV reads accept `--encoding` (default `utf8`). The flag works before a subcommand and on it, and the subcommand value wins. An unknown name fails with `Unknown encoding`. Writes stay UTF-8.
- `--encoding` is only validated for CSV and TSV input. A script that passes it globally no longer fails on a parquet or xlsx file.
- A CSV or TSV column that cannot be decoded with `--encoding` fails the read, including a header whose name is not valid in that encoding. A bad byte after the first block names that column. UTF-16 read as UTF-8 reports the NUL bytes instead of a column-count error. PyArrow would otherwise return an undecoded data column as raw bytes.
- `--encoding punycode` is rejected. PyArrow passes a buffer to that decoder and it raises AttributeError.
- `diff` reads `.xlsx` files. It parses the selected sheet in full so a value past the first 1000 rows can widen a column before types are compared. Rows are kept only up to 100,000 per side; a larger sheet is parsed again in batches instead of held in memory.
- A type difference stays in `schema_type_mismatches`, including with `--summary-only`. The text produced by `str(value)` when a number, date, or boolean is widened is not a cell edit, in any input format. Any other text is. Keys stay exact, so `1` and `"1"` do not match.
- `diff` can sample rows whose keys are null next to numbers or strings. A Decimal NaN key matches another Decimal NaN. A list or struct column used as a key is rejected with the column name.
- Parquet `head` stops after the requested rows, including inside one large row group. A preview no longer reads that whole group.
- Streamed `tail` keeps the trailing Arrow batches and slices them. CSV `tail` no longer converts every cell to Python.

## 0.2.3

- `merge --force` and `convert --force` replace an existing output file only after the new file is fully written. A schema mismatch or a write error leaves the previous file in place, and the temporary file is removed, including on Windows.
- `diff` treats NaN as equal to NaN, including when NaN is the key.
- `diff` and `merge` close Parquet file handles when they finish.
- JSON table output summarizes binary values, matching rich and plain output.
- `split --force` replaces existing part files only after every part is written. A later write error leaves the previous parts in place.
- Column stats ignore NaN when computing min, max, and mean. Nulls are still counted separately, and NaN stays in the non-null count.
- An empty, multi-character, or non-ASCII `--delimiter` is rejected with `delimiter must be a single ASCII character` before CSV parsing or writing.
- `stats --limit`, `stats --top-n`, and `diff --limit` reject negative values instead of slicing from the end. Zero still means an empty result.
- Selecting a duplicated column name reports that the name cannot be selected, instead of claiming the column does not exist. A unique column in the same file can still be selected.
- `diff` compares repeated column names by position. A diff that would compare those names by default asks for `--columns` instead of claiming the column does not exist.

## 0.2.2

- `merge --force` and `convert` now refuse an output path that is one of the inputs. Previously `--force` deleted that input before reading it.
- XLSX workbook XML is parsed with `defusedxml` (added to the `xlsx` extra).
- Table output escapes cell markup and summarizes binary values (#4).
- Errors include the file path (#5).
- Dependency security updates: certifi, urllib3, idna, pygments, cryptography, pyarrow.

## 0.2.1

- `diff` and `merge` honor the global `--delimiter` and `--sheet` options.
- `.tsv` is a supported output format for `convert`, `split`, and `merge`.
- XLSX `schema` still samples the first 1000 rows. Full reads widen a column when a later row does not fit that sample.
- String cardinality and top values stay exact past 10,000 distinct values. Nulls are not counted as empty strings.
- `diff` spills inputs larger than 100,000 rows per side into temporary files.
- `split --record-count` progress stays indeterminate while the total is still unknown.
- PyPI uploads run from a `v*` tag and use the repository `PYPI_API_TOKEN`.
- XLSX reads use `python-calamine`. A full read parses the workbook once and reuses those rows.
- CSV `convert` no longer counts every row before writing when a progress bar is shown.
- `--output`, `--delimiter`, and `--sheet` work before a subcommand and on the subcommand. The value on the subcommand wins when both are set.

## 0.2.0

- Inspect, split, convert, diff, and merge Parquet, CSV, TSV, and XLSX files.
