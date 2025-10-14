# parq-cli

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A powerful command-line tool for Apache Parquet files 🚀

[简体中文](README.md) | English

## ✨ Features

- 📊 **Metadata Viewing**: Quickly view Parquet file metadata (row count, column count, file size, compression type, etc.)
- 📋 **Schema Display**: Beautifully display file column structure and data types
- 👀 **Data Preview**: Support viewing the first N rows or last N rows of a file
- 🔢 **Row Count**: Quickly get the total number of rows in a file
- 🗜️ **Compression Info**: Display file compression type and file size
- 🎨 **Beautiful Output**: Use Rich library for colorful, formatted terminal output
- 📦 **Smart Display**: Automatically detect nested structures, showing logical and physical column counts

## 📦 Installation

### Install from Source

```bash
git clone https://github.com/yourusername/parq-cli.git
cd parq-cli
pip install -e .
```

### Install via pip (Coming Soon)

```bash
pip install parq-cli
```

## 🚀 Quick Start

### Basic Usage

```bash
# View file metadata
parq data.parquet

# Display schema information
parq data.parquet --schema

# Display first 10 rows
parq data.parquet --head 10

# Display last 5 rows
parq data.parquet --tail 5

# Display total row count
parq data.parquet --count
```

### Combined Usage

```bash
# Display schema and row count together
parq data.parquet --schema --count

# Display first 5 rows and schema
parq data.parquet --head 5 --schema
```

## 📖 Command Reference

### Main Command

```
parq FILE [OPTIONS]
```

**Arguments:**
- `FILE`: Path to Parquet file (required)

**Options:**
- `--schema, -s`: Display schema information
- `--head N`: Display first N rows
- `--tail N`: Display last N rows
- `--count, -c`: Display total row count
- `--version, -v`: Display version information
- `--help`: Display help information

## 🎨 Output Examples

### Metadata Display

**Regular File (No Nested Structure):**

```bash
$ parq data.parquet
```

```
╭─────────────────────── 📊 Parquet File Metadata ───────────────────────╮
│ file_path: data.parquet                                                │
│ num_rows: 1000                                                         │
│ num_columns: 5 (logical)                                               │
│ file_size: 123.45 KB                                                   │
│ compression: SNAPPY                                                    │
│ num_row_groups: 1                                                      │
│ format_version: 2.6                                                    │
│ serialized_size: 126412                                                │
│ created_by: parquet-cpp-arrow version 18.0.0                          │
╰────────────────────────────────────────────────────────────────────────╯
```

**Nested Structure File (Shows Physical Column Count):**

```bash
$ parq nested.parquet
```

```
╭─────────────────────── 📊 Parquet File Metadata ───────────────────────╮
│ file_path: nested.parquet                                              │
│ num_rows: 500                                                          │
│ num_columns: 3 (logical)                                               │
│ num_physical_columns: 8 (storage)                                      │
│ file_size: 2.34 MB                                                     │
│ compression: ZSTD                                                      │
│ num_row_groups: 2                                                      │
│ format_version: 2.6                                                    │
│ serialized_size: 2451789                                               │
│ created_by: parquet-cpp-arrow version 21.0.0                          │
╰────────────────────────────────────────────────────────────────────────╯
```

### Schema Display

```bash
$ parq data.parquet --schema
```

```
                    📋 Schema Information
┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ Column Name ┃ Data Type     ┃ Nullable ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ id          │ int64         │ ✗        │
│ name        │ string        │ ✓        │
│ age         │ int64         │ ✓        │
│ city        │ string        │ ✓        │
│ salary      │ double        │ ✓        │
└─────────────┴───────────────┴──────────┘
```

## 🛠️ Tech Stack

- **[PyArrow](https://arrow.apache.org/docs/python/)**: High-performance Parquet reading engine
- **[Typer](https://typer.tiangolo.com/)**: Modern CLI framework
- **[Rich](https://rich.readthedocs.io/)**: Beautiful terminal output

## 🧪 Development

### Install Development Dependencies

```bash
pip install -e ".[dev]"
```

### Run Tests

```bash
pytest
```

### Run Tests (With Coverage)

```bash
pytest --cov=parq --cov-report=html
```

### Code Formatting and Checking

```bash
# Check and auto-fix with Ruff

ruff check --fix parq tests
```

## 🗺️ Roadmap

- [x] Basic metadata viewing
- [x] Schema display
- [x] Data preview (head/tail)
- [x] Row count statistics
- [x] File size and compression information display
- [x] Nested structure smart detection (logical vs physical column count)
- [ ] SQL query support
- [ ] Data statistical analysis
- [ ] Format conversion (CSV, JSON, Excel)
- [ ] File comparison
- [ ] Cloud storage support (S3, GCS, Azure)

## 🤝 Contributing

Issues and Pull Requests are welcome!

1. Fork this repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## 🙏 Acknowledgments

- Inspired by [parquet-cli](https://github.com/chhantyal/parquet-cli)
- Thanks to the Apache Arrow team for powerful Parquet support
- Thanks to the Rich library for adding color to terminal output

## 📮 Contact

- Author: Jinfeng Sun
- Project URL: https://github.com/Tendo33/parq-cli

---

**⭐ If this project helps you, please give it a Star!**

