# parq-cli

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

一个强大的 Apache Parquet 文件命令行工具 🚀

[English](https://github.com/Tendo33/parq-cli/blob/main/README.md) | 简体中文

## ✨ 特性

- 📊 **元数据查看**: 快速查看 Parquet 文件的元数据信息（行数、列数、文件大小、压缩类型等）
- 📋 **Schema 展示**: 美观地展示文件的列结构和数据类型
- 👀 **数据预览**: 支持查看文件的前 N 行或后 N 行
- 🔢 **行数统计**: 快速获取文件的总行数
- ✂️ **文件分割**: 将大型 Parquet 文件分割成多个较小的文件
- 🗜️ **压缩信息**: 显示文件压缩类型和文件大小
- 🎨 **美观输出**: 使用 Rich 库提供彩色、格式化的终端输出
- 📦 **智能显示**: 自动检测嵌套结构，显示逻辑列数和物理列数

## 📦 安装

```bash
pip install parq-cli
```

## 🚀 快速开始

### 基本用法

```bash
# 查看文件元数据
parq meta data.parquet

# 显示 schema 信息
parq schema data.parquet

# 显示前 5 行（默认）
parq head data.parquet

# 显示前 10 行
parq head -n 10 data.parquet

# 显示后 5 行（默认）
parq tail data.parquet

# 显示后 20 行
parq tail -n 20 data.parquet

# 显示总行数
parq count data.parquet

# 将文件分割成 3 个部分
parq split data.parquet --file-count 3

# 每个文件包含 1000 条记录
parq split data.parquet --record-count 1000
```

## 📖 命令参考

### 查看元数据

```bash
parq meta FILE
```

显示 Parquet 文件的元数据信息（行数、列数、文件大小、压缩类型等）。

### 查看 Schema

```bash
parq schema FILE
```

显示 Parquet 文件的列结构和数据类型。

### 预览数据

```bash
# 显示前 N 行（默认 5 行）
parq head FILE
parq head -n N FILE

# 显示后 N 行（默认 5 行）
parq tail FILE
parq tail -n N FILE
```

说明：
- `N` 必须是非负整数。
- 当输入文件不存在时，parq 会以退出码 `1` 退出并输出友好错误信息。

### 统计信息

```bash
# 显示总行数
parq count FILE
```

### 分割文件

```bash
# 分割成 N 个文件
parq split FILE --file-count N

# 每个文件包含 M 条记录
parq split FILE --record-count M

# 自定义输出格式
parq split FILE -f N -n "output-%03d.parquet"

# 分割到子目录
parq split FILE -f 3 -n "output/part-%02d.parquet"
```

将 Parquet 文件分割成多个较小的文件。你可以指定输出文件的数量（`--file-count`）或每个文件的记录数（`--record-count`）。输出文件名根据 `--name-format` 参数格式化（默认：`result-%06d.parquet`）。

### 全局选项

- `--version, -v`: 显示版本信息
- `--help`: 显示帮助信息

## 🎨 输出示例

### 元数据展示

**普通文件（无嵌套结构）：**

```bash
$ parq meta data.parquet
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

**嵌套结构文件（显示物理列数）：**

```bash
$ parq meta nested.parquet
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

### Schema 展示

```bash
$ parq schema data.parquet
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

## 🛠️ 技术栈

- **[PyArrow](https://arrow.apache.org/docs/python/)**: 高性能的 Parquet 读取引擎
- **[Typer](https://typer.tiangolo.com/)**: 现代化的 CLI 框架
- **[Rich](https://rich.readthedocs.io/)**: 美观的终端输出

## 🧪 开发

### 安装开发依赖

```bash
# 推荐使用 uv
uv sync --extra dev

# 或使用 pip
pip install -e ".[dev]"
```

### 运行测试

```bash
pytest
```

### 运行测试（带覆盖率）

```bash
pytest --cov=parq --cov-report=html
```

### 代码格式化和检查

```bash
# 使用 Ruff 检查和自动修复

ruff check --fix parq tests

# 检查可能的无用代码
vulture parq tests scripts
```

## 🗺️ 路线图

- [x] 基础元数据查看
- [x] Schema 展示
- [x] 数据预览（head/tail）
- [x] 行数统计
- [x] 文件大小和压缩信息显示
- [x] 嵌套结构智能识别（逻辑列数 vs 物理列数）
- [x] 添加split命令，将一个parquet文件拆分成多个parquet文件
- [ ] 数据统计分析
- [ ] 添加convert命令，将一个parquet文件转换成其他格式（CSV, JSON, Excel）
- [ ] 添加diff命令，比较两个parquet文件的差异
- [ ] 添加merge命令，将多个parquet文件合并成一个parquet文件

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

- 灵感来源于 [parquet-cli](https://github.com/chhantyal/parquet-cli)
- 感谢 Apache Arrow 团队提供强大的 Parquet 支持
- 感谢 Rich 库为终端输出增添色彩

## 📮 联系方式

- 作者: SimonSun
- 项目地址: https://github.com/Tendo33/parq-cli

---

**⭐ 如果这个项目对你有帮助，请给个 Star！**
