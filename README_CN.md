# parq-cli

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

一个用于查看、转换、对比和处理表格文件的命令行工具。

[English](https://github.com/Tendo33/parq-cli/blob/main/README.md)

## 简介

`parq` 面向日常最常见的表格文件操作，支持 `.parquet`、`.csv`、`.xlsx`：

- 查看元数据和 schema
- 预览前几行或后几行
- 统计总行数
- 按文件数或记录数拆分文件
- 生成轻量列统计
- 在多种格式之间转换
- 按主键对比两份数据
- 合并兼容文件

CLI 保持惰性导入，启动成本低；同时保留 `plain` 和 `json` 输出模式，方便脚本集成。对大 CSV/XLSX 的若干场景也尽量避免无意义的整表物化。

## 安装

```bash
pip install parq-cli
```

如果你需要 `.xlsx` 支持，请安装可选依赖：

```bash
pip install "parq-cli[xlsx]"
```

## 快速开始

```bash
# 查看元数据
parq meta data.parquet
parq meta --fast data.csv

# 查看 schema
parq schema data.xlsx

# 预览数据
parq head data.parquet
parq head -n 10 --columns id,name data.csv
parq tail -n 20 data.csv

# 统计总行数
parq count data.parquet

# 拆分文件
parq split data.csv --record-count 100000 -n "chunks/part-%03d.csv"
parq split data.parquet --file-count 4 -n "chunks/part-%02d.parquet"

# 列统计
parq stats sales.parquet --columns amount,discount --limit 10

# 格式转换
parq convert raw.xlsx cleaned.parquet
parq convert source.parquet export.csv --columns id,name,status

# 数据对比
parq diff old.parquet new.parquet --key id --columns status,amount
parq diff left.csv right.csv --key id --summary-only

# 合并文件
parq merge part-001.parquet part-002.parquet merged.parquet
```

## 支持的格式

| 命令 | Parquet | CSV | XLSX |
| --- | --- | --- | --- |
| `meta` | 支持 | 支持 | 支持 |
| `schema` | 支持 | 支持 | 支持 |
| `head` / `tail` | 支持 | 支持 | 支持 |
| `count` | 支持 | 支持 | 支持 |
| `split` | 支持 | 支持 | 支持 |
| `stats` | 支持 | 支持 | 支持 |
| `convert` | 支持 | 支持 | 支持 |
| `diff` | 支持 | 支持 | 先转换再对比 |
| `merge` | 支持 | 支持 | 支持 |

`XLSX` 依赖 `openpyxl`。

## 命令说明

### `meta`

```bash
parq meta FILE
parq meta --fast FILE
```

显示文件级元数据，例如路径、格式、列数、文件大小、row group 数量，以及在可用时显示行数和 Parquet 特有信息。

`--fast` 适合大 CSV/XLSX 文件的快速探测模式，会跳过完整行数这类代价较高的字段。

### `schema`

```bash
parq schema FILE
```

显示列名、类型和 nullable 信息。

### `head` 与 `tail`

```bash
parq head FILE
parq head -n 20 FILE
parq head -n 20 --columns id,name FILE

parq tail FILE
parq tail -n 20 FILE
parq tail -n 20 --columns id,name FILE
```

说明：

- 默认预览 `5` 行
- `--columns` 支持逗号分隔列名
- 文件不存在时返回友好错误并以退出码 `1` 结束
- 只有表头的 CSV/XLSX 会返回空预览，但仍保留识别到的列
- 没有表头且完全为空的 CSV 会返回友好 `Empty CSV file` 错误

### `count`

```bash
parq count FILE
```

返回总行数。

### `split`

```bash
parq split FILE --file-count N
parq split FILE --record-count N
parq split FILE --record-count 100000 -n "chunks/part-%03d.parquet"
```

把一个输入文件拆成多个输出文件。

规则：

- `--file-count` 与 `--record-count` 二选一
- 输出格式由 `--name-format` 的后缀决定
- 不会覆盖已存在的目标文件
- 在 `--record-count` 模式下，CSV/XLSX 现在走单遍流式拆分，不再先整表计数

### `stats`

```bash
parq stats FILE
parq stats FILE --columns amount,discount
parq stats FILE --limit 20
```

计算按列汇总的轻量统计：

- 数值列输出 `count`、`null_count`、`min`、`max`、`mean`
- 非数值列输出 `count` 和 `null_count`
- 默认 `--limit 50`，避免宽表直接刷满终端

### `convert`

```bash
parq convert SOURCE OUTPUT
parq convert SOURCE OUTPUT --columns id,name,status
```

把一个支持的输入格式转换成另一个支持的输出格式，输出格式由 `OUTPUT` 后缀决定。

说明：

- 当前目标格式为 `.parquet`、`.csv`、`.xlsx`
- 能流式处理的路径会尽量流式处理
- 若输出文件已存在，会直接报错，不会覆盖

### `diff`

```bash
parq diff LEFT RIGHT --key id
parq diff LEFT RIGHT --key id1,id2 --columns status,amount
parq diff LEFT RIGHT --key id --summary-only
```

按主键比较两份数据，并输出：

- 行数差值
- 仅存在于左侧的数据
- 仅存在于右侧的数据
- 选定列上的变更行
- 仅某一侧存在的 schema 列，以及同名异型字段

说明：

- `--key` 必填
- `diff` 当前仅支持 Parquet 和 CSV
- XLSX 需要先转换
- 任一侧若出现重复 key，会直接报错
- `--summary-only` 只保留计数，不输出样本内容

### `merge`

```bash
parq merge INPUT1 INPUT2 OUTPUT
parq merge chunks/*.parquet merged.parquet
```

把多个兼容输入文件合并成一个输出文件。最后一个位置参数是输出路径。

说明：

- schema 必须一致，或能被 Arrow 安全统一
- 不会覆盖已存在的输出文件
- 输出格式由输出文件后缀决定

## 输出模式

全局选项：

- `--version`, `-v`：显示版本号
- `--output`, `-o`：选择输出格式
- `--help`：显示帮助

可选输出模式：

- `rich`：适合人在终端中阅读
- `plain`：低开销、便于 shell 管道处理
- `json`：适合程序消费和集成

示例：

```bash
parq meta data.parquet --output json
parq stats data.csv --output plain
parq diff left.parquet right.parquet --key id --summary-only --output json
```

在 Windows 非 UTF-8 终端中，Rich 标题会自动降级为安全样式，不再因为 emoji 或特殊字符编码失败而崩溃。

## 大文件说明

- Parquet 的元数据、行数和预览会优先利用 Arrow metadata 与 row group 能力。
- CSV `tail` 使用固定大小列窗口，而不是把整份数据都转成 Python dict。
- CSV/XLSX 的 `split --record-count` 走单遍流式处理。
- 对大 CSV/XLSX，`meta --fast` 是最快的元数据探查入口。
- XLSX 的 schema 推断只采样前 1000 行，不再默认整表扫描。

如果你会反复处理大文件，把 CSV/XLSX 先转成 Parquet 依然是吞吐量最好的路径。

## 开发

安装开发依赖：

```bash
uv sync --extra dev
```

或者：

```bash
pip install -e ".[dev]"
```

常用命令：

```bash
python -m parq --help
pytest -m "not performance"
pytest tests/test_performance.py -m performance -q -s
ruff check parq tests
ruff check --fix parq tests
pytest --cov=parq --cov-report=html
```

## 当前状态

已经完成：

- 元数据与 schema 查看
- head / tail 预览
- 行数统计
- 文件拆分
- 列统计
- 格式转换
- 基于主键的数据对比
- 兼容文件合并

后续更适合继续打磨的方向，是更深入的性能优化、更丰富的 diff 工作流，以及更完整的报告能力，而不是再从零补齐这些基础命令。

## License

[MIT](LICENSE)
