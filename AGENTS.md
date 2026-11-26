# parq-cli

纯 Python 命令行工具，读写 Parquet、CSV、TSV、XLSX。没有前端。Python `>=3.10`，命令用 Typer。

## 契约

- 保持 Typer 命令签名稳定。
- 保持 `rich` / `plain` / `json` 三种输出模式，其中 `plain` 与 `json` 常被自动化消费，字段和格式不要随手改。
- 保持大文件上的流式行为。
- 默认测试排除 performance 场景。

## 结构

- `parq/cli.py`：命令入口。`parq/reader.py`：读取逻辑。`parq/formats/*`：各格式实现。输出格式化器单独成模块。
- `tests/`：CLI、reader、output 三组用例。

## 命令

```bash
pytest -m "not performance"
pytest tests/test_performance.py -m performance -q -s
ruff check parq tests
```

## 容易踩的点

- `parq schema` 对 XLSX 先采样前 1000 行；完整读取时用 python-calamine 解析一次，遇到更宽的值会扩列。写 XLSX 仍走 openpyxl。
- Parquet `head` 会停在大 row group 内部；`tail` 在最后几行同属一个 group 时仍会读整个 group。CSV 类型推断用 PyArrow 的 1 MiB 块。
- `diff` 与 `merge` 使用全局 `--delimiter`、`--sheet`、`--encoding`。`diff` 读 XLSX 走完整 sheet 解析，每侧最多保留 100,000 行，超过则重新分批解析，两侧都落到临时文件。CSV/TSV 的 `--encoding` 只作用于读取，列名或内容解码失败即报错；对 parquet 与 xlsx 输入不做校验。写出始终 UTF-8。
- `.tsv` 输入输出都支持，输出用 tab 分隔。
