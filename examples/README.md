# Examples

这个目录包含示例 Parquet 文件和使用示例。

## 生成示例数据

运行以下命令生成示例 Parquet 文件：

```bash
python examples/create_sample_data.py
```

这将创建以下示例文件：

- `simple.parquet` - 简单的示例数据（5行）
- `large.parquet` - 较大的数据集（1000行）
- `types.parquet` - 展示各种数据类型

提示:
- `.xlsx` 输入需要先安装可选依赖: `pip install "parq-cli[xlsx]"`
- 只有表头的 CSV/XLSX 会显示空表和列名; 完全空的 CSV 会返回 `Empty CSV file`

## 使用示例

### 1. 查看文件元数据

```bash
parq meta examples/simple.parquet
```

输出示例：
```
╭─────────────────────── 📊 Parquet File Metadata ───────────────────────╮
│ file_path: examples/simple.parquet                                     │
│ num_rows: 5                                                            │
│ num_columns: 5                                                         │
│ num_row_groups: 1                                                      │
│ format_version: 2.6                                                    │
│ serialized_size: 1234                                                  │
│ created_by: parquet-cpp-arrow version 18.0.0                          │
╰────────────────────────────────────────────────────────────────────────╯
```

### 2. 查看 Schema

```bash
parq schema examples/simple.parquet
```

### 3. 预览数据（前 N 行）

```bash
parq head -n 3 examples/simple.parquet
```

### 4. 查看最后几行

```bash
parq tail -n 2 examples/simple.parquet
```

### 5. 统计行数

```bash
parq count examples/simple.parquet
```

### 6. 组合使用

```bash
# 先查看 schema，再查看行数
parq schema examples/simple.parquet
parq count examples/simple.parquet

# 查看 schema 后预览前 5 行
parq schema examples/simple.parquet
parq head -n 5 examples/simple.parquet
```

## 数据类型示例

查看包含多种数据类型的示例：

```bash
parq schema examples/types.parquet
```

这将展示：
- int32, int64 整数类型
- float 浮点类型
- string 字符串类型
- bool 布尔类型
- date 日期类型
- nullable 可空类型

## 大数据集示例

处理大数据集：

```bash
# 查看前 10 行
parq head -n 10 examples/large.parquet

# 查看总行数
parq count examples/large.parquet

# 查看 schema
parq schema examples/large.parquet
```
