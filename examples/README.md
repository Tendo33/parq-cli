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

## 使用示例

### 1. 查看文件元数据

```bash
parq examples/simple.parquet
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
parq examples/simple.parquet --schema
```

### 3. 预览数据（前 N 行）

```bash
parq examples/simple.parquet --head 3
```

### 4. 查看最后几行

```bash
parq examples/simple.parquet --tail 2
```

### 5. 统计行数

```bash
parq examples/simple.parquet --count
```

### 6. 组合使用

```bash
# 同时显示 schema 和行数
parq examples/simple.parquet --schema --count

# 显示 schema 和前 5 行
parq examples/simple.parquet --schema --head 5
```

## 数据类型示例

查看包含多种数据类型的示例：

```bash
parq examples/types.parquet --schema
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
parq examples/large.parquet --head 10

# 查看总行数
parq examples/large.parquet --count

# 查看 schema
parq examples/large.parquet --schema
```

