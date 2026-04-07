# 快速开始指南

## 📦 安装

### 1. 克隆项目

```bash
git clone <your-repo-url>
cd parq-cli
```

### 2. 安装依赖

#### 使用 pip（推荐开发环境）

```bash
# 安装开发模式（可编辑安装）
pip install -e .

# 或安装开发依赖
pip install -e ".[dev]"
```

#### 使用 uv（更快）

```bash
uv pip install -e .
```

### 3. 验证安装

```bash
parq --help
parq --version
```

## 🚀 快速使用

### 生成测试数据

首先，生成一些示例 Parquet 文件用于测试：

```bash
python examples/create_sample_data.py
```

### 基础命令

#### 1. 查看文件元数据

```bash
parq meta examples/simple.parquet
```

#### 2. 显示 Schema

```bash
parq schema examples/simple.parquet
```

#### 3. 预览数据

```bash
# 显示前 10 行
parq head -n 10 examples/simple.parquet

# 显示后 5 行（默认）
parq tail examples/simple.parquet
```

#### 4. 统计行数

```bash
parq count examples/simple.parquet
```

#### 5. 组合使用

（已简化为子命令模式，推荐按需分别执行上述命令）

## 🧪 运行测试

### 运行所有测试

```bash
pytest
```

### 运行特定测试

```bash
# 测试 CLI
pytest tests/test_cli.py

# 测试 Reader
pytest tests/test_reader.py
```

### 查看测试覆盖率

```bash
pytest --cov=parq --cov-report=html
# 然后打开 htmlcov/index.html 查看详细报告
```

## 🛠️ 开发

### 代码格式化和检查

```bash
# 使用 Ruff 检查
ruff check parq tests

# 使用 Ruff 自动修复
ruff check --fix parq tests
```

### 类型检查（可选）

```bash
pip install mypy
mypy parq
```

## 📋 常见问题

### Q: 如何处理大文件？

A: Parquet 文件会尽量利用元数据和 row group 做更高效的读取；CSV 会按批次流式处理，XLSX 会按行增量处理，因此预览、计数和分割都不再需要先整表载入内存。对于超大数据集，Parquet 仍然会有更好的整体吞吐。

### Q: 支持哪些 Parquet 版本？

A: 支持所有 PyArrow 支持的 Parquet 版本（1.0 和 2.x）。

### Q: 如何贡献代码？

A: 
1. Fork 本仓库
2. 创建特性分支
3. 提交更改
4. 运行测试确保通过
5. 提交 Pull Request

## 🎯 下一步

- 查看完整文档：[README.md](README.md)
- 查看示例：[examples/README.md](examples/README.md)
- 查看发布说明：[docs/RELEASE.md](docs/RELEASE.md)

## 💡 提示

- 使用 `parq --help` 查看所有可用选项
- 大文件建议先用 `parq count FILE` 查看行数，再用 `parq head -n N FILE` 预览数据
- 结合使用多个选项可以快速了解文件内容
