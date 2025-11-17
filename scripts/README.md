# 版本管理工具使用指南

## 📚 工具概览

### 1. `bump_version.py` - 自动版本升级

自动升级版本号、创建 git commit 和 tag。

**快速使用:**

```bash
# 补丁版本升级 (0.1.0 -> 0.1.1) - Bug 修复
python scripts/bump_version.py patch

# 次版本升级 (0.1.0 -> 0.2.0) - 新功能
python scripts/bump_version.py minor

# 主版本升级 (0.1.0 -> 1.0.0) - 重大变更
python scripts/bump_version.py major

# 手动指定版本
python scripts/bump_version.py --version 0.2.5

# 自动推送到远程
python scripts/bump_version.py patch --push

# 跳过确认
python scripts/bump_version.py patch --yes
```

**工作流程:**
1. ✅ 从 `pyproject.toml` 读取当前版本
2. ✅ 根据类型计算新版本号
3. ✅ 更新 `pyproject.toml`
4. ✅ 创建 git commit
5. ✅ 创建 git tag (格式: `vX.Y.Z`)
6. ✅ (可选) 自动推送到远程

### 2. `check_version.py` - 版本冲突检查

检查当前版本是否已在 PyPI 上存在,防止重复发布。

**快速使用:**

```bash
# 检查版本冲突
python scripts/check_version.py
```

**使用场景:**
- ✅ 手动发布前检查
- ✅ CI/CD 流程中自动检查 (已集成到 `.github/workflows/publish.yml`)

## 🚀 完整发布流程

### 方案 A: 使用自动化脚本 (推荐)

```bash
# 1. 升级版本并自动创建 tag
python scripts/bump_version.py patch --yes

# 2. 推送到远程 (触发 GitHub Actions)
git push origin main
git push origin v0.1.1  # 替换为实际版本号

# 3. GitHub Actions 自动完成:
#    - 版本冲突检查
#    - 构建包
#    - 发布到 PyPI
#    - 创建 GitHub Release
```

### 方案 B: 手动流程

```bash
# 1. 检查版本是否冲突
python scripts/check_version.py

# 2. 手动修改 pyproject.toml 中的版本号
# version = "0.1.1"

# 3. 提交并创建 tag
git add pyproject.toml
git commit -m "chore: bump version to 0.1.1"
git tag -a v0.1.1 -m "Release 0.1.1"

# 4. 推送
git push origin main
git push origin v0.1.1
```

## 📋 语义化版本规范

遵循 [Semantic Versioning 2.0.0](https://semver.org/):

- **主版本号 (MAJOR):** 不兼容的 API 变更
  ```bash
  python scripts/bump_version.py major  # 1.0.0 -> 2.0.0
  ```

- **次版本号 (MINOR):** 向下兼容的新功能
  ```bash
  python scripts/bump_version.py minor  # 1.0.0 -> 1.1.0
  ```

- **补丁版本号 (PATCH):** 向下兼容的 Bug 修复
  ```bash
  python scripts/bump_version.py patch  # 1.0.0 -> 1.0.1
  ```

## 🔧 CI/CD 集成

版本检查已自动集成到 GitHub Actions 发布流程:

```yaml
# .github/workflows/publish.yml
- name: 🔍 Check version conflict
  run: python scripts/check_version.py
```

**工作原理:**
1. 当推送 `v*` tag 时触发 workflow
2. 自动检查版本是否已存在于 PyPI
3. 如果冲突,立即失败并提示
4. 通过检查后继续构建和发布

## ⚠️ 常见问题

### Q1: 版本已存在怎么办?

```bash
# 升级到下一个版本
python scripts/bump_version.py patch --yes
git push origin main
git push origin v0.1.2
```

### Q2: 如何撤销错误的 tag?

```bash
# 删除本地 tag
git tag -d v0.1.1

# 删除远程 tag
git push origin :refs/tags/v0.1.1
```

### Q3: GitHub Actions 失败了怎么办?

1. 检查 Actions 日志
2. 确认版本号没有冲突
3. 检查 PyPI token 配置 (`PYPI_API_TOKEN`)

## 📝 最佳实践

1. ✅ **每次发布前运行检查:**
   ```bash
   python scripts/check_version.py
   ```

2. ✅ **使用自动化脚本避免手误:**
   ```bash
   python scripts/bump_version.py patch --yes
   ```

3. ✅ **遵循语义化版本规范**

4. ✅ **在 CHANGELOG.md 中记录变更**

5. ✅ **测试通过后再发布:**
   ```bash
   pytest  # 运行测试
   python scripts/bump_version.py patch --push
   ```

## 📞 需要帮助?

- 查看脚本源码: `scripts/bump_version.py`, `scripts/check_version.py`
- 查看 CI 配置: `.github/workflows/publish.yml`
- 提交 Issue: [GitHub Issues](https://github.com/Tendo33/parq-cli/issues)

