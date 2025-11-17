# 发布流程快速指南

## 🚀 快速发布新版本

### 一键发布流程 (推荐)

```bash
# 1. 确保所有测试通过
pytest

# 2. 自动升级版本、创建 commit 和 tag
python scripts/bump_version.py patch --yes  # Bug 修复
# 或
python scripts/bump_version.py minor --yes  # 新功能
# 或
python scripts/bump_version.py major --yes  # 重大变更

# 3. 推送到 GitHub (自动触发 CI/CD)
git push origin main
git push origin v0.1.2  # 替换为实际版本号
```

就这么简单!GitHub Actions 会自动:
- ✅ 检查版本冲突
- ✅ 运行测试
- ✅ 构建包
- ✅ 发布到 PyPI
- ✅ 创建 GitHub Release

## 📋 语义化版本选择指南

| 变更类型 | 版本类型 | 命令 | 示例 |
|---------|---------|------|------|
| 🐛 Bug 修复 | PATCH | `bump_version.py patch` | 0.1.0 → 0.1.1 |
| ✨ 新功能 (向下兼容) | MINOR | `bump_version.py minor` | 0.1.0 → 0.2.0 |
| 💥 破坏性变更 | MAJOR | `bump_version.py major` | 0.1.0 → 1.0.0 |

## 🔍 发布前检查清单

- [ ] 所有测试通过: `pytest`
- [ ] 代码检查通过: `ruff check parq tests`
- [ ] 版本号没有冲突: `python scripts/check_version.py`
- [ ] CHANGELOG 已更新
- [ ] 文档已更新

## ⚠️ 常见问题解决

### 问题 1: PyPI 版本冲突

**错误信息:**
```
ERROR HTTPError: 400 Bad Request from https://upload.pypi.org/legacy/
File already exists ('parq_cli-0.1.0-py3-none-any.whl'...)
```

**解决方案:**
```bash
# 升级版本号
python scripts/bump_version.py patch --yes
git push origin main
git push origin v0.1.1  # 新版本号
```

### 问题 2: Tag 已存在

**错误信息:**
```
fatal: tag 'v0.1.0' already exists
```

**解决方案:**
```bash
# 删除本地和远程 tag
git tag -d v0.1.0
git push origin :refs/tags/v0.1.0

# 重新创建
python scripts/bump_version.py --version 0.1.1 --yes
```

### 问题 3: GitHub Actions 失败

**检查步骤:**
1. 查看 Actions 日志: https://github.com/Tendo33/parq-cli/actions
2. 确认 PyPI token 配置正确 (Repository Settings → Secrets)
3. 运行本地检查: `python scripts/check_version.py`

## 🎯 手动发布 (不推荐)

如果自动化脚本有问题,可以手动操作:

```bash
# 1. 手动修改 pyproject.toml 中的版本号
# version = "0.1.2"

# 2. 检查版本冲突
python scripts/check_version.py

# 3. 创建 commit 和 tag
git add pyproject.toml
git commit -m "chore: bump version to 0.1.2"
git tag -a v0.1.2 -m "Release 0.1.2"

# 4. 推送
git push origin main
git push origin v0.1.2
```

## 📚 更多信息

- **详细文档:** [scripts/README.md](scripts/README.md)
- **工作流配置:** [.github/workflows/publish.yml](.github/workflows/publish.yml)
- **PyPI 页面:** https://pypi.org/project/parq-cli/
- **GitHub Releases:** https://github.com/Tendo33/parq-cli/releases

## 💡 最佳实践

1. **发布前测试:** 始终运行 `pytest` 确保测试通过
2. **使用自动化:** 优先使用 `bump_version.py` 脚本
3. **遵循语义化版本:** 正确选择 major/minor/patch
4. **更新 CHANGELOG:** 每次发布前记录变更
5. **小步快跑:** 频繁发布小版本比大版本更安全

---

**上次更新:** 2025-11-17
**维护者:** SimonSun (@Tendo33)

