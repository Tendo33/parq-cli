#!/bin/bash
# 发布脚本 - Linux/Mac

set -e  # 遇到错误立即退出

PYTHON_BIN="./.venv/bin/python"
PYTEST_BIN="./.venv/bin/pytest"
TWINE_BIN="./.venv/bin/twine"
RUFF_BIN="./.venv/bin/ruff"

if [ ! -x "$PYTHON_BIN" ]; then
    echo "❌ 未找到项目虚拟环境 Python: $PYTHON_BIN"
    echo "💡 先运行: uv sync --extra dev --extra xlsx"
    exit 1
fi

echo "🚀 开始发布 parq-cli 到 PyPI..."
echo ""

# 1. 检查是否有未提交的更改
if [[ -n $(git status -s) ]]; then
    echo "⚠️  警告: 有未提交的更改"
    git status -s
    read -p "是否继续? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 2. 运行测试
echo "🧪 运行测试..."
$PYTEST_BIN -m "not performance"
echo "✅ 测试通过"
echo ""

# 3. 代码质量检查
echo "🔍 代码质量检查..."
$RUFF_BIN check parq tests
echo "✅ 代码检查通过"
echo ""

# 4. 清理旧构建
echo "🧹 清理旧构建..."
rm -rf dist/ build/ *.egg-info/
echo "✅ 清理完成"
echo ""

# 5. 构建包
echo "📦 构建分发包..."
$PYTHON_BIN -m build
echo "✅ 构建完成"
echo ""

# 6. 检查包
echo "🔎 检查包完整性..."
$TWINE_BIN check dist/*
echo "✅ 包检查通过"
echo ""

# 7. 询问发布目标
echo "📤 选择发布目标:"
echo "  1) TestPyPI (测试环境)"
echo "  2) PyPI (正式环境)"
read -p "请选择 (1/2): " target

if [ "$target" = "1" ]; then
    echo ""
    echo "📤 上传到 TestPyPI..."
    $TWINE_BIN upload --repository testpypi dist/*
    echo ""
    echo "✅ 发布到 TestPyPI 成功！"
    echo "🔗 https://test.pypi.org/project/parq-cli/"
    echo ""
    echo "测试安装命令:"
    echo "  pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple parq-cli"
elif [ "$target" = "2" ]; then
    echo ""
    read -p "⚠️  确认发布到 PyPI 正式环境? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        echo ""
        echo "📤 上传到 PyPI..."
        $TWINE_BIN upload dist/*
        echo ""
        echo "🎉 发布到 PyPI 成功！"
        echo "🔗 https://pypi.org/project/parq-cli/"
        echo ""
        echo "安装命令:"
        echo "  pip install parq-cli"
        
        # 创建 Git 标签
        VERSION=$(
            $PYTHON_BIN - <<'PY'
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

with open("pyproject.toml", "rb") as f:
    print(tomllib.load(f)["project"]["version"])
PY
        )
        read -p "是否创建 Git 标签 v$VERSION? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            git tag -a "v$VERSION" -m "Release version $VERSION"
            git push origin "v$VERSION"
            echo "✅ Git 标签已创建并推送"
        fi
    else
        echo "❌ 发布取消"
        exit 1
    fi
else
    echo "❌ 无效选择"
    exit 1
fi

echo ""
echo "🎊 发布流程完成！"
