#!/bin/bash
# 发布脚本 - Linux/Mac

set -e  # 遇到错误立即退出

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
pytest
if [ $? -ne 0 ]; then
    echo "❌ 测试失败，发布中止"
    exit 1
fi
echo "✅ 测试通过"
echo ""

# 3. 代码质量检查
echo "🔍 代码质量检查..."
ruff check parq tests
echo "✅ 代码检查通过"
echo ""

# 4. 清理旧构建
echo "🧹 清理旧构建..."
rm -rf dist/ build/ *.egg-info/
echo "✅ 清理完成"
echo ""

# 5. 构建包
echo "📦 构建分发包..."
python -m build
echo "✅ 构建完成"
echo ""

# 6. 检查包
echo "🔎 检查包完整性..."
twine check dist/*
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
    twine upload --repository testpypi dist/*
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
        twine upload dist/*
        echo ""
        echo "🎉 发布到 PyPI 成功！"
        echo "🔗 https://pypi.org/project/parq-cli/"
        echo ""
        echo "安装命令:"
        echo "  pip install parq-cli"
        
        # 创建 Git 标签
        VERSION=$(python -c "import tomli; print(tomli.load(open('pyproject.toml', 'rb'))['project']['version'])")
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

