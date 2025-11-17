#!/usr/bin/env python3
"""
版本检查工具 - 在 CI/CD 中使用,防止重复发布
用法:
    python scripts/check_version.py
"""
# {{CHENGQI:
# Action: Added; Timestamp: 2025-11-17; Reason: 创建版本检查工具防止PyPI冲突; Principle_Applied: 防御性编程;
# }}

import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path


def get_local_version(pyproject_path: Path) -> str:
    """从 pyproject.toml 读取本地版本"""
    content = pyproject_path.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if not match:
        raise ValueError("无法在 pyproject.toml 中找到版本号")
    return match.group(1)


def get_pypi_versions(package_name: str) -> list:
    """从 PyPI 获取已发布的版本列表"""
    url = f"https://pypi.org/pypi/{package_name}/json"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())
            return list(data["releases"].keys())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # 包不存在,这是第一次发布
            return []
        raise
    except Exception as e:
        print(f"⚠️  警告: 无法从 PyPI 获取版本信息: {e}")
        return []


def check_version_conflict(local_version: str, pypi_versions: list) -> bool:
    """检查版本是否冲突"""
    if local_version in pypi_versions:
        print(f"❌ 错误: 版本 {local_version} 已在 PyPI 上存在!")
        print(f"\n📋 PyPI 已有版本: {', '.join(sorted(pypi_versions, reverse=True)[:10])}")
        print("\n💡 解决方案:")
        print("   1. 使用脚本升级版本:")
        print(f"      python scripts/bump_version.py patch   # {local_version} -> 下一个补丁版本")
        print(f"      python scripts/bump_version.py minor   # {local_version} -> 下一个次版本")
        print("   2. 手动修改 pyproject.toml 中的版本号")
        return True
    return False


def main():
    """主函数"""
    # 获取项目根目录
    root_dir = Path(__file__).parent.parent
    pyproject_path = root_dir / "pyproject.toml"

    if not pyproject_path.exists():
        print("❌ 错误: 找不到 pyproject.toml")
        sys.exit(1)

    # 读取包名和版本
    content = pyproject_path.read_text(encoding="utf-8")
    name_match = re.search(r'^name\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if not name_match:
        print("❌ 错误: 无法在 pyproject.toml 中找到包名")
        sys.exit(1)

    package_name = name_match.group(1)
    local_version = get_local_version(pyproject_path)

    print(f"📦 包名: {package_name}")
    print(f"📦 本地版本: {local_version}")
    print("🔍 检查 PyPI 版本...")

    # 获取 PyPI 版本
    pypi_versions = get_pypi_versions(package_name)

    if not pypi_versions:
        print("✅ 这是首次发布到 PyPI")
        sys.exit(0)

    # 检查冲突
    if check_version_conflict(local_version, pypi_versions):
        sys.exit(1)

    print("✅ 版本检查通过! 可以发布到 PyPI")
    print(f"📋 PyPI 最新版本: {max(pypi_versions, key=lambda v: [int(x) for x in v.split('.')])}")


if __name__ == "__main__":
    main()
