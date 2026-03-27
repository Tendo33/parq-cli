#!/usr/bin/env python3
"""
版本管理自动化脚本
用法:
    python scripts/bump_version.py patch  # 0.1.0 -> 0.1.1
    python scripts/bump_version.py minor  # 0.1.0 -> 0.2.0
    python scripts/bump_version.py major  # 0.1.0 -> 1.0.0
    python scripts/bump_version.py --version 0.2.0  # 手动指定版本
"""
# {{CHENGQI:
# Action: Added; Timestamp: 2025-11-17; Reason: 创建自动化版本管理工具; Principle_Applied: DRY, KISS;
# }}

import re
import subprocess
import sys
from pathlib import Path
from typing import Tuple


def get_current_version(pyproject_path: Path) -> str:
    """从 pyproject.toml 读取当前版本"""
    content = pyproject_path.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if not match:
        raise ValueError("无法在 pyproject.toml 中找到版本号")
    return match.group(1)


def parse_version(version: str) -> Tuple[int, int, int]:
    """解析版本号为 (major, minor, patch)"""
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", version)
    if not match:
        raise ValueError(f"无效的版本号格式: {version}")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def bump_version(current: str, bump_type: str) -> str:
    """根据类型递增版本号"""
    major, minor, patch = parse_version(current)

    if bump_type == "major":
        return f"{major + 1}.0.0"
    elif bump_type == "minor":
        return f"{major}.{minor + 1}.0"
    elif bump_type == "patch":
        return f"{major}.{minor}.{patch + 1}"
    else:
        raise ValueError(f"无效的版本递增类型: {bump_type}")


def update_pyproject(pyproject_path: Path, new_version: str) -> None:
    """更新 pyproject.toml 中的版本号"""
    content = pyproject_path.read_text(encoding="utf-8")
    new_content = re.sub(
        r'^(version\s*=\s*["\'])([^"\']+)(["\'])',
        rf"\g<1>{new_version}\g<3>",
        content,
        flags=re.MULTILINE,
    )
    pyproject_path.write_text(new_content, encoding="utf-8")
    print(f"[OK] 已更新 pyproject.toml: version = '{new_version}'")


def check_git_status() -> bool:
    """检查 git 工作区是否干净"""
    result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    return len(result.stdout.strip()) == 0


def create_git_commit_and_tag(version: str, auto_push: bool = False) -> None:
    """创建 git commit 和 tag"""
    # 添加 pyproject.toml
    subprocess.run(["git", "add", "pyproject.toml"], check=True)

    # 创建 commit
    subprocess.run(["git", "commit", "-m", f"chore: bump version to {version}"], check=True)
    print(f"[OK] 已创建 commit: 'chore: bump version to {version}'")

    # 创建 tag
    tag_name = f"v{version}"
    subprocess.run(["git", "tag", "-a", tag_name, "-m", f"Release {version}"], check=True)
    print(f"[OK] 已创建 tag: {tag_name}")

    if auto_push:
        subprocess.run(["git", "push", "origin", "main"], check=True)
        subprocess.run(["git", "push", "origin", tag_name], check=True)
        print("[OK] 已推送到远程仓库")


def main():
    """主函数"""
    # 检查参数
    if len(sys.argv) < 2:
        print("[ERROR] 用法: python scripts/bump_version.py [major|minor|patch|--version VERSION]")
        print("\n示例:")
        print("  python scripts/bump_version.py patch      # 0.1.0 -> 0.1.1")
        print("  python scripts/bump_version.py minor      # 0.1.0 -> 0.2.0")
        print("  python scripts/bump_version.py major      # 0.1.0 -> 1.0.0")
        print("  python scripts/bump_version.py --version 0.2.0")
        sys.exit(1)

    # 获取项目根目录
    root_dir = Path(__file__).parent.parent
    pyproject_path = root_dir / "pyproject.toml"

    if not pyproject_path.exists():
        print(f"[ERROR] 找不到 pyproject.toml 文件: {pyproject_path}")
        sys.exit(1)

    if not check_git_status():
        print("[ERROR] Git 工作区不干净，请先提交或暂存当前改动")
        print("[HINT] 运行 `git status --short` 查看未提交文件")
        sys.exit(1)

    # 获取当前版本
    current_version = get_current_version(pyproject_path)
    print(f"[INFO] 当前版本: {current_version}")

    # 确定新版本
    arg = sys.argv[1]
    if arg == "--version":
        if len(sys.argv) < 3:
            print("[ERROR] --version 需要指定版本号")
            sys.exit(1)
        new_version = sys.argv[2]
    elif arg in ["major", "minor", "patch"]:
        new_version = bump_version(current_version, arg)
    else:
        print(f"[ERROR] 无效的参数 '{arg}'")
        sys.exit(1)

    print(f"[INFO] 新版本: {new_version}")

    # 检查是否需要推送
    auto_push = "--push" in sys.argv

    # 确认
    if "--yes" not in sys.argv and "-y" not in sys.argv:
        response = input(f"\n是否继续更新版本到 {new_version}? (y/N): ")
        if response.lower() not in ["y", "yes"]:
            print("[INFO] 已取消")
            sys.exit(0)

    # 更新版本
    update_pyproject(pyproject_path, new_version)

    # Git 操作
    print("\n[CHECK] 检查 git 状态...")
    try:
        create_git_commit_and_tag(new_version, auto_push)

        if not auto_push:
            print("\n📌 下一步操作:")
            print("   git push origin main")
            print(f"   git push origin v{new_version}")
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Git 操作失败: {e}")
        print("[HINT] 请手动执行:")
        print("   git add pyproject.toml")
        print(f"   git commit -m 'chore: bump version to {new_version}'")
        print(f"   git tag -a v{new_version} -m 'Release {new_version}'")
        print("   git push origin main")
        print(f"   git push origin v{new_version}")
        sys.exit(1)

    print("\n[OK] 版本更新完成! GitHub Actions 将自动发布到 PyPI")


if __name__ == "__main__":
    main()
