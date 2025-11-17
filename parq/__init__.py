"""
parq-cli: A powerful command-line tool for inspecting and analyzing Apache Parquet files.
"""

import re
from pathlib import Path

__author__ = "SimonSun"


def _get_version() -> str:
    """
    动态获取版本号,保持单一真相源(pyproject.toml)
    
    优先级:
    1. 开发模式: 从 pyproject.toml 读取
    2. 安装模式: 从 importlib.metadata 读取
    3. 降级: 返回默认值
    """
    # 尝试从已安装的包元数据读取 (pip install 后)
    try:
        from importlib.metadata import version
        return version("parq-cli")
    except Exception:
        pass
    
    # 尝试从 pyproject.toml 读取 (开发模式)
    try:
        pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
        if pyproject_path.exists():
            content = pyproject_path.read_text(encoding="utf-8")
            match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
            if match:
                return match.group(1)
    except Exception:
        pass
    
    # 降级: 返回默认值
    return "unknown"


__version__ = _get_version()
