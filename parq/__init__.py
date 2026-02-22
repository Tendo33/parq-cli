"""
parq-cli: A powerful command-line tool for inspecting and analyzing Apache Parquet files.
"""

__author__ = "SimonSun"


def _get_version() -> str:
    """
    动态获取版本号,保持单一真相源(pyproject.toml)

    优先级:
    1. 安装模式: 从 importlib.metadata 读取
    2. 开发模式: 从 pyproject.toml 读取
    3. 降级: 返回默认值

    All heavy imports (re, pathlib, importlib.metadata) are deferred
    into this function so that ``import parq`` stays cheap until
    ``__version__`` is actually accessed.
    """
    try:
        from importlib.metadata import version

        return version("parq-cli")
    except Exception:
        pass

    try:
        import re
        from pathlib import Path

        pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
        if pyproject_path.exists():
            content = pyproject_path.read_text(encoding="utf-8")
            match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
            if match:
                return match.group(1)
    except Exception:
        pass

    return "unknown"


def __getattr__(name: str):
    if name == "__version__":
        value = _get_version()
        globals()["__version__"] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
