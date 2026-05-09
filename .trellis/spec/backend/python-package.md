# Python Package And Release

## Package Metadata

- Distribution: `parq-cli`
- CLI entrypoint: `parq = "parq.cli:app"`
- Python baseline: `>=3.10`
- Optional extra: `xlsx`
- Dev extra: tests, Ruff, build, twine, vulture

## Release Flow

- `scripts/bump_version.py` updates package version and creates version tags.
- `scripts/check_version.py` checks PyPI version conflicts.
- `scripts/publish.sh` is the local publish helper.
- GitHub publish workflow runs tests, builds, checks, uploads artifacts, publishes to PyPI, then creates a GitHub release.

Keep `pyproject.toml`, `parq/__init__.py`, docs, tests, and workflows aligned when release behavior changes.
