# Release Publish Boundary

Publishing is tag-driven and PyPI-facing. Version checks, build artifacts, GitHub release creation, and package metadata must stay aligned.

Do not modify publish workflows or scripts as incidental cleanup. Run release-script tests and `scripts/check_version.py` when release behavior changes.
