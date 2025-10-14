"""
Entry point for running parq as a module.
Allows execution via: python -m parq
"""

from parq.cli import app

if __name__ == "__main__":
    app()

# {{CHENGQI:
# Action: Created; Timestamp: 2025-10-14 16:19:00 +08:00;
# Reason: Module entry point for python -m parq execution;
# Principle_Applied: KISS
# }}
