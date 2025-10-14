"""
Entry point for running parq as a module.
Allows execution via: python -m parq
"""

from parq.cli import app

if __name__ == "__main__":
    app()

# {{CHENGQI:
# Action: Modified; Timestamp: 2025-10-14 HH:MM:SS +08:00;
# Reason: Restored to use app() with callback-based design;
# Principle_Applied: KISS
# }}
