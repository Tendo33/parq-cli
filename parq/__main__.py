"""
Entry point for running parq as a module.
Allows execution via: python -m parq
"""

from parq.cli import app

if __name__ == "__main__":
    app()

# {{CHENGQI:
# Action: Modified; Timestamp: 2025-10-14 18:07:04 +08:00;
# Reason: Entry point using app() with command-based design;
# Principle_Applied: KISS, Typer best practices
# }}
