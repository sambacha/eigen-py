"""
Command-line interface for EigenLayer analysis.
"""

from .main import main
from .commands import setup_commands

__all__ = [
    "main",
    "setup_commands"
]