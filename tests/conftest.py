"""Pytest configuration: register asyncio mode and package path."""
import sys
from pathlib import Path

# Ensure the project root is on sys.path so `import core` works
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
