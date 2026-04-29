"""Pytest configuration: add dags/ to sys.path so tests can import dag modules."""

import sys
from pathlib import Path

DAGS_DIR = Path(__file__).parent / "dags"
sys.path.insert(0, str(DAGS_DIR))
