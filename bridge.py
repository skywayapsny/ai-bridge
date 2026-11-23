#!/usr/bin/env python3
"""Compat shim — real implementation lives in the aibridge package.

Keeps `python3 bridge.py` working (docs, muscle memory, scripts).
New entry points: `python3 -m aibridge`, `ai-bridge` (pip install -e .).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aibridge.server import main

if __name__ == "__main__":
    main()
