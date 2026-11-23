#!/usr/bin/env python3
"""Compat shim — real implementation lives in aibridge.agent."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aibridge.agent import main

if __name__ == "__main__":
    raise SystemExit(main())
