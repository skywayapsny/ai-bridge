#!/usr/bin/env python3
"""Compat shim — real implementation lives in aibridge.poll."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aibridge.poll import main

if __name__ == "__main__":
    main()
