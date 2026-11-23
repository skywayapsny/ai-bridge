#!/usr/bin/env python3
"""Compat shim — real implementation lives in aibridge.worker.

Keeps `python3 worker.py` and the systemd unit working.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aibridge.worker import main

if __name__ == "__main__":
    raise SystemExit(main())
