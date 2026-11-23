#!/usr/bin/env python3
"""Compat shim — real implementation lives in aibridge.queueio.

Lets `import queueio` keep working for scripts run from this directory.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aibridge.queueio import *  # noqa: F401,F403
from aibridge.queueio import QUEUE_DIR  # noqa: F401  (module constant)
