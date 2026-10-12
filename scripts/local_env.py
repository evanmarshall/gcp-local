#!/usr/bin/env python3
"""Manage one private, disposable Compose environment from a source checkout.

This wrapper keeps the documented ``python3 scripts/local_env.py`` entry point;
the implementation is the ``gcp_local`` package behind the ``gcp-local`` CLI.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gcp_local.bootstrap import initialize, load, private_json
from gcp_local.environment import main

__all__ = ["initialize", "load", "main", "private_json"]

if __name__ == "__main__":
    sys.exit(main())
