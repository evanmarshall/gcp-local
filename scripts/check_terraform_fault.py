#!/usr/bin/env python3
"""Require the upstream assertion to reject deliberately incorrect bucket RPO."""

import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
result = subprocess.run([sys.executable, str(root / 'scripts/run_terraform.py'),
                         '--test', 'TestAccStorageBucket_basic', '--output', '.artifacts/terraform-fault'],
                        capture_output=True, text=True, timeout=220)
print(result.stdout, end='')
print(result.stderr, end='', file=sys.stderr)
report = json.loads((root / '.artifacts/terraform-fault/results.json').read_text())
if (result.returncode == 0 or report['timed_out'] or len(report['tests']) != 1
        or report['tests'][0]['Action'] != 'fail'
        or "Attribute 'rpo' found when not expected" not in result.stdout):
    raise SystemExit('The negative control did not fail on the intended upstream RPO assertion')
print('Terraform negative control passed: incorrect bucket metadata was detected.')
