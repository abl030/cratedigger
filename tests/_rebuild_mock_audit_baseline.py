"""Re-snapshot tests/mock_audit_baseline.json from the current tree.

Run this after a PR that legitimately migrates anti-pattern call sites
to fakes — the audit then accepts the new (smaller) baseline as
authoritative.

Usage:
    python3 tests/_rebuild_mock_audit_baseline.py
"""

from __future__ import annotations

import json
import os
import sys

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

from _mock_audit_scanner import scan_tree  # noqa: E402

BASELINE_PATH = os.path.join(THIS_DIR, "mock_audit_baseline.json")


def main() -> None:
    tree = scan_tree()
    with open(BASELINE_PATH, "w", encoding="utf-8") as f:
        json.dump(tree, f, indent=2, sort_keys=True)
        f.write("\n")
    total = sum(sum(v.values()) for v in tree.values())
    print(f"Baseline rewritten: {total} findings across {len(tree)} files")


if __name__ == "__main__":
    main()
