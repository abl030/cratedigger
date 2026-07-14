#!/usr/bin/env python3
"""JSON entry point for the pinned-Beets exact library delete operation."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import msgspec


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.beets_delete import (  # noqa: E402
    BeetsDeleteOutcome,
    BeetsDeleteRequest,
    execute_pinned_beets_delete,
)


def main() -> None:
    os.umask(0o002)
    try:
        request = msgspec.json.decode(sys.stdin.buffer.read(), type=BeetsDeleteRequest)
        outcome: BeetsDeleteOutcome = execute_pinned_beets_delete(request)
    except Exception as exc:  # noqa: BLE001 -- stderr + nonzero protocol failure
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    sys.stdout.buffer.write(msgspec.json.encode(outcome))


if __name__ == "__main__":
    main()
