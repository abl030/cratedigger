"""Validate the active external Beets authority and print stable JSON."""

from __future__ import annotations

import argparse
import configparser
import os
import sys

import confuse
import msgspec

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.beets_config_contract import (
    BeetsConfigError,
    BeetsConfigReport,
    check_beets_config,
)
from lib.config import read_runtime_config_strict


class CheckerResult(msgspec.Struct, frozen=True):
    """Stable command wire shape for both admitted and load-error worlds."""

    ok: bool
    report: BeetsConfigReport | None = None
    error: str | None = None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate Cratedigger's external Beets configuration contract"
    )
    parser.add_argument("--config", required=True, help="immutable runtime config.ini")
    parser.add_argument("--runtime-dir", required=True, help="mutable Cratedigger state directory")
    parser.add_argument(
        "--role",
        required=True,
        choices=("main", "importer", "preview", "web"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        cfg = read_runtime_config_strict(args.config, args.runtime_dir)
    except (
        OSError,
        UnicodeError,
        configparser.Error,
        confuse.ConfigError,
        msgspec.ValidationError,
        ValueError,
    ) as exc:
        # Native parser/load detail intentionally remains on stderr (KD9); the
        # machine channel stays token-free and schema-stable.
        print(f"Beets configuration load failed: {exc}", file=sys.stderr)
        print(msgspec.json.encode(CheckerResult(ok=False, error="config_load_error")).decode())
        return 1

    try:
        report = check_beets_config(cfg, role=args.role)
    except (
        OSError,
        UnicodeError,
        configparser.Error,
        confuse.ConfigError,
        msgspec.ValidationError,
        BeetsConfigError,
    ) as exc:
        # Native parser/load detail intentionally remains on stderr (KD9); the
        # machine channel stays token-free and schema-stable.
        print(f"Beets configuration load failed: {exc}", file=sys.stderr)
        print(msgspec.json.encode(CheckerResult(ok=False, error="config_load_error")).decode())
        return 1

    for warning in report.warnings:
        print(f"WARNING [{warning.code}] {warning.message}", file=sys.stderr)
    for failure in report.hard_failures:
        print(f"ERROR [{failure.code}] {failure.message}", file=sys.stderr)
    print(msgspec.json.encode(CheckerResult(ok=report.ok, report=report)).decode())
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
