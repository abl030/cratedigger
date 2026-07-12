#!/usr/bin/env python3
"""Reject GitHub issue references that can close an issue on merge.

This audit is intentionally narrow: feed it a pull-request body or branch
commit messages.  It recognizes only GitHub's closing-keyword/reference
grammar, leaving ordinary prose about closing work untouched.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import sys
from typing import Sequence


_CLOSING_KEYWORDS = (
    "close",
    "closes",
    "closed",
    "fix",
    "fixes",
    "fixed",
    "resolve",
    "resolves",
    "resolved",
)
_NAME = r"[A-Za-z0-9_.-]+"
_ISSUE_REFERENCE = (
    rf"(?:https://github\.com/{_NAME}/{_NAME}/issues/[0-9]+"
    rf"|{_NAME}/{_NAME}#[0-9]+"
    r"|#[0-9]+)"
)
_CLOSING_REFERENCE = re.compile(
    rf"(?<![\w])"
    rf"(?P<keyword>{'|'.join(_CLOSING_KEYWORDS)})"
    rf"(?![\w])"
    rf"(?:[ \t\r\n]*:[ \t\r\n]*|[ \t\r\n]+)"
    rf"(?P<reference>{_ISSUE_REFERENCE})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ClosingIssueReference:
    """One auto-closing keyword/reference pair in an audited surface."""

    keyword: str
    reference: str
    line: int
    column: int


def find_closing_issue_references(
    text: str,
) -> tuple[ClosingIssueReference, ...]:
    """Return every GitHub auto-closing issue reference in *text*."""
    violations: list[ClosingIssueReference] = []
    for match in _CLOSING_REFERENCE.finditer(text):
        start = match.start("keyword")
        line = text.count("\n", 0, start) + 1
        previous_newline = text.rfind("\n", 0, start)
        column = start - previous_newline
        violations.append(
            ClosingIssueReference(
                keyword=match.group("keyword"),
                reference=match.group("reference"),
                line=line,
                column=column,
            )
        )
    return tuple(violations)


def _read_inputs(paths: Sequence[str]) -> tuple[tuple[str, str], ...]:
    if paths:
        return tuple(
            (path, Path(path).read_text(encoding="utf-8")) for path in paths
        )
    return (("stdin", sys.stdin.read()),)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "files",
        nargs="*",
        help="PR-body/commit-message files; reads stdin when omitted",
    )
    args = parser.parse_args(argv)

    found = False
    for label, text in _read_inputs(args.files):
        for violation in find_closing_issue_references(text):
            found = True
            print(
                f"{label}:{violation.line}:{violation.column}: "
                f"GitHub closing reference is forbidden: "
                f"{violation.keyword} {violation.reference}",
                file=sys.stderr,
            )
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())
