"""Structural checks for shell forms forbidden in the deploy-pin helper."""

from __future__ import annotations

import re


_UNQUOTED_GIT_FORMAT = re.compile(
    r"(?<!['\"])--format=%G\?(?!['\"])",
)
_ZSH_READONLY_STATUS = re.compile(r"\blocal\s+status=\$\?")


def find_shell_contract_violations(source: str) -> tuple[str, ...]:
    """Return the historical caller-shell-sensitive forms in ``source``."""
    violations: list[str] = []
    if _UNQUOTED_GIT_FORMAT.search(source):
        violations.append("unquoted --format=%G?")
    if _ZSH_READONLY_STATUS.search(source):
        violations.append("local status=$?")
    return tuple(violations)
