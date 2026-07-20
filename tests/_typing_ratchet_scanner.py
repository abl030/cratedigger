"""Shared scanner for the production typing escape-hatch ratchet (issue #765).

Production code (everything the scanner walks — repo-root ``*.py``, ``lib/``,
``web/``, ``scripts/``, ``harness/``, ``tools/`` minus the generated vulture
whitelist) is migrating to pyright strict with three escape hatches banned:

1. explicit ``Any``
2. ``cast(...)``
3. ``# type: ignore``

plus **bare** ``# pyright: ignore`` (the scoped ``# pyright: ignore[rule]``
form remains the single sanctioned escape hatch — auditable, and flagged by
``reportUnnecessaryTypeIgnoreComment`` when stale).

Counting is lexical via the stdlib tokenizer — a deliberately bounded
grammar, not a semantic analyzer (see code-quality.md § "Semantic source
scanners are prohibited"). ``Any`` and ``cast`` are counted as NAME tokens,
so mentions inside strings, docstrings, and comments do NOT count; the
ignore comments are counted inside COMMENT tokens. Overcounting oddities
(a local function literally named ``cast``) are acceptable strictness —
rename it.

The audit test (``tests/test_typing_ratchet.py``) requires the live counts
to match ``tests/_typing_ratchet_baseline.py`` EXACTLY — growth fails, and
an un-shrunk baseline after an improvement also fails, so every migration
PR records its own progress. Regenerate the baseline with:

    nix-shell --run "python3 -m tests._typing_ratchet_scanner" \
        > tests/_typing_ratchet_baseline.py
"""

from __future__ import annotations

import io
import os
import re
import tokenize
from typing import Dict

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

FINDING_KEYS = ("any", "cast", "type_ignore", "bare_pyright_ignore")

# Comment-token grammars. A COMMENT token spans '#' to end-of-line, so a
# line carrying both markers back-to-back is ONE token and both patterns
# run over it. The word boundary keeps "type-ignored" prose out; the
# negative lookahead keeps the scoped pyright form out of the bare count.
_TYPE_IGNORE_RE = re.compile(r"#\s*type:\s*ignore\b")
_BARE_PYRIGHT_IGNORE_RE = re.compile(r"#\s*pyright:\s*ignore(?!\s*\[)")

# Directory names pruned at every depth. Hidden dirs (.git, .claude,
# .pyright-venv, ...) are pruned wholesale — .claude/worktrees especially
# (see the repo-walker lesson: an unpruned walk crawls thousands of stale
# worktree files).
_PRUNE_DIRS = {"tests", "build", "docs", "examples", "__pycache__"}


def count_escape_hatches(source: str) -> Dict[str, int]:
    """Count banned typing escape hatches in one file's source.

    Returns only nonzero keys so baselines stay compact.
    """
    counts = {key: 0 for key in FINDING_KEYS}
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.NAME:
            if tok.string == "Any":
                counts["any"] += 1
            elif tok.string == "cast":
                counts["cast"] += 1
        elif tok.type == tokenize.COMMENT:
            counts["type_ignore"] += len(_TYPE_IGNORE_RE.findall(tok.string))
            counts["bare_pyright_ignore"] += len(
                _BARE_PYRIGHT_IGNORE_RE.findall(tok.string))
    return {key: n for key, n in counts.items() if n}


def iter_production_paths():
    """Yield ``(relpath, abspath)`` for every production ``.py`` file."""
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        rel_dir = os.path.relpath(dirpath, REPO_ROOT)
        dirnames[:] = sorted(
            d for d in dirnames
            if not d.startswith(".")
            and d not in _PRUNE_DIRS
            and not (rel_dir == "tools" and d == "vulture")
        )
        for fname in sorted(filenames):
            if not fname.endswith(".py"):
                continue
            path = os.path.join(dirpath, fname)
            yield os.path.relpath(path, REPO_ROOT), path


def scan_production_tree() -> Dict[str, Dict[str, int]]:
    """Return ``{relpath: {finding_key: count}}`` for offending files only."""
    result: Dict[str, Dict[str, int]] = {}
    for rel, path in iter_production_paths():
        with open(path, encoding="utf-8") as f:
            counts = count_escape_hatches(f.read())
        if counts:
            result[rel] = counts
    return result


def render_baseline_module(scan: Dict[str, Dict[str, int]]) -> str:
    """Render the baseline module source for the current scan."""
    lines = [
        '"""GENERATED baseline for the typing escape-hatch ratchet (#765).',
        "",
        "Do not edit counts by hand. Regenerate after removing escape",
        "hatches with:",
        "",
        '    nix-shell --run "python3 -m tests._typing_ratchet_scanner" \\',
        "        > tests/_typing_ratchet_baseline.py",
        '"""',
        "",
        "TYPING_RATCHET_BASELINE: dict[str, dict[str, int]] = {",
    ]
    for rel in sorted(scan):
        counts = ", ".join(
            f'"{key}": {scan[rel][key]}'
            for key in FINDING_KEYS if key in scan[rel]
        )
        lines.append(f'    "{rel}": {{{counts}}},')
    lines.append("}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    print(render_baseline_module(scan_production_tree()), end="")
