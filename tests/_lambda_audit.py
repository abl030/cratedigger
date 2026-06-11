"""Shared scanner for the adapter-lambda audit (test-fidelity Rule B, #382).

Isolated so ``test_lambda_audit.py`` and any future baseline helper share one
source of truth for the heuristic — same arrangement as
``tests/_mock_audit_scanner.py``.

What it flags
-------------
The precise Rule-B smell: a **strict-raise** mirror-adapter kwarg bound to a
lambda whose body is a bare ``None``, used to fake a not-found::

    compute_beets_distance(..., mb_get_release=lambda mbid: None)   # FLAGGED

Production ``web.mb.get_release`` / ``web.discogs.get_release`` (and the
release-group / master listings) **raise** ``urllib.error.HTTPError`` on a
404 — they never return ``None``. A ``lambda: None`` miss-fake therefore
exercises a branch production cannot produce (round-1 P0 of the YT-resolver
PR). New tests must use ``tests/fakes.py::FakeMBLookup`` /
``FakeDiscogsLookup`` (``raises_on_404=True``) instead, so the fake mirrors
the real exception contract pinned by ``tests/test_mirror_contracts.py``.

Why it is deliberately NARROW (no false positives)
--------------------------------------------------
* Only the strict-raise adapters are scanned. The year lookups
  (``mb_get_release_group_year`` / ``discogs_get_master_year``) and
  ``resolve_failed_path`` legitimately return ``None`` on their documented
  "record exists but no year" / "file not on disk" paths — ``: None`` there
  is correct, not a violation.
* Only a bare-``None`` lambda body is flagged. A lambda returning a
  constructed payload is a happy-path stub, not a miss-fake, and is fine.

Finding keys are ``relpath::enclosing_function`` — stable and line-number
free, so the allowlist survives edits above the flagged line (mirrors the
write-audit's name-keyed allowlist).
"""
from __future__ import annotations

import ast
import os
from typing import Dict, List, Tuple

TESTS_DIR = os.path.abspath(os.path.dirname(__file__))

# Mirror adapters whose production contract is "raise HTTPError on 404,
# never return None". A bare-None lambda for one of these is the Rule-B
# anti-pattern. Keep this set in lockstep with the contracts pinned in
# tests/test_mirror_contracts.py.
STRICT_RAISE_ADAPTER_KWARGS = frozenset({
    "mb_get_release",
    "mb_get_release_group_releases",
    "discogs_get_release",
    "discogs_get_master_releases",
})

# Grandfathered call sites: flagged-shape but benign. Each entry is
# ``"<filename>::<enclosing_function>" -> rationale``. New violations must
# either use the canonical fake or earn an entry here with a one-line
# reason. Keep this list short; it is a tripwire, not a dumping ground.
ALLOWLIST: Dict[str, str] = {
    "test_beets_distance.py::test_mb_lookup_failed_when_returns_empty":
        "benign: compute_beets_distance has an explicit `if release is None` "
        "branch (-> mb_lookup_failed); the production exception path (real "
        "adapter raises HTTPError) is separately covered by "
        "test_mb_lookup_failed_on_exception.",
    "test_beets_distance.py::test_mb_lookup_failed_in_override_path":
        "benign: same explicit None branch on the items_override path; "
        "exception path covered by test_mb_lookup_failed_on_exception.",
}


def _is_bare_none_lambda(node: ast.expr) -> bool:
    """True for ``lambda ...: None`` (body is the literal ``None``)."""
    return (
        isinstance(node, ast.Lambda)
        and isinstance(node.body, ast.Constant)
        and node.body.value is None
    )


class _Visitor(ast.NodeVisitor):
    """Collects ``(enclosing_function, lineno, kwarg)`` violations, tracking
    the function stack so each finding gets a stable, line-free key."""

    def __init__(self) -> None:
        self._func_stack: List[str] = []
        self.findings: List[Tuple[str, int, str]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._func_stack.append(node.name)
        self.generic_visit(node)
        self._func_stack.pop()

    # async defs are functions too.
    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_Call(self, node: ast.Call) -> None:
        for kw in node.keywords:
            if (
                kw.arg in STRICT_RAISE_ADAPTER_KWARGS
                and _is_bare_none_lambda(kw.value)
            ):
                func = self._func_stack[-1] if self._func_stack else "<module>"
                self.findings.append((func, kw.value.lineno, kw.arg))
        self.generic_visit(node)


def scan_file(path: str) -> List[Tuple[str, int, str]]:
    """Return ``[(enclosing_function, lineno, kwarg), ...]`` for one file."""
    with open(path, "r", encoding="utf-8") as fh:
        tree = ast.parse(fh.read(), filename=path)
    visitor = _Visitor()
    visitor.visit(tree)
    return visitor.findings


def iter_test_files():
    """Yield ``(relpath, abspath)`` for every module this audit scans.

    Recursive walk since #408 so subpackages (``tests/web/``) stay under
    audit. ``web/_harness.py`` is included explicitly even though it isn't
    a ``test_`` module: the shared HTTP harness was audited for its whole
    life inside ``test_web_server.py``, and the split must not relax that.
    """
    for dirpath, dirnames, filenames in os.walk(TESTS_DIR):
        dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
        for fname in sorted(filenames):
            if not fname.endswith(".py"):
                continue
            path = os.path.join(dirpath, fname)
            rel = os.path.relpath(path, TESTS_DIR)
            if fname.startswith("test_") or rel == os.path.join("web", "_harness.py"):
                yield rel, path


def scan_tree() -> Dict[str, List[Tuple[str, int, str]]]:
    """Scan every test module under ``tests/`` (recursively) and return
    non-allowlisted violations.

    Result shape: ``{relpath: [(enclosing_function, lineno, kwarg), ...]}``.
    Allowlisted ``relpath::function`` keys are filtered out; for files
    directly in ``tests/`` the relpath is the bare filename, so existing
    allowlist keys are unchanged.
    """
    out: Dict[str, List[Tuple[str, int, str]]] = {}
    for name, path in iter_test_files():
        kept = [
            (func, lineno, kwarg)
            for func, lineno, kwarg in scan_file(path)
            if f"{name}::{func}" not in ALLOWLIST
        ]
        if kept:
            out[name] = kept
    return out
