"""Shared scanner for the production strict-coverage ratchet (issue #784).

Runs pyright with ``pyrightconfig.strict-production.json`` — the campaign's
end-state config (strict mode over production code, with the house-convention
rules configured off per the authorities recorded in issue #784) — and
aggregates per-file error counts.

The audit test (``tests/test_strict_ratchet.py``) requires the live counts to
match ``tests/_strict_ratchet_baseline.py`` EXACTLY: new strict errors fail,
and an improvement must tighten the baseline in the same PR, so every
annotation-campaign PR records its own progress. When the baseline reaches
empty, the final campaign PR flips ``pyrightconfig.production.json`` to strict
and deletes this machinery (the config becomes the enforcement).

Regenerate after improving:

    nix-shell --run "python3 -m tests._strict_ratchet_scanner" \
        > tests/_strict_ratchet_baseline.py
"""

from __future__ import annotations

import collections
import json
import os
import subprocess

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STRICT_CONFIG = "pyrightconfig.strict-production.json"


def count_strict_errors(
    repo_root: str = REPO_ROOT,
    config: str = STRICT_CONFIG,
) -> dict[str, int]:
    """Return ``{relpath: strict_error_count}`` for offending files.

    Invokes pyright once with the campaign config; any invocation problem
    (missing binary, unparseable output) raises rather than passing vacuously.
    """
    proc = subprocess.run(
        ["pyright", "-p", os.path.join(repo_root, config), "--outputjson"],
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    # pyright exits 1 when errors exist — only exits >1 on invocation failure.
    if proc.returncode > 1 or not proc.stdout.strip():
        raise RuntimeError(
            f"pyright invocation failed (rc={proc.returncode}): "
            f"{proc.stderr[:500]}"
        )
    payload = json.loads(proc.stdout)
    counts: collections.Counter[str] = collections.Counter()
    for diag in payload["generalDiagnostics"]:
        if diag["severity"] != "error":
            continue
        rel = os.path.relpath(diag["file"], repo_root)
        counts[rel] += 1
    return dict(counts)


def render_baseline_module(scan: dict[str, int]) -> str:
    """Render the baseline module source for the current scan."""
    lines = [
        '"""GENERATED baseline for the strict-coverage ratchet (#784).',
        "",
        "Do not edit counts by hand. Regenerate after reducing strict",
        "errors with:",
        "",
        '    nix-shell --run "python3 -m tests._strict_ratchet_scanner" \\',
        "        > tests/_strict_ratchet_baseline.py",
        '"""',
        "",
        "STRICT_RATCHET_BASELINE: dict[str, int] = {",
    ]
    for rel in sorted(scan):
        lines.append(f'    "{rel}": {scan[rel]},')
    lines.append("}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    print(render_baseline_module(count_strict_errors()), end="")
