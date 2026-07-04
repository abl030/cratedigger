#!/usr/bin/env python3
"""Thin entry shim for ``pipeline-cli`` (#495 carve).

Nix (``nix/module.nix`` / ``nix/wrappers.nix``) execs this file directly
as a script (``python .../scripts/pipeline_cli/__main__.py``), same
invocation style the old flat ``scripts/pipeline_cli.py`` used. Running a
file directly sets ``sys.path[0]`` to *this file's own directory*
(``scripts/pipeline_cli/``), not the repo root — the #445 script-mode
sys.path[0] hazard. The bootstrap below inserts the repo root (two levels
up from here, vs. one for the old flat file) before importing anything
package-local, exactly mirroring the old file's top-of-module fixup.

Deliberately minimal: this module is never imported under a dotted name
(nothing does ``import scripts.pipeline_cli.__main__``), so running it as
a script cannot dual-load it under two names — the only code here is the
bootstrap plus a delegated call into ``scripts.pipeline_cli.cli.main``.
"""

import os
import sys

REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.pipeline_cli.cli import main  # noqa: E402

if __name__ == "__main__":
    main()
