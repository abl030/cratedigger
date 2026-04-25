"""Contract tests for nix/module.nix.

The Nix wrappers in ``nix/module.nix`` decide what environment
subprocesses (``beet``, ``import_one.py``, etc.) inherit. Historically,
leaks here have caused pipeline-wide failures that were hard to trace:

* 2026-04-21 ``cratedigger-web`` force-import path crashed on every
  post-import ``beet remove`` with ``ModuleNotFoundError: No module
  named 'msgspec'``. Root cause: the wrapper exported
  ``PYTHONPATH=${src}:${src}/lib:${src}/web:...`` which put
  ``lib/beets.py`` at sys.path top level as a bare ``beets`` module,
  shadowing the real beets PyPI package. The ``beet`` subprocess did
  ``from beets.ui import main`` → loaded our ``lib/beets.py`` → hit
  ``import msgspec`` (line 11) → ``ModuleNotFoundError`` because the
  beet-wrapped Python doesn't carry msgspec. The accumulated effect
  was three split-brain rows for one MBID (Unter Null "Sick Fuck"
  request 1748).

These grep-based contracts are cheap to write and catch the whole
class of "an export in module.nix leaked into a subprocess and broke
something five layers away". They run inside the Python suite because
we don't want to depend on ``nix eval`` at test time — a text grep
against the source file is enough for the invariants we care about.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_NIX = REPO_ROOT / "nix" / "module.nix"


class TestPythonPathCarriesOnlyRepoRoot(unittest.TestCase):
    """No wrapper in ``nix/module.nix`` may export PYTHONPATH that includes
    ``${src}/lib`` or ``${src}/web``.

    All internal imports use the qualified form ``from lib.X import Y`` /
    ``from web.X import Y``, so the repo root on PYTHONPATH is sufficient.
    Adding the sub-directories promotes our internal modules (``lib/beets.py``,
    ``web/discogs.py``, ``web/classify.py``) to top-level names, where they
    shadow the real ``beets``, ``discogs_client`` and anything else a
    subprocess might import. The beet subprocess has historically been
    the first victim because its wrapper does ``from beets.ui import main``.
    """

    # Matches any ``export PYTHONPATH=...${src}/<subdir>...``
    # The test looks for the forbidden sub-paths specifically rather than
    # trying to parse the full expression — that keeps the pattern simple
    # and catches any future ``${src}/foo`` that would cause the same class
    # of shadowing.
    FORBIDDEN = re.compile(r'PYTHONPATH=.*\$\{src\}/(lib|web)')

    def test_no_wrapper_leaks_subdir(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        hits: list[tuple[int, str]] = []
        for lineno, line in enumerate(text.splitlines(), start=1):
            # Skip comments — comments are explanation, not code.
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if self.FORBIDDEN.search(line):
                hits.append((lineno, line.strip()))
        self.assertEqual(
            hits, [],
            f"{MODULE_NIX} exports PYTHONPATH with ${{src}}/lib or "
            f"${{src}}/web — these shadow PyPI packages (beets, "
            f"discogs_client, ...) in any subprocess that inherits "
            f"PYTHONPATH. Use ${{src}} only; internal imports are "
            f"qualified (from lib.X import Y). Offending lines:\n"
            + "\n".join(f"  {n}: {s}" for n, s in hits)
        )


class TestImporterServiceContract(unittest.TestCase):
    def test_importer_wrapper_and_service_are_defined(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('writeShellScriptBin "cratedigger-importer"', text)
        self.assertIn("${src}/scripts/importer.py", text)
        self.assertIn("systemd.services.cratedigger-importer", text)
        self.assertIn('after = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('requires = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('ExecStart = "${importerPkg}/bin/cratedigger-importer"', text)
        self.assertIn('Environment = "PIPELINE_DB_DSN=${cfg.pipelineDb.dsn}"', text)
        self.assertIn("WorkingDirectory = cfg.stateDir", text)

    def test_preview_worker_wrapper_service_and_worker_count_are_defined(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('writeShellScriptBin "cratedigger-import-preview-worker"', text)
        self.assertIn("${src}/scripts/import_preview_worker.py", text)
        self.assertIn("systemd.services.cratedigger-import-preview-worker", text)
        self.assertIn("previewWorkers", text)
        self.assertIn("default = 2", text)
        self.assertIn("cfg.importer.previewWorkers >= 1", text)
        self.assertIn("services.cratedigger.importer.previewWorkers must be at least 1", text)
        self.assertIn('--workers ${toString cfg.importer.previewWorkers}', text)
        self.assertIn('after = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('requires = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('ExecStart = "${previewWorkerPkg}/bin/cratedigger-import-preview-worker"', text)
        self.assertIn('Environment = "PIPELINE_DB_DSN=${cfg.pipelineDb.dsn}"', text)


if __name__ == "__main__":
    unittest.main()
