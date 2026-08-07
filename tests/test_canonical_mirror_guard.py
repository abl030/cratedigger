"""Every reconciling surface must wire the resolver before it sweeps.

``lib/mb_canonical`` is inert until a process wires a WS/2 base. A surface
that forgets does not fail loudly — it reports ``no_redirect`` for every row
and exits 0, which the outcome vocabulary reads as "the library is already
correct". Independent review found exactly that shipped: only the systemd
oneshot wired it, so ``pipeline-cli canonical reconcile`` would have scanned
8,099 rows, written nothing, and reported success.

That happened because ``scripts/pipeline_cli/canonical.py`` had no test at
all. It has these.

Where the base points is deliberately NOT policed here. Pointing at public
MusicBrainz is the operator's business, and no more this code's concern than
it is the long-tail dashboard's.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

# Bootstrap ephemeral PostgreSQL (sets TEST_DB_DSN) — the oneshot connects.
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib import mb_canonical
from lib.canonical_release_service import configure_reconciliation_mirror
from tests.fakes import FakePipelineDB

LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
MIRROR = "http://192.168.1.43:5200"


class _WiringCase(unittest.TestCase):
    """Restores the process-global resolver base after every test."""

    def setUp(self) -> None:
        previous = mb_canonical.configured_canonical_base()
        self.addCleanup(mb_canonical.configure_canonical_base, previous)
        mb_canonical.configure_canonical_base(None)

    def _config(self, api_base: str = MIRROR) -> str:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = os.path.join(directory.name, "config.ini")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"[MusicBrainz]\napi_base = {api_base}\n")
        return path


class TestConfigureReconciliationMirror(_WiringCase):
    def test_it_actually_wires_the_resolver(self) -> None:
        """Returning a base without wiring it leaves the resolver inert —
        which is the whole defect, and reports success while doing it."""
        base = configure_reconciliation_mirror(MIRROR)

        self.assertEqual(base, f"{MIRROR}/ws/2")
        self.assertEqual(mb_canonical.configured_canonical_base(), base)

    def test_a_trailing_slash_does_not_double_up(self) -> None:
        configure_reconciliation_mirror(f"{MIRROR}/")
        self.assertEqual(
            mb_canonical.configured_canonical_base(), f"{MIRROR}/ws/2")


class TestEverySurfaceWiresBeforeSweeping(_WiringCase):
    """One test per surface. Removing the wiring call from any of them
    turns that surface back into a silent no-op."""

    def _seed(self) -> tuple[FakePipelineDB, int]:
        db = FakePipelineDB()
        request_id = db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=LOSER,
        )
        return db, request_id

    def test_the_cli_wires_the_mirror(self) -> None:
        from scripts.pipeline_cli.canonical import cmd_canonical

        db, request_id = self._seed()
        args = argparse.Namespace(canonical_command="reconcile", id=request_id)

        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": self._config()},
            clear=False,
        ), patch(
            "scripts.pipeline_cli.canonical.canonical_release_fn",
            lambda _rid: SURVIVOR,
        ):
            rc = cmd_canonical(db, args)

        self.assertEqual(rc, 0)
        self.assertEqual(
            mb_canonical.configured_canonical_base(), f"{MIRROR}/ws/2")
        self.assertEqual(
            db.request(request_id)["canonical_release_id"], SURVIVOR)

    def test_the_oneshot_wires_the_mirror(self) -> None:
        from scripts.run_canonical_reconciliation import main

        argv = [
            "cratedigger-canonical-reconcile",
            "--dsn", os.environ["TEST_DB_DSN"],
            "--config", self._config(),
        ]
        with patch("sys.argv", argv):
            main()

        self.assertEqual(
            mb_canonical.configured_canonical_base(), f"{MIRROR}/ws/2")


class TestCliReconcileOutcomes(_WiringCase):
    """Exit-code mapping for the CLI adapter (CLI ⇄ API symmetry)."""

    def _run(self, args: argparse.Namespace, db: FakePipelineDB,
             survivor: str | None) -> int:
        from scripts.pipeline_cli.canonical import cmd_canonical

        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": self._config()},
            clear=False,
        ), patch(
            "scripts.pipeline_cli.canonical.canonical_release_fn",
            lambda _rid: survivor,
        ):
            return cmd_canonical(db, args)

    def test_unknown_request_exits_two(self) -> None:
        args = argparse.Namespace(canonical_command="reconcile", id=999_999)
        self.assertEqual(self._run(args, FakePipelineDB(), None), 2)

    def test_unusable_identity_exits_five(self) -> None:
        db = FakePipelineDB()
        request_id = db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id="not-a-uuid",
        )
        args = argparse.Namespace(canonical_command="reconcile", id=request_id)
        self.assertEqual(self._run(args, db, None), 5)

    def test_no_declared_merge_is_success_not_failure(self) -> None:
        db = FakePipelineDB()
        request_id = db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=LOSER,
        )
        args = argparse.Namespace(canonical_command="reconcile", id=request_id)

        self.assertEqual(self._run(args, db, None), 0)
        self.assertIsNone(db.request(request_id)["canonical_release_id"])


if __name__ == "__main__":
    unittest.main()
