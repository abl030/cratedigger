"""The reconciliation mirror guard, and every surface that must call it.

Independent review round 1 found the reconciler was an inert no-op on two of
its three surfaces: ``lib/mb_canonical`` stays inert until a process wires a
base, only the systemd oneshot wired it, and a surface that forgets does not
fail loudly — it reports ``no_redirect`` for every row and exits 0, which the
outcome vocabulary reads as "the library is already correct".

Round 2 found the *fix* for that was itself unconstrained: six mutants
deleting the wiring call, both refusal conditions, and all three surface
guards each survived the complete suite. The root cause both times was that
``scripts/pipeline_cli/canonical.py`` had no test at all.

So this module pins the guard's decision table, proves the wiring actually
happens, and drives each of the three surfaces through a forgotten mirror.
"""

from __future__ import annotations

import unittest

from lib import mb_canonical
from lib.canonical_release_service import configure_reconciliation_mirror

LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"


class TestMirrorGuardDecisionTable(unittest.TestCase):
    """Which origins may be swept, and which may not."""

    def setUp(self) -> None:
        mb_canonical.configure_canonical_base(None)
        self.addCleanup(mb_canonical.configure_canonical_base, None)

    def test_a_local_mirror_is_accepted_and_actually_wired(self) -> None:
        """The wiring is the point: a truthy return with an inert resolver
        reproduces the original defect exactly."""
        base = configure_reconciliation_mirror("http://192.168.1.43:5200")

        self.assertEqual(base, "http://192.168.1.43:5200/ws/2")
        self.assertEqual(
            mb_canonical.configured_canonical_base(),
            "http://192.168.1.43:5200/ws/2",
            "returning a base without wiring it leaves the resolver inert",
        )

    def test_public_musicbrainz_is_refused_however_it_is_spelled(self) -> None:
        """DNS is case-insensitive; an earlier substring test was not, so
        ``MusicBrainz.org`` sailed through at the real public API."""
        for origin in (
            "https://musicbrainz.org",
            "https://MusicBrainz.org",
            "https://MUSICBRAINZ.ORG",
            "https://www.musicbrainz.org",
            "https://musicbrainz.org/",
        ):
            with self.subTest(origin=origin):
                mb_canonical.configure_canonical_base(None)
                self.assertIsNone(configure_reconciliation_mirror(origin))
                self.assertIsNone(
                    mb_canonical.configured_canonical_base(),
                    "a refused origin must leave the resolver inert",
                )

    def test_a_local_mirror_named_after_musicbrainz_is_not_refused(
        self,
    ) -> None:
        """``musicbrainz.org.lan`` is the obvious split-horizon name for a
        self-hosted mirror. A substring test refused it and told the
        operator their local mirror was public MusicBrainz."""
        for origin in (
            "http://musicbrainz.org.lan:5200",
            "http://musicbrainz.org.home.arpa:5200",
            "http://my-musicbrainz.org.mirror:5200",
            "http://musicbrainz.internal:5200",
        ):
            with self.subTest(origin=origin):
                self.assertIsNotNone(configure_reconciliation_mirror(origin))

    def test_unusable_origins_are_refused(self) -> None:
        for origin in ("", "   ", "musicbrainz.org", "not a url"):
            with self.subTest(origin=origin):
                mb_canonical.configure_canonical_base(None)
                self.assertIsNone(configure_reconciliation_mirror(origin))


class TestEverySurfaceRefusesAForgottenMirror(unittest.TestCase):
    """A surface that skips the guard reports success having done nothing.

    Each test drives the REAL surface with public MusicBrainz configured —
    the shipped default — and asserts it refuses. Removing the guard from
    any one of them turns that surface back into a silent no-op.
    """

    def setUp(self) -> None:
        mb_canonical.configure_canonical_base(None)
        self.addCleanup(mb_canonical.configure_canonical_base, None)
        self._config_dir = self._write_config("https://musicbrainz.org")

    def _write_config(self, api_base: str) -> str:
        import tempfile

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = f"{directory.name}/config.ini"
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"[MusicBrainz]\napi_base = {api_base}\n")
        return path

    def test_the_cli_refuses_and_exits_nonzero(self) -> None:
        import argparse
        import os
        from unittest.mock import patch

        from scripts.pipeline_cli.canonical import cmd_canonical
        from tests.fakes import FakePipelineDB

        db = FakePipelineDB()
        db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=LOSER,
        )
        args = argparse.Namespace(canonical_command="reconcile", id=None)

        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": self._config_dir},
            clear=False,
        ):
            rc = cmd_canonical(db, args)

        self.assertNotEqual(
            rc, 0, "a sweep that resolved nothing must not report success")
        self.assertEqual(db.record_canonical_release_id_calls, [])

    def test_the_oneshot_refuses_and_exits_nonzero(self) -> None:
        import os
        from unittest.mock import patch

        from scripts.run_canonical_reconciliation import main

        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": self._config_dir},
            clear=False,
        ), patch("sys.argv", ["cratedigger-canonical-reconcile"]):
            rc = main()

        self.assertEqual(rc, 1)


class TestCliReconcilesThroughTheRealService(unittest.TestCase):
    """The CLI had zero coverage across two review rounds. It has some now."""

    def setUp(self) -> None:
        mb_canonical.configure_canonical_base(None)
        self.addCleanup(mb_canonical.configure_canonical_base, None)

    def test_reconcile_writes_the_survivor_and_exits_zero(self) -> None:
        import argparse
        import os
        import tempfile
        from unittest.mock import patch

        from scripts.pipeline_cli.canonical import cmd_canonical
        from tests.fakes import FakePipelineDB

        survivor = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        config_path = f"{directory.name}/config.ini"
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write("[MusicBrainz]\napi_base = http://mirror.test\n")

        db = FakePipelineDB()
        request_id = db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=LOSER,
        )
        args = argparse.Namespace(canonical_command="reconcile", id=request_id)

        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
            clear=False,
        ), patch(
            "scripts.pipeline_cli.canonical.canonical_release_fn",
            lambda _rid: survivor,
        ):
            rc = cmd_canonical(db, args)

        self.assertEqual(rc, 0)
        self.assertEqual(
            db.request(request_id)["canonical_release_id"], survivor)

    def test_unknown_request_exits_two(self) -> None:
        import argparse
        import os
        import tempfile
        from unittest.mock import patch

        from scripts.pipeline_cli.canonical import cmd_canonical
        from tests.fakes import FakePipelineDB

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        config_path = f"{directory.name}/config.ini"
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write("[MusicBrainz]\napi_base = http://mirror.test\n")

        args = argparse.Namespace(canonical_command="reconcile", id=999_999)
        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
            clear=False,
        ), patch(
            "scripts.pipeline_cli.canonical.canonical_release_fn",
            lambda _rid: None,
        ):
            rc = cmd_canonical(FakePipelineDB(), args)

        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
