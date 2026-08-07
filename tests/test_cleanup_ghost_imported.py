"""Tests for scripts/cleanup_ghost_imported.py."""

import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.beets_db import BeetsDB
from lib.pipeline_db.rows import AlbumRequestRow, album_request_row
from scripts import cleanup_ghost_imported
from scripts.cleanup_ghost_imported import classify_imported_rows
from tests.fakes import FakePipelineDB
from tests.helpers import handoff_automation_owner, make_request_row


def _make_beets_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE albums (
            id INTEGER PRIMARY KEY,
            mb_albumid TEXT,
            discogs_albumid INTEGER
        );
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            album_id INTEGER,
            path BLOB,
            title TEXT,
            track INTEGER,
            disc INTEGER,
            length REAL,
            format TEXT,
            bitrate INTEGER,
            samplerate INTEGER,
            bitdepth INTEGER
        );
    """)
    conn.commit()
    conn.close()


class _RejectingDeleteFake(FakePipelineDB):
    """Typed conditional-delete loser used by the ghost cleanup tests."""

    def __init__(self) -> None:
        super().__init__()
        self.delete_calls: list[int] = []

    def delete_request(self, request_id: int) -> bool:
        self.delete_calls.append(request_id)
        return False


class _ProcessingRaceDeleteFake(_RejectingDeleteFake):
    """Return a stale imported scan while exposing the current exact owner."""

    def __init__(self, stale_imported: AlbumRequestRow) -> None:
        super().__init__()
        self._stale_imported = stale_imported

    def get_by_status(
        self,
        status: str,
        *,
        limit: int | None = None,
        newest_first: bool = False,
    ) -> list[AlbumRequestRow]:
        del limit, newest_first
        return [self._stale_imported] if status == "imported" else []


class TestCleanupGhostImported(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "beets.db")
        _make_beets_db(self.db_path)

    def tearDown(self) -> None:
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass
        os.rmdir(self.tmpdir)

    def test_classify_imported_rows_detects_missing_mb_and_discogs_releases(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO albums (id, mb_albumid) VALUES (1, ?)",
            ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",),
        )
        conn.execute(
            "INSERT INTO albums (id, discogs_albumid) VALUES (2, ?)",
            (12856590,),
        )
        conn.execute(
            "INSERT INTO items (id, album_id, path) VALUES (11, 1, ?)",
            (os.path.join(self.tmpdir, "mb", "01.flac"),),
        )
        conn.execute(
            "INSERT INTO items (id, album_id, path) VALUES (21, 2, ?)",
            (os.path.join(self.tmpdir, "discogs", "01.flac"),),
        )
        conn.commit()
        conn.close()

        rows = [
            {
                "id": 1,
                "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_release_id": None,
                "artist_name": "Present MB",
                "album_title": "Keep",
            },
            {
                "id": 2,
                "mb_release_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "discogs_release_id": None,
                "artist_name": "Missing MB",
                "album_title": "Ghost",
            },
            {
                "id": 3,
                "mb_release_id": None,
                "discogs_release_id": "12856590",
                "artist_name": "Present Discogs",
                "album_title": "Keep Too",
            },
            {
                "id": 4,
                "mb_release_id": None,
                "discogs_release_id": "5555555",
                "artist_name": "Missing Discogs",
                "album_title": "Ghost Too",
            },
        ]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual([row["id"] for row in ghosts], [2, 4])
        self.assertEqual(manual_review, [])

    def test_classify_imported_rows_flags_missing_release_ids_for_manual_review(self):
        rows = [
            {
                "id": 7,
                "mb_release_id": None,
                "discogs_release_id": None,
                "artist_name": "Unknown",
                "album_title": "Needs Review",
            }
        ]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual(ghosts, [])
        self.assertEqual([row["id"] for row in manual_review], [7])

    def test_conflicting_release_fields_require_manual_review(self):
        rows = [{
            "id": 9,
            "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "discogs_release_id": "12856590",
            "artist_name": "Conflicting",
            "album_title": "Never Auto Delete",
        }]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual(ghosts, [])
        self.assertEqual([row["id"] for row in manual_review], [9])

    def test_classify_imported_rows_fails_closed_on_ambiguous_exact_identity(self):
        release_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        conn = sqlite3.connect(self.db_path)
        for album_id in (1, 2):
            conn.execute(
                "INSERT INTO albums (id, mb_albumid) VALUES (?, ?)",
                (album_id, release_id),
            )
            conn.execute(
                "INSERT INTO items (id, album_id, path) VALUES (?, ?, ?)",
                (
                    album_id * 10,
                    album_id,
                    os.path.join(self.tmpdir, str(album_id), "01.flac"),
                ),
            )
        conn.commit()
        conn.close()
        rows = [{
            "id": 8,
            "mb_release_id": release_id,
            "discogs_release_id": None,
            "artist_name": "Ambiguous",
            "album_title": "Manual Review",
        }]

        with BeetsDB(self.db_path) as beets:
            ghosts, manual_review = classify_imported_rows(rows, beets)

        self.assertEqual(ghosts, [])
        self.assertEqual([row["id"] for row in manual_review], [8])

    def test_apply_reports_conditional_delete_rejection(self) -> None:
        row = make_request_row(
            id=12,
            status="imported",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            discogs_release_id=None,
            artist_name="Owned",
            album_title="Preserved",
        )
        db = _RejectingDeleteFake()
        db.seed_request(row)
        stdout = io.StringIO()

        with BeetsDB(self.db_path) as beets, redirect_stdout(stdout):
            result = cleanup_ghost_imported.cmd_apply(
                db,  # pyright: ignore[reportArgumentType]
                beets,
            )

        self.assertEqual(result, 4)
        self.assertEqual(db.delete_calls, [12])
        self.assertIn("preserved 12", stdout.getvalue())
        self.assertIn("deleted 0 ghost imported rows", stdout.getvalue())

    def test_apply_reports_exact_processing_owner_rejection(self) -> None:
        row = album_request_row(make_request_row(
            id=13,
            status="imported",
            mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            discogs_release_id=None,
            artist_name="Owned",
            album_title="Processing",
        ))
        db = _ProcessingRaceDeleteFake(row)
        db.seed_request(make_request_row(
            **{**row, "status": "wanted"},
        ))
        owner = handoff_automation_owner(db, 13)
        stdout = io.StringIO()

        with BeetsDB(self.db_path) as beets, redirect_stdout(stdout):
            result = cleanup_ghost_imported.cmd_apply(
                db,  # pyright: ignore[reportArgumentType]
                beets,
            )

        self.assertEqual(result, 4)
        payload = json.loads(stdout.getvalue().splitlines()[0])
        self.assertEqual(payload["reason"], "processing_locked")
        self.assertEqual(payload["processing_owner"], {
            "job_id": owner.id,
            "status": owner.status,
            "preview_status": owner.preview_status,
        })
        self.assertIn("deleted 0 ghost imported rows", stdout.getvalue())


class TestMergedRequestIsNeverAGhost(unittest.TestCase):
    """This script DELETES what it calls a ghost (#1059).

    After a MusicBrainz merge and an ``mbsync`` retag the album is on disk
    under the survivor's id. An acquisition-only resolve misses it, calls the
    row a ghost, and ``--apply`` deletes the frozen acquisition history this
    whole design rests on. Independent review confirmed that would have hit
    live requests 316 and 8832.

    Drives the REAL ``classify_imported_rows`` over a REAL Beets library.
    """

    LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
    SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"

    def _rows(self, *, canonical: str | None) -> list[dict[str, object]]:
        return [{
            "id": 316,
            "mb_release_id": self.LOSER,
            "discogs_release_id": None,
            "canonical_release_id": canonical,
            "artist_name": "Merged Artist",
            "album_title": "Merged Album",
        }]

    def _classify(self, rows: list[dict[str, object]]):
        from pathlib import Path

        from lib.beets_db import open_beets_db
        from tests.beets_world import BeetsWorld, BeetsWorldRelease

        repo = Path(__file__).resolve().parent.parent
        with BeetsWorld(repo) as world:
            world.import_release(BeetsWorldRelease(
                release_id=self.SURVIVOR,
                artist="Merged Artist",
                album="Merged Album",
                year=1996,
            ))
            with open_beets_db(
                db_path=str(world.library_db),
                library_root=str(world.library_root),
            ) as beets:
                return cleanup_ghost_imported.classify_imported_rows(
                    rows, beets)

    def test_a_merged_request_is_not_deleted(self) -> None:
        ghosts, manual = self._classify(self._rows(canonical=self.SURVIVOR))
        self.assertEqual([r["id"] for r in ghosts], [])
        self.assertEqual([r["id"] for r in manual], [])

    def test_without_the_survivor_it_would_have_been_deleted(self) -> None:
        """The must-still-work control, and the proof the fix matters: the
        exact same world with no stored survivor IS classified a ghost."""
        ghosts, _manual = self._classify(self._rows(canonical=None))
        self.assertEqual([r["id"] for r in ghosts], [316])

    def test_an_authority_failure_is_manual_review_never_a_ghost(self) -> None:
        """A resolver that omits a requested identity is an authority
        failure. It is never a licence to delete."""
        from lib.beets_db import CurrentBeetsResolution
        from lib.release_identity import ReleaseIdentity

        class _OmittingBeets:
            def resolve_current_releases(
                self, identities: list[ReleaseIdentity],
            ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
                return {}

        ghosts, manual = cleanup_ghost_imported.classify_imported_rows(
            self._rows(canonical=self.SURVIVOR), _OmittingBeets(),
        )
        self.assertEqual([r["id"] for r in ghosts], [])
        self.assertEqual([r["id"] for r in manual], [316])


class TestDefaultDsnFailsLoud(unittest.TestCase):
    """#479 item 2: no hardcoded fallback — fail loud instead."""

    @patch.object(cleanup_ghost_imported, "DEFAULT_DSN", None)
    def test_main_fails_loud_when_dsn_is_not_configured(self) -> None:
        with patch.object(sys, "argv", ["cleanup_ghost_imported.py"]):
            stderr = io.StringIO()
            with redirect_stderr(stderr), self.assertRaises(SystemExit) as cm:
                cleanup_ghost_imported.main()

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("PIPELINE_DB_DSN", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
