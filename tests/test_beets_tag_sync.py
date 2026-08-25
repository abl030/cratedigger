"""Deterministic pins for the one-album file-tag sync lane (#1260).

The retag ``-W`` residual made live contact on 2026-08-25: Beets album
16948 (*RA.1000*) had its DB identity moved to a MusicBrainz merge
survivor by the import-time retag (#1059/#1087, deliberately ``-W``
DB-only), the revalidated download was rejected, and no import ever
rewrote the installed file's tag. The census (#1142) surfaced it; this
lane heals it: one ``beet write`` scoped to exactly one album, written
DB→file, verified by re-reading the files themselves.

Invariants (each has a generated sibling in
``tests/test_beets_tag_sync_generated.py``):

S1  **Compare-and-set, twice.** The service refuses unless the album's DB
    identity equals the identity the caller authorized, and the write
    query itself pins BOTH the album (``album_id:=``) and the identity
    (``mb_albumid:=``) — a row that moved between the read and the
    subprocess matches nothing (live-verified: ``beet write`` answers
    ``No matching items found.`` and touches no file).
S2  **Success is decided only by re-reading file tags.** The write
    subprocess's exit status is never evidence (the ``lib/beets_retag.py``
    doctrine, applied at the file layer): a green exit with divergent
    re-read tags is ``residual_divergence``; a raised subprocess whose
    write actually landed is ``synced``.
S3  **Lock refusal leaves the world untouched.** A contended RELEASE
    advisory lock is a typed retryable outcome; the write never runs.
S4  **The write never touches another album's files** — pinned against a
    real ``beet write`` subprocess over a real two-album library.
"""

from __future__ import annotations

import contextlib
import logging
import os
import subprocess as sp
import tempfile
import unittest
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

from lib.beets_db import (
    BeetsAlbumIdentityRow,
    BeetsDB,
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.beets_tag_sync import (
    RESULT_ALREADY_SYNCED,
    RESULT_BEETS_UNAVAILABLE,
    RESULT_DB_IDENTITY_ABSENT,
    RESULT_IDENTITY_MISMATCH,
    RESULT_NOT_FOUND,
    RESULT_NOT_UNIQUE,
    RESULT_RELEASE_LOCKED,
    RESULT_RESIDUAL_DIVERGENCE,
    RESULT_SYNCED,
    TAG_SYNC_HTTP_STATUS,
    TagSyncResult,
    TagSyncWriteRun,
    run_beets_write_tags,
    sync_album_file_tags_from_borrowed_factory,
    sync_album_file_tags_from_factory,
    sync_release_file_tags_from_factory,
    tag_sync_query,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    release_id_to_lock_key,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakePipelineDB

# The live world this lane was built for: RA.1000, Beets album 16948.
DB_ID = "26693e58-02c0-4bb1-b66f-f0f44f8a234d"
OLD_TAG = "fdc54a6a-27c7-4936-87d7-7ab146812d4e"
ALBUM_ID = 16948
TRACK = "/library/Terre Thaemlitz/2025 - RA.1000/01 RA.1000.opus"
TRACK2 = "/library/Terre Thaemlitz/2025 - RA.1000/02 Bonus.opus"


@contextlib.contextmanager
def _silence_logs() -> Generator[None]:
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous)


class _FakeSyncBeets:
    """The narrow Beets surface the sync lane reads, over a mutable world.

    ``file_tags`` is the on-disk truth the injected ``read_tag`` serves;
    the write fake mutates it (or doesn't), so the service's verdict is
    always derived from a genuine re-read of this world, never from the
    write's report.
    """

    def __init__(self) -> None:
        self.rows: dict[int, BeetsAlbumIdentityRow] = {}
        self.file_tags: dict[str, str] = {}
        self.unreadable: set[str] = set()
        self.resolutions: dict[str, CurrentBeetsResolution] = {}
        self.close_calls = 0

    def seed_album(
        self,
        album_id: int,
        db_mb_albumid: str,
        item_paths: tuple[str, ...],
        *,
        file_tag: str = "",
        albumartist: str = "Terre Thaemlitz / DJ Sprinkles",
        album: str = "RA.1000",
    ) -> None:
        self.rows[album_id] = BeetsAlbumIdentityRow(
            album_id=album_id,
            mb_albumid=db_mb_albumid,
            item_paths=item_paths,
            albumartist=albumartist,
            album=album,
        )
        for path in item_paths:
            self.file_tags[path] = file_tag

    def get_album_mb_identity(
        self, album_id: int,
    ) -> BeetsAlbumIdentityRow | None:
        return self.rows.get(album_id)

    def resolve_current_release(
        self, identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution:
        return self.resolutions.get(
            identity.release_id, CurrentBeetsMissing(identity),
        )

    def read_tag(self, path: str) -> str:
        if path in self.unreadable:
            raise OSError(f"EIO reading {path}")
        return self.file_tags.get(path, "")

    def close(self) -> None:
        self.close_calls += 1


class _RecordingWrite:
    """A recording ``beet write`` stand-in with configurable real effect."""

    def __init__(
        self,
        beets: _FakeSyncBeets,
        *,
        applies: bool = True,
        returncode: int = 0,
        raises: Exception | None = None,
        raises_after_apply: bool = False,
    ) -> None:
        self._beets = beets
        self._applies = applies
        self._returncode = returncode
        self._raises = raises
        self._raises_after_apply = raises_after_apply
        self.calls: list[tuple[str, str]] = []
        #: RELEASE-lock keys held at the moment the write ran (S3's
        #: instrument — "the lock was held DURING the write").
        self.locks_at_call: list[tuple[tuple[int, int], ...]] = []
        self.lock_db: FakePipelineDB | None = None

    def __call__(self, query_tokens: tuple[str, str]) -> TagSyncWriteRun:
        self.calls.append(query_tokens)
        if self.lock_db is not None:
            self.locks_at_call.append(tuple(self.lock_db.advisory_lock_calls))
        if self._raises is not None and not self._raises_after_apply:
            raise self._raises
        if self._applies:
            # Mirror the real command's own compare-and-set: only items
            # whose DB rows carry the queried identity are written.
            album_token, identity_token = query_tokens
            album_id = int(album_token.split(":=", 1)[1])
            wanted = identity_token.split(":=", 1)[1]
            row = self._beets.rows.get(album_id)
            if row is not None and row.mb_albumid == wanted:
                for path in row.item_paths:
                    if path not in self._beets.unreadable:
                        self._beets.file_tags[path] = row.mb_albumid
        if self._raises is not None:
            raise self._raises
        return TagSyncWriteRun(
            returncode=self._returncode, stdout="", stderr="",
        )


def _sync(
    beets: _FakeSyncBeets,
    write: _RecordingWrite,
    *,
    album_id: int = ALBUM_ID,
    expected: str = DB_ID,
    lock_db: FakePipelineDB | None = None,
) -> TagSyncResult:
    db = lock_db if lock_db is not None else FakePipelineDB()
    write.lock_db = db
    with _silence_logs():
        return sync_album_file_tags_from_factory(
            lambda: beets,
            db,
            album_id=album_id,
            expected_mb_albumid=expected,
            read_tag=beets.read_tag,
            run_write=write,
        )


class TestTagSyncQuery(unittest.TestCase):
    """The compound item query — one selection mechanism, exact-match only."""

    def test_tokens_pin_album_primary_key_and_identity(self) -> None:
        identity = ReleaseIdentity(source="musicbrainz", release_id=DB_ID)
        self.assertEqual(
            tag_sync_query(identity, album_id=ALBUM_ID),
            (f"album_id:={ALBUM_ID}", f"mb_albumid:={DB_ID}"),
        )

    def test_non_musicbrainz_identity_is_refused(self) -> None:
        identity = ReleaseIdentity(source="discogs", release_id="123456")
        with self.assertRaises(ValueError):
            tag_sync_query(identity, album_id=ALBUM_ID)


class TestSyncOutcomeBranches(unittest.TestCase):
    """Every typed outcome, over the real service composition."""

    def test_missing_album_is_not_found(self) -> None:
        beets = _FakeSyncBeets()
        write = _RecordingWrite(beets)

        result = _sync(beets, write, album_id=999)

        self.assertEqual(result.outcome, RESULT_NOT_FOUND)
        self.assertEqual(write.calls, [])
        self.assertEqual(beets.close_calls, 1)

    def test_db_absent_identity_is_refused(self) -> None:
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, "", (TRACK,), file_tag=OLD_TAG)
        write = _RecordingWrite(beets)

        result = _sync(beets, write)

        self.assertEqual(result.outcome, RESULT_DB_IDENTITY_ABSENT)
        self.assertEqual(write.calls, [])

    def test_stale_expected_identity_is_refused_without_a_write(self) -> None:
        """S1 — the caller authorized an identity the DB no longer names."""
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=OLD_TAG)
        write = _RecordingWrite(beets)

        result = _sync(beets, write, expected=OLD_TAG)

        self.assertEqual(result.outcome, RESULT_IDENTITY_MISMATCH)
        self.assertEqual(result.db_mb_albumid, DB_ID)
        self.assertEqual(write.calls, [])
        self.assertEqual(beets.file_tags[TRACK], OLD_TAG)

    def test_invalid_expected_identity_is_refused(self) -> None:
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=OLD_TAG)
        write = _RecordingWrite(beets)

        for bad in ("", "not-a-uuid", "[r123456]"):
            with self.subTest(expected=bad):
                result = _sync(beets, write, expected=bad)
                self.assertEqual(result.outcome, RESULT_IDENTITY_MISMATCH)
        self.assertEqual(write.calls, [])

    def test_agreeing_album_is_already_synced_without_a_write(self) -> None:
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=DB_ID)
        write = _RecordingWrite(beets)

        result = _sync(beets, write)

        self.assertEqual(result.outcome, RESULT_ALREADY_SYNCED)
        self.assertEqual(write.calls, [])
        assert result.album is not None
        self.assertEqual(result.album.album_class, "agrees")

    def test_empty_album_is_already_synced_without_a_write(self) -> None:
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, ())
        write = _RecordingWrite(beets)

        result = _sync(beets, write)

        self.assertEqual(result.outcome, RESULT_ALREADY_SYNCED)
        self.assertEqual(write.calls, [])

    def test_contended_release_lock_refuses_before_the_write(self) -> None:
        """S3 — typed retryable refusal; no write, files untouched."""
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=OLD_TAG)
        write = _RecordingWrite(beets)
        lock_db = FakePipelineDB()
        lock_db.set_advisory_lock_result(False)

        result = _sync(beets, write, lock_db=lock_db)

        self.assertEqual(result.outcome, RESULT_RELEASE_LOCKED)
        self.assertEqual(write.calls, [])
        self.assertEqual(beets.file_tags[TRACK], OLD_TAG)

    def test_divergent_album_syncs_under_the_release_lock(self) -> None:
        """The RA.1000 heal — the write runs holding RELEASE(db identity),
        and the verdict comes from the re-read files."""
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=OLD_TAG)
        write = _RecordingWrite(beets)
        lock_db = FakePipelineDB()

        result = _sync(beets, write, lock_db=lock_db)

        self.assertEqual(result.outcome, RESULT_SYNCED)
        self.assertEqual(
            write.calls,
            [(f"album_id:={ALBUM_ID}", f"mb_albumid:={DB_ID}")],
        )
        self.assertEqual(beets.file_tags[TRACK], DB_ID)
        assert result.album is not None
        self.assertEqual(result.album.album_class, "agrees")
        expected_lock = (
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(DB_ID),
        )
        self.assertEqual(write.locks_at_call, [(expected_lock,)])

    def test_green_exit_with_divergent_reread_is_residual(self) -> None:
        """S2 — the exit code is not evidence; the re-read files are."""
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=OLD_TAG)
        write = _RecordingWrite(beets, applies=False, returncode=0)

        result = _sync(beets, write)

        self.assertEqual(result.outcome, RESULT_RESIDUAL_DIVERGENCE)
        assert result.album is not None
        self.assertEqual(result.album.album_class, "diverges")

    def test_raised_write_whose_effect_landed_is_synced(self) -> None:
        """S2, other direction — a raise is not evidence either."""
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=OLD_TAG)
        write = _RecordingWrite(
            beets, raises=sp.TimeoutExpired(cmd="beet", timeout=1),
            raises_after_apply=True,
        )

        result = _sync(beets, write)

        self.assertEqual(result.outcome, RESULT_SYNCED)
        self.assertEqual(beets.file_tags[TRACK], DB_ID)

    def test_unreadable_item_survives_as_residual(self) -> None:
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK, TRACK2), file_tag=OLD_TAG)
        beets.unreadable.add(TRACK2)
        write = _RecordingWrite(beets)

        result = _sync(beets, write)

        self.assertEqual(result.outcome, RESULT_RESIDUAL_DIVERGENCE)
        self.assertEqual(beets.file_tags[TRACK], DB_ID)

    def test_unavailable_beets_authority_is_typed(self) -> None:
        def broken_factory() -> _FakeSyncBeets:
            raise FileNotFoundError("Beets DB not configured")

        with _silence_logs():
            result = sync_album_file_tags_from_factory(
                broken_factory,
                FakePipelineDB(),
                album_id=ALBUM_ID,
                expected_mb_albumid=DB_ID,
                read_tag=lambda _path: "",
                run_write=_RecordingWrite(_FakeSyncBeets()),
            )

        self.assertEqual(result.outcome, RESULT_BEETS_UNAVAILABLE)

    def test_borrowed_factory_never_closes_the_handle(self) -> None:
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=OLD_TAG)
        write = _RecordingWrite(beets)
        db = FakePipelineDB()
        write.lock_db = db

        with _silence_logs():
            result = sync_album_file_tags_from_borrowed_factory(
                lambda: beets,
                db,
                album_id=ALBUM_ID,
                expected_mb_albumid=DB_ID,
                read_tag=beets.read_tag,
                run_write=write,
            )

        self.assertEqual(result.outcome, RESULT_SYNCED)
        self.assertEqual(beets.close_calls, 0)


class TestReleaseEntry(unittest.TestCase):
    """The seam's release-id entry — resolve the one album, then delegate."""

    def _sync_release(
        self, beets: _FakeSyncBeets, write: _RecordingWrite,
        *, release_id: str = DB_ID,
    ) -> TagSyncResult:
        db = FakePipelineDB()
        write.lock_db = db
        with _silence_logs():
            return sync_release_file_tags_from_factory(
                lambda: beets,
                db,
                release_id=release_id,
                read_tag=beets.read_tag,
                run_write=write,
            )

    def test_unique_album_at_the_release_is_synced(self) -> None:
        beets = _FakeSyncBeets()
        beets.seed_album(ALBUM_ID, DB_ID, (TRACK,), file_tag=OLD_TAG)
        beets.resolutions[DB_ID] = CurrentBeetsUnique(
            identity=ReleaseIdentity(source="musicbrainz", release_id=DB_ID),
            album_id=ALBUM_ID,
            album_path="/library/x",
            items=(),
            selectors=(),
        )
        write = _RecordingWrite(beets)

        result = self._sync_release(beets, write)

        self.assertEqual(result.outcome, RESULT_SYNCED)
        self.assertEqual(beets.file_tags[TRACK], DB_ID)

    def test_release_held_by_no_album_is_not_found(self) -> None:
        beets = _FakeSyncBeets()
        write = _RecordingWrite(beets)

        result = self._sync_release(beets, write)

        self.assertEqual(result.outcome, RESULT_NOT_FOUND)
        self.assertEqual(write.calls, [])

    def test_ambiguous_release_is_refused(self) -> None:
        beets = _FakeSyncBeets()
        beets.resolutions[DB_ID] = CurrentBeetsAmbiguous(
            identity=ReleaseIdentity(source="musicbrainz", release_id=DB_ID),
            album_ids=(7, 9),
            reason="multiple_matches",
        )
        write = _RecordingWrite(beets)

        result = self._sync_release(beets, write)

        self.assertEqual(result.outcome, RESULT_NOT_UNIQUE)
        self.assertEqual(write.calls, [])

    def test_non_musicbrainz_release_is_refused(self) -> None:
        beets = _FakeSyncBeets()
        write = _RecordingWrite(beets)

        result = self._sync_release(beets, write, release_id="[r123456]")

        self.assertEqual(result.outcome, RESULT_IDENTITY_MISMATCH)
        self.assertEqual(write.calls, [])


class TestHttpStatusMap(unittest.TestCase):
    """The route mapping follows the CLI ⇄ API convention table."""

    def test_every_outcome_is_mapped(self) -> None:
        self.assertEqual(TAG_SYNC_HTTP_STATUS, {
            RESULT_SYNCED: 200,
            RESULT_ALREADY_SYNCED: 200,
            RESULT_NOT_FOUND: 404,
            RESULT_NOT_UNIQUE: 409,
            RESULT_IDENTITY_MISMATCH: 409,
            RESULT_DB_IDENTITY_ABSENT: 409,
            RESULT_RESIDUAL_DIVERGENCE: 409,
            RESULT_RELEASE_LOCKED: 503,
            RESULT_BEETS_UNAVAILABLE: 503,
        })


class TestRunBeetsWriteTagsSeam(unittest.TestCase):
    """The subprocess argv seam — flags and token separation are the contract."""

    def test_argv_shape(self) -> None:
        recorded: list[list[str]] = []

        def runner(argv: list[str], **kwargs: object) -> sp.CompletedProcess[bytes]:
            del kwargs
            recorded.append(argv)
            return sp.CompletedProcess(argv, 0, b"", b"")

        with patch.dict(
            os.environ,
            {"CRATEDIGGER_BEETS_PYTHON": "/fake/python"},
            clear=False,
        ):
            run = run_beets_write_tags(
                (f"album_id:={ALBUM_ID}", f"mb_albumid:={DB_ID}"),
                runner=runner,
            )

        self.assertEqual(run.returncode, 0)
        self.assertEqual(len(recorded), 1)
        argv = recorded[0]
        self.assertEqual(argv[1:3], ["-m", "beets"])
        self.assertEqual(argv[3], "write")
        # The two query tokens stay SEPARATE argv elements — beets ANDs
        # distinct tokens implicitly; a joined string parses as one
        # malformed token (the lib/beets_retag.py precedent).
        self.assertEqual(
            argv[4:], [f"album_id:={ALBUM_ID}", f"mb_albumid:={DB_ID}"],
        )


def _read_file_mb_albumid(path: Path) -> str:
    import mediafile

    return str(mediafile.MediaFile(str(path)).mb_albumid or "")


class TestRealBeetsWriteTagSync(unittest.TestCase):
    """S1/S2/S4 against the real ``beet write`` subprocess and real files.

    Reuses ``tests/test_beets_retag.py``'s real-world fixture builder: a
    genuine Beets library DB, a real taggable MP3 per item, and a runtime
    ``config.ini`` that ``beets_subprocess_env()`` resolves — so the
    production ``run_beets_write_tags`` runs unmodified.
    """

    def _seed(self, base: Path) -> tuple[Path, Path, int]:
        from tests.test_beets_retag import _seed_real_modify_world

        return _seed_real_modify_world(base, item_count=2, real_audio=True)

    def test_the_real_write_heals_the_divergence_and_spares_the_decoy(
        self,
    ) -> None:
        from tests.test_beets_retag import MERGED, _installed_dir

        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root, library_db, album_id = self._seed(base)

            # A decoy album beside the target: same library, own identity,
            # own real file. S4's instrument.
            import beets.library as beets_library

            decoy_dir = root / "Decoy" / "2020 - Decoy Album"
            decoy_dir.mkdir(parents=True)
            decoy_path = decoy_dir / "01 Decoy.mp3"
            from tests.test_beets_retag import _make_real_mp3

            _make_real_mp3(decoy_path)
            decoy_id = "eeeeeeee-1111-4111-8111-111111111111"
            lib = beets_library.Library(str(library_db), str(root))
            lib.add_album([beets_library.Item(
                path=str(decoy_path),
                title="Decoy",
                artist="Decoy",
                album="Decoy Album",
                albumartist="Decoy",
                track=1,
                disc=1,
                year=2020,
                mb_albumid=decoy_id,
            )])
            lib._close()
            decoy_mtime_before = decoy_path.stat().st_mtime_ns

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ), _silence_logs():
                result = sync_album_file_tags_from_factory(
                    lambda: BeetsDB(str(library_db), library_root=str(root)),
                    FakePipelineDB(),
                    album_id=album_id,
                    expected_mb_albumid=MERGED,
                )

            self.assertEqual(result.outcome, RESULT_SYNCED)
            assert result.album is not None
            self.assertEqual(result.album.album_class, "agrees")
            for track in sorted(_installed_dir(root).glob("*.mp3")):
                self.assertEqual(_read_file_mb_albumid(track), MERGED)
            # S4: the decoy album's file carries its own untouched tag
            # world — no write reached it.
            self.assertEqual(_read_file_mb_albumid(decoy_path), "")
            self.assertEqual(
                decoy_path.stat().st_mtime_ns, decoy_mtime_before,
            )

    def test_a_stale_authorized_identity_matches_nothing_for_real(
        self,
    ) -> None:
        """S1 against the real subprocess: the compound query's identity
        clause refuses a moved row — ``No matching items found.``, files
        untouched."""
        from tests.test_beets_retag import SURVIVOR, _installed_dir

        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root, _library_db, album_id = self._seed(base)
            tracks = sorted(_installed_dir(root).glob("*.mp3"))
            mtimes_before = [t.stat().st_mtime_ns for t in tracks]

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ), _silence_logs():
                # The DB names MERGED; authorize SURVIVOR. The service's
                # own pre-check refuses first — so drive the WRITE layer
                # directly to prove the query itself also refuses, the
                # defense-in-depth half of S1.
                run = run_beets_write_tags(
                    (f"album_id:={album_id}", f"mb_albumid:={SURVIVOR}"),
                )

            self.assertNotEqual(run.returncode, 0)
            self.assertIn("No matching", run.stderr + run.stdout)
            for track, before in zip(tracks, mtimes_before, strict=True):
                self.assertEqual(track.stat().st_mtime_ns, before)
                self.assertEqual(_read_file_mb_albumid(track), "")


if __name__ == "__main__":
    unittest.main()
