"""Tests for ``lib.beets_distance.compute_beets_distance``.

Covers the outcome matrix end-to-end. Filesystem-touching paths use a
temp directory + a real audio fixture (copied from
``tests/fixtures/audio_hash``) tagged via ``music_tag``; non-FS paths
use a tiny stub DB so we exercise the real service logic without
mocking the function under test.

The integration slice in ``TestBeetsDistanceIntegrationSlice`` is the
authoritative coverage of the happy path — it runs the real beets
distance computation against real on-disk files and asserts the result
is in a sane range and that the cache fast-path returns the same
number on a second call (mtime-stable).
"""

from __future__ import annotations

import dataclasses
import errno
import os
import shutil
import tempfile
import unittest

import music_tag

from lib.beets_distance import (
    BeetsDistanceCache,
    BeetsDistanceResult,
    SyntheticItem,
    compute_beets_distance,
)
from lib.fs_authority import DirectoryObservation


def _present(path: str) -> DirectoryObservation:
    """Injected observer: this exact name holds a directory."""
    return DirectoryObservation(presence="present", path=path)


def _absent(_path: str) -> DirectoryObservation:
    """Injected observer: proven absent (never merely unreadable)."""
    return DirectoryObservation(presence="absent", code="missing")


def _unreadable(path: str) -> DirectoryObservation:
    """Injected observer mirroring a real EACCES probe refusal."""
    return DirectoryObservation(
        presence="indeterminate",
        code="open_failed",
        errno_symbol="EACCES",
        detail=f"{path}: Permission denied",
    )


FIXTURE_FLAC = os.path.join(
    os.path.dirname(__file__), "fixtures", "audio_hash", "sine_440.flac")


# Canonical outcome strings emitted by compute_beets_distance. Pinned here
# because they're wire contract (CLI exit codes, HTTP status, web UI) —
# any change requires coordinated updates downstream. ``ok`` is exercised
# by the integration slice further down; the rest by TestComputeBeetsDistanceOutcomes.
OUTCOMES = (
    "ok",
    "download_log_not_found",
    "request_not_found",
    "folder_missing",
    "folder_unavailable",
    "no_audio",
    "mb_lookup_failed",
    "mb_no_release_group",
    "wrong_release_group",
    "distance_failed",
)


class DictCache:
    """In-memory BeetsDistanceCache implementation, test-only."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        self._store[key] = value


class _StubPDB:
    """Tiny ``PipelineDB`` stand-in for the service tests.

    Only the two methods ``compute_beets_distance`` touches are
    implemented; ``FakePipelineDB`` in ``tests/fakes.py`` is overkill
    for a 1-call read path and would couple this test file to its
    unrelated schema.
    """

    def __init__(
        self,
        *,
        download_log_entry: dict | None = None,
        request: dict | None = None,
    ) -> None:
        self._dl = download_log_entry
        self._request = request

    def get_download_log_entry(self, log_id):
        if self._dl is None:
            return None
        if self._dl.get("id") != log_id:
            return None
        return dict(self._dl)

    def get_request(self, request_id):
        if self._request is None or self._request.get("id") != request_id:
            return None
        return dict(self._request)


def _ok_mb_release(
    *,
    mbid: str = "rel-aaa",
    rg: str = "rg-shared",
    artist: str = "Dr. Octagon",
    album: str = "Dr. Octagonecologyst",
    tracks: list[dict] | None = None,
):
    return {
        "id": mbid,
        "title": album,
        "artist_name": artist,
        "artist_id": "artist-1",
        "release_group_id": rg,
        "date": "1996-05-07",
        "year": 1996,
        "country": "US",
        "status": "Official",
        "tracks": tracks if tracks is not None else [
            {"disc_number": 1, "track_number": 1, "title": "Intro", "length_seconds": 60.0},
            {"disc_number": 1, "track_number": 2, "title": "3000",  "length_seconds": 180.0},
        ],
    }


# ============================================================================
# Outcome-matrix tests — DB/MB/disk are stubbed, every branch is one subTest.
# ============================================================================


class TestComputeBeetsDistanceOutcomes(unittest.TestCase):
    """Every outcome in ``OUTCOMES`` reachable from a single subTest table.

    Each row drives ``compute_beets_distance`` to exactly one outcome.
    ``ok`` is exercised by the integration slice further down (it needs
    a real audio file).
    """

    def test_outcome_set_is_stable(self) -> None:
        """Adding/removing an outcome is a wire-contract change — pin it."""
        self.assertEqual(
            set(OUTCOMES),
            {
                "ok",
                "download_log_not_found",
                "request_not_found",
                "folder_missing",
                "folder_unavailable",
                "no_audio",
                "mb_lookup_failed",
                "mb_no_release_group",
                "wrong_release_group",
                "distance_failed",
            },
        )

    def test_download_log_not_found(self) -> None:
        pdb = _StubPDB()  # no download_log
        r = compute_beets_distance(
            42, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            observe_failed_path=_present,
        )
        self.assertEqual(r.outcome, "download_log_not_found")
        self.assertIsNone(r.distance)
        self.assertEqual(r.download_log_id, 42)
        self.assertEqual(r.candidate_mbid, "rel-x")

    def test_request_not_found_when_log_has_no_request(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": None},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            observe_failed_path=_present,
        )
        self.assertEqual(r.outcome, "request_not_found")

    def test_request_not_found_when_request_missing(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 99},
            request=None,
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            observe_failed_path=_present,
        )
        self.assertEqual(r.outcome, "request_not_found")

    def test_mb_lookup_failed_when_returns_empty(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: None,
            observe_failed_path=_present,
        )
        self.assertEqual(r.outcome, "mb_lookup_failed")
        self.assertEqual(r.request_release_group_id, "rg-shared")

    def test_mb_lookup_failed_on_exception(self) -> None:
        def _boom(mbid):
            raise RuntimeError("MB mirror down")

        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=_boom,
            observe_failed_path=_present,
        )
        self.assertEqual(r.outcome, "mb_lookup_failed")
        assert r.error_message is not None
        self.assertIn("MB mirror down", r.error_message)

    def test_mb_no_release_group(self) -> None:
        mb = _ok_mb_release(mbid="rel-x")
        mb["release_group_id"] = None
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: mb,
            observe_failed_path=_present,
        )
        self.assertEqual(r.outcome, "mb_no_release_group")

    def test_wrong_release_group_guardrail(self) -> None:
        """The sanity-stop: candidate MBID in a different RG → refuse."""
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/whatever"}},
            request={"id": 7, "mb_release_group_id": "rg-source"},
        )
        r = compute_beets_distance(
            1, "rel-alien",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
            observe_failed_path=_present,
        )
        self.assertEqual(r.outcome, "wrong_release_group")
        self.assertEqual(r.request_release_group_id, "rg-source")
        self.assertEqual(r.candidate_release_group_id, "rg-other")
        # Guardrail must fire BEFORE filesystem access.
        # (We never resolved a path — fingerprints/distance never ran.)
        self.assertIsNone(r.folder_path)
        self.assertIsNone(r.distance)

    def test_wrong_release_group_passes_when_request_rg_is_null(self) -> None:
        """If the request has no RG (legacy row), we can't refuse — fall through.

        Documenting the asymmetry: the guardrail only fires when *both*
        sides know their release group. A null request RG drops us into
        the rest of the pipeline, which will fail later (folder_missing
        or no_audio) but on a different signal.
        """
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/missing"}},
            request={"id": 7, "mb_release_group_id": None},
        )
        r = compute_beets_distance(
            1, "rel-alien",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
            observe_failed_path=_absent,
        )
        self.assertEqual(r.outcome, "folder_missing")  # not wrong_release_group
        self.assertEqual(r.candidate_release_group_id, "rg-other")

    def test_folder_missing(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/not/there"}},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            observe_failed_path=_absent,
        )
        self.assertEqual(r.outcome, "folder_missing")

    def test_folder_unavailable_is_not_folder_missing(self) -> None:
        """An unreadable folder is a distinct, non-absent outcome (#1063)."""
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/private/x"}},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            observe_failed_path=_unreadable,
        )
        self.assertEqual(r.outcome, "folder_unavailable")
        assert r.error_message is not None
        self.assertIn("EACCES", r.error_message)

    def test_unreadable_folder_contents_are_not_no_audio(self) -> None:
        """The READ leg owes the same distinction (#1063 review T2.2).

        The folder resolves; the walk is then refused. ``os.walk`` swallows
        that by default, so zero fingerprints used to mean ``no_audio`` — a
        definitive negative from an observation we were not allowed to make.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        shutil.copy(FIXTURE_FLAC, os.path.join(album, "01.flac"))
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": album}},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        os.chmod(album, 0o000)
        try:
            r = compute_beets_distance(
                1, "rel-x",
                pdb=pdb,
                mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
                observe_failed_path=_present,
            )
        finally:
            os.chmod(album, 0o700)
        self.assertEqual(r.outcome, "folder_unavailable")
        assert r.error_message is not None
        self.assertIn("could not read the contents", r.error_message)

    def _refused_tag_read_world(self, locked: tuple[str, ...]) -> str:
        """Real FLACs under a readable folder, some of them mode 0000.

        The refusal shape the walk CANNOT see: the directory lists fine,
        every file stats fine, and only the tag read is refused (issue
        #1063). Restores the modes on teardown so ``TemporaryDirectory``
        can clean up.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        for name in ("01 - one.flac", "02 - two.flac"):
            shutil.copy(FIXTURE_FLAC, os.path.join(album, name))
        for name in locked:
            os.chmod(os.path.join(album, name), 0o000)

        def _restore() -> None:
            for name in locked:
                os.chmod(os.path.join(album, name), 0o600)

        self.addCleanup(_restore)
        return album

    def _compute_over(self, album: str) -> BeetsDistanceResult:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": album}},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        return compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            observe_failed_path=_present,
        )

    def test_refused_tag_read_flags_the_partial_manifest(self) -> None:
        """One readable + one refused file is an INCOMPLETE manifest.

        ``partial_read`` is the field that says so on the otherwise-``ok``
        result. Before the fix, ``_fingerprint_file`` swallowed the
        refusal (mediafile converts the ``OSError`` into its own
        ``UnreadableFileError``), so a distance computed over half an
        album shipped as a plain number (issue #1063).
        """
        album = self._refused_tag_read_world(("02 - two.flac",))
        result = self._compute_over(album)
        self.assertEqual(result.outcome, "ok")
        assert result.partial_read is not None
        self.assertIn("02 - two.flac", result.partial_read)
        self.assertIn("Permission denied", result.partial_read)
        self.assertEqual(result.total_local_tracks, 1)

    def test_every_tag_read_refused_is_not_no_audio(self) -> None:
        """ALL files refused is the #1063 defect verbatim, one layer down.

        Zero fingerprints with no walk-level error used to mean
        ``no_audio`` — HTTP 410 Gone, CLI exit 4, "the artifacts we
        wanted to compare are gone" — over an intact album.
        """
        album = self._refused_tag_read_world(
            ("01 - one.flac", "02 - two.flac"))
        result = self._compute_over(album)
        self.assertEqual(result.outcome, "folder_unavailable")
        self.assertNotEqual(result.outcome, "no_audio")
        assert result.error_message is not None
        self.assertIn("could not read the contents", result.error_message)

    def test_a_proven_absence_is_not_reported_as_a_refusal(self) -> None:
        """ENOENT is the one errno that EARNS a definitive negative.

        A dangling symlink is listed by the walk and answers ENOENT on
        ``stat``: the file is provably gone. Recording that as a refusal
        reaches the operator as an amber ``· incomplete manifest`` badge
        on the Replace picker — over a manifest that is complete. Issue
        #1063's rule cuts both ways.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        shutil.copy(FIXTURE_FLAC, os.path.join(album, "01 - one.flac"))
        os.symlink(os.path.join(album, "nothing-here.flac"),
                   os.path.join(album, "02 - dangling.flac"))
        result = self._compute_over(album)
        self.assertEqual(result.outcome, "ok")
        self.assertIsNone(result.partial_read)
        self.assertEqual(result.total_local_tracks, 1)

    def test_only_proven_absences_is_no_audio_not_unavailable(self) -> None:
        """A folder holding one dangling name WAS observed and read.

        ``folder_unavailable`` (503 / exit 5) means the folder could not
        be observed; that would be false here, and the CLI doc this
        series wrote says so.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        os.symlink(os.path.join(album, "nothing-here.flac"),
                   os.path.join(album, "01 - dangling.flac"))
        result = self._compute_over(album)
        self.assertEqual(result.outcome, "no_audio")
        self.assertIsNone(result.partial_read)

    def _mid_read_refusal(self, album: str, name: str) -> None:
        """Plant a file that OPENS fine and answers EIO on every read.

        The deployment's live refusal shape (nested virtiofs
        EIO/ESTALE), reproduced without a flaky mount.
        """
        os.symlink("/proc/self/mem", os.path.join(album, name))

    def test_a_mid_read_refusal_is_never_no_audio(self) -> None:
        """The live shape: errno and filename on DIFFERENT chain links.

        Only ``open()`` attaches a filename; ``FileIO.read`` does not. An
        attribution guard that demanded both on one link answered
        ``no_audio`` — HTTP 410 Gone, CLI exit 4 — over an intact album
        on the exact mount this deployment runs on.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        self._mid_read_refusal(album, "01 - one.flac")
        result = self._compute_over(album)
        self.assertEqual(result.outcome, "folder_unavailable")
        self.assertNotEqual(result.outcome, "no_audio")
        assert result.error_message is not None
        self.assertIn("could not read the contents", result.error_message)

    def test_a_partial_mid_read_refusal_flags_the_manifest(self) -> None:
        """…and one readable file beside it is an incomplete manifest."""
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        shutil.copy(FIXTURE_FLAC, os.path.join(album, "01 - one.flac"))
        self._mid_read_refusal(album, "02 - sick-mount.flac")
        result = self._compute_over(album)
        self.assertEqual(result.outcome, "ok")
        assert result.partial_read is not None
        self.assertIn("02 - sick-mount.flac", result.partial_read)
        self.assertEqual(result.total_local_tracks, 1)

    def test_a_walk_refusal_and_a_walk_absence_are_told_apart(self) -> None:
        """The third refusal site: ``os.walk``'s ``onerror``.

        A subdirectory we may not descend into is a refusal; a name that
        is not there when the walk reaches it is not. The generated
        property reaches the EACCES direction (``refused_dir``); the
        ENOENT direction needs the folder to disappear between the
        observation and the walk, which is a race no strategy can
        generate — so it is pinned here instead of faked.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        shutil.copy(FIXTURE_FLAC, os.path.join(album, "01 - one.flac"))
        disc = os.path.join(album, "Disc 2")
        os.makedirs(disc)
        os.chmod(disc, 0o000)
        self.addCleanup(os.chmod, disc, 0o700)
        refused = self._compute_over(album)
        self.assertEqual(refused.outcome, "ok")
        assert refused.partial_read is not None
        self.assertIn("Disc 2", refused.partial_read)

        # ENOENT at the walk proves absence, so it claims no refusal.
        vanished = os.path.join(root, "wrong_matches", "Vanished")
        absent = self._compute_over(vanished)
        self.assertEqual(absent.outcome, "no_audio")
        self.assertIsNone(absent.partial_read)

    def test_a_symlink_loop_is_a_refusal_at_both_ends(self) -> None:
        """ELOOP proves nothing absent, and both ends must agree.

        ``refusal_is_indeterminate`` answers ``False`` for a symlink loop
        because it is a containment verdict, not a sick mount — but
        ``False`` there means "not retryable", never "proved absent".
        Keying the read off it dropped ELOOP into ``no_audio`` while
        ``observe_directory`` called the very same errno indeterminate
        for the folder.
        """
        from lib.fs_authority import observe_directory

        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        loop = os.path.join(album, "01 - loop.flac")
        os.symlink(loop, loop)
        self.assertEqual(observe_directory(loop).presence, "indeterminate")
        result = self._compute_over(album)
        self.assertEqual(result.outcome, "folder_unavailable")

    def test_refusal_attribution_survives_an_ambient_os_error(self) -> None:
        """A refusal names ONE file; an unrelated error is not evidence.

        Python sets ``__context__`` implicitly to whatever exception is
        being handled anywhere up the stack, so reading a corrupt file
        from inside an ``except OSError:`` block put that unrelated error
        on the chain — and the walk reported a refusal with the wrong
        errno and someone else's filename.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        shutil.copy(FIXTURE_FLAC, os.path.join(album, "01 - one.flac"))
        with open(os.path.join(album, "02 - garbage.flac"), "wb") as handle:
            handle.write(b"not a flac at all" * 8)
        try:
            raise PermissionError(13, "Permission denied", "/elsewhere.flac")
        except OSError:
            result = self._compute_over(album)
        self.assertEqual(result.outcome, "ok")
        self.assertIsNone(result.partial_read)

    def test_a_nameless_ambient_error_is_the_accepted_cost(self) -> None:
        """Pin the half the relaxation newly ACCEPTS, in both directions.

        "Names the subject, or names nothing" rejects an error naming a
        DIFFERENT file — the ambient-leak protection that survives — but
        accepts a NAMELESS one, because only ``open()`` attaches a
        filename and the live mid-read producer therefore has none. The
        cost is that a nameless *ambient* error is accepted too.

        Unreachable today: the sole production caller does not run inside
        an ``except OSError:`` body. Pinned anyway, so a future caller
        that does sit under one fails here instead of shipping a false
        refusal with everything green. If someone later closes the hole,
        this test is what tells them they changed the contract.
        """
        from lib.fs_authority import os_refusal_in_chain

        subject = "/album/01.flac"
        # ACCEPTED: nameless. This is the live mid-read shape.
        nameless = OSError(errno.EIO, "Input/output error")
        self.assertIs(
            os_refusal_in_chain(nameless, subject=subject), nameless,
            "a nameless OSError must still be found — the deployment's "
            "real mid-read refusal has no filename",
        )
        # REJECTED: names a different file. The protection that survives.
        self.assertIsNone(os_refusal_in_chain(
            OSError(errno.EIO, "Input/output error", "/elsewhere.flac"),
            subject=subject,
        ))
        # The accepted cost, stated as a fact rather than a hope: an
        # ambient nameless error reached through ``__context__`` is
        # indistinguishable from the mid-read one and IS accepted.
        try:
            raise OSError(errno.EIO, "Input/output error")
        except OSError:
            ambient_chain = ValueError("unrelated parse failure")
            try:
                raise ambient_chain
            except ValueError as exc:
                found = os_refusal_in_chain(exc, subject=subject)
        self.assertIsNotNone(
            found,
            "if this now returns None the residual hole was closed — "
            "update this pin deliberately rather than deleting it",
        )

    def test_both_real_refusal_producers_are_found_and_attributed(self) -> None:
        """Rule C: the attribution guard rests on facts of the real stack.

        There are TWO producers, and they carry different evidence:

        * **open-time** (EACCES on a mode-0000 file) — ``open()`` attaches
          the exact filename, which is what lets an unrelated ambient
          error be rejected;
        * **mid-read** (EIO/ESTALE on an already-open descriptor — the
          shape this deployment's nested virtiofs really produces) —
          ``FileIO.read`` attaches NO filename, and the errno and the
          filename land on two different links of the chain.

        Requiring a filename therefore drops the live producer into
        ``no_audio``, which is why the guard accepts "names the subject,
        or names nothing". Both halves are pinned so a future upgrade
        that changed either producer fails loudly.
        """
        from lib.beets_distance import _item_from_path
        from lib.fs_authority import os_refusal_in_chain

        root = self.enterContext(tempfile.TemporaryDirectory())

        locked = os.path.join(root, "locked.flac")
        shutil.copy(FIXTURE_FLAC, locked)
        os.chmod(locked, 0o000)
        self.addCleanup(os.chmod, locked, 0o600)
        with self.assertRaises(Exception) as caught_open:
            _item_from_path(locked)
        opened = os_refusal_in_chain(caught_open.exception, subject=locked)
        assert opened is not None
        self.assertEqual(opened.filename, locked)
        self.assertEqual(opened.errno, errno.EACCES)
        # …and the same chain read for a DIFFERENT subject is not evidence.
        self.assertIsNone(os_refusal_in_chain(
            caught_open.exception, subject=os.path.join(root, "other.flac")))

        # /proc/self/mem opens fine and answers EIO on every read: a real
        # mid-read refusal with no flaky mount required.
        mid_read = os.path.join(root, "mid-read.flac")
        os.symlink("/proc/self/mem", mid_read)
        with self.assertRaises(Exception) as caught_read:
            _item_from_path(mid_read)
        during = os_refusal_in_chain(caught_read.exception, subject=mid_read)
        assert during is not None
        self.assertEqual(during.errno, errno.EIO)
        self.assertIsNone(
            during.filename,
            "a read on an open descriptor attaches no filename — the guard "
            "must not require one",
        )

        # beets works in ``syspath`` bytes internally, so a bytes filename
        # naming the SAME file must still match, and one naming a
        # different file must still be rejected.
        as_bytes = PermissionError(
            errno.EACCES, "Permission denied", os.fsencode(locked))
        self.assertIs(os_refusal_in_chain(as_bytes, subject=locked), as_bytes)
        self.assertIsNone(os_refusal_in_chain(
            PermissionError(errno.EACCES, "Permission denied",
                            os.fsencode(os.path.join(root, "other.flac"))),
            subject=locked))

    def test_unparseable_file_is_not_reported_as_a_refusal(self) -> None:
        """Must still work: a corrupt tag block is a fact about the file.

        It carries no ``OSError`` anywhere on its exception chain, so it
        must not claim ``partial_read`` — that would make the signal
        meaningless, which is the mirror-image of the bug.
        """
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        shutil.copy(FIXTURE_FLAC, os.path.join(album, "01 - one.flac"))
        with open(os.path.join(album, "02 - garbage.flac"), "wb") as handle:
            handle.write(b"not a flac at all" * 8)
        result = self._compute_over(album)
        self.assertEqual(result.outcome, "ok")
        self.assertIsNone(result.partial_read)
        self.assertEqual(result.total_local_tracks, 1)

    def test_empty_readable_folder_is_still_no_audio(self) -> None:
        """Must still work: a readable folder with no audio is no_audio."""
        root = self.enterContext(tempfile.TemporaryDirectory())
        album = os.path.join(root, "wrong_matches", "Album")
        os.makedirs(album)
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": album}},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            observe_failed_path=_present,
        )
        self.assertEqual(r.outcome, "no_audio")

    def test_folder_missing_when_validation_result_absent(self) -> None:
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": None},
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        r = compute_beets_distance(
            1, "rel-x",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
            observe_failed_path=_absent,
        )
        self.assertEqual(r.outcome, "folder_missing")

    def test_no_audio_when_folder_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pdb = _StubPDB(
                download_log_entry={"id": 1, "request_id": 7,
                                    "validation_result": {"failed_path": tmp}},
                request={"id": 7, "mb_release_group_id": "rg-shared"},
            )
            r = compute_beets_distance(
                1, "rel-x",
                pdb=pdb,
                mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
                observe_failed_path=_present,
            )
            self.assertEqual(r.outcome, "no_audio")
            self.assertEqual(r.folder_path, tmp)


# ============================================================================
# Integration slice — real beets distance against real on-disk audio.
# ============================================================================


def _make_tagged_folder(
    target_dir: str,
    tracks: list[dict],
) -> None:
    """Copy the sine fixture N times into ``target_dir`` and apply tags.

    Each ``tracks[i]`` dict supplies the tag fields. We use FLAC because
    the fixture set already contains a FLAC; sine_440.mp3 would work
    too. Length is fixed (the fixture is ~1s) — distance compares to
    ``length_seconds`` on the MB side, so we set the MB lengths to
    match the fixture for a clean ``ok``.
    """
    for i, t in enumerate(tracks):
        dest = os.path.join(target_dir, f"{i + 1:02d} - {t['title']}.flac")
        shutil.copyfile(FIXTURE_FLAC, dest)
        f = music_tag.load_file(dest)
        assert f is not None
        f["artist"] = t["artist"]
        f["album"] = t["album"]
        f["albumartist"] = t.get("albumartist", t["artist"])
        f["title"] = t["title"]
        f["tracknumber"] = t["track"]
        f["totaltracks"] = len(tracks)
        f["discnumber"] = t.get("disc", 1)
        f.save()


class TestBeetsDistanceIntegrationSlice(unittest.TestCase):
    """Drive the real beets distance pipeline end-to-end.

    The fixture FLACs are ~1 s; we tag them with realistic metadata
    and point MB-side TrackInfo lengths at the fixture's true length
    so a clean tag set produces a small distance. Then we mutate the
    tags and confirm the distance grows. That's enough signal to
    prove the real beets distance call is plumbed correctly without
    coupling to specific numeric values that the beets default
    weights can shift between versions.
    """

    @classmethod
    def setUpClass(cls) -> None:
        # Read the fixture's real length once so MB tracks line up
        # with whatever the on-disk file actually decodes to. Import
        # via ``lib.beets_distance``, which pins the upstream
        # ``beets.library`` module eagerly at load time. (Historically
        # this also guarded against tests injecting ``tests/../lib``
        # onto sys.path and shadowing the real ``beets`` package —
        # those inserts are gone and TestSysPathAudit bans the class.)
        from lib.beets_distance import _beets_library
        item = _beets_library.Item.from_path(FIXTURE_FLAC)
        cls.fixture_length = float(item.get("length") or 1.0)

    def _build_request_and_mb(self, *, artist: str, album: str, titles: list[str]):
        tracks = [
            {
                "disc_number": 1,
                "track_number": i + 1,
                "title": title,
                "length_seconds": self.fixture_length,
            }
            for i, title in enumerate(titles)
        ]
        mb = {
            "id": "rel-aaa",
            "title": album,
            "artist_name": artist,
            "artist_id": "artist-1",
            "release_group_id": "rg-shared",
            "year": 2020,
            "date": "2020-01-01",
            "country": "US",
            "status": "Official",
            "tracks": tracks,
        }
        return mb

    def _run_compute(
        self,
        *,
        folder: str,
        mb: dict,
        cache: BeetsDistanceCache | None = None,
    ) -> BeetsDistanceResult:
        pdb = _StubPDB(
            download_log_entry={
                "id": 100,
                "request_id": 7,
                "validation_result": {"failed_path": folder},
            },
            request={"id": 7, "mb_release_group_id": "rg-shared"},
        )
        return compute_beets_distance(
            100, "rel-aaa",
            pdb=pdb,
            mb_get_release=lambda mbid: mb,
            observe_failed_path=_present,
            cache=cache,
        )

    def test_clean_match_is_low_distance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _make_tagged_folder(tmp, [
                {"title": "Intro",  "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 1},
                {"title": "3000",   "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 2},
            ])
            mb = self._build_request_and_mb(
                artist="Dr. Octagon",
                album="Dr. Octagonecologyst",
                titles=["Intro", "3000"],
            )
            r = self._run_compute(folder=tmp, mb=mb)

            self.assertEqual(r.outcome, "ok", msg=r.error_message)
            assert r.distance is not None
            self.assertGreaterEqual(r.distance, 0.0)
            self.assertLess(r.distance, 0.5, msg="clean tag match should score < 0.5")
            self.assertEqual(r.matched_tracks, 2)
            self.assertEqual(r.total_local_tracks, 2)
            self.assertEqual(r.total_mb_tracks, 2)
            assert r.duration_ms is not None
            # First-read latency: tag IO + beets fit. Generous ceiling
            # so the test doesn't flake on slow CI; the cached-fast-path
            # test below pins what "fast" actually means.
            self.assertLess(r.duration_ms, 10_000)

    def test_wrong_album_metadata_is_higher_distance(self) -> None:
        """Sanity: mistagged folder vs. correct MB → distance grows."""
        with tempfile.TemporaryDirectory() as tmp:
            _make_tagged_folder(tmp, [
                {"title": "Nothing Like It",  "artist": "Other Person", "album": "Wrong Album", "track": 1},
                {"title": "Some Other Song",  "artist": "Other Person", "album": "Wrong Album", "track": 2},
            ])
            mb = self._build_request_and_mb(
                artist="Dr. Octagon",
                album="Dr. Octagonecologyst",
                titles=["Intro", "3000"],
            )
            r = self._run_compute(folder=tmp, mb=mb)
            self.assertEqual(r.outcome, "ok", msg=r.error_message)
            assert r.distance is not None
            self.assertGreater(r.distance, 0.3,
                msg="mismatched album metadata should score > 0.3")

    def test_cache_makes_second_call_fast(self) -> None:
        """Same folder, same MB → second compute reuses cached fingerprints.

        First call: tag IO across N files (slow).
        Second call: cache hit, no FS reads beyond os.walk + stat.

        We don't assert a hard wall-clock bound — flaky on shared
        hardware. Instead we assert (a) the cache picked up entries,
        and (b) the second call returns the same distance bit-for-bit
        (proves we round-tripped through the cache without drift).
        """
        cache = DictCache()
        with tempfile.TemporaryDirectory() as tmp:
            _make_tagged_folder(tmp, [
                {"title": "Intro",  "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 1},
                {"title": "3000",   "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 2},
            ])
            mb = self._build_request_and_mb(
                artist="Dr. Octagon",
                album="Dr. Octagonecologyst",
                titles=["Intro", "3000"],
            )
            r1 = self._run_compute(folder=tmp, mb=mb, cache=cache)
            self.assertEqual(r1.outcome, "ok")
            # Two files → two cache entries.
            self.assertEqual(len(cache._store), 2)
            r2 = self._run_compute(folder=tmp, mb=mb, cache=cache)
            self.assertEqual(r2.outcome, "ok")
            self.assertEqual(r1.distance, r2.distance,
                msg="cached fingerprint round-trip must reproduce the same distance")

    def test_distance_result_serializes_to_json(self) -> None:
        """Wire-boundary smoke test: result encodes via msgspec without error."""
        import msgspec
        with tempfile.TemporaryDirectory() as tmp:
            _make_tagged_folder(tmp, [
                {"title": "Intro",  "artist": "Dr. Octagon", "album": "Dr. Octagonecologyst", "track": 1},
            ])
            mb = self._build_request_and_mb(
                artist="Dr. Octagon",
                album="Dr. Octagonecologyst",
                titles=["Intro"],
            )
            r = self._run_compute(folder=tmp, mb=mb)
            self.assertEqual(r.outcome, "ok", msg=r.error_message)
            blob = msgspec.json.encode(r)
            self.assertIn(b'"outcome":"ok"', blob)
            # Round-trip back to a struct of the same shape.
            r2 = msgspec.json.decode(blob, type=BeetsDistanceResult)
            self.assertEqual(r2.distance, r.distance)


class TestBeetsMatchDistanceEraAdaptation(unittest.TestCase):
    """``_beets_match_distance`` calls beets' ``distance()`` with the exact
    argument shape each ``task_metadata_era`` requires (issue #1088)."""

    @staticmethod
    def _dummy_distance_inputs():
        """Minimal, correctly-typed (not semantically meaningful) beets
        objects — only the CALL SHAPE is under test here, but they must be
        real ``Item``/``AlbumInfo``/``TrackInfo`` instances to keep this
        seam test itself pyright-clean against ``_beets_match_distance``'s
        real signature."""
        from beets import library
        from beets.autotag import hooks

        items = [library.Item()]
        album_info = hooks.AlbumInfo(tracks=[], album="X", artist="Y", album_id="Z")
        mapping = [(library.Item(), hooks.TrackInfo(title="T", artist="Y", index=1))]
        extra_items = [library.Item(), library.Item()]
        return items, album_info, mapping, extra_items

    def test_legacy_era_calls_the_three_arg_shape(self) -> None:
        from unittest import mock

        from harness import beets_compat
        from lib import beets_distance as bd

        items, album_info, mapping, extra_items = self._dummy_distance_inputs()
        calls: list[tuple[object, ...]] = []

        def fake_distance(*args: object) -> str:
            calls.append(args)
            return "sentinel-distance"

        legacy_caps = dataclasses.replace(
            beets_compat.CAPABILITIES, task_metadata_era="legacy")
        with (
            mock.patch.object(beets_compat, "CAPABILITIES", legacy_caps),
            mock.patch.object(bd, "_beets_distance_fn", fake_distance),
        ):
            result = bd._beets_match_distance(items, album_info, mapping, extra_items)

        self.assertEqual(result, "sentinel-distance")
        self.assertEqual(calls, [(items, album_info, mapping)])

    def test_modern_era_calls_the_four_arg_likelies_shape(self) -> None:
        from unittest import mock

        from harness import beets_compat
        from lib import beets_distance as bd

        items, album_info, mapping, extra_items = self._dummy_distance_inputs()
        distance_calls: list[tuple[object, ...]] = []
        likelies_calls: list[object] = []

        def fake_get_most_common_tags(passed_items: object) -> str:
            likelies_calls.append(passed_items)
            return "sentinel-likelies"

        def fake_distance(*args: object) -> str:
            distance_calls.append(args)
            return "sentinel-distance"

        modern_caps = dataclasses.replace(
            beets_compat.CAPABILITIES, task_metadata_era="modern")
        with (
            mock.patch.object(beets_compat, "CAPABILITIES", modern_caps),
            mock.patch.object(bd, "_beets_distance_fn", fake_distance),
            mock.patch.object(bd, "_get_most_common_tags", fake_get_most_common_tags),
        ):
            result = bd._beets_match_distance(items, album_info, mapping, extra_items)

        self.assertEqual(result, "sentinel-distance")
        self.assertEqual(likelies_calls, [items])
        # unmatched_count is len(extra_items) — the exact upstream call-site
        # shape (beets/autotag/match.py: distance(source.data, info,
        # item_info_pairs, len(extra_items))), not len(items) or a track count.
        self.assertEqual(
            distance_calls, [("sentinel-likelies", album_info, mapping, 2)])


# ============================================================================
# items_override path — synthetic items scored without filesystem IO.
# ============================================================================


def _synth_items(titles: list[str], *, length: float = 60.0,
                 artist: str = "Dr. Octagon",
                 album: str = "Dr. Octagonecologyst",
                 disc: int = 1) -> list[SyntheticItem]:
    """Build a list of SyntheticItems matching ``_ok_mb_release`` defaults."""
    return [
        SyntheticItem(
            title=t,
            artist=artist,
            album=album,
            albumartist=artist,
            track=i + 1,
            tracktotal=len(titles),
            disc=disc,
            disctotal=1,
            length=length,
        )
        for i, t in enumerate(titles)
    ]


class _PDBExploder:
    """PDB stand-in whose every method raises — verifies override path
    skips DB completely (no get_download_log_entry, no get_request)."""

    def get_download_log_entry(self, log_id):  # pragma: no cover — must not be called
        raise AssertionError(
            "items_override path must NOT call get_download_log_entry")

    def get_request(self, request_id):  # pragma: no cover — must not be called
        raise AssertionError(
            "items_override path must NOT call get_request")


class TestComputeBeetsDistanceWithItemsOverride(unittest.TestCase):
    """Coverage for the additive ``items_override`` parameter.

    The override path scores caller-provided items without touching the
    filesystem or the download_log/request rows. Its guardrails are the
    same as the existing path except the cross-RG check is opt-in via
    ``mb_release_group_id``.
    """

    # ---------- Happy path: synthetic items match MB tracks ---------- #

    def test_happy_path_matches_with_zero_distance(self) -> None:
        """Synthetic items with matching titles/length → small distance, outcome ok."""
        mb = _ok_mb_release(mbid="rel-aaa", rg="rg-shared")
        # MB tracks are Intro (60s) and 3000 (180s) per _ok_mb_release default.
        items = [
            SyntheticItem(
                title="Intro", artist="Dr. Octagon", album="Dr. Octagonecologyst",
                albumartist="Dr. Octagon", track=1, tracktotal=2,
                disc=1, disctotal=1, length=60.0,
            ),
            SyntheticItem(
                title="3000", artist="Dr. Octagon", album="Dr. Octagonecologyst",
                albumartist="Dr. Octagon", track=2, tracktotal=2,
                disc=1, disctotal=1, length=180.0,
            ),
        ]
        r = compute_beets_distance(
            mbid="rel-aaa",
            items_override=items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(r.outcome, "ok", msg=r.error_message)
        assert r.distance is not None
        self.assertGreaterEqual(r.distance, 0.0)
        self.assertLess(r.distance, 0.5)
        self.assertIsNotNone(r.components)
        assert r.components is not None
        # components may be EMPTY here: with stock beets defaults a perfect
        # synthetic match accrues no penalties. The old assertGreater(len)
        # only held because the test process inherited the operator's
        # ~/.config/beets (match.preferred penalties) before tier-2 U5
        # pinned BEETSDIR — the per-component contract lives in
        # test_per_component_breakdown_penalises_wrong_title.
        # Override path means no download_log was consulted.
        self.assertIsNone(r.download_log_id)
        self.assertIsNone(r.request_id)
        self.assertEqual(r.matched_tracks, 2)
        self.assertEqual(r.total_local_tracks, 2)
        self.assertEqual(r.total_mb_tracks, 2)
        self.assertEqual(r.candidate_mbid, "rel-aaa")
        self.assertEqual(r.candidate_release_group_id, "rg-shared")

    # ---------- Track-count asymmetries ---------- #

    def test_mismatched_tracks_reports_extras(self) -> None:
        """12 synth items vs 10 MB tracks → matched=10, extra_local=2, extra_mb=0."""
        mb_tracks = [
            {"disc_number": 1, "track_number": i + 1,
             "title": f"Track {i + 1}", "length_seconds": 60.0}
            for i in range(10)
        ]
        mb = _ok_mb_release(mbid="rel-aaa", rg="rg-shared", tracks=mb_tracks)
        items = _synth_items([f"Track {i + 1}" for i in range(12)])
        r = compute_beets_distance(
            mbid="rel-aaa",
            items_override=items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(r.outcome, "ok", msg=r.error_message)
        self.assertEqual(r.total_local_tracks, 12)
        self.assertEqual(r.total_mb_tracks, 10)
        self.assertEqual(r.matched_tracks, 10)
        self.assertEqual(r.extra_local_tracks, 2)
        self.assertEqual(r.extra_mb_tracks, 0)

    # ---------- Per-component penalties ---------- #

    def test_per_component_breakdown_penalises_wrong_title(self) -> None:
        """Wrong track-title → "tracks" component carries a non-zero penalty.

        Beets aggregates per-track distances (title + length + position)
        under the single ``tracks`` key, so we assert on the aggregate
        rather than separate ``track_title`` / ``track_length`` keys.
        Distinct ``length_penalty`` clarity is preserved by comparing a
        wrong-title result to a clean-tag baseline.
        """
        mb_tracks = [
            {"disc_number": 1, "track_number": 1, "title": "RealTitle",
             "length_seconds": 60.0},
        ]
        mb = _ok_mb_release(mbid="rel-aaa", rg="rg-shared", tracks=mb_tracks)
        wrong_title_items = [
            SyntheticItem(
                title="CompletelyDifferentTitle",
                artist="Dr. Octagon", album="Dr. Octagonecologyst",
                albumartist="Dr. Octagon",
                track=1, tracktotal=1, disc=1, disctotal=1,
                length=60.0,
            ),
        ]
        clean_items = [
            SyntheticItem(
                title="RealTitle",
                artist="Dr. Octagon", album="Dr. Octagonecologyst",
                albumartist="Dr. Octagon",
                track=1, tracktotal=1, disc=1, disctotal=1,
                length=60.0,
            ),
        ]
        wrong = compute_beets_distance(
            mbid="rel-aaa",
            items_override=wrong_title_items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        clean = compute_beets_distance(
            mbid="rel-aaa",
            items_override=clean_items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(wrong.outcome, "ok", msg=wrong.error_message)
        self.assertEqual(clean.outcome, "ok", msg=clean.error_message)
        assert wrong.components is not None and clean.components is not None
        wrong_tracks = wrong.components.get("tracks", 0.0)
        clean_tracks = clean.components.get("tracks", 0.0)
        self.assertGreater(wrong_tracks, clean_tracks,
            msg=(f"expected wrong-title tracks penalty > clean tracks penalty, "
                 f"got wrong={wrong.components} clean={clean.components}"))

    # ---------- MB-side guardrails still fire ---------- #

    def test_mb_lookup_failed_in_override_path(self) -> None:
        """mb_get_release returns None → mb_lookup_failed, no IO attempted."""
        items = _synth_items(["A", "B"])
        r = compute_beets_distance(
            mbid="rel-x",
            items_override=items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: None,
        )
        self.assertEqual(r.outcome, "mb_lookup_failed")
        self.assertIsNone(r.distance)
        self.assertIsNone(r.folder_path)

    def test_mb_no_release_group_in_override_path(self) -> None:
        """MB release lacks release_group_id → mb_no_release_group, no IO.

        Step 4 still fires when the caller supplies an RG to compare
        against — the candidate without an RG would fail step 5
        anyway, and step 4 gives the more specific error.
        """
        mb = _ok_mb_release(mbid="rel-x")
        mb["release_group_id"] = None
        items = _synth_items(["A", "B"])
        r = compute_beets_distance(
            mbid="rel-x",
            items_override=items,
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(r.outcome, "mb_no_release_group")
        self.assertIsNone(r.distance)
        self.assertIsNone(r.folder_path)

    def test_orphan_candidate_succeeds_when_caller_rg_is_null(self) -> None:
        """Issue #384: candidate has no release_group_id AND caller passes
        ``mb_release_group_id=None`` → distance is computed.

        Step 4's hard-fail is narrowed: with no caller-supplied RG, the
        cross-RG guardrail (step 5) would skip anyway, so step 4 has
        nothing to early-exit on. This is the path the YouTube resolver
        uses for orphan releases (Discogs releases with no master,
        legacy MB releases without a release group).
        """
        mb = _ok_mb_release(mbid="rel-orphan")
        mb["release_group_id"] = None  # orphan release
        items = _synth_items(["Intro", "3000"])
        r = compute_beets_distance(
            mbid="rel-orphan",
            items_override=items,
            mb_release_group_id=None,  # opt out — orphan scoring contract
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: mb,
        )
        self.assertEqual(r.outcome, "ok", msg=r.error_message)
        self.assertIsNotNone(r.distance)
        self.assertIsNone(r.candidate_release_group_id)
        self.assertIsNone(r.request_release_group_id)

    # ---------- Empty items list ---------- #

    def test_empty_items_override_distinct_outcome(self) -> None:
        """items_override=[] → empty_items_override outcome (NOT no_audio).

        The two are deliberately distinguishable — empty_items_override is a
        caller error, no_audio means a real folder on disk had no readable
        audio. Conflating them would erode audit data.
        """
        r = compute_beets_distance(
            mbid="rel-x",
            items_override=[],
            mb_release_group_id="rg-shared",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
        )
        self.assertEqual(r.outcome, "empty_items_override")
        self.assertIsNone(r.distance)
        # The empty-items condition is detected without DB / MB / FS IO.
        self.assertIsNone(r.folder_path)

    # ---------- Input validation guardrail ---------- #

    def test_invalid_input_both_signaled(self) -> None:
        """Both download_log_id AND items_override → invalid_input, no IO."""
        items = _synth_items(["A"])
        r = compute_beets_distance(
            download_log_id=42,
            items_override=items,
            mbid="rel-x",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
        )
        self.assertEqual(r.outcome, "invalid_input")
        self.assertIsNone(r.distance)
        # No DB / MB / FS touch — the exploder PDB would have raised.

    def test_invalid_input_neither_signaled(self) -> None:
        """Neither download_log_id NOR items_override → invalid_input, no IO."""
        r = compute_beets_distance(
            mbid="rel-x",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
        )
        self.assertEqual(r.outcome, "invalid_input")
        self.assertIsNone(r.distance)

    # ---------- Cross-RG guardrail in the override path ---------- #

    def test_cross_rg_guardrail_fires_with_explicit_rg(self) -> None:
        """items_override + mb_release_group_id pointing at a different RG
        from the candidate → wrong_release_group, no scoring attempted."""
        items = _synth_items(["A", "B"])
        # Candidate MBID's RG is "rg-other" but caller asserts "rg-source".
        r = compute_beets_distance(
            mbid="rel-alien",
            items_override=items,
            mb_release_group_id="rg-source",
            pdb=_PDBExploder(),
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
        )
        self.assertEqual(r.outcome, "wrong_release_group")
        self.assertEqual(r.request_release_group_id, "rg-source")
        self.assertEqual(r.candidate_release_group_id, "rg-other")
        # No scoring — distance never computed.
        self.assertIsNone(r.distance)
        self.assertIsNone(r.folder_path)

    def test_cross_rg_guardrail_skipped_without_rg_param(self) -> None:
        """items_override + mb_release_group_id=None → guardrail off; proceeds
        to scoring even when candidate MBID's RG differs from anything implicit.
        The function MUST NOT consult any request row in this path."""
        items = _synth_items(["Intro", "3000"])
        # Candidate is in rg-other; no mb_release_group_id passed, so no check.
        r = compute_beets_distance(
            mbid="rel-alien",
            items_override=items,
            pdb=_PDBExploder(),  # would raise if DB consulted
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
        )
        self.assertEqual(r.outcome, "ok", msg=r.error_message)
        self.assertEqual(r.candidate_release_group_id, "rg-other")
        # No request RG was looked up — the override-without-RG path doesn't
        # know the caller's RG and doesn't make one up.
        self.assertIsNone(r.request_release_group_id)

    # ---------- Regression: Replace-picker path unchanged ---------- #

    def test_replace_picker_path_unchanged_when_no_override(self) -> None:
        """download_log_id alone, no items_override, no mb_release_group_id →
        exact same behaviour as before. Drive to a known outcome (wrong_release_group)
        to prove the existing guardrails still fire identically.
        """
        pdb = _StubPDB(
            download_log_entry={"id": 1, "request_id": 7,
                                "validation_result": {"failed_path": "/whatever"}},
            request={"id": 7, "mb_release_group_id": "rg-source"},
        )
        r = compute_beets_distance(
            download_log_id=1,
            mbid="rel-alien",
            pdb=pdb,
            mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid, rg="rg-other"),
            observe_failed_path=_present,
        )
        # Existing test_wrong_release_group_guardrail asserts the same shape.
        self.assertEqual(r.outcome, "wrong_release_group")
        self.assertEqual(r.request_release_group_id, "rg-source")
        self.assertEqual(r.candidate_release_group_id, "rg-other")
        self.assertEqual(r.download_log_id, 1)
        self.assertEqual(r.request_id, 7)


if __name__ == "__main__":
    unittest.main()
