"""Outcome-branch coverage for the operator merge-rekey action (#1089).

``MergeRekeyService.rekey_request`` is the ONE canonical execution path
behind both ``POST /api/pipeline/<id>/merge-rekey`` and
``pipeline-cli merge-rekey``. Every branch below is named by its
``RESULT_*`` outcome; ``tests/test_pipeline_db.py::TestMergeRekeyUnderOperatorClaim``
covers the real-PostgreSQL write this service's happy path,
``rekey_refused``, and ``survivor_collision`` branches delegate to.
"""

from __future__ import annotations

import sqlite3
import unittest

from lib.beets_db import CurrentBeetsAmbiguous, CurrentBeetsMissing
from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload
from lib.mb_canonical import (
    CanonicalReleaseAnswer,
    CanonicalReleaseCurrent,
    CanonicalReleaseRedirected,
    CanonicalReleaseUnavailable,
)
from lib.merge_rekey_service import (
    RESULT_BEETS_UNAVAILABLE,
    RESULT_LIBRARY_NOT_AT_SURVIVOR,
    RESULT_LIBRARY_STILL_AT_STORED,
    RESULT_MIRROR_UNAVAILABLE,
    RESULT_NOT_FOUND,
    RESULT_NOT_MERGED,
    RESULT_REKEY_REFUSED,
    RESULT_REKEYED,
    RESULT_SURVIVOR_COLLISION,
    RESULT_WRONG_STATE,
    MergeRekeyService,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import handoff_automation_owner, make_request_row

MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"
REQUEST_ID = 8792


def _locked_error() -> sqlite3.OperationalError:
    """A real ``sqlite3`` exception shaped exactly like
    ``beets_authority_availability_category`` classifies (test-fidelity
    Rule B: never a synthetic stand-in)."""
    error = sqlite3.OperationalError("database is locked")
    error.sqlite_errorcode = sqlite3.SQLITE_LOCKED
    return error


class RecordingCanonical:
    """A recording TAGGED merge-survivor lookup. Never a network call in
    tests. Returns whichever :class:`CanonicalReleaseAnswer` the test
    configures — see :func:`lib.mb_canonical.canonical_release_status` for
    the production contract this stands in for."""

    def __init__(self, answer: CanonicalReleaseAnswer) -> None:
        self.answer = answer
        self.calls: list[str] = []

    def __call__(self, release_id: str) -> CanonicalReleaseAnswer:
        self.calls.append(release_id)
        return self.answer


class TestMergeRekeyServiceOutcomes(unittest.TestCase):
    def _db(self, **overrides: object) -> FakePipelineDB:
        db = FakePipelineDB()
        fields: dict[str, object] = {
            "id": REQUEST_ID,
            "mb_release_id": MERGED,
            "status": "imported",
            "active_automation_import_job_id": None,
            "artist_name": "Slipknot",
            "album_title": "Vol. 3: (The Subliminal Verses)",
        }
        fields.update(overrides)
        db.seed_request(make_request_row(**fields))
        return db

    def _service(
        self,
        db: FakePipelineDB,
        beets: FakeBeetsDB,
        *,
        answer: CanonicalReleaseAnswer,
    ) -> tuple[MergeRekeyService, RecordingCanonical]:
        canonical = RecordingCanonical(answer)
        return (
            MergeRekeyService(db, beets, canonical_release_fn=canonical),
            canonical,
        )

    def test_a_missing_request_is_not_found(self) -> None:
        db = FakePipelineDB()
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(999)

        self.assertEqual(result.outcome, RESULT_NOT_FOUND)
        self.assertEqual(result.request_id, 999)
        self.assertEqual(canonical.calls, [])

    def test_a_non_imported_request_is_wrong_state(self) -> None:
        for status in ("wanted", "downloading", "unsearchable", "replaced"):
            with self.subTest(status=status):
                db = self._db(status=status)
                beets = FakeBeetsDB()
                service, canonical = self._service(
                    db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
                )

                result = service.rekey_request(REQUEST_ID)

                self.assertEqual(result.outcome, RESULT_WRONG_STATE)
                self.assertEqual(canonical.calls, [])

    def test_an_owned_processing_request_is_wrong_state(self) -> None:
        """A real ``wanted -> downloading -> processing`` transcript.

        Uses ``handoff_automation_owner`` rather than seeding the owner
        pointer directly — the fake's ``get_request`` presentation join
        requires a real joined automation job for a ``processing`` row,
        exactly like production.
        """
        db = self._db(status="wanted", active_automation_import_job_id=None)
        handoff_automation_owner(
            db,
            REQUEST_ID,
            state={
                "filetype": "flac",
                "enqueued_at": "2026-08-13T00:00:00+00:00",
                "current_path": "/processing/albums/slipknot",
                "files": [],
            },
            canonical_path="/processing/albums/slipknot",
        )
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(canonical.calls, [])

    def test_a_discogs_sourced_request_is_wrong_state(self) -> None:
        """Not MB-sourced — the resolver has no redirect concept for it."""
        db = self._db(mb_release_id="1870", discogs_release_id="1870")
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(canonical.calls, [])

    def test_a_request_with_no_release_identity_is_wrong_state(self) -> None:
        db = self._db(mb_release_id=None, discogs_release_id=None)
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertIsNone(result.old_release_id)
        self.assertEqual(canonical.calls, [])

    def test_wrong_state_reports_the_raw_stored_id_on_a_conflicting_identity(
        self,
    ) -> None:
        """#1089 NOTE-11: a conflicting ``discogs_release_id`` makes
        ``ReleaseIdentity.from_strict_fields`` return ``None`` — the
        refusal must still name the raw stored ``mb_release_id`` rather
        than reading as "no identity at all"."""
        db = self._db(mb_release_id=MERGED, discogs_release_id="12345678")
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(result.old_release_id, MERGED)
        self.assertEqual(canonical.calls, [])

    def test_a_down_mirror_is_mirror_unavailable_not_not_merged(self) -> None:
        """#1089 BLOCKING-1: a configured-but-unreachable mirror must
        report ``mirror_unavailable`` (retryable), never ``not_merged`` — a
        transport failure is not MusicBrainz answering "no redirect"."""
        db = self._db()
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseUnavailable(),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_MIRROR_UNAVAILABLE)
        self.assertEqual(result.old_release_id, MERGED)
        # The tagged resolver WAS asked — unlike the retired
        # is_mirror_configured_fn seam, this outcome is discovered by
        # asking and being told "no answer", not by a pre-check that
        # skips the call.
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_an_answered_no_redirect_is_not_merged(self) -> None:
        """#1089 BLOCKING-1: the #8792 refusal — MusicBrainz ANSWERED and
        names no different survivor. Distinct from a down mirror."""
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [6612, 18672])
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseCurrent(),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_NOT_MERGED)
        self.assertEqual(result.old_release_id, MERGED)
        self.assertEqual(result.new_release_id, MERGED)
        self.assertEqual(result.beets_checked_release_id, MERGED)
        self.assertEqual(set(result.beets_album_ids), {6612, 18672})
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_a_resolver_that_hands_back_the_stored_id_is_not_merged(self) -> None:
        """Defensive re-check: never trust a same-id redirect, even one the
        real resolver's own contract forbids returning."""
        db = self._db()
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(MERGED),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_NOT_MERGED)
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_a_non_musicbrainz_survivor_is_not_merged(self) -> None:
        """Defensive re-check: no adapter between MusicBrainz and Discogs."""
        db = self._db()
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected("1870"),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_NOT_MERGED)
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_a_beets_failure_resolving_the_stored_id_is_beets_unavailable(
        self,
    ) -> None:
        """MINOR-5, the ``not_merged`` path's own stored-id lookup."""
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_resolve_current_release_error(MERGED, _locked_error())
        service, _canonical = self._service(
            db, beets, answer=CanonicalReleaseCurrent(),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_BEETS_UNAVAILABLE)
        self.assertIsNotNone(result.error_message)
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_beets_missing_the_survivor_is_library_not_at_survivor(self) -> None:
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [])
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_LIBRARY_NOT_AT_SURVIVOR)
        self.assertEqual(result.new_release_id, SURVIVOR)
        self.assertEqual(result.beets_checked_release_id, SURVIVOR)
        self.assertEqual(result.beets_album_ids, ())
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_beets_ambiguous_at_the_survivor_is_library_not_at_survivor(
        self,
    ) -> None:
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [19345, 19999])
        service, _canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_LIBRARY_NOT_AT_SURVIVOR)
        self.assertEqual(set(result.beets_album_ids), {19345, 19999})
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_a_beets_failure_resolving_the_survivor_is_beets_unavailable(
        self,
    ) -> None:
        """MINOR-5, the survivor-must-be-unique lookup."""
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_resolve_current_release_error(SURVIVOR, _locked_error())
        service, _canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_BEETS_UNAVAILABLE)
        self.assertIsNotNone(result.error_message)
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_library_still_at_stored_refuses_before_any_write(self) -> None:
        """#1089 MAJOR-3: "exactly one album at the survivor" alone does not
        witness the library MOVED. Both ids resolve to real albums here —
        an unrelated album occupies the survivor while the request's own
        album still sits at the merged-away id — and the ledger must not
        transplant its evidence lineage onto that unrelated album."""
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [111])
        beets.set_album_ids_for_release(SURVIVOR, [19345])
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_LIBRARY_STILL_AT_STORED)
        self.assertEqual(result.old_release_id, MERGED)
        self.assertEqual(result.new_release_id, SURVIVOR)
        self.assertEqual(result.beets_checked_release_id, MERGED)
        self.assertEqual(result.beets_album_ids, (111,))
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)
        self.assertEqual(db.update_request_release_for_merge_calls, [])

    def test_a_beets_failure_resolving_the_still_at_stored_check_is_beets_unavailable(
        self,
    ) -> None:
        """MINOR-5, MAJOR-3's own re-check of the stored id, reached only
        once the survivor already resolved to exactly one album."""
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [19345])
        beets.set_resolve_current_release_error(MERGED, _locked_error())
        service, _canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_BEETS_UNAVAILABLE)
        self.assertIsNotNone(result.error_message)
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_survivor_collision_blocks_before_any_write(self) -> None:
        """#1089 MAJOR-2: a rival request already at the survivor persists
        until an operator acts — this must not be folded into
        ``rekey_refused`` (whose message tells the operator to just
        retry, which cannot help a permanent collision), and the write
        must never run."""
        db = self._db()
        db.seed_request(make_request_row(
            id=REQUEST_ID + 1, mb_release_id=SURVIVOR, status="imported",
        ))
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [19345])
        service, _canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_SURVIVOR_COLLISION)
        self.assertEqual(result.rival_request_id, REQUEST_ID + 1)
        self.assertEqual(result.colliding_fingerprints, ())
        self.assertIsNotNone(result.error_message)
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)
        self.assertEqual(db.update_request_release_for_merge_calls, [])

    def test_an_in_flight_import_job_makes_the_write_refuse(self) -> None:
        """The DB claim arm's own ``NOT EXISTS`` term, reached through the
        service — a genuinely transient cause, unlike ``survivor_collision``
        above."""
        db = self._db()
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=REQUEST_ID,
            dedupe_key=f"force-{REQUEST_ID}",
            payload=force_import_payload(
                download_log_id=1, failed_path="/quarantine/slipknot",
            ),
        )
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [19345])
        service, _canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_REKEY_REFUSED)
        self.assertEqual(result.new_release_id, SURVIVOR)
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_a_clean_world_rekeys(self) -> None:
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [19345])
        service, canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_REKEYED)
        self.assertEqual(result.old_release_id, MERGED)
        self.assertEqual(result.new_release_id, SURVIVOR)
        self.assertEqual(result.beets_album_id, 19345)
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], SURVIVOR)
        self.assertEqual(
            db.update_request_release_for_merge_calls,
            [(REQUEST_ID, MERGED, SURVIVOR, None)],
        )

    def test_the_default_seam_reads_the_process_wide_configuration(
        self,
    ) -> None:
        """No injected kwarg → the seam reads shared process state.

        An unwired process (``configure_canonical_base(None)``, the startup
        default before ``configure_canonical_release_lookup`` runs) must
        report ``mirror_unavailable`` — the tagged resolver's own
        ``CanonicalReleaseUnavailable`` answer, not a silent "no redirect"
        degrade.
        """
        from lib.mb_canonical import (
            configure_canonical_base,
            configured_canonical_base,
        )

        previous = configured_canonical_base()
        self.addCleanup(configure_canonical_base, previous)
        configure_canonical_base(None)

        db = self._db()
        beets = FakeBeetsDB()
        service = MergeRekeyService(db, beets)

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_MIRROR_UNAVAILABLE)


class TestMergeRekeyBeetsAlbumIdsHelper(unittest.TestCase):
    """Direct coverage of the small ``_beets_album_ids`` projection."""

    def test_ambiguous_reports_every_album_id(self) -> None:
        from lib.merge_rekey_service import _beets_album_ids

        identity = ReleaseIdentity(source="musicbrainz", release_id=SURVIVOR)
        self.assertEqual(
            _beets_album_ids(CurrentBeetsAmbiguous(
                identity=identity, album_ids=(1, 2), reason="multiple_matches",
            )),
            (1, 2),
        )

    def test_missing_reports_no_album_ids(self) -> None:
        from lib.merge_rekey_service import _beets_album_ids

        identity = ReleaseIdentity(source="musicbrainz", release_id=SURVIVOR)
        self.assertEqual(
            _beets_album_ids(CurrentBeetsMissing(identity=identity)), (),
        )


if __name__ == "__main__":
    unittest.main()
