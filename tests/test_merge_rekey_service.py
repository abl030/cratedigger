"""Outcome-branch coverage for the operator merge-rekey action (#1089).

``MergeRekeyService.rekey_request`` is the ONE canonical execution path
behind both ``POST /api/pipeline/<id>/merge-rekey`` and
``pipeline-cli merge-rekey``. Every branch below is named by its
``RESULT_*`` outcome; ``tests/test_pipeline_db.py::TestMergeRekeyUnderOperatorClaim``
covers the real-PostgreSQL write this service's happy path,
``rekey_refused``, and ``survivor_collision`` branches delegate to.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
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
    RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
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
from lib.quality_evidence import AlbumQualityEvidenceFile
from lib.release_identity import ReleaseIdentity
from tests.dispatch_helpers import handoff_automation_owner
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row

MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"
REQUEST_ID = 8792
#: The real, on-disk size a matching evidence-lineage fixture writes and
#: describes identically.
_LINEAGE_FILE_SIZE = 4096


def _write_real_audio_file(
    directory: str, relative_path: str, size_bytes: int,
) -> None:
    """A real file on disk with an exact byte count — ``snapshot_audio_files``
    walks the REAL filesystem, so the MAJOR-3 evidence-lineage witness can
    only be exercised with real bytes, never a seeded fake shape."""
    full_path = os.path.join(directory, relative_path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "wb") as handle:
        handle.write(b"\x00" * size_bytes)


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

    def _seed_matching_lineage(
        self,
        db: FakePipelineDB,
        beets: FakeBeetsDB,
        tmp_dir: str,
        *,
        album_id: int = 19345,
    ) -> int:
        """Seed a real audio file plus MATCHING evidence AT THE OLD id
        (#1089 MINOR-F, review round 3): pre-rekey, a request's linked
        current evidence is content-addressed under the release id it
        CURRENTLY holds (``MERGED`` here) — evidence seeded at the
        survivor instead is a shape production cannot produce before the
        rekey has already happened. Also wires the survivor's real Beets
        item path at the SAME bytes, so the mandatory witness (#1089
        MAJOR-C) passes and the write really moves the linked evidence row
        old→survivor. Returns the seeded evidence row's id.
        """
        real_path = os.path.join(tmp_dir, "01 Track.mp3")
        _write_real_audio_file(tmp_dir, "01 Track.mp3", _LINEAGE_FILE_SIZE)
        files = [AlbumQualityEvidenceFile(
            relative_path="01 Track.mp3", size_bytes=_LINEAGE_FILE_SIZE,
            mtime_ns=1_700_000_000_000_000_000, extension="mp3",
            container="mp3", codec="mp3",
        )]
        evidence = make_album_quality_evidence(
            mb_release_id=MERGED, source_path=tmp_dir, files=files,
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=MERGED,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        assert db.set_request_current_evidence(REQUEST_ID, stored.id)
        beets.set_album_ids_for_release(SURVIVOR, [album_id])
        beets.set_item_paths(SURVIVOR, [(album_id, real_path)])
        return stored.id

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
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = self._db()
            db.seed_request(make_request_row(
                id=REQUEST_ID + 1, mb_release_id=SURVIVOR, status="imported",
            ))
            beets = FakeBeetsDB()
            self._seed_matching_lineage(db, beets, tmp_dir)
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
        service — #1089 MINOR-4 (review round 2): a PRE-EXISTING queued
        job is the MOST ORDINARY cause of this refusal — nothing raced,
        and the message must say wait-for-drain, not retry."""
        with tempfile.TemporaryDirectory() as tmp_dir:
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
            evidence_id = self._seed_matching_lineage(db, beets, tmp_dir)
            service, _canonical = self._service(
                db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
            )

            result = service.rekey_request(REQUEST_ID)

            self.assertEqual(result.outcome, RESULT_REKEY_REFUSED)
            self.assertEqual(result.new_release_id, SURVIVOR)
            assert result.error_message is not None
            self.assertIn("queued or running import job", result.error_message)
            self.assertIn("wait for the job to finish", result.error_message)
            self.assertNotIn("— retry", result.error_message)
            row = db.request(REQUEST_ID)
            self.assertEqual(row["mb_release_id"], MERGED)
            evidence = db.load_album_quality_evidence_by_id(evidence_id)
            assert evidence is not None
            self.assertEqual(evidence.mb_release_id, MERGED)

    def test_a_genuine_race_with_no_active_job_says_retry(self) -> None:
        """#1089 MINOR-4 (review round 2): the converse of the above — the
        stored identity changed underneath the write (simulated by
        mutating state from inside the write call itself, the only place
        a synchronous fake can express a race) with NO import job
        involved at all. This is the one case where retry genuinely can
        succeed, so the message must keep saying so."""

        class _RacingDB(FakePipelineDB):
            def update_request_release_for_merge(self, request_id, **kwargs):
                self._requests[request_id]["mb_release_id"] = (
                    "cccccccc-cccc-cccc-cccc-cccccccccccc"
                )
                return super().update_request_release_for_merge(
                    request_id, **kwargs,
                )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _RacingDB()
            db.seed_request(make_request_row(
                id=REQUEST_ID, mb_release_id=MERGED, status="imported",
                active_automation_import_job_id=None,
                artist_name="Slipknot",
                album_title="Vol. 3: (The Subliminal Verses)",
            ))
            beets = FakeBeetsDB()
            self._seed_matching_lineage(db, beets, tmp_dir)
            service, _canonical = self._service(
                db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
            )

            result = service.rekey_request(REQUEST_ID)

            self.assertEqual(result.outcome, RESULT_REKEY_REFUSED)
            assert result.error_message is not None
            self.assertIn("— retry", result.error_message)
            self.assertNotIn("wait for the job to finish", result.error_message)
            self.assertNotIn("queued or running import job", result.error_message)

    def test_a_clean_world_rekeys_and_moves_the_linked_evidence(self) -> None:
        """#1089 MINOR-F (review round 3): the passing-witness world must
        also exercise the write MOVING the linked evidence row old→survivor
        (the composition the fake and real-PG write both perform in the
        same transaction as the identity move)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = self._db()
            beets = FakeBeetsDB()
            evidence_id = self._seed_matching_lineage(db, beets, tmp_dir)
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
            evidence = db.load_album_quality_evidence_by_id(evidence_id)
            assert evidence is not None
            self.assertEqual(
                evidence.mb_release_id, SURVIVOR,
                "the linked evidence row must move old->survivor with the "
                "identity, in the same write",
            )
            self.assertIsNone(
                db.find_album_quality_evidence(
                    mb_release_id=MERGED,
                    snapshot_fingerprint=evidence.snapshot_fingerprint,
                ),
                "nothing should remain behind at the merged-away id",
            )

    def test_no_linked_evidence_refuses(self) -> None:
        """#1089 MAJOR-C (review round 3): the witness is MANDATORY — no
        ``current_evidence_id`` at all is itself a refusal, not a skip.
        The write moves EVERY evidence row at the old id regardless of
        which one (if any) is linked, so "no linked row" is not "nothing
        to transplant"; there is simply nothing to verify the
        untracked-album adoption hazard against."""
        db = self._db(current_evidence_id=None)
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [19345])
        service, _canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_EVIDENCE_FINGERPRINT_MISMATCH)
        assert result.error_message is not None
        self.assertIn(
            "no current evidence lineage", result.error_message,
        )
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)
        self.assertEqual(db.update_request_release_for_merge_calls, [])

    def test_matching_evidence_lineage_rekeys(self) -> None:
        """Must-still-work (#1089 MAJOR-3, review round 2): a fresh import
        at the survivor whose evidence was captured from THOSE EXACT bytes
        must PASS this witness — the live cohort shape the fix must not
        break. Evidence seeded at the OLD id (#1089 MINOR-F, review round
        3) — the real pre-rekey shape."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = self._db()
            beets = FakeBeetsDB()
            self._seed_matching_lineage(db, beets, tmp_dir)
            service, _canonical = self._service(
                db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
            )

            result = service.rekey_request(REQUEST_ID)

            self.assertEqual(result.outcome, RESULT_REKEYED)
            row = db.request(REQUEST_ID)
            self.assertEqual(row["mb_release_id"], SURVIVOR)

    def test_mismatched_evidence_lineage_refuses_before_any_write(self) -> None:
        """#1089 MAJOR-3 (review round 2): the exact hazard this witness
        exists to catch — an unrelated, pipeline-untracked album occupies
        the survivor MBID, so the evidence describes bytes that are NOT
        what Beets actually holds there now. Never path equality: only the
        content fingerprint, freshly computed. Evidence seeded at the OLD
        id (#1089 MINOR-F, review round 3) — the real pre-rekey shape."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            _write_real_audio_file(tmp_dir, "01 Track.mp3", 4096)
            # The linked evidence describes a DIFFERENT size — bytes
            # nobody measured for the album that is really at the
            # survivor now.
            files = [AlbumQualityEvidenceFile(
                relative_path="01 Track.mp3", size_bytes=999999,
                mtime_ns=1_700_000_000_000_000_000, extension="mp3",
                container="mp3", codec="mp3",
            )]
            db = self._db()
            evidence = make_album_quality_evidence(
                mb_release_id=MERGED, source_path=tmp_dir, files=files,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=MERGED,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            self.assertTrue(
                db.set_request_current_evidence(REQUEST_ID, stored.id),
            )
            beets = FakeBeetsDB()
            beets.set_album_ids_for_release(SURVIVOR, [19345])
            beets.set_item_paths(
                SURVIVOR, [(19345, os.path.join(tmp_dir, "01 Track.mp3"))],
            )
            service, _canonical = self._service(
                db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
            )

            result = service.rekey_request(REQUEST_ID)

            self.assertEqual(
                result.outcome, RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
            )
            assert result.error_message is not None
            self.assertIn("operator must decide", result.error_message)
            row = db.request(REQUEST_ID)
            self.assertEqual(row["mb_release_id"], MERGED)
            self.assertEqual(db.update_request_release_for_merge_calls, [])

    def test_a_deleted_linked_evidence_row_refuses(self) -> None:
        """#1089 MAJOR-3 (review round 2): ``current_evidence_id`` points
        at a row that no longer exists — there is nothing to verify
        against, so this fails closed rather than silently proceeding."""
        db = self._db(current_evidence_id=999_999_999)
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [19345])
        service, _canonical = self._service(
            db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_EVIDENCE_FINGERPRINT_MISMATCH)
        assert result.error_message is not None
        self.assertIn("no longer exists", result.error_message)
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)
        self.assertEqual(db.update_request_release_for_merge_calls, [])

    def test_an_uncomputable_survivor_fingerprint_refuses(self) -> None:
        """#1089 MAJOR-3 (review round 2): a broken symlink makes
        ``os.stat`` raise inside ``snapshot_audio_files`` — the
        "uncomputable" half of the refusal, distinct from a clean
        mismatch. Evidence seeded at the OLD id (#1089 MINOR-F, review
        round 3) — the real pre-rekey shape."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.symlink(
                os.path.join(tmp_dir, "does-not-exist.mp3"),
                os.path.join(tmp_dir, "01 Track.mp3"),
            )
            files = [AlbumQualityEvidenceFile(
                relative_path="01 Track.mp3", size_bytes=4096,
                mtime_ns=1_700_000_000_000_000_000, extension="mp3",
                container="mp3", codec="mp3",
            )]
            db = self._db()
            evidence = make_album_quality_evidence(
                mb_release_id=MERGED, source_path=tmp_dir, files=files,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=MERGED,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            self.assertTrue(
                db.set_request_current_evidence(REQUEST_ID, stored.id),
            )
            beets = FakeBeetsDB()
            beets.set_album_ids_for_release(SURVIVOR, [19345])
            beets.set_item_paths(
                SURVIVOR, [(19345, os.path.join(tmp_dir, "01 Track.mp3"))],
            )
            service, _canonical = self._service(
                db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
            )

            result = service.rekey_request(REQUEST_ID)

            self.assertEqual(
                result.outcome, RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
            )
            assert result.error_message is not None
            self.assertIn("could not read", result.error_message)
            row = db.request(REQUEST_ID)
            self.assertEqual(row["mb_release_id"], MERGED)
            self.assertEqual(db.update_request_release_for_merge_calls, [])

    def test_a_vanished_or_empty_survivor_directory_refuses(self) -> None:
        """#1089 NOTE-I (review round 3): the survivor directory walks
        cleanly but has ZERO audio files (vanished, or genuinely empty) —
        must refuse, never silently compare against the well-defined
        empty-fileset digest. Distinct from the broken-symlink
        "uncomputable" case above, which RAISES; this one returns
        cleanly with nothing found."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            files = [AlbumQualityEvidenceFile(
                relative_path="01 Track.mp3", size_bytes=4096,
                mtime_ns=1_700_000_000_000_000_000, extension="mp3",
                container="mp3", codec="mp3",
            )]
            db = self._db()
            evidence = make_album_quality_evidence(
                mb_release_id=MERGED, source_path=tmp_dir, files=files,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=MERGED,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            self.assertTrue(
                db.set_request_current_evidence(REQUEST_ID, stored.id),
            )
            beets = FakeBeetsDB()
            beets.set_album_ids_for_release(SURVIVOR, [19345])
            # tmp_dir itself is real but EMPTY — no file written into it,
            # unlike the sibling "uncomputable" test above.
            beets.set_item_paths(
                SURVIVOR, [(19345, os.path.join(tmp_dir, "01 Track.mp3"))],
            )
            service, _canonical = self._service(
                db, beets, answer=CanonicalReleaseRedirected(SURVIVOR),
            )

            result = service.rekey_request(REQUEST_ID)

            self.assertEqual(
                result.outcome, RESULT_EVIDENCE_FINGERPRINT_MISMATCH,
            )
            assert result.error_message is not None
            self.assertIn("no audio files", result.error_message)
            row = db.request(REQUEST_ID)
            self.assertEqual(row["mb_release_id"], MERGED)
            self.assertEqual(db.update_request_release_for_merge_calls, [])

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
