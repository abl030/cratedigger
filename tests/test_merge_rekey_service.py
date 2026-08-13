"""Outcome-branch coverage for the operator merge-rekey action (#1089).

``MergeRekeyService.rekey_request`` is the ONE canonical execution path
behind both ``POST /api/pipeline/<id>/merge-rekey`` and
``pipeline-cli merge-rekey``. Every branch below is named by its
``RESULT_*`` outcome; ``tests/test_pipeline_db.py::TestMergeRekeyUnderOperatorClaim``
covers the real-PostgreSQL write this service's happy path and
``rekey_refused`` branch delegate to.
"""

from __future__ import annotations

import unittest

from lib.beets_db import CurrentBeetsAmbiguous, CurrentBeetsMissing
from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload
from lib.merge_rekey_service import (
    RESULT_LIBRARY_NOT_AT_SURVIVOR,
    RESULT_MIRROR_UNAVAILABLE,
    RESULT_NOT_FOUND,
    RESULT_NOT_MERGED,
    RESULT_REKEY_REFUSED,
    RESULT_REKEYED,
    RESULT_WRONG_STATE,
    MergeRekeyService,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import handoff_automation_owner, make_request_row

MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"
REQUEST_ID = 8792


class RecordingCanonical:
    """A recording merge-survivor lookup. Never a network call in tests."""

    def __init__(self, survivor: str | None) -> None:
        self.survivor = survivor
        self.calls: list[str] = []

    def __call__(self, release_id: str) -> str | None:
        self.calls.append(release_id)
        return self.survivor


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
        survivor: str | None,
        mirror_configured: bool = True,
    ) -> tuple[MergeRekeyService, RecordingCanonical]:
        canonical = RecordingCanonical(survivor)
        return (
            MergeRekeyService(
                db,
                beets,
                canonical_release_fn=canonical,
                is_mirror_configured_fn=lambda: mirror_configured,
            ),
            canonical,
        )

    def test_a_missing_request_is_not_found(self) -> None:
        db = FakePipelineDB()
        beets = FakeBeetsDB()
        service, canonical = self._service(db, beets, survivor=SURVIVOR)

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
                    db, beets, survivor=SURVIVOR,
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
        service, canonical = self._service(db, beets, survivor=SURVIVOR)

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(canonical.calls, [])

    def test_a_discogs_sourced_request_is_wrong_state(self) -> None:
        """Not MB-sourced — the resolver has no redirect concept for it."""
        db = self._db(mb_release_id="1870", discogs_release_id="1870")
        beets = FakeBeetsDB()
        service, canonical = self._service(db, beets, survivor=SURVIVOR)

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(canonical.calls, [])

    def test_a_request_with_no_release_identity_is_wrong_state(self) -> None:
        db = self._db(mb_release_id=None, discogs_release_id=None)
        beets = FakeBeetsDB()
        service, canonical = self._service(db, beets, survivor=SURVIVOR)

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(canonical.calls, [])

    def test_an_unconfigured_mirror_is_mirror_unavailable(self) -> None:
        """Checked BEFORE asking — the resolver is never even called."""
        db = self._db()
        beets = FakeBeetsDB()
        service, canonical = self._service(
            db, beets, survivor=SURVIVOR, mirror_configured=False,
        )

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_MIRROR_UNAVAILABLE)
        self.assertEqual(result.old_release_id, MERGED)
        self.assertEqual(canonical.calls, [])

    def test_an_unmerged_request_is_not_merged(self) -> None:
        """The #8792 refusal: two current albums, MusicBrainz names no redirect.

        ``canonical_release_fn`` returns ``None`` here — its real contract
        (``lib/mb_canonical.py``) never hands back the stored id itself, even
        when MusicBrainz still considers it current; every "no different
        canonical" world collapses to ``None``.
        """
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [6612, 18672])
        service, canonical = self._service(db, beets, survivor=None)

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
        """Defensive re-check (M3): never trust a same-id answer, even one
        the real resolver's own contract forbids returning."""
        db = self._db()
        beets = FakeBeetsDB()
        service, canonical = self._service(db, beets, survivor=MERGED)

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_NOT_MERGED)
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_a_non_musicbrainz_survivor_is_not_merged(self) -> None:
        """Defensive re-check: no adapter between MusicBrainz and Discogs."""
        db = self._db()
        beets = FakeBeetsDB()
        service, canonical = self._service(db, beets, survivor="1870")

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_NOT_MERGED)
        self.assertEqual(canonical.calls, [MERGED])
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_beets_missing_the_survivor_is_library_not_at_survivor(self) -> None:
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [])
        service, canonical = self._service(db, beets, survivor=SURVIVOR)

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
        service, _canonical = self._service(db, beets, survivor=SURVIVOR)

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_LIBRARY_NOT_AT_SURVIVOR)
        self.assertEqual(set(result.beets_album_ids), {19345, 19999})
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_an_in_flight_import_job_makes_the_write_refuse(self) -> None:
        """The DB claim arm's own ``NOT EXISTS`` term, reached through the service."""
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
        service, _canonical = self._service(db, beets, survivor=SURVIVOR)

        result = service.rekey_request(REQUEST_ID)

        self.assertEqual(result.outcome, RESULT_REKEY_REFUSED)
        self.assertEqual(result.new_release_id, SURVIVOR)
        row = db.request(REQUEST_ID)
        self.assertEqual(row["mb_release_id"], MERGED)

    def test_a_clean_world_rekeys(self) -> None:
        db = self._db()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(SURVIVOR, [19345])
        service, canonical = self._service(db, beets, survivor=SURVIVOR)

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

    def test_the_default_seams_read_the_process_wide_configuration(
        self,
    ) -> None:
        """No injected kwargs → both seams read the shared process state.

        An unwired process (``configure_canonical_base(None)``, the startup
        default before ``configure_canonical_release_lookup`` runs) must
        report ``mirror_unavailable``, not silently degrade to "no redirect"
        — that distinction is exactly why ``is_mirror_configured_fn`` exists
        as its own seam (see :data:`lib.merge_rekey_service.IsMirrorConfiguredFn`).
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
