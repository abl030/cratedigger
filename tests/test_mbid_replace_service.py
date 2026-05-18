"""Tests for ``lib.mbid_replace_service.MbidReplaceService`` (U4).

Covers every outcome string, status-dispatch coverage per pre-supersede
status, ordering invariants, and the warning surface for non-fatal
filesystem cleanup failures.
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import URLError

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)

from lib.config import CratediggerConfig
from lib.mbid_replace_service import (
    MbidReplaceService,
    ReplaceResult,
    RESULT_NOT_FOUND,
    RESULT_REPLACED,
    RESULT_TARGET_COLLISION_REQUEST,
    RESULT_TARGET_INVALID,
    RESULT_TARGET_RELEASE_GROUP_MISMATCH,
    RESULT_TARGET_SAME_AS_CURRENT,
    RESULT_TRANSIENT,
    RESULT_WRONG_STATE,
)
from lib.pipeline_db import MbidCollisionError, SupersedeRaceError
from lib.release_cleanup import ReleaseCleanupResult
from lib.wrong_match_delete_service import WrongMatchDeleteSummary
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


OLD_MBID = "old-mbid-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
NEW_MBID = "new-mbid-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
RG_ID = "rg-uuid-1111-1111-1111-111111111111"
OTHER_RG_ID = "rg-uuid-2222-2222-2222-222222222222"


def _empty_wrong_match_summary(request_id: int) -> WrongMatchDeleteSummary:
    return WrongMatchDeleteSummary(
        request_id=request_id,
        outcome="group_empty",
        success=True,
        processed=0,
        deleted=0,
        deleted_paths=0,
        cleared=0,
        skipped=0,
        errors=0,
        remaining=0,
        group_empty=True,
        results=(),
    )


def _fake_target_payload(
    *,
    mbid: str = NEW_MBID,
    rg_id: str = RG_ID,
    artist_name: str = "Pet Grief",
    title: str = "New Pressing",
    artist_id: str | None = "art-1",
    year: int | None = 2025,
    country: str | None = "JP",
    tracks: list[dict] | None = None,
) -> dict:
    return {
        "id": mbid,
        "title": title,
        "artist_name": artist_name,
        "artist_id": artist_id,
        "release_group_id": rg_id,
        "year": year,
        "country": country,
        "tracks": tracks if tracks is not None else [
            {"disc_number": 1, "track_number": 1, "title": "T1"},
            {"disc_number": 1, "track_number": 2, "title": "T2"},
        ],
    }


class _ServiceCase(unittest.TestCase):
    """Shared scaffolding for MbidReplaceService tests."""

    def _seed_old(
        self,
        db: FakePipelineDB,
        *,
        request_id: int = 42,
        status: str = "wanted",
        mb_release_group_id: str | None = RG_ID,
        mb_release_id: str = OLD_MBID,
        imported_path: str | None = None,
        source: str = "request",
    ) -> int:
        row = make_request_row(
            id=request_id,
            mb_release_id=mb_release_id,
            mb_release_group_id=mb_release_group_id,
            mb_artist_id="art-1",
            artist_name="Pet Grief",
            album_title="Old Pressing",
            year=2024,
            country="US",
            status=status,
            imported_path=imported_path,
            source=source,
            verified_lossless=True,
            current_spectral_grade="A",
            current_spectral_bitrate=900,
        )
        db.seed_request(row)
        return request_id

    def _make_service(
        self,
        db: FakePipelineDB,
        *,
        mb_lookup=None,
        search_plan_service=None,
        beets_db_factory=None,
        cfg: CratediggerConfig | None = None,
    ) -> MbidReplaceService:
        if mb_lookup is None:
            mb_lookup = lambda mbid, *, fresh=False: _fake_target_payload()
        if search_plan_service is None:
            search_plan_service = MagicMock()
        if beets_db_factory is None:
            beets_db_factory = lambda: MagicMock()
        return MbidReplaceService(
            db=db,
            config=cfg or CratediggerConfig(),
            slskd=MagicMock(),
            beets_db_factory=beets_db_factory,
            mb_lookup=mb_lookup,
            search_plan_service=search_plan_service,
        )


class TestReplaceOutcomeMatrix(_ServiceCase):
    """Cover every RESULT_* outcome with the minimum reproducer."""

    def test_not_found(self):
        db = FakePipelineDB()
        svc = self._make_service(db)
        result = svc.replace_request_mbid(99, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_NOT_FOUND)
        self.assertIsNone(result.new_request_id)

    def test_same_as_current(self):
        db = FakePipelineDB()
        self._seed_old(db)
        svc = self._make_service(db)
        result = svc.replace_request_mbid(42, target_mb_release_id=OLD_MBID)
        self.assertEqual(result.outcome, RESULT_TARGET_SAME_AS_CURRENT)

    def test_release_group_mismatch(self):
        db = FakePipelineDB()
        self._seed_old(db)
        svc = self._make_service(
            db,
            mb_lookup=lambda mbid, *, fresh=False: _fake_target_payload(
                rg_id=OTHER_RG_ID
            ),
        )
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(
            result.outcome, RESULT_TARGET_RELEASE_GROUP_MISMATCH
        )

    def test_release_group_mismatch_after_lazy_backfill(self):
        db = FakePipelineDB()
        self._seed_old(db, mb_release_group_id=None)
        calls: list[tuple[str, dict]] = []

        def fake_lookup(mbid, *, fresh=False):
            calls.append((mbid, {"fresh": fresh}))
            if mbid == OLD_MBID:
                return _fake_target_payload(mbid=OLD_MBID, rg_id=RG_ID)
            return _fake_target_payload(rg_id=OTHER_RG_ID)

        svc = self._make_service(db, mb_lookup=fake_lookup)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(
            result.outcome, RESULT_TARGET_RELEASE_GROUP_MISMATCH
        )
        # Lazy-backfill issued a fresh lookup of the source.
        self.assertTrue(any(mbid == OLD_MBID for mbid, _ in calls))

    def test_target_invalid_missing_rg(self):
        db = FakePipelineDB()
        self._seed_old(db)
        svc = self._make_service(
            db,
            mb_lookup=lambda mbid, *, fresh=False: {
                "id": NEW_MBID, "title": "X", "artist_name": "Y",
                "release_group_id": None, "tracks": [],
            },
        )
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)

    def test_target_invalid_source_resolve_failure(self):
        db = FakePipelineDB()
        self._seed_old(db, mb_release_group_id=None)

        def fake_lookup(mbid, *, fresh=False):
            if mbid == OLD_MBID:
                raise RuntimeError("MB mirror 404")
            return _fake_target_payload()

        svc = self._make_service(db, mb_lookup=fake_lookup)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)

    def test_transient_urlerror(self):
        db = FakePipelineDB()
        self._seed_old(db)

        def fake_lookup(mbid, *, fresh=False):
            raise URLError("connection refused")

        svc = self._make_service(db, mb_lookup=fake_lookup)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_TRANSIENT)

    def test_collision_precheck_active_row(self):
        db = FakePipelineDB()
        self._seed_old(db)
        db.seed_request(make_request_row(
            id=43, mb_release_id=NEW_MBID, mb_release_group_id=RG_ID,
            status="downloading",
        ))
        svc = self._make_service(db)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(
            result.outcome, RESULT_TARGET_COLLISION_REQUEST
        )
        self.assertEqual(result.current_status, "downloading")

    def test_collision_precheck_replaced_row(self):
        db = FakePipelineDB()
        self._seed_old(db)
        db.seed_request(make_request_row(
            id=43, mb_release_id=NEW_MBID, mb_release_group_id=RG_ID,
            status="replaced",
        ))
        svc = self._make_service(db)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(
            result.outcome, RESULT_TARGET_COLLISION_REQUEST
        )
        self.assertEqual(result.current_status, "replaced")

    def test_collision_defensive_unique_violation(self):
        db = FakePipelineDB()
        self._seed_old(db)
        # Intercept supersede to raise MbidCollisionError after Phase 0
        # checks pass.
        with patch.object(
            db, "supersede_request_mbid",
            side_effect=MbidCollisionError("simulated UNIQUE violation"),
        ):
            svc = self._make_service(db)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID
            )
        self.assertEqual(result.outcome, RESULT_TARGET_COLLISION_REQUEST)

    def test_wrong_state_on_lock_contention(self):
        db = FakePipelineDB()
        self._seed_old(db)
        db.set_advisory_lock_result(False)
        svc = self._make_service(db)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertIsNone(result.descendant_request_id)

    def test_wrong_state_source_already_replaced(self):
        db = FakePipelineDB()
        # Source row is already replaced + has a descendant.
        db.seed_request(make_request_row(
            id=42, mb_release_id=OLD_MBID, status="replaced",
            mb_release_group_id=RG_ID,
        ))
        db.seed_request(make_request_row(
            id=43, mb_release_id="some-newer", mb_release_group_id=RG_ID,
            status="wanted", replaces_request_id=42,
        ))
        svc = self._make_service(db)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(result.descendant_request_id, 43)

    def test_wrong_state_source_already_replaced_no_descendant(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id=OLD_MBID, status="replaced",
            mb_release_group_id=RG_ID,
        ))
        svc = self._make_service(db)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertIsNone(result.descendant_request_id)

    def test_transient_supersede_race(self):
        db = FakePipelineDB()
        self._seed_old(db)
        with patch.object(
            db, "supersede_request_mbid",
            side_effect=SupersedeRaceError("row already replaced"),
        ):
            svc = self._make_service(db)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
        self.assertEqual(result.outcome, RESULT_TRANSIENT)

    def test_canonical_equals_source(self):
        """MB lookup follows a 301 redirect and the canonical MBID is
        the source's own current MBID. Treated as a self-collision —
        the operator effectively asked to Replace into a value that
        normalises to the same MBID."""
        db = FakePipelineDB()
        self._seed_old(db)

        def fake_lookup(mbid, *, fresh=False):
            # Target redirects to source's MBID.
            return _fake_target_payload(mbid=OLD_MBID, rg_id=RG_ID)

        svc = self._make_service(db, mb_lookup=fake_lookup)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(
            result.outcome, RESULT_TARGET_COLLISION_REQUEST
        )
        self.assertEqual(result.current_status, "wanted")

    def test_canonical_redirects_to_other_request(self):
        """MB lookup follows a 301 to a canonical MBID that's already
        held by a different active request. The redirect-recheck branch
        in Phase 0 catches it and reports the holder's status."""
        CANONICAL = "canonical-cccccccc-cccc-cccc-cccc-cccccccccccc"
        db = FakePipelineDB()
        self._seed_old(db)
        # Third request holds the canonical the redirect resolves to.
        db.seed_request(make_request_row(
            id=44, mb_release_id=CANONICAL, mb_release_group_id=RG_ID,
            status="imported",
        ))

        def fake_lookup(mbid, *, fresh=False):
            # NEW_MBID redirects to CANONICAL.
            return _fake_target_payload(mbid=CANONICAL, rg_id=RG_ID)

        svc = self._make_service(db, mb_lookup=fake_lookup)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(
            result.outcome, RESULT_TARGET_COLLISION_REQUEST
        )
        self.assertEqual(result.current_status, "imported")

    def test_state_capture_under_lock_sees_fresh_imported_status(self):
        """Race-window guard (P0 fix): the importer finishes between
        Phase 0 (source loaded as ``downloading``) and Phase 1 (lock
        acquired). Without the fix the service would see the stale
        ``downloading`` status and skip beets cleanup. With the fix
        the state-capture inside the lock re-reads the row and Phase 4
        routes through ``remove_and_reset_release``."""

        db = FakePipelineDB()
        self._seed_old(db, status="downloading")

        # The importer mutation: when the lock is acquired, flip the
        # row to ``imported`` with an ``imported_path``.
        def lock_callable(namespace, key):
            row = db._requests[42]
            row["status"] = "imported"
            row["imported_path"] = "/mnt/virtio/Music/Beets/Pet Grief/Old Pressing"
            return True

        db.set_advisory_lock_result(lock_callable)

        # Patch every external. ``remove_and_reset_release`` MUST be
        # called — that's the assertion.
        with patch(
            "lib.mbid_replace_service.remove_and_reset_release",
            MagicMock(return_value=ReleaseCleanupResult(
                beets_removed=True,
                absent_after=True,
                selector_failures=(),
            )),
        ) as mock_remove, patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ), patch(
            "lib.mbid_replace_service.trigger_meelo_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan"
        ) as mock_plex, patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan"
        ):
            svc = self._make_service(db)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        # Fresh status was seen → beets removal ran.
        mock_remove.assert_called_once()
        _, kwargs = mock_remove.call_args
        self.assertEqual(kwargs.get("clear_pipeline_state"), False)
        self.assertEqual(kwargs.get("release_id"), OLD_MBID)
        # Fresh imported_path was seen → Plex partial scan routed to it.
        mock_plex.assert_called_once()
        _, plex_kwargs = mock_plex.call_args
        self.assertEqual(
            plex_kwargs.get("imported_path"),
            "/mnt/virtio/Music/Beets/Pet Grief/Old Pressing",
        )


class TestReplaceHappyPath(_ServiceCase):
    """Cover RESULT_REPLACED + ordering invariants + the Pet Grief
    merged-upstream case (target redirects to canonical)."""

    def _patched_externals(self):
        return [
            patch(
                "lib.mbid_replace_service.remove_and_reset_release",
                MagicMock(return_value=ReleaseCleanupResult(
                    beets_removed=True,
                    absent_after=True,
                    selector_failures=(),
                )),
            ),
            patch(
                "lib.mbid_replace_service.delete_wrong_match_group",
                MagicMock(side_effect=_empty_wrong_match_summary),
            ),
            patch("lib.mbid_replace_service.trigger_meelo_scan", MagicMock()),
            patch("lib.mbid_replace_service.trigger_plex_scan", MagicMock()),
            patch(
                "lib.mbid_replace_service.trigger_jellyfin_scan",
                MagicMock(),
            ),
        ]

    def _replace(self, *, old_status="wanted", **service_kwargs):
        db = FakePipelineDB()
        self._seed_old(db, status=old_status,
                       imported_path=(
                           "/mnt/virtio/Music/Beets/Pet Grief/Old Pressing"
                           if old_status == "imported" else None
                       ))
        plan_svc = MagicMock()
        svc = self._make_service(
            db, search_plan_service=plan_svc, **service_kwargs,
        )
        return db, plan_svc, svc

    def test_happy_path_wanted(self):
        for p in self._patched_externals():
            p.start()
        try:
            db, plan_svc, svc = self._replace(old_status="wanted")
            slskd = svc.slskd
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
            assert result.new_request_id is not None
            # Old row flipped to replaced.
            old = db.get_request(42)
            assert old is not None
            self.assertEqual(old["status"], "replaced")
            # Characteristic fields preserved.
            self.assertTrue(old["verified_lossless"])
            self.assertEqual(old["current_spectral_grade"], "A")
            # New row born wanted with lineage.
            new = db.get_request(result.new_request_id)
            assert new is not None
            self.assertEqual(new["status"], "wanted")
            self.assertEqual(new["replaces_request_id"], 42)
            # Plan service called on the new id.
            plan_svc.generate_for_request.assert_called_once_with(
                result.new_request_id, regenerate=False,
            )
            # slskd never touched.
            self.assertEqual(slskd.mock_calls, [])
        finally:
            patch.stopall()

    def test_happy_path_imported_calls_beets_with_clear_false(self):
        for p in self._patched_externals():
            p.start()
        try:
            db, _, svc = self._replace(old_status="imported")
            from lib import mbid_replace_service as svcmod
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
            # Beets removal was called WITH clear_pipeline_state=False —
            # regression guard so a future maintainer can't accidentally
            # clear the OLD row's characteristic fields.
            mock_remove = svcmod.remove_and_reset_release
            assert isinstance(mock_remove, MagicMock)
            mock_remove.assert_called_once()
            _, kwargs = mock_remove.call_args
            self.assertEqual(kwargs.get("clear_pipeline_state"), False)
            self.assertEqual(kwargs.get("request_id"), 42)
            self.assertEqual(kwargs.get("release_id"), OLD_MBID)
        finally:
            patch.stopall()

    def test_happy_path_downloading_skips_staging_logs_warning(self):
        for p in self._patched_externals():
            p.start()
        try:
            db, _, svc = self._replace(old_status="downloading")
            slskd = svc.slskd
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
            # Warning about orphaned transfer.
            self.assertTrue(any("downloading" in w for w in result.warnings))
            # slskd was never called.
            self.assertEqual(slskd.mock_calls, [])
        finally:
            patch.stopall()

    def test_happy_path_manual(self):
        for p in self._patched_externals():
            p.start()
        try:
            db, _, svc = self._replace(old_status="manual")
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
            old = db.get_request(42)
            assert old is not None
            self.assertEqual(old["status"], "replaced")
        finally:
            patch.stopall()

    def test_pet_grief_merged_upstream_canonical_redirect(self):
        """AE4 representative: source MBID resolves to canonical via 301.
        The MB lookup follows the redirect; canonical_mbid != requested
        target but the RG matches the source RG, so Replace succeeds."""
        for p in self._patched_externals():
            p.start()
        try:
            CANONICAL = "18056805-33f5-3e99-aa4b-5f5919c4f8af"
            db = FakePipelineDB()
            self._seed_old(db, mb_release_id=OLD_MBID)
            plan_svc = MagicMock()
            svc = self._make_service(
                db,
                search_plan_service=plan_svc,
                mb_lookup=lambda mbid, *, fresh=False: (
                    _fake_target_payload(mbid=CANONICAL, rg_id=RG_ID)
                ),
            )
            result = svc.replace_request_mbid(
                42, target_mb_release_id="72988560-e8fc-4429-9c69-7045bb63e248",
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
            assert result.new_request_id is not None
            new = db.get_request(result.new_request_id)
            assert new is not None
            self.assertEqual(new["mb_release_id"], CANONICAL)
        finally:
            patch.stopall()

    def test_pet_grief_with_null_stored_rg(self):
        """Source row's mb_release_group_id is NULL; lazy-backfill via
        fresh MB lookup of OLD_MBID returns canonical RG."""
        for p in self._patched_externals():
            p.start()
        try:
            db = FakePipelineDB()
            self._seed_old(db, mb_release_group_id=None)

            def fake_lookup(mbid, *, fresh=False):
                # Both old and new resolve to the same RG.
                return _fake_target_payload(
                    mbid=mbid if mbid == OLD_MBID else NEW_MBID,
                    rg_id=RG_ID,
                )

            svc = self._make_service(db, mb_lookup=fake_lookup)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
        finally:
            patch.stopall()


class TestReplaceWarnings(_ServiceCase):
    """Filesystem cleanup failures surface as warnings; outcome stays
    RESULT_REPLACED (R26)."""

    def test_beets_removal_failure_warning(self):
        with patch(
            "lib.mbid_replace_service.remove_and_reset_release",
            side_effect=RuntimeError("beet remove crashed"),
        ), patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ), patch(
            "lib.mbid_replace_service.trigger_meelo_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan"
        ):
            db = FakePipelineDB()
            self._seed_old(
                db, status="imported",
                imported_path="/mnt/virtio/Music/Beets/X",
            )
            svc = self._make_service(db)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
            self.assertTrue(any("beets removal" in w for w in result.warnings))

    def test_wrong_match_failure_warning(self):
        with patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=RuntimeError("wm crashed"),
        ), patch(
            "lib.mbid_replace_service.trigger_meelo_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan"
        ):
            db = FakePipelineDB()
            self._seed_old(db)
            svc = self._make_service(db)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
            self.assertTrue(
                any("wrong-matches cleanup raised" in w
                    for w in result.warnings)
            )

    def test_search_plan_failure_warning(self):
        with patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ), patch(
            "lib.mbid_replace_service.trigger_meelo_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan"
        ):
            db = FakePipelineDB()
            self._seed_old(db)
            plan_svc = MagicMock()
            plan_svc.generate_for_request.side_effect = RuntimeError("kaboom")
            svc = self._make_service(db, search_plan_service=plan_svc)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
            self.assertEqual(result.outcome, RESULT_REPLACED)
            self.assertTrue(
                any("search-plan generation failed" in w
                    for w in result.warnings)
            )


if __name__ == "__main__":
    unittest.main()
