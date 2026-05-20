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
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import make_request_row


# NOTE: must be parseable by ``uuid.UUID`` — the service rejects
# malformed MBIDs at the boundary with RESULT_TARGET_INVALID. Keep the
# repeated-digit pattern for at-a-glance reading of test scenarios.
OLD_MBID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
NEW_MBID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
RG_ID = "11111111-1111-1111-1111-111111111111"
OTHER_RG_ID = "22222222-2222-2222-2222-222222222222"


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
            slskd=FakeSlskdAPI(),
            beets_db_factory=beets_db_factory,
            mb_lookup=mb_lookup,
            search_plan_service=search_plan_service,
        )

    def _assert_slskd_untouched(self, slskd: object) -> None:
        """Assert FakeSlskdAPI recorded zero calls across every surface.

        Replaces the legacy ``MagicMock.mock_calls == []`` snapshot pattern.
        With a typed fake, every API entry-point appends to a per-method
        call log — so "never touched" means every log is empty.
        """
        assert isinstance(slskd, FakeSlskdAPI)
        self.assertEqual(slskd.transfers.enqueue_calls, [])
        self.assertEqual(slskd.transfers.cancel_download_calls, [])
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [])
        self.assertEqual(slskd.transfers.get_download_calls, [])
        self.assertEqual(slskd.users.directory_calls, [])
        self.assertEqual(slskd.users.status_calls, [])


class TestReplaceOutcomeMatrix(_ServiceCase):
    """Cover every RESULT_* outcome with the minimum reproducer."""

    def test_not_found(self):
        db = FakePipelineDB()
        svc = self._make_service(db)
        result = svc.replace_request_mbid(99, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_NOT_FOUND)
        self.assertIsNone(result.new_request_id)

    def test_target_invalid_malformed_uuid(self):
        """Defense-in-depth: the service rejects a non-UUID
        ``target_mb_release_id`` at the boundary with RESULT_TARGET_INVALID,
        even though the route regex + CLI argparse normally catch it
        upstream. Without this guard a malformed MBID would slip past
        Phase 0's same-as-current check (mismatched string compare) and
        explode inside the MB mirror lookup with an unhelpful trace.
        """
        for bad in (
            "not-a-uuid",
            "12345",
            "",
            "00000000-0000-0000-0000-00000000000",  # 1 short hex digit
            "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",
        ):
            with self.subTest(target=bad):
                db = FakePipelineDB()
                self._seed_old(db)
                svc = self._make_service(db)
                result = svc.replace_request_mbid(
                    42, target_mb_release_id=bad,
                )
                self.assertEqual(
                    result.outcome, RESULT_TARGET_INVALID,
                    f"malformed UUID {bad!r} did not yield "
                    f"target_invalid (got {result.outcome})",
                )
                self.assertIsNotNone(result.error_message)
                # The MB lookup must NOT have been reached — pre-Phase 0
                # rejection. We can confirm by checking the request was
                # never advanced past the validation step (no DB
                # mutation).
                src = db.get_request(42)
                assert src is not None
                self.assertEqual(src["status"], "wanted")

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

    def test_transient_timeout_on_source_lookup(self):
        """The lazy-backfill source lookup observes the same transient
        classification as the target lookup. A TimeoutError must NOT be
        collapsed into RESULT_TARGET_INVALID — the source MBID is
        already known-good (it's in the DB); we just couldn't reach the
        mirror."""
        db = FakePipelineDB()
        self._seed_old(db, mb_release_group_id=None)

        def fake_lookup(mbid, *, fresh=False):
            if mbid == OLD_MBID:
                raise TimeoutError("mirror timed out")
            return _fake_target_payload()

        svc = self._make_service(db, mb_lookup=fake_lookup)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_TRANSIENT)

    def test_transient_json_decode_error_on_target_lookup(self):
        """A malformed JSON payload from the MB mirror is treated as a
        transient — the mirror returned bytes but couldn't parse them.
        Retrying gives the operator a chance to land on a healthy
        replica."""
        import json as _json
        db = FakePipelineDB()
        self._seed_old(db)

        def fake_lookup(mbid, *, fresh=False):
            raise _json.JSONDecodeError("bad", "doc", 0)

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
            id=43, mb_release_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
            mb_release_group_id=RG_ID,
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

    def test_supersede_race_maps_to_wrong_state_with_descendant(self):
        """SupersedeRaceError (double-click landed first) maps to
        RESULT_WRONG_STATE with descendant_request_id populated, NOT
        RESULT_TRANSIENT. Retrying a race that has already succeeded
        is unhelpful — the UI should deep-link the operator to the
        new request the first click produced."""
        db = FakePipelineDB()
        self._seed_old(db)
        # Seed the descendant the racing Replace would have created
        # so get_request_by_replaces_request_id can find it.
        db.seed_request(make_request_row(
            id=43, mb_release_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
            mb_release_group_id=RG_ID, status="wanted",
            replaces_request_id=42,
        ))
        with patch.object(
            db, "supersede_request_mbid",
            side_effect=SupersedeRaceError("row already replaced"),
        ):
            svc = self._make_service(db)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(result.descendant_request_id, 43)

    def test_supersede_race_without_descendant(self):
        """If the descendant lookup also raced (unlikely but possible),
        we still report wrong_state with descendant_request_id=None
        rather than transient."""
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
        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertIsNone(result.descendant_request_id)

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

    def _patch_externals(self):
        """Patch the five external edges and register cleanup via
        ``self.addCleanup``. Returns the patched mocks as a tuple so
        tests can assert on them. Scoped per-test — unlike
        ``patch.stopall`` which would stop EVERY active patch in the
        process (including any unrelated patch the test runner is
        holding) and is a known footgun on shared mocks."""
        patches = [
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
        mocks = []
        for p in patches:
            mocks.append(p.start())
            self.addCleanup(p.stop)
        return mocks

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
        self._patch_externals()
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
        self._assert_slskd_untouched(slskd)

    def test_happy_path_imported_calls_beets_with_clear_false(self):
        self._patch_externals()
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

    def test_happy_path_downloading_skips_staging_logs_warning(self):
        self._patch_externals()
        db, _, svc = self._replace(old_status="downloading")
        slskd = svc.slskd
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_MBID,
        )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        # Warning about orphaned transfer.
        self.assertTrue(any("downloading" in w for w in result.warnings))
        # slskd was never called.
        self._assert_slskd_untouched(slskd)

    def test_happy_path_manual(self):
        self._patch_externals()
        db, _, svc = self._replace(old_status="manual")
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_MBID,
        )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        old = db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "replaced")

    def test_pet_grief_merged_upstream_canonical_redirect(self):
        """AE4 representative: source MBID resolves to canonical via 301.
        The MB lookup follows the redirect; canonical_mbid != requested
        target but the RG matches the source RG, so Replace succeeds."""
        self._patch_externals()
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

    def test_pet_grief_with_null_stored_rg(self):
        """Source row's mb_release_group_id is NULL; lazy-backfill via
        fresh MB lookup of OLD_MBID returns canonical RG."""
        self._patch_externals()
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

    def test_selector_failures_collected_as_warnings(self):
        """``remove_and_reset_release`` returns selector_failures (e.g.
        beets ``beet remove`` reported a timeout or nonzero rc). The
        outcome is still RESULT_REPLACED — Phase 4 errors are
        non-fatal — but each failure surfaces as a warning string."""
        from lib.beets_album_op import BeetsOpFailure
        result_with_failures = ReleaseCleanupResult(
            beets_removed=True,
            absent_after=True,
            selector_failures=(
                BeetsOpFailure(
                    reason="timeout", detail="60s",
                    selector="id:42",
                ),
                BeetsOpFailure(
                    reason="nonzero_rc", detail="rc=1",
                    selector="mb_albumid:abc",
                ),
            ),
        )
        with patch(
            "lib.mbid_replace_service.remove_and_reset_release",
            MagicMock(return_value=result_with_failures),
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
        # Both failures surfaced; assertion is order-independent.
        self.assertTrue(
            any("id:42" in w for w in result.warnings),
            f"selector id:42 missing from warnings: {result.warnings}",
        )
        self.assertTrue(
            any("mb_albumid:abc" in w for w in result.warnings),
            f"selector mb_albumid:abc missing from warnings: "
            f"{result.warnings}",
        )

    def test_staging_rmtree_permission_error_warns(self):
        """``shutil.rmtree`` failure on the staging dir (e.g. permission
        denied) becomes a warning; outcome stays RESULT_REPLACED."""
        import tempfile
        import os as _os
        import shutil as _shutil
        tmpdir = tempfile.mkdtemp(prefix="cratedigger-test-staging-")
        # Register cleanup before the patch context so it runs after
        # the patch is rolled back; otherwise the patched rmtree would
        # be called here and raise PermissionError again.
        self.addCleanup(_shutil.rmtree, tmpdir, True)
        cfg = CratediggerConfig()
        # Re-bind beets_staging_dir to the temp root so stage_to_ai_path
        # produces a path under it that we can pre-create.
        object.__setattr__(cfg, "beets_staging_dir", tmpdir)
        # Pre-create the staging path the rmtree branch will target.
        from lib.processing_paths import stage_to_ai_path
        target = stage_to_ai_path(
            artist="Pet Grief", title="Old Pressing",
            staging_dir=tmpdir, request_id=42, auto_import=True,
        )
        _os.makedirs(target, exist_ok=True)

        with patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ), patch(
            "lib.mbid_replace_service.trigger_meelo_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan"
        ), patch(
            "lib.mbid_replace_service.shutil.rmtree",
            side_effect=PermissionError("simulated denied"),
        ):
            db = FakePipelineDB()
            self._seed_old(db, status="wanted")
            svc = self._make_service(db, cfg=cfg)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        self.assertTrue(
            any("staging rmtree failed" in w and "PermissionError" in w
                for w in result.warnings),
            f"PermissionError warning missing: {result.warnings}",
        )


class TestReplaceCallOrder(_ServiceCase):
    """Ordering invariants — call-order matters because:

    - supersede MUST land before any fs helper (otherwise we'd be
      cleaning up filesystem state for a still-live request).
    - Rescan helpers MUST run AFTER fs cleanup (so Meelo / Plex see
      the deleted folder, not the pre-cleanup view).
    - slskd.transfers.cancel_download MUST never be called (R23 —
      Replace intentionally never touches in-flight transfers).
    """

    def test_rescans_run_after_advisory_lock_release(self):
        """Phase 5 (search plan + Meelo/Plex/Jellyfin rescans) must run
        AFTER the IMPORT advisory lock is released. Holding the lock
        across ~10s rescan timeouts is wasted contention — the new
        request's ``active_plan_id`` is NULL until SearchPlanService
        runs, so the importer worker wouldn't grab the lock anyway.

        The fake's ``advisory_lock`` records every entry / exit on the
        manager. The assertion is order-only: the lock context-manager
        must have exited before the first rescan helper fires.
        """
        manager = MagicMock()
        lock_released = {"flag": False}

        # Wrap the fake's advisory_lock so we observe enter/exit.
        db = FakePipelineDB()
        self._seed_old(db, status="wanted")
        real_lock = db.advisory_lock

        from contextlib import contextmanager

        @contextmanager
        def recording_lock(namespace, key):
            manager.advisory_lock_enter(namespace, key)
            with real_lock(namespace, key) as acquired:
                yield acquired
            manager.advisory_lock_exit(namespace, key)
            lock_released["flag"] = True

        # search_plan + each rescan records on the manager, and asserts
        # the lock has already been released when they fire.
        plan_svc = MagicMock()

        def assert_released_search_plan(*args, **kwargs):
            manager.search_plan(*args, **kwargs)
            assert lock_released["flag"], (
                "search-plan generation ran while the IMPORT advisory "
                "lock was still held"
            )

        plan_svc.generate_for_request.side_effect = (
            assert_released_search_plan
        )

        def assert_released_meelo(*args, **kwargs):
            manager.trigger_meelo_scan(*args, **kwargs)
            assert lock_released["flag"], (
                "Meelo rescan ran while the IMPORT advisory lock was "
                "still held"
            )

        def assert_released_plex(*args, **kwargs):
            manager.trigger_plex_scan(*args, **kwargs)
            assert lock_released["flag"], (
                "Plex rescan ran while the IMPORT advisory lock was "
                "still held"
            )

        def assert_released_jellyfin(*args, **kwargs):
            manager.trigger_jellyfin_scan(*args, **kwargs)
            assert lock_released["flag"], (
                "Jellyfin rescan ran while the IMPORT advisory lock "
                "was still held"
            )

        with patch.object(
            db, "advisory_lock", side_effect=recording_lock,
        ), patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ), patch(
            "lib.mbid_replace_service.trigger_meelo_scan",
            side_effect=assert_released_meelo,
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan",
            side_effect=assert_released_plex,
        ), patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan",
            side_effect=assert_released_jellyfin,
        ):
            svc = self._make_service(db, search_plan_service=plan_svc)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )

        self.assertEqual(result.outcome, RESULT_REPLACED)
        # Each Phase 5 helper recorded its call AFTER the lock exit was
        # recorded. Independently of order, the in-line assertions inside
        # the side_effects would have raised if a helper fired with the
        # lock still held.
        call_names = [c[0] for c in manager.mock_calls]
        lock_exit_idx = call_names.index("advisory_lock_exit")
        for helper_name in (
            "search_plan",
            "trigger_meelo_scan",
            "trigger_plex_scan",
            "trigger_jellyfin_scan",
        ):
            self.assertIn(helper_name, call_names)
            self.assertGreater(
                call_names.index(helper_name), lock_exit_idx,
                f"{helper_name} ran before advisory_lock_exit "
                f"(call order: {call_names})",
            )

    def test_supersede_before_fs_helpers_and_rescans_after_cleanup(self):
        manager = MagicMock()
        # Wrap db.supersede_request_mbid so it lands in the manager.
        db = FakePipelineDB()
        self._seed_old(db, status="imported",
                       imported_path="/mnt/virtio/Music/Beets/X")
        real_supersede = db.supersede_request_mbid

        def supersede_recording(*args, **kwargs):
            manager.supersede(*args, **kwargs)
            return real_supersede(*args, **kwargs)

        with patch.object(
            db, "supersede_request_mbid",
            side_effect=supersede_recording,
        ), patch(
            "lib.mbid_replace_service.remove_and_reset_release",
            MagicMock(return_value=ReleaseCleanupResult(
                beets_removed=True, absent_after=True,
                selector_failures=(),
            )),
        ) as mock_remove, patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ) as mock_wm_delete, patch(
            "lib.mbid_replace_service.trigger_meelo_scan",
        ) as mock_meelo, patch(
            "lib.mbid_replace_service.trigger_plex_scan",
        ) as mock_plex, patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan",
        ) as mock_jelly:
            manager.attach_mock(mock_remove, "remove_and_reset_release")
            manager.attach_mock(mock_wm_delete, "delete_wrong_match_group")
            manager.attach_mock(mock_meelo, "trigger_meelo_scan")
            manager.attach_mock(mock_plex, "trigger_plex_scan")
            manager.attach_mock(mock_jelly, "trigger_jellyfin_scan")

            svc = self._make_service(db)
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )

        self.assertEqual(result.outcome, RESULT_REPLACED)
        # Extract the recorded call sequence as method names.
        call_names = [c[0] for c in manager.mock_calls]
        # Supersede comes first.
        self.assertEqual(call_names[0], "supersede")
        # Filesystem cleanup helpers must precede every rescan helper.
        first_rescan_idx = min(
            call_names.index(name) for name in (
                "trigger_meelo_scan",
                "trigger_plex_scan",
                "trigger_jellyfin_scan",
            )
        )
        for fs_helper in ("remove_and_reset_release",
                          "delete_wrong_match_group"):
            self.assertLess(
                call_names.index(fs_helper), first_rescan_idx,
                f"{fs_helper} ran after a rescan helper "
                f"(call order: {call_names})",
            )
        # slskd was never touched (R23).
        self._assert_slskd_untouched(svc.slskd)


if __name__ == "__main__":
    unittest.main()
