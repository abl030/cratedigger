"""Tests for ``lib.mbid_replace_service.MbidReplaceService`` (U4).

Covers every outcome string, status-dispatch coverage per pre-supersede
status, ordering invariants, and the warning surface for non-fatal
filesystem cleanup failures.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from urllib.error import URLError

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from hypothesis import given, strategies as st

from lib.config import CratediggerConfig
from lib.beets_db import BeetsDB, CurrentBeetsMissing
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteFailed,
    BeetsDeleteRequest,
    run_beets_delete,
)
from lib.mbid_replace_service import (
    MbidReplaceService,
    REPLACE_REASON_CURRENT_BEETS_AMBIGUOUS,
    REPLACE_REASON_SOURCE_IDENTITY_INVALID,
    ReplaceResult,
    REPLACE_REASON_CROSS_PATHWAY_TARGET,
    REPLACE_REASON_SOURCE_NO_RELEASE_GROUP,
    REPLACE_REASON_TARGET_NO_RELEASE_GROUP,
    REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR,
    REPLACE_REASON_UNRESOLVABLE_TARGET,
    RESULT_MIRROR_UNCONFIGURED,
    RESULT_NOT_FOUND,
    RESULT_REPLACED,
    RESULT_TARGET_COLLISION_REQUEST,
    RESULT_TARGET_INVALID,
    RESULT_TARGET_RELEASE_GROUP_MISMATCH,
    RESULT_TARGET_SAME_AS_CURRENT,
    RESULT_TRANSIENT,
    RESULT_WRONG_STATE,
)
from web.discogs import DiscogsMirrorNotConfigured
from lib.pipeline_db import MbidCollisionError, SupersedeRaceError
from lib.release_identity import ReleaseIdentity
from lib.wrong_match_delete_service import WrongMatchDeleteSummary
from tests.fakes import FakeBeetsDB, FakePipelineDB, FakeSlskdAPI
from tests.beets_world import BeetsWorld, BeetsWorldRelease
from tests.helpers import make_request_row


# NOTE: must be a valid MB release id per ``detect_release_source``
# (lib/release_identity.py regex) — the service rejects malformed MBIDs
# at the boundary with RESULT_TARGET_INVALID. Keep the repeated-digit
# pattern for at-a-glance reading of test scenarios.
OLD_MBID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
NEW_MBID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
RG_ID = "11111111-1111-1111-1111-111111111111"
OTHER_RG_ID = "22222222-2222-2222-2222-222222222222"

# Discogs-pathway fixtures — numeric release ids; the master anchor lives
# in ``mb_release_group_id`` as a numeric id (KTD-1). A Discogs source row
# dual-writes the numeric id into both ``mb_release_id`` and
# ``discogs_release_id`` (KTD-4), as the add flow does.
OLD_DISCOGS_ID = "1001"
NEW_DISCOGS_ID = "1002"
DISCOGS_MASTER = "5000"
OTHER_DISCOGS_MASTER = "6000"
REPO = Path(__file__).resolve().parent.parent


def _empty_wrong_match_summary(_db, request_id: int) -> WrongMatchDeleteSummary:
    """Stand-in for ``delete_wrong_match_group(db, request_id)``.

    Matches the production signature (two positional args). Tests previously
    declared this with one arg; production caught the resulting TypeError
    and logged ``Replace: warning ... wrong-matches cleanup raised
    TypeError`` while still passing because the warning path continued.
    The migration to ``FakeSlskdAPI`` surfaced the latent mismatch.
    """
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


def _fake_discogs_payload(
    *,
    release_id: str = NEW_DISCOGS_ID,
    master: str | None = DISCOGS_MASTER,
    artist_name: str = "Pet Grief",
    title: str = "New Pressing",
    artist_id: str | None = "art-d-1",
    year: int | None = 2025,
    country: str | None = "JP",
    tracks: list[dict] | None = None,
) -> dict:
    """Mirror of ``web.discogs.get_release``'s normalized shape.

    ``master`` maps to the ``release_group_id`` key (the mirror remaps
    ``master_id`` there); ``None`` models a masterless release.
    """
    return {
        "id": str(release_id),
        "title": title,
        "artist_name": artist_name,
        "artist_id": artist_id,
        "release_group_id": master,
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
            source=source,
            verified_lossless=True,
            current_spectral_grade="A",
            current_spectral_bitrate=900,
        )
        db.seed_request(row)
        return request_id

    def _seed_discogs(
        self,
        db: FakePipelineDB,
        *,
        request_id: int = 42,
        status: str = "wanted",
        master: str | None = DISCOGS_MASTER,
        release_id: str = OLD_DISCOGS_ID,
        source: str = "request",
    ) -> int:
        """Seed a Discogs-pathway source row.

        The numeric release id is dual-written into both ``mb_release_id``
        and ``discogs_release_id`` (KTD-4); the numeric master lives in
        ``mb_release_group_id`` (KTD-1). ``master=None`` models a legacy row
        that predates master persistence (the lazy-backfill target).
        """
        row = make_request_row(
            id=request_id,
            mb_release_id=release_id,
            discogs_release_id=release_id,
            mb_release_group_id=master,
            mb_artist_id="art-d-1",
            artist_name="Pet Grief",
            album_title="Old Pressing",
            year=2024,
            country="US",
            status=status,
            source=source,
        )
        db.seed_request(row)
        return request_id

    def _make_service(
        self,
        db: FakePipelineDB,
        *,
        mb_lookup=None,
        discogs_lookup=None,
        search_plan_service=None,
        beets_db_factory=None,
        beets_delete_fn=None,
        cfg: CratediggerConfig | None = None,
    ) -> MbidReplaceService:
        if mb_lookup is None:
            mb_lookup = lambda mbid, *, fresh=False: _fake_target_payload()
        if discogs_lookup is None:
            discogs_lookup = (
                lambda rid, *, fresh=False: _fake_discogs_payload()
            )
        if search_plan_service is None:
            search_plan_service = MagicMock()
        if beets_db_factory is None:
            beets_db_factory = lambda: FakeBeetsDB()
        return MbidReplaceService(
            db=db,
            config=cfg or CratediggerConfig(),
            slskd=FakeSlskdAPI(),
            beets_db_factory=beets_db_factory,
            mb_lookup=mb_lookup,
            discogs_lookup=discogs_lookup,
            search_plan_service=search_plan_service,
            beets_delete_fn=beets_delete_fn,
        )

    def _installed_beets(
        self,
        *,
        release_id: str = OLD_MBID,
        album_id: int = 77,
        album_path: str = "/library/Current Artist/Current Album",
    ) -> FakeBeetsDB:
        beets = FakeBeetsDB(library_root="/library")
        beets.set_album_ids_for_release(release_id, [album_id])
        beets.set_item_paths(
            release_id,
            [(album_id * 10, f"{album_path}/01.flac")],
        )
        return beets

    @staticmethod
    def _completed_delete(request: BeetsDeleteRequest) -> BeetsDeleteCompleted:
        return BeetsDeleteCompleted(
            album_id=request.album_id,
            album_name="Current Album",
            artist_name="Current Artist",
            former_album_path="/library/Current Artist/Current Album",
            deleted_tracks=1,
            deleted_artifacts=1,
            preserved_paths=(),
        )

    def _patch_externals(self):
        """Patch wrong-match cleanup and the two rescan notifiers.

        Beets deletion is injected through the service constructor; missing
        current authority is the ordinary no-op default for unrelated tests.
        Register cleanup via
        ``self.addCleanup``. Returns the patched mocks as a list so tests
        can assert on them. Scoped per-test — unlike ``patch.stopall``
        which would stop EVERY active patch in the process."""
        patches = [
            patch(
                "lib.mbid_replace_service.delete_wrong_match_group",
                MagicMock(side_effect=_empty_wrong_match_summary),
            ),
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

    def test_ambiguous_current_beets_authority_is_zero_mutation(self):
        db = FakePipelineDB()
        self._seed_old(db, status="imported")
        before = db.request(42).copy()
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(OLD_MBID, [7, 8])
        delete_calls: list[BeetsDeleteRequest] = []
        svc = self._make_service(
            db,
            beets_db_factory=lambda: beets,
            beets_delete_fn=lambda request: delete_calls.append(request),
        )

        result = svc.replace_request_mbid(
            42,
            target_mb_release_id=NEW_MBID,
        )

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(result.reason, REPLACE_REASON_CURRENT_BEETS_AMBIGUOUS)
        self.assertEqual(db.request(42), before)
        self.assertEqual(delete_calls, [])

    def test_conflicting_source_identities_are_typed_zero_mutation(self):
        db = FakePipelineDB()
        self._seed_old(db, status="imported")
        db.request(42)["discogs_release_id"] = OLD_DISCOGS_ID
        before = db.request(42).copy()
        mb_lookup = MagicMock(side_effect=AssertionError("lookup reached"))
        discogs_lookup = MagicMock(
            side_effect=AssertionError("lookup reached"),
        )
        beets_factory = MagicMock(
            side_effect=AssertionError("Beets authority reached"),
        )
        svc = self._make_service(
            db,
            mb_lookup=mb_lookup,
            discogs_lookup=discogs_lookup,
            beets_db_factory=beets_factory,
        )

        result = svc.replace_request_mbid(
            42,
            target_mb_release_id=NEW_MBID,
        )

        self.assertEqual(result.outcome, RESULT_WRONG_STATE)
        self.assertEqual(result.reason, REPLACE_REASON_SOURCE_IDENTITY_INVALID)
        self.assertEqual(db.request(42), before)
        self.assertEqual(db.advisory_lock_calls, [])
        mb_lookup.assert_not_called()
        discogs_lookup.assert_not_called()
        beets_factory.assert_not_called()

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
                self.assertEqual(
                    result.reason, REPLACE_REASON_CROSS_PATHWAY_TARGET,
                )
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
        self.assertEqual(result.reason, REPLACE_REASON_TARGET_NO_RELEASE_GROUP)

    def test_target_invalid_target_empty_payload(self):
        """#501 item 2: the target lookup succeeds but returns a falsy
        payload (distinct from an unresolvable-with-no-RG payload) —
        REASON: unresolvable_target."""
        db = FakePipelineDB()
        self._seed_old(db)
        svc = self._make_service(
            db, mb_lookup=lambda mbid, *, fresh=False: {},
        )
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_UNRESOLVABLE_TARGET)

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
        self.assertEqual(result.reason, REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR)

    def test_target_invalid_source_no_release_group_after_lazy_backfill(self):
        """#501 item 2: the source lazy-backfill lookup succeeds but the
        MB mirror has no release_group_id for it — REASON:
        source_no_release_group (the MB-arm analog of a masterless
        Discogs source)."""
        db = FakePipelineDB()
        self._seed_old(db, mb_release_group_id=None)

        def fake_lookup(mbid, *, fresh=False):
            if mbid == OLD_MBID:
                return {
                    "id": OLD_MBID, "title": "Old", "artist_name": "X",
                    "release_group_id": None, "tracks": [],
                }
            return _fake_target_payload()

        svc = self._make_service(db, mb_lookup=fake_lookup)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(
            result.reason, REPLACE_REASON_SOURCE_NO_RELEASE_GROUP,
        )

    def test_mb_source_lazy_backfill_unexpected_exception_logs_warning(self):
        """#501 item 3: the MB arm gains the Discogs arm's parity — an
        unexpected (non-transient) exception during the source
        lazy-backfill lookup is TARGET_INVALID AND logs a warning, so a
        real client bug doesn't present identically to expected operator
        input error (mirrors
        test_source_lazy_backfill_generic_exception_target_invalid on the
        Discogs arm)."""
        db = FakePipelineDB()
        self._seed_old(db, mb_release_group_id=None)

        def fake_lookup(mbid, *, fresh=False):
            if mbid == OLD_MBID:
                raise RuntimeError("MB mirror 500")
            return _fake_target_payload()

        svc = self._make_service(db, mb_lookup=fake_lookup)
        with self.assertLogs("lib.mbid_replace_service", level="WARNING") as cm:
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR)
        self.assertTrue(
            any("source" in m.lower() for m in cm.output),
            f"expected a warning naming the source lookup, got: {cm.output}",
        )

    def test_mb_target_lookup_unexpected_exception_logs_warning(self):
        """#501 item 3: mirrors the above for the TARGET lookup site — a
        previously-untested branch (the MB arm's target-lookup generic
        exception handler had no outcome OR logging coverage) gains
        both."""
        db = FakePipelineDB()
        self._seed_old(db)

        def fake_lookup(mbid, *, fresh=False):
            raise RuntimeError("MB mirror 500")

        svc = self._make_service(db, mb_lookup=fake_lookup)
        with self.assertLogs("lib.mbid_replace_service", level="WARNING") as cm:
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR)
        self.assertTrue(
            any("target" in m.lower() for m in cm.output),
            f"expected a warning naming the target lookup, got: {cm.output}",
        )

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

    def test_state_capture_under_lock_uses_fresh_beets_path(
        self,
    ):
        """Race-window guard (P0 fix): the importer finishes between
        Phase 0 (source loaded as ``downloading``) and Phase 1 (lock
        acquired). Without the fix the service would see the stale
        ``downloading`` status and skip beets cleanup. With the fix
        the state-capture inside the lock re-reads the row and Phase 4
        targets the freshly resolved album PK and current path."""

        db = FakePipelineDB()
        self._seed_old(db, status="downloading")

        # The importer mutation: when the lock is acquired, flip the row to
        # ``imported``.
        def lock_callable(namespace, key):
            row = db._requests[42]
            row["status"] = "imported"
            return True

        db.set_advisory_lock_result(lock_callable)

        beets = FakeBeetsDB(library_root="/mnt/virtio/Music/Beets")
        beets.set_album_ids_for_release(OLD_MBID, [77])
        beets.set_item_paths(OLD_MBID, [(
            701,
            "/mnt/virtio/Music/Beets/Fresh Artist/Fresh Album/01.flac",
        )])
        delete_calls: list[BeetsDeleteRequest] = []

        def exact_delete(request: BeetsDeleteRequest) -> BeetsDeleteCompleted:
            delete_calls.append(request)
            return BeetsDeleteCompleted(
                album_id=request.album_id,
                album_name="Fresh Album",
                artist_name="Fresh Artist",
                former_album_path=(
                    "/mnt/virtio/Music/Beets/Fresh Artist/Fresh Album"
                ),
                deleted_tracks=1,
                deleted_artifacts=1,
                preserved_paths=(),
            )

        with patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan"
        ) as mock_plex, patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan"
        ) as mock_jellyfin:
            svc = self._make_service(
                db,
                beets_db_factory=lambda: beets,
                beets_delete_fn=exact_delete,
            )
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        self.assertEqual([request.album_id for request in delete_calls], [77])
        # The fresh Beets path drives both media notifications.
        mock_plex.assert_called_once()
        _, plex_kwargs = mock_plex.call_args
        self.assertEqual(
            plex_kwargs.get("imported_path"),
            "/mnt/virtio/Music/Beets/Fresh Artist/Fresh Album",
        )
        mock_jellyfin.assert_called_once()
        _, jellyfin_kwargs = mock_jellyfin.call_args
        self.assertEqual(
            jellyfin_kwargs.get("imported_path"),
            "/mnt/virtio/Music/Beets/Fresh Artist/Fresh Album",
        )


class TestReplaceDiscogsArm(_ServiceCase):
    """Discogs-pathway Replace (U3): the full outcome matrix for a
    numeric Discogs source. The MB arm is exercised separately and must
    stay byte-for-byte unchanged (R3)."""

    def test_discogs_happy_path_dual_writes_identity(self):
        """Discogs source with a persisted master + sibling target →
        replaced. The superseded-into row carries dual identity (KTD-4):
        ``mb_release_id`` AND ``discogs_release_id`` both hold the picked
        canonical id, and the numeric master lands in
        ``mb_release_group_id``."""
        self._patch_externals()
        db = FakePipelineDB()
        self._seed_discogs(db, status="wanted")
        plan_svc = MagicMock()
        svc = self._make_service(
            db,
            search_plan_service=plan_svc,
            discogs_lookup=(
                lambda rid, *, fresh=False: _fake_discogs_payload(
                    release_id=NEW_DISCOGS_ID, master=DISCOGS_MASTER,
                )
            ),
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        assert result.new_request_id is not None
        # Old row frozen as replaced audit.
        old = db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "replaced")
        # New row born wanted, back-linked, with dual identity + master.
        new = db.get_request(result.new_request_id)
        assert new is not None
        self.assertEqual(new["status"], "wanted")
        self.assertEqual(new["replaces_request_id"], 42)
        self.assertEqual(new["mb_release_id"], NEW_DISCOGS_ID)
        self.assertEqual(new["discogs_release_id"], NEW_DISCOGS_ID)
        self.assertEqual(new["mb_release_group_id"], DISCOGS_MASTER)
        plan_svc.generate_for_request.assert_called_once_with(
            result.new_request_id, regenerate=False,
        )
        self._assert_slskd_untouched(svc.slskd)

    def test_discogs_lazy_backfill_source_master(self):
        """Source row with a NULL master (legacy row): the source id is
        looked up fresh to resolve the master, then the replace proceeds
        (mirror of ``test_release_group_mismatch_after_lazy_backfill``,
        happy variant). No persistence of the source master is required —
        the superseded-into row carries the master via the supersede
        call, and the old row is about to freeze."""
        self._patch_externals()
        db = FakePipelineDB()
        self._seed_discogs(db, master=None)
        calls: list[str] = []

        def fake_lookup(rid, *, fresh=False):
            calls.append(str(rid))
            if str(rid) == OLD_DISCOGS_ID:
                return _fake_discogs_payload(
                    release_id=OLD_DISCOGS_ID, master=DISCOGS_MASTER,
                )
            return _fake_discogs_payload(
                release_id=NEW_DISCOGS_ID, master=DISCOGS_MASTER,
            )

        svc = self._make_service(db, discogs_lookup=fake_lookup)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        # Lazy-backfill issued a fresh lookup of the source id.
        self.assertIn(OLD_DISCOGS_ID, calls)

    def test_source_lazy_backfill_mirror_unconfigured(self):
        """SOURCE lazy-backfill lookup (masterless legacy row) surfaces
        RESULT_MIRROR_UNCONFIGURED when the source-master resolution hits
        an unconfigured Discogs mirror — the target lookup is never
        reached and no supersede occurs (Rule B: the real
        ``DiscogsMirrorNotConfigured`` is raised)."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=None)
        calls: list[str] = []

        def fake_lookup(rid, *, fresh=False):
            calls.append(str(rid))
            if str(rid) == OLD_DISCOGS_ID:
                raise DiscogsMirrorNotConfigured("no mirror")
            return _fake_discogs_payload()

        svc = self._make_service(db, discogs_lookup=fake_lookup)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_MIRROR_UNCONFIGURED)
        # Source lookup raised first → target lookup never ran.
        self.assertEqual(calls, [OLD_DISCOGS_ID])
        # No supersede: old row untouched, no descendant.
        self.assertIsNone(result.new_request_id)
        old = db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "wanted")

    def test_source_lazy_backfill_transient_urlerror(self):
        """SOURCE lazy-backfill lookup classifies a network blip as
        RESULT_TRANSIENT (retryable), not RESULT_TARGET_INVALID — the
        source id is already known-good; the mirror was just unreachable
        (Rule B: the real ``URLError`` is raised). No supersede occurs."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=None)
        calls: list[str] = []

        def fake_lookup(rid, *, fresh=False):
            calls.append(str(rid))
            if str(rid) == OLD_DISCOGS_ID:
                raise URLError("connection refused")
            return _fake_discogs_payload()

        svc = self._make_service(db, discogs_lookup=fake_lookup)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TRANSIENT)
        # Source lookup raised first → target lookup never ran.
        self.assertEqual(calls, [OLD_DISCOGS_ID])
        self.assertIsNone(result.new_request_id)
        old = db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "wanted")

    def test_source_lazy_backfill_generic_exception_target_invalid(self):
        """SOURCE lazy-backfill lookup maps an unexpected error to
        RESULT_TARGET_INVALID (the generic branch, which also logs a
        warning). Target lookup never runs and no supersede occurs."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=None)
        calls: list[str] = []

        def fake_lookup(rid, *, fresh=False):
            calls.append(str(rid))
            if str(rid) == OLD_DISCOGS_ID:
                raise RuntimeError("Discogs mirror 500")
            return _fake_discogs_payload()

        svc = self._make_service(db, discogs_lookup=fake_lookup)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR)
        # Source lookup raised first → target lookup never ran.
        self.assertEqual(calls, [OLD_DISCOGS_ID])
        self.assertIsNone(result.new_request_id)
        old = db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "wanted")

    def test_discogs_source_uuid_target_invalid(self):
        """AE2: a UUID target against a Discogs source is cross-pathway →
        target_invalid (never reaches the mirror)."""
        db = FakePipelineDB()
        self._seed_discogs(db)
        called = {"hit": False}

        def fake_lookup(rid, *, fresh=False):
            called["hit"] = True
            return _fake_discogs_payload()

        svc = self._make_service(db, discogs_lookup=fake_lookup)
        result = svc.replace_request_mbid(42, target_mb_release_id=NEW_MBID)
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_CROSS_PATHWAY_TARGET)
        self.assertFalse(called["hit"])

    def test_mb_source_numeric_target_invalid(self):
        """AE2 (mirror direction): a numeric Discogs target against an MB
        source is cross-pathway → target_invalid."""
        db = FakePipelineDB()
        self._seed_old(db)
        svc = self._make_service(db)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_CROSS_PATHWAY_TARGET)

    def test_masterless_source_other_target_rejected(self):
        """AE1 / R10: a masterless Discogs source rejects any target that
        is not the source itself."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=None)

        def fake_lookup(rid, *, fresh=False):
            # Source resolves masterless; target never reached.
            return _fake_discogs_payload(release_id=str(rid), master=None)

        svc = self._make_service(db, discogs_lookup=fake_lookup)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(
            result.reason, REPLACE_REASON_SOURCE_NO_RELEASE_GROUP,
        )
        assert result.error_message is not None
        self.assertIn("master", result.error_message)

    def test_masterless_source_same_target_same_as_current(self):
        """AE1: a masterless source with target == source is caught by the
        shared same-as-current gate (before the arm runs)."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=None)
        svc = self._make_service(db)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=OLD_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TARGET_SAME_AS_CURRENT)

    def test_discogs_master_mismatch(self):
        """R10: target's master ≠ source's master → release-group mismatch
        (reuses the shared MB mismatch outcome)."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=DISCOGS_MASTER)
        svc = self._make_service(
            db,
            discogs_lookup=(
                lambda rid, *, fresh=False: _fake_discogs_payload(
                    release_id=NEW_DISCOGS_ID, master=OTHER_DISCOGS_MASTER,
                )
            ),
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(
            result.outcome, RESULT_TARGET_RELEASE_GROUP_MISMATCH
        )

    def test_discogs_target_no_master(self):
        """#501 item 2: the Discogs arm's analog of
        test_target_invalid_missing_rg — target resolves but has no
        master. REASON: target_no_release_group (pathway-neutral naming;
        same reason code as the MB arm's missing-release_group_id case)."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=DISCOGS_MASTER)
        svc = self._make_service(
            db,
            discogs_lookup=(
                lambda rid, *, fresh=False: _fake_discogs_payload(
                    release_id=NEW_DISCOGS_ID, master=None,
                )
            ),
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(
            result.reason, REPLACE_REASON_TARGET_NO_RELEASE_GROUP,
        )

    def test_discogs_target_empty_payload(self):
        """#501 item 2: the Discogs arm's analog of
        test_target_invalid_target_empty_payload — the target lookup
        succeeds but returns a falsy payload. REASON: unresolvable_target."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=DISCOGS_MASTER)
        svc = self._make_service(
            db, discogs_lookup=lambda rid, *, fresh=False: {},
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TARGET_INVALID)
        self.assertEqual(result.reason, REPLACE_REASON_UNRESOLVABLE_TARGET)

    def test_discogs_mirror_unconfigured(self):
        """AE3 / R11: an unconfigured Discogs mirror surfaces its own
        outcome — distinct from target_invalid and transient (Rule B: the
        real ``DiscogsMirrorNotConfigured`` is raised)."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=DISCOGS_MASTER)

        def fake_lookup(rid, *, fresh=False):
            raise DiscogsMirrorNotConfigured("no mirror")

        svc = self._make_service(db, discogs_lookup=fake_lookup)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_MIRROR_UNCONFIGURED)

    def test_discogs_transient_urlerror(self):
        """A network blip on the Discogs lookup is retryable → transient
        (Rule B: the real ``URLError`` is raised)."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=DISCOGS_MASTER)

        def fake_lookup(rid, *, fresh=False):
            raise URLError("connection refused")

        svc = self._make_service(db, discogs_lookup=fake_lookup)
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TRANSIENT)

    def test_discogs_collision_via_identity_lookup(self):
        """KTD-6: an active row already holding the target Discogs id (in
        ``discogs_release_id``) is a collision — found via the
        identity-aware ``get_request_by_release_id``, NOT
        ``get_request_by_mb_release_id`` (the holder's ``mb_release_id``
        deliberately differs)."""
        db = FakePipelineDB()
        self._seed_discogs(db, master=DISCOGS_MASTER)
        db.seed_request(make_request_row(
            id=43, mb_release_id="9999",
            discogs_release_id=NEW_DISCOGS_ID,
            mb_release_group_id=DISCOGS_MASTER, status="downloading",
        ))
        svc = self._make_service(
            db,
            discogs_lookup=(
                lambda rid, *, fresh=False: _fake_discogs_payload(
                    release_id=NEW_DISCOGS_ID, master=DISCOGS_MASTER,
                )
            ),
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_DISCOGS_ID,
        )
        self.assertEqual(result.outcome, RESULT_TARGET_COLLISION_REQUEST)
        self.assertEqual(result.current_status, "downloading")


class TestReplaceHappyPath(_ServiceCase):
    """Cover RESULT_REPLACED + ordering invariants + the Pet Grief
    merged-upstream case (target redirects to canonical). ``_patch_externals``
    is inherited from ``_ServiceCase``."""

    def _replace(self, *, old_status="wanted", **service_kwargs):
        db = FakePipelineDB()
        self._seed_old(db, status=old_status)
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

    def test_happy_path_imported_calls_pinned_exact_delete(self):
        self._patch_externals()
        beets = self._installed_beets()
        exact_delete = MagicMock(side_effect=self._completed_delete)
        db, _, svc = self._replace(
            old_status="imported",
            beets_db_factory=lambda: beets,
            beets_delete_fn=exact_delete,
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_MBID,
        )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        exact_delete.assert_called_once()
        request = exact_delete.call_args.args[0]
        self.assertEqual(request.album_id, 77)
        self.assertEqual(request.expected_release_id, OLD_MBID)

    def test_injected_exact_delete_runs_at_the_real_service_boundary(self):
        self._patch_externals()
        beets = self._installed_beets(album_id=91)
        calls: list[BeetsDeleteRequest] = []

        def exact_delete(request: BeetsDeleteRequest) -> BeetsDeleteCompleted:
            calls.append(request)
            return self._completed_delete(request)

        _db, _, svc = self._replace(
            old_status="imported",
            beets_db_factory=lambda: beets,
            beets_delete_fn=exact_delete,
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_MBID,
        )

        self.assertEqual(result.outcome, RESULT_REPLACED)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].album_id, 91)
        self.assertEqual(calls[0].expected_release_id, OLD_MBID)

    def test_replace_removes_preexisting_install_on_wanted_backfill_row(self):
        """The Passenger regression (2026-07-18): a library-backfill row is
        ``wanted`` while its pre-existing install sits in beets. Replace
        gated cleanup on ``old_status == "imported"`` — an invariant that
        was true at U4 time and silently broke when the 2026-06-04 full-
        library backfill created wanted rows tracking on-disk installs.
        Replace REPLACES: the old release's install is displaced whenever
        the old release resolves uniquely, whatever the request status."""
        self._patch_externals()
        beets = self._installed_beets()
        exact_delete = MagicMock(side_effect=self._completed_delete)
        db, _, svc = self._replace(
            old_status="wanted",
            beets_db_factory=lambda: beets,
            beets_delete_fn=exact_delete,
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_MBID,
        )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        exact_delete.assert_called_once()
        self.assertEqual(exact_delete.call_args.args[0].album_id, 77)

    @given(old_status=st.sampled_from(
        ["wanted", "downloading", "unsearchable", "imported"]))
    def test_replace_displaces_old_install_for_every_source_status(
        self, old_status,
    ):
        """Generated pair for the pin above: over the complete source-status
        space, Replace always routes a uniquely resolved current album through
        pinned exact deletion. Status is lifecycle; displacement keys on the
        fresh identity snapshot."""
        self._patch_externals()
        beets = self._installed_beets()
        exact_delete = MagicMock(side_effect=self._completed_delete)
        db, _, svc = self._replace(
            old_status=old_status,
            beets_db_factory=lambda: beets,
            beets_delete_fn=exact_delete,
        )
        result = svc.replace_request_mbid(
            42, target_mb_release_id=NEW_MBID,
        )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        exact_delete.assert_called_once()
        self.assertEqual(exact_delete.call_args.args[0].album_id, 77)

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
        db, _, svc = self._replace(old_status="unsearchable")
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
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan"
        ):
            db = FakePipelineDB()
            self._seed_old(db, status="imported")
            beets = self._installed_beets()
            svc = self._make_service(
                db,
                beets_db_factory=lambda: beets,
                beets_delete_fn=MagicMock(
                    side_effect=RuntimeError("pinned delete crashed"),
                ),
            )
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

    def test_pinned_delete_failure_reason_is_preserved_in_warning(self):
        """#777 configuration failures remain explicit and non-fatal."""
        with patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ), patch(
            "lib.mbid_replace_service.trigger_plex_scan"
        ), patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan"
        ):
            db = FakePipelineDB()
            self._seed_old(db, status="imported")
            beets = self._installed_beets()
            svc = self._make_service(
                db,
                beets_db_factory=lambda: beets,
                beets_delete_fn=lambda request: BeetsDeleteFailed(
                    album_id=request.album_id,
                    reason="configuration_mismatch",
                    detail="active pinned config differs",
                    album_still_present=True,
                ),
            )
            result = svc.replace_request_mbid(
                42, target_mb_release_id=NEW_MBID,
            )
        self.assertEqual(result.outcome, RESULT_REPLACED)
        self.assertTrue(
            any(
                "id:77" in warning
                and "configuration_mismatch" in warning
                for warning in result.warnings
            ),
            f"exact pinned failure missing from warnings: {result.warnings}",
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
    - Rescan helpers MUST run AFTER fs cleanup (so Plex sees
      the deleted folder, not the pre-cleanup view).
    - slskd.transfers.cancel_download MUST never be called (R23 —
      Replace intentionally never touches in-flight transfers).
    """

    def test_rescans_run_after_advisory_lock_release(self):
        """Phase 5 (search plan + Plex/Jellyfin rescans) must run
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
        self._seed_old(db, status="imported")
        real_supersede = db.supersede_request_mbid

        def supersede_recording(*args, **kwargs):
            manager.supersede(*args, **kwargs)
            return real_supersede(*args, **kwargs)

        beets = self._installed_beets()
        exact_delete = MagicMock(side_effect=self._completed_delete)

        with patch.object(
            db, "supersede_request_mbid",
            side_effect=supersede_recording,
        ), patch(
            "lib.mbid_replace_service.delete_wrong_match_group",
            side_effect=_empty_wrong_match_summary,
        ) as mock_wm_delete, patch(
            "lib.mbid_replace_service.trigger_plex_scan",
        ) as mock_plex, patch(
            "lib.mbid_replace_service.trigger_jellyfin_scan",
        ) as mock_jelly:
            manager.attach_mock(exact_delete, "beets_delete")
            manager.attach_mock(mock_wm_delete, "delete_wrong_match_group")
            manager.attach_mock(mock_plex, "trigger_plex_scan")
            manager.attach_mock(mock_jelly, "trigger_jellyfin_scan")

            svc = self._make_service(
                db,
                beets_db_factory=lambda: beets,
                beets_delete_fn=exact_delete,
            )
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
                "trigger_plex_scan",
                "trigger_jellyfin_scan",
            )
        )
        for fs_helper in ("beets_delete", "delete_wrong_match_group"):
            self.assertLess(
                call_names.index(fs_helper), first_rescan_idx,
                f"{fs_helper} ran after a rescan helper "
                f"(call order: {call_names})",
            )
        # slskd was never touched (R23).
        self._assert_slskd_untouched(svc.slskd)


class TestReplaceCurrentAuthorityRealBeets(_ServiceCase):
    def test_moved_real_album_exact_delete_and_rescans_across_identities(self):
        worlds = (
            ("mb", OLD_MBID, NEW_MBID, RG_ID, False),
            (
                "discogs-modern",
                OLD_DISCOGS_ID,
                NEW_DISCOGS_ID,
                DISCOGS_MASTER,
                False,
            ),
            (
                "discogs-legacy",
                OLD_DISCOGS_ID,
                NEW_DISCOGS_ID,
                DISCOGS_MASTER,
                True,
            ),
        )
        for name, source_id, target_id, group_id, legacy in worlds:
            with self.subTest(identity=name):
                with BeetsWorld(
                    REPO,
                    subprocess_mirror_url="http://127.0.0.1:9",
                ) as world:
                    world.import_release(BeetsWorldRelease(
                        release_id=source_id,
                        artist="Archive Artist",
                        album="Replace Source",
                        year=2001,
                        track_count=2,
                    ))
                    if name.startswith("discogs"):
                        world.set_discogs_identity_layout(
                            source_id,
                            legacy=legacy,
                        )
                    moved = world.relocate_release_out_of_band(
                        source_id,
                        world.library_root / name / "fresh current path",
                        store_relative_paths=True,
                    )
                    db = FakePipelineDB()
                    if name == "mb":
                        self._seed_old(
                            db,
                            status="imported",
                        )
                    else:
                        self._seed_discogs(
                            db,
                            status="imported",
                        )
                    target = (
                        _fake_target_payload(
                            mbid=target_id,
                            rg_id=group_id,
                        )
                        if name == "mb"
                        else _fake_discogs_payload(
                            release_id=target_id,
                            master=group_id,
                        )
                    )
                    scans: list[str | None] = []
                    with (
                        world.subprocess_environment(),
                        BeetsDB(
                            str(world.library_db),
                            library_root=str(world.library_root),
                        ) as beets,
                        patch(
                            "lib.mbid_replace_service.trigger_plex_scan",
                            side_effect=lambda _cfg, imported_path=None: (
                                scans.append(imported_path)
                            ),
                        ),
                        patch(
                            "lib.mbid_replace_service.trigger_jellyfin_scan",
                            side_effect=lambda _cfg, imported_path=None: (
                                scans.append(imported_path)
                            ),
                        ),
                    ):
                        service = self._make_service(
                            db,
                            beets_db_factory=lambda: beets,
                            beets_delete_fn=run_beets_delete,
                            mb_lookup=lambda _rid, *, fresh=False: target,
                            discogs_lookup=lambda _rid, *, fresh=False: target,
                        )
                        result = service.replace_request_mbid(
                            42,
                            target_mb_release_id=target_id,
                        )

                    self.assertEqual(result.outcome, RESULT_REPLACED)
                    self.assertEqual(scans, [moved.album_path, moved.album_path])
                    self.assertTrue(
                        all(not Path(path).exists() for path in moved.item_paths)
                    )
                    with BeetsDB(
                        str(world.library_db),
                        library_root=str(world.library_root),
                    ) as beets:
                        identity = ReleaseIdentity.from_id(source_id)
                        assert identity is not None
                        self.assertIsInstance(
                            beets.resolve_current_release(identity),
                            CurrentBeetsMissing,
                        )

    def test_real_missing_authority_proceeds_without_beets_mutation(self):
        with BeetsWorld(REPO) as world:
            sibling = world.import_release(BeetsWorldRelease(
                release_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
                artist="Archive Artist",
                album="Sibling Pressing",
                year=2001,
                track_count=1,
            ))
            before = {
                path: Path(path).read_bytes() for path in sibling.item_paths
            }
            db = FakePipelineDB()
            self._seed_old(db, status="imported")
            delete_op = MagicMock(side_effect=AssertionError(
                "missing authority reached Beets mutation",
            ))
            scans: list[str | None] = []
            with (
                BeetsDB(
                    str(world.library_db),
                    library_root=str(world.library_root),
                ) as beets,
                patch(
                    "lib.mbid_replace_service.trigger_plex_scan",
                    side_effect=lambda _cfg, imported_path=None: scans.append(
                        imported_path,
                    ),
                ),
                patch(
                    "lib.mbid_replace_service.trigger_jellyfin_scan",
                    side_effect=lambda _cfg, imported_path=None: scans.append(
                        imported_path,
                    ),
                ),
            ):
                service = self._make_service(
                    db,
                    beets_db_factory=lambda: beets,
                    beets_delete_fn=delete_op,
                )
                result = service.replace_request_mbid(
                    42,
                    target_mb_release_id=NEW_MBID,
                )

            self.assertEqual(result.outcome, RESULT_REPLACED)
            self.assertEqual(scans, [None, None])
            delete_op.assert_not_called()
            self.assertEqual(
                {path: Path(path).read_bytes() for path in before},
                before,
            )


if TYPE_CHECKING:
    from typing import cast

    from lib.mbid_replace_service import MbidReplaceDB as _ReplaceDB
    from lib.pipeline_db import PipelineDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_replace_protocol: _ReplaceDB = cast("PipelineDB", None)
    _fake_db_satisfies_replace_protocol: _ReplaceDB = cast("FakePipelineDB", None)


class TestReplaceDBProtocolParity(unittest.TestCase):
    """#409: PipelineDB and FakePipelineDB must satisfy MbidReplaceDB."""

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.mbid_replace_service import MbidReplaceDB
        from lib.pipeline_db import PipelineDB

        self.assertTrue(issubclass(PipelineDB, MbidReplaceDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        from lib.mbid_replace_service import MbidReplaceDB

        self.assertTrue(issubclass(FakePipelineDB, MbidReplaceDB))

    def test_replace_protocol_extends_forwarded_surfaces(self) -> None:
        """Replace forwards its handle into wrong-match group delete,
        and SearchPlanService."""
        from lib.mbid_replace_service import MbidReplaceDB
        from lib.search_plan_service import SearchPlanDB
        from lib.wrong_match_delete_service import WrongMatchDeleteDB

        self.assertTrue(issubclass(MbidReplaceDB, WrongMatchDeleteDB))
        self.assertTrue(issubclass(MbidReplaceDB, SearchPlanDB))


if __name__ == "__main__":
    unittest.main()
