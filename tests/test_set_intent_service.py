"""Authoritative branch coverage for ``lib/set_intent_service.py`` (#1278).

Both operator surfaces (``pipeline-cli set-intent`` and
``POST /api/pipeline/set-intent``) are thin adapters over
:func:`set_lossless_intent`; every outcome branch is proven here against
domain state, and the surface tests check only their wrapper mappings.
"""

from __future__ import annotations

import unittest
from typing import get_args

from lib import transitions
from lib.quality import QUALITY_LOSSLESS
from lib.set_intent_service import (
    SET_INTENT_EXIT_CODES,
    SET_INTENT_HTTP_STATUS,
    SetIntentOutcome,
    SetIntentResult,
    set_lossless_intent,
)
from tests.dispatch_helpers import handoff_automation_owner
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _updated(result: SetIntentResult | transitions.TransitionConflict) -> SetIntentResult:
    assert isinstance(result, SetIntentResult)
    return result


def _conflict(
    result: SetIntentResult | transitions.TransitionConflict,
) -> transitions.TransitionConflict:
    assert isinstance(result, transitions.TransitionConflict)
    return result


class TestSetLosslessIntent(unittest.TestCase):
    def test_not_found(self) -> None:
        result = _updated(set_lossless_intent(FakePipelineDB(), 5, intent="lossless"))
        self.assertEqual(result.outcome, "not_found")
        self.assertEqual(result.request_id, 5)

    def test_initializing_refused_without_write(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=7, status="initializing"))
        result = _updated(set_lossless_intent(db, 7, intent="lossless"))
        self.assertEqual(result.outcome, "initializing")
        self.assertEqual(db.request(7)["status"], "initializing")
        self.assertIsNone(db.request(7)["target_format"])

    def test_downloading_refused_without_write(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=8, status="downloading"))
        result = _updated(set_lossless_intent(db, 8, intent="lossless"))
        self.assertEqual(result.outcome, "downloading")
        self.assertEqual(db.request(8)["status"], "downloading")
        self.assertIsNone(db.request(8)["target_format"])

    def test_replaced_reports_frozen_edge_conflict(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=9, status="replaced"))
        conflict = _conflict(set_lossless_intent(db, 9, intent="lossless"))
        self.assertEqual(
            conflict.kind, transitions.TransitionConflictKind.invalid_edge
        )
        self.assertEqual(conflict.actual_status, "replaced")
        self.assertEqual(db.request(9)["status"], "replaced")
        self.assertIsNone(db.request(9)["target_format"])

    def test_processing_locked_reports_exact_owner(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=10, status="wanted", target_format=None,
        ))
        owner = handoff_automation_owner(db, 10)
        conflict = _conflict(set_lossless_intent(db, 10, intent="lossless"))
        self.assertEqual(
            conflict.kind, transitions.TransitionConflictKind.processing_locked
        )
        assert conflict.processing_owner is not None
        self.assertEqual(conflict.processing_owner.job_id, owner.id)
        self.assertIsNone(db.request(10)["target_format"])

    def test_wanted_lossless_updates_persistent_intent(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", artist_name="A", album_title="B",
        ))
        result = _updated(set_lossless_intent(db, 1, intent="lossless"))
        self.assertEqual(result.outcome, "updated")
        self.assertEqual(result.intent, "lossless")
        self.assertEqual(result.target_format, QUALITY_LOSSLESS)
        self.assertIsNone(result.old_target_format)
        self.assertEqual(result.artist_name, "A")
        self.assertEqual(result.album_title, "B")
        self.assertEqual(db.request(1)["target_format"], QUALITY_LOSSLESS)
        self.assertEqual(db.request(1)["status"], "wanted")

    def test_default_clears_target_and_stale_override(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=2, status="wanted",
            target_format=QUALITY_LOSSLESS,
            search_filetype_override=QUALITY_LOSSLESS,
        ))
        result = _updated(set_lossless_intent(db, 2, intent="default"))
        self.assertEqual(result.outcome, "updated")
        self.assertEqual(result.old_target_format, QUALITY_LOSSLESS)
        self.assertIsNone(db.request(2)["target_format"])
        self.assertIsNone(db.request(2)["search_filetype_override"])

    def test_imported_lossless_requeues_for_search(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=3, status="imported", artist_name="A", album_title="B",
            min_bitrate=245,
        ))
        result = _updated(set_lossless_intent(db, 3, intent="lossless"))
        self.assertEqual(result.outcome, "requeued")
        row = db.request(3)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["target_format"], QUALITY_LOSSLESS)
        self.assertEqual(row["search_filetype_override"], QUALITY_LOSSLESS)
        # prev_min_bitrate is the observable the min_bitrate kwarg actually
        # produces: reset_to_wanted preserves an OMITTED min_bitrate, so
        # row["min_bitrate"] == 245 alone cannot distinguish "passed
        # through" from "never touched" (PR2 mutant-runner survivor M5).
        self.assertEqual(row["min_bitrate"], 245)
        self.assertEqual(row["prev_min_bitrate"], 245)

    def test_requeue_phase_cas_race_reports_conflict(self) -> None:
        class RequeueRacingDB(FakePipelineDB):
            def update_request_fields(
                self,
                request_id: int,
                *,
                expected_status: str | None = None,
                **fields: object,
            ) -> bool:
                self.supersede_request_mbid(
                    request_id,
                    new_mb_release_id="requeue-race-new",
                    new_mb_release_group_id=None,
                    new_mb_artist_id=None,
                    new_artist_name="A",
                    new_album_title="B (correct pressing)",
                    new_year=None,
                    new_country=None,
                    new_tracks=[],
                )
                return super().update_request_fields(
                    request_id,
                    expected_status=expected_status,
                    **fields,
                )

        db = RequeueRacingDB()
        db.seed_request(make_request_row(
            id=12, status="imported", min_bitrate=245, target_format=None,
        ))
        conflict = _conflict(set_lossless_intent(db, 12, intent="lossless"))
        self.assertEqual(
            conflict.kind, transitions.TransitionConflictKind.stale_source
        )
        self.assertIsNone(db.request(12)["target_format"])

    def test_requeue_rebases_onto_a_committed_requeueable_status(self) -> None:
        # finalize_operator_request deliberately rebases operator intent
        # onto the committed status: a row that left ``imported`` for
        # ``unsearchable`` between the service's read and the transition
        # still requeues (``unsearchable -> wanted`` is a valid edge).
        class FlippingDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self._reads = 0

            def get_request(self, request_id: int):
                row = super().get_request(request_id)
                self._reads += 1
                if row is not None and self._reads == 1:
                    self._requests[request_id]["status"] = "unsearchable"
                return row

        db = FlippingDB()
        db.seed_request(make_request_row(
            id=13, status="imported", target_format=None,
        ))
        result = _updated(set_lossless_intent(db, 13, intent="lossless"))
        self.assertEqual(result.outcome, "requeued")
        row = db.request(13)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["target_format"], QUALITY_LOSSLESS)

    def test_requeue_phase_replace_race_reports_frozen_conflict(self) -> None:
        # The rebase stops at ``replaced``: frozen rows have no outgoing
        # edge, so a supersede landing mid-flight yields the canonical
        # invalid-edge conflict rather than a revival.
        class ReplacingDB(FakePipelineDB):
            def __init__(self) -> None:
                super().__init__()
                self._reads = 0

            def get_request(self, request_id: int):
                row = super().get_request(request_id)
                self._reads += 1
                if row is not None and self._reads == 1:
                    self._requests[request_id]["status"] = "replaced"
                return row

        db = ReplacingDB()
        db.seed_request(make_request_row(
            id=15, status="imported", target_format=None,
        ))
        conflict = _conflict(set_lossless_intent(db, 15, intent="lossless"))
        self.assertEqual(
            conflict.kind, transitions.TransitionConflictKind.invalid_edge
        )
        self.assertIsNone(db.request(15)["target_format"])

    def test_vanished_row_cas_miss_is_not_found(self) -> None:
        class VanishingDB(FakePipelineDB):
            def update_request_fields(
                self,
                request_id: int,
                *,
                expected_status: str | None = None,
                **fields: object,
            ) -> bool:
                del expected_status, fields
                self._requests.pop(request_id, None)
                return False

        db = VanishingDB()
        db.seed_request(make_request_row(id=14, status="wanted"))
        conflict = _conflict(set_lossless_intent(db, 14, intent="lossless"))
        self.assertEqual(
            conflict.kind, transitions.TransitionConflictKind.not_found
        )

    def test_imported_default_is_a_plain_update(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4, status="imported", target_format=QUALITY_LOSSLESS,
        ))
        result = _updated(set_lossless_intent(db, 4, intent="default"))
        self.assertEqual(result.outcome, "updated")
        row = db.request(4)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["target_format"])

    def test_update_cas_race_reports_conflict_not_success(self) -> None:
        class RacingDB(FakePipelineDB):
            def update_request_fields(
                self,
                request_id: int,
                *,
                expected_status: str | None = None,
                **fields: object,
            ) -> bool:
                self.supersede_request_mbid(
                    request_id,
                    new_mb_release_id="race-new",
                    new_mb_release_group_id=None,
                    new_mb_artist_id=None,
                    new_artist_name="A",
                    new_album_title="B (correct pressing)",
                    new_year=None,
                    new_country=None,
                    new_tracks=[],
                )
                return super().update_request_fields(
                    request_id,
                    expected_status=expected_status,
                    **fields,
                )

        db = RacingDB()
        db.seed_request(make_request_row(
            id=6, status="wanted", target_format=None,
        ))
        conflict = _conflict(set_lossless_intent(db, 6, intent="lossless"))
        self.assertEqual(
            conflict.kind, transitions.TransitionConflictKind.stale_source
        )
        self.assertEqual(conflict.actual_status, "replaced")
        self.assertEqual(db.request(6)["status"], "replaced")
        self.assertIsNone(db.request(6)["target_format"])


class TestSetIntentOutcomeTable(unittest.TestCase):
    def test_table_matches_the_outcome_literal(self) -> None:
        self.assertEqual(
            set(SET_INTENT_HTTP_STATUS), set(get_args(SetIntentOutcome))
        )

    def test_convention_values(self) -> None:
        self.assertEqual(SET_INTENT_HTTP_STATUS, {
            "updated": 200,
            "requeued": 200,
            "not_found": 404,
            "initializing": 409,
            "downloading": 409,
        })
        self.assertEqual(SET_INTENT_EXIT_CODES, {
            "updated": 0,
            "requeued": 0,
            "not_found": 2,
            "initializing": 4,
            "downloading": 4,
        })


if __name__ == "__main__":
    unittest.main()
