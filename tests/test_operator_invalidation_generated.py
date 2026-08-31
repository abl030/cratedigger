"""Generated owner-ordering patrol for canonical invalidators."""

from __future__ import annotations

import copy
import os
import tempfile
import unittest
from types import SimpleNamespace

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib import transitions
from lib.force_import_service import (
    RESULT_PROCESSING_LOCKED,
    RESULT_QUEUED,
    enqueue_force_import,
)
from lib.pipeline_delete_service import (
    PipelineDeleteApplied,
    delete_pipeline_request,
)
from tests.dispatch_helpers import handoff_automation_owner
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _owner_blind_delete_mutant(db: FakePipelineDB, request_id: int) -> bool:
    """Known-bad implementation that deletes without the owner predicate."""
    existed = request_id in db._requests
    db._requests.pop(request_id, None)
    return existed


class TestOperatorInvalidationGenerated(unittest.TestCase):
    @given(processing_owner=st.booleans())
    @example(processing_owner=True)
    def test_delete_applies_exactly_when_no_processing_owner_exists(
        self,
        *,
        processing_owner: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=71, status="wanted"))
        if processing_owner:
            handoff_automation_owner(db, 71)
        before = copy.deepcopy(db.get_request(71))

        result = delete_pipeline_request(db, 71)

        if processing_owner:
            self.assertIsInstance(result, transitions.TransitionConflict)
            assert isinstance(result, transitions.TransitionConflict)
            self.assertEqual(
                result.kind,
                transitions.TransitionConflictKind.processing_locked,
            )
            self.assertEqual(db.get_request(71), before)
        else:
            self.assertEqual(result, PipelineDeleteApplied(71))
            self.assertIsNone(db.get_request(71))

    def test_known_bad_owner_blind_delete_is_detected(self) -> None:
        mutant = FakePipelineDB()
        mutant.seed_request(make_request_row(id=72, status="wanted"))
        handoff_automation_owner(mutant, 72)
        self.assertTrue(_owner_blind_delete_mutant(mutant, 72))
        self.assertIsNone(mutant.get_request(72))

        production = FakePipelineDB()
        production.seed_request(make_request_row(id=72, status="wanted"))
        handoff_automation_owner(production, 72)
        result = delete_pipeline_request(production, 72)
        self.assertIsInstance(result, transitions.TransitionConflict)
        self.assertIsNotNone(production.get_request(72))

    @given(processing_owner=st.booleans())
    @example(processing_owner=True)
    def test_force_enqueue_and_generic_transition_share_owner_truth(
        self,
        *,
        processing_owner: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            staging = os.path.join(root, "Incoming")
            slskd = os.path.join(root, "slskd")
            processing = os.path.join(root, "processing")
            failed = os.path.join(
                staging,
                "failed_imports",
                "generated",
                "Album",
            )
            os.makedirs(failed)
            os.makedirs(slskd)
            os.makedirs(processing)
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=73,
                status="wanted",
                mb_release_id="generated-force-owner",
            ))
            log_id = db.log_download(
                request_id=73,
                outcome="rejected",
                validation_result={"failed_path": failed},
            )
            if processing_owner:
                handoff_automation_owner(db, 73)
            before = copy.deepcopy(db.get_request(73))

            force = enqueue_force_import(
                db,
                SimpleNamespace(
                    beets_staging_dir=staging,
                    slskd_download_dir=slskd,
                    processing_dir=processing,
                ),
                log_id,
            )
            transition = transitions.finalize_operator_request(
                db,
                73,
                transitions.RequestTransition.to_unsearchable(
                    from_status=(
                        "processing" if processing_owner else "wanted"
                    ),
                ),
            )

            if processing_owner:
                self.assertEqual(force.outcome, RESULT_PROCESSING_LOCKED)
                self.assertIsInstance(
                    transition,
                    transitions.TransitionConflict,
                )
                assert isinstance(
                    transition,
                    transitions.TransitionConflict,
                )
                self.assertEqual(
                    transition.kind,
                    transitions.TransitionConflictKind.processing_locked,
                )
                self.assertEqual(db.get_request(73), before)
            else:
                self.assertEqual(force.outcome, RESULT_QUEUED)
                self.assertIsInstance(
                    transition,
                    transitions.TransitionApplied,
                )
                self.assertEqual(db.request(73)["status"], "unsearchable")


if __name__ == "__main__":
    unittest.main()
