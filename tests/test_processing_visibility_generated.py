"""Generated contracts for request visibility and status taxonomy (#898 U6)."""

from __future__ import annotations

import unittest
from collections.abc import Callable, Mapping

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile
from lib.pipeline_db._shared import (
    ACQUISITION_REQUEST_STATUSES,
    DASHBOARD_WANTED_BACKLOG_STATUSES,
    REQUEST_STATUSES,
    processing_owner_payload,
)
from lib.pipeline_db.transfer_ledger import _ACTIVE_REQUEST_STATUSES

OwnerProjector = Callable[
    [Mapping[str, object]],
    dict[str, object] | None,
]


def _latest_job_mutant(
    row: Mapping[str, object],
) -> dict[str, object] | None:
    """Known-bad projector: presents whichever joined job happens to exist."""
    joined_job_id = row.get("_processing_owner_job_id")
    if not isinstance(joined_job_id, int):
        return None
    return {
        "job_id": joined_job_id,
        "status": row.get("_processing_owner_status"),
        "preview_status": row.get("_processing_owner_preview_status"),
    }


class TestProcessingOwnerProjectionGenerated(unittest.TestCase):
    @staticmethod
    def _row(status: str) -> dict[str, object]:
        return {
            "status": status,
            "active_automation_import_job_id": 17,
            "_processing_owner_job_id": 17,
            "_processing_owner_status": "queued",
            "_processing_owner_preview_status": "waiting",
        }

    def _assert_exact_owner_contract(self, projector: OwnerProjector) -> None:
        self.assertEqual(projector(self._row("processing")), {
            "job_id": 17,
            "status": "queued",
            "preview_status": "waiting",
        })
        stale_pointer = self._row("wanted")
        self.assertIsNone(projector(stale_pointer))

        mismatched_join = self._row("processing")
        mismatched_join["_processing_owner_job_id"] = 18
        with self.assertRaises(RuntimeError):
            projector(mismatched_join)

    @given(status=st.sampled_from(sorted(REQUEST_STATUSES)))
    def test_owner_is_visible_if_and_only_if_request_is_processing(
        self,
        status: str,
    ) -> None:
        projected = processing_owner_payload(self._row(status))
        self.assertEqual(projected is not None, status == "processing")

    def test_exact_owner_contract_pin(self) -> None:
        self._assert_exact_owner_contract(processing_owner_payload)

    def test_known_bad_latest_job_projection_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            self._assert_exact_owner_contract(_latest_job_mutant)


class TestProcessingStatusTaxonomyGenerated(unittest.TestCase):
    @staticmethod
    def _assert_taxonomy(
        *,
        acquisition: tuple[str, ...],
        backlog: tuple[str, ...],
        transfer_ledger: tuple[str, ...],
    ) -> None:
        assert set(acquisition) == {"downloading", "processing"}
        assert set(backlog) == {"wanted", "downloading", "processing"}
        assert set(transfer_ledger) == {"wanted", "downloading"}

    @given(status=st.sampled_from(sorted(REQUEST_STATUSES)))
    def test_processing_enters_visibility_but_not_slskd_ownership(
        self,
        status: str,
    ) -> None:
        self.assertEqual(
            status in ACQUISITION_REQUEST_STATUSES,
            status in {"downloading", "processing"},
        )
        self.assertEqual(
            status in DASHBOARD_WANTED_BACKLOG_STATUSES,
            status in {"wanted", "downloading", "processing"},
        )
        if status == "processing":
            self.assertNotIn(status, _ACTIVE_REQUEST_STATUSES)

    def test_taxonomy_pin(self) -> None:
        self._assert_taxonomy(
            acquisition=ACQUISITION_REQUEST_STATUSES,
            backlog=DASHBOARD_WANTED_BACKLOG_STATUSES,
            transfer_ledger=_ACTIVE_REQUEST_STATUSES,
        )

    def test_known_bad_processing_transfer_ownership_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            self._assert_taxonomy(
                acquisition=ACQUISITION_REQUEST_STATUSES,
                backlog=DASHBOARD_WANTED_BACKLOG_STATUSES,
                transfer_ledger=(*_ACTIVE_REQUEST_STATUSES, "processing"),
            )


if __name__ == "__main__":
    unittest.main()
