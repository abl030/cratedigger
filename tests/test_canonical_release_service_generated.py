"""Generated state properties for explicit canonical retirement (#1059 F6)."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.canonical_release_service import (
    OUTCOME_RETIRED,
    CanonicalReleaseService,
)
from tests.fakes import FakePipelineDB

LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
SECOND = "abe18a1c-ad01-423c-b6ca-63cfa8a9daf1"
OBSERVED = datetime(2026, 8, 6, tzinfo=UTC)


def check_retirement_preserves_request(
    before: dict[str, object], after: dict[str, object], *, changed: bool,
) -> None:
    """A retirement changes precisely its two canonical fields."""
    if changed:
        if after["canonical_release_id"] is not None:
            raise AssertionError("retired request retained its survivor")
        if after["canonical_resolved_at"] is not None:
            raise AssertionError("retired request retained its observation")
        changed_fields = {
            field for field in before if before[field] != after[field]
        }
        if changed_fields != {"canonical_release_id", "canonical_resolved_at"}:
            raise AssertionError(
                f"retirement changed fields beyond canonical state: {changed_fields}")
    elif after != before:
        raise AssertionError("a no-op retirement changed the request")


def check_lookup_non_answer_preserves_canonical(
    before: str | None, after: str | None,
) -> None:
    """A mirror non-answer never authorizes a clear."""
    if after != before:
        raise AssertionError("lookup non-answer changed canonical state")


class TestCanonicalRetirementGenerated(unittest.TestCase):
    @given(st.sampled_from(("canonical", "none", "replaced", "stale")))
    def test_retirement_is_explicit_and_compare_and_set(self, state: str) -> None:
        class RaceDB(FakePipelineDB):
            def retire_canonical_release_id(self, request_id, **kwargs):
                if state == "stale":
                    return False
                return super().retire_canonical_release_id(request_id, **kwargs)

        db = RaceDB()
        request_id = db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=LOSER,
        )
        if state != "none":
            db.record_canonical_release_id(
                request_id, canonical_release_id=SURVIVOR, resolved_at=OBSERVED,
            )
        if state == "replaced":
            db.supersede_request_mbid(
                request_id, new_mb_release_id=SECOND,
                new_mb_release_group_id=None, new_mb_artist_id=None,
                new_artist_name="Merged", new_album_title="Release",
                new_year=None, new_country=None, new_tracks=[],
            )
        before = dict(db.request(request_id))
        result = CanonicalReleaseService(
            db, now_fn=lambda: OBSERVED.replace(hour=2),
        ).retire_request(request_id)
        after = db.request(request_id)

        check_retirement_preserves_request(
            before, after, changed=result.outcome == OUTCOME_RETIRED,
        )

    @given(st.sampled_from((SURVIVOR, SECOND)))
    def test_lookup_non_answers_cannot_clear(self, survivor: str) -> None:
        db = FakePipelineDB()
        request_id = db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=LOSER,
        )
        db.record_canonical_release_id(
            request_id, canonical_release_id=survivor, resolved_at=OBSERVED,
        )
        before = db.request(request_id)["canonical_release_id"]
        CanonicalReleaseService(
            db, canonical_fn=lambda _id: None, now_fn=lambda: OBSERVED,
        ).reconcile_request(request_id)
        check_lookup_non_answer_preserves_canonical(
            before, db.request(request_id)["canonical_release_id"],
        )

    def test_known_bad_unconditional_clear_dies(self) -> None:
        with self.assertRaises(AssertionError):
            check_retirement_preserves_request(
                {"mb_release_id": LOSER, "status": "wanted", "current_evidence_id": None,
                 "canonical_release_id": SURVIVOR, "canonical_resolved_at": OBSERVED},
                {"mb_release_id": LOSER, "status": "wanted", "current_evidence_id": None,
                 "canonical_release_id": None, "canonical_resolved_at": None},
                changed=False,
            )

    def test_known_bad_lookup_clear_dies(self) -> None:
        with self.assertRaises(AssertionError):
            check_lookup_non_answer_preserves_canonical(SURVIVOR, None)
