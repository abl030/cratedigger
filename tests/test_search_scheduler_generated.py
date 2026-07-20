"""Generated scheduler-capacity invariants for issue #768.

The deterministic PostgreSQL pins live in
``tests.test_pipeline_db.TestGetWantedSearchable``.  This module patrols the
same contract over generated cohort sizes, ages, eligibility states, attempt
counts, and page sizes through ``FakePipelineDB``'s production-parity method.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Literal

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.pipeline_db import SearchPlanItemInput
from lib.search_scheduler import search_cohort_slots
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


NOW = datetime(2026, 7, 20, 4, 0, tzinfo=timezone.utc)
NEW_RESERVED_DIVISOR = 4
EligibilityState = Literal[
    "eligible",
    "backoff",
    "downloading",
    "no_plan",
    "wrong_generator",
    "youtube_running",
    "blacklisted",
]


@dataclass(frozen=True)
class SchedulerWorld:
    request_id: int
    age_seconds: int
    state: EligibilityState
    attempts: int

    @property
    def is_new(self) -> bool:
        return self.age_seconds < 24 * 60 * 60

    @property
    def is_eligible(self) -> bool:
        return self.state == "eligible"


@st.composite
def scheduler_worlds(draw: st.DrawFn) -> tuple[SchedulerWorld, ...]:
    row_count = draw(st.integers(min_value=0, max_value=45))
    rows: list[SchedulerWorld] = []
    for index in range(row_count):
        is_new = draw(st.booleans())
        age_seconds = draw(
            st.integers(min_value=0, max_value=(24 * 60 * 60) - 1)
            if is_new
            else st.integers(min_value=24 * 60 * 60, max_value=7 * 24 * 60 * 60)
        )
        rows.append(SchedulerWorld(
            request_id=index + 1,
            age_seconds=age_seconds,
            state=draw(st.sampled_from((
                "eligible",
                "backoff",
                "downloading",
                "no_plan",
                "wrong_generator",
                "youtube_running",
                "blacklisted",
            ))),
            attempts=draw(st.integers(min_value=0, max_value=8)),
        ))
    return tuple(rows)


def assert_scheduler_selection_invariants(
    worlds: tuple[SchedulerWorld, ...],
    selected_ids: tuple[int, ...],
    *,
    page_size: int,
) -> None:
    """Assert exact eligibility, capacity, borrowing, and cohort floors."""
    eligible = {world.request_id for world in worlds if world.is_eligible}
    eligible_new = {
        world.request_id
        for world in worlds
        if world.is_eligible and world.is_new
    }
    eligible_established = eligible - eligible_new
    selected = set(selected_ids)

    assert len(selected_ids) == len(selected), "scheduler returned duplicates"
    assert selected <= eligible, "scheduler returned an ineligible request"
    assert len(selected) == min(page_size, len(eligible)), (
        "scheduler left an eligible slot idle")

    new_slots = min(
        page_size,
        max(1, page_size // NEW_RESERVED_DIVISOR),
    )
    established_slots = max(page_size - new_slots, 0)
    selected_new = selected & eligible_new
    selected_established = selected & eligible_established

    assert len(selected_new) >= min(len(eligible_new), new_slots), (
        "new-request reserved capacity was not filled")
    assert len(selected_established) >= min(
        len(eligible_established), established_slots), (
        "established-request floor was not filled")

    if len(eligible_established) >= established_slots:
        assert len(selected_new) <= new_slots, (
            "new requests exceeded their reservation while established work "
            "was available")
    if len(eligible_new) <= new_slots:
        assert eligible_new <= selected, (
            "a low-volume new request missed its first eligible page")


def _run_world(
    worlds: tuple[SchedulerWorld, ...],
    *,
    page_size: int,
) -> tuple[int, ...]:
    db = FakePipelineDB()
    for world in worlds:
        status = "downloading" if world.state == "downloading" else "wanted"
        next_retry_after = (
            NOW + timedelta(minutes=1)
            if world.state == "backoff"
            else None
        )
        title = (
            f"Blocked request {world.request_id}"
            if world.state == "blacklisted"
            else f"Album {world.request_id}"
        )
        db.seed_request(make_request_row(
            id=world.request_id,
            mb_release_id=f"scheduler-{world.request_id}",
            album_title=title,
            status=status,
            created_at=NOW - timedelta(seconds=world.age_seconds),
            next_retry_after=next_retry_after,
            search_attempts=world.attempts,
            download_attempts=world.attempts,
            validation_attempts=world.attempts,
        ))
        if world.state != "no_plan":
            generator_id = (
                "old-generator"
                if world.state == "wrong_generator"
                else "current-generator"
            )
            db.create_successful_search_plan(
                request_id=world.request_id,
                generator_id=generator_id,
                items=[SearchPlanItemInput(
                    ordinal=0,
                    strategy="default",
                    query=f"query-{world.request_id}",
                )],
            )
        if world.state == "youtube_running":
            db.insert_youtube_running(
                request_id=world.request_id,
                browse_id=f"browse-{world.request_id}",
                audio_playlist_id=None,
                yt_url=f"https://example.invalid/{world.request_id}",
                expected_track_count=1,
            )

    return tuple(
        int(row["id"])
        for row in db.get_wanted_searchable(
            "current-generator",
            limit=page_size,
            title_blacklist=("blocked",),
            now=NOW,
        )
    )


class TestSchedulerInvariantCheckerKnownBad(unittest.TestCase):
    def test_scheduler_rejects_page_sizes_that_cannot_preserve_both_cohorts(
        self,
    ) -> None:
        for page_size in (-1, 0, 1):
            with self.subTest(page_size=page_size):
                with self.assertRaisesRegex(ValueError, "at least 2"):
                    search_cohort_slots(page_size)

    def test_checker_rejects_new_cohort_monopoly(self) -> None:
        worlds = tuple(
            SchedulerWorld(
                request_id=index,
                age_seconds=60 if index <= 10 else 48 * 60 * 60,
                state="eligible",
                attempts=1,
            )
            for index in range(1, 31)
        )
        bad_selection = tuple(range(1, 6)) + tuple(range(11, 22))

        with self.assertRaises(AssertionError):
            assert_scheduler_selection_invariants(
                worlds, bad_selection, page_size=16)


class TestGeneratedSearchScheduler(unittest.TestCase):
    @given(
        worlds=scheduler_worlds(),
        page_size=st.integers(min_value=2, max_value=32),
    )
    def test_capacity_and_eligibility_contract(
        self,
        worlds: tuple[SchedulerWorld, ...],
        page_size: int,
    ) -> None:
        selected_ids = _run_world(worlds, page_size=page_size)

        assert_scheduler_selection_invariants(
            worlds, selected_ids, page_size=page_size)

        changed_attempts = tuple(
            replace(world, attempts=8 - world.attempts)
            for world in worlds
        )
        self.assertEqual(
            _run_world(changed_attempts, page_size=page_size),
            selected_ids,
            "attempt counters changed scheduler cohort membership or order",
        )


if __name__ == "__main__":
    unittest.main()
