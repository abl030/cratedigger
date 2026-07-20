"""Search-page cohort allocation policy.

Eligibility remains owned by ``PipelineDB.get_wanted_searchable``.  This
module only centralizes how a finite eligible page is divided before unused
capacity is borrowed by the other cohort.
"""

from __future__ import annotations

from dataclasses import dataclass


NEW_REQUEST_PRIORITY_HOURS = 24
NEW_REQUEST_RESERVED_DIVISOR = 4


@dataclass(frozen=True)
class SearchCohortSlots:
    new: int
    established: int


def search_cohort_slots(page_size: int) -> SearchCohortSlots:
    """Return reserved slots for a bounded search page.

    Production's 16-row page therefore reserves 4 slots for requests younger
    than 24 hours and keeps a 12-slot established-request floor. Other page
    sizes reserve a floor-rounded quarter share, with at least one new slot.
    At least two slots are required so both cohorts retain capacity whenever
    both have eligible work.
    """
    if page_size < 2:
        raise ValueError("page_size must be at least 2")
    new_slots = min(
        page_size,
        max(1, page_size // NEW_REQUEST_RESERVED_DIVISOR),
    )
    return SearchCohortSlots(
        new=new_slots,
        established=page_size - new_slots,
    )
