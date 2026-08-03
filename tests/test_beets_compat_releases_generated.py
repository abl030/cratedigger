"""Generated invariant for the rolling Beets release selector."""

from __future__ import annotations

import datetime as dt
import importlib.util
import unittest
from pathlib import Path
from typing import Protocol

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - selects the repository profile

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "refresh_beets_compat_releases.py"
SPEC = importlib.util.spec_from_file_location("beets_compat_releases_generated", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
refresh = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(refresh)

AS_OF = dt.date(2026, 8, 3)


class GithubRelease(Protocol):
    tag_name: str
    published_at: str | None
    draft: bool
    prerelease: bool


@st.composite
def release_records(draw: st.DrawFn) -> list[GithubRelease]:
    count = draw(st.integers(min_value=1, max_value=12))
    records: list[GithubRelease] = []
    for index in range(count):
        offset = draw(st.integers(min_value=-5, max_value=WINDOW_DAYS + 5))
        records.append(refresh.GithubRelease(
            tag_name=f"v2.{index}.{draw(st.integers(min_value=0, max_value=9))}",
            published_at=f"{(AS_OF - dt.timedelta(days=offset)).isoformat()}T00:00:00Z",
            draft=draw(st.booleans()), prerelease=draw(st.booleans()),
        ))
    return records


WINDOW_DAYS = refresh.WINDOW_DAYS


def assert_selected_final_window(records: list[GithubRelease]) -> None:
    selected = refresh.select_final_releases(records, as_of=AS_OF)
    keys = [(record.published_at, record.tag_name) for record in selected]
    if keys != sorted(keys):
        raise AssertionError(f"selector order is not deterministic: {keys!r}")
    for record in selected:
        assert record.published_at is not None
        published = refresh.parse_utc_timestamp(record.published_at).date()
        if record.draft or record.prerelease or not (
            AS_OF - dt.timedelta(days=WINDOW_DAYS) <= published <= AS_OF
        ):
            raise AssertionError(f"selector admitted an ineligible release: {record!r}")


class TestGeneratedBeetsCompatibilitySelection(unittest.TestCase):
    @given(release_records())
    @example([
        refresh.GithubRelease(tag_name="v2.12.9", published_at="2024-08-02T00:00:00Z", draft=False, prerelease=False),
        refresh.GithubRelease(tag_name="v2.13.0", published_at="2024-08-04T00:00:00Z", draft=False, prerelease=False),
        refresh.GithubRelease(tag_name="v2.14.0rc1", published_at="2026-08-01T00:00:00Z", draft=False, prerelease=True),
    ])
    def test_selector_admits_only_ordered_final_window(
        self, records: list[GithubRelease],
    ) -> None:
        assert_selected_final_window(records)

    def test_known_bad_duplicate_selector_and_manifest_checker_trip(self) -> None:
        duplicate = [
            refresh.GithubRelease(tag_name="v2.13.1", published_at="2026-08-01T00:00:00Z", draft=False, prerelease=False),
            refresh.GithubRelease(tag_name="v2.13.1", published_at="2026-08-02T00:00:00Z", draft=False, prerelease=False),
        ]
        with self.assertRaisesRegex(ValueError, "duplicate qualifying"):
            refresh.select_final_releases(duplicate, as_of=AS_OF)
