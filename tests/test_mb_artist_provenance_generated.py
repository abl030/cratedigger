#!/usr/bin/env python3
"""Generated contract for MusicBrainz artist provenance projection."""
from __future__ import annotations

import os
import sys
import unittest
from typing import Any
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import given
from hypothesis import strategies as st

from web.mb import get_artist_release_groups


ARTIST_ID = "00000000-0000-0000-0000-000000000695"
OWN_RG = "00000000-0000-0000-0000-000000000696"
APPEARANCE_RG = "00000000-0000-0000-0000-000000000697"
STATUS_TO_PROVENANCE = {
    "Official": "ordinary",
    "Promotion": "promo",
    "Bootleg": "unofficial",
}
STATUSES = st.sampled_from((None, "", "Official", "Promotion", "Bootleg", "Pseudo-Release"))


def _rg(rg_id: str, title: str, primary_artist_id: str) -> dict[str, Any]:
    return {
        "id": rg_id,
        "title": title,
        "primary-type": "Album",
        "secondary-types": [],
        "first-release-date": "1964",
        "artist-credit": [{
            "name": "Artist",
            "artist": {"id": primary_artist_id, "name": "Artist"},
        }],
    }


def expected_provenance(statuses: list[str | None]) -> list[str]:
    return sorted({
        STATUS_TO_PROVENANCE[status]
        for status in statuses
        if status in STATUS_TO_PROVENANCE
    })


def assert_exact_provenance(expected: list[str], actual: list[str]) -> None:
    if actual != expected:
        raise AssertionError(f"provenance drifted: {actual!r} != {expected!r}")


def _run_consumer(
    direct_statuses: list[str | None], track_statuses: list[str | None],
) -> dict[str, list[str]]:
    own_rg = _rg(OWN_RG, "Own work", ARTIST_ID)
    appearance_rg = _rg(APPEARANCE_RG, "Appears on", "various-artists")

    def get(url: str) -> dict[str, Any]:
        if "/release-group?artist=" in url:
            return {"release-group-count": 1, "release-groups": [own_rg]}
        if "/release?artist=" in url:
            return {
                "release-count": len(direct_statuses),
                "releases": [
                    {"status": status, "release-group": {"id": OWN_RG}}
                    for status in direct_statuses
                ],
            }
        if "/release?track_artist=" in url:
            return {
                "release-count": len(track_statuses),
                "releases": [
                    {"status": status, "release-group": appearance_rg}
                    for status in track_statuses
                ],
            }
        raise AssertionError(f"unexpected MusicBrainz URL: {url}")

    with patch("web.mb._get", side_effect=get), patch(
        "web.mb._cache.memoize_meta", side_effect=lambda _key, fetch: fetch(),
    ):
        rows = get_artist_release_groups(ARTIST_ID)
    return {row.id: list(row.provenance) for row in rows}


class TestGeneratedArtistProvenance(unittest.TestCase):
    @given(
        direct=st.lists(STATUSES, min_size=1, max_size=8),
        track=st.lists(STATUSES, min_size=1, max_size=8),
    )
    def test_direct_and_track_release_statuses_union_per_release_group(
        self,
        direct: list[str | None],
        track: list[str | None],
    ) -> None:
        actual = _run_consumer(direct, track)
        assert_exact_provenance(expected_provenance(direct), actual[OWN_RG])
        assert_exact_provenance(expected_provenance(track), actual[APPEARANCE_RG])


class TestArtistProvenanceKnownBad(unittest.TestCase):
    def test_checker_rejects_absence_inferred_as_unofficial(self) -> None:
        with self.assertRaisesRegex(AssertionError, "provenance drifted"):
            assert_exact_provenance([], ["unofficial"])

    def test_checker_rejects_direct_only_projection(self) -> None:
        expected = expected_provenance(["Official", "Promotion"])
        direct_only_mutant = expected_provenance(["Official"])
        with self.assertRaisesRegex(AssertionError, "provenance drifted"):
            assert_exact_provenance(expected, direct_only_mutant)


if __name__ == "__main__":
    unittest.main()
