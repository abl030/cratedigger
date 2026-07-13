"""Generated contract for exact-search related-identity merging."""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from web.artist_search import merge_exact_artist_identities


def assert_identity_merge(
    merged: list[dict], exact_id: str, related_ids: list[str], limit: int,
) -> None:
    ids = [str(row["id"]) for row in merged]
    if not ids or ids[0] != exact_id:
        raise AssertionError("the exact artist did not remain first")
    if len(ids) != len(set(ids)):
        raise AssertionError("artist identities were not deduplicated")
    if len(ids) > limit:
        raise AssertionError("artist identity results exceeded the limit")
    expected_related = [rid for rid in related_ids if rid != exact_id]
    expected_related = list(dict.fromkeys(expected_related))
    available = max(0, limit - 1)
    expected_related = expected_related[:available]
    if ids[1:1 + len(expected_related)] != expected_related:
        raise AssertionError("related identities were not adjacent to the exact hit")


_ID = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd")),
    min_size=1,
    max_size=8,
)


class TestIdentityMergeProperties(unittest.TestCase):
    @given(
        other_ids=st.lists(_ID, unique=True, max_size=30),
        related_ids=st.lists(_ID, max_size=15),
        limit=st.integers(min_value=1, max_value=20),
    )
    @example(other_ids=["other"], related_ids=["related"], limit=20)
    @example(
        other_ids=["related", "other"],
        related_ids=["related", "related", "exact"],
        limit=20,
    )
    def test_exact_and_related_identity_contract(
        self, other_ids: list[str], related_ids: list[str], limit: int,
    ) -> None:
        exact_id = "exact"
        base = [
            {"id": value, "name": value, "disambiguation": "", "score": 1}
            for value in other_ids if value != exact_id
        ]
        base.insert(
            len(base) // 2,
            {"id": exact_id, "name": "Exact", "disambiguation": "", "score": 100},
        )
        related = [
            {"id": value, "name": value, "disambiguation": "", "score": 99}
            for value in related_ids
        ]

        merged = merge_exact_artist_identities(
            base, exact_id=exact_id, related=related, limit=limit,
        )

        assert_identity_merge(merged, exact_id, related_ids, limit)


class TestIdentityMergeCheckerTrips(unittest.TestCase):
    def test_checker_rejects_related_identity_after_unrelated_hit(self) -> None:
        bad = [{"id": "exact"}, {"id": "other"}, {"id": "related"}]
        with self.assertRaises(AssertionError):
            assert_identity_merge(bad, "exact", ["related"], 20)


if __name__ == "__main__":
    unittest.main()
