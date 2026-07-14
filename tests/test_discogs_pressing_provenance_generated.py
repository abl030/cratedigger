"""Generated contract for Discogs child-pressing provenance display."""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from web.discogs import _status_from_formats


def _expected_status(*, promo: bool, unofficial: bool) -> str:
    if promo and unofficial:
        return "Bootleg / Promo"
    if unofficial:
        return "Bootleg"
    if promo:
        return "Promotion"
    return "Official"


def assert_pressing_status(expected: str, actual: str) -> None:
    if actual != expected:
        raise AssertionError(
            f"pressing provenance drifted: expected={expected!r}, actual={actual!r}"
        )


class TestDiscogsPressingProvenanceGenerated(unittest.TestCase):
    @given(
        promo=st.booleans(),
        unofficial=st.booleans(),
        as_string=st.booleans(),
        unrelated=st.lists(
            st.sampled_from(("Album", "LP", "Compilation", "Reissue")),
            max_size=4,
            unique=True,
        ),
    )
    @example(
        promo=True, unofficial=True, as_string=True,
        unrelated=["Album", "LP"],
    )
    def test_real_child_pressing_projection_matches_independent_oracle(
        self,
        promo: bool,
        unofficial: bool,
        as_string: bool,
        unrelated: list[str],
    ) -> None:
        descriptions = list(unrelated)
        if promo:
            descriptions.append("Promo")
        if unofficial:
            descriptions.append("Unofficial Release")
        wire_value: str | list[str] = (
            ", ".join(descriptions) if as_string else descriptions
        )
        actual = _status_from_formats([{"descriptions": wire_value}])
        assert_pressing_status(
            _expected_status(promo=promo, unofficial=unofficial), actual,
        )

    def test_checker_rejects_hard_coded_official_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "provenance drifted"):
            assert_pressing_status("Bootleg / Promo", "Official")


if __name__ == "__main__":
    unittest.main()
