"""Generated policy patrol for MB/Discogs artist-discography pairing."""

from __future__ import annotations

import unittest
from typing import Literal

import msgspec
from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz
from lib.artist_compare import CompareBuckets, merge_discographies
from web.discogs import (
    _DiscogsArtistMasterEntry,
    _DiscogsArtistMastersResponse,
)


StructuralType = Literal["Album", "EP", "Single"]
_STRUCTURAL_TYPES: tuple[StructuralType, ...] = ("Album", "EP", "Single")


def _mb(
    *,
    year: int | None,
    type_: str,
    appearance: bool,
    id_: str = "mb",
) -> dict:
    return {
        "id": id_,
        "title": "Shared Title",
        "first_release_date": str(year) if year is not None else "",
        "type": type_,
        "is_appearance": appearance,
    }


def _discogs(
    *,
    year: int | None,
    types: list[StructuralType],
    appearance: bool,
    id_: str = "discogs",
    scalar_type: str = "Other",
) -> dict:
    return {
        "id": id_,
        "title": "Shared Title",
        "first_release_date": str(year) if year is not None else "",
        "type": scalar_type,
        "primary_types": types,
        "is_appearance": appearance,
    }


def _mb_types(type_: str) -> frozenset[str]:
    return frozenset({type_}) if type_ in _STRUCTURAL_TYPES else frozenset()


def _dedupe_type_identity(
    structural_types: frozenset[str], scalar_type: str
) -> tuple[str, frozenset[str] | str]:
    if structural_types:
        return ("structural", structural_types)
    return ("scalar-fallback", scalar_type.lower())


def _pair_is_allowed(
    *,
    mb_year: int | None,
    discogs_year: int | None,
    mb_types: frozenset[str],
    discogs_types: frozenset[str],
    mb_appearance: bool,
    discogs_appearance: bool,
) -> bool:
    if mb_appearance != discogs_appearance:
        return False
    overlap = bool(mb_types & discogs_types)
    if mb_types and discogs_types and not overlap:
        return False
    if mb_year is None or discogs_year is None:
        return mb_year is None and discogs_year is None
    delta = abs(mb_year - discogs_year)
    if delta == 0:
        return True
    return delta == 1 and bool(mb_types) and bool(discogs_types) and overlap


def assert_single_pairing(
    result: CompareBuckets,
    *,
    expected_pair: bool,
) -> None:
    """A one-row world must either pair once or preserve both source rows."""
    if expected_pair:
        assert len(result.both) == 1
        assert result.mb_only == []
        assert result.discogs_only == []
    else:
        assert result.both == []
        assert len(result.mb_only) == 1
        assert len(result.discogs_only) == 1


def assert_partition_is_one_to_one(
    result: CompareBuckets,
    *,
    expected_mb_ids: set[str],
    expected_discogs_ids: set[str],
) -> None:
    """Every expected post-dedupe identity appears exactly once."""
    mb_ids = [pair["mb"]["id"] for pair in result.both]
    mb_ids.extend(row["id"] for row in result.mb_only)
    discogs_ids = [pair["discogs"]["id"] for pair in result.both]
    discogs_ids.extend(row["id"] for row in result.discogs_only)
    assert len(mb_ids) == len(set(mb_ids))
    assert len(discogs_ids) == len(set(discogs_ids))
    assert set(mb_ids) == expected_mb_ids
    assert set(discogs_ids) == expected_discogs_ids


def assert_wire_payload_rejected(payload: dict) -> None:
    """An invalid artist response must fail strict boundary validation."""
    try:
        msgspec.convert(payload, type=_DiscogsArtistMastersResponse)
    except msgspec.ValidationError:
        return
    raise AssertionError("invalid artist response crossed the wire boundary")


_STRUCTURAL_SET = st.sets(
    st.sampled_from(_STRUCTURAL_TYPES), min_size=0, max_size=3
).map(lambda values: sorted(values, key=_STRUCTURAL_TYPES.index))
_YEAR = st.one_of(st.none(), st.integers(min_value=1900, max_value=2100))
_MB_TYPE = st.sampled_from((*_STRUCTURAL_TYPES, "Other", ""))
_DISPLAY_TYPE = st.sampled_from(
    (*_STRUCTURAL_TYPES, "Other", "Compilation", "")
)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_single_pairing_checker_rejects_hidden_source_row(self) -> None:
        with self.assertRaises(AssertionError):
            assert_single_pairing(
                CompareBuckets(both=[], mb_only=[], discogs_only=[]),
                expected_pair=False,
            )

    def test_partition_checker_rejects_reused_discogs_identity(self) -> None:
        row = {"id": "d"}
        bad = CompareBuckets(
            both=[
                {"mb": {"id": "m1"}, "discogs": row},
                {"mb": {"id": "m2"}, "discogs": row},
            ],
            mb_only=[],
            discogs_only=[],
        )
        with self.assertRaises(AssertionError):
            assert_partition_is_one_to_one(
                bad,
                expected_mb_ids={"m1", "m2"},
                expected_discogs_ids={"d"},
            )

    def test_partition_checker_rejects_silently_dropped_identities(self) -> None:
        bad = CompareBuckets(
            both=[{"mb": {"id": "m1"}, "discogs": {"id": "d1"}}],
            mb_only=[],
            discogs_only=[],
        )
        with self.assertRaises(AssertionError):
            assert_partition_is_one_to_one(
                bad,
                expected_mb_ids={"m1", "m2"},
                expected_discogs_ids={"d1", "d2"},
            )

    def test_wire_checker_rejects_known_bad_checker_input(self) -> None:
        valid = {
            "results": [{
                "id": 1,
                "title": "Valid",
                "type": "Album",
                "primary_types": ["Album"],
                "first_release_date": "2000",
                "artist_credit": "Artist",
                "primary_artist_id": 1,
                "is_masterless": False,
            }],
            "total": 1,
            "page": 1,
            "per_page": 100,
        }
        with self.assertRaises(AssertionError):
            assert_wire_payload_rejected(valid)


class TestArtistCompareGenerated(unittest.TestCase):
    @example(
        mb_year=2000,
        discogs_year=2001,
        mb_type="Album",
        discogs_types=[],
        mb_appearance=False,
        discogs_appearance=False,
        misleading_scalar="Album",
    )
    @given(
        mb_year=_YEAR,
        discogs_year=_YEAR,
        mb_type=_MB_TYPE,
        discogs_types=_STRUCTURAL_SET,
        mb_appearance=st.booleans(),
        discogs_appearance=st.booleans(),
        misleading_scalar=st.sampled_from((*_STRUCTURAL_TYPES, "Other", "")),
    )
    def test_pairing_matches_independent_policy_oracle(
        self,
        mb_year: int | None,
        discogs_year: int | None,
        mb_type: str,
        discogs_types: list[StructuralType],
        mb_appearance: bool,
        discogs_appearance: bool,
        misleading_scalar: str,
    ) -> None:
        mb = _mb(year=mb_year, type_=mb_type, appearance=mb_appearance)
        discogs = _discogs(
            year=discogs_year,
            types=discogs_types,
            appearance=discogs_appearance,
            scalar_type=misleading_scalar,
        )
        result = merge_discographies([mb], [discogs])
        expected = _pair_is_allowed(
            mb_year=mb_year,
            discogs_year=discogs_year,
            mb_types=_mb_types(mb_type),
            discogs_types=frozenset(discogs_types),
            mb_appearance=mb_appearance,
            discogs_appearance=discogs_appearance,
        )
        assert_single_pairing(result, expected_pair=expected)
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids={"mb"},
            expected_discogs_ids={"discogs"},
        )

    @given(
        year=st.integers(min_value=1901, max_value=2099),
        type_=st.sampled_from(_STRUCTURAL_TYPES),
        reverse=st.booleans(),
    )
    def test_exact_year_candidate_always_beats_adjacent(
        self, year: int, type_: StructuralType, reverse: bool
    ) -> None:
        mb = _mb(year=year, type_=type_, appearance=False)
        exact = _discogs(
            year=year, types=[type_], appearance=False, id_="exact"
        )
        adjacent = _discogs(
            year=year + 1, types=[type_], appearance=False, id_="adjacent"
        )
        rows = [adjacent, exact] if reverse else [exact, adjacent]
        result = merge_discographies([mb], rows)
        assert result.both[0]["discogs"]["id"] == "exact"
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids={"mb"},
            expected_discogs_ids={"exact", "adjacent"},
        )

    @given(
        year=st.integers(min_value=1901, max_value=2099),
        type_=st.sampled_from(_STRUCTURAL_TYPES),
    )
    def test_exact_edge_precedes_earlier_mb_adjacent_edge(
        self, year: int, type_: StructuralType
    ) -> None:
        result = merge_discographies(
            [
                _mb(
                    year=year - 1,
                    type_=type_,
                    appearance=False,
                    id_="adjacent-mb",
                ),
                _mb(
                    year=year,
                    type_=type_,
                    appearance=False,
                    id_="exact-mb",
                ),
            ],
            [_discogs(year=year, types=[type_], appearance=False)],
        )
        assert result.both[0]["mb"]["id"] == "exact-mb"
        assert result.mb_only[0]["id"] == "adjacent-mb"
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids={"adjacent-mb", "exact-mb"},
            expected_discogs_ids={"discogs"},
        )

    @given(
        year=st.integers(min_value=1900, max_value=2100),
        type_=st.sampled_from(_STRUCTURAL_TYPES),
        reverse=st.booleans(),
    )
    def test_overlapping_evidence_beats_unknown_type_on_year_tie(
        self, year: int, type_: StructuralType, reverse: bool
    ) -> None:
        mb = _mb(year=year, type_=type_, appearance=False)
        overlapping = _discogs(
            year=year, types=[type_], appearance=False, id_="overlap"
        )
        unknown = _discogs(
            year=year, types=[], appearance=False, id_="unknown",
            scalar_type=type_,
        )
        rows = [unknown, overlapping] if reverse else [overlapping, unknown]
        result = merge_discographies([mb], rows)
        assert result.both[0]["discogs"]["id"] == "overlap"
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids={"mb"},
            expected_discogs_ids={"overlap", "unknown"},
        )

    @given(
        year=st.integers(min_value=1900, max_value=2100),
        reverse=st.booleans(),
    )
    def test_equal_candidate_rank_uses_stable_input_order(
        self, year: int, reverse: bool
    ) -> None:
        mb = _mb(year=year, type_="Album", appearance=False)
        first = _discogs(
            year=year,
            types=["Album", "EP"],
            appearance=False,
            id_="first",
        )
        second = _discogs(
            year=year,
            types=["Album", "Single"],
            appearance=False,
            id_="second",
        )
        rows = [second, first] if reverse else [first, second]
        result = merge_discographies([mb], rows)
        assert result.both[0]["discogs"]["id"] == rows[0]["id"]
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids={"mb"},
            expected_discogs_ids={"first", "second"},
        )

    @given(
        first_types=_STRUCTURAL_SET,
        second_types=_STRUCTURAL_SET,
        first_appearance=st.booleans(),
        second_appearance=st.booleans(),
        first_year=_YEAR,
        second_year=_YEAR,
        first_scalar=_DISPLAY_TYPE,
        second_scalar=_DISPLAY_TYPE,
    )
    def test_within_source_dedupe_respects_type_and_appearance_boundaries(
        self,
        first_types: list[StructuralType],
        second_types: list[StructuralType],
        first_appearance: bool,
        second_appearance: bool,
        first_year: int | None,
        second_year: int | None,
        first_scalar: str,
        second_scalar: str,
    ) -> None:
        rows = [
            _discogs(
                year=first_year,
                types=first_types,
                appearance=first_appearance,
                id_="first",
                scalar_type=first_scalar,
            ),
            _discogs(
                year=second_year,
                types=second_types,
                appearance=second_appearance,
                id_="second",
                scalar_type=second_scalar,
            ),
        ]
        result = merge_discographies([], rows)
        expected_count = (
            1
            if (
                first_year == second_year
                and first_types == second_types
                and (
                    bool(first_types)
                    or first_scalar.lower() == second_scalar.lower()
                )
                and first_appearance == second_appearance
            )
            else 2
        )
        assert len(result.discogs_only) == expected_count
        expected_discogs_ids = (
            {"first"} if expected_count == 1 else {"first", "second"}
        )
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids=set(),
            expected_discogs_ids=expected_discogs_ids,
        )

    @given(
        first_type=_DISPLAY_TYPE,
        second_type=_DISPLAY_TYPE,
        first_appearance=st.booleans(),
        second_appearance=st.booleans(),
        first_year=_YEAR,
        second_year=_YEAR,
    )
    def test_mb_within_source_dedupe_uses_recognized_type_and_provenance(
        self,
        first_type: str,
        second_type: str,
        first_appearance: bool,
        second_appearance: bool,
        first_year: int | None,
        second_year: int | None,
    ) -> None:
        rows = [
            _mb(
                year=first_year,
                type_=first_type,
                appearance=first_appearance,
                id_="first",
            ),
            _mb(
                year=second_year,
                type_=second_type,
                appearance=second_appearance,
                id_="second",
            ),
        ]
        result = merge_discographies(rows, [])
        expected_count = (
            1
            if (
                first_year == second_year
                and _dedupe_type_identity(
                    _mb_types(first_type), first_type
                ) == _dedupe_type_identity(
                    _mb_types(second_type), second_type
                )
                and first_appearance == second_appearance
            )
            else 2
        )
        assert len(result.mb_only) == expected_count
        expected_mb_ids = (
            {"first"} if expected_count == 1 else {"first", "second"}
        )
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids=expected_mb_ids,
            expected_discogs_ids=set(),
        )

    @given(
        invalid_element=st.one_of(
            st.integers(), st.none(), st.booleans(), st.lists(st.text(), max_size=2)
        )
    )
    def test_wrong_primary_type_element_never_crosses_boundary(
        self, invalid_element: object
    ) -> None:
        payload = {
            "results": [{
                "id": 1,
                "title": "Invalid",
                "type": "Album",
                "primary_types": [invalid_element],
                "first_release_date": "2000",
                "artist_credit": "Artist",
                "primary_artist_id": 1,
                "is_masterless": False,
            }],
            "total": 1,
            "page": 1,
            "per_page": 100,
        }
        assert_wire_payload_rejected(payload)

    @given(
        missing=st.sampled_from(
            (
                "id",
                "title",
                "type",
                "primary_types",
                "first_release_date",
                "artist_credit",
                "primary_artist_id",
                "is_masterless",
            )
        )
    )
    def test_missing_artist_row_field_never_crosses_boundary(
        self, missing: str
    ) -> None:
        row = {
            "id": 1,
            "title": "Invalid",
            "type": "Album",
            "primary_types": ["Album"],
            "first_release_date": "2000",
            "artist_credit": "Artist",
            "primary_artist_id": 1,
            "is_masterless": False,
        }
        del row[missing]
        assert_wire_payload_rejected({
            "results": [row],
            "total": 1,
            "page": 1,
            "per_page": 100,
        })

    def test_declared_wire_row_type_is_strict(self) -> None:
        self.assertTrue(issubclass(_DiscogsArtistMasterEntry, msgspec.Struct))


if __name__ == "__main__":
    unittest.main()
