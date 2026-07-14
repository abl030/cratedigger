"""Generated policy patrol for MB/Discogs artist-discography pairing."""

from __future__ import annotations

import unittest
from typing import Literal, cast

import msgspec
from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz
from lib.artist_catalogue import ArtistCataloguePair, ArtistCatalogueRow
from lib.artist_compare import (
    CompareBuckets,
    annotate_in_library,
    merge_discographies,
)
from web.discogs import (
    _DiscogsArtistMasterEntry,
    _DiscogsArtistMastersResponse,
)
from web.routes.browse import _apply_rg_pipeline_overlay
from web.mb import _normalize_artist_release_group


StructuralType = Literal["Album", "EP", "Single"]
Provenance = Literal["ordinary", "promo", "unofficial"]
_STRUCTURAL_TYPES: tuple[StructuralType, ...] = ("Album", "EP", "Single")
_PROVENANCE: tuple[Provenance, ...] = ("ordinary", "promo", "unofficial")


def _mb(
    *,
    year: int | None,
    type_: str,
    appearance: bool,
    id_: str = "mb",
    provenance: list[Provenance] | None = None,
) -> ArtistCatalogueRow:
    primary_types: list[StructuralType] = (
        [cast(StructuralType, type_)] if type_ in _STRUCTURAL_TYPES else []
    )
    return ArtistCatalogueRow(
        id=id_,
        title="Shared Title",
        first_release_date=str(year) if year is not None else "",
        type=type_,
        source="mb",
        identity_kind="work",
        primary_types=primary_types,
        secondary_types=[],
        format_qualifiers=[],
        provenance=provenance if provenance is not None else ["ordinary"],
        artist_credit="Artist",
        primary_artist_id="artist-id",
        is_appearance=appearance,
    )


def _discogs(
    *,
    year: int | None,
    types: list[StructuralType],
    appearance: bool,
    id_: str = "discogs",
    scalar_type: str = "Other",
    provenance: list[Provenance] | None = None,
) -> ArtistCatalogueRow:
    return ArtistCatalogueRow(
        id=id_,
        title="Shared Title",
        first_release_date=str(year) if year is not None else "",
        type=scalar_type,
        source="discogs",
        identity_kind="work",
        primary_types=types,
        secondary_types=[],
        format_qualifiers=[],
        provenance=provenance if provenance is not None else ["ordinary"],
        artist_credit="Artist",
        primary_artist_id="artist-id",
        is_appearance=appearance,
    )


def _mb_types(type_: str) -> frozenset[str]:
    return frozenset({type_}) if type_ in _STRUCTURAL_TYPES else frozenset()


def _pair_is_allowed(
    *,
    mb_year: int | None,
    discogs_year: int | None,
    mb_types: frozenset[str],
    discogs_types: frozenset[str],
    mb_appearance: bool,
    discogs_appearance: bool,
    mb_provenance: frozenset[str],
    discogs_provenance: frozenset[str],
) -> bool:
    if mb_appearance != discogs_appearance:
        return False
    if not mb_provenance or not discogs_provenance:
        return False
    if "ordinary" in mb_provenance or "ordinary" in discogs_provenance:
        if not (
            "ordinary" in mb_provenance
            and "ordinary" in discogs_provenance
        ):
            return False
    elif not (mb_provenance & discogs_provenance):
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
        assert result.mb_unpaired == []
        assert result.discogs_unpaired == []
        assert result.discogs_ungrouped_releases == []
    else:
        assert result.both == []
        assert len(result.mb_unpaired) == 1
        assert len(result.discogs_unpaired) == 1


def assert_partition_is_one_to_one(
    result: CompareBuckets,
    *,
    expected_mb_ids: set[str],
    expected_discogs_ids: set[str],
) -> None:
    """Every expected source identity appears exactly once."""
    mb_ids = [pair.mb.id for pair in result.both]
    mb_ids.extend(row.id for row in result.mb_unpaired)
    discogs_ids = [pair.discogs.id for pair in result.both]
    discogs_ids.extend(row.id for row in result.discogs_unpaired)
    discogs_ids.extend(row.id for row in result.discogs_ungrouped_releases)
    assert len(mb_ids) == len(set(mb_ids))
    assert len(discogs_ids) == len(set(discogs_ids))
    assert set(mb_ids) == expected_mb_ids
    assert set(discogs_ids) == expected_discogs_ids


def assert_release_units_never_enter_work_buckets(
    result: CompareBuckets,
    *, release_ids: set[str],
) -> None:
    """Every pressing identity stays in the explicit ungrouped bucket."""
    work_ids = {
        pair.discogs.id for pair in result.both
    } | {row.id for row in result.discogs_unpaired}
    ungrouped_ids = {
        row.id for row in result.discogs_ungrouped_releases
    }
    if work_ids & release_ids or ungrouped_ids != release_ids:
        raise AssertionError("release identity crossed into a work bucket")


def assert_wire_payload_rejected(payload: dict) -> None:
    """An invalid artist response must fail strict boundary validation."""
    try:
        msgspec.convert(payload, type=_DiscogsArtistMastersResponse)
    except msgspec.ValidationError:
        return
    raise AssertionError("invalid artist response crossed the wire boundary")


def assert_exact_ownership(
    rows: list[ArtistCatalogueRow], expected: list[bool],
) -> None:
    actual = [row.in_library is True for row in rows]
    if actual != expected:
        raise AssertionError(
            f"catalogue ownership drifted: expected={expected}, actual={actual}"
        )


def assert_pipeline_overlay(
    row: ArtistCatalogueRow, *, expected: bool,
) -> None:
    actual = row.pipeline_id == 42 and row.pipeline_status == "wanted"
    if actual != expected:
        raise AssertionError(
            f"pipeline overlay crossed identity namespace: "
            f"expected={expected}, actual={actual}"
        )


def assert_mb_normalized_row(
    row: ArtistCatalogueRow,
    *,
    upstream_type: str | None,
    upstream_title: str | None,
    upstream_date: str | None,
) -> None:
    expected_type = upstream_type or ""
    expected_structural = (
        [expected_type]
        if expected_type in _STRUCTURAL_TYPES
        else []
    )
    if (
        row.type != expected_type
        or row.title != (upstream_title or "")
        or row.first_release_date != (upstream_date or "")
        or row.primary_types != expected_structural
    ):
        raise AssertionError("nullable MB fields escaped normalization")


_STRUCTURAL_SET = st.sets(
    st.sampled_from(_STRUCTURAL_TYPES), min_size=0, max_size=3
).map(lambda values: sorted(values, key=_STRUCTURAL_TYPES.index))
_YEAR = st.one_of(st.none(), st.integers(min_value=1900, max_value=2100))
_MB_TYPE = st.sampled_from((*_STRUCTURAL_TYPES, "Other", ""))
_DISPLAY_TYPE = st.sampled_from(
    (*_STRUCTURAL_TYPES, "Other", "Compilation", "")
)
_PROVENANCE_SET = st.sets(
    st.sampled_from(_PROVENANCE), max_size=3,
).map(sorted)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_release_unit_checker_rejects_wrong_unit_admission(self) -> None:
        release = _discogs(
            year=2000, types=["Album"], appearance=False, id_="release-1",
        )
        release.identity_kind = "release"
        bad = CompareBuckets(
            both=[],
            mb_unpaired=[],
            discogs_unpaired=[release],
            discogs_ungrouped_releases=[],
        )
        with self.assertRaisesRegex(AssertionError, "release identity"):
            assert_release_units_never_enter_work_buckets(
                bad, release_ids={"release-1"}
            )

    def test_single_pairing_checker_rejects_hidden_source_row(self) -> None:
        with self.assertRaises(AssertionError):
            assert_single_pairing(
                CompareBuckets(
                    both=[], mb_unpaired=[], discogs_unpaired=[],
                    discogs_ungrouped_releases=[],
                ),
                expected_pair=False,
            )

    def test_partition_checker_rejects_reused_discogs_identity(self) -> None:
        row = _discogs(
            year=2000, types=["Album"], appearance=False, id_="d",
        )
        bad = CompareBuckets(
            both=[
                ArtistCataloguePair(
                    mb=_mb(
                        year=2000, type_="Album", appearance=False, id_="m1",
                    ),
                    discogs=row,
                ),
                ArtistCataloguePair(
                    mb=_mb(
                        year=2000, type_="Album", appearance=False, id_="m2",
                    ),
                    discogs=row,
                ),
            ],
            mb_unpaired=[],
            discogs_unpaired=[],
            discogs_ungrouped_releases=[],
        )
        with self.assertRaises(AssertionError):
            assert_partition_is_one_to_one(
                bad,
                expected_mb_ids={"m1", "m2"},
                expected_discogs_ids={"d"},
            )

    def test_partition_checker_rejects_silently_dropped_identities(self) -> None:
        bad = CompareBuckets(
            both=[ArtistCataloguePair(
                mb=_mb(
                    year=2000, type_="Album", appearance=False, id_="m1",
                ),
                discogs=_discogs(
                    year=2000, types=["Album"], appearance=False, id_="d1",
                ),
            )],
            mb_unpaired=[],
            discogs_unpaired=[],
            discogs_ungrouped_releases=[],
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
                "format_qualifiers": [],
                "provenance": ["ordinary"],
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

    def test_ownership_checker_rejects_title_fallback_mutant(self) -> None:
        row = _mb(
            year=1964, type_="Album", appearance=False, id_="wrong-rg",
        )
        row.in_library = True
        with self.assertRaisesRegex(AssertionError, "ownership drifted"):
            assert_exact_ownership([row], [False])

    def test_pipeline_checker_rejects_discogs_master_badge_mutant(self) -> None:
        row = _discogs(
            year=1964, types=["Album"], appearance=False, id_="122",
        )
        row.pipeline_status = "wanted"
        row.pipeline_id = 42
        with self.assertRaisesRegex(AssertionError, "identity namespace"):
            assert_pipeline_overlay(row, expected=False)

    def test_mb_normalizer_checker_rejects_null_scalar_mutant(self) -> None:
        row = _mb(
            year=1964, type_="Album", appearance=False, id_="mb-null",
        )
        row.type = cast(str, None)
        with self.assertRaisesRegex(AssertionError, "escaped normalization"):
            assert_mb_normalized_row(
                row,
                upstream_type=None,
                upstream_title="Title",
                upstream_date="1964",
            )


class TestArtistCompareGenerated(unittest.TestCase):
    @given(
        primary_type=st.one_of(st.none(), _DISPLAY_TYPE),
        title=st.one_of(st.none(), st.text(max_size=24)),
        date=st.one_of(st.none(), st.text(max_size=16)),
        secondary_types=st.one_of(
            st.none(), st.lists(st.text(max_size=16), max_size=4),
        ),
    )
    @example(
        primary_type=None, title="Unclassified Work", date=None,
        secondary_types=None,
    )
    def test_nullable_mb_fields_normalize_before_common_contract(
        self,
        primary_type: str | None,
        title: str | None,
        date: str | None,
        secondary_types: list[str] | None,
    ) -> None:
        row = _normalize_artist_release_group({
            "id": "rg-nullable",
            "title": title,
            "primary-type": primary_type,
            "secondary-types": secondary_types,
            "first-release-date": date,
            "artist-credit": [],
        }, is_appearance=False)

        assert_mb_normalized_row(
            row,
            upstream_type=primary_type,
            upstream_title=title,
            upstream_date=date,
        )
        self.assertEqual(row.secondary_types, secondary_types or [])

    @given(
        identity=st.sampled_from(("mb_work", "discogs_work", "discogs_release")),
        catalogue_id=st.text(min_size=1, max_size=24),
    )
    @example(identity="discogs_work", catalogue_id="122")
    def test_pipeline_overlay_uses_exact_identity_namespace(
        self, identity: str, catalogue_id: str,
    ) -> None:
        if identity == "mb_work":
            row = _mb(
                year=1964, type_="Album", appearance=False, id_=catalogue_id,
            )
            expected = True
        else:
            row = _discogs(
                year=1964, types=["Album"], appearance=False,
                id_=catalogue_id,
            )
            row.identity_kind = (
                "release" if identity == "discogs_release" else "work"
            )
            expected = identity == "discogs_release"

        hit = {"status": "wanted", "id": 42}
        _apply_rg_pipeline_overlay(
            [row], {catalogue_id: hit}, {catalogue_id: hit},
        )

        assert_pipeline_overlay(row, expected=expected)

    @given(
        releases=st.lists(
            st.tuples(
                st.integers(min_value=1, max_value=10000),
                st.sampled_from(_STRUCTURAL_TYPES),
                st.booleans(),
            ),
            unique_by=lambda value: value[0],
            max_size=20,
        )
    )
    def test_masterless_releases_are_conserved_outside_work_matching(
        self,
        releases: list[tuple[int, StructuralType, bool]],
    ) -> None:
        rows = []
        for release_id, type_, appearance in releases:
            row = _discogs(
                year=2000,
                types=[type_],
                appearance=appearance,
                id_=str(release_id),
                scalar_type=type_,
            )
            row.identity_kind = "release"
            row.provenance = ["ordinary"]
            rows.append(row)

        result = merge_discographies([], rows)

        assert_release_units_never_enter_work_buckets(
            result, release_ids={str(value[0]) for value in releases}
        )
    @example(
        mb_year=2000,
        discogs_year=2001,
        mb_type="Album",
        discogs_types=[],
        mb_appearance=False,
        discogs_appearance=False,
        misleading_scalar="Album",
        mb_provenance=["ordinary"],
        discogs_provenance=["unofficial"],
    )
    @given(
        mb_year=_YEAR,
        discogs_year=_YEAR,
        mb_type=_MB_TYPE,
        discogs_types=_STRUCTURAL_SET,
        mb_appearance=st.booleans(),
        discogs_appearance=st.booleans(),
        misleading_scalar=st.sampled_from((*_STRUCTURAL_TYPES, "Other", "")),
        mb_provenance=_PROVENANCE_SET,
        discogs_provenance=_PROVENANCE_SET,
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
        mb_provenance: list[Provenance],
        discogs_provenance: list[Provenance],
    ) -> None:
        mb = _mb(
            year=mb_year, type_=mb_type, appearance=mb_appearance,
            provenance=mb_provenance,
        )
        discogs = _discogs(
            year=discogs_year,
            types=discogs_types,
            appearance=discogs_appearance,
            scalar_type=misleading_scalar,
            provenance=discogs_provenance,
        )
        result = merge_discographies([mb], [discogs])
        expected = _pair_is_allowed(
            mb_year=mb_year,
            discogs_year=discogs_year,
            mb_types=_mb_types(mb_type),
            discogs_types=frozenset(discogs_types),
            mb_appearance=mb_appearance,
            discogs_appearance=discogs_appearance,
            mb_provenance=frozenset(mb_provenance),
            discogs_provenance=frozenset(discogs_provenance),
        )
        assert_single_pairing(result, expected_pair=expected)
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids={"mb"},
            expected_discogs_ids={"discogs"},
        )

    @given(
        title=st.text(min_size=1, max_size=24),
        source=st.sampled_from(("mb", "discogs_release", "discogs_master")),
    )
    @example(title="The Rolling Stones", source="mb")
    def test_title_collision_never_establishes_library_ownership(
        self, title: str, source: str,
    ) -> None:
        library = [{
            "album": title,
            "mb_releasegroupid": "owned-rg",
            "mb_albumid": "owned-release",
        }]
        if source == "mb":
            row = _mb(
                year=1964, type_="Album", appearance=False,
                id_="foreign-rg",
            )
            row.title = title
            annotate_in_library([row], [], library)
        else:
            row = _discogs(
                year=1964, types=["Album"], appearance=False,
                id_=(
                    "owned-release"
                    if source == "discogs_master" else "foreign-release"
                ),
            )
            row.title = title
            if source == "discogs_release":
                row.identity_kind = "release"
            annotate_in_library([], [row], library)
        assert_exact_ownership([row], [False])

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
        assert result.both[0].discogs.id == "exact"
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
        assert result.both[0].mb.id == "exact-mb"
        assert result.mb_unpaired[0].id == "adjacent-mb"
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
        assert result.both[0].discogs.id == "overlap"
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
        assert result.both[0].discogs.id == rows[0].id
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
    def test_distinct_discogs_work_ids_are_always_conserved(
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
        assert len(result.discogs_unpaired) == 2
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids=set(),
            expected_discogs_ids={"first", "second"},
        )

    @given(
        first_type=_DISPLAY_TYPE,
        second_type=_DISPLAY_TYPE,
        first_appearance=st.booleans(),
        second_appearance=st.booleans(),
        first_year=_YEAR,
        second_year=_YEAR,
    )
    def test_distinct_mb_work_ids_are_always_conserved(
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
        assert len(result.mb_unpaired) == 2
        assert_partition_is_one_to_one(
            result,
            expected_mb_ids={"first", "second"},
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
                "format_qualifiers": [],
                "provenance": ["ordinary"],
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
                "format_qualifiers",
                "provenance",
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
            "format_qualifiers": [],
            "provenance": ["ordinary"],
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
        self.assertTrue(issubclass(ArtistCatalogueRow, msgspec.Struct))


if __name__ == "__main__":
    unittest.main()
