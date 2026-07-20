"""Generated simple-catalogue invariants for the unified artist page."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
ARTIST_ID = "artist-id"
PROVENANCE = ("ordinary", "promo", "unofficial")
STRUCTURAL_TYPES = ("Album", "EP", "Single")


def _run_artist_page(script_body: str, payload: object) -> object:
    script = f"""
import {{
  classifyArtistRows,
  composeCompareCatalogue,
  renderArtistSections,
}} from './web/js/artist_page.js';
import {{ classify as classifyType }} from './web/js/grouping.js';
let input = '';
for await (const chunk of process.stdin) input += chunk;
const payload = JSON.parse(input);
{script_body}
"""
    proc = subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=ROOT,
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(proc.stdout)


def _real_partition(rows: list[dict[str, object]]) -> dict[str, object]:
    indexed = [
        {
            "id": str(index),
            "title": f"Row {index}",
            "source": "mb",
            "identity_kind": "work",
            "artist_credit": "Artist",
            "primary_artist_id": ARTIST_ID,
            "first_release_date": "2000",
            **row,
            "_index": index,
        }
        for index, row in enumerate(rows)
    ]
    return _run_artist_page("""
const sections = classifyArtistRows({
  artistId: 'artist-id', artistName: 'Artist', releaseGroups: payload,
  ungroupedReleases: [], libraryAlbums: [],
});
const html = renderArtistSections(sections, {
  artistId: 'artist-id', artistName: 'Artist',
});
process.stdout.write(JSON.stringify({
  inLibrary: sections.inLibrary.map(row => row._index),
  missing: sections.missing.map(row => row._index),
  other: sections.otherReleases.map(row => row._index),
  html,
}));
""", indexed)  # type: ignore[return-value]


def _real_pair_projection(payload: dict[str, object]) -> dict[str, object]:
    return _run_artist_page("""
const rows = composeCompareCatalogue(payload.compare, payload.source);
const sections = classifyArtistRows({
  artistId: 'artist-id', artistName: 'Artist', releaseGroups: rows,
  ungroupedReleases: [], libraryAlbums: [],
});
const bucket = sections.inLibrary.length ? 'inLibrary'
  : sections.missing.length ? 'missing' : 'other';
process.stdout.write(JSON.stringify({
  ...rows[0], _bucket: bucket, _type_section: classifyType(rows[0]),
}));
""", payload)  # type: ignore[return-value]


def _real_composition(payload: dict[str, object]) -> list[dict[str, str]]:
    result = _run_artist_page("""
const rows = composeCompareCatalogue(payload.compare, payload.source);
const sections = classifyArtistRows({
  artistId: 'artist-id', artistName: 'Artist', releaseGroups: rows,
  ungroupedReleases: [], libraryAlbums: [],
});
const bucket = new Map([
  ...sections.inLibrary.map(row => [row, 'inLibrary']),
  ...sections.missing.map(row => [row, 'missing']),
  ...sections.otherReleases.map(row => [row, 'other']),
]);
process.stdout.write(JSON.stringify(rows.map(row => ({
  key: `${row.source}:${row.identity_kind}:${row.id}`,
  bucket: bucket.get(row),
}))));
""", payload)
    assert isinstance(result, list)
    return [value for value in result if isinstance(value, dict)]


def _real_rolling_collision(payload: dict[str, object]) -> str:
    result = _run_artist_page("""
const sections = classifyArtistRows({
  artistId: 'rolling', artistName: 'The Rolling Stones',
  releaseGroups: [payload.row], ungroupedReleases: [],
  libraryAlbums: [payload.library],
});
process.stdout.write(JSON.stringify(renderArtistSections(sections, {
  artistId: 'rolling', artistName: 'The Rolling Stones',
})));
""", payload)
    assert isinstance(result, str)
    return result


def assert_simple_partition(
    rows: list[dict[str, object]], actual: dict[str, object],
) -> None:
    expected = {"inLibrary": [], "missing": [], "other": []}
    for index, row in enumerate(rows):
        raw_provenance = row.get("provenance")
        provenance = set(
            raw_provenance if isinstance(raw_provenance, list) else []
        )
        if row.get("is_appearance") is True or "ordinary" not in provenance:
            expected["other"].append(index)
        elif row.get("in_library") is True:
            expected["inLibrary"].append(index)
        else:
            expected["missing"].append(index)
    observed = {
        key: actual[key] for key in ("inLibrary", "missing", "other")
    }
    if observed != expected:
        raise AssertionError(
            f"simple catalogue partition drifted: {observed=} {expected=}"
        )


def assert_simple_vocabulary(html: str) -> None:
    forbidden = (
        "Unpaired", "Ungrouped", "Appears on", "Appearances",
        "Promo-only", "Unofficial-only", "Unknown provenance",
    )
    present = [heading for heading in forbidden if heading in html]
    if present:
        raise AssertionError(f"storage topology leaked into headings: {present}")
    if 'id="catalogue-other-releases"' in html:
        tail = html.split('id="catalogue-other-releases"', 1)[1]
        outer = tail.split('<div class="type-body', 1)[1].split('>', 1)[0]
        if "open" in outer.split():
            raise AssertionError("Other releases started expanded")


def assert_selected_identity(
    row: dict[str, object], *, source: str, selected_id: str,
    selected_kind: str, selected_owned: bool,
) -> None:
    actual = (
        row.get("source"), row.get("id"), row.get("identity_kind"),
        row.get("in_library") is True,
    )
    expected = (source, selected_id, selected_kind, selected_owned)
    if actual != expected:
        raise AssertionError(
            f"selected catalogue identity drifted: {actual=} {expected=}"
        )


def assert_display_conservation(actual: list[str], expected: set[str]) -> None:
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise AssertionError(
            f"display identities duplicated or disappeared: {actual=} {expected=}"
        )


def _classify_display_evidence(
    primary: list[str], secondary: list[str], qualifiers: list[str],
) -> str:
    combined = [*secondary, *qualifiers]
    if "Compilation" in combined:
        return "Compilations"
    if "Live" in combined:
        return "Live"
    if "Remix" in combined:
        return "Remixes"
    if "DJ-mix" in combined:
        return "DJ Mixes"
    if "Demo" in combined:
        return "Demos"
    if "Album" in primary:
        return "Albums"
    if "EP" in primary:
        return "EPs"
    if "Single" in primary:
        return "Singles"
    return "Other"


def assert_mb_work_classification_precedence(
    row: dict[str, object], *,
    mb_primary: list[str], mb_secondary: list[str],
    discogs_primary: list[str], discogs_secondary: list[str],
    discogs_qualifiers: list[str],
) -> None:
    """Independent oracle: positive MB work type wins; otherwise use DG."""
    if mb_primary or mb_secondary:
        expected_primary = mb_primary
        expected_secondary = mb_secondary
        expected_qualifiers: list[str] = []
    else:
        expected_primary = discogs_primary
        expected_secondary = discogs_secondary
        expected_qualifiers = discogs_qualifiers
    expected = (
        expected_primary,
        expected_secondary,
        expected_qualifiers,
        _classify_display_evidence(
            expected_primary, expected_secondary, expected_qualifiers,
        ),
    )
    actual = (
        row.get("display_primary_types"),
        row.get("display_secondary_types"),
        row.get("display_format_qualifiers"),
        row.get("_type_section"),
    )
    if actual != expected:
        raise AssertionError(
            f"MB classification precedence drifted: {actual=} {expected=}"
        )


row_strategy = st.builds(
    lambda primary_type, provenance, in_library, is_appearance,
           secondary_type: {
        "type": primary_type,
        "primary_types": (
            [primary_type] if primary_type in STRUCTURAL_TYPES else []
        ),
        "secondary_types": [secondary_type] if secondary_type else [],
        "format_qualifiers": [],
        "provenance": provenance,
        "in_library": in_library,
        "is_appearance": is_appearance,
    },
    primary_type=st.sampled_from((*STRUCTURAL_TYPES, "Other")),
    provenance=st.lists(
        st.sampled_from(PROVENANCE), max_size=3, unique=True,
    ),
    in_library=st.one_of(st.none(), st.booleans()),
    is_appearance=st.booleans(),
    secondary_type=st.sampled_from(
        (None, "Compilation", "Live", "Remix", "DJ-mix", "Demo"),
    ),
)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_partition_checker_rejects_exceptional_mainline_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "partition drifted"):
            assert_simple_partition(
                [{"provenance": ["unofficial"], "is_appearance": False}],
                {"inLibrary": [], "missing": [0], "other": [], "html": ""},
            )

    def test_vocabulary_checker_rejects_ungrouped_heading(self) -> None:
        with self.assertRaisesRegex(AssertionError, "topology leaked"):
            assert_simple_vocabulary("Ungrouped Discogs releases")

    def test_identity_checker_rejects_counterpart_ownership_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "identity drifted"):
            assert_selected_identity(
                {
                    "source": "discogs", "id": "3938744",
                    "identity_kind": "release", "in_library": True,
                },
                source="discogs", selected_id="3938744",
                selected_kind="release", selected_owned=False,
            )

    def test_display_checker_rejects_duplicate_identity_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "duplicated or disappeared"):
            assert_display_conservation(
                ["mb:work:1", "mb:work:1"], {"mb:work:1"},
            )

    def test_precedence_checker_rejects_union_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "precedence drifted"):
            assert_mb_work_classification_precedence(
                {
                    "display_primary_types": ["Album"],
                    "display_secondary_types": [],
                    "display_format_qualifiers": ["Compilation"],
                    "_type_section": "Compilations",
                },
                mb_primary=["Album"], mb_secondary=[],
                discogs_primary=["Album"], discogs_secondary=[],
                discogs_qualifiers=["Compilation"],
            )


class TestGeneratedSimpleArtistCatalogue(unittest.TestCase):
    @given(rows=st.lists(row_strategy, max_size=16))
    @example(rows=[{
        "type": "Album", "primary_types": ["Album"],
        "secondary_types": ["Compilation"], "format_qualifiers": [],
        "provenance": ["unofficial"], "in_library": False,
        "is_appearance": False,
    }])
    def test_partition_is_total_and_page_vocabulary_stays_simple(
        self, rows: list[dict[str, object]],
    ) -> None:
        actual = _real_partition(rows)
        assert_simple_partition(rows, actual)
        html = actual["html"]
        assert isinstance(html, str)
        assert_simple_vocabulary(html)

    @given(
        source=st.sampled_from(("mb", "discogs")),
        mb_owned=st.booleans(),
        discogs_owned=st.booleans(),
        mb_provenance=st.lists(
            st.sampled_from(PROVENANCE), max_size=3, unique=True,
        ),
        discogs_provenance=st.lists(
            st.sampled_from(PROVENANCE), max_size=3, unique=True,
        ),
        mb_primary=st.lists(
            st.sampled_from(STRUCTURAL_TYPES), max_size=3, unique=True,
        ),
        discogs_primary=st.lists(
            st.sampled_from(STRUCTURAL_TYPES), max_size=3, unique=True,
        ),
        mb_secondary=st.lists(
            st.sampled_from(("Compilation", "Live")), max_size=2, unique=True,
        ),
        discogs_secondary=st.lists(
            st.sampled_from(("Compilation", "Live")), max_size=2, unique=True,
        ),
        mb_qualifiers=st.lists(
            st.sampled_from(("Remix", "DJ-mix", "Demo")), max_size=3,
            unique=True,
        ),
        discogs_qualifiers=st.lists(
            st.sampled_from(("Remix", "DJ-mix", "Demo")), max_size=3,
            unique=True,
        ),
    )
    @example(
        source="discogs", mb_owned=True, discogs_owned=False,
        mb_provenance=["ordinary"], discogs_provenance=["ordinary"],
        mb_primary=["Album"], discogs_primary=["Album"],
        mb_secondary=["Live"], discogs_secondary=[],
        mb_qualifiers=["Demo"], discogs_qualifiers=["Remix"],
    )
    def test_pair_projection_keeps_selected_exact_identity_and_source_evidence(
        self,
        source: str,
        mb_owned: bool,
        discogs_owned: bool,
        mb_provenance: list[str],
        discogs_provenance: list[str],
        mb_primary: list[str],
        discogs_primary: list[str],
        mb_secondary: list[str],
        discogs_secondary: list[str],
        mb_qualifiers: list[str],
        discogs_qualifiers: list[str],
    ) -> None:
        mb = {
            "id": "mb-rg", "title": "Shared", "source": "mb",
            "identity_kind": "work", "provenance": mb_provenance,
            "in_library": mb_owned,
            "primary_types": mb_primary, "secondary_types": mb_secondary,
            "format_qualifiers": mb_qualifiers,
        }
        discogs = {
            "id": "3938744", "title": "Shared", "source": "discogs",
            "identity_kind": "release", "provenance": discogs_provenance,
            "in_library": discogs_owned,
            "primary_types": discogs_primary,
            "secondary_types": discogs_secondary,
            "format_qualifiers": discogs_qualifiers,
        }
        row = _real_pair_projection({
            "source": source,
            "compare": {
                "both": [{"mb": mb, "discogs": discogs}],
                "mb_unpaired": [], "discogs_unpaired": [],
                "discogs_ungrouped_releases": [],
            },
        })
        selected = mb if source == "mb" else discogs
        counterpart = discogs if source == "mb" else mb
        assert_selected_identity(
            row,
            source=source,
            selected_id=str(selected["id"]),
            selected_kind=str(selected["identity_kind"]),
            selected_owned=bool(selected["in_library"]),
        )
        projected_provenance = row["display_provenance"]
        projected_counterpart = row["counterpart"]
        self.assertIsInstance(projected_provenance, list)
        self.assertIsInstance(projected_counterpart, dict)
        assert isinstance(projected_provenance, list)
        assert isinstance(projected_counterpart, dict)
        self.assertEqual(
            set(projected_provenance),
            set(mb_provenance) | set(discogs_provenance),
        )
        self.assertEqual(
            projected_counterpart["in_library"], counterpart["in_library"],
        )
        selected_primary = mb_primary if source == "mb" else discogs_primary
        selected_secondary = (
            mb_secondary if source == "mb" else discogs_secondary
        )
        selected_qualifiers = (
            mb_qualifiers if source == "mb" else discogs_qualifiers
        )
        self.assertEqual(row["primary_types"], selected_primary)
        self.assertEqual(row["secondary_types"], selected_secondary)
        self.assertEqual(row["format_qualifiers"], selected_qualifiers)
        assert_mb_work_classification_precedence(
            row,
            mb_primary=mb_primary, mb_secondary=mb_secondary,
            discogs_primary=discogs_primary,
            discogs_secondary=discogs_secondary,
            discogs_qualifiers=discogs_qualifiers,
        )
        display_provenance = set(mb_provenance) | set(discogs_provenance)
        expected_bucket = (
            "inLibrary" if selected["in_library"] and "ordinary" in display_provenance
            else "missing" if "ordinary" in display_provenance
            else "other"
        )
        self.assertEqual(row["_bucket"], expected_bucket)

    @given(
        source=st.sampled_from(("mb", "discogs")),
        pair_count=st.integers(min_value=0, max_value=8),
        mb_unpaired_count=st.integers(min_value=0, max_value=8),
        discogs_work_count=st.integers(min_value=0, max_value=8),
        discogs_release_count=st.integers(min_value=0, max_value=8),
    )
    @example(
        source="discogs", pair_count=1, mb_unpaired_count=0,
        discogs_work_count=0, discogs_release_count=0,
    )
    def test_composite_display_dedupes_pairs_and_conserves_unpaired_rows(
        self,
        source: str,
        pair_count: int,
        mb_unpaired_count: int,
        discogs_work_count: int,
        discogs_release_count: int,
    ) -> None:
        pairs = [{
            "mb": {
                "id": f"mb-pair-{index}", "source": "mb",
                "identity_kind": "work", "provenance": ["ordinary"],
            },
            "discogs": {
                "id": f"dg-pair-{index}", "source": "discogs",
                "identity_kind": "release", "provenance": ["ordinary"],
            },
        } for index in range(pair_count)]
        mb_unpaired = [{
            "id": f"mb-only-{index}", "source": "mb",
            "identity_kind": "work", "provenance": ["ordinary"],
        } for index in range(mb_unpaired_count)]
        discogs_unpaired = [{
            "id": f"dg-work-{index}", "source": "discogs",
            "identity_kind": "work", "provenance": ["ordinary"],
        } for index in range(discogs_work_count)]
        discogs_releases = [{
            "id": f"dg-release-{index}", "source": "discogs",
            "identity_kind": "release", "provenance": ["ordinary"],
        } for index in range(discogs_release_count)]
        actual = _real_composition({
            "source": source,
            "compare": {
                "both": pairs,
                "mb_unpaired": mb_unpaired,
                "discogs_unpaired": discogs_unpaired,
                "discogs_ungrouped_releases": discogs_releases,
            },
        })
        expected = {
            *(
                f"{source}:{'work' if source == 'mb' else 'release'}:"
                f"{'mb' if source == 'mb' else 'dg'}-pair-{index}"
                for index in range(pair_count)
            ),
            *(f"mb:work:mb-only-{index}" for index in range(mb_unpaired_count)),
            *(
                f"discogs:work:dg-work-{index}"
                for index in range(discogs_work_count)
            ),
            *(
                f"discogs:release:dg-release-{index}"
                for index in range(discogs_release_count)
            ),
        }
        actual_keys = [row["key"] for row in actual]
        assert_display_conservation(actual_keys, expected)
        unmatched_release_keys = {
            f"discogs:release:dg-release-{index}"
            for index in range(discogs_release_count)
        }
        self.assertEqual(
            {
                row["key"] for row in actual
                if row["key"] in unmatched_release_keys
                and row["bucket"] == "other"
            },
            unmatched_release_keys,
        )
        expected_other_source_works = (
            {
                f"discogs:work:dg-work-{index}"
                for index in range(discogs_work_count)
            }
            if source == "mb"
            else {
                f"mb:work:mb-only-{index}"
                for index in range(mb_unpaired_count)
            }
        )
        self.assertEqual(
            {
                row["key"] for row in actual
                if row["key"] in expected_other_source_works
                and row["bucket"] == "other"
            },
            expected_other_source_works,
        )

    @given(
        qualifier=st.sampled_from(("Compilation", "Live")),
        exact_owned=st.booleans(),
    )
    @example(qualifier="Compilation", exact_owned=False)
    def test_rolling_title_collision_never_fabricates_ownership_or_expansion(
        self, qualifier: str, exact_owned: bool,
    ) -> None:
        row_id = "owned-rg" if exact_owned else "collision-rg"
        html = _real_rolling_collision({
            "row": {
                "id": row_id,
                "title": "The Rolling Stones",
                "source": "mb", "identity_kind": "work",
                "primary_types": ["Album"],
                "secondary_types": [qualifier], "format_qualifiers": [],
                "provenance": ["unofficial"],
                "first_release_date": "1964",
                "artist_credit": "The Rolling Stones",
                "primary_artist_id": "rolling", "is_appearance": False,
                "in_library": exact_owned,
            },
            "library": {
                "id": 1, "album": "The Rolling Stones",
                "mb_releasegroupid": "owned-rg",
                "mb_albumid": "owned-release", "in_library": True,
            },
        })
        assert_simple_vocabulary(html)
        other_tail = html.split('id="catalogue-other-releases"', 1)[1]
        outer = other_tail.split('<div class="type-body', 1)[1].split('>', 1)[0]
        self.assertNotIn("open", outer.split())
        type_tail = html.split(f"{qualifier}s" if qualifier == "Compilation" else qualifier, 1)[1]
        type_body = type_tail.split('<div class="type-body', 1)[1].split('>', 1)[0]
        self.assertNotIn("open", type_body.split())
        row_header = html.split(f'data-rg-id="{row_id}"', 1)[1].split('</div>', 1)[0]
        self.assertEqual("in library" in row_header, exact_owned)


if __name__ == "__main__":
    unittest.main()
