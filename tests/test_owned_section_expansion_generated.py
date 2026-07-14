"""Generated ownership/type expansion invariant for artist-page exceptions."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
TYPES = ("Album", "EP", "Single", "Other", None)
SECONDARY_TYPES = (
    (),
    ("Compilation",),
    ("Live",),
    ("Remix",),
    ("DJ-mix",),
    ("Demo",),
)
PROVENANCE = ("ordinary", "promo", "unofficial")


def _expected_section(row: dict[str, object]) -> str:
    qualifiers: list[object] = []
    for field in ("secondary_types", "format_qualifiers"):
        values = row.get(field)
        if isinstance(values, list):
            qualifiers.extend(values)
    if qualifiers:
        for value, section in (
            ("Compilation", "Compilations"),
            ("Live", "Live"),
            ("Remix", "Remixes"),
            ("DJ-mix", "DJ Mixes"),
            ("Demo", "Demos"),
        ):
            if value in qualifiers:
                return section
    primary_types = row.get("primary_types")
    if isinstance(primary_types, list):
        if "Album" in primary_types:
            return "Albums"
        if "EP" in primary_types:
            return "EPs"
        if "Single" in primary_types:
            return "Singles"
        return "Other"
    row_type = row.get("type")
    if not isinstance(row_type, str):
        row_type = None
    if row_type == "Album":
        return "Albums"
    if row_type == "EP":
        return "EPs"
    if row_type == "Single":
        return "Singles"
    return "Other"


def _real_owned_type_sections(rows: list[dict[str, object]]) -> list[str]:
    script = """
import { ownedTypeSections } from './web/js/artist_page.js';
let input = '';
for await (const chunk of process.stdin) input += chunk;
process.stdout.write(JSON.stringify(ownedTypeSections(JSON.parse(input))));
"""
    proc = subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=ROOT,
        input=json.dumps(rows),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(proc.stdout)


def _real_appearance_split(
    rows: list[dict[str, object]],
) -> dict[str, list[int]]:
    indexed = [{**row, "_index": index} for index, row in enumerate(rows)]
    script = """
import { partitionWorkRows } from './web/js/artist_page.js';
let input = '';
for await (const chunk of process.stdin) input += chunk;
const split = partitionWorkRows(JSON.parse(input));
process.stdout.write(JSON.stringify({
  mainline: [
    ...split.mainline,
    ...split.promoOnly,
    ...split.unofficialOnly,
    ...split.unknown,
  ].map(row => row._index).sort((a, b) => a - b),
  appearances: split.appearances.map(row => row._index),
}));
"""
    proc = subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=ROOT,
        input=json.dumps(indexed),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(proc.stdout)


def _real_provenance_partition(
    rows: list[dict[str, object]],
) -> dict[str, list[int]]:
    indexed = [{**row, "_index": index} for index, row in enumerate(rows)]
    script = """
import { partitionWorkRows } from './web/js/artist_page.js';
let input = '';
for await (const chunk of process.stdin) input += chunk;
const split = partitionWorkRows(JSON.parse(input));
process.stdout.write(JSON.stringify(Object.fromEntries(
  Object.entries(split).map(([key, values]) => [
    key, values.map(row => row._index),
  ]),
)));
"""
    proc = subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=ROOT,
        input=json.dumps(indexed),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(proc.stdout)


def _real_unpaired_html(rows: list[dict[str, object]]) -> str:
    renderable = [
        {"id": str(index), "title": f"Work {index}", **row}
        for index, row in enumerate(rows)
    ]
    script = """
import { renderUnpairedSourceSections } from './web/js/artist_page.js';
let input = '';
for await (const chunk of process.stdin) input += chunk;
process.stdout.write(renderUnpairedSourceSections(JSON.parse(input), [], {
  artistName: 'The Rolling Stones', source: 'discogs',
}));
"""
    proc = subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=ROOT,
        input=json.dumps(renderable),
        text=True,
        capture_output=True,
        check=True,
    )
    return proc.stdout


def assert_owned_type_contract(
    rows: list[dict[str, object]],
    actual: list[str],
) -> None:
    expected = sorted({
        _expected_section(row)
        for row in rows
        if row.get("in_library") is True
    })
    if sorted(actual) != expected:
        raise AssertionError(
            f"owned type expansion mismatch: expected={expected}, actual={actual}"
        )


def assert_appearance_partition(
    rows: list[dict[str, object]],
    actual: dict[str, list[int]],
) -> None:
    expected_appearances = [
        index for index, row in enumerate(rows)
        if row.get("is_appearance") is True
    ]
    expected_mainline = [
        index for index, row in enumerate(rows)
        if row.get("is_appearance") is not True
    ]
    if actual != {
        "mainline": expected_mainline,
        "appearances": expected_appearances,
    }:
        raise AssertionError("appearance provenance leaked across the partition")


def assert_provenance_partition(
    rows: list[dict[str, object]],
    actual: dict[str, list[int]],
) -> None:
    expected = {
        "mainline": [],
        "appearances": [],
        "promoOnly": [],
        "unofficialOnly": [],
        "unknown": [],
    }
    for index, row in enumerate(rows):
        if row.get("is_appearance") is True:
            expected["appearances"].append(index)
            continue
        raw_provenance = row.get("provenance")
        provenance = set(raw_provenance) if isinstance(raw_provenance, list) else set()
        if "ordinary" in provenance:
            expected["mainline"].append(index)
        elif "unofficial" in provenance:
            expected["unofficialOnly"].append(index)
        elif "promo" in provenance:
            expected["promoOnly"].append(index)
        else:
            expected["unknown"].append(index)
    if actual != expected:
        raise AssertionError("work provenance partition lost or reclassified a row")


def assert_unpaired_wording(html: str) -> None:
    if "Unpaired Discogs works" not in html or "Only on Discogs" in html:
        raise AssertionError("unpaired catalogue was presented as source-exclusive")


row_strategy = st.builds(
    lambda row_type, primary_types, secondary_types, format_qualifiers,
           provenance, in_library, pipeline_status, is_appearance: {
        "type": row_type,
        "primary_types": primary_types,
        "secondary_types": list(secondary_types),
        "format_qualifiers": list(format_qualifiers),
        "provenance": provenance,
        "identity_kind": "work",
        "in_library": in_library,
        "pipeline_status": pipeline_status,
        "is_appearance": is_appearance,
    },
    row_type=st.sampled_from(TYPES),
    primary_types=st.lists(
        st.sampled_from(("Album", "EP", "Single")),
        max_size=3,
        unique=True,
    ),
    secondary_types=st.sampled_from(SECONDARY_TYPES),
    format_qualifiers=st.sampled_from(SECONDARY_TYPES),
    provenance=st.lists(
        st.sampled_from(PROVENANCE), max_size=3, unique=True,
    ),
    in_library=st.one_of(st.none(), st.booleans()),
    pipeline_status=st.sampled_from([None, "wanted", "downloading", "imported"]),
    is_appearance=st.one_of(st.none(), st.booleans()),
)


class TestGeneratedOwnedSectionExpansion(unittest.TestCase):
    @given(rows=st.lists(row_strategy, min_size=0, max_size=16))
    @example(rows=[
        {
            "type": None,
            "primary_types": [],
            "secondary_types": [],
            "format_qualifiers": [],
            "in_library": True,
            "pipeline_status": "wanted",
        },
        {
            "type": "Album",
            "primary_types": ["Album"],
            "secondary_types": [],
            "format_qualifiers": [],
            "in_library": False,
            "pipeline_status": "wanted",
        },
    ])
    def test_only_types_with_owned_rows_auto_expand(
        self,
        rows: list[dict[str, object]],
    ) -> None:
        assert_owned_type_contract(rows, _real_owned_type_sections(rows))

    def test_checker_rejects_pipeline_only_expansion(self) -> None:
        rows = [{
            "type": "Album",
            "primary_types": ["Album"],
            "secondary_types": [],
            "format_qualifiers": [],
            "in_library": False,
            "pipeline_status": "wanted",
        }]
        with self.assertRaisesRegex(AssertionError, "owned type expansion"):
            assert_owned_type_contract(rows, ["Albums"])

    def test_checker_rejects_legacy_scalar_album_mutant(self) -> None:
        rows = [{
            "type": "Album",
            "primary_types": [],
            "secondary_types": [],
            "format_qualifiers": [],
            "in_library": True,
        }]
        with self.assertRaisesRegex(AssertionError, "owned type expansion"):
            assert_owned_type_contract(rows, ["Albums"])

    @given(rows=st.lists(row_strategy, min_size=0, max_size=16))
    @example(rows=[{
        "type": "Album",
        "primary_types": ["Album"],
        "secondary_types": ["Compilation"],
        "format_qualifiers": [],
        "in_library": False,
        "pipeline_status": None,
        "is_appearance": True,
    }])
    def test_appearance_provenance_never_leaks_into_mainline(
        self,
        rows: list[dict[str, object]],
    ) -> None:
        assert_appearance_partition(rows, _real_appearance_split(rows))

    def test_appearance_checker_rejects_flattened_compilation(self) -> None:
        rows: list[dict[str, object]] = [{"is_appearance": True}]
        with self.assertRaisesRegex(AssertionError, "appearance provenance"):
            assert_appearance_partition(rows, {
                "mainline": [0],
                "appearances": [],
            })

    @given(rows=st.lists(row_strategy, min_size=0, max_size=16))
    @example(rows=[{
        "type": "Album",
        "primary_types": [],
        "secondary_types": [],
        "format_qualifiers": [],
        "provenance": ["ordinary", "unofficial"],
        "is_appearance": False,
    }])
    def test_provenance_partition_is_total_and_mixed_stays_mainline(
        self, rows: list[dict[str, object]],
    ) -> None:
        assert_provenance_partition(rows, _real_provenance_partition(rows))

    def test_provenance_checker_rejects_flattened_exceptional_rows(self) -> None:
        rows: list[dict[str, object]] = [{
            "provenance": ["unofficial"], "is_appearance": False,
        }]
        with self.assertRaisesRegex(AssertionError, "provenance partition"):
            assert_provenance_partition(rows, {
                "mainline": [0],
                "appearances": [],
                "promoOnly": [],
                "unofficialOnly": [],
                "unknown": [],
            })

    @given(rows=st.lists(row_strategy, min_size=1, max_size=8))
    def test_other_source_heading_never_claims_exclusivity(
        self, rows: list[dict[str, object]],
    ) -> None:
        assert_unpaired_wording(_real_unpaired_html(rows))

    def test_wording_checker_rejects_only_on_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "source-exclusive"):
            assert_unpaired_wording("Only on Discogs")


if __name__ == "__main__":
    unittest.main()
