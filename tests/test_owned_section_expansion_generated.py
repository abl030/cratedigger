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


def _expected_section(row: dict[str, object]) -> str:
    secondary = row.get("secondary_types")
    if isinstance(secondary, list):
        for value, section in (
            ("Compilation", "Compilations"),
            ("Live", "Live"),
            ("Remix", "Remixes"),
            ("DJ-mix", "DJ Mixes"),
            ("Demo", "Demos"),
        ):
            if value in secondary:
                return section
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
import { splitAppearanceRows } from './web/js/artist_page.js';
let input = '';
for await (const chunk of process.stdin) input += chunk;
const split = splitAppearanceRows(JSON.parse(input));
process.stdout.write(JSON.stringify({
  mainline: split.mainline.map(row => row._index),
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


row_strategy = st.builds(
    lambda row_type, secondary_types, in_library, pipeline_status,
           is_appearance: {
        "type": row_type,
        "secondary_types": list(secondary_types),
        "in_library": in_library,
        "pipeline_status": pipeline_status,
        "is_appearance": is_appearance,
    },
    row_type=st.sampled_from(TYPES),
    secondary_types=st.sampled_from(SECONDARY_TYPES),
    in_library=st.one_of(st.none(), st.booleans()),
    pipeline_status=st.sampled_from([None, "wanted", "downloading", "imported"]),
    is_appearance=st.one_of(st.none(), st.booleans()),
)


class TestGeneratedOwnedSectionExpansion(unittest.TestCase):
    @given(rows=st.lists(row_strategy, min_size=0, max_size=16))
    @example(rows=[
        {
            "type": None,
            "secondary_types": [],
            "in_library": True,
            "pipeline_status": "wanted",
        },
        {
            "type": "Album",
            "secondary_types": [],
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
            "secondary_types": [],
            "in_library": False,
            "pipeline_status": "wanted",
        }]
        with self.assertRaisesRegex(AssertionError, "owned type expansion"):
            assert_owned_type_contract(rows, ["Albums"])

    @given(rows=st.lists(row_strategy, min_size=0, max_size=16))
    @example(rows=[{
        "type": "Album",
        "secondary_types": ["Compilation"],
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


if __name__ == "__main__":
    unittest.main()
