#!/usr/bin/env python3
"""Generated contract for Discogs' bulk artist-catalogue consumer.

The mirror returns primary-credit and track-appearance rows separately. The
consumer must conserve every unique Discogs catalogue identity, keep master
and release namespaces distinct, let a primary credit shadow only the same
appearance identity, validate the complete wire boundary, and retain the
existing stable ``(first_release_date, id)`` ordering.
"""
from __future__ import annotations

import os
import sys
import unittest
from copy import deepcopy
from typing import Any
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec
import tests._hypothesis_profiles  # noqa: F401
from hypothesis import given
from hypothesis import strategies as st

from web.discogs import get_artist_releases


_PRIMARY_TYPES = ("Album", "EP", "Single")
_ROW_FIELDS = (
    "id",
    "title",
    "type",
    "primary_types",
    "first_release_date",
    "artist_credit",
    "primary_artist_id",
    "is_masterless",
)
_ENVELOPE_FIELDS = ("results", "total", "page", "per_page")


@st.composite
def _artist_rows(draw: st.DrawFn) -> dict[str, Any]:
    release_id = draw(st.integers(min_value=1, max_value=8))
    masterless = draw(st.booleans())
    return {
        "id": f"release-{release_id}" if masterless else release_id,
        "title": draw(st.text(max_size=20)),
        "type": draw(st.sampled_from((*_PRIMARY_TYPES, "Other"))),
        "primary_types": draw(st.lists(
            st.sampled_from(_PRIMARY_TYPES), max_size=3, unique=True,
        )),
        "first_release_date": draw(st.sampled_from(("", "1963", "2001-02-03"))),
        "artist_credit": draw(st.text(max_size=20)),
        "primary_artist_id": draw(st.one_of(
            st.none(), st.integers(min_value=1, max_value=8),
        )),
        "is_masterless": masterless,
    }


def _expected_row(raw: dict[str, Any], *, appearance: bool) -> dict[str, Any]:
    masterless = raw["is_masterless"]
    raw_id = raw["id"]
    bare_id = (
        raw_id.removeprefix("release-")
        if masterless and isinstance(raw_id, str)
        else str(raw_id)
    )
    row = {
        "id": bare_id,
        "title": raw["title"],
        "type": raw["type"],
        "primary_types": list(raw["primary_types"]),
        "secondary_types": [],
        "first_release_date": raw["first_release_date"],
        "artist_credit": raw["artist_credit"],
        "primary_artist_id": (
            str(raw["primary_artist_id"])
            if raw["primary_artist_id"] is not None
            else ""
        ),
        "is_appearance": appearance,
    }
    if masterless:
        row["is_masterless"] = True
        row["discogs_release_id"] = bare_id
    return row


def _identity(row: dict[str, Any]) -> tuple[str, str]:
    namespace = "release" if row.get("is_masterless") is True else "master"
    return namespace, row["id"]


def _expected_catalogue(
    primary: list[dict[str, Any]],
    appearances: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in primary:
        row = _expected_row(raw, appearance=False)
        selected.setdefault(_identity(row), row)
    for raw in appearances:
        row = _expected_row(raw, appearance=True)
        selected.setdefault(_identity(row), row)
    return sorted(
        selected.values(),
        key=lambda row: (row["first_release_date"] or "", row["id"]),
    )


def assert_catalogue_projection(
    expected: list[dict[str, Any]], actual: list[dict[str, Any]],
) -> None:
    """Check conservation, provenance, fields, and stable ordering together."""
    if actual != expected:
        raise AssertionError(f"catalogue projection drifted: {actual!r} != {expected!r}")


def _response(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "results": rows,
        "total": len(rows),
        "page": 1,
        "per_page": max(1, len(rows)),
    }


def _run_consumer(
    primary: list[dict[str, Any]], appearances: list[dict[str, Any]],
    *, primary_response: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    def get(url: str) -> dict[str, Any]:
        if url.endswith("/masters/all"):
            return primary_response if primary_response is not None else _response(primary)
        if url.endswith("/appearances"):
            return _response(appearances)
        raise AssertionError(f"unexpected legacy or fallback URL: {url}")

    with patch("web.discogs.DISCOGS_API_BASE", "https://mirror.test"), patch(
        "web.discogs._get", side_effect=get,
    ), patch(
        "web.discogs._cache.memoize_meta",
        side_effect=lambda _key, fetch: fetch(),
    ):
        return get_artist_releases(82730)


class TestGeneratedBulkCatalogue(unittest.TestCase):
    @given(
        primary=st.lists(_artist_rows(), max_size=8),
        appearances=st.lists(_artist_rows(), max_size=8),
    )
    def test_catalogue_is_conserved_and_stably_normalized(
        self,
        primary: list[dict[str, Any]],
        appearances: list[dict[str, Any]],
    ) -> None:
        expected = _expected_catalogue(primary, appearances)
        actual = _run_consumer(primary, appearances)
        assert_catalogue_projection(expected, actual)

    @given(row=_artist_rows(), missing=st.sampled_from(_ROW_FIELDS))
    def test_every_required_row_field_is_strict(
        self, row: dict[str, Any], missing: str,
    ) -> None:
        invalid = deepcopy(row)
        invalid.pop(missing)
        with self.assertRaises(msgspec.ValidationError):
            _run_consumer([invalid], [])

    @given(row=_artist_rows(), corrupt=st.sampled_from(_ROW_FIELDS))
    def test_every_row_field_rejects_wrong_types(
        self, row: dict[str, Any], corrupt: str,
    ) -> None:
        invalid = deepcopy(row)
        wrong: dict[str, Any] = {
            "id": [],
            "title": 7,
            "type": 7,
            "primary_types": [7],
            "first_release_date": 7,
            "artist_credit": 7,
            "primary_artist_id": "7",
            "is_masterless": "false",
        }
        invalid[corrupt] = wrong[corrupt]
        with self.assertRaises(msgspec.ValidationError):
            _run_consumer([invalid], [])

    @given(
        rows=st.lists(_artist_rows(), max_size=4),
        field=st.sampled_from(_ENVELOPE_FIELDS),
        remove=st.booleans(),
    )
    def test_bulk_envelope_is_complete_and_strict(
        self, rows: list[dict[str, Any]], field: str, remove: bool,
    ) -> None:
        invalid = _response(rows)
        if remove:
            invalid.pop(field)
        else:
            wrong: dict[str, Any] = {
                "results": {},
                "total": "0",
                "page": "1",
                "per_page": "100",
            }
            invalid[field] = wrong[field]
        with self.assertRaises(msgspec.ValidationError):
            _run_consumer([], [], primary_response=invalid)


class TestBulkCatalogueCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_a_dropped_identity(self) -> None:
        expected = _expected_catalogue(
            [{
                "id": 4,
                "title": "Master",
                "type": "Album",
                "primary_types": ["Album"],
                "first_release_date": "1964",
                "artist_credit": "Artist",
                "primary_artist_id": 1,
                "is_masterless": False,
            }],
            [],
        )
        with self.assertRaises(AssertionError):
            assert_catalogue_projection(expected, [])

    def test_checker_rejects_unstable_order(self) -> None:
        expected = _expected_catalogue(
            [
                {
                    "id": row_id,
                    "title": str(row_id),
                    "type": "Album",
                    "primary_types": ["Album"],
                    "first_release_date": date,
                    "artist_credit": "Artist",
                    "primary_artist_id": 1,
                    "is_masterless": False,
                }
                for row_id, date in ((2, "1964"), (1, "1963"))
            ],
            [],
        )
        with self.assertRaises(AssertionError):
            assert_catalogue_projection(expected, list(reversed(expected)))


if __name__ == "__main__":
    unittest.main()
