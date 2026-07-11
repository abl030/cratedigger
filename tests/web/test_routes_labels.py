#!/usr/bin/env python3
"""Contract tests for web/routes/labels.py.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import os
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.fakes import FakeBeetsDB
from tests.web._harness import _assert_required_fields, _WebServerCase



class TestLabelRouteContracts(_WebServerCase):
    """Contract tests for the Discogs label routes (Phase A)."""

    LABEL_HIT_REQUIRED_FIELDS = {
        "source", "id", "name", "country", "profile",
        "parent_label_id", "parent_label_name", "release_count",
    }
    # Required fields the frontend reads on each release row in the
    # label-detail response. Mirrors `web/js/discography.js` and
    # `web/js/badges.js`. The overlay sets `library_format` /
    # `library_min_bitrate` / `library_avg_bitrate` / `library_rank` only
    # when a row is in the beets library — same convention as the existing
    # `DISCOGS_MASTER_RELEASE_REQUIRED_FIELDS`. The JS reads them
    # defensively (`item.library_format || ''`), so the contract
    # asserts only the always-present overlay fields here, plus the
    # label-specific `sub_label_name`. The integration test below
    # exercises the populated path explicitly.
    LABEL_RELEASE_REQUIRED_FIELDS = {
        "id", "title", "artist_name", "date", "format", "primary_type",
        "sub_label_name", "in_library", "beets_album_id",
        "pipeline_status", "pipeline_id",
    }
    LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS = {
        "label", "releases", "sub_labels", "pagination", "include_sublabels",
        "sub_labels_dropped",
    }

    def _make_label_entity(self, **overrides):
        """Build a `LabelEntity` with sensible defaults for tests."""
        from web.discogs import LabelEntity
        defaults = {
            "source": "discogs",
            "id": "757",
            "name": "Hymen Records",
            "country": None,
            "profile": "Industrial / IDM label",
            "parent_label_id": None,
            "parent_label_name": None,
            "release_count": 42,
        }
        defaults.update(overrides)
        return LabelEntity(**defaults)

    def _make_release_row(self, **overrides):
        """Build a release row matching `get_label_releases` adapter shape."""
        row = {
            "id": "1001",
            "title": "Roniwasp",
            "country": "Germany",
            "date": "2002-01-01",
            "year": 2002,
            "primary_type": "Album",
            "release_group_id": None,
            "master_title": None,
            "master_first_released": None,
            "artist_name": "Gridlock",
            "artist_id": "1234",
            "label_id": "757",
            "sub_label_name": None,
            "format": "CD",
            "media_count": 1,
            "labels": [],
            "formats": [],
        }
        row.update(overrides)
        return row

    def test_label_search_contract(self):
        """Search hits expose every disambiguation field the UI needs."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.search_labels.return_value = [
                self._make_label_entity(),
                self._make_label_entity(
                    id="999", name="Hymen Substream",
                    parent_label_id="757", parent_label_name="Hymen Records",
                    release_count=7),
            ]
            status, data = self._get("/api/discogs/label/search?q=hymen")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"results"}, "label search response")
        self.assertEqual(len(data["results"]), 2)
        for hit in data["results"]:
            _assert_required_fields(self, hit, self.LABEL_HIT_REQUIRED_FIELDS,
                                    "label search hit")
        self.assertEqual(data["results"][1]["parent_label_id"], "757")

    def test_label_search_missing_query(self):
        status, data = self._get("/api/discogs/label/search?q=")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_label_detail_contract(self):
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [self._make_release_row()],
                "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 1},
                "include_sublabels": True,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data,
                                self.LABEL_DETAIL_RESPONSE_REQUIRED_FIELDS,
                                "label detail response")
        _assert_required_fields(self, data["label"],
                                self.LABEL_HIT_REQUIRED_FIELDS,
                                "label detail entity")
        self.assertEqual(len(data["releases"]), 1)
        _assert_required_fields(self, data["releases"][0],
                                self.LABEL_RELEASE_REQUIRED_FIELDS,
                                "label release row")

    def test_label_detail_forwards_sub_labels(self):
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity(
                sub_labels=[
                    {"id": 25693, "name": "Hymen Substream", "release_count": 7},
                ]
            )
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        self.assertEqual(data["sub_labels"], [
            {"id": 25693, "name": "Hymen Substream", "release_count": 7},
        ])

    def test_label_detail_overlay_integration(self):
        """End-to-end overlay: with one release in library AND one in
        pipeline, both rows are correctly annotated. This is the test
        that proves the overlay actually runs — not just that helpers
        were called."""
        held_id = "1001"
        in_pipeline_id = "1002"
        beets_db = FakeBeetsDB()
        beets_db.set_album_ids_for_release(held_id, [17])
        beets_db.set_mbid_detail(
            held_id, {"beets_format": "FLAC", "beets_bitrate": 900,
                      "beets_avg_bitrate": 1100})

        def _compute_rank(fmt, br):
            return "lossless" if fmt == "FLAC" else "transparent"

        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library",
                      return_value={held_id}), \
                patch("web.server.check_pipeline",
                      return_value={in_pipeline_id: {"id": 99, "status": "wanted"}}), \
                patch("web.server._beets_db", return_value=beets_db), \
                patch("web.server.compute_library_rank",
                      side_effect=_compute_rank):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [
                    self._make_release_row(id=held_id, title="Roniwasp"),
                    self._make_release_row(
                        id=in_pipeline_id, title="Formless",
                        sub_label_name="Hymen Substream"),
                ],
                "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 2},
                "include_sublabels": True,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        held_row = next(r for r in data["releases"] if r["id"] == held_id)
        pipeline_row = next(r for r in data["releases"] if r["id"] == in_pipeline_id)

        # In-library row: overlay populated, pipeline empty
        self.assertTrue(held_row["in_library"])
        self.assertEqual(held_row["beets_album_id"], 17)
        self.assertEqual(held_row["library_format"], "FLAC")
        self.assertEqual(held_row["library_min_bitrate"], 900)
        self.assertEqual(held_row["library_avg_bitrate"], 1100)
        self.assertEqual(held_row["library_rank"], "lossless")
        self.assertIsNone(held_row["pipeline_status"])
        self.assertIsNone(held_row["pipeline_id"])

        # In-pipeline row: pipeline populated, library empty
        self.assertFalse(pipeline_row["in_library"])
        self.assertIsNone(pipeline_row["beets_album_id"])
        self.assertEqual(pipeline_row["pipeline_status"], "wanted")
        self.assertEqual(pipeline_row["pipeline_id"], 99)
        self.assertEqual(pipeline_row["sub_label_name"], "Hymen Substream")

    def test_label_detail_404(self):
        """Adapter raises HTTPError(404) → route returns 404 JSON, not 5xx."""
        from urllib.error import HTTPError
        from io import BytesIO

        def _raise_404(_label_id):
            raise HTTPError(
                "https://discogs.ablz.au/api/labels/99999999",
                404, "Not Found", hdrs=None, fp=BytesIO(b""),  # type: ignore[arg-type]
            )

        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.side_effect = _raise_404
            status, data = self._get("/api/discogs/label/99999999")

        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_label_detail_include_sublabels_param_forwarded(self):
        """`?include_sublabels=false` flows through to the adapter call."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": False,
            }
            status, _data = self._get("/api/discogs/label/757?include_sublabels=false")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=False, page=1, per_page=100)

    def test_label_detail_auto_flips_include_sublabels_for_big_labels(self):
        """Big label (release_count > BIG_LABEL_THRESHOLD) without an
        explicit `include_sublabels=` query param auto-flips to False so
        the recursive sub-label CTE never hits the upstream timeout."""
        big_entity = self._make_label_entity(
            id="1", name="Universal Music Group", release_count=5000)
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = big_entity
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": False,
            }
            status, _data = self._get("/api/discogs/label/1")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "1", include_sublabels=False, page=1, per_page=100)

    def test_label_detail_respects_explicit_include_sublabels_on_big_labels(self):
        """If the caller explicitly opts in via `?include_sublabels=true`,
        the auto-flip MUST NOT override their choice — even for big
        labels. This is the API consumer's escape hatch."""
        big_entity = self._make_label_entity(
            id="1", name="Universal Music Group", release_count=5000)
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = big_entity
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
            }
            status, _data = self._get("/api/discogs/label/1?include_sublabels=true")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "1", include_sublabels=True, page=1, per_page=100)

    def test_label_detail_does_not_auto_flip_small_labels(self):
        """Boutique labels (release_count <= threshold) keep the
        default `include_sublabels=True` even with no explicit param."""
        small_entity = self._make_label_entity(
            id="757", name="Hymen Records", release_count=42)
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = small_entity
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
            }
            status, _data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=True, page=1, per_page=100)

    def test_label_detail_rejects_malformed_include_sublabels(self):
        """`?include_sublabels=` must be one of true/false/1/0 (case-
        insensitive). Anything else → 400. Silently coercing typos
        masks frontend bugs and lets bots pollute caches."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get("/api/discogs/label/757?include_sublabels=yes")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        # And get_label_releases should NEVER be called when the param is bad.
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_accepts_truthy_synonyms(self):
        """`include_sublabels=1` and `=0` are valid spellings."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": False,
            }
            status, _data = self._get("/api/discogs/label/757?include_sublabels=0")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=False, page=1, per_page=100)

    def test_label_detail_releases_404_propagates(self):
        """If `get_label` succeeds but `get_label_releases` raises 404
        (label vanished mid-flight), surface 404 to the client — not a
        generic 500."""
        from urllib.error import HTTPError
        from io import BytesIO

        def _raise_404(_label_id, **_kwargs):
            raise HTTPError(
                "https://discogs.ablz.au/api/labels/757/releases",
                404, "Not Found", hdrs=None, fp=BytesIO(b""),  # type: ignore[arg-type]
            )

        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.side_effect = _raise_404
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_label_detail_forwards_pagination_params(self):
        """`?page=2&per_page=50` flows through to the adapter — Plan 003 U1."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 2, "per_page": 50, "pages": 3, "items": 120},
                "include_sublabels": True,
            }
            status, data = self._get(
                "/api/discogs/label/757?page=2&per_page=50")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=True, page=2, per_page=50)
        self.assertEqual(data["pagination"]["page"], 2)
        self.assertEqual(data["pagination"]["per_page"], 50)

    def test_label_detail_clamps_per_page(self):
        """`?per_page=500` clamps to the mirror's 100-row label-release max."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 0},
                "include_sublabels": True,
            }
            status, _data = self._get(
                "/api/discogs/label/757?per_page=500")

        self.assertEqual(status, 200)
        mock_dg.get_label_releases.assert_called_once_with(
            "757", include_sublabels=True, page=1, per_page=100)

    def test_label_detail_rejects_non_integer_page(self):
        """`?page=foo` returns 400 — silently coercing to 1 would mask
        frontend pagination bugs."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get(
                "/api/discogs/label/757?page=foo")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_rejects_zero_page(self):
        """`?page=0` returns 400 — pages are 1-indexed."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get(
                "/api/discogs/label/757?page=0")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_rejects_non_integer_per_page(self):
        """`?per_page=foo` returns 400."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get(
                "/api/discogs/label/757?per_page=foo")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_rejects_zero_per_page(self):
        """`?per_page=0` returns 400 — would otherwise cause divide-by-zero
        on the pages calculation."""
        with patch("web.routes.labels.discogs_api") as mock_dg:
            mock_dg.get_label.return_value = self._make_label_entity()
            status, data = self._get(
                "/api/discogs/label/757?per_page=0")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertFalse(mock_dg.get_label_releases.called)

    def test_label_detail_forwards_sub_labels_dropped(self):
        """Plan 002 U3: when the adapter signals a 503 fallback, the route
        forwards `sub_labels_dropped=True` so the UI can surface a banner."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 1, "items": 0},
                "include_sublabels": False,
                "sub_labels_dropped": True,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        self.assertTrue(data["sub_labels_dropped"])

    def test_label_detail_default_sub_labels_dropped_false(self):
        """Plan 002 U3: every label-detail response carries the field with
        default False so the contract is stable."""
        with patch("web.routes.labels.discogs_api") as mock_dg, \
                patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            mock_dg.get_label.return_value = self._make_label_entity()
            mock_dg.get_label_releases.return_value = {
                "results": [],
                "pagination": {"page": 1, "per_page": 100, "pages": 0, "items": 0},
                "include_sublabels": True,
                "sub_labels_dropped": False,
            }
            status, data = self._get("/api/discogs/label/757")

        self.assertEqual(status, 200)
        self.assertIn("sub_labels_dropped", data)
        self.assertFalse(data["sub_labels_dropped"])

if __name__ == "__main__":
    unittest.main()
