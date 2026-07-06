#!/usr/bin/env python3
"""Contract tests for web/routes/beets_distance.py.

Split from tests/web/test_routes_pipeline.py (#522), which itself split
from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase


class TestBeetsDistanceRouteContract(_FakeDbWebServerCase):
    """Contract for ``GET /api/beets-distance/<download_log_id>/<mbid>``.

    Service-layer correctness is covered by ``tests.test_beets_distance``.
    Here we pin the HTTP wrapper: every ``BeetsDistanceResult.outcome``
    maps to the documented status code, every required response field
    is present, and the route is registered (the
    ``TestRouteContractAudit`` guard catches missing classification).

    The service function is patched at its import site
    (``web.routes.beets_distance.compute_beets_distance``-equivalent —
    actually imported lazily inside the handler so we patch the module
    attribute) and we drive each outcome through the wrapper. The real
    beets distance pipeline is not exercised in this class; the
    integration slice in ``tests.test_beets_distance`` is the
    authority on that.
    """

    REQUIRED_FIELDS = {
        "outcome",
        "distance",
        "matched_tracks",
        "total_local_tracks",
        "total_mb_tracks",
        "extra_local_tracks",
        "extra_mb_tracks",
        "components",
        "request_release_group_id",
        "candidate_release_group_id",
        "candidate_mbid",
        "download_log_id",
        "request_id",
        "folder_path",
        "error_message",
        "duration_ms",
    }

    UUID_A = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    UUID_B = "12345678-1234-1234-1234-123456789abc"

    def setUp(self) -> None:
        super().setUp()
        from lib.beets_distance import BeetsDistanceResult
        self._Result = BeetsDistanceResult

    def _patch_service(self, **kwargs):
        from unittest.mock import patch as _patch
        return _patch(
            "lib.beets_distance.compute_beets_distance",
            return_value=self._Result(**kwargs),
        )

    def test_ok_returns_200_with_distance_and_required_fields(self):
        with self._patch_service(
            outcome="ok",
            distance=0.07,
            matched_tracks=12,
            total_local_tracks=12,
            total_mb_tracks=12,
            extra_local_tracks=0,
            extra_mb_tracks=0,
            components={"album": 0.0, "artist": 0.0},
            request_release_group_id="rg-1",
            candidate_release_group_id="rg-1",
            candidate_mbid=self.UUID_A,
            download_log_id=100,
            request_id=7,
            folder_path="/tmp/x",
            duration_ms=8,
        ):
            status, data = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REQUIRED_FIELDS,
                                "beets-distance ok response")
        self.assertEqual(data["outcome"], "ok")
        self.assertAlmostEqual(data["distance"], 0.07, places=4)
        self.assertEqual(data["matched_tracks"], 12)

    def test_download_log_not_found_returns_404(self):
        with self._patch_service(
            outcome="download_log_not_found",
            download_log_id=999,
            candidate_mbid=self.UUID_A,
            error_message="download_log #999 not found",
        ):
            status, _ = self._get(f"/api/beets-distance/999/{self.UUID_A}")
        self.assertEqual(status, 404)

    def test_request_not_found_returns_404(self):
        with self._patch_service(
            outcome="request_not_found",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="request #7 not found",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 404)

    def test_wrong_release_group_returns_422(self):
        """The cross-RG guardrail surfaces as 422 (semantic violation)."""
        with self._patch_service(
            outcome="wrong_release_group",
            download_log_id=100,
            request_id=7,
            request_release_group_id="rg-source",
            candidate_release_group_id="rg-other",
            candidate_mbid=self.UUID_A,
            error_message="MBID is in a different release group",
        ):
            status, data = self._get(
                f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 422)
        self.assertEqual(data["outcome"], "wrong_release_group")

    def test_mb_no_release_group_returns_422(self):
        with self._patch_service(
            outcome="mb_no_release_group",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="MB release has no release_group_id",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 422)

    def test_folder_missing_returns_410(self):
        with self._patch_service(
            outcome="folder_missing",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="failed_path is gone",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 410)

    def test_no_audio_returns_410(self):
        with self._patch_service(
            outcome="no_audio",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            folder_path="/tmp/empty",
            error_message="no readable audio files",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 410)

    def test_mb_lookup_failed_returns_503(self):
        with self._patch_service(
            outcome="mb_lookup_failed",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="MB mirror unreachable",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 503)

    def test_distance_failed_returns_500(self):
        with self._patch_service(
            outcome="distance_failed",
            download_log_id=100,
            candidate_mbid=self.UUID_A,
            error_message="beets blew up",
        ):
            status, _ = self._get(f"/api/beets-distance/100/{self.UUID_A}")
        self.assertEqual(status, 500)

    def test_route_pattern_rejects_non_id_shapes(self):
        """The route pattern matches full MB UUIDs and bare Discogs
        numeric ids (#530) — a shape that is neither (partial UUID,
        non-numeric junk) doesn't even hit the handler.
        """
        status, _ = self._get("/api/beets-distance/100/not-a-real-id")
        self.assertEqual(status, 404)

    def test_discogs_numeric_id_routes_through_discogs_lookup(self):
        """A numeric candidate id (Discogs sibling — e.g. surfaced by the
        Replace picker against a Discogs-sourced request per #501) must
        resolve via ``discogs_api.get_release``, not ``mb_api.get_release``
        (#530). This mirrors ``browse.py``'s numeric-dispatch idiom and
        the YouTube resolver's ``discogs_get_release`` seam — no new
        MB<->Discogs adapter: ``compute_beets_distance`` already treats
        ``release_group_id`` as optional and ``discogs_api.get_release``
        mirrors ``mb_api.get_release``'s dict shape exactly.
        """
        captured = {}

        def _fake_compute(download_log_id, mbid, *, pdb, mb_get_release,
                           cache=None, **_kw):
            captured["mb_get_release"] = mb_get_release
            return self._Result(
                outcome="ok",
                distance=0.05,
                download_log_id=download_log_id,
                candidate_mbid=mbid,
            )

        discogs_release = {
            "id": "2048516",
            "title": "Fake Album",
            "artist_name": "Fake Artist",
            "artist_id": "999",
            "release_group_id": None,
            "tracks": [],
        }
        with patch(
            "lib.beets_distance.compute_beets_distance",
            side_effect=_fake_compute,
        ), patch(
            "web.discogs.get_release",
            return_value=discogs_release,
        ) as discogs_get:
            status, data = self._get("/api/beets-distance/100/2048516")
            self.assertIn("mb_get_release", captured)
            resolved = captured["mb_get_release"]("2048516")
            discogs_get.assert_called_once_with(2048516, fresh=False)

        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "ok")
        # The handler's mb_get_release seam must be wired to Discogs,
        # not MB, for a numeric candidate id.
        self.assertEqual(resolved, discogs_release)


if __name__ == "__main__":
    unittest.main()
