"""Contract for ``GET /api/other-pressings/<download_log_id>`` (issue #1445).

Drives the real route, the real service and the real ``lib.mb_api``
client functions over the fake pipeline DB; only the HTTP leaf
``lib.mb_api._get`` is replaced, answering from the real captured mirror
responses in ``tests/fixtures/other_pressings``.
"""

from __future__ import annotations

import unittest
import urllib.error
from unittest.mock import patch

from tests.helpers import make_request_row
from tests.other_pressings_fixtures import (
    RAZORLIGHT,
    load_fixture,
    mirror_get,
    validation_result,
)
from tests.web._harness import _FakeDbWebServerCase


class TestOtherPressingsRoute(_FakeDbWebServerCase):
    REQUIRED_FIELDS = frozenset({
        "outcome", "download_log_id", "sentences", "target_release_id",
        "target_url", "release_group_id", "compared_with_pressings",
        "matching_pressings", "target_lengths", "releases_in_group",
        "releases_checked", "capped", "detail",
    })

    def _seed(self, request_id: int) -> int:
        self.db.seed_request(make_request_row(id=request_id, status="imported"))
        return self.db.log_download(
            request_id, outcome="success",
            validation_result=validation_result(load_fixture(RAZORLIGHT)))

    def _get_with_mirror(self, path: str) -> tuple[int, dict[str, object]]:
        with patch("lib.mb_api._get",
                   side_effect=mirror_get(load_fixture(RAZORLIGHT))):
            return self._get(path)

    def test_razorlight_answer_is_200_with_every_field(self) -> None:
        log_id = self._seed(960)
        status, data = self._get_with_mirror(f"/api/other-pressings/{log_id}")
        self.assertEqual(status, 200)
        self.assertEqual(self.REQUIRED_FIELDS - data.keys(), set())
        self.assertEqual(data["outcome"], "checked")
        matches = data["matching_pressings"]
        assert isinstance(matches, list)
        self.assertEqual(
            [str(p["release_id"])[:8] for p in matches],
            ["07039d4c", "d63463aa"])
        self.assertEqual(
            matches[1]["title_differences"][0],
            {"medium": 1, "position": 12, "pressing_title": "To the Sea",
             "release_title": "Hang By, Hang By"})
        sentences = data["sentences"]
        assert isinstance(sentences, list)
        self.assertEqual(
            sentences[0],
            "This copy does not fit this release's listing on tracks 9, 12 "
            "and 13; for every track except track 9 the release gives no "
            "length of its own, so the recording's length was used.")
        self.assertTrue(str(sentences[1]).startswith(
            "This copy matches 2 other pressings"))

    def test_unknown_row_is_404(self) -> None:
        status, data = self._get_with_mirror("/api/other-pressings/987654")
        self.assertEqual(status, 404)
        self.assertEqual(data["outcome"], "download_log_not_found")

    def test_mirror_failure_is_503_with_a_sentence(self) -> None:
        log_id = self._seed(961)
        with patch("lib.mb_api._get",
                   side_effect=urllib.error.URLError("refused")):
            status, data = self._get(f"/api/other-pressings/{log_id}")
        self.assertEqual(status, 503)
        self.assertEqual(data["outcome"], "mirror_unavailable")
        self.assertEqual(data["sentences"], [
            ("Could not reach MusicBrainz, so other pressings were not "
             "checked. Try again later.")])

    def test_discogs_target_is_200_not_available(self) -> None:
        self.db.seed_request(make_request_row(id=962, status="imported"))
        log_id = self.db.log_download(
            962, outcome="success",
            validation_result={"candidates": [{
                "album_id": "4567890", "is_target": True, "mapping": []}]})
        status, data = self._get(f"/api/other-pressings/{log_id}")
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "discogs_release")


if __name__ == "__main__":
    unittest.main()
