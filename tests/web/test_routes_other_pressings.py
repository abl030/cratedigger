"""Contract for ``GET /api/other-pressings/<download_log_id>`` (issue #1445).

Drives the real route and the real service over the fake pipeline DB; only
the two MusicBrainz reads in ``lib.mb_api`` are replaced, by the real
captured responses in ``tests/fixtures/other_pressings``.
"""

from __future__ import annotations

import email.message
import io
import json
import unittest
import urllib.error
from pathlib import Path
from typing import Any
from unittest.mock import patch

import msgspec

from lib.mb_api import MBDiscRelease, MBDiscReleasePage
from tests.helpers import make_request_row
from tests.web._harness import _FakeDbWebServerCase

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "other_pressings"


def _fixture(log_id: int) -> dict[str, Any]:
    return json.loads((_FIXTURES / f"{log_id}.json").read_text(encoding="utf-8"))


def _validation_result(fixture: dict[str, Any]) -> dict[str, object]:
    candidate = fixture["candidate"]
    return {
        "valid": True, "scenario": "strong_match",
        "candidates": [{
            "album_id": candidate["album_id"], "is_target": True,
            "mapping": candidate["mapping"],
            "extra_items": [], "extra_tracks": [],
        }],
    }


class _Mirror:
    def __init__(self, fixture: dict[str, Any]) -> None:
        self.fixture = fixture

    def release(self, release_id: str, *, timeout: float) -> MBDiscRelease:
        assert timeout > 0
        if release_id != self.fixture["target"]["id"]:
            raise urllib.error.HTTPError(
                "http://mirror", 404, "Not Found",
                hdrs=email.message.Message(), fp=io.BytesIO())
        return msgspec.convert(self.fixture["target"], type=MBDiscRelease)

    def page(
        self, rg_mbid: str, *, offset: int, limit: int, timeout: float,
    ) -> MBDiscReleasePage:
        pages = self.fixture["release_group_pages"]
        return msgspec.convert(pages[offset // limit], type=MBDiscReleasePage)


class TestOtherPressingsRoute(_FakeDbWebServerCase):
    REQUIRED_FIELDS = frozenset({
        "outcome", "download_log_id", "sentences", "target_release_id",
        "target_url", "release_group_id", "compared_with_pressings",
        "matching_pressings", "target_lengths", "releases_in_group",
        "releases_checked", "capped", "detail",
    })

    def _seed(self, log_id_fixture: int, request_id: int) -> int:
        fixture = _fixture(log_id_fixture)
        self.db.seed_request(make_request_row(id=request_id, status="imported"))
        return self.db.log_download(
            request_id, outcome="success",
            validation_result=_validation_result(fixture))

    def _get_with_mirror(
        self, path: str, fixture: dict[str, Any],
    ) -> tuple[int, Any]:
        mirror = _Mirror(fixture)
        with patch("lib.mb_api.get_release_with_discids",
                   side_effect=mirror.release), \
                patch("lib.mb_api.browse_release_group_with_discids",
                      side_effect=mirror.page):
            return self._get(path)

    def test_razorlight_answer_is_200_with_every_field(self) -> None:
        log_id = self._seed(28366, 960)
        status, data = self._get_with_mirror(
            f"/api/other-pressings/{log_id}", _fixture(28366))
        self.assertEqual(status, 200)
        self.assertEqual(self.REQUIRED_FIELDS - data.keys(), set())
        self.assertEqual(data["outcome"], "checked")
        self.assertEqual(
            [p["release_id"][:8] for p in data["matching_pressings"]],
            ["07039d4c", "d63463aa"])
        self.assertEqual(
            data["matching_pressings"][1]["title_differences"][0],
            {"medium": 1, "position": 12, "pressing_title": "To the Sea",
             "release_title": "Hang By, Hang By"})
        self.assertTrue(data["sentences"][0].startswith(
            "This copy matches 2 other pressings"))

    def test_unknown_row_is_404(self) -> None:
        status, data = self._get_with_mirror(
            "/api/other-pressings/987654", _fixture(28366))
        self.assertEqual(status, 404)
        self.assertEqual(data["outcome"], "download_log_not_found")

    def test_mirror_failure_is_503_with_a_sentence(self) -> None:
        log_id = self._seed(28366, 961)
        with patch("lib.mb_api.get_release_with_discids",
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
