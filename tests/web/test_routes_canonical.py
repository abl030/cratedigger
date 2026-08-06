"""Contract tests for web/routes/canonical.py (#1059).

Thin adapters over ``CanonicalReleaseService`` — the same service
``pipeline-cli canonical`` wraps. These assert the wire contract and the
outcome→status mapping only; every outcome branch is covered
authoritatively in ``tests/test_canonical_release_service.py``.

The MusicBrainz lookup is the external HTTP edge and the only seam
replaced here.
"""
import os
import sys
import unittest
from typing import ClassVar
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase

LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"


def _no_redirect(_release_id: str) -> str | None:
    return None


def _redirects_to_survivor(release_id: str) -> str | None:
    return SURVIVOR if release_id == LOSER else None


class TestCanonicalRouteContracts(_FakeDbWebServerCase):
    """``GET /api/canonical`` and ``POST /api/canonical/reconcile``."""

    SHOW_REQUIRED_FIELDS: ClassVar[set[str]] = {
        "request_id",
        "status",
        "mb_release_id",
        "discogs_release_id",
        "canonical_release_id",
        "canonical_resolved_at",
    }
    RECONCILE_REQUIRED_FIELDS: ClassVar[set[str]] = {
        "request_id",
        "outcome",
        "acquisition_release_id",
        "canonical_release_id",
        "previous_canonical_release_id",
        "changed",
    }
    SWEEP_REQUIRED_FIELDS: ClassVar[set[str]] = {
        "scanned",
        "changed",
        "outcome_counts",
        "resolved",
    }

    def _seed(self, *, mb: str | None = LOSER) -> int:
        return self.db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=mb,
        )

    def test_show_returns_every_frontend_consumed_field(self) -> None:
        request_id = self._seed()
        status, payload = self._get(f"/api/canonical?id={request_id}")
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, payload, self.SHOW_REQUIRED_FIELDS, "GET /api/canonical")
        self.assertEqual(payload["mb_release_id"], LOSER)
        self.assertIsNone(payload["canonical_release_id"])

    def test_show_requires_an_integer_id(self) -> None:
        status, _payload = self._get("/api/canonical?id=abc")
        self.assertEqual(status, 400)
        status, _payload = self._get("/api/canonical")
        self.assertEqual(status, 400)

    def test_show_unknown_request_is_404(self) -> None:
        status, _payload = self._get("/api/canonical?id=999999")
        self.assertEqual(status, 404)

    def test_reconcile_one_request_stores_the_survivor(self) -> None:
        request_id = self._seed()
        with patch(
            "web.routes.canonical.canonical_release_fn",
            _redirects_to_survivor,
        ):
            status, payload = self._post(
                "/api/canonical/reconcile", {"request_id": request_id})

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, payload, self.RECONCILE_REQUIRED_FIELDS,
            "POST /api/canonical/reconcile")
        self.assertEqual(payload["outcome"], "resolved")
        self.assertTrue(payload["changed"])
        self.assertEqual(
            self.db.request(request_id)["canonical_release_id"], SURVIVOR)

    def test_reconcile_unknown_request_is_404(self) -> None:
        with patch(
            "web.routes.canonical.canonical_release_fn",
            _no_redirect,
        ):
            status, payload = self._post(
                "/api/canonical/reconcile", {"request_id": 999999})
        self.assertEqual(status, 404)
        self.assertEqual(payload["outcome"], "not_found")

    def test_reconcile_unusable_identity_is_422(self) -> None:
        request_id = self._seed(mb="not-a-uuid")
        with patch(
            "web.routes.canonical.canonical_release_fn",
            _no_redirect,
        ):
            status, payload = self._post(
                "/api/canonical/reconcile", {"request_id": request_id})
        self.assertEqual(status, 422)
        self.assertEqual(payload["outcome"], "invalid_identity")

    def test_reconcile_superseded_request_is_409(self) -> None:
        request_id = self._seed()
        self.db.supersede_request_mbid(
            request_id,
            new_mb_release_id=SURVIVOR,
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Merged",
            new_album_title="Release",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        with patch(
            "web.routes.canonical.canonical_release_fn",
            _redirects_to_survivor,
        ):
            status, payload = self._post(
                "/api/canonical/reconcile", {"request_id": request_id})
        self.assertEqual(status, 409)
        self.assertEqual(payload["outcome"], "frozen")

    def test_sweep_without_a_request_id_covers_the_library(self) -> None:
        merged = self._seed()
        self._seed(mb=SURVIVOR)
        with patch(
            "web.routes.canonical.canonical_release_fn",
            _redirects_to_survivor,
        ):
            status, payload = self._post("/api/canonical/reconcile", {})

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, payload, self.SWEEP_REQUIRED_FIELDS,
            "POST /api/canonical/reconcile (sweep)")
        self.assertEqual(payload["scanned"], 2)
        self.assertEqual(payload["changed"], 1)
        self.assertEqual(payload["resolved"][0]["request_id"], merged)

if __name__ == "__main__":
    unittest.main()
