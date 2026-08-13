"""HTTP contract for GET /api/audit/retag-divergence (#1093 item 1)."""

from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from lib.beets_db import BeetsAlbumIdentityRow
from tests.fakes import FakeBeetsDB
from tests.web._harness import _FakeDbWebServerCase


class TestRetagDivergenceAuditRoute(_FakeDbWebServerCase):
    def test_reports_shared_service_payload(self) -> None:
        from web import server

        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=1,
                mb_albumid="7aabf975-9a06-4b2e-854c-2c700380ebd5",
                item_paths=("/nonexistent/library/Album/01.flac",),
            ),
        ])
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "divergence_found")
        self.assertTrue(payload["complete"])
        self.assertEqual(payload["counts"]["albums_scanned"], 1)
        self.assertEqual(len(payload["albums"]), 1)
        self.assertEqual(payload["albums"][0]["album_class"], "unreadable")
        self.assertEqual(beets.close_calls, 0)

    def test_clean_report_lists_no_albums(self) -> None:
        from web import server

        beets = FakeBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "clean")
        self.assertEqual(payload["albums"], [])

    def test_missing_beets_is_an_unavailable_report(self) -> None:
        from web import server

        with patch.object(server, "_beets_db", return_value=None):
            status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "beets_unavailable")
        self.assertFalse(payload["complete"])
        self.assertIsNotNone(payload["unavailable_detail"])

    def test_unexpected_failure_is_logged_and_returns_503(self) -> None:
        from web import server

        with (
            patch.object(
                server,
                "_beets_db",
                side_effect=RuntimeError("programmer defect"),
            ),
            self.assertLogs(
                "web.routes.retag_divergence_audit", level="ERROR",
            ) as logs,
        ):
            status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 503)
        self.assertEqual(payload["error"], "Retag divergence audit failed")
        self.assertIn(
            "retag divergence audit failed unexpectedly", "\n".join(logs.output),
        )

    def test_expected_open_failure_is_an_unavailable_report(self) -> None:
        from web import server

        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
        with patch.object(server, "_beets_db", side_effect=failure):
            status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 200)
        self.assertFalse(payload["complete"])
        self.assertEqual(payload["status"], "beets_unavailable")


if __name__ == "__main__":
    unittest.main()
