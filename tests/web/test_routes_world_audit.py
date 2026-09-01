"""HTTP contract for GET /api/audit/world."""

from __future__ import annotations

import sqlite3
import unittest
from dataclasses import replace
from unittest.mock import patch

from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row, make_web_runtime
from tests.web._harness import _FakeDbWebServerCase
from web.runtime import WebRuntime, install_runtime, runtime


class TestWorldAuditRoute(_FakeDbWebServerCase):
    def test_reports_shared_service_payload(self) -> None:
        beets = FakeBeetsDB()
        self.db.seed_request(make_request_row(
            id=31,
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            status="imported",
        ))
        with install_runtime(make_web_runtime(runtime(), beets=beets)):
            status, payload = self._get("/api/audit/world")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "observations_only")
        self.assertTrue(payload["complete"])
        self.assertEqual(payload["counts"]["active_requests"], 1)
        self.assertIn("status_membership", payload["audited_invariants"])
        self.assertIn(
            "proof_lock_terminality_across_operation",
            payload["temporal_invariants_not_auditable"],
        )
        self.assertIn(
            "current_beets_missing",
            {row["code"] for row in payload["groups"]["b"]["members"]},
        )
        self.assertEqual(beets.close_calls, 0)

    def test_missing_beets_is_an_incomplete_bucket_b_observation(self) -> None:
        with install_runtime(replace(runtime(), shared_beets=None)):
            status, payload = self._get("/api/audit/world")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "observations_only")
        self.assertFalse(payload["complete"])
        self.assertEqual(
            [row["code"] for row in payload["groups"]["b"]["members"]],
            ["current_beets_authority_unavailable"],
        )

    def test_unexpected_failure_is_logged_and_returns_503(self) -> None:
        with (
            patch.object(
                WebRuntime,
                "beets_db",
                side_effect=RuntimeError("programmer defect"),
            ),
            self.assertLogs("web.routes.world_audit", level="ERROR") as logs,
        ):
            status, payload = self._get("/api/audit/world")

        self.assertEqual(status, 503)
        self.assertEqual(payload["error"], "World audit failed")
        self.assertIn("world audit failed unexpectedly", "\n".join(logs.output))

    def test_expected_open_failure_is_an_incomplete_bucket_b_observation(self) -> None:
        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
        with patch.object(WebRuntime, "beets_db", side_effect=failure):
            status, payload = self._get("/api/audit/world")

        self.assertEqual(status, 200)
        self.assertFalse(payload["complete"])
        self.assertEqual(
            [row["code"] for row in payload["groups"]["b"]["members"]],
            ["current_beets_authority_unavailable"],
        )


if __name__ == "__main__":
    unittest.main()
