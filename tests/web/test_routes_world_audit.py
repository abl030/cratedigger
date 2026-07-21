"""HTTP contract for GET /api/audit/world."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row
from tests.web._harness import _FakeDbWebServerCase


class TestWorldAuditRoute(_FakeDbWebServerCase):
    def test_reports_shared_service_payload(self) -> None:
        import web.server as server

        self.db.seed_request(make_request_row(
            id=31,
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            status="imported",
            imported_path="/missing/world-audit-album",
        ))
        with patch.object(server, "_beets_db", return_value=FakeBeetsDB()):
            status, payload = self._get("/api/audit/world")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "violations")
        self.assertEqual(payload["counts"]["active_requests"], 1)
        self.assertIn("status_membership", payload["audited_invariants"])
        self.assertIn(
            "proof_lock_terminality_across_operation",
            payload["temporal_invariants_not_auditable"],
        )
        self.assertIn(
            "current_beets_missing",
            {row["code"] for row in payload["violations"]},
        )

    def test_missing_beets_is_explicitly_unavailable(self) -> None:
        status, payload = self._get("/api/audit/world")

        self.assertEqual(status, 503)
        self.assertIn("Beets DB not configured", payload["error"])


if __name__ == "__main__":
    unittest.main()
