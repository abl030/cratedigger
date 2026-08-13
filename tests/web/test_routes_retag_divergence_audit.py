"""HTTP contract for GET /api/audit/retag-divergence (#1093 item 1)."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mediafile import MediaFile

from lib.beets_db import BeetsAlbumIdentityRow
from tests.fakes import FakeBeetsDB
from tests.test_beets_retag import MERGED, SURVIVOR, _make_real_mp3
from tests.web._harness import _FakeDbWebServerCase


class TestRetagDivergenceAuditRoute(_FakeDbWebServerCase):
    def test_reports_an_incomplete_finding_for_an_unreadable_file(self) -> None:
        """#1093 review round 4, finding 5 — 409, not 200: ``incomplete``
        means the world blocked a complete answer, so a caller must never
        read it as "no divergence" the way a plain 200 would suggest."""
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

        self.assertEqual(status, 409)
        self.assertEqual(payload["status"], "incomplete")
        self.assertTrue(payload["complete"])
        self.assertEqual(payload["counts"]["albums_scanned"], 1)
        self.assertEqual(len(payload["albums"]), 1)
        self.assertEqual(payload["albums"][0]["album_class"], "unreadable")
        self.assertEqual(beets.close_calls, 0)

    def test_reports_a_genuine_divergence(self) -> None:
        from web import server

        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = Path(tmpdir) / "01.mp3"
            _make_real_mp3(track_path)
            media = MediaFile(track_path)
            media.mb_albumid = MERGED
            media.save()

            beets = FakeBeetsDB()
            beets.set_album_mb_identities([
                BeetsAlbumIdentityRow(
                    album_id=1, mb_albumid=SURVIVOR,
                    item_paths=(str(track_path),),
                ),
            ])
            with patch.object(server, "_beets_db", return_value=beets):
                status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "divergence_found")
        self.assertEqual(len(payload["albums"]), 1)
        self.assertEqual(payload["albums"][0]["album_class"], "diverges")

    def test_clean_report_lists_no_albums(self) -> None:
        from web import server

        beets = FakeBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "clean")
        self.assertEqual(payload["albums"], [])

    def test_missing_beets_is_an_unavailable_report(self) -> None:
        """#1093 review round 3, finding 1 — 503, not 200: the audit
        never actually ran, so 200 would let a caller read "no
        divergence" from a report that answered nothing."""
        from web import server

        with patch.object(server, "_beets_db", return_value=None):
            status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 503)
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
        """#1093 review round 3, finding 1 — 503, not 200 (see
        ``test_missing_beets_is_an_unavailable_report``)."""
        from web import server

        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
        with patch.object(server, "_beets_db", side_effect=failure):
            status, payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 503)
        self.assertFalse(payload["complete"])
        self.assertEqual(payload["status"], "beets_unavailable")

    def test_route_bounds_the_scan_with_a_positive_deadline(self) -> None:
        """#1093 review round 2, finding 2 — the route must never launch
        an unbounded scan: a measured full census took ~196s against the
        deployed vhost's inherited 60s nginx default. Seam test: the route
        wires SOME positive deadline into the shared service call; the
        deadline's own truncation behaviour is proven at the service level
        (``tests/test_retag_divergence_audit.py::TestScanDeadline``), and
        the deadline VALUE is pinned against the nginx default separately
        (``TestApiScanDeadlineConstant`` below)."""
        from lib.retag_divergence_audit import (
            scan_retag_divergence_from_borrowed_factory as real_scan,
        )
        from web import server
        from web.routes import retag_divergence_audit as route_module

        recorded: dict[str, object] = {}

        def recording_scan(beets_factory, **kwargs):
            recorded.update(kwargs)
            return real_scan(beets_factory, **kwargs)

        beets = FakeBeetsDB()
        with (
            patch.object(server, "_beets_db", return_value=beets),
            patch.object(
                route_module,
                "scan_retag_divergence_from_borrowed_factory",
                recording_scan,
            ),
        ):
            status, _payload = self._get("/api/audit/retag-divergence")

        self.assertEqual(status, 200)
        self.assertIn("deadline_seconds", recorded)
        deadline = recorded["deadline_seconds"]
        self.assertIsInstance(deadline, float)
        assert isinstance(deadline, float)
        self.assertGreater(deadline, 0.0)

    def test_after_album_id_query_param_is_forwarded(self) -> None:
        """#1093 review round 4, finding 4."""
        from web import server

        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=1,
                mb_albumid="7aabf975-9a06-4b2e-854c-2c700380ebd5",
                item_paths=(),
            ),
            BeetsAlbumIdentityRow(
                album_id=2,
                mb_albumid="7aabf975-9a06-4b2e-854c-2c700380ebd5",
                item_paths=(),
            ),
        ])
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get(
                "/api/audit/retag-divergence?after_album_id=1",
            )

        self.assertEqual(status, 409)  # album 2 alone: a real zero-item row
        self.assertEqual(payload["counts"]["albums_scanned"], 1)
        self.assertEqual(payload["albums"][0]["album_id"], 2)

    def test_resumed_call_over_agreeing_content_is_409_not_200(self) -> None:
        """#1093 review round 5, finding 1 — the exact API-level defect: a
        resumed call (``?after_album_id=N``) that finds nothing wrong must
        still be 409/``incomplete``, never 200/``clean``, because it only
        vouches for the range it scanned, not the prefix the cursor
        skipped. The chained walk in
        ``tests/test_retag_divergence_audit.py::TestCursorResume`` proves
        the deterministic invariant; this proves the SAME thing through
        the real HTTP route and status-code mapping."""
        from web import server

        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=1,
                mb_albumid="7aabf975-9a06-4b2e-854c-2c700380ebd5",
                item_paths=(),
            ),
        ])
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get(
                "/api/audit/retag-divergence?after_album_id=1",
            )

        # after_album_id=1 filters out the only album — nothing scanned,
        # nothing listed, complete — yet still not "clean" (round 5,
        # finding 1).
        self.assertEqual(status, 409)
        self.assertEqual(payload["status"], "incomplete")
        self.assertEqual(payload["counts"]["albums_scanned"], 0)
        self.assertEqual(payload["albums"], [])
        self.assertEqual(payload["after_album_id"], 1)
        self.assertIsNone(payload["next_after_album_id"])

    def test_malformed_after_album_id_is_a_400(self) -> None:
        from web import server

        beets = FakeBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get(
                "/api/audit/retag-divergence?after_album_id=not-an-int",
            )

        self.assertEqual(status, 400)
        self.assertIn("after_album_id", payload.get("error", ""))

    def test_after_album_id_underscore_grouping_is_a_400_not_silently_reinterpreted(
        self,
    ) -> None:
        """#1093 review round 5, finding 5 — Python's bare ``int()`` would
        silently accept ``"1_0"`` as ``10``; the API must refuse it rather
        than use a different cursor than the caller typed."""
        from web import server

        beets = FakeBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get(
                "/api/audit/retag-divergence?after_album_id=1_0",
            )

        self.assertEqual(status, 400)
        self.assertIn("after_album_id", payload.get("error", ""))


class TestApiScanDeadlineConstant(unittest.TestCase):
    """#1093 review round 4, finding 3 — a check that cannot fail: the only
    prior test asserted ``deadline > 0``, so
    ``API_SCAN_DEADLINE_SECONDS = 36000.0`` (10 hours) would have stayed
    green. The deadline must leave REAL margin under nginx's default
    ``proxy_read_timeout``, not merely be positive — and margin must cover
    the UNBOUNDED overhead (the DB fetch before the loop, the JSON encode
    after it) the deadline itself never bounds."""

    def test_deadline_leaves_real_margin_under_the_reverse_proxy_default(
        self,
    ) -> None:
        from web.routes.retag_divergence_audit import (
            API_SCAN_DEADLINE_SECONDS,
            NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS,
        )

        self.assertLess(
            API_SCAN_DEADLINE_SECONDS, NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS,
        )
        # A meaningful margin, not merely "less than" — measured live
        # unbounded overhead (DB fetch + JSON encode) was several seconds.
        self.assertLessEqual(
            API_SCAN_DEADLINE_SECONDS,
            NGINX_DEFAULT_PROXY_READ_TIMEOUT_SECONDS - 15.0,
        )


if __name__ == "__main__":
    unittest.main()
