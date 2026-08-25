"""HTTP contract for GET /api/audit/retag-divergence (#1093 item 1)."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

from mediafile import MediaFile

from lib.beets_db import BeetsAlbumIdentityRow
from tests.fakes import FakeBeetsDB
from tests.test_beets_retag import MERGED, SURVIVOR, _make_real_mp3
from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase


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


class _PoisonedWholeLibraryBeetsDB(FakeBeetsDB):
    """#1142 acceptance 6 — the per-album recheck must never invoke the
    whole-library reader, even when a Beets handle IS available."""

    def list_album_mb_identities(self) -> list[BeetsAlbumIdentityRow]:
        raise AssertionError(
            "per-album recheck must not invoke the whole-library "
            "retag-divergence reader"
        )


class TestRetagDivergenceAuditAlbumRoute(_FakeDbWebServerCase):
    """``GET /api/audit/retag-divergence/album/<id>`` (#1142) — a cheap,
    explicit per-album recheck reusing the SAME classifier/tag-reader as
    the whole-library census, never the whole-library scan itself."""

    def test_agreeing_album_is_200_with_agrees_class(self) -> None:
        from web import server

        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = Path(tmpdir) / "01.mp3"
            _make_real_mp3(track_path)
            media = MediaFile(track_path)
            media.mb_albumid = SURVIVOR
            media.save()

            beets = _PoisonedWholeLibraryBeetsDB()
            beets.set_album_mb_identities([
                BeetsAlbumIdentityRow(
                    album_id=1, mb_albumid=SURVIVOR,
                    item_paths=(str(track_path),),
                ),
            ])
            with patch.object(server, "_beets_db", return_value=beets):
                status, payload = self._get(
                    "/api/audit/retag-divergence/album/1",
                )

        self.assertEqual(status, 200)
        self.assertEqual(payload["album_id"], 1)
        self.assertEqual(payload["album_class"], "agrees")

    def test_diverging_album_is_200_with_diverges_class(self) -> None:
        from web import server

        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = Path(tmpdir) / "01.mp3"
            _make_real_mp3(track_path)
            media = MediaFile(track_path)
            media.mb_albumid = MERGED
            media.save()

            beets = _PoisonedWholeLibraryBeetsDB()
            beets.set_album_mb_identities([
                BeetsAlbumIdentityRow(
                    album_id=5, mb_albumid=SURVIVOR,
                    item_paths=(str(track_path),),
                ),
            ])
            with patch.object(server, "_beets_db", return_value=beets):
                status, payload = self._get(
                    "/api/audit/retag-divergence/album/5",
                )

        self.assertEqual(status, 200)
        self.assertEqual(payload["album_class"], "diverges")

    def test_unknown_album_is_404(self) -> None:
        from web import server

        beets = _PoisonedWholeLibraryBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get(
                "/api/audit/retag-divergence/album/999",
            )

        self.assertEqual(status, 404)
        self.assertIn("error", payload)

    def test_oversized_album_id_is_a_400_not_a_503(self) -> None:
        """N10 (#1142 review) — an id past SQLite's signed-64-bit INTEGER
        range can never be bound as a query parameter at all (sqlite3
        raises ``OverflowError`` before any query runs); this is invalid
        CLIENT input, not a transient/retryable Beets-unavailable
        condition, so it must reject as 400 before ever reaching Beets —
        never a 503 with a swallowed traceback."""
        from web import server

        beets = _PoisonedWholeLibraryBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._get(
                "/api/audit/retag-divergence/album/99999999999999999999999999999",
            )

        self.assertEqual(status, 400)
        self.assertIn("error", payload)

    def test_a_4301_digit_album_id_is_a_clean_400_not_a_500(self) -> None:
        """N3 (fresh review) — Python's own int() conversion refuses a
        string past ``sys.int_info.default_max_str_digits`` (4300) with a
        bare ``ValueError``. The route's URL regex (\\d+) matches ANY
        digit run, so a 4301-digit path previously reached an unguarded
        ``int(album_id_str)`` and propagated all the way out of the
        handler to ``web.server.Handler.do_GET``'s broad
        ``except Exception`` — a full traceback log, an unnecessary
        ``_try_reconnect_db()`` DB-reconnect churn, and a generic 500,
        for what is really just malformed input. Must be a clean 400,
        driven through the REAL route dispatch (not a mocked handler
        call), with no ERROR-level log from either logger the two
        failure paths use."""
        from web import server

        digit_string = "9" * 4301
        beets = _PoisonedWholeLibraryBeetsDB()
        with (
            patch.object(server, "_beets_db", return_value=beets),
            self.assertNoLogs("web.server", level="ERROR"),
            self.assertNoLogs(
                "web.routes.retag_divergence_audit", level="ERROR",
            ),
        ):
            status, payload = self._get(
                f"/api/audit/retag-divergence/album/{digit_string}",
            )

        self.assertEqual(status, 400)
        self.assertIn("out of range", payload.get("error", ""))

    def test_missing_beets_is_503(self) -> None:
        """#1266 item 3 — the exact copy pins the INTENDED classified
        path: a bare 503 + "error"-key assertion also passes when a
        deleted guard routes through the generic unexpected-failure
        except with a spurious traceback (#1264 mutant runner S2)."""
        from web import server

        with patch.object(server, "_beets_db", return_value=None):
            status, payload = self._get(
                "/api/audit/retag-divergence/album/1",
            )

        self.assertEqual(status, 503)
        self.assertEqual(
            payload["error"],
            "current Beets authority unavailable (FileNotFoundError)",
        )

    def test_expected_open_failure_is_503(self) -> None:
        """#1266 item 3 — same exact-copy pin for the classified SQLite
        lane (BUSY is primary code 5)."""
        from web import server

        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
        with patch.object(server, "_beets_db", side_effect=failure):
            status, payload = self._get(
                "/api/audit/retag-divergence/album/1",
            )

        self.assertEqual(status, 503)
        self.assertEqual(
            payload["error"],
            "current Beets authority unavailable (sqlite_5)",
        )

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
            status, _payload = self._get(
                "/api/audit/retag-divergence/album/1",
            )

        self.assertEqual(status, 503)
        self.assertIn(
            "per-album retag divergence check failed unexpectedly",
            "\n".join(logs.output),
        )

    def test_borrowed_beets_handle_is_never_closed(self) -> None:
        """The route mediates a server-owned handle (like the
        whole-library route) — it must not close the shared per-thread
        Beets connection."""
        from web import server

        beets = _PoisonedWholeLibraryBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(album_id=1, mb_albumid="", item_paths=()),
        ])
        with patch.object(server, "_beets_db", return_value=beets):
            status, _payload = self._get(
                "/api/audit/retag-divergence/album/1",
            )

        self.assertEqual(status, 200)
        self.assertEqual(beets.close_calls, 0)


class TestRetagDivergenceSyncTagsRoute(_FakeDbWebServerCase):
    """``POST /api/audit/retag-divergence/album/<id>/sync-tags`` (#1260) —
    the census card's Write-tags action. These tests never let a real
    ``beet write`` subprocess launch: every scenario either refuses
    before the write, or fails the write's environment resolution
    deterministically (a nonexistent ``CRATEDIGGER_RUNTIME_CONFIG``) so
    the verdict provably comes from the re-read files."""

    REQUIRED_FIELDS: ClassVar[set[str]] = {
        "outcome", "album_id", "db_mb_albumid", "album", "error_message",
    }

    def _post_sync(
        self, album_id: object, body: dict[str, object],
    ) -> tuple[int, dict]:
        return self._post(
            f"/api/audit/retag-divergence/album/{album_id}/sync-tags",
            body,
        )

    def test_agreeing_album_is_200_already_synced(self) -> None:
        from web import server

        with tempfile.TemporaryDirectory() as tmpdir:
            track_path = Path(tmpdir) / "01.mp3"
            _make_real_mp3(track_path)
            media = MediaFile(track_path)
            media.mb_albumid = SURVIVOR
            media.save()

            beets = FakeBeetsDB()
            beets.set_album_mb_identities([
                BeetsAlbumIdentityRow(
                    album_id=1, mb_albumid=SURVIVOR,
                    item_paths=(str(track_path),),
                    albumartist="Terre Thaemlitz / DJ Sprinkles",
                    album="RA.1000",
                ),
            ])
            with patch.object(server, "_beets_db", return_value=beets):
                status, payload = self._post_sync(
                    1, {"expected_mb_albumid": SURVIVOR},
                )

        self.assertEqual(status, 200)
        self.assertEqual(payload["outcome"], "already_synced")
        _assert_required_fields(
            self, payload, self.REQUIRED_FIELDS, "sync-tags",
        )
        self.assertEqual(payload["album"]["album_class"], "agrees")
        self.assertEqual(
            payload["album"]["albumartist"], "Terre Thaemlitz / DJ Sprinkles",
        )
        self.assertEqual(beets.close_calls, 0)

    def test_stale_authorized_identity_is_409(self) -> None:
        from web import server

        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=1, mb_albumid=SURVIVOR, item_paths=(),
            ),
        ])
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._post_sync(
                1, {"expected_mb_albumid": MERGED},
            )

        self.assertEqual(status, 409)
        self.assertEqual(payload["outcome"], "identity_mismatch")
        self.assertEqual(payload["db_mb_albumid"], SURVIVOR)

    def test_unknown_album_is_404(self) -> None:
        from web import server

        beets = FakeBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, payload = self._post_sync(
                999, {"expected_mb_albumid": SURVIVOR},
            )

        self.assertEqual(status, 404)
        self.assertEqual(payload["outcome"], "not_found")

    def test_missing_body_field_is_400(self) -> None:
        from web import server

        beets = FakeBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, _payload = self._post_sync(1, {})

        self.assertEqual(status, 400)

    def test_out_of_range_album_id_is_400(self) -> None:
        from web import server

        beets = FakeBeetsDB()
        with patch.object(server, "_beets_db", return_value=beets):
            status, _payload = self._post_sync(
                "9223372036854775808", {"expected_mb_albumid": SURVIVOR},
            )

        self.assertEqual(status, 400)

    def test_missing_beets_is_503(self) -> None:
        from web import server

        with patch.object(server, "_beets_db", return_value=None):
            status, payload = self._post_sync(
                1, {"expected_mb_albumid": SURVIVOR},
            )

        self.assertEqual(status, 503)
        # The exact copy pins the INTENDED clean-unavailability path — a
        # deleted None-guard also 503s, but via the generic "Tag sync
        # failed" except-branch with a spurious traceback (#1260 mutant
        # runner S2/M6).
        self.assertEqual(payload["error"], "Beets DB not available")

    def test_divergent_album_with_a_failing_write_is_409_residual(
        self,
    ) -> None:
        """The verdict provably comes from the re-read files: the write's
        environment resolution fails deterministically, the file still
        carries the merged-away tag, and the route reports the residual
        with the per-item detail."""
        import os

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
                    album_id=7, mb_albumid=SURVIVOR,
                    item_paths=(str(track_path),),
                ),
            ])
            # Hermetic write environment: the runtime config resolves
            # nowhere and BEETSDIR points INSIDE this test's tmpdir, so
            # the real subprocess (if it launches at all) opens an empty
            # scratch library, matches nothing, and touches no ambient
            # state — never the invoking user's ~/.config/beets.
            scratch_beetsdir = Path(tmpdir) / "scratch-beetsdir"
            scratch_beetsdir.mkdir()
            with patch.object(server, "_beets_db", return_value=beets), \
                    patch.dict(os.environ, {
                        "CRATEDIGGER_RUNTIME_CONFIG":
                            str(Path(tmpdir) / "nonexistent-config.ini"),
                        "BEETSDIR": str(scratch_beetsdir),
                    }, clear=False):
                status, payload = self._post_sync(
                    7, {"expected_mb_albumid": SURVIVOR},
                )

            # The file on disk is untouched — still the merged-away tag.
            self.assertEqual(str(MediaFile(track_path).mb_albumid), MERGED)

        self.assertEqual(status, 409)
        self.assertEqual(payload["outcome"], "residual_divergence")
        _assert_required_fields(
            self, payload, self.REQUIRED_FIELDS, "sync-tags residual",
        )
        self.assertEqual(payload["album"]["album_class"], "diverges")
        items = payload["album"]["items"]
        self.assertEqual(items[0]["file_mb_albumid"], MERGED)


if __name__ == "__main__":
    unittest.main()
