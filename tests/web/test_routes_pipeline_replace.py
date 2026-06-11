#!/usr/bin/env python3
"""Contract tests for the Replace / resolve-rg routes (web/routes/pipeline.py).

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import os
import sys
import unittest
from unittest.mock import patch


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _WebServerCase



class TestReplacedFilterContract(_WebServerCase):
    """U10 backend tests for the ``?include_replaced`` query parameter
    on pipeline + wrong-matches list endpoints, plus the descendant_*
    fields surfaced from ``post_pipeline_add`` when the existing row is
    ``status='replaced'``.
    """

    def setUp(self) -> None:
        # Default mock for the supersede-lookup so the add-flow tests
        # below can override per-test.
        self.mock_db.get_request_by_replaces_request_id.return_value = None

    def test_pipeline_all_default_excludes_replaced(self):
        captured: list[tuple[str, ...]] = []
        def fake_get_by_status(status):
            captured.append((status,))
            return []
        self.mock_db.get_by_status.side_effect = fake_get_by_status
        self.mock_db.count_by_status.return_value = {}
        self.mock_db.get_download_history_batch.return_value = {}
        status, _ = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        statuses = [c[0] for c in captured]
        self.assertNotIn("replaced", statuses)

    def test_pipeline_all_include_replaced_true_fetches_replaced(self):
        captured: list[tuple[str, ...]] = []
        def fake_get_by_status(status):
            captured.append((status,))
            return []
        self.mock_db.get_by_status.side_effect = fake_get_by_status
        self.mock_db.count_by_status.return_value = {}
        self.mock_db.get_download_history_batch.return_value = {}
        status, _ = self._get("/api/pipeline/all?include_replaced=true")
        self.assertEqual(status, 200)
        statuses = [c[0] for c in captured]
        self.assertIn("replaced", statuses)

    def test_post_pipeline_add_with_replaced_existing_surfaces_descendant(self):
        # The harness routes get_request_by_release_id through
        # get_request_by_mb_release_id (see _make_server), so mock that.
        self.mock_db.get_request_by_mb_release_id.return_value = {
            "id": 42, "status": "replaced",
            "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        }
        self.mock_db.get_request_by_replaces_request_id.return_value = {
            "id": 99, "status": "wanted",
            "mb_release_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        }
        status, data = self._post(
            "/api/pipeline/add",
            {"mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "exists")
        self.assertEqual(data["current_status"], "replaced")
        self.assertEqual(data["descendant_request_id"], 99)
        self.assertEqual(data["descendant_status"], "wanted")

    def test_post_pipeline_add_with_non_replaced_existing_omits_descendant(self):
        self.mock_db.get_request_by_mb_release_id.return_value = {
            "id": 42, "status": "wanted",
            "mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        }
        status, data = self._post(
            "/api/pipeline/add",
            {"mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["current_status"], "wanted")
        self.assertNotIn("descendant_request_id", data)


class TestPipelineReplaceContract(_WebServerCase):
    """Contract for ``POST /api/pipeline/<id>/replace`` plus the two
    auxiliary endpoints (``GET /api/pipeline/requests-by-rg/<rg>`` and
    ``GET /api/pipeline/active-rgs``).

    The endpoint wraps ``MbidReplaceService.replace_request_mbid``. The
    CLI counterpart (``pipeline-cli replace``) must stay in sync — see
    ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"; touching one without
    the other is a contract drift waiting to happen.

    Status-code mapping mirrors the CLI exit codes:
      * 200 — RESULT_REPLACED
      * 400 — body validation failure (missing/empty target)
      * 404 — RESULT_NOT_FOUND
      * 409 — RESULT_WRONG_STATE, RESULT_TARGET_COLLISION_REQUEST
      * 422 — RESULT_TARGET_INVALID, RESULT_TARGET_RELEASE_GROUP_MISMATCH,
              RESULT_TARGET_SAME_AS_CURRENT
      * 503 — RESULT_TRANSIENT
    """

    REPLACE_REQUIRED_FIELDS = {
        "outcome", "request_id", "new_request_id", "current_status",
        "descendant_request_id", "error_message", "warnings",
    }
    REQUESTS_BY_RG_FIELDS = {
        "id", "mb_release_id", "mb_release_group_id", "status",
        "artist_name", "album_title",
    }

    def setUp(self) -> None:
        from lib.config import CratediggerConfig
        import configparser
        cp = configparser.RawConfigParser()
        cp.read_string("[General]\n")
        self._cfg_patcher = patch(
            "lib.config.read_runtime_config",
            return_value=CratediggerConfig.from_ini(cp),
        )
        self._cfg_patcher.start()

    def tearDown(self) -> None:
        self._cfg_patcher.stop()

    def _patch_service(self, **result_kwargs):
        from unittest.mock import patch as _patch
        from lib.mbid_replace_service import ReplaceResult
        return _patch(
            "lib.mbid_replace_service.MbidReplaceService"
            ".replace_request_mbid",
            return_value=ReplaceResult(**result_kwargs),
        )

    def test_replace_success_returns_200(self):
        with self._patch_service(
            outcome="replaced", request_id=100, new_request_id=200,
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.REPLACE_REQUIRED_FIELDS,
                                "replace response")
        self.assertEqual(data["outcome"], "replaced")
        self.assertEqual(data["new_request_id"], 200)

    def test_replace_not_found_returns_404(self):
        with self._patch_service(
            outcome="not_found", request_id=9999,
            error_message="request 9999 not found",
        ):
            status, data = self._post(
                "/api/pipeline/9999/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_replace_wrong_state_lock_contention_returns_409(self):
        with self._patch_service(
            outcome="wrong_state", request_id=100,
            error_message="importer holds the lock",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 409)
        self.assertIsNone(data["descendant_request_id"])

    def test_replace_wrong_state_source_already_replaced_carries_descendant(self):
        with self._patch_service(
            outcome="wrong_state", request_id=42, descendant_request_id=99,
            error_message="already replaced",
        ):
            status, data = self._post(
                "/api/pipeline/42/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 409)
        self.assertEqual(data["descendant_request_id"], 99)

    def test_replace_collision_carries_current_status(self):
        with self._patch_service(
            outcome="target_collision_request", request_id=100,
            current_status="wanted",
            error_message="target held by request 43",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 409)
        self.assertEqual(data["current_status"], "wanted")

    def test_replace_target_invalid_returns_422(self):
        with self._patch_service(
            outcome="target_invalid", request_id=100,
            error_message="MB lookup empty",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "bogus"},
            )
        self.assertEqual(status, 422)

    def test_replace_rg_mismatch_returns_422(self):
        with self._patch_service(
            outcome="target_release_group_mismatch", request_id=100,
            error_message="rg mismatch",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "other-rg"},
            )
        self.assertEqual(status, 422)

    def test_replace_same_as_current_returns_422(self):
        with self._patch_service(
            outcome="target_same_as_current", request_id=100,
            error_message="target == source",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "same-uuid"},
            )
        self.assertEqual(status, 422)

    def test_replace_transient_returns_503(self):
        """503 maps to RESULT_TRANSIENT — typically an MB-mirror
        network blip / timeout / JSON decode error during the fresh
        target lookup. The response body must still carry the full
        REPLACE_REQUIRED_FIELDS contract so the frontend can show the
        "Retry" affordance and the error message uniformly with the
        other outcomes."""
        with self._patch_service(
            outcome="transient", request_id=100,
            error_message="MB mirror unreachable",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.REPLACE_REQUIRED_FIELDS,
            "replace 503 response",
        )
        self.assertEqual(data["outcome"], "transient")
        self.assertEqual(data["request_id"], 100)
        self.assertEqual(
            data["error_message"], "MB mirror unreachable",
        )
        # Optional payload fields stay null on a transient outcome
        # (no new row, no current_status, no descendant).
        self.assertIsNone(data["new_request_id"])
        self.assertIsNone(data["current_status"])
        self.assertIsNone(data["descendant_request_id"])

    def test_replace_missing_target_returns_400(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.mbid_replace_service.MbidReplaceService"
            ".replace_request_mbid"
        ) as mock_svc:
            status, data = self._post(
                "/api/pipeline/100/replace", {},
            )
        self.assertEqual(status, 400)
        self.assertIn("target_mb_release_id", data["error"])
        mock_svc.assert_not_called()

    def test_replace_empty_target_returns_400(self):
        from unittest.mock import patch as _patch
        with _patch(
            "lib.mbid_replace_service.MbidReplaceService"
            ".replace_request_mbid"
        ) as mock_svc:
            status, _ = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "  "},
            )
        self.assertEqual(status, 400)
        mock_svc.assert_not_called()

    def test_requests_by_rg_returns_200_with_required_fields(self):
        self.mock_db.list_requests_in_release_group.return_value = [
            {
                "id": 42, "mb_release_id": "old-uuid",
                "mb_release_group_id": "rg-1",
                "status": "wanted",
                "artist_name": "Pet Grief", "album_title": "X",
            },
        ]
        status, data = self._get(
            "/api/pipeline/requests-by-rg/"
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        self.assertEqual(status, 200)
        self.assertIn("requests", data)
        self.assertEqual(len(data["requests"]), 1)
        _assert_required_fields(
            self, data["requests"][0],
            self.REQUESTS_BY_RG_FIELDS,
            "requests-by-rg row",
        )

    def test_requests_by_rg_empty_list(self):
        self.mock_db.list_requests_in_release_group.return_value = []
        status, data = self._get(
            "/api/pipeline/requests-by-rg/"
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["requests"], [])

    def test_active_rgs_returns_sorted_list(self):
        self.mock_db.list_active_release_group_ids.return_value = {
            "rg-bbbb", "rg-aaaa",
        }
        status, data = self._get("/api/pipeline/active-rgs")
        self.assertEqual(status, 200)
        self.assertEqual(data["release_group_ids"], ["rg-aaaa", "rg-bbbb"])

    def test_active_rgs_empty(self):
        self.mock_db.list_active_release_group_ids.return_value = set()
        status, data = self._get("/api/pipeline/active-rgs")
        self.assertEqual(status, 200)
        self.assertEqual(data["release_group_ids"], [])


class TestPipelineResolveRgContract(_WebServerCase):
    """Contract for ``POST /api/pipeline/<id>/resolve-rg``.

    Lazy-backfill ``mb_release_group_id`` for legacy rows. The Replace
    picker calls this in standard mode when the row has a null RG so the
    sibling-fetch can proceed.

    Status-code mapping:
      * 200 — ``status='resolved'`` (RG found or already set)
      * 404 — request not found
      * 422 — non-UUID release id (Discogs) or MB returned no RG
      * 503 — transient MB-mirror failure
    """

    RESOLVE_RG_REQUIRED_FIELDS = {
        "request_id", "mb_release_group_id", "status",
    }

    def setUp(self) -> None:
        # _WebServerCase shares ``mock_db`` across tests in the class via
        # setUpClass; reset call counts here so per-test assertions don't
        # see stale calls from earlier tests in the same class.
        self.mock_db.reset_mock()

    def test_resolve_rg_already_set_returns_200(self):
        """Idempotent: row already has a RG → return it untouched
        and do NOT hit the MB mirror or write to the DB."""
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "mb_release_group_id": "rrrrrrrr-rrrr-rrrr-rrrr-rrrrrrrrrrrr",
        }
        with patch("web.mb.get_release") as mock_mb:
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg already-set response",
        )
        self.assertEqual(data["status"], "resolved")
        self.assertEqual(
            data["mb_release_group_id"],
            "rrrrrrrr-rrrr-rrrr-rrrr-rrrrrrrrrrrr",
        )
        mock_mb.assert_not_called()
        self.mock_db.update_request_fields.assert_not_called()

    def test_resolve_rg_lazy_backfill_happy_path_returns_200(self):
        """Row has no RG → MB lookup → UPDATE row → 200."""
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "mb_release_group_id": None,
        }
        with patch(
            "web.mb.get_release",
            return_value={"release_group_id": "rrrr-rrrr-rrrr"},
        ) as mock_mb:
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg happy-path response",
        )
        self.assertEqual(data["status"], "resolved")
        self.assertEqual(
            data["mb_release_group_id"], "rrrr-rrrr-rrrr",
        )
        mock_mb.assert_called_once_with(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", fresh=False,
        )
        self.mock_db.update_request_fields.assert_called_once_with(
            42, mb_release_group_id="rrrr-rrrr-rrrr",
        )

    def test_resolve_rg_not_found_returns_404(self):
        self.mock_db.get_request.return_value = None
        with patch("web.mb.get_release") as mock_mb:
            status, data = self._post(
                "/api/pipeline/9999/resolve-rg", {},
            )
        self.assertEqual(status, 404)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg not-found response",
        )
        self.assertEqual(data["status"], "not_found")
        self.assertIsNone(data["mb_release_group_id"])
        mock_mb.assert_not_called()

    def test_resolve_rg_no_release_group_returns_422(self):
        """MB returns a payload but no release_group_id (e.g. mirror
        anomaly, or a release whose RG is missing upstream)."""
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "mb_release_group_id": None,
        }
        with patch(
            "web.mb.get_release",
            return_value={"release_group_id": None},
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 422)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg 422 response",
        )
        self.assertEqual(data["status"], "no_release_group")
        self.mock_db.update_request_fields.assert_not_called()

    def test_resolve_rg_discogs_release_id_returns_422(self):
        """Numeric Discogs release id → 422 short-circuit, no MB hit."""
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "12345",
            "mb_release_group_id": None,
        }
        with patch("web.mb.get_release") as mock_mb:
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 422)
        self.assertEqual(data["status"], "non_mb_release_id")
        mock_mb.assert_not_called()
        self.mock_db.update_request_fields.assert_not_called()

    def test_resolve_rg_transient_returns_503(self):
        """Network blip / timeout → 503 retryable."""
        from urllib.error import URLError
        self.mock_db.get_request.return_value = {
            "id": 42,
            "mb_release_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "mb_release_group_id": None,
        }
        with patch(
            "web.mb.get_release",
            side_effect=URLError("connection refused"),
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg 503 response",
        )
        self.assertEqual(data["status"], "transient")
        self.mock_db.update_request_fields.assert_not_called()

if __name__ == "__main__":
    unittest.main()
