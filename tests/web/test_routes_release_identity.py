"""Contract tests for the Replace / resolve-rg routes
(web/routes/release_identity_routes.py), plus the closely-related
requests-by-rg / active-rgs auxiliary endpoints that stayed in
web/routes/pipeline.py (#522 — same Replace-picker UI flow).

Split from tests/test_web_server.py (#408); renamed from
test_routes_pipeline_replace.py when web/routes/pipeline.py's
resolve-rg/replace handlers moved to web/routes/release_identity_routes.py
(#522). Shared harness in tests/web/_harness.py.
"""
import os
import sys
import unittest
from typing import ClassVar
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.dispatch_helpers import handoff_automation_owner
from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row, make_web_runtime
from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase
from web.runtime import WebRuntime, install_runtime, runtime


class TestReplacedFilterContract(_FakeDbWebServerCase):
    """U10 backend tests for the ``?include_replaced`` query parameter
    on pipeline + wrong-matches list endpoints, plus the descendant_*
    fields surfaced from ``post_pipeline_add`` when the existing row is
    ``status='replaced'``.
    """

    def setUp(self) -> None:
        super().setUp()
        # One active row + one frozen audit row — the filter contract
        # is about which of these the list endpoints surface.
        self.db.seed_request(make_request_row(
            id=1, status="wanted",
            mb_release_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
        ))
        self.db.seed_request(make_request_row(
            id=42, status="replaced",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))

    def test_pipeline_all_default_excludes_replaced(self):
        status, data = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        self.assertNotIn("replaced", data)
        self.assertEqual(
            [r["id"] for r in data["wanted"]], [1],
        )

    def test_pipeline_all_include_replaced_true_fetches_replaced(self):
        status, data = self._get("/api/pipeline/all?include_replaced=true")
        self.assertEqual(status, 200)
        self.assertIn("replaced", data)
        self.assertEqual(
            [r["id"] for r in data["replaced"]], [42],
        )

    def test_post_pipeline_add_with_replaced_existing_surfaces_descendant(self):
        # Request 42 (seeded replaced in setUp) was superseded by 99 —
        # the descendant chain the add-flow surfaces.
        self.db.seed_request(make_request_row(
            id=99, status="wanted",
            mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            replaces_request_id=42,
        ))
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
        self.db.seed_request(make_request_row(
            id=42, status="wanted",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))
        status, data = self._post(
            "/api/pipeline/add",
            {"mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["current_status"], "wanted")
        self.assertNotIn("descendant_request_id", data)


class TestPipelineReplaceContract(_FakeDbWebServerCase):
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
      * 503 — RESULT_TRANSIENT, RESULT_MIRROR_UNCONFIGURED
    """

    REPLACE_REQUIRED_FIELDS: ClassVar = {
        "outcome", "request_id", "new_request_id", "current_status",
        "descendant_request_id", "error_message", "reason", "warnings",
        "processing_owner",
    }
    REQUESTS_BY_RG_FIELDS: ClassVar = {
        "id", "mb_release_id", "mb_release_group_id", "status",
        "artist_name", "album_title", "processing_owner",
    }

    def setUp(self) -> None:
        super().setUp()
        import configparser

        from lib.config import CratediggerConfig
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

    def test_replace_processing_returns_exact_owner_conflict(self):
        from lib.pipeline_db._shared import ProcessingOwnerProjection

        owner = ProcessingOwnerProjection(
            job_id=77,
            status="running",
            preview_status="evidence_ready",
        )
        with self._patch_service(
            outcome="wrong_state",
            request_id=100,
            error_message="request 100 is owned by automation job 77",
            reason="processing_locked",
            processing_owner=owner,
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "new-uuid"},
            )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "processing_locked")
        self.assertEqual(data["reason"], "processing_locked")
        self.assertEqual(data["processing_owner"], {
            "job_id": 77,
            "status": "running",
            "preview_status": "evidence_ready",
        })

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
            reason="unresolvable_target",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "bogus"},
            )
        self.assertEqual(status, 422)
        self.assertEqual(data["reason"], "unresolvable_target")

    def test_replace_rg_mismatch_returns_422(self):
        with self._patch_service(
            outcome="target_release_group_mismatch", request_id=100,
            error_message="rg mismatch",
        ):
            status, _data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "other-rg"},
            )
        self.assertEqual(status, 422)

    def test_replace_same_as_current_returns_422(self):
        with self._patch_service(
            outcome="target_same_as_current", request_id=100,
            error_message="target == source",
        ):
            status, _data = self._post(
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

    def test_replace_mirror_unconfigured_returns_503(self):
        """503 also maps to RESULT_MIRROR_UNCONFIGURED — the Discogs
        mirror is not configured on this host (R11 / AE3). The operator
        sees "mirror not set up", distinct from target_invalid (422) and
        transient. The response carries the full required-fields contract
        so the frontend renders it uniformly."""
        with self._patch_service(
            outcome="mirror_unconfigured", request_id=100,
            error_message="Discogs mirror not configured",
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "1002"},
            )
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.REPLACE_REQUIRED_FIELDS,
            "replace 503 mirror_unconfigured response",
        )
        self.assertEqual(data["outcome"], "mirror_unconfigured")
        self.assertEqual(
            data["error_message"], "Discogs mirror not configured",
        )
        self.assertIsNone(data["new_request_id"])

    def test_replace_numeric_discogs_target_passes_body(self):
        """A numeric Discogs id passes the pydantic body (the wire param
        stays ``target_mb_release_id``; the service dispatches on shape).
        The service is patched, so this pins the route accepts the body
        and returns the mapped success status."""
        with self._patch_service(
            outcome="replaced", request_id=100, new_request_id=200,
        ):
            status, data = self._post(
                "/api/pipeline/100/replace",
                {"target_mb_release_id": "1002"},
            )
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "replaced")
        self.assertEqual(data["new_request_id"], 200)

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
        self.db.seed_request(make_request_row(
            id=42, mb_release_id="old-uuid",
            mb_release_group_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            status="wanted",
            artist_name="Pet Grief", album_title="X",
        ))
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
        status, data = self._get(
            "/api/pipeline/requests-by-rg/"
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["requests"], [])

    def test_requests_by_rg_accepts_a_numeric_discogs_master_id(self):
        """A Discogs master id lives in the same mb_release_group_id
        column an MB release-group UUID does (KTD-1) — the
        Browse-search inverted-click picker's ``runInverted`` calls this
        exact route with that numeric id when the clicked row is a
        Discogs pressing under a master. Before this route's pattern
        widened to match ``/api/release-group/<id>``'s own
        ``[a-f0-9-]+`` shape, a numeric id 404'd here even though the
        underlying ``list_requests_in_release_group`` query has no
        shape assumption of its own.
        """
        self.db.seed_request(make_request_row(
            id=42, mb_release_id="883018",
            mb_release_group_id="14187",
            status="imported",
            artist_name="Lithops", album_title="Scrypt",
        ))
        status, data = self._get(
            "/api/pipeline/requests-by-rg/14187",
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(data["requests"]), 1)
        _assert_required_fields(
            self, data["requests"][0],
            self.REQUESTS_BY_RG_FIELDS,
            "requests-by-rg row",
        )

    def test_active_rgs_returns_sorted_list(self):
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="m-1",
            mb_release_group_id="rg-bbbb",
        ))
        self.db.seed_request(make_request_row(
            id=2, status="imported", mb_release_id="m-2",
            mb_release_group_id="rg-aaaa",
        ))
        # Replaced rows are frozen audit — their RG must NOT count as
        # active.
        self.db.seed_request(make_request_row(
            id=3, status="replaced", mb_release_id="m-3",
            mb_release_group_id="rg-cccc",
        ))
        status, data = self._get("/api/pipeline/active-rgs")
        self.assertEqual(status, 200)
        self.assertEqual(data["release_group_ids"], ["rg-aaaa", "rg-bbbb"])

    def test_active_rgs_empty(self):
        status, data = self._get("/api/pipeline/active-rgs")
        self.assertEqual(status, 200)
        self.assertEqual(data["release_group_ids"], [])


class TestPipelineResolveRgContract(_FakeDbWebServerCase):
    """Contract for ``POST /api/pipeline/<id>/resolve-rg``.

    Lazy-backfill ``mb_release_group_id`` for legacy rows. The Replace
    picker calls this in standard mode when the row has a null RG so the
    sibling-fetch can proceed.

    Status-code mapping:
      * 200 — ``status='resolved'`` (RG found or already set)
      * 409 — request changed while the mirror lookup was in flight
      * 404 — request not found
      * 422 — non-UUID release id (Discogs) or MB returned no RG
      * 503 — transient MB-mirror failure
    """

    RESOLVE_RG_REQUIRED_FIELDS: ClassVar = {
        "request_id", "mb_release_group_id", "status",
    }

    def _seed(self, rg: str | None,
              mb_release_id: str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
              ) -> None:
        self.db.seed_request(make_request_row(
            id=42, status="wanted",
            mb_release_id=mb_release_id,
            mb_release_group_id=rg,
        ))

    def test_resolve_rg_already_set_returns_200(self):
        """Idempotent: row already has a RG → return it untouched
        and do NOT hit the MB mirror or write to the DB."""
        self._seed("rrrrrrrr-rrrr-rrrr-rrrr-rrrrrrrrrrrr")
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
        self.assertEqual(
            self.db.request(42)["mb_release_group_id"],
            "rrrrrrrr-rrrr-rrrr-rrrr-rrrrrrrrrrrr",
        )
        # No write at all — not even a redundant same-value UPDATE
        # (the fake records every update_request_fields call).
        self.assertEqual(self.db.update_request_fields_calls, [])

    def test_resolve_rg_lazy_backfill_happy_path_returns_200(self):
        """Row has no RG → MB lookup → UPDATE row → 200."""
        self._seed(None)
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
        # The lazy backfill landed on the row itself.
        self.assertEqual(
            self.db.request(42)["mb_release_group_id"], "rrrr-rrrr-rrrr",
        )

    def test_resolve_rg_replace_during_lookup_returns_409(self):
        """A late mirror result cannot mutate or report success on ancestor."""
        self._seed(None)

        def replace_then_resolve(*_args, **_kwargs):
            self.db.supersede_request_mbid(
                42,
                new_mb_release_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                new_mb_release_group_id="ssssssss-ssss-ssss-ssss-ssssssssssss",
                new_mb_artist_id=None,
                new_artist_name="Replacement Artist",
                new_album_title="Replacement Album",
                new_year=None,
                new_country=None,
                new_tracks=[],
            )
            return {"release_group_id": "rrrr-rrrr-rrrr"}

        with patch("web.mb.get_release", side_effect=replace_then_resolve):
            status, data = self._post("/api/pipeline/42/resolve-rg", {})

        self.assertEqual(status, 409)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg conflict response",
        )
        self.assertEqual(data["status"], "conflict")
        self.assertIsNone(data["mb_release_group_id"])
        row = self.db.request(42)
        self.assertEqual(row["status"], "replaced")
        self.assertIsNone(row["mb_release_group_id"])

    def test_resolve_rg_processing_returns_exact_owner_conflict(self):
        self._seed(None)
        owner = handoff_automation_owner(self.db, 42)

        with patch(
            "web.mb.get_release",
            return_value={"release_group_id": "rrrr-rrrr-rrrr"},
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg",
                {},
            )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "transition_conflict")
        self.assertEqual(data["reason"], "processing_locked")
        self.assertEqual(data["request_id"], 42)
        self.assertEqual(data["status"], "conflict")
        self.assertEqual(data["processing_owner"], {
            "job_id": owner.id,
            "status": owner.status,
            "preview_status": owner.preview_status,
        })
        self.assertIsNone(self.db.request(42)["mb_release_group_id"])

    def test_resolve_rg_not_found_returns_404(self):
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
        self._seed(None)
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
        self.assertIsNone(self.db.request(42)["mb_release_group_id"])
        self.assertEqual(self.db.update_request_fields_calls, [])

    # U4 (docs/plans/2026-07-04-001-feat-discogs-pathway-replace-plan.md):
    # a numeric Discogs release id used to short-circuit with a 422
    # ("non_mb_release_id") before ever touching a mirror. R9 removes
    # that short-circuit — the route now resolves (and persists) the
    # Discogs master the same way the MB branch resolves the release
    # group, via the same ``update_request_fields`` DB method. The 422
    # equivalence: today's behavior asserted no mirror call and a 422;
    # the replacement scenarios below assert a resolved/masterless 200
    # (or a 503 on mirror trouble) with a real Discogs mirror call.

    def test_resolve_rg_discogs_master_found_returns_200_and_persists(self):
        """Discogs row, master exists → 200 resolved, row updated with
        the master id via the same DB method the MB branch uses."""
        self._seed(None, mb_release_id="12345")
        with patch(
            "web.discogs.get_release",
            return_value={"id": "12345", "release_group_id": "98765"},
        ) as mock_discogs:
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg discogs master-found response",
        )
        self.assertEqual(data["status"], "resolved")
        self.assertEqual(data["mb_release_group_id"], "98765")
        mock_discogs.assert_called_once_with(12345, fresh=True)
        self.assertEqual(
            self.db.request(42)["mb_release_group_id"], "98765",
        )

    def test_resolve_rg_discogs_masterless_returns_200_untouched(self):
        """Discogs row, no master → 200 'masterless' (R2), row left
        untouched — not an error shape."""
        self._seed(None, mb_release_id="12345")
        with patch(
            "web.discogs.get_release",
            return_value={"id": "12345", "release_group_id": None},
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg discogs masterless response",
        )
        self.assertEqual(data["status"], "masterless")
        self.assertIsNone(self.db.request(42)["mb_release_group_id"])
        self.assertEqual(self.db.update_request_fields_calls, [])

    def test_resolve_rg_discogs_mirror_unconfigured_returns_503(self):
        """AE3 / R11: unconfigured mirror is its own outcome, distinct
        from a lookup failure or an invalid target."""
        from web.discogs import DiscogsMirrorNotConfigured
        self._seed(None, mb_release_id="12345")
        with patch(
            "web.discogs.get_release",
            side_effect=DiscogsMirrorNotConfigured("no mirror configured"),
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg discogs mirror-unconfigured response",
        )
        self.assertEqual(data["status"], "mirror_unconfigured")
        self.assertIsNone(self.db.request(42)["mb_release_group_id"])
        self.assertEqual(self.db.update_request_fields_calls, [])

    def test_resolve_rg_discogs_transient_returns_503(self):
        """Network blip on the Discogs mirror → 503 retryable (mirrors
        the MB transient mapping)."""
        from urllib.error import URLError
        self._seed(None, mb_release_id="12345")
        with patch(
            "web.discogs.get_release",
            side_effect=URLError("connection refused"),
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg discogs transient response",
        )
        self.assertEqual(data["status"], "transient")
        self.assertIsNone(self.db.request(42)["mb_release_group_id"])
        self.assertEqual(self.db.update_request_fields_calls, [])

    def test_resolve_rg_discogs_lookup_failed_returns_422(self):
        """Non-transient, non-mirror-config Discogs failure (e.g. a
        malformed payload) falls into the generic lookup_failed branch —
        422, not 503 — and leaves the row untouched."""
        self._seed(None, mb_release_id="12345")
        with patch(
            "web.discogs.get_release",
            side_effect=KeyError("malformed payload"),
        ):
            status, data = self._post(
                "/api/pipeline/42/resolve-rg", {},
            )
        self.assertEqual(status, 422)
        _assert_required_fields(
            self, data, self.RESOLVE_RG_REQUIRED_FIELDS,
            "resolve-rg discogs lookup-failed response",
        )
        self.assertEqual(data["status"], "lookup_failed")
        self.assertIsNone(self.db.request(42)["mb_release_group_id"])
        self.assertEqual(self.db.update_request_fields_calls, [])

    def test_resolve_rg_transient_returns_503(self):
        """Network blip / timeout → 503 retryable."""
        from urllib.error import URLError
        self._seed(None)
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
        self.assertIsNone(self.db.request(42)["mb_release_group_id"])
        self.assertEqual(self.db.update_request_fields_calls, [])


class TestPipelineMergeRekeyContract(_FakeDbWebServerCase):
    """Contract for ``POST /api/pipeline/<id>/merge-rekey`` (#1089).

    Wraps ``MergeRekeyService.rekey_request`` — the CLI counterpart
    (``pipeline-cli merge-rekey``) relays this same canonical route
    (CD-QUAL-01 shape). See ``CLAUDE.md`` § "CLI ⇄ API surface symmetry".

    Status-code mapping (``lib.merge_rekey_service.MERGE_REKEY_HTTP_STATUS``):
      * 200 — rekeyed
      * 404 — not_found
      * 409 — wrong_state / library_not_at_survivor / library_still_at_stored
              / survivor_collision / rekey_refused
      * 422 — not_merged
      * 503 — mirror_unavailable / beets_unavailable
    """

    MERGE_REKEY_REQUIRED_FIELDS: ClassVar = {
        "outcome", "request_id", "old_release_id", "new_release_id",
        "beets_album_id", "beets_checked_release_id", "beets_album_ids",
        "rival_request_id", "colliding_fingerprints", "error_message",
    }

    MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
    SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"

    def _patch_service(self, **result_kwargs):
        from unittest.mock import patch as _patch

        from lib.merge_rekey_service import MergeRekeyResult
        return _patch(
            "lib.merge_rekey_service.MergeRekeyService.rekey_request",
            return_value=MergeRekeyResult(**result_kwargs),
        )

    def test_merge_rekey_success_returns_200(self):
        with self._patch_service(
            outcome="rekeyed", request_id=316, old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR, beets_album_id=19345,
        ):
            status, data = self._post("/api/pipeline/316/merge-rekey", {})
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.MERGE_REKEY_REQUIRED_FIELDS,
            "merge-rekey success response",
        )
        self.assertEqual(data["outcome"], "rekeyed")
        self.assertEqual(data["new_release_id"], self.SURVIVOR)
        self.assertEqual(data["beets_album_id"], 19345)
        self.assertNotIn("error", data)

    def test_merge_rekey_not_found_returns_404(self):
        with self._patch_service(
            outcome="not_found", request_id=9999,
            error_message="request 9999 not found",
        ):
            status, data = self._post("/api/pipeline/9999/merge-rekey", {})
        self.assertEqual(status, 404)
        _assert_required_fields(
            self, data, self.MERGE_REKEY_REQUIRED_FIELDS,
            "merge-rekey not-found response",
        )
        self.assertEqual(data["error"], "request 9999 not found")

    def test_merge_rekey_wrong_state_returns_409(self):
        with self._patch_service(
            outcome="wrong_state", request_id=42,
            error_message="request 42 is not an owner-free imported "
                           "MusicBrainz-sourced request",
        ):
            status, data = self._post("/api/pipeline/42/merge-rekey", {})
        self.assertEqual(status, 409)
        self.assertIn("error", data)

    def test_merge_rekey_not_merged_returns_422(self):
        """The #8792 refusal — Slipknot Vol. 3, no redirect, two albums."""
        with self._patch_service(
            outcome="not_merged", request_id=8792,
            old_release_id="d990b8af-0000-0000-0000-000000000000",
            new_release_id="d990b8af-0000-0000-0000-000000000000",
            beets_checked_release_id="d990b8af-0000-0000-0000-000000000000",
            beets_album_ids=(6612, 18672),
            error_message="MusicBrainz names no merge survivor",
        ):
            status, data = self._post("/api/pipeline/8792/merge-rekey", {})
        self.assertEqual(status, 422)
        _assert_required_fields(
            self, data, self.MERGE_REKEY_REQUIRED_FIELDS,
            "merge-rekey not-merged response",
        )
        self.assertEqual(sorted(data["beets_album_ids"]), [6612, 18672])
        self.assertIn("error", data)

    def test_merge_rekey_library_not_at_survivor_returns_409(self):
        with self._patch_service(
            outcome="library_not_at_survivor", request_id=42,
            new_release_id=self.SURVIVOR,
            beets_checked_release_id=self.SURVIVOR,
            error_message="Beets does not resolve exactly one album",
        ):
            status, data = self._post("/api/pipeline/42/merge-rekey", {})
        self.assertEqual(status, 409)
        self.assertIn("error", data)

    def test_merge_rekey_refused_returns_409(self):
        with self._patch_service(
            outcome="rekey_refused", request_id=42,
            new_release_id=self.SURVIVOR,
            error_message="request 42 changed underneath the rekey",
        ):
            status, data = self._post("/api/pipeline/42/merge-rekey", {})
        self.assertEqual(status, 409)
        self.assertIn("error", data)

    def test_merge_rekey_mirror_unavailable_returns_503(self):
        with self._patch_service(
            outcome="mirror_unavailable", request_id=42,
            error_message="resolution not configured",
        ):
            status, data = self._post("/api/pipeline/42/merge-rekey", {})
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.MERGE_REKEY_REQUIRED_FIELDS,
            "merge-rekey mirror-unavailable response",
        )
        self.assertIn("error", data)

    def test_merge_rekey_beets_unavailable_returns_503(self):
        """#1089 MINOR-5: a classified Beets SQLite authority failure."""
        with self._patch_service(
            outcome="beets_unavailable", request_id=42,
            error_message="Current Beets authority is unavailable; retry later.",
        ):
            status, data = self._post("/api/pipeline/42/merge-rekey", {})
        self.assertEqual(status, 503)
        _assert_required_fields(
            self, data, self.MERGE_REKEY_REQUIRED_FIELDS,
            "merge-rekey beets-unavailable response",
        )
        self.assertIn("error", data)

    def test_merge_rekey_beets_open_failure_returns_503_not_500(self):
        """#1089 MAJOR-1 (review round 2): the classified boundary must
        cover OPENING the database, not just reads through an already-open
        handle. ``rt.beets_db()`` itself raises here — before
        ``MergeRekeyService`` is even constructed, so
        ``_patch_service`` (which patches ``rekey_request``) cannot model
        this; the real seam is ``WebRuntime.beets_db`` (#1313), a real
        ``sqlite3.OperationalError`` shaped exactly like
        ``beets_authority_availability_category`` classifies (test-fidelity
        Rule B). Before the fix this reached no classified branch at all
        and 500'd with no ``outcome``.
        """
        import sqlite3

        locked = sqlite3.OperationalError("unable to open database file")
        locked.sqlite_errorcode = sqlite3.SQLITE_CANTOPEN
        with patch.object(WebRuntime, "beets_db", side_effect=locked):
            status, data = self._post("/api/pipeline/42/merge-rekey", {})
        self.assertEqual(status, 503)
        self.assertIn("error", data)
        self.assertNotIn("outcome", data)

    def test_merge_rekey_library_still_at_stored_returns_409(self):
        """#1089 MAJOR-3: Beets has not moved off the merged-away id yet."""
        with self._patch_service(
            outcome="library_still_at_stored", request_id=42,
            old_release_id=self.MERGED, new_release_id=self.SURVIVOR,
            beets_checked_release_id=self.MERGED, beets_album_ids=(111,),
            error_message="Beets still resolves an album at the merged-away id",
        ):
            status, data = self._post("/api/pipeline/42/merge-rekey", {})
        self.assertEqual(status, 409)
        _assert_required_fields(
            self, data, self.MERGE_REKEY_REQUIRED_FIELDS,
            "merge-rekey library-still-at-stored response",
        )
        self.assertEqual(data["beets_album_ids"], [111])
        self.assertIn("error", data)

    def test_merge_rekey_survivor_collision_returns_409(self):
        """#1089 MAJOR-2: a rival request already occupies the survivor —
        the response must name it, not just describe a bare refusal."""
        with self._patch_service(
            outcome="survivor_collision", request_id=42,
            old_release_id=self.MERGED, new_release_id=self.SURVIVOR,
            rival_request_id=777, colliding_fingerprints=("abc123",),
            error_message="cannot rekey request 42 onto "
                           f"{self.SURVIVOR}: request 777 already holds it",
        ):
            status, data = self._post("/api/pipeline/42/merge-rekey", {})
        self.assertEqual(status, 409)
        _assert_required_fields(
            self, data, self.MERGE_REKEY_REQUIRED_FIELDS,
            "merge-rekey survivor-collision response",
        )
        self.assertEqual(data["rival_request_id"], 777)
        self.assertEqual(data["colliding_fingerprints"], ["abc123"])
        self.assertIn("error", data)

    def test_merge_rekey_http_status_covers_every_outcome_constant(self):
        """#1089 NOTE-8: the guarded ``.get(..., 500)`` lookup exists
        precisely because an outcome missing from the mapping must not
        crash — but every real ``RESULT_*`` constant must still be
        covered, so that safety net never actually fires in production."""
        from lib import merge_rekey_service as svc

        result_constants = {
            getattr(svc, name)
            for name in dir(svc)
            if name.startswith("RESULT_")
        }
        self.assertEqual(
            result_constants, set(svc.MERGE_REKEY_HTTP_STATUS),
            "MERGE_REKEY_HTTP_STATUS is missing or has an extra outcome",
        )

    def test_merge_rekey_end_to_end_real_service_rekeys_and_moves_evidence(
        self,
    ):
        """No service mock — the real ``MergeRekeyService`` over the fake
        DB/Beets state the harness wires in, driven through the actual HTTP
        route. Proves the route really constructs and calls the service
        (rather than a shape a mock could paper over) and that the
        request's evidence lineage really follows the row (#1059/#1089).

        #1089 MAJOR-C (review round 3): the evidence-lineage witness is now
        mandatory, so this happy path needs REAL, MATCHING bytes at the
        survivor — not merely a Beets album-id seed and an unlinked
        evidence row.
        """
        import tempfile

        from lib.mb_canonical import (
            configure_canonical_base,
            configured_canonical_base,
        )
        from lib.quality import AlbumQualityEvidenceFile, AudioQualityMeasurement
        from tests.evidence_helpers import make_album_quality_evidence

        self.db.seed_request(make_request_row(
            id=316, mb_release_id=self.MERGED, status="imported",
            artist_name="Rebecca Black", album_title="Sing It",
        ))

        with tempfile.TemporaryDirectory() as tmp_dir:
            real_path = os.path.join(tmp_dir, "01 Track.flac")
            with open(real_path, "wb") as handle:
                handle.write(b"\x00" * 4096)
            stored = make_album_quality_evidence(
                mb_release_id=self.MERGED, source_path=tmp_dir,
                files=[AlbumQualityEvidenceFile(
                    relative_path="01 Track.flac", size_bytes=4096,
                    mtime_ns=1_700_000_000_000_000_000,
                    extension="flac", container="flac", codec="flac",
                )],
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=900, avg_bitrate_kbps=950,
                    median_bitrate_kbps=940, format="FLAC",
                ),
                codec="flac", container="flac", storage_format="FLAC",
            )
            self.db.upsert_album_quality_evidence(stored)
            seeded = self.db.find_album_quality_evidence(
                mb_release_id=self.MERGED,
                snapshot_fingerprint=stored.snapshot_fingerprint,
            )
            assert seeded is not None and seeded.id is not None
            self.assertTrue(
                self.db.set_request_current_evidence(316, seeded.id),
            )

            beets = FakeBeetsDB()
            beets.set_album_ids_for_release(self.SURVIVOR, [19345])
            beets.set_item_paths(self.SURVIVOR, [(19345, real_path)])

            previous_base = configured_canonical_base()
            self.addCleanup(configure_canonical_base, previous_base)
            configure_canonical_base("http://fake-mirror/ws/2")
            with (
                install_runtime(make_web_runtime(runtime(), beets=beets)),
                # The TRUE external edge (#1089 NOTE-2, review round 2) —
                # canonical_release_status is ~50 lines of real decision logic,
                # not a thin forwarder, so it is not allowlisted; this patches
                # the raw fetch one hop below it instead, exactly mirroring
                # the real ``{"payload": ..., "redirected": ...}`` envelope
                # ``_fetch_json`` produces.
                patch(
                    "lib.mb_canonical._fetch_json",
                    return_value={
                        "payload": {"id": self.SURVIVOR}, "redirected": True,
                    },
                ),
            ):
                status, data = self._post("/api/pipeline/316/merge-rekey", {})

        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "rekeyed")
        self.assertEqual(data["new_release_id"], self.SURVIVOR)
        self.assertEqual(data["beets_album_id"], 19345)
        row = self.db.request(316)
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)
        found = self.db.find_album_quality_evidence(
            mb_release_id=self.SURVIVOR,
            snapshot_fingerprint=stored.snapshot_fingerprint,
        )
        self.assertIsNotNone(found)


if __name__ == "__main__":
    unittest.main()
