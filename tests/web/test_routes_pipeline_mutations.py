#!/usr/bin/env python3
"""Contract tests for pipeline mutation routes (add/update/delete/ban-source/...).

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import (
    _assert_required_fields,
    _FakeDbWebServerCase,
)

from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from lib import transitions
from lib.transitions import TransitionConflict, TransitionConflictKind


class _RacingDeleteDB(FakePipelineDB):
    """delete_request races: a superseding descendant lands concurrently,
    then the FK violation fires — the post-FK walk must see it."""

    def delete_request(self, request_id: int) -> None:
        import psycopg2.errors
        self.seed_request(make_request_row(
            id=250, status="wanted", mb_release_id="race-250",
            replaces_request_id=request_id))
        raise psycopg2.errors.ForeignKeyViolation("descendant landed")


class _RacingRequestFieldsDB(FakePipelineDB):
    """Replace the target immediately before a guarded metadata write."""

    def __init__(self) -> None:
        super().__init__()
        self.raced = False

    def update_request_fields(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **fields: object,
    ) -> bool:
        if not self.raced:
            self.raced = True
            self.supersede_request_mbid(
                request_id,
                new_mb_release_id=f"metadata-race-new-{request_id}",
                new_mb_release_group_id=None,
                new_mb_artist_id=None,
                new_artist_name="Race Artist",
                new_album_title="Correct pressing",
                new_year=None,
                new_country=None,
                new_tracks=[],
            )
        return super().update_request_fields(
            request_id,
            expected_status=expected_status,
            **fields,
        )


class TestPipelineMutationRouteContracts(_FakeDbWebServerCase):
    """Contract tests for frontend-consumed pipeline mutation routes."""

    ADD_REQUIRED_FIELDS = {"status", "id", "artist", "album", "tracks"}
    EXISTS_REQUIRED_FIELDS = {"status", "id", "current_status"}
    UPDATE_REQUIRED_FIELDS = {"status", "id", "new_status"}
    UPGRADE_REQUIRED_FIELDS = {
        "status", "id", "min_bitrate", "search_filetype_override",
    }
    SET_QUALITY_REQUIRED_FIELDS = {"status", "id", "new_status", "min_bitrate"}
    SET_INTENT_REQUIRED_FIELDS = {
        "status", "id", "intent", "target_format", "requeued",
    }
    BAN_SOURCE_REQUIRED_FIELDS = {
        "status", "username", "beets_removed", "hashes_recorded",
    }
    FORCE_IMPORT_REQUIRED_FIELDS = {
        "status", "request_id", "artist", "album", "message",
    }
    DELETE_REQUIRED_FIELDS = {"status", "id"}

    def setUp(self) -> None:
        super().setUp()
        # Request 100: the lookup target for update/upgrade/set-quality
        # bodies that pass mb_release_id="abc-123".
        self.db.seed_request(make_request_row(
            id=100, status="imported", min_bitrate=320,
            mb_release_id="abc-123",
            imported_path="/mnt/virtio/Music/Beets/Test",
        ))
        # MB add path also calls ``get_release_raw`` (for the resolver's
        # raw payload) alongside the existing ``get_release`` (for slim
        # add_request fields). Class-wide stub so individual tests only
        # need to mock ``get_release``; the resolver receives an empty
        # dict and records ``unresolved_field_missing_upstream`` for
        # catalog/track_artist as before. Tests that care about
        # catalog_number / track_artist / VA Rule 2 resolution mock
        # ``get_release_raw`` themselves.
        _patch_raw = patch(
            "web.routes.pipeline_mutations.mb_api.get_release_raw",
            return_value={},
        )
        _patch_raw.start()
        self.addCleanup(_patch_raw.stop)

    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year",
           return_value=2024)
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_contract(self, mock_get_release, _mock_rgy):
        mock_get_release.return_value = {
            "release_group_id": "rg-1",
            "artist_id": "artist-1",
            "artist_name": "Test Artist",
            "title": "Test Album",
            "year": 2024,
            "country": "US",
            "tracks": [{"title": "Track", "track_number": 1,
                        "disc_number": 1}],
        }

        status, data = self._post(
            "/api/pipeline/add", {"mb_release_id": "add-contract-mbid"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADD_REQUIRED_FIELDS,
                                "pipeline add response")

    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year",
           return_value=2014)
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_runs_plan_generation_after_set_tracks(
        self, mock_get_release, _mock_rgy,
    ):
        """Web add path generates a search plan after `set_tracks()`,
        consistent with the CLI add path. Failures must not break the
        HTTP response."""
        mock_get_release.return_value = {
            "release_group_id": "rg-1",
            "artist_id": "artist-1",
            "artist_name": "Tycho",
            "title": "Awake",
            "year": 2014,
            "country": "US",
            "tracks": [
                {"title": "Awake", "track_number": 1, "disc_number": 1},
                {"title": "Montana", "track_number": 2, "disc_number": 1},
                {"title": "L", "track_number": 3, "disc_number": 1},
                {"title": "Apogee", "track_number": 4, "disc_number": 1},
            ],
        }

        status, data = self._post(
            "/api/pipeline/add", {"mb_release_id": "abc-plan-1"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADD_REQUIRED_FIELDS,
                                "pipeline add response (plan)")
        new_id = data["id"]
        active = self.db.get_active_search_plan(new_id)
        self.assertIsNotNone(active)
        assert active is not None
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        self.assertEqual(active.plan.generator_id, SEARCH_PLAN_GENERATOR_ID)
        self.assertEqual(active.next_ordinal, 0)

    def test_pipeline_add_exists_contract(self):
        # Request 100 (setUp) already holds mb_release_id="abc-123".
        status, data = self._post("/api/pipeline/add", {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.EXISTS_REQUIRED_FIELDS,
                                "pipeline add exists response")

    @patch("web.routes.pipeline_mutations.mb_api.get_release_raw")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year")
    def test_pipeline_add_replace_during_resolution_returns_409(
        self, mock_get_group_year, mock_get_release, mock_get_raw,
    ):
        import web.server as srv

        release = {
            "release_group_id": "rg-race",
            "artist_id": "artist-race",
            "artist_name": "Race Artist",
            "title": "Race Album",
            "year": 2020,
            "country": "AU",
            "tracks": [{
                "title": "Track",
                "track_number": 1,
                "disc_number": 1,
            }],
        }
        mock_get_release.return_value = release
        mock_get_raw.return_value = {
            **release,
            "media": [{
                "tracks": [{
                    "artist-credit": [{"name": "Late Artist"}],
                }],
            }],
        }
        mock_get_group_year.return_value = 2020

        racing_db = _RacingRequestFieldsDB()
        with patch.object(srv, "db", racing_db):
            status, data = self._post(
                "/api/pipeline/add", {"mb_release_id": "add-race-source"},
            )

        self.assertEqual(status, 409)
        self.assertIn("changed during field resolution", data["error"])
        source = racing_db.get_request_by_release_id("add-race-source")
        assert source is not None
        self.assertEqual(source["status"], "replaced")
        self.assertIsNone(racing_db.get_tracks(source["id"])[0]["track_artist"])
        self.assertIsNone(racing_db.get_active_search_plan(source["id"]))

    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_mb_persists_release_group_year_reissue(
        self, mock_get_release, mock_rgy,
    ):
        """U4: reissue MB release → ``release_group_year`` is fetched
        from the mirror via the resolver service and written via
        ``update_request_fields`` after the row is inserted (the resolver
        needs a real request_id for the FK in
        ``album_request_field_resolutions``)."""
        mock_get_release.return_value = {
            "release_group_id": "rg-kid-a",
            "artist_id": "rh-1",
            "artist_name": "Radiohead",
            "title": "Kid A",
            "year": 2008,  # reissue
            "country": "US",
            "tracks": [{"title": "Everything In Its Right Place",
                        "track_number": 1, "disc_number": 1}],
        }
        mock_rgy.return_value = 2000  # release-group's first year

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "kid-a-mbid"})

        self.assertEqual(status, 200)
        mock_rgy.assert_called_once_with("rg-kid-a")
        row = self.db.get_request(_data["id"])
        assert row is not None
        self.assertEqual(row["year"], 2008)
        # add_request no longer carries release_group_year directly; the
        # resolver service writes it via update_request_fields once the
        # FK in album_request_field_resolutions is satisfiable. One
        # write, landed on the row.
        rg_year_writes = [
            c for c in self.db.update_request_fields_calls
            if "release_group_year" in c[1]
        ]
        self.assertEqual(len(rg_year_writes), 1)
        self.assertEqual(row["release_group_year"], 2000)
        self.assertIs(row["is_va_compilation"], False)

    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_mb_persists_release_group_year_original(
        self, mock_get_release, mock_rgy,
    ):
        """U4: original release MB release → ``release_group_year``
        equals the per-release year."""
        mock_get_release.return_value = {
            "release_group_id": "rg-self",
            "artist_id": "willow-1",
            "artist_name": "Willow",
            "title": "Willow",
            "year": 2007,
            "country": "AU",
            "tracks": [{"title": "And Finally I Can Breathe",
                        "track_number": 1, "disc_number": 1}],
        }
        mock_rgy.return_value = 2007

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "willow-mbid"})

        self.assertEqual(status, 200)
        row = self.db.get_request(_data["id"])
        assert row is not None
        self.assertEqual(row["year"], 2007)
        rg_year_writes = [
            c for c in self.db.update_request_fields_calls
            if "release_group_year" in c[1]
        ]
        self.assertEqual(len(rg_year_writes), 1)
        self.assertEqual(row["release_group_year"], 2007)

    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_mb_release_group_404_leaves_column_null(
        self, mock_get_release, mock_rgy,
    ):
        """U4: 404 from the release-group fetch → ``release_group_year``
        is NULL on the new row, no error raised, request still added.
        The resolver service surfaces 404 / unparseable as
        ``unresolved_field_missing_upstream``; the helper writes
        ``is_va_compilation`` but never writes a NULL
        ``release_group_year`` (only resolved values land on the row)."""
        mock_get_release.return_value = {
            "release_group_id": "rg-missing",
            "artist_id": "a-1",
            "artist_name": "A",
            "title": "T",
            "year": 2020,
            "country": "US",
            "tracks": [{"title": "Track", "track_number": 1,
                        "disc_number": 1}],
        }
        mock_rgy.return_value = None  # mirror returned 404 / unparseable

        status, data = self._post(
            "/api/pipeline/add", {"mb_release_id": "abc-rgmiss"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADD_REQUIRED_FIELDS,
                                "pipeline add response (rg 404)")
        row = self.db.get_request(data["id"])
        assert row is not None
        self.assertEqual(row["year"], 2020)
        self.assertIsNone(row["release_group_year"])
        rg_year_writes = [
            c for c in self.db.update_request_fields_calls
            if "release_group_year" in c[1]
        ]
        self.assertEqual(rg_year_writes, [],
                         "unresolved rg_year must NOT be written")

    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_mb_skips_rgy_lookup_when_no_rg_id(
        self, mock_get_release, mock_rgy,
    ):
        """U4: when MB doesn't return a ``release_group_id`` (e.g. very
        old data), the resolver's release-group-year branch sees no
        rg_id and returns ``unresolved_malformed`` without touching the
        mirror; ``release_group_year`` is left NULL on the row."""
        mock_get_release.return_value = {
            # No release_group_id key — get() returns None.
            "artist_id": "a-1",
            "artist_name": "A",
            "title": "T",
            "year": 2020,
            "country": "US",
            "tracks": [{"title": "Track", "track_number": 1,
                        "disc_number": 1}],
        }

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "abc-norg"})

        self.assertEqual(status, 200)
        mock_rgy.assert_not_called()
        rg_year_writes = [
            c for c in self.db.update_request_fields_calls
            if "release_group_year" in c[1]
        ]
        self.assertEqual(rg_year_writes, [],
                         "unresolved rg_year must NOT be written")

    @patch("web.routes.pipeline_mutations.mb_api.get_release_raw")
    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_mb_va_compilation_flag_set(
        self, mock_get_release, mock_rgy, mock_get_raw,
    ):
        """U4 web happy path for VA: a release-group typed as
        Compilation flips ``is_va_compilation=True`` once at enqueue,
        via the resolver service detecting the type on rule 2.

        VA Rule 2 reads ``release-group.primary-type`` from the raw MB
        payload — so the test mocks ``get_release_raw`` (the new
        primary fetcher) with a shape that has the rg nested. The
        slim ``get_release`` mock supplies the fields ``add_request``
        / ``set_tracks`` need.
        """
        mock_get_release.return_value = {
            "release_group_id": "rg-va",
            "artist_id": "a-1",
            "artist_name": "Various Artists",
            "title": "Tarantino Presents",
            "year": 2008,
            "country": "US",
            "tracks": [{"title": "T1", "track_number": 1,
                        "disc_number": 1}],
        }
        # Real-VA shape (post-#373): Compilation rg AND per-track
        # artist credits diverge from the album-level credit. The
        # divergence is what flips Rule 2 in `detect_va_compilation`.
        mock_get_raw.return_value = {
            "id": "va-mbid",
            "release-group": {"primary-type": "Compilation"},
            "artist-credit": [{"name": "Various Artists"}],
            "media": [{
                "position": 1,
                "tracks": [
                    {"position": 1, "title": "T1",
                     "artist-credit": [{"name": "Artist A"}]},
                    {"position": 2, "title": "T2",
                     "artist-credit": [{"name": "Artist B"}]},
                ],
            }],
        }
        mock_rgy.return_value = 2008

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "va-mbid"})
        self.assertEqual(status, 200)
        row = self.db.get_request(_data["id"])
        assert row is not None
        self.assertTrue(row["is_va_compilation"],
                        "is_va_compilation=True must land on the row")

    @patch("web.routes.pipeline_mutations.mb_api.get_release_raw")
    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_mb_va_compilation_emits_va_plan(
        self, mock_get_release, mock_rgy, mock_get_raw,
    ):
        """PR2 Apply #2: when the resolver flips ``is_va_compilation``
        on the add path, the SAME add call must produce a VA-shaped
        plan (``va_track_artist_<idx>`` slots from ``_generate_va_plan``)
        — not a normal-shaped plan that would have to wait for the
        next operator regeneration to flip.

        The active plan is fetched after the add lands — the per-test
        fake round-trips it through the real plan store.
        """
        mock_get_release.return_value = {
            "release_group_id": "rg-va",
            "artist_id": "a-1",
            "artist_name": "Various Artists",
            "title": "Tarantino Presents",
            "year": 2008,
            "country": "US",
            "tracks": [
                {"title": "T1", "track_number": 1, "disc_number": 1},
                {"title": "T2", "track_number": 2, "disc_number": 1},
                {"title": "T3", "track_number": 3, "disc_number": 1},
            ],
        }
        mock_get_raw.return_value = {
            "id": "va-plan-mbid",
            "release-group": {"primary-type": "Compilation"},
            "artist-credit": [{"name": "Various Artists"}],
            "media": [{
                "position": 1,
                "tracks": [
                    {"position": 1, "title": "T1",
                     "artist-credit": [{"name": "Artist A"}]},
                    {"position": 2, "title": "T2",
                     "artist-credit": [{"name": "Artist B"}]},
                    {"position": 3, "title": "T3",
                     "artist-credit": [{"name": "Artist C"}]},
                ],
            }],
        }
        mock_rgy.return_value = 2008

        status, data = self._post(
            "/api/pipeline/add", {"mb_release_id": "va-plan-mbid"})
        self.assertEqual(status, 200)

        new_id = data["id"]
        # VA flag landed.
        row = self.db.get_request(new_id)
        assert row is not None
        self.assertTrue(row["is_va_compilation"])

        # And the plan respects it — at least one va_track_artist_*
        # slot from ``_generate_va_plan``. Pre-fix, the add path
        # silently passed ``is_va_compilation=False`` into the
        # generator and the plan was the normal-shape (default /
        # literal / literal_flac).
        active = self.db.get_active_search_plan(new_id)
        assert active is not None
        strategies = [item.strategy for item in active.items]
        self.assertTrue(
            any(s.startswith("va_track_artist_") for s in strategies),
            f"VA add path must emit va_track_artist_* slot; got "
            f"{strategies}",
        )

    @patch("web.routes.pipeline_mutations.mb_api.get_release_raw")
    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year",
           return_value=2010)
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_mb_resolves_catalog_number_from_raw_payload(
        self, mock_get_release, _mock_rgy, mock_get_raw,
    ):
        """Fix #2 regression guard: when the raw MB payload carries
        ``label-info``, the resolver service extracts the catno and
        the helper writes it to ``album_requests.catalog_number``.

        Pre-fix, ``post_pipeline_add`` passed the slim ``get_release``
        shape to the resolver — which doesn't include ``label-info`` —
        and the catno landed as ``unresolved_field_missing_upstream``
        every single time. Post-fix the inline path also fetches
        ``get_release_raw`` and passes that as ``mb_release_payload``,
        so the catno reaches the resolver.
        """
        mock_get_release.return_value = {
            "release_group_id": "rg-1",
            "artist_id": "a-1",
            "artist_name": "Artist",
            "title": "Album",
            "year": 2010,
            "country": "GB",
            "tracks": [{"title": "T1", "track_number": 1,
                        "disc_number": 1}],
        }
        # Raw MB JSON shape with label-info present.
        mock_get_raw.return_value = {
            "id": "abc-mbid",
            "label-info": [{"catalog-number": "STRMRT-001"}],
        }

        status, _data = self._post(
            "/api/pipeline/add", {"mb_release_id": "abc-mbid"})
        self.assertEqual(status, 200)

        row = self.db.get_request(_data["id"])
        assert row is not None
        self.assertEqual(
            row["catalog_number"], "STRMRT-001",
            "resolver-extracted catalog_number must be persisted",
        )

    def test_pipeline_add_mb_integration_persists_release_group_year(self):
        """U4 integration: full add-from-web flow against ``FakePipelineDB``
        creates the new row with ``release_group_year`` populated and
        the request reads back correctly."""
        with patch("web.routes.pipeline_mutations.mb_api.get_release") as mock_rel, \
             patch("web.routes.pipeline_mutations.mb_api.get_release_group_year",
                   return_value=2000) as mock_rgy:
            mock_rel.return_value = {
                "release_group_id": "rg-kid-a",
                "artist_id": "rh-1",
                "artist_name": "Radiohead",
                "title": "Kid A",
                "year": 2008,
                "country": "US",
                "tracks": [
                    {"title": "Everything In Its Right Place",
                     "track_number": 1, "disc_number": 1},
                ],
            }
            status, data = self._post(
                "/api/pipeline/add", {"mb_release_id": "kid-a-int"})

        self.assertEqual(status, 200)
        new_id = data["id"]
        row = self.db.get_request(new_id)
        assert row is not None
        self.assertEqual(row["year"], 2008)
        self.assertEqual(row["release_group_year"], 2000)
        mock_rgy.assert_called_once_with("rg-kid-a")

    def test_pipeline_add_duplicate_does_not_regenerate(self):
        """Duplicate add returns the existing request without generating
        a second plan."""
        # Pre-seed an existing request matching the release id.
        self.db.add_request(
            mb_release_id="abc-dupe",
            artist_name="Dupe", album_title="Existing", source="request",
        )
        before = len(self.db.search_plans)

        status, data = self._post(
            "/api/pipeline/add", {"mb_release_id": "abc-dupe"})

        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "exists")
        self.assertEqual(len(self.db.search_plans), before)

    @patch("web.routes.pipeline_mutations.discogs_api.get_release")
    def test_pipeline_add_discogs_contract(self, mock_get_release):
        mock_get_release.return_value = {
            "artist_id": "3840",
            "artist_name": "Radiohead",
            "title": "OK Computer",
            "year": 1997,
            "country": "Europe",
            "tracks": [{"title": "Airbag", "track_number": 1,
                        "disc_number": 1}],
        }

        status, data = self._post("/api/pipeline/add", {"discogs_release_id": "83182"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ADD_REQUIRED_FIELDS,
                                "pipeline add discogs response")
        # Verify both columns populated on the persisted row
        row = self.db.get_request(data["id"])
        assert row is not None
        self.assertEqual(row["mb_release_id"], "83182")
        self.assertEqual(row["discogs_release_id"], "83182")

    def test_pipeline_add_discogs_exists_contract(self):
        self.db.seed_request(make_request_row(
            id=503, status="imported",
            mb_release_id="83182", discogs_release_id="83182",
        ))

        status, data = self._post("/api/pipeline/add", {"discogs_release_id": "83182"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.EXISTS_REQUIRED_FIELDS,
                                "pipeline add discogs exists response")

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_pipeline_update_contract(self, _mock_transition):
        status, data = self._post("/api/pipeline/update", {"id": 100, "status": "unsearchable"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPDATE_REQUIRED_FIELDS,
                                "pipeline update response")

    def test_pipeline_update_same_status_is_idempotent_for_operator_statuses(self):
        for index, request_status in enumerate(
            ("wanted", "imported", "unsearchable"),
            start=601,
        ):
            with self.subTest(status=request_status):
                self.db.seed_request(make_request_row(
                    id=index,
                    status=request_status,
                    mb_release_id=f"same-status-{index}",
                ))
                before = self.db.get_request(index)

                status, data = self._post(
                    "/api/pipeline/update",
                    {"id": index, "status": request_status},
                )

                self.assertEqual(status, 200)
                self.assertEqual(data["new_status"], request_status)
                self.assertEqual(self.db.get_request(index), before)

    def test_pipeline_update_imported_to_unsearchable_is_rejected(self):
        self.db.seed_request(make_request_row(
            id=604,
            status="imported",
            mb_release_id="imported-to-unsearchable",
        ))

        status, data = self._post(
            "/api/pipeline/update",
            {"id": 604, "status": "unsearchable"},
        )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "transition_conflict")
        self.assertEqual(self.db.request(604)["status"], "imported")

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_pipeline_update_maps_stale_transition_to_409_without_success(
        self, mock_transition,
    ):
        mock_transition.return_value = TransitionConflict(
            request_id=100,
            target_status="unsearchable",
            kind=TransitionConflictKind.stale_source,
            expected_status="imported",
            actual_status="replaced",
        )

        status, data = self._post(
            "/api/pipeline/update", {"id": 100, "status": "unsearchable"})

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "transition_conflict")
        self.assertEqual(data["reason"], "stale_source")
        self.assertNotIn("status", data)

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_pipeline_upgrade_contract(self, _mock_transition):
        status, data = self._post("/api/pipeline/upgrade", {"mb_release_id": "abc-123"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPGRADE_REQUIRED_FIELDS,
                                "pipeline upgrade response")

    @patch("web.routes.pipeline_mutations.finalize_request")
    @patch("web.routes.pipeline_mutations.discogs_api.get_release")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_upgrade_discogs_new_request_uses_discogs_api(
        self, mock_mb_get, mock_dg_get, _mock_transition,
    ):
        """Numeric mb_release_id (Discogs) routes to discogs_api, not mb_api."""
        mock_dg_get.return_value = {
            "id": "12856590",
            "title": "New.Old.Rare",
            "artist_name": "Blueline Medic",
            "artist_id": "3640",
            "year": 2010,
            "country": "Australia",
            "tracks": [],
        }

        status, data = self._post(
            "/api/pipeline/upgrade", {"mb_release_id": "12856590"},
        )

        self.assertEqual(status, 200)
        mock_dg_get.assert_called_once_with(12856590, fresh=True)
        mock_mb_get.assert_not_called()
        # Confirm Discogs ID is mirrored into both columns for pipeline-compat
        row = self.db.get_request(data["id"])
        assert row is not None
        self.assertEqual(row["mb_release_id"], "12856590")
        self.assertEqual(row["discogs_release_id"], "12856590")
        _assert_required_fields(self, data, self.UPGRADE_REQUIRED_FIELDS,
                                "pipeline upgrade response (discogs)")

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_pipeline_set_quality_contract(self, mock_transition):
        mock_transition.side_effect = transitions.finalize_request
        self.db.request(100)["status"] = "wanted"
        status, data = self._post(
            "/api/pipeline/set-quality",
            {"mb_release_id": "abc-123", "status": "unsearchable", "min_bitrate": 245},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_QUALITY_REQUIRED_FIELDS,
                                "pipeline set-quality response")

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_pipeline_set_quality_discogs_request_normalizes_and_falls_back(
        self, mock_transition,
    ):
        mock_transition.side_effect = transitions.finalize_request
        self.db.seed_request(make_request_row(
            id=100,
            status="wanted",
            mb_release_id="12856590",
            discogs_release_id=None,
        ))

        status, data = self._post(
            "/api/pipeline/set-quality",
            {"mb_release_id": " 0012856590 ", "status": "unsearchable", "min_bitrate": 245},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_QUALITY_REQUIRED_FIELDS,
                                "pipeline set-quality response (discogs)")

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_pipeline_upgrade_normalizes_uppercase_uuid(self, mock_transition):
        self.db.seed_request(make_request_row(
            id=1704,
            status="imported",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            min_bitrate=320,
        ))

        status, data = self._post(
            "/api/pipeline/upgrade",
            {"mb_release_id": "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"},
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.UPGRADE_REQUIRED_FIELDS,
                                "pipeline upgrade response (uppercase)")
        self.assertEqual(mock_transition.call_args.args[1], 1704)

    def test_pipeline_set_intent_contract(self):
        self.db.seed_request(make_request_row(
            id=100, status="wanted", mb_release_id="abc-123"))

        status, data = self._post("/api/pipeline/set-intent",
                                  {"id": 100, "intent": "lossless"})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SET_INTENT_REQUIRED_FIELDS,
                                "pipeline set-intent response")

    def test_pipeline_set_intent_reports_replace_race(self):
        import web.server as srv

        racing_db = _RacingRequestFieldsDB()
        racing_db.seed_request(make_request_row(
            id=1710,
            status="wanted",
            mb_release_id="intent-race-old",
            target_format=None,
        ))
        with patch.object(srv, "db", racing_db):
            status, data = self._post(
                "/api/pipeline/set-intent",
                {"id": 1710, "intent": "lossless"},
            )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "transition_conflict")
        self.assertEqual(data["actual_status"], "replaced")
        row = racing_db.get_request(1710)
        assert row is not None
        self.assertIsNone(row["target_format"])

    def test_pipeline_set_quality_reports_replace_race(self):
        import web.server as srv

        racing_db = _RacingRequestFieldsDB()
        racing_db.seed_request(make_request_row(
            id=1711,
            status="imported",
            mb_release_id="quality-race-old",
            min_bitrate=192,
        ))
        with patch.object(srv, "db", racing_db):
            status, data = self._post(
                "/api/pipeline/set-quality",
                {"mb_release_id": "quality-race-old", "min_bitrate": 320},
            )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "transition_conflict")
        self.assertEqual(data["actual_status"], "replaced")
        row = racing_db.get_request(1711)
        assert row is not None
        self.assertEqual(row["min_bitrate"], 192)

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_pipeline_ban_source_contract(self, _mock_transition):
        import web.server as srv
        release_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        self.db.seed_request(make_request_row(
            id=101, status="imported", mb_release_id=release_id,
        ))
        old_beets = srv._beets
        srv._beets = FakeBeetsDB()
        try:
            status, data = self._post(
                "/api/pipeline/ban-source",
                {"request_id": 101, "confirm": "BAN", "mb_release_id": release_id},
            )
        finally:
            srv._beets = old_beets

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.BAN_SOURCE_REQUIRED_FIELDS,
                                "pipeline ban-source response")

    def test_pipeline_ban_source_requires_confirmation(self):
        status, data = self._post(
            "/api/pipeline/ban-source",
            {
                "request_id": 100,
                "confirm": "DELETE",
                "mb_release_id": "abc-123",
            },
        )

        self.assertEqual(status, 400)
        self.assertIn("confirm", data["error"])

    @patch("web.routes.pipeline_mutations.resolve_failed_path", return_value="/tmp/Test Album")
    def test_pipeline_force_import_contract(self, _mock_resolve):
        log_id = self.db.log_download(
            100, outcome="rejected",
            validation_result={
                "failed_path": "/mnt/virtio/music/slskd/failed_imports/Test",
                "scenario": "high_distance",
            },
        )
        status, data = self._post(
            "/api/pipeline/force-import", {"download_log_id": log_id})

        self.assertEqual(status, 202)
        _assert_required_fields(self, data, self.FORCE_IMPORT_REQUIRED_FIELDS,
                                "pipeline force-import response")

    def test_pipeline_delete_contract(self):
        # No descendant — delete succeeds and the row is gone.
        status, data = self._post("/api/pipeline/delete", {"id": 100})

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DELETE_REQUIRED_FIELDS,
                                "pipeline delete response")
        self.assertIsNone(self.db.get_request(100))

    def test_pipeline_delete_with_descendant_returns_409(self):
        """Deleting a request that has a superseding descendant is
        blocked by the ON DELETE RESTRICT FK on
        ``album_requests.replaces_request_id`` (migration 023). The
        route walks the descendant chain and returns 409 with the
        list of descendant IDs so the operator can prune the lineage
        leaf-first."""
        # Real supersede chain: 100 ← 200 ← 300.
        self.db.seed_request(make_request_row(
            id=200, status="wanted", mb_release_id="chain-200",
            replaces_request_id=100))
        self.db.seed_request(make_request_row(
            id=300, status="imported", mb_release_id="chain-300",
            replaces_request_id=200))
        status, data = self._post("/api/pipeline/delete", {"id": 100})

        self.assertEqual(status, 409)
        self.assertIn("error", data)
        self.assertIn("descendant_request_ids", data)
        self.assertEqual(data["descendant_request_ids"], [200, 300])
        # The descendant block fired before any delete — the row
        # survives.
        self.assertIsNotNone(self.db.get_request(100))

    def test_pipeline_delete_fk_violation_returns_409(self):
        """Defensive race-window guard: a descendant lands between the
        route's read and the delete (modelled by a typed fake whose
        delete seeds the descendant then raises the FK violation). The
        violation surfaces as 409 rather than a 500, mirroring the
        pre-check shape."""
        import web.server as srv
        racing = _RacingDeleteDB()
        racing.seed_request(make_request_row(
            id=100, status="imported", mb_release_id="abc-123"))
        # Re-bind self.db so any assertion in this test targets the
        # live fake, never setUp's now-shadowed one.
        self.db = racing
        with patch.object(srv, "db", racing):
            status, data = self._post("/api/pipeline/delete", {"id": 100})
        self.assertEqual(status, 409)
        self.assertIn("error", data)
        self.assertEqual(data["descendant_request_ids"], [250])

    # -- fresh=True seam (Codex review on issue #101) ----------------

    @patch("web.routes.pipeline_mutations.mb_api.get_release_group_year",
           return_value=2024)
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_add_mb_fetches_release_fresh(
        self, mock_get_release, _mock_rgy,
    ):
        """POST /api/pipeline/add (MusicBrainz) MUST bypass the 24h meta
        cache — the fetched metadata is persisted into `album_requests`
        and `request_tracks`. A stale cached payload from an earlier
        browse would silently bake pre-correction artist / title / tracks
        into the pipeline DB.
        """
        mock_get_release.return_value = {
            "release_group_id": "rg-1",
            "artist_id": "artist-1",
            "artist_name": "Test Artist",
            "title": "Test Album",
            "year": 2024,
            "country": "US",
            "tracks": [{"title": "Track", "track_number": 1,
                        "disc_number": 1}],
        }

        status, _data = self._post("/api/pipeline/add",
                                   {"mb_release_id": "fresh-add-mbid"})

        self.assertEqual(status, 200)
        # ``get_release`` is now called multiple times — once by the
        # add handler and again by the U4 resolver service's release_group_id /
        # track_artist / catalog_number resolvers. Every call MUST go
        # through ``fresh=True`` so the pipeline DB never persists a
        # stale cache snapshot.
        self.assertGreaterEqual(mock_get_release.call_count, 1)
        for call in mock_get_release.call_args_list:
            self.assertEqual(call.args, ("fresh-add-mbid",))
            self.assertEqual(call.kwargs, {"fresh": True})

    @patch("web.routes.pipeline_mutations.discogs_api.get_release")
    def test_pipeline_add_discogs_fetches_release_fresh(self, mock_get_release):
        """POST /api/pipeline/add (Discogs) MUST bypass the 24h meta cache."""
        mock_get_release.return_value = {
            "artist_id": "3840",
            "artist_name": "Radiohead",
            "title": "OK Computer",
            "year": 1997,
            "country": "Europe",
            "tracks": [{"title": "Airbag", "track_number": 1,
                        "disc_number": 1}],
        }

        status, _data = self._post("/api/pipeline/add",
                                   {"discogs_release_id": "83182"})

        self.assertEqual(status, 200)
        # Same as the MB branch: post-U4 the resolver service also goes
        # through ``get_release(fresh=True)``. Every call must bypass
        # the cache.
        self.assertGreaterEqual(mock_get_release.call_count, 1)
        for call in mock_get_release.call_args_list:
            self.assertEqual(call.args, (83182,))
            self.assertEqual(call.kwargs, {"fresh": True})

    @patch("web.routes.pipeline_mutations.finalize_request")
    @patch("web.routes.pipeline_mutations.mb_api.get_release")
    def test_pipeline_upgrade_new_mb_fetches_release_fresh(
            self, mock_get_release, _mock_transition):
        """POST /api/pipeline/upgrade creating a brand-new MB request
        MUST bypass the meta cache — same rationale as add."""
        mock_get_release.return_value = {
            "artist_id": "a-1", "artist_name": "A", "title": "T",
            "year": 2024, "country": "US", "tracks": [],
        }

        status, _data = self._post(
            "/api/pipeline/upgrade",
            {"mb_release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
        )

        self.assertEqual(status, 200)
        mock_get_release.assert_called_once_with(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", fresh=True)

    @patch("web.routes.pipeline_mutations.finalize_request")
    @patch("web.routes.pipeline_mutations.discogs_api.get_release")
    def test_pipeline_upgrade_new_discogs_fetches_release_fresh(
            self, mock_get_release, _mock_transition):
        """POST /api/pipeline/upgrade creating a brand-new Discogs request
        MUST bypass the meta cache — same rationale as add."""
        mock_get_release.return_value = {
            "id": "12856590", "title": "New.Old.Rare",
            "artist_name": "Blueline Medic", "artist_id": "3640",
            "year": 2010, "country": "Australia", "tracks": [],
        }

        status, _data = self._post(
            "/api/pipeline/upgrade", {"mb_release_id": "12856590"},
        )

        self.assertEqual(status, 200)
        mock_get_release.assert_called_once_with(12856590, fresh=True)


class TestUserRequeueOverridePreservation(_FakeDbWebServerCase):
    """User-initiated requeue endpoints must preserve a stricter existing
    search_filetype_override — e.g. 'lossless' set by the quality gate after a
    CBR 320 import. Clicking Upgrade or flipping status back to wanted must not
    re-open MP3 tiers the gate intentionally closed (which would trigger
    redundant re-downloads of the same-or-worse quality).

    ban_source already does the right thing via `req.get(...) or QUALITY_UPGRADE_TIERS`;
    this class guards upgrade + update against regressing to a blind clobber,
    and pins ban_source's behaviour so future refactors don't drop it.
    """

    RELEASE_ID = "c6cd62c4-da2a-4a89-a219-adba66d6c7d4"

    def setUp(self) -> None:
        super().setUp()
        import web.server as srv
        self._srv = srv
        self._orig_beets = srv._beets
        # Beets fake: update() only hits this via album_exists / get_min_bitrate.
        # A live beets DB is the usual preceding state for a requeue.
        self.beets_db = FakeBeetsDB()
        self.beets_db._album_exists_default = True
        self.beets_db._min_bitrate_default = 320
        # Ban-source now also calls ``get_item_paths`` for the bad-rip
        # hash-capture step (plan 2026-04-29-005, U4). The fake defaults
        # to "no tracks" so legacy ban-source tests don't trip over the
        # new gate; tests that exercise hash capture seed item paths.
        # Ban-source routes through ``BeetsDB.locate`` (issue #121).
        # Default the queue to 'album present before and removed after'
        # so the legacy `album_exists.side_effect = [True, False]`
        # tests read as "exact → absent" in the new vocabulary.
        # Individual tests override this via ``_set_locate_sequence``.
        self._set_locate_sequence([
            ("exact", 1, ()),  # selectors auto-filled by the fake
            ("absent", None, ()),
        ])
        srv._beets = self.beets_db

    def _set_locate_sequence(
            self, results: list[tuple[str, object, tuple]]) -> None:
        """Queue ``(kind, album_id, selectors)`` locate outcomes on the
        fake. Extra calls reuse the final entry; blank selectors on an
        'exact' entry are auto-filled from the queried id's shape by
        the fake (the locate contract derives them from the ID)."""
        from lib.beets_db import ReleaseLocation
        entries: list[ReleaseLocation] = []
        for kind, album_id, selectors in results:
            assert kind in ("exact", "absent"), kind
            # No coercion — queue_locate_results rejects
            # production-impossible (kind, album_id, selectors) combos.
            entries.append(ReleaseLocation(
                kind="exact" if kind == "exact" else "absent",
                album_id=album_id,  # type: ignore[arg-type]
                selectors=tuple(selectors)))
        self.beets_db.queue_locate_results(entries)

    def tearDown(self) -> None:
        self._srv._beets = self._orig_beets

    def _override_passed(self, mock_transition) -> object:
        """Extract the search override from the last routed transition."""
        self.assertTrue(mock_transition.call_args_list,
                        "finalize_request was not called")
        transition = mock_transition.call_args_list[-1].args[2]
        return transition.fields.get(
            "search_filetype_override",
            "<MISSING>",
        )

    # -- Upgrade --------------------------------------------------------

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_upgrade_preserves_stricter_override(self, mock_transition):
        """Upgrade on an imported album with override='lossless' must keep it."""
        self.db.seed_request(make_request_row(
            id=1704, status="imported", min_bitrate=320,
            mb_release_id=self.RELEASE_ID,
            search_filetype_override="lossless",
        ))

        status, _data = self._post("/api/pipeline/upgrade",
                                    {"mb_release_id": self.RELEASE_ID})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition), "lossless")

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_upgrade_preserves_narrowed_override(self, mock_transition):
        """Upgrade must preserve a post-downgrade-narrow like 'lossless,mp3 v0'."""
        self.db.seed_request(make_request_row(
            id=1704, status="imported", min_bitrate=320,
            mb_release_id=self.RELEASE_ID,
            search_filetype_override="lossless,mp3 v0",
        ))

        status, _data = self._post("/api/pipeline/upgrade",
                                    {"mb_release_id": self.RELEASE_ID})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition), "lossless,mp3 v0")

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_upgrade_falls_back_to_full_tiers_when_no_override(self, mock_transition):
        """Upgrade on an imported album with no override falls back to the full ladder."""
        from lib.quality import QUALITY_UPGRADE_TIERS

        self.db.seed_request(make_request_row(
            id=1704, status="imported", min_bitrate=160,
            mb_release_id=self.RELEASE_ID,
            search_filetype_override=None,
        ))

        status, _data = self._post("/api/pipeline/upgrade",
                                    {"mb_release_id": self.RELEASE_ID})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition),
                         QUALITY_UPGRADE_TIERS)

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_upgrade_omits_min_bitrate_when_beets_lookup_misses(
            self, mock_transition):
        """Missing Beets quality data must not clear the existing DB baseline."""
        self.beets_db._min_bitrate_default = None
        self.db.seed_request(make_request_row(
            id=1704, status="imported", min_bitrate=320,
            mb_release_id=self.RELEASE_ID,
            search_filetype_override="lossless",
        ))

        status, _data = self._post("/api/pipeline/upgrade",
                                    {"mb_release_id": self.RELEASE_ID})

        self.assertEqual(status, 200)
        transition = mock_transition.call_args.args[2]
        self.assertNotIn("min_bitrate", transition.fields)
        self.assertEqual(transition.fields["search_filetype_override"], "lossless")

    # -- Update (status → wanted) ---------------------------------------

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_update_to_wanted_preserves_stricter_override(self, mock_transition):
        """Flipping an imported album back to wanted must preserve 'lossless'."""
        self.db.seed_request(make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            search_filetype_override="lossless",
        ))

        status, _data = self._post("/api/pipeline/update",
                                    {"id": 1704, "status": "wanted"})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition), "lossless")

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_update_to_wanted_falls_back_to_full_tiers_when_no_override(
            self, mock_transition):
        """Flipping imported→wanted with no override uses the full upgrade ladder."""
        from lib.quality import QUALITY_UPGRADE_TIERS

        self.db.seed_request(make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=160,
            search_filetype_override=None,
        ))

        status, _data = self._post("/api/pipeline/update",
                                    {"id": 1704, "status": "wanted"})

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition),
                         QUALITY_UPGRADE_TIERS)

    # -- Ban source (regression pin) ------------------------------------

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_ban_source_preserves_stricter_override(self, mock_transition):
        """Pin: ban_source already preserves override. Guard against future regression."""
        self.db.seed_request(make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            search_filetype_override="lossless",
        ))

        status, _data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "confirm": "BAN", "username": "baduser",
            "mb_release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        self.assertEqual(self._override_passed(mock_transition), "lossless")

    @patch("lib.beets_album_op.sp.run")
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_ban_source_clears_on_disk_quality_fields(
            self, _mock_transition, mock_subprocess):
        """After ``beet remove -d``, pipeline DB must forget on-disk quality.

        ``current_spectral_*`` and ``verified_lossless`` describe files that
        live in beets. Once the ban flow wipes those files, leaving the
        fields populated misleads every downstream consumer (wrong-matches
        UI shows ghost quality, library views, quality gate uses stale
        baselines). The write-side invariant: remove-from-beets implies
        clear-on-disk-quality. Issue #121 couples both sides via
        ``lib.release_cleanup.remove_and_reset_release``.
        """
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="", stderr="")
        self.db.seed_request(make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
            verified_lossless=False,
        ))
        # First locate: was present. Second (after remove): gone.
        self._set_locate_sequence([
            ("exact", 1, ()),
            ("absent", None, ()),
        ])

        status, _data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "confirm": "BAN", "username": "baduser",
            "mb_release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        # The full wipe ran (recorder pins the helper, not an inline
        # partial wipe) and the operator-visible fields are gone.
        self.assertEqual(self.db.clear_on_disk_quality_fields_calls, [1704])
        row = self.db.request(1704)
        self.assertIsNone(row["current_spectral_grade"])
        self.assertIsNone(row["current_spectral_bitrate"])
        self.assertFalse(row["verified_lossless"])
        self.assertIsNone(row["imported_path"])

    @patch("lib.beets_album_op.sp.run")
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_ban_source_skips_clear_when_beet_remove_failed(
            self, _mock_transition, mock_subprocess):
        """Conservative: if beets still holds the album after the remove
        attempts (e.g. permissions error, wrong column and no legacy
        fallback matched), the on-disk quality state is still accurate,
        so don't clear it. Modelled by ``locate`` returning 'exact'
        both before and after the subprocess calls. The non-zero rc
        also surfaces in ``cleanup_errors`` so the UI can tell the
        user the ban committed but the on-disk remove was incomplete
        (issue #123 PR B).
        """
        mock_subprocess.return_value = MagicMock(
            returncode=1, stdout="", stderr="beet failed")
        self.db.seed_request(make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            current_spectral_grade="genuine",
            verified_lossless=True,
        ))
        # Album is still there after the remove attempt. Seed the
        # selector tuple so the remove loop has something to iterate.
        self._set_locate_sequence([
            ("exact", 1, (f"mb_albumid:{self.RELEASE_ID}",)),
            ("exact", 1, (f"mb_albumid:{self.RELEASE_ID}",)),
        ])

        status, data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "confirm": "BAN", "username": "baduser",
            "mb_release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        # Album still on disk → the wipe must not run at all.
        self.assertEqual(self.db.clear_on_disk_quality_fields_calls, [])
        row = self.db.request(1704)
        self.assertEqual(row["current_spectral_grade"], "genuine")
        self.assertTrue(row["verified_lossless"])
        # #123 PR B + plan 2026-04-29-005 U4: the non-zero rc now
        # surfaces under ``partial_failures.cleanup_errors`` (the
        # unified shape). Distinguishes "banned cleanly" from
        # "banned but album still on disk".
        cleanup_errors = data["partial_failures"]["cleanup_errors"]
        self.assertEqual(len(cleanup_errors), 1)
        self.assertEqual(cleanup_errors[0]["reason"], "nonzero_rc")
        self.assertFalse(data["beets_removed"])

    @patch("lib.beets_album_op.sp.run")
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_ban_source_uses_discogs_selector_for_numeric_id(
            self, _mock_transition, mock_subprocess):
        """Discogs-backed requests carry a numeric ID. ``beet remove -d``
        must try ``discogs_albumid:<id>`` (the new layout) AND
        ``mb_albumid:<id>`` (the legacy layout documented in
        artist_compare.py / webui-primer.md), otherwise one of the two
        layouts goes unremoved and the banned copy stays on disk.
        After issue #121 the selectors come from ``BeetsDB.locate`` so
        every caller that asks 'is this release on disk?' agrees on
        the same selector set.
        """
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="", stderr="")
        self.db.seed_request(make_request_row(
            id=1704, status="imported", mb_release_id="12856590",
            min_bitrate=320,
        ))
        # Was there (with BOTH Discogs selectors); after both removes, gone.
        self._set_locate_sequence([
            ("exact", 1, ("discogs_albumid:12856590", "mb_albumid:12856590")),
            ("absent", None, ()),
        ])

        status, _data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "confirm": "BAN", "username": "baduser",
            "mb_release_id": " 0012856590 ",
        })

        self.assertEqual(status, 200)
        argvs = [call.args[0] for call in mock_subprocess.call_args_list]
        flattened = [token for argv in argvs for token in argv]
        self.assertIn("discogs_albumid:12856590", flattened,
                      "Must attempt the new-layout selector.")
        self.assertIn("mb_albumid:12856590", flattened,
                      "Must also attempt the legacy mb_albumid selector "
                      "so older beets libraries don't regress.")

    @patch("lib.beets_album_op.sp.run")
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_ban_source_clears_stale_state_when_album_already_gone(
            self, _mock_transition, mock_subprocess):
        """Ghost state can pre-date the handler: a user runs
        ``beet rm mb_albumid:X`` manually, then days later bans the
        source. ``locate`` returns 'absent' before ban-source even
        starts, so no ``beet remove`` runs — but the pipeline DB still
        carries the old ``current_spectral_*`` / ``imported_path``.
        The handler must still clear those fields so ``dispatch_import_core``
        doesn't keep deriving ``--override-min-bitrate`` from phantom
        baselines on the next import attempt.
        """
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="", stderr="")
        self.db.seed_request(make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
            imported_path="/mnt/virtio/Music/Beets/Stale/Path",
        ))
        # Album was already gone when ban-source ran (earlier beet rm).
        self._set_locate_sequence([
            ("absent", None, ()),
            ("absent", None, ()),
        ])

        status, _data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "confirm": "BAN", "username": "baduser",
            "mb_release_id": self.RELEASE_ID,
        })

        self.assertEqual(status, 200)
        # Phantom baselines wiped on the row itself — INCLUDING the
        # stale imported_path that misleads every downstream consumer.
        self.assertEqual(self.db.clear_on_disk_quality_fields_calls, [1704])
        row = self.db.request(1704)
        self.assertIsNone(row["current_spectral_grade"])
        self.assertIsNone(row["current_spectral_bitrate"])
        self.assertIsNone(row["imported_path"])
        # No remove ran — the handler had nothing to remove.
        mock_subprocess.assert_not_called()

    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_ban_source_rejects_request_without_server_release_identity(
        self, _mock_transition,
    ):
        """A request without an exact server identity cannot be destroyed."""
        self.db.seed_request(make_request_row(
            id=1704, status="imported",
            min_bitrate=320,
            current_spectral_grade="genuine",
            verified_lossless=True,
        ))

        status, data = self._post("/api/pipeline/ban-source", {
            "request_id": 1704, "confirm": "BAN", "username": "baduser",
            # No mb_release_id.
        })

        self.assertEqual(status, 422)
        self.assertEqual(data.get("error"), "release_mismatch")
        self.assertEqual(self.db.clear_on_disk_quality_fields_calls, [])
        row = self.db.request(1704)
        self.assertEqual(row["current_spectral_grade"], "genuine")


class TestBanSourceBadRipExtensions(_FakeDbWebServerCase):
    """Plan 2026-04-29-005 U4: bad-rip hash capture + server-side
    username resolution + importer-race 409 + unified
    ``partial_failures`` response shape on ``POST /api/pipeline/ban-source``.
    """

    RELEASE_ID = "c6cd62c4-da2a-4a89-a219-adba66d6c7d4"
    # Two distinct fake hashes (32 bytes each) — content doesn't matter
    # for the route, only that ``hash_audio_content`` returned something.
    HASH_A = b"\x01" * 32
    HASH_B = b"\x02" * 32

    def setUp(self) -> None:
        super().setUp()
        import web.server as srv
        self._srv = srv
        self._orig_beets = srv._beets
        self.beets_db = FakeBeetsDB()
        # Defaults: no tracks (tests seed item paths), and locate is
        # state-derived — nothing seeded means "absent", so
        # ``remove_and_reset_release`` is a no-op unless a test seeds
        # album ids or queues locate results.
        srv._beets = self.beets_db

        self.db.seed_request(make_request_row(
            id=1704, status="imported", mb_release_id=self.RELEASE_ID,
            min_bitrate=320,
        ))

    def tearDown(self) -> None:
        self._srv._beets = self._orig_beets

    # AE1, AE2 — body-without-username, server resolves uploader, hashes recorded.
    @patch("lib.destructive_release_service.hash_audio_content")
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_resolves_username_and_records_hashes(
            self, _mock_transition, mock_hash):
        """POST {request_id, mb_release_id} only — server resolves
        ``reported_username`` from the most recent successful
        download_log, hashes every track via ``hash_audio_content``,
        and persists them with the resolved username (R3, R5, R7).
        """
        # A prior successful download from Hxrco — the server resolves
        # the uploader from the real download_log.
        self.db.log_download(
            1704, outcome="success", soulseek_username="Hxrco")
        self.beets_db.set_item_paths(self.RELEASE_ID, [
            (1, "/mnt/Music/Beets/A/track-01.flac"),
            (2, "/mnt/Music/Beets/A/track-02.flac"),
        ])
        # Distinct digests per call so the route inserts both rows.
        mock_hash.side_effect = [self.HASH_A, self.HASH_B]

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "confirm": "BAN", "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["username"], "Hxrco")
        self.assertEqual(data["hashes_recorded"], 2)
        # Happy path: no partial_failures on the response.
        self.assertNotIn("partial_failures", data)
        # Both hashes persisted with the resolved username + reason.
        rows = self.db.bad_audio_hashes
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].request_id, 1704)
        self.assertEqual(rows[0].reported_username, "Hxrco")
        self.assertEqual(rows[0].reason, "manually banned via operator action")
        self.assertEqual(rows[0].hash_value, self.HASH_A)
        self.assertEqual(rows[0].audio_format, "flac")
        self.assertEqual(rows[1].hash_value, self.HASH_B)
        # Denylist written for the resolved user.
        self.assertEqual(len(self.db.denylist), 1)
        self.assertEqual(self.db.denylist[0].username, "Hxrco")
        self.assertEqual(
            self.db.denylist[0].reason, "manually banned via operator action")
        # #188 follow-up: EXACTLY ONE download_log row records the ban.
        ban_rows = [r for r in self.db.download_logs
                    if r.outcome == "curator_ban"]
        self.assertEqual(len(ban_rows), 1)
        ban_row = ban_rows[0]
        self.assertEqual(ban_row.request_id, 1704)
        self.assertEqual(ban_row.soulseek_username, "Hxrco")
        self.assertEqual(ban_row.outcome, "curator_ban")
        assert ban_row.beets_detail is not None
        self.assertIn("Marked bad rip", ban_row.beets_detail)
        ban_meta = json.loads(ban_row.validation_result)
        self.assertEqual(ban_meta["scenario"], "curator_ban")
        self.assertEqual(ban_meta["hashes_recorded"], 2)
        self.assertEqual(ban_meta["denylisted_username"], "Hxrco")

    # AE4 — partial hash failure does not block the ban.
    @patch("lib.destructive_release_service.hash_audio_content")
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_hash_failure_partial_does_not_block_ban(
            self, _mock_transition, mock_hash):
        """One unreadable track → ``hashes_recorded`` reflects the
        succeeded count, ``partial_failures.hash_capture_errors``
        names the failed path, denylist + remove + requeue still run.
        """
        self.db.log_download(
            1704, outcome="success", soulseek_username="Hxrco")
        self.beets_db.set_item_paths(self.RELEASE_ID, [
            (1, "/mnt/Music/Beets/A/track-01.flac"),
            (2, "/mnt/Music/Beets/A/track-02.flac"),
            (3, "/mnt/Music/Beets/A/track-03.flac"),
        ])
        # Track 2 raises; tracks 1 and 3 succeed.
        from lib.audio_hash import AudioHashError
        mock_hash.side_effect = [
            self.HASH_A,
            AudioHashError("ffmpeg failed (rc=1): truncated mp3"),
            self.HASH_B,
        ]

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "confirm": "BAN", "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["hashes_recorded"], 2)
        self.assertIn("partial_failures", data)
        errors = data["partial_failures"]["hash_capture_errors"]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["track_path"],
                         "/mnt/Music/Beets/A/track-02.flac")
        self.assertIn("truncated", errors[0]["reason"])
        # Denylist still runs for the resolved user.
        self.assertEqual(len(self.db.denylist), 1)
        # Only the two SUCCESSFUL hashes persisted.
        self.assertEqual(len(self.db.bad_audio_hashes), 2)

    # E1.1 — no successful uploader on record.
    @patch("lib.destructive_release_service.hash_audio_content")
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_no_uploader_records_hashes_with_null_username(
            self, _mock_transition, mock_hash):
        """No successful download_log → ``username: null`` returned,
        ``add_denylist`` not called, but hashes ARE recorded with
        ``reported_username=None`` (the bytes are still protected).
        """
        # No successful download on record — nothing seeded.
        self.beets_db.set_item_paths(self.RELEASE_ID, [
            (1, "/mnt/Music/Beets/A/track-01.mp3"),
        ])
        mock_hash.return_value = self.HASH_A

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "confirm": "BAN", "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertIsNone(data["username"])
        self.assertEqual(data["hashes_recorded"], 1)
        self.assertNotIn("partial_failures", data)
        # #188 follow-up: EXACTLY ONE ban event, with NULL username.
        ban_rows = [r for r in self.db.download_logs
                    if r.outcome == "curator_ban"]
        self.assertEqual(len(ban_rows), 1)
        ban_row = ban_rows[0]
        self.assertEqual(ban_row.outcome, "curator_ban")
        self.assertIsNone(ban_row.soulseek_username)
        # Hashes recorded with username=None.
        self.assertEqual(len(self.db.bad_audio_hashes), 1)
        self.assertIsNone(self.db.bad_audio_hashes[0].reported_username)
        # No denylist entry when no user resolved.
        self.assertEqual(self.db.denylist, [])

    # E1.2 — album not in beets / no track paths.
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_no_tracks_in_beets_records_capture_error(
            self, _mock_transition):
        """``get_item_paths`` empty → response includes
        ``partial_failures.hash_capture_errors`` with one
        ``no_tracks_in_beets`` entry; denylist still runs if
        username resolved; no hashes recorded.
        """
        self.db.log_download(
            1704, outcome="success", soulseek_username="Hxrco")
        # No item paths seeded — the fake returns [] for the release.

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "confirm": "BAN", "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["hashes_recorded"], 0)
        self.assertIn("partial_failures", data)
        errors = data["partial_failures"]["hash_capture_errors"]
        self.assertEqual(len(errors), 1)
        self.assertIsNone(errors[0]["track_path"])
        self.assertEqual(errors[0]["reason"], "no_tracks_in_beets")
        # Denylist still written.
        self.assertEqual(len(self.db.denylist), 1)
        self.assertEqual(self.db.denylist[0].username, "Hxrco")
        # No hashes persisted (empty list short-circuit).
        self.assertEqual(self.db.bad_audio_hashes, [])

    # E1.3 — importer race: 409 before any work.
    def test_importer_busy_returns_409_no_writes(self):
        """``import_jobs`` row exists with status running → 409, body
        ``{error: "importer_busy", retry_after_seconds: 30}``. No
        denylist, no hashes, no beets_db calls.
        """
        # An active (queued) import job for the request — the fake's
        # get_active_import_job_for_request treats queued and running
        # alike, mirroring the production active-set.
        self.db.enqueue_import_job(
            "force_import", request_id=1704,
            dedupe_key="force_import:download_log:99",
            payload={"failed_path": "/tmp/Busy Album"},
        )

        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "confirm": "BAN", "mb_release_id": self.RELEASE_ID,
             "username": "anyone"},
        )

        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "importer_busy")
        self.assertEqual(data["retry_after_seconds"], 30)
        # No mutation of any kind.
        self.assertEqual(self.db.denylist, [])
        self.assertEqual(self.db.bad_audio_hashes, [])
        self.assertEqual(self.beets_db.get_item_paths_calls, [])
        self.assertEqual(self.beets_db.locate_calls, [])

        # The active set is queued OR running — flip the job to the
        # state the importer worker would hold and re-assert the 409.
        self.db._import_jobs[0]["status"] = "running"
        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "confirm": "BAN", "mb_release_id": self.RELEASE_ID,
             "username": "anyone"},
        )
        self.assertEqual(status, 409)
        self.assertEqual(data["error"], "importer_busy")

    # E1.6 — idempotency: second click is a no-op insert.
    @patch("lib.destructive_release_service.hash_audio_content")
    @patch("web.routes.pipeline_mutations.finalize_request")
    def test_idempotent_second_click_records_zero_new_hashes(
            self, _mock_transition, mock_hash):
        """Second call inserts 0 new rows (ON CONFLICT DO NOTHING in
        the DB layer). Response is 200 with ``hashes_recorded: 0`` and
        no ``partial_failures``.

        Modeled timing window: the second click lands BEFORE the first
        ban's beets removal completes — get_item_paths still returns
        the track while locate reports absent. That impossible-looking
        combination is deliberate: it isolates the real (hash, format)
        dedupe in add_bad_audio_hashes. Do not "fix" the fixture to an
        emptied library — that reroutes through no_tracks_in_beets and
        silently loses the dedupe-path coverage.
        """
        self.db.log_download(
            1704, outcome="success", soulseek_username="Hxrco")
        self.beets_db.set_item_paths(self.RELEASE_ID, [
            (1, "/mnt/Music/Beets/A/track-01.flac"),
        ])
        mock_hash.return_value = self.HASH_A

        # First click inserts the hash for real...
        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "confirm": "BAN", "mb_release_id": self.RELEASE_ID},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["hashes_recorded"], 1)

        # ...the second click dedupes on (hash, format) and inserts 0.
        status, data = self._post(
            "/api/pipeline/ban-source",
            {"request_id": 1704, "confirm": "BAN", "mb_release_id": self.RELEASE_ID},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["hashes_recorded"], 0)
        self.assertNotIn("partial_failures", data)
        self.assertEqual(len(self.db.bad_audio_hashes), 1)

if __name__ == "__main__":
    unittest.main()
