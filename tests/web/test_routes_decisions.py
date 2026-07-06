#!/usr/bin/env python3
"""Contract tests for web/routes/decisions.py.

Split from tests/web/test_routes_pipeline.py (#522), which itself split
from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase


class TestDecisionsRouteContracts(_FakeDbWebServerCase):
    """Contract tests for ``GET /api/pipeline/constants`` and
    ``GET /api/pipeline/simulate`` — the Decisions tab's two endpoints.
    Neither handler touches the DB (no fixtures needed).
    """

    CONSTANTS_REQUIRED_FIELDS = {"constants", "paths", "path_labels", "stages"}
    STAGE_REQUIRED_FIELDS = {
        "id", "title", "path", "function", "when", "inputs", "rules",
    }
    SIMULATE_REQUIRED_FIELDS = {
        "stage0_spectral_gate",
        "stage1_spectral", "stage2_import", "stage3_quality_gate",
        "final_status", "imported", "denylisted", "keep_searching",
        "target_final_format",
    }

    def test_pipeline_constants_contract(self):
        status, data = self._get("/api/pipeline/constants")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.CONSTANTS_REQUIRED_FIELDS,
                                "pipeline constants response")
        _assert_required_fields(self, data["stages"][0], self.STAGE_REQUIRED_FIELDS,
                                "pipeline constants stage")
        # Issue #60: rank config surfaced to UI for the Decisions tab.
        # Issue #68: within_rank_tolerance_kbps joins gate_min_rank and
        # bitrate_metric as the third rank policy field the UI renders as
        # a labeled badge at the top of the Decisions tab.
        self.assertIn("rank_gate_min_rank", data["constants"])
        self.assertIn("rank_bitrate_metric", data["constants"])
        self.assertIn("rank_within_tolerance_kbps", data["constants"])
        # Pin the type so the frontend can display it without conversion.
        self.assertIsInstance(
            data["constants"]["rank_within_tolerance_kbps"], int)

    def test_pipeline_simulate_contract(self):
        status, data = self._get(
            "/api/pipeline/simulate?is_flac=false&min_bitrate=320&is_cbr=true"
        )

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SIMULATE_REQUIRED_FIELDS,
                                "pipeline simulate response")

    def test_pipeline_simulate_threads_candidate_v0_probe_min(self):
        status, data = self._get(
            "/api/pipeline/simulate?"
            "is_flac=true&is_cbr=false&spectral_grade=likely_transcode"
            "&spectral_bitrate=160&converted_count=12"
            "&post_conversion_min_bitrate=237"
            "&candidate_v0_probe_avg=276&candidate_v0_probe_min=237"
            "&verified_lossless_target=opus%20128"
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["stage2_import"], "import")
        self.assertTrue(data["verified_lossless"])
        self.assertEqual(data["final_status"], "imported")
        self.assertFalse(data["keep_searching"])

    def test_pipeline_simulate_threads_target_format(self):
        """Real ``preview_import_from_values`` honours the ``target_format``
        query param.

        Drives the real preview engine; the simulator's own coverage lives in
        ``tests/test_import_preview.py``. This test asserts only the wire-
        boundary contract: query params → response JSON.

        Threading proof: with ``verified_lossless_target=opus+128`` set,
        the simulator's ``target_final_format`` defaults to ``opus 128``.
        Adding ``target_format=flac`` overrides that — FLAC is a no-convert
        target and the simulator emits ``target_final_format=None``. If the
        route dropped ``target_format``, the simulator would fall back to
        the lossless target and the assertion would fail.
        """
        status, data = self._get(
            "/api/pipeline/simulate?is_flac=true&min_bitrate=900"
            "&spectral_grade=genuine&converted_count=12"
            "&post_conversion_min_bitrate=128"
            "&verified_lossless_target=opus+128"
            "&target_format=flac"
        )

        self.assertEqual(status, 200)
        # target_format=flac overrides verified_lossless_target=opus 128 →
        # target_final_format is None (no conversion happens for FLAC).
        self.assertIsNone(data.get("target_final_format"))
        # Sanity: without the target_format=flac override the simulator
        # would expose verified_lossless_target as target_final_format.
        status2, data2 = self._get(
            "/api/pipeline/simulate?is_flac=true&min_bitrate=900"
            "&spectral_grade=genuine&converted_count=12"
            "&post_conversion_min_bitrate=128"
            "&verified_lossless_target=opus+128"
        )
        self.assertEqual(status2, 200)
        self.assertEqual(data2.get("target_final_format"), "opus 128")

    def test_pipeline_simulate_threads_avg_bitrate_to_stage0(self):
        """Issue #93: the web simulator must accept avg_bitrate and return
        stage0_spectral_gate so the UI can drive/display the new gate.
        """
        # VBR MP3 with high avg → stage 0 must say skipped_vbr_high_avg
        status, data = self._get(
            "/api/pipeline/simulate?"
            "is_flac=false&min_bitrate=240&is_cbr=false&is_vbr=true&avg_bitrate=245"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            data["stage0_spectral_gate"], "skipped_vbr_high_avg",
            "high-avg VBR must short-circuit the spectral gate in the "
            "web simulator (matches production lib.measurement)")

        # VBR MP3 with low avg → stage 0 must say would_run
        status, data = self._get(
            "/api/pipeline/simulate?"
            "is_flac=false&min_bitrate=126&is_cbr=false&is_vbr=true&avg_bitrate=182"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            data["stage0_spectral_gate"], "would_run",
            "Go! Team-shape transcode must trigger the gate in the simulator")


def _kwargs_to_query(kwargs: dict) -> str:
    """Serialize scenario kwargs to a query string the way the form would.

    Mirrors the route's decode rules (see ``get_pipeline_simulate`` in
    ``web/routes/decisions.py``):
      - ``None`` → omit (route's ``_str``/``_int``/``_opt_bool`` return None
        for absent keys; ``_bool`` returns False).
      - ``True`` / ``False`` → ``"true"`` / ``"false"``.
      - int → ``str(int)``.
      - str → URL-encoded (values like ``"opus 128"`` contain spaces).

    Deliberately dumb — the test depends on the route's decoders to
    round-trip these values. If the route's decoding changes, this
    helper must change too, or the equivalence guarantee breaks.
    """
    from urllib.parse import quote_plus
    parts: list[str] = []
    for k, v in kwargs.items():
        if v is None:
            continue
        if isinstance(v, bool):
            parts.append(f"{k}={'true' if v else 'false'}")
        else:
            parts.append(f"{k}={quote_plus(str(v))}")
    return "&".join(parts)


class TestDecisionsRouteDirectEquivalence(_FakeDbWebServerCase):
    """Every pure-function web route must return the same value as a
    direct call to the underlying library function with equivalent inputs.

    Why this matters (post-deploy hotfix on PR #94): the route and the
    library function were computing different answers for the same inputs
    because ``web/routes/pipeline.py`` had mixed imports of ``quality`` and
    ``lib.quality``. Python loaded the module twice; ``is EnumMember``
    compared False across the module boundary; the AVG rank policy
    silently fell through to min_bitrate in the web simulator.

    Shape-only contract tests (``SIMULATE_REQUIRED_FIELDS``) were green —
    the response had the right keys with plausible values. Equivalence
    tests catch the divergence that contract tests can't see.

    The dual-load ambiguity itself is gone (#445 item 3: one canonical
    import name per module, enforced by tests/test_no_dual_load.py and
    TestSysPathAudit), but the equivalence contract this test pins is
    independent of how a divergence arises — keep it.
    """

    # Scenario table — each is a direct-call kwargs dict. The helper
    # translates to query params for the HTTP side. Coverage spans the
    # stages the route's output exposes + the specific cases that caught
    # review rounds' issues on PR #94 (VBR gate, avg threshold, AVG
    # policy, transcode paths).
    SCENARIOS: list[tuple[str, dict]] = [
        ("cbr_mp3_basic",
         dict(is_flac=False, min_bitrate=320, is_cbr=True)),
        ("vbr_mp3_legacy_no_avg",
         dict(is_flac=False, min_bitrate=245, is_cbr=False)),
        ("vbr_mp3_genuine_v0_high_avg_skips_gate",
         dict(is_flac=False, min_bitrate=200, is_cbr=False,
              is_vbr=True, avg_bitrate=245)),
        ("vbr_mp3_low_avg_triggers_gate",
         dict(is_flac=False, min_bitrate=126, is_cbr=False,
              is_vbr=True, avg_bitrate=182,
              spectral_grade="likely_transcode", spectral_bitrate=96)),
        ("vbr_mp3_low_avg_with_existing_rejected",
         dict(is_flac=False, min_bitrate=126, is_cbr=False,
              is_vbr=True, avg_bitrate=182,
              spectral_grade="likely_transcode", spectral_bitrate=96,
              existing_min_bitrate=200)),
        ("flac_genuine_converted_to_v0",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="genuine", converted_count=10,
              post_conversion_min_bitrate=245)),
        ("flac_suspect_transcode",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="suspect", converted_count=10,
              post_conversion_min_bitrate=190)),
        ("flac_kept_lossless_target_format",
         dict(is_flac=True, min_bitrate=900, is_cbr=False,
              target_format="flac")),
        ("existing_avg_bitrate_threaded",
         dict(is_flac=False, min_bitrate=210, is_cbr=False,
              is_vbr=True, avg_bitrate=210,
              existing_min_bitrate=200, existing_avg_bitrate=245)),
        ("downgrade_rejected",
         dict(is_flac=False, min_bitrate=128, is_cbr=False,
              existing_min_bitrate=256)),
        ("spectral_clamp_with_override",
         dict(is_flac=False, min_bitrate=320, is_cbr=True,
              spectral_grade="suspect", spectral_bitrate=160,
              existing_spectral_grade="genuine",
              existing_spectral_bitrate=160)),
        ("verified_lossless_target_opus",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="genuine", converted_count=10,
              post_conversion_min_bitrate=245,
              verified_lossless_target="opus 128")),
        ("live_mountain_goats_bride_durandurfan_provisional_source",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="likely_transcode", converted_count=1,
              post_conversion_min_bitrate=214,
              candidate_v0_probe_avg=214,
              existing_min_bitrate=320, existing_avg_bitrate=320,
              existing_format="MP3", existing_is_cbr=True,
              verified_lossless_target="opus 128")),
        ("live_iron_wine_creek_maplebug_reject_after_spencertpsn_probe",
         dict(is_flac=True, min_bitrate=0, is_cbr=False,
              spectral_grade="likely_transcode", spectral_bitrate=96,
              existing_spectral_grade="likely_transcode",
              existing_spectral_bitrate=96,
              converted_count=11, post_conversion_min_bitrate=165,
              candidate_v0_probe_avg=171, existing_v0_probe_avg=228,
              existing_min_bitrate=220, existing_avg_bitrate=228,
              existing_format="MP3", existing_is_cbr=False,
              verified_lossless_target="opus 128")),
    ]

    def test_simulate_route_matches_direct_call(self):
        """For every scenario, calling full_pipeline_decision directly
        must produce the same dict as hitting /api/pipeline/simulate."""
        from lib.quality import full_pipeline_decision
        from lib.config import read_runtime_rank_config

        # The route reads the runtime cfg via `_runtime_rank_config()`.
        # In the test env there's no /var/lib/cratedigger/config.ini, so it
        # falls back to CratediggerConfig() defaults. Read it once here so
        # both sides use identical cfg.
        cfg = read_runtime_rank_config()

        for name, kwargs in self.SCENARIOS:
            with self.subTest(scenario=name):
                direct = full_pipeline_decision(**kwargs, cfg=cfg)
                status, route = self._get(
                    f"/api/pipeline/simulate?{_kwargs_to_query(kwargs)}")
                self.assertEqual(status, 200)

                self.assertEqual(
                    set(direct.keys()), set(route.keys()),
                    f"{name}: route result has different keys than direct call")
                for key in direct:
                    self.assertEqual(
                        direct[key], route[key],
                        f"{name}: {key} differs — "
                        f"direct={direct[key]!r}, route={route[key]!r}. "
                        f"Divergence here means the HTTP surface is "
                        f"computing a different answer than the library "
                        f"function, e.g. dual-module-load, cfg mismatch, "
                        f"or a param the route forgot to thread.")

    def test_constants_route_matches_direct_call(self):
        """get_pipeline_constants must return the same tree as
        get_decision_tree(cfg=runtime_cfg), plus the hardcoded spectral
        constants the route overlays."""
        from lib.quality import get_decision_tree
        from lib.config import read_runtime_rank_config

        cfg = read_runtime_rank_config()
        direct = get_decision_tree(cfg=cfg)

        status, route = self._get("/api/pipeline/constants")
        self.assertEqual(status, 200)

        # Route overlays a handful of spectral_check + policy constants
        # on top of the tree. Assert the tree structure matches, then
        # strip the overlay keys before comparing the constants dict.
        self.assertEqual(route["stages"], direct["stages"],
                         "decision tree stages must match direct call")
        self.assertEqual(route["paths"], direct["paths"])
        self.assertEqual(route["path_labels"], direct["path_labels"])

        overlay_keys = {
            "HF_DEFICIT_SUSPECT", "HF_DEFICIT_MARGINAL", "ALBUM_SUSPECT_PCT",
            "MIN_CLIFF_SLICES", "CLIFF_THRESHOLD_DB_PER_KHZ",
            "rank_gate_min_rank", "rank_bitrate_metric",
            "rank_within_tolerance_kbps",
            # Preimport audio_check_mode is loaded from runtime config so
            # the Decisions tab presets reflect the deployment (issue #91).
            "audio_check_mode",
        }
        route_consts = {k: v for k, v in route["constants"].items()
                        if k not in overlay_keys}
        self.assertEqual(
            route_consts, direct["constants"],
            "constants (sans route overlay) must match direct call")


if __name__ == "__main__":
    unittest.main()
