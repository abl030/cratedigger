"""Decisions tab routes — decision-tree structure + the simulator.

Split from web/routes/pipeline.py (#522) — mirrors ``web/js/decisions.js``
on the frontend side. Neither handler here touches the DB (no ``_server()``
seam) — both are pure functions of query-string input plus the runtime
rank config.
"""

from lib.import_preview import ImportPreviewValues, preview_import_from_values
from lib.quality import get_decision_tree
from lib.spectral_check import (
    ALBUM_SUSPECT_PCT,
    CLIFF_THRESHOLD_DB_PER_KHZ,
    HF_DEFICIT_MARGINAL,
    HF_DEFICIT_SUSPECT,
    MIN_CLIFF_SLICES,
)
from web.routes._registry import RouteRegistration, route


def _runtime_rank_config():
    """Load the runtime QualityRankConfig from the same config.ini the main
    cratedigger process reads, so web simulator matches production dispatch."""
    from lib.config import read_runtime_rank_config  # type: ignore[import-not-found]

    return read_runtime_rank_config()


def get_pipeline_constants(h, params: dict[str, list[str]]) -> None:
    """Return decision tree structure + thresholds for the diagram.

    The runtime rank config is threaded into ``get_decision_tree`` so the
    transcode-detection threshold displayed in the UI tracks the live
    ``cfg.mp3_vbr.excellent`` (issue #66 follow-up). Without this, an
    operator who retuned the gate would see a stale Decisions tab while
    the actual pipeline ran at the new threshold.
    """
    rank_cfg = _runtime_rank_config()
    tree = get_decision_tree(cfg=rank_cfg)
    tree["constants"]["HF_DEFICIT_SUSPECT"] = HF_DEFICIT_SUSPECT
    tree["constants"]["HF_DEFICIT_MARGINAL"] = HF_DEFICIT_MARGINAL
    tree["constants"]["ALBUM_SUSPECT_PCT"] = ALBUM_SUSPECT_PCT
    tree["constants"]["MIN_CLIFF_SLICES"] = MIN_CLIFF_SLICES
    tree["constants"]["CLIFF_THRESHOLD_DB_PER_KHZ"] = CLIFF_THRESHOLD_DB_PER_KHZ
    # Expose the runtime rank config to the UI so the Decisions tab shows
    # the configured gate_min_rank, bitrate_metric, and the same-rank
    # tolerance. The frontend renders these three as labeled badges at
    # the top of the tab (issue #68).
    tree["constants"]["rank_gate_min_rank"] = rank_cfg.gate_min_rank.name
    tree["constants"]["rank_bitrate_metric"] = rank_cfg.bitrate_metric.value
    tree["constants"]["rank_within_tolerance_kbps"] = (
        rank_cfg.within_rank_tolerance_kbps)
    # Expose the runtime audio_check_mode so the simulator presets can
    # reflect deployments with `[Beets Validation] audio_check = off`.
    # Without this, the Decisions tab would claim corrupt downloads get
    # rejected even though run_preimport_gates() skips validation there
    # (issue #91 codex round 2).
    from lib.config import read_runtime_config  # type: ignore[import-not-found]
    tree["constants"]["audio_check_mode"] = read_runtime_config().audio_check_mode
    h._json(tree)


def get_pipeline_simulate(h, params: dict[str, list[str]]) -> None:
    """Run full_pipeline_decision() with query-string inputs."""

    def _str(key: str) -> str | None:
        v = params.get(key, [None])[0]
        return v if v else None

    def _int(key: str) -> int | None:
        v = _str(key)
        return int(v) if v else None

    def _bool(key: str) -> bool:
        v = _str(key)
        return v in ("true", "1", "yes") if v else False

    # is_vbr defaults to None (not False) so the simulator can tell
    # "not supplied, derive from is_cbr" apart from "explicit CBR".
    def _opt_bool(key: str) -> bool | None:
        v = _str(key)
        if v is None:
            return None
        return v in ("true", "1", "yes")

    preview = preview_import_from_values(
        ImportPreviewValues(
            is_flac=_bool("is_flac"),
            min_bitrate=_int("min_bitrate") or 0,
            is_cbr=_bool("is_cbr"),
            is_vbr=_opt_bool("is_vbr"),
            avg_bitrate=_int("avg_bitrate"),
            spectral_grade=_str("spectral_grade"),
            spectral_bitrate=_int("spectral_bitrate"),
            existing_min_bitrate=_int("existing_min_bitrate"),
            existing_avg_bitrate=_int("existing_avg_bitrate"),
            existing_spectral_grade=_str("existing_spectral_grade"),
            existing_spectral_bitrate=_int("existing_spectral_bitrate"),
            override_min_bitrate=_int("override_min_bitrate"),
            existing_format=_str("existing_format"),
            existing_is_cbr=_bool("existing_is_cbr"),
            new_format=_str("new_format"),
            post_conversion_min_bitrate=_int("post_conversion_min_bitrate"),
            converted_count=_int("converted_count") or 0,
            verified_lossless=_bool("verified_lossless"),
            target_format=_str("target_format"),
            verified_lossless_target=_str("verified_lossless_target"),
            # Preimport gate inputs (issue #91). Defaults preserve legacy simulator
            # behavior — a caller that omits these runs the pipeline as if audio
            # validation passed and the auto path flattened the download.
            audio_check_mode=_str("audio_check_mode") or "normal",
            audio_corrupt=_bool("audio_corrupt"),
            import_mode=_str("import_mode") or "auto",
            has_nested_audio=_bool("has_nested_audio"),
            candidate_v0_probe_avg=_int("candidate_v0_probe_avg"),
            candidate_v0_probe_min=_int("candidate_v0_probe_min"),
            existing_v0_probe_avg=_int("existing_v0_probe_avg"),
            candidate_v0_probe_kind=_str("candidate_v0_probe_kind"),
            existing_v0_probe_kind=_str("existing_v0_probe_kind"),
            supported_lossless_source=_opt_bool("supported_lossless_source"),
        ),
        cfg=_runtime_rank_config(),
    )
    h._json(preview.simulation or {})


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/pipeline/constants", get_pipeline_constants,
        "Decision tree structure + thresholds for the Decisions diagram.",
        classified=True,
    ),
    route(
        "GET", "/api/pipeline/simulate", get_pipeline_simulate,
        "Run the full pipeline decision with query-string inputs "
        "(simulator).",
        classified=True,
    ),
]
