"""Library/pipeline overlay helpers — domain logic with explicit deps.

These functions used to live inside ``web/server.py`` interleaved with
the Handler and the connection-management globals; #428's P1 (pipeline
badges silently vanishing because a helper gated on a connection global
production stopped assigning) lived exactly in that blur. Here every
function takes its DB handle as a parameter — ``web/server.py`` is the
composition root that binds its per-thread handles and re-exports the
bound names for the route modules (issue #432).

None-handle inputs degrade gracefully (empty overlay), matching the
"web UI without a beets DB still works" contract.
"""

from __future__ import annotations

from typing import Any, Protocol


class OverlayPipelineDB(Protocol):
    """The slice of PipelineDB the overlay consumes (per-consumer
    protocol, the #409 pattern — FakePipelineDB satisfies it too)."""

    def get_pipeline_overlay(
        self, mbids: list[str],
    ) -> dict[str, dict[str, Any]]: ...


class OverlayBeetsDB(Protocol):
    """The slice of BeetsDB the overlay consumes."""

    def check_mbids(self, mbids: list[str]) -> set[str]: ...

    def check_mbids_detail(
        self, mbids: list[str],
    ) -> dict[str, dict[str, object]]: ...

    def get_albums_by_artist(
        self, name: str, mbid: str = "",
    ) -> list[dict[str, object]]: ...


def serialize_row(row: dict[str, object]) -> dict[str, object]:
    """Serialize a DB row dict — convert datetime objects to ISO strings."""
    result: dict[str, object] = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            result[k] = v.isoformat()  # type: ignore[union-attr]
        else:
            result[k] = v
    return result


def check_beets_library(
    beets: "OverlayBeetsDB | None",
    mbids: list[str] | list[object],
) -> set[str]:
    """Check which MBIDs are already in the beets library."""
    return beets.check_mbids([str(m) for m in mbids]) if beets else set()


def check_beets_library_detail(
    beets: "OverlayBeetsDB | None",
    mbids: list[str] | list[object],
) -> dict[str, dict[str, object]]:
    """Check beets library with track counts and audio quality."""
    return beets.check_mbids_detail([str(m) for m in mbids]) if beets else {}


def get_library_artist(
    beets: "OverlayBeetsDB | None",
    artist_name: str,
    mb_artist_id: str = "",
) -> list[dict[str, object]]:
    """Get albums by an artist from the beets library."""
    if not beets:
        return []
    return beets.get_albums_by_artist(artist_name, mb_artist_id)


def check_pipeline(pdb: "OverlayPipelineDB | None", mbids) -> dict:
    """Check which MBIDs are already in the pipeline DB. Returns dict of mbid → info."""
    if not mbids or pdb is None:
        return {}
    return pdb.get_pipeline_overlay([str(m) for m in mbids])


def enrich_with_pipeline(
    pdb: "OverlayPipelineDB | None",
    albums: list[dict[str, object]],
) -> None:
    """Add pipeline_status/upgrade_queued to album dicts. Mutates in place."""
    if pdb is None:
        return
    mbids = [str(a["mb_albumid"]) for a in albums if a.get("mb_albumid")]
    if not mbids:
        return
    pipeline_info = check_pipeline(pdb, mbids)
    for a in albums:
        pi = pipeline_info.get(a.get("mb_albumid"))
        if pi:
            apply_pipeline_bitrate_override(a, pi)


def apply_pipeline_bitrate_override(album: dict, pipeline_info: dict) -> None:
    """Apply pipeline DB min_bitrate and upgrade_queued flag to a beets album dict.

    Pipeline DB stores kbps, beets stores bps. Only overrides when pipeline is higher.
    """
    if pipeline_info.get("status") == "wanted" and (pipeline_info.get("search_filetype_override") or pipeline_info.get("target_format")):
        album["upgrade_queued"] = True
    pi_br = pipeline_info.get("min_bitrate")
    a_br = album.get("min_bitrate")
    if pi_br is not None and a_br is not None:
        pi_br_bps = pi_br * 1000  # kbps → bps
        if pi_br_bps > a_br:
            album["min_bitrate"] = pi_br_bps


_rank_cfg_cache = None


def _rank_cfg():
    """Cached QualityRankConfig from runtime config.ini.

    Falls back to defaults if the ini can't be read (e.g. tests / first-
    boot). The cache is module-scoped and set-once — a concurrent first
    request can compute it twice, but the value is deterministic so the
    race is benign. A deploy restart picks up any [Quality Ranks]
    changes via the cratedigger-web service restart deploy.md guarantees.
    """
    global _rank_cfg_cache
    if _rank_cfg_cache is None:
        try:
            from lib.config import read_runtime_rank_config
            _rank_cfg_cache = read_runtime_rank_config()
        except Exception:
            from lib.quality import QualityRankConfig
            _rank_cfg_cache = QualityRankConfig.defaults()
    return _rank_cfg_cache


def compute_library_rank(format_str: str | None, bitrate_kbps: int | None) -> str:
    """Codec-aware quality rank label for a beets album.

    Single source of truth for the in-library badge's tier — same logic
    the import gate uses, so what you see in the badge matches what the
    pipeline's quality decisions act on. Returns lowercase rank name
    ('lossless', 'transparent', 'excellent', 'good', 'acceptable',
    'poor', 'unknown'). Treats MP3 as VBR — cratedigger's pipeline only
    produces VBR-V0 MP3, and for the bitrate buckets the badge cares
    about the VBR-vs-CBR distinction barely matters at the display level.

    Thin wrapper supplying the web process's cached rank cfg; the pure
    decision lives in ``lib.banding`` so the CLI bands without importing web.
    """
    from lib.banding import compute_library_rank as _band_rank
    return _band_rank(format_str, bitrate_kbps, _rank_cfg())
