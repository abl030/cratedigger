"""Beets-library quality banding — shared by the web overlay and the CLI.

Lives in ``lib/`` (not ``web/``) so ``scripts/pipeline_cli.py`` can band the
long-tail worklist without importing the web server. ``web/server.py::
compute_library_rank`` and ``web/routes/_overlay.py::_band_from_detail`` both
delegate here, so every surface (web badges, browse/label overlays, the
long-tail list, and the CLI) shares ONE banding decision (KTD1) with no
parallel logic — only the beets-access seam and the config source differ.
"""

from __future__ import annotations

from lib.quality import QualityRankConfig

BAND_MISSING = "missing"
BAND_UNKNOWN = "unknown"


def load_rank_config() -> QualityRankConfig:
    """Runtime ``QualityRankConfig`` (config.ini), falling back to defaults.

    The CLI calls this directly — the web process's cached ``_rank_cfg`` is
    unavailable cross-process. Mirrors ``web/server.py::_rank_cfg``'s loader.
    """
    try:
        from lib.config import read_runtime_rank_config
        return read_runtime_rank_config()
    except Exception:
        return QualityRankConfig.defaults()


def compute_library_rank(
    format_str: str | None,
    bitrate_kbps: int | None,
    cfg: QualityRankConfig,
) -> str:
    """Codec-aware quality-rank label for a beets album, given the rank cfg.

    Current-state callers pass the positive-track average, never the minimum
    floor. Pure (moved from ``web/server.py``, which keeps a 2-arg wrapper
    supplying the cached cfg). Returns the lowercase rank name (``lossless`` /
    ``transparent`` / ``excellent`` / ``good`` / ``acceptable`` / ``poor`` /
    ``unknown``). Treats MP3 as VBR — cratedigger only produces VBR-V0 MP3, and
    the badge buckets barely care about the VBR/CBR distinction.
    """
    if not format_str:
        return BAND_UNKNOWN
    fmt = format_str.split(",")[0].strip()
    if not fmt:
        return BAND_UNKNOWN
    from lib.quality import quality_rank
    return quality_rank(fmt, bitrate_kbps, is_cbr=False, cfg=cfg).name.lower()


def current_library_bitrate(detail: dict[str, object]) -> int:
    """Return the positive-track average bitrate for current-state ranking.

    ``beets_bitrate`` is deliberately not a fallback: that field is the
    minimum-track floor retained for display and operator controls. A missing
    average contributes no bitrate evidence, rather than reviving the
    min-derived VBR label bug. Codec-only rules may still determine a rank.
    """
    raw = detail.get("beets_avg_bitrate")
    return raw if isinstance(raw, int) and not isinstance(raw, bool) else 0


def band_from_detail(
    rid: str,
    in_library: set[str],
    quality: dict[str, dict[str, object]],
    cfg: QualityRankConfig,
) -> str:
    """Three-way band for one release id given already-fetched membership +
    ``check_mbids_detail`` output (KTD1).

    * ``rid`` absent from the beets membership set → ``"missing"``.
    * present but no detail row / unrankable → ``"unknown"`` (has audio, never
      ``"missing"``).
    * otherwise → the lowercase ``QualityRank`` band.

    The single banding decision the web overlay and the CLI both route through
    — header, list, and sibling panel can never diverge. Membership / detail
    queries are the caller's responsibility (batched once); this is pure.
    """
    if rid not in in_library:
        return BAND_MISSING
    q = quality.get(rid)
    if not q:
        return BAND_UNKNOWN
    fmt_raw = q.get("beets_format")
    fmt = fmt_raw if isinstance(fmt_raw, str) else ""
    br = current_library_bitrate(q)
    return compute_library_rank(fmt, br, cfg)
