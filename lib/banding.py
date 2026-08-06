"""Beets-library quality banding shared by web and CLI projections.

Lives in ``lib/`` (not ``web/``) so ``scripts/pipeline_cli.py`` can band the
long-tail worklist without importing the web server. Long-tail callers use one
exact-resolution snapshot decision here; browse/label overlay callers retain
the legacy pre-fetched detail adapter while sharing the same rank primitive.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Protocol

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    _reduce_album_format,
)
from lib.quality import QualityRankConfig
from lib.release_identity import ReleaseIdentity
from lib.request_identity import (
    acceptable_identities,
    merge_union_resolutions,
)

BAND_MISSING = "missing"
BAND_UNKNOWN = "unknown"


class CurrentBeetsBandingAmbiguityError(RuntimeError):
    """One or more exact releases cannot authorize a current band."""

    ambiguities: tuple[CurrentBeetsAmbiguous, ...]

    def __init__(
        self,
        ambiguities: Sequence[CurrentBeetsAmbiguous],
    ) -> None:
        values = tuple(ambiguities)
        if not values:
            raise ValueError("banding ambiguity error needs an ambiguity")
        self.ambiguities = values
        detail = ", ".join(
            f"{value.identity.release_id}:{value.reason}"
            for value in values
        )
        super().__init__(f"ambiguous current Beets releases: {detail}")


class CurrentBeetsBandingIdentityError(ValueError):
    """A banding caller supplied a malformed release identity."""


class CurrentBeetsBandingUnavailableError(RuntimeError):
    """The exact resolver did not return a requested release observation."""


class CurrentBeetsResolver(Protocol):
    """Exact current-library batch resolver consumed by banding."""

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]: ...


def load_rank_config() -> QualityRankConfig:
    """Runtime ``QualityRankConfig`` (config.ini), falling back to defaults.

    The CLI calls this directly — the web process's cached ``_rank_cfg`` is
    unavailable cross-process. Mirrors ``web/server.py::_rank_cfg``'s loader.
    """
    try:
        from lib.config import read_runtime_rank_config
        return read_runtime_rank_config()
    except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
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


def _band_current_unique(
    current: CurrentBeetsUnique,
    cfg: QualityRankConfig,
) -> str:
    """Rank one exact resolution from its coherent item snapshot."""
    album_format = _reduce_album_format(
        {item.format for item in current.items if item.format},
        cfg,
    )
    bitrates = [
        item.bitrate
        for item in current.items
        if item.bitrate is not None and item.bitrate > 0
    ]
    average_kbps = (
        int(sum(bitrates) / len(bitrates) / 1000)
        if bitrates else 0
    )
    return compute_library_rank(album_format, average_kbps, cfg)


def band_current_resolutions(
    resolutions: Mapping[ReleaseIdentity, CurrentBeetsResolution],
    cfg: QualityRankConfig,
) -> dict[str, str]:
    """Band one coherent exact-resolution batch without cardinality loss.

    Only :class:`CurrentBeetsMissing` authorizes ``missing``. Unique releases
    derive their band from the items carried by that exact resolution. Any
    ambiguity aborts the complete batch before a payload is constructed.
    """
    ambiguities = tuple(
        resolution
        for resolution in resolutions.values()
        if isinstance(resolution, CurrentBeetsAmbiguous)
    )
    if ambiguities:
        raise CurrentBeetsBandingAmbiguityError(ambiguities)

    bands: dict[str, str] = {}
    for identity, resolution in resolutions.items():
        if resolution.identity != identity:
            raise CurrentBeetsBandingUnavailableError(
                "current Beets resolution identity mismatch: "
                f"requested={identity.release_id}, "
                f"observed={resolution.identity.release_id}"
            )
        if isinstance(resolution, CurrentBeetsMissing):
            bands[identity.release_id] = BAND_MISSING
        elif isinstance(resolution, CurrentBeetsUnique):
            bands[identity.release_id] = _band_current_unique(resolution, cfg)
    return bands


def resolve_current_release_bands(
    beets: CurrentBeetsResolver,
    requests: Iterable[Mapping[str, object]],
    cfg: QualityRankConfig,
) -> dict[str, str]:
    """Band a request cohort over each row's identity union (#1059).

    Keyed by the **acquisition** release id, because that is what the
    long-tail row displays and what the caller asked about. The MusicBrainz
    merge survivor, when one is stored, only widens which albums may answer
    — it never becomes the key.

    Still one batched query for the whole cohort, merged or not.
    """
    rows = list(requests)
    requested: list[tuple[str, tuple[ReleaseIdentity, ...]]] = []
    for row in rows:
        acceptable = acceptable_identities(row)
        if not acceptable:
            raise CurrentBeetsBandingIdentityError(
                "invalid exact release identity for banding: "
                f"{row.get('mb_release_id')!r}/"
                f"{row.get('discogs_release_id')!r}"
            )
        requested.append((acceptable[-1].release_id, acceptable))
    if not requested:
        return {}

    identities: list[ReleaseIdentity] = []
    for _key, acceptable in requested:
        for identity in acceptable:
            if identity not in identities:
                identities.append(identity)

    observed = beets.resolve_current_releases(identities)
    omitted = [
        identity.release_id
        for identity in identities
        if identity not in observed
    ]
    if omitted:
        raise CurrentBeetsBandingUnavailableError(
            "current Beets resolver omitted exact release identities: "
            + ", ".join(omitted)
        )

    folded: dict[ReleaseIdentity, CurrentBeetsResolution] = {}
    for _key, acceptable in requested:
        folded[acceptable[-1]] = merge_union_resolutions(
            acceptable[-1],
            [observed[identity] for identity in acceptable],
        )
    bands = band_current_resolutions(folded, cfg)
    return {key: bands[key] for key, _acceptable in requested}


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

    Legacy browse/label overlay consumers route through this adapter. The
    long-tail web/CLI surfaces instead use ``resolve_current_release_bands``
    so an ambiguous exact resolution can never disappear between membership
    and detail projections.
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
