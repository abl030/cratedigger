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
from lib.media_readiness import kbps_from_bps
from lib.quality import QualityRankConfig
from lib.release_identity import ReleaseIdentity

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
    ``unknown``).

    The badge reads the codec label Beets stores (``"MP3"``, ``"FLAC"``, …),
    which is a bare codec, never a ``-V`` contract — Beets has no field for
    one and this projection does no per-file header read. So an MP3 badge is
    always the measured single-ladder band, which since issue #1145 is the
    only MP3 ladder there is; before it, this call passed ``is_cbr=False`` and
    got the more generous of two. A library MP3 that a decision would promote
    on its LAME contract therefore bands below the rank the importer computes
    for it — by however far the contract outruns the measurement, which for a
    lo-fi ``-V 0`` can be several tiers.
    """
    if not format_str:
        return BAND_UNKNOWN
    fmt = format_str.split(",")[0].strip()
    if not fmt:
        return BAND_UNKNOWN
    from lib.quality import quality_rank
    return quality_rank(fmt, bitrate_kbps, cfg=cfg).name.lower()


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
    # The one bps->kbps reduction (issue #1144's ``kbps_from_bps``), not a
    # local float-divide-and-truncate: the band a release shows must not sit a
    # kbps below the rank the decision path computes from the same Beets rows.
    # Its generated pin (``test_mixed_format_precedence_is_item_order_invariant``)
    # already said the divergence was invisible only because no band edge fell
    # where floor and round disagree — moving the MP3 edges in #1145 made one.
    # ``lib/artist_compare.py`` still floors its own bps values for the artist
    # catalogue overlay; that surface is untouched here and is recorded as a
    # residual on the #1145 PR rather than claimed as fixed.
    average_kbps = (
        kbps_from_bps(sum(bitrates) // len(bitrates))
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
    release_ids: Iterable[str],
    cfg: QualityRankConfig,
) -> dict[str, str]:
    """Resolve one exact batch and band it through the shared decision."""
    requested: list[tuple[str, ReleaseIdentity]] = []
    for raw_release_id in release_ids:
        release_id = str(raw_release_id)
        identity = ReleaseIdentity.from_id(release_id)
        if identity is None:
            raise CurrentBeetsBandingIdentityError(
                f"invalid exact release identity for banding: {release_id!r}"
            )
        requested.append((release_id, identity))
    if not requested:
        return {}

    identities = list(dict.fromkeys(
        identity for _release_id, identity in requested
    ))
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
    bands = band_current_resolutions(
        {identity: observed[identity] for identity in identities},
        cfg,
    )
    return {
        release_id: bands[identity.release_id]
        for release_id, identity in requested
    }


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
