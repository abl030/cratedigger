"""Dual-source field resolver — MusicBrainz + Discogs.

One service module that resolves the external-metadata fields cratedigger
populates from MB or Discogs (release_group_year, release_group_id,
track_artist, catalog_number) and records every resolution attempt into
the ``album_request_field_resolutions`` side table (migration 030).

Single uniform shape is returned regardless of upstream source: every
caller gets a ``ResolverResult`` carrying a status enum plus the resolved
value (or ``None``). The service body never imports from
``web.routes`` or ``scripts``; those are caller concerns.

Plan reference: U2 of
``docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md``.

VA detection lives here too — three rules in order (primary identity,
release-group primary-type, joinphrase divergence). Captures the
false-negative classes the brainstorm's narrowest case missed
(Tarantino-presents, label samplers, split-artist compilations).

This is the canonical implementation of the "service first, glue
follows" pattern (``docs/solutions/architecture/service-first-then-glue.md``)
for PR1 of search-plan iteration 2. Inline-at-enqueue (U4) and the
backfill script (U3) are thin adapters on top of these resolvers.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import socket
import threading
import time
import urllib.error
from typing import Any, Callable, Literal, Protocol

import msgspec


logger = logging.getLogger(__name__)


# Status enum -- mirrors the working values in migration 030's comment.
# The DB has no CHECK constraint; this module is the source of truth.
#
# ``unresolved_internal_error`` distinguishes programmer-error escapes
# (e.g. ``KeyError`` in the orchestrator) from genuine transient mirror
# failures (``unresolved_mirror_unavailable``). The two used to be
# conflated under "mirror_unavailable" — that's a triage bug; bugs that
# look transient get re-run forever instead of being seen.
ResolverStatus = Literal[
    "resolved",
    "unresolved_404",
    "unresolved_4xx_client",
    "unresolved_malformed",
    "unresolved_mirror_unavailable",
    "unresolved_timeout",
    "unresolved_field_missing_upstream",
    "unresolved_internal_error",
]


# Field-name enum -- the side table's ``field_name`` column. Pinning the
# names here keeps cohort queries ("show me all unresolved track_artist
# rows") and the operator-facing triage stable.
FIELD_RELEASE_GROUP_YEAR = "release_group_year"
FIELD_RELEASE_GROUP_ID = "release_group_id"
FIELD_TRACK_ARTIST = "track_artist"
FIELD_CATALOG_NUMBER = "catalog_number"


# Canonical VA identity constants — single declaration site at
# ``lib/va_identity.py``. Re-exported here so the existing import paths
# (``from lib.field_resolver_service import MB_VA_ARTIST_MBID``) keep
# working without forcing every caller to learn the new module.
from lib.va_identity import (  # noqa: E402
    DISCOGS_VA_ARTIST_ID,
    MB_VA_ARTIST_MBID,
)


# Network-style exceptions we treat as transient mirror failures
# (``unresolved_mirror_unavailable``). Subclasses of ``URLError`` and
# ``ConnectionError`` are caught broadly; timeouts get classified
# separately downstream because their retry window differs.
_MIRROR_UNAVAILABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    urllib.error.URLError,
    ConnectionError,
    json.JSONDecodeError,
)


class ResolverResult(msgspec.Struct, kw_only=True):
    """Typed result of a single field resolution attempt.

    In-process container — the resolver service produces it, the
    backfill/enqueue paths consume it, and the side-table row is built
    from ``status`` + ``reason_code`` separately. ``msgspec.Struct``
    purely for fast attribute access and clear field types; not a
    wire-boundary type. The ``status`` is a tagged enum; consumers
    branch on ``status == "resolved"`` rather than truthiness of
    ``value``.

    ``value`` is narrowed to ``int | str | None`` — the union of every
    concrete resolver's output: ``int`` for release_group_year, ``str``
    for release_group_id, catalog_number, and track_artist. Listing
    the concrete types lets static analysis catch a future resolver
    that returns a wider shape without an explicit Union widening.
    """

    field_name: str
    value: int | str | None = None
    status: ResolverStatus
    reason_code: str | None = None


# Injectable collaborator protocols. The defaults import lazily inside
# helper wrappers so the service module is importable in environments
# that don't pull in ``web.mb`` / ``web.discogs`` (e.g. lib-only tests).
class _PdbRecorder(Protocol):
    def record_field_resolution(
        self,
        request_id: int,
        field_name: str,
        status: str,
        reason_code: str | None,
    ) -> None: ...


MBReleaseGroupYearFn = Callable[[str], int | None]
"""``mb_get_release_group_year(rg_mbid) -> int | None``. Raises
``urllib.error.HTTPError`` (code=404) on missing release-group, other
URLError subclasses on transient transport failures, ``TimeoutError``
or ``socket.timeout`` on timeouts."""

DiscogsMasterYearFn = Callable[[str], int | None]
"""``discogs_get_master_year(master_id) -> int | None``. Same error
discipline as the MB callable. Returns ``None`` when the master record
exists but carries no ``year`` field — the resolver maps that to
``unresolved_field_missing_upstream``."""

MBReleaseFn = Callable[..., dict[str, Any]]
"""``mb_get_release(mbid, *, fresh: bool=False) -> dict``. Used by the
release_group_id resolver and the track-artist resolver."""

DiscogsReleaseFn = Callable[..., dict[str, Any]]
"""``discogs_get_release(release_id, *, fresh: bool=False) -> dict``.
Used by the release_group_id resolver and the track-artist resolver."""


def _looks_numeric(value: Any) -> bool:
    """Heuristic: does this id look like a Discogs numeric id (vs a UUID)?

    Used by the release_group_year resolver to dispatch between the MB
    mirror (UUIDs) and the Discogs mirror (numeric master IDs). The
    pipeline DB stores both in ``mb_release_group_id`` — historical
    rows where Discogs was the source carry the numeric master id in
    that column. Whitespace-stripped string of decimal digits matches.
    """
    if value is None:
        return False
    s = str(value).strip()
    if not s:
        return False
    return s.isdigit()


def _is_timeout_exception(exc: BaseException) -> bool:
    """Distinguish a wall-clock timeout from other URLError variants.

    ``TimeoutError`` / ``socket.timeout`` are the direct signals.
    ``urllib.error.URLError`` wraps the underlying reason; if its
    ``reason`` is a timeout we treat it as such so retry-window
    classification is correct (timeouts retry in 1d, generic mirror
    failures also retry in 1d — but unresolved_404 is sticky-30d).
    """
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return True
        reason_text = str(reason).lower()
        return "timed out" in reason_text or "timeout" in reason_text
    return False


def _classify_lookup_exception(
    exc: BaseException,
) -> tuple[ResolverStatus, str]:
    """Map an exception from a mirror call to a status + reason_code.

    Centralised so the four resolvers share the same retry-window
    semantics:

    - ``HTTPError(404)`` → ``unresolved_404`` (sticky 30d). Treated
      as its own bucket because "release doesn't exist" is the
      most common upstream cause and operators triage it
      differently from other client errors.
    - ``HTTPError(4xx)`` other than 404 / 408 → ``unresolved_4xx_client``
      (sticky). Permanent client-error semantics — retrying the same
      input gives the same answer (400 Bad Request, 410 Gone, 422
      Unprocessable Entity, etc.). 408 Request Timeout stays in the
      transient bucket. Fix #3 from the 2026-05-25 deploy: 74 wanted
      rows hit MB 400 Bad Request and were retried-forever as
      ``unresolved_mirror_unavailable`` instead of being surfaced.
    - ``HTTPError(5xx)`` → ``unresolved_mirror_unavailable`` (retry 1d).
      Server-side, legitimate to retry.
    - ``TimeoutError`` / ``socket.timeout`` / ``HTTPError(408)`` →
      ``unresolved_timeout`` (retry 1d).
    - Other URL/socket/JSON errors → ``unresolved_mirror_unavailable``
      (retry 1d).

    Anything else re-raises — callers are responsible for catching
    programmer errors.
    """
    if isinstance(exc, urllib.error.HTTPError):
        if exc.code == 404:
            return ("unresolved_404", "http_404")
        if 400 <= exc.code < 500 and exc.code != 408:
            return ("unresolved_4xx_client", f"http_{exc.code}")
        # 408 falls through to timeout, 5xx falls through to mirror_unavailable.
    if _is_timeout_exception(exc):
        return ("unresolved_timeout", type(exc).__name__)
    if isinstance(exc, _MIRROR_UNAVAILABLE_EXCEPTIONS):
        return ("unresolved_mirror_unavailable", type(exc).__name__)
    raise exc  # programmer error; surface it


# Default collaborator wrappers. Imports stay lazy so this module is
# importable from contexts that don't have ``web.mb`` / ``web.discogs``
# on the path (e.g. fast unit-test boots).


def _default_mb_get_release_group_year(rg_mbid: str) -> int | None:
    from web.mb import get_release_group_year
    return get_release_group_year(rg_mbid)


def _default_discogs_get_master_year(master_id: str) -> int | None:
    from web.discogs import get_master_releases
    data = get_master_releases(int(master_id))
    if not isinstance(data, dict):
        return None
    raw = data.get("first_release_date")
    if not raw:
        return None
    try:
        return int(str(raw)[:4])
    except (ValueError, IndexError):
        return None


def _default_mb_get_release(
    mbid: str, *, fresh: bool = False,
) -> dict[str, Any]:
    # The resolvers need the *raw* MB JSON shape — they extract
    # ``label-info`` (for catalog_number), ``media[].tracks[].artist-
    # credit`` (for track_artist), and ``release-group`` nested fields
    # (for VA Rule 2). The slimmed shape returned by
    # ``web.mb.get_release`` drops all three. Pre-2026-05-25 deploy
    # the resolver service silently downgraded those fields to
    # ``unresolved_field_missing_upstream`` for every MB request
    # because the wrong fetcher was wired in here.
    from web.mb import get_release_raw
    return get_release_raw(mbid, fresh=fresh)


def _default_discogs_get_release(
    release_id: str, *, fresh: bool = False,
) -> dict[str, Any]:
    from web.discogs import get_release
    return get_release(int(release_id), fresh=fresh)


# === Helpers for record-keeping =========================================


def _record(
    pdb: _PdbRecorder,
    request_id: int,
    field_name: str,
    result: ResolverResult,
) -> None:
    """Persist the result into ``album_request_field_resolutions``.

    Service guarantee: every resolver call writes exactly one side-table
    row (upsert; the DB layer increments ``attempts`` on conflict). This
    helper exists so the four resolvers share the same call shape and
    we never accidentally skip a record on a code branch.
    """
    try:
        pdb.record_field_resolution(
            request_id=request_id,
            field_name=field_name,
            status=result.status,
            reason_code=result.reason_code,
        )
    except Exception:  # noqa: BLE001
        # Recording is best-effort observability; an upsert failure must
        # not block the caller from using the resolved value. The
        # resolver itself already succeeded; the value is what the
        # caller wanted.
        logger.exception(
            "record_field_resolution failed for request=%d field=%s "
            "status=%s; continuing without persisting",
            request_id, field_name, result.status,
        )


# === Resolver: release_group_year =======================================


def resolve_release_group_year(
    request: dict[str, Any],
    pdb: _PdbRecorder,
    *,
    mb_get_release_group_year: MBReleaseGroupYearFn | None = None,
    discogs_get_master_year: DiscogsMasterYearFn | None = None,
) -> ResolverResult:
    """Resolve a request's release-group year via MB or Discogs.

    Dispatch rule: when ``mb_release_group_id`` is numeric (legacy
    Discogs-sourced row) OR the request carries a ``discogs_release_id``
    but no MB rg, use the Discogs mirror's master endpoint. Otherwise
    use the MB mirror's release-group endpoint.
    """
    request_id = int(request["id"])
    rg_id = request.get("mb_release_group_id")
    discogs_release_id = request.get("discogs_release_id")

    # No identifying field at all -- malformed input.
    if not rg_id and not discogs_release_id:
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_YEAR,
            status="unresolved_malformed",
            reason_code="missing_rg_id",
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
        return result

    # Discogs branch: numeric rg_id OR pure-discogs row.
    use_discogs = (
        _looks_numeric(rg_id)
        or (discogs_release_id and not rg_id)
    )

    if use_discogs:
        fetch = discogs_get_master_year or _default_discogs_get_master_year
        master_id = str(rg_id) if rg_id else None
        if not master_id:
            # discogs_release_id is the release, not the master --
            # without a master_id we can't ask the master endpoint.
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status="unresolved_malformed",
                reason_code="missing_discogs_master_id",
            )
            _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
            return result
        try:
            year = fetch(master_id)
        except BaseException as exc:  # noqa: BLE001
            status, reason = _classify_lookup_exception(exc)
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status=status,
                reason_code=reason,
            )
            _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
            return result
        if year is None:
            # The master record exists (no exception) but carries no
            # parseable year -- genuinely missing upstream.
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status="unresolved_field_missing_upstream",
                reason_code="discogs_master_no_year",
            )
            _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
            return result
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_YEAR,
            value=year,
            status="resolved",
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
        return result

    # MB branch -- rg_id is a UUID.
    rg_mbid = str(rg_id).strip()
    if not rg_mbid:
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_YEAR,
            status="unresolved_malformed",
            reason_code="empty_rg_mbid",
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
        return result

    fetch_mb = mb_get_release_group_year or _default_mb_get_release_group_year
    try:
        year = fetch_mb(rg_mbid)
    except BaseException as exc:  # noqa: BLE001
        status, reason = _classify_lookup_exception(exc)
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_YEAR,
            status=status,
            reason_code=reason,
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
        return result
    if year is None:
        # ``web.mb.get_release_group_year`` now propagates
        # ``HTTPError(404)`` (the resolver classifies that as
        # ``unresolved_404`` via ``_classify_lookup_exception``). So a
        # ``None`` here unambiguously means "release-group record exists
        # but carries no parseable year" → record as
        # ``unresolved_field_missing_upstream``.
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_YEAR,
            status="unresolved_field_missing_upstream",
            reason_code="mb_release_group_no_year",
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
        return result
    result = ResolverResult(
        field_name=FIELD_RELEASE_GROUP_YEAR,
        value=year,
        status="resolved",
    )
    _record(pdb, request_id, FIELD_RELEASE_GROUP_YEAR, result)
    return result


# === Resolver: release_group_id =========================================


def resolve_release_group_id(
    request: dict[str, Any],
    pdb: _PdbRecorder,
    *,
    mb_get_release: MBReleaseFn | None = None,
    discogs_get_release: DiscogsReleaseFn | None = None,
    mb_release_payload: dict[str, Any] | None = None,
    discogs_release_payload: dict[str, Any] | None = None,
) -> ResolverResult:
    """Resolve a request's release-group / master id.

    For MB requests: fetch ``GET /release/{mbid}`` and return the
    ``release_group_id`` field. For Discogs requests: fetch
    ``GET /api/releases/{id}`` and return the ``master_id`` field.

    ``mb_release_payload`` / ``discogs_release_payload`` short-circuit
    the fetch when the caller already has the payload in hand (the
    ``resolve_all`` orchestrator threads its single fetch through to
    every per-field resolver — code-review finding #1). Stand-alone
    callers can keep passing the fetch callables; payload kwargs are
    optional.

    The release_group_id may legitimately be ``None`` upstream (e.g.
    Discogs masterless releases). In that case we record
    ``unresolved_field_missing_upstream``.
    """
    request_id = int(request["id"])
    mb_release_id = request.get("mb_release_id")
    discogs_release_id = request.get("discogs_release_id")

    if not mb_release_id and not discogs_release_id:
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_ID,
            status="unresolved_malformed",
            reason_code="missing_release_id",
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_ID, result)
        return result

    # Discogs branch.
    if discogs_release_id and not mb_release_id:
        if discogs_release_payload is not None:
            data = discogs_release_payload
        else:
            fetch = discogs_get_release or _default_discogs_get_release
            try:
                data = fetch(str(discogs_release_id), fresh=True)
            except BaseException as exc:  # noqa: BLE001
                status, reason = _classify_lookup_exception(exc)
                result = ResolverResult(
                    field_name=FIELD_RELEASE_GROUP_ID,
                    status=status,
                    reason_code=reason,
                )
                _record(pdb, request_id, FIELD_RELEASE_GROUP_ID, result)
                return result
        rg_id = data.get("release_group_id") if isinstance(data, dict) else None
        if not rg_id:
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_ID,
                status="unresolved_field_missing_upstream",
                reason_code="discogs_release_no_master",
            )
            _record(pdb, request_id, FIELD_RELEASE_GROUP_ID, result)
            return result
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_ID,
            value=str(rg_id),
            status="resolved",
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_ID, result)
        return result

    # MB branch.
    mb_id = str(mb_release_id).strip() if mb_release_id else ""
    if not mb_id:
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_ID,
            status="unresolved_malformed",
            reason_code="empty_mb_release_id",
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_ID, result)
        return result

    if mb_release_payload is not None:
        data = mb_release_payload
    else:
        fetch_mb = mb_get_release or _default_mb_get_release
        try:
            data = fetch_mb(mb_id, fresh=True)
        except BaseException as exc:  # noqa: BLE001
            status, reason = _classify_lookup_exception(exc)
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_ID,
                status=status,
                reason_code=reason,
            )
            _record(pdb, request_id, FIELD_RELEASE_GROUP_ID, result)
            return result
    rg_id = data.get("release_group_id") if isinstance(data, dict) else None
    if not rg_id:
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_ID,
            status="unresolved_field_missing_upstream",
            reason_code="mb_release_no_release_group",
        )
        _record(pdb, request_id, FIELD_RELEASE_GROUP_ID, result)
        return result
    result = ResolverResult(
        field_name=FIELD_RELEASE_GROUP_ID,
        value=str(rg_id),
        status="resolved",
    )
    _record(pdb, request_id, FIELD_RELEASE_GROUP_ID, result)
    return result


# === Resolver: track_artists (list, one per track) ======================


def resolve_track_artists(
    request: dict[str, Any],
    pdb: _PdbRecorder,
    *,
    mb_get_release: MBReleaseFn | None = None,
    discogs_get_release: DiscogsReleaseFn | None = None,
    mb_release_payload: dict[str, Any] | None = None,
    discogs_release_payload: dict[str, Any] | None = None,
) -> list[ResolverResult]:
    """Resolve per-track artist credits for the album.

    Returns one ``ResolverResult`` per track in upstream order. The
    "field_name" on each row is ``"track_artist"``; the side table
    therefore records a *single* row per (request, "track_artist") --
    we summarise the per-track outcome into one composite status:

    - ``"resolved"`` when at least one track artist was resolved.
    - The first non-resolved status when every track came back empty
      (failure modes propagate -- 404 of the parent release surfaces
      to every row).

    Per-track results stay in the return list so the caller can
    populate ``album_tracks.track_artist`` row-by-row.

    Payload kwargs short-circuit the fetch when the caller already has
    the payload — see ``resolve_release_group_id`` for the wider
    rationale (code-review finding #1).
    """
    request_id = int(request["id"])
    mb_release_id = request.get("mb_release_id")
    discogs_release_id = request.get("discogs_release_id")

    def _record_summary(per_track: list[ResolverResult]) -> None:
        _record(
            pdb, request_id, FIELD_TRACK_ARTIST,
            _build_track_artist_summary(per_track),
        )

    if not mb_release_id and not discogs_release_id:
        result = ResolverResult(
            field_name=FIELD_TRACK_ARTIST,
            status="unresolved_malformed",
            reason_code="missing_release_id",
        )
        _record(pdb, request_id, FIELD_TRACK_ARTIST, result)
        return [result]

    # Pick branch.
    if discogs_release_id and not mb_release_id:
        if discogs_release_payload is not None:
            data = discogs_release_payload
        else:
            fetch_d = discogs_get_release or _default_discogs_get_release
            try:
                data = fetch_d(str(discogs_release_id), fresh=True)
            except BaseException as exc:  # noqa: BLE001
                status, reason = _classify_lookup_exception(exc)
                per_track = [ResolverResult(
                    field_name=FIELD_TRACK_ARTIST,
                    status=status,
                    reason_code=reason,
                )]
                _record_summary(per_track)
                return per_track
        return _resolve_discogs_track_artists(
            data, pdb=pdb, request_id=request_id,
        )

    mb_id = str(mb_release_id).strip() if mb_release_id else ""
    if not mb_id:
        result = ResolverResult(
            field_name=FIELD_TRACK_ARTIST,
            status="unresolved_malformed",
            reason_code="empty_mb_release_id",
        )
        _record(pdb, request_id, FIELD_TRACK_ARTIST, result)
        return [result]

    if mb_release_payload is not None:
        data = mb_release_payload
    else:
        fetch_mb = mb_get_release or _default_mb_get_release
        try:
            data = fetch_mb(mb_id, fresh=True)
        except BaseException as exc:  # noqa: BLE001
            status, reason = _classify_lookup_exception(exc)
            per_track = [ResolverResult(
                field_name=FIELD_TRACK_ARTIST,
                status=status,
                reason_code=reason,
            )]
            _record_summary(per_track)
            return per_track
    return _resolve_mb_track_artists(
        data, pdb=pdb, request_id=request_id,
    )


def _resolve_mb_track_artists(
    data: dict[str, Any],
    *,
    pdb: _PdbRecorder,
    request_id: int,
) -> list[ResolverResult]:
    """Walk an MB release payload and extract per-track artist credits.

    The shape we see depends on whether the caller fetched via
    ``web.mb.get_release`` (which strips most artist-credit info) or
    via a direct ``inc=artist-credits`` call. We try both shapes:

    1. Direct MB JSON: ``media[].tracks[].artist-credit[].name``,
       joined with ``joinphrase``.
    2. ``web.mb.get_release`` shape: ``tracks[].title`` only -- no
       artist info, so we surface ``unresolved_field_missing_upstream``.

    The integration slice fetches via a real MB mirror response shape
    so this branch is exercised end-to-end.
    """
    per_track: list[ResolverResult] = []

    # Shape 1: direct MB JSON with media[].tracks[]
    media_list = data.get("media") if isinstance(data, dict) else None
    if isinstance(media_list, list) and media_list:
        for medium in media_list:
            if not isinstance(medium, dict):
                continue
            for track in medium.get("tracks") or []:
                if not isinstance(track, dict):
                    continue
                ac = track.get("artist-credit") or (
                    (track.get("recording") or {}).get("artist-credit")
                    if isinstance(track.get("recording"), dict) else None
                )
                if not isinstance(ac, list) or not ac:
                    per_track.append(ResolverResult(
                        field_name=FIELD_TRACK_ARTIST,
                        status="unresolved_field_missing_upstream",
                        reason_code="mb_track_no_artist_credit",
                    ))
                    continue
                name = _format_mb_artist_credit(ac)
                if not name:
                    per_track.append(ResolverResult(
                        field_name=FIELD_TRACK_ARTIST,
                        status="unresolved_field_missing_upstream",
                        reason_code="mb_track_artist_credit_empty",
                    ))
                    continue
                per_track.append(ResolverResult(
                    field_name=FIELD_TRACK_ARTIST,
                    value=name,
                    status="resolved",
                ))
    else:
        # Shape 2: web.mb.get_release shape -- no per-track artist info.
        tracks_summary = data.get("tracks") if isinstance(data, dict) else None
        if isinstance(tracks_summary, list) and tracks_summary:
            # Fall back to the release-level artist for every track --
            # this is "the album artist" rather than the per-track
            # featured artist, so we record it as
            # unresolved_field_missing_upstream and leave value=None.
            for _ in tracks_summary:
                per_track.append(ResolverResult(
                    field_name=FIELD_TRACK_ARTIST,
                    status="unresolved_field_missing_upstream",
                    reason_code="mb_payload_lacks_per_track_artist",
                ))

    summary = _build_track_artist_summary(per_track)
    _record(pdb, request_id, FIELD_TRACK_ARTIST, summary)
    return per_track or [summary]


def _build_track_artist_summary(
    per_track: list[ResolverResult],
) -> ResolverResult:
    """Collapse a list of per-track ResolverResults into the summary
    row that lands in the side table for ``FIELD_TRACK_ARTIST``.

    Single canonical place for the "any resolved → resolved; otherwise
    surface the first failure" rule. Used by ``resolve_track_artists``,
    ``_resolve_mb_track_artists``, and ``_resolve_discogs_track_artists``;
    previously each had its own near-identical copy of the logic.
    """
    if not per_track:
        return ResolverResult(
            field_name=FIELD_TRACK_ARTIST,
            status="unresolved_field_missing_upstream",
            reason_code="no_tracks_returned",
        )
    if any(r.status == "resolved" for r in per_track):
        return ResolverResult(
            field_name=FIELD_TRACK_ARTIST,
            status="resolved",
        )
    # All per-track entries failed; the first row is representative
    # (the parent-call failure path mirrors its status across every
    # synthesised track, so [0] is canonical).
    first = per_track[0]
    return ResolverResult(
        field_name=FIELD_TRACK_ARTIST,
        status=first.status,
        reason_code=first.reason_code,
    )


def _format_mb_artist_credit(ac: list[Any]) -> str:
    """Render an MB ``artist-credit`` array into a display string.

    Pattern: ``[{name, joinphrase}, {name, joinphrase}, ...]`` ->
    ``"Artist A & Artist B"``. Each credit may also carry a nested
    ``artist`` dict; we prefer ``name`` at the outer level (the
    canonical credit-name) and fall back to ``artist.name``.
    """
    parts: list[str] = []
    for entry in ac:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not name and isinstance(entry.get("artist"), dict):
            name = entry["artist"].get("name")
        if not name:
            continue
        parts.append(str(name))
        join = entry.get("joinphrase")
        if join:
            parts.append(str(join))
    rendered = "".join(parts).strip()
    return rendered


def _resolve_discogs_track_artists(
    data: dict[str, Any],
    *,
    pdb: _PdbRecorder,
    request_id: int,
) -> list[ResolverResult]:
    """Walk a Discogs release payload and extract per-track artist credits.

    Discogs shape: ``tracks[].artists[].name`` (the existing
    ``web.discogs.get_release`` flattens to ``tracks[]`` without
    per-track artist info, so the direct mirror endpoint is what we
    actually want here). We try both shapes:

    1. ``tracks[].artists[].name`` (per-track credit).
    2. Top-level ``artists[].name`` repeated per track (release-level
       only -- ``unresolved_field_missing_upstream``).
    """
    per_track: list[ResolverResult] = []
    tracks = data.get("tracks") if isinstance(data, dict) else None
    if not isinstance(tracks, list) or not tracks:
        result = ResolverResult(
            field_name=FIELD_TRACK_ARTIST,
            status="unresolved_field_missing_upstream",
            reason_code="discogs_release_no_tracks",
        )
        _record(pdb, request_id, FIELD_TRACK_ARTIST, result)
        return [result]

    for track in tracks:
        if not isinstance(track, dict):
            per_track.append(ResolverResult(
                field_name=FIELD_TRACK_ARTIST,
                status="unresolved_malformed",
                reason_code="discogs_track_not_dict",
            ))
            continue
        artists = track.get("artists")
        if isinstance(artists, list) and artists:
            name = _format_discogs_artist_list(artists)
            if name:
                per_track.append(ResolverResult(
                    field_name=FIELD_TRACK_ARTIST,
                    value=name,
                    status="resolved",
                ))
                continue
        per_track.append(ResolverResult(
            field_name=FIELD_TRACK_ARTIST,
            status="unresolved_field_missing_upstream",
            reason_code="discogs_track_no_artist",
        ))

    summary = _build_track_artist_summary(per_track)
    _record(pdb, request_id, FIELD_TRACK_ARTIST, summary)
    return per_track


def _format_discogs_artist_list(artists: list[Any]) -> str:
    """Render a Discogs artist list into a display string."""
    parts: list[str] = []
    for entry in artists:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not name:
            continue
        # ``join`` is the Discogs equivalent of MB's joinphrase
        # (typically " " or ", " or "&"). Default to "& " when missing.
        join = entry.get("join") or ""
        if parts and not parts[-1].endswith(" "):
            parts.append(" ")
        parts.append(str(name))
        if join:
            parts.append(" " + str(join).strip() + " ")
    return "".join(parts).strip()


# === Resolver: catalog_number ===========================================


def resolve_catalog_number(
    request: dict[str, Any],
    pdb: _PdbRecorder,
    *,
    mb_get_release: MBReleaseFn | None = None,
    discogs_get_release: DiscogsReleaseFn | None = None,
    mb_release_payload: dict[str, Any] | None = None,
    discogs_release_payload: dict[str, Any] | None = None,
) -> ResolverResult:
    """Resolve a label catalog number for the request.

    MB shape: ``labels[].catalog-number``. Discogs shape:
    ``labels[].catno``. We pick the first non-empty value seen on the
    release row -- per the plan's Phase 2 generator slot (R2), a
    catalog_number >= 4 chars unlocks an extra search query.

    Payload kwargs short-circuit the fetch when the caller already has
    the payload — see ``resolve_release_group_id`` (code-review #1).
    """
    request_id = int(request["id"])
    mb_release_id = request.get("mb_release_id")
    discogs_release_id = request.get("discogs_release_id")

    if not mb_release_id and not discogs_release_id:
        result = ResolverResult(
            field_name=FIELD_CATALOG_NUMBER,
            status="unresolved_malformed",
            reason_code="missing_release_id",
        )
        _record(pdb, request_id, FIELD_CATALOG_NUMBER, result)
        return result

    if discogs_release_id and not mb_release_id:
        if discogs_release_payload is not None:
            data = discogs_release_payload
        else:
            fetch = discogs_get_release or _default_discogs_get_release
            try:
                data = fetch(str(discogs_release_id), fresh=True)
            except BaseException as exc:  # noqa: BLE001
                status, reason = _classify_lookup_exception(exc)
                result = ResolverResult(
                    field_name=FIELD_CATALOG_NUMBER,
                    status=status,
                    reason_code=reason,
                )
                _record(pdb, request_id, FIELD_CATALOG_NUMBER, result)
                return result
        catno = _first_discogs_catno(data)
        if not catno:
            result = ResolverResult(
                field_name=FIELD_CATALOG_NUMBER,
                status="unresolved_field_missing_upstream",
                reason_code="discogs_no_catno",
            )
            _record(pdb, request_id, FIELD_CATALOG_NUMBER, result)
            return result
        result = ResolverResult(
            field_name=FIELD_CATALOG_NUMBER,
            value=catno,
            status="resolved",
        )
        _record(pdb, request_id, FIELD_CATALOG_NUMBER, result)
        return result

    mb_id = str(mb_release_id).strip() if mb_release_id else ""
    if not mb_id:
        result = ResolverResult(
            field_name=FIELD_CATALOG_NUMBER,
            status="unresolved_malformed",
            reason_code="empty_mb_release_id",
        )
        _record(pdb, request_id, FIELD_CATALOG_NUMBER, result)
        return result

    if mb_release_payload is not None:
        data = mb_release_payload
    else:
        fetch_mb = mb_get_release or _default_mb_get_release
        try:
            data = fetch_mb(mb_id, fresh=True)
        except BaseException as exc:  # noqa: BLE001
            status, reason = _classify_lookup_exception(exc)
            result = ResolverResult(
                field_name=FIELD_CATALOG_NUMBER,
                status=status,
                reason_code=reason,
            )
            _record(pdb, request_id, FIELD_CATALOG_NUMBER, result)
            return result
    catno = _first_mb_catalog_number(data)
    if not catno:
        result = ResolverResult(
            field_name=FIELD_CATALOG_NUMBER,
            status="unresolved_field_missing_upstream",
            reason_code="mb_no_catalog_number",
        )
        _record(pdb, request_id, FIELD_CATALOG_NUMBER, result)
        return result
    result = ResolverResult(
        field_name=FIELD_CATALOG_NUMBER,
        value=catno,
        status="resolved",
    )
    _record(pdb, request_id, FIELD_CATALOG_NUMBER, result)
    return result


def _first_mb_catalog_number(data: dict[str, Any]) -> str | None:
    """Pick the first non-empty ``catalog-number`` from an MB release."""
    if not isinstance(data, dict):
        return None
    labels = data.get("label-info") or data.get("labels") or []
    if not isinstance(labels, list):
        return None
    for entry in labels:
        if not isinstance(entry, dict):
            continue
        # MB JSON uses kebab-case key for label-info; our
        # ``web.mb.get_release`` shape doesn't currently carry catno
        # at all, but the direct MB JSON does.
        catno = entry.get("catalog-number")
        if catno:
            return str(catno)
    return None


def _first_discogs_catno(data: dict[str, Any]) -> str | None:
    """Pick the first non-empty ``catno`` from a Discogs release."""
    if not isinstance(data, dict):
        return None
    labels = data.get("labels") or []
    if not isinstance(labels, list):
        return None
    for entry in labels:
        if not isinstance(entry, dict):
            continue
        catno = entry.get("catno")
        if catno:
            return str(catno)
    return None


# === VA detection =======================================================


def _is_canonical_va_credit(
    artist_id: Any,
    *,
    source_is_discogs: bool,
) -> bool:
    """Rule 1: primary-artist-credit identity matches the canonical VA id."""
    if artist_id is None:
        return False
    s = str(artist_id).strip()
    if not s:
        return False
    if source_is_discogs:
        return s == DISCOGS_VA_ARTIST_ID
    return s == MB_VA_ARTIST_MBID


def _is_compilation_by_release_group_type(
    primary_type: Any, secondary_types: Any,
) -> bool:
    """Rule 2: MB release-group is typed as a Compilation."""
    if isinstance(primary_type, str) and primary_type.lower() == "compilation":
        return True
    if isinstance(secondary_types, list):
        for t in secondary_types:
            if isinstance(t, str) and t.lower() == "compilation":
                return True
    return False


def _has_divergent_track_credits_only(
    album_artist_credit: Any, tracks: Any,
) -> bool:
    """True when at least one track's artist credit differs from the
    rendered album-level credit. No joinphrase precondition.

    Used by Rule 2's tightened version (issue #373): any Compilation
    release-group can be cross-checked against per-track divergence,
    independent of whether the album-level credit happens to have a
    "/"-joinphrase. Greatest-hits / B-sides / single-artist comps
    were getting falsely flagged as VA under the pre-tightening Rule
    2 (Compilation rg alone was sufficient). Without per-track
    divergence the VA strategy mix is strictly worse for those
    requests — it drops default/literal, which are the natural query
    shapes when every track shares the album artist.
    """
    if not isinstance(album_artist_credit, list) or not album_artist_credit:
        return False
    album_credit_str = _format_mb_artist_credit(album_artist_credit)
    if not album_credit_str:
        return False
    if not isinstance(tracks, list):
        return False
    for track in tracks:
        if not isinstance(track, dict):
            continue
        ac = track.get("artist-credit")
        if not isinstance(ac, list) or not ac:
            continue
        track_credit_str = _format_mb_artist_credit(ac)
        if track_credit_str and track_credit_str != album_credit_str:
            return True
    return False


def _has_divergent_track_credits(
    album_artist_credit: Any, tracks: Any,
) -> bool:
    """Rule 3: split-artist comp -- joinphrase has "/" AND per-track
    credits diverge from the album credit.
    """
    # Render the album-level credit for comparison.
    if not isinstance(album_artist_credit, list) or not album_artist_credit:
        return False
    album_credit_str = _format_mb_artist_credit(album_artist_credit)
    if not album_credit_str:
        return False

    # Look for joinphrase containing "/" -- the canonical MB marker
    # for split-artist credits (e.g. "Artist A / Artist B").
    has_slash_joinphrase = any(
        isinstance(e, dict)
        and isinstance(e.get("joinphrase"), str)
        and "/" in e["joinphrase"]
        for e in album_artist_credit
    )
    if not has_slash_joinphrase:
        return False

    # And at least one track credit must differ from the album credit.
    return _has_divergent_track_credits_only(album_artist_credit, tracks)


def _flatten_release_tracks(mb_release_payload: dict[str, Any]) -> list[Any]:
    """Flatten ``media[].tracks[]`` into a single track list.

    Used by VA detection Rules 2 (Compilation rg + divergent credits)
    and 3 (split-artist joinphrase) — both need the per-track artist
    credits in flat form. Defensive against malformed payloads: returns
    ``[]`` for missing/non-list ``media`` or non-list ``tracks``.
    """
    media = mb_release_payload.get("media") or []
    if not isinstance(media, list):
        return []
    flat: list[Any] = []
    for m in media:
        if not isinstance(m, dict):
            continue
        tr = m.get("tracks") or []
        if isinstance(tr, list):
            flat.extend(tr)
    return flat


def detect_va_compilation(
    request: dict[str, Any],
    *,
    mb_release_payload: dict[str, Any] | None = None,
    discogs_release_payload: dict[str, Any] | None = None,
    mb_release_group_payload: dict[str, Any] | None = None,
) -> bool:
    """Detect whether ``request`` is a Various Artists compilation.

    Three rules in order. Returns True on any rule match. Each rule is
    testable in isolation. Payloads are passed in (not fetched) because
    the caller already has them in hand at enqueue time -- the
    inline-at-enqueue path in U4 fetches once and threads through.

    Rules:

    1. Primary-artist-credit identity matches the canonical VA id
       (MB: ``89ad4ac3-...``; Discogs: ``194``).
    2. MB release-group is typed as a Compilation (primary or
       secondary type) AND per-track artist credits diverge from
       the album-level credit. The divergence requirement is the
       2026-05-25 tightening (#373) — Compilation rg alone falsely
       flags greatest-hits / B-sides / single-artist comps where
       every track shares the album artist.
    3. MB album artist-credit has a "/"-joinphrase AND per-track
       credits diverge from the album credit (split-artist comp).

    Note: rule 1 explicitly compares **IDs**, never name strings.
    "Various" or "Various Artists" as a name without the canonical ID
    is NOT a positive -- that's the regression guard the plan calls
    out.
    """
    discogs_release_id = request.get("discogs_release_id")
    mb_release_id = request.get("mb_release_id")
    # Discogs-sourced rows store the discogs id in BOTH columns for
    # pipeline-compat (the web/CLI Discogs add path stuffs the discogs
    # id into ``mb_release_id`` so existing pipeline code that keys on
    # ``mb_release_id`` keeps working). A numeric ``mb_release_id`` is
    # therefore a Discogs signal — MB MBIDs are always hyphenated UUIDs.
    # Without this, the 18 wanted Discogs-VA rows from the 2026-05-25
    # backfill weren't VA-flagged because Rule 1 compared the canonical
    # Discogs id ``"194"`` against the MB UUID and never matched.
    is_discogs = bool(discogs_release_id) and (
        not mb_release_id or _looks_numeric(mb_release_id)
    )

    # Rule 1.
    artist_id = request.get("mb_artist_id")
    if is_discogs:
        # request rows from the Discogs path don't always carry the
        # discogs artist id directly; the payload is authoritative.
        # Two shapes appear in the wild:
        #   * ``web/discogs.py::get_release`` (the real production caller)
        #     flattens to ``payload["artist_id"]`` at the top level.
        #   * Direct Discogs-mirror payloads carry the nested
        #     ``payload["artists"][0]["id"]`` shape.
        # Read both; either presence overrides the skeleton value.
        if isinstance(discogs_release_payload, dict):
            top_level = discogs_release_payload.get("artist_id")
            if top_level not in (None, ""):
                artist_id = top_level
            else:
                artists = discogs_release_payload.get("artists") or []
                if isinstance(artists, list) and artists:
                    first = artists[0] if isinstance(artists[0], dict) else None
                    if first is not None:
                        artist_id = first.get("id")
    if _is_canonical_va_credit(artist_id, source_is_discogs=is_discogs):
        return True

    # Rule 2 (MB-only, TIGHTENED post-2026-05-25 deploy — issue #373):
    # any Compilation-typed rg by itself is NOT enough; we also need
    # per-track artist credits to actually diverge from the album
    # credit. Greatest-hits / B-sides / single-artist comps were
    # getting falsely flagged because MB tags them as Compilation
    # primary-type. Without per-track divergence, the VA strategy mix
    # is strictly worse for them (it drops default/literal, which are
    # the natural query shapes for "Greatest Hits"-style requests
    # where every track shares the album artist).
    is_compilation_rg = False
    if isinstance(mb_release_group_payload, dict):
        is_compilation_rg = _is_compilation_by_release_group_type(
            mb_release_group_payload.get("primary-type"),
            mb_release_group_payload.get("secondary-types"),
        )
    if not is_compilation_rg and isinstance(mb_release_payload, dict):
        rg = mb_release_payload.get("release-group")
        if isinstance(rg, dict):
            is_compilation_rg = _is_compilation_by_release_group_type(
                rg.get("primary-type"),
                rg.get("secondary-types"),
            )
    if is_compilation_rg and isinstance(mb_release_payload, dict):
        album_ac = mb_release_payload.get("artist-credit")
        all_tracks_r2 = _flatten_release_tracks(mb_release_payload)
        if _has_divergent_track_credits_only(album_ac, all_tracks_r2):
            return True

    # Rule 3 (MB-only -- split-artist compilations).
    if isinstance(mb_release_payload, dict):
        album_ac = mb_release_payload.get("artist-credit")
        all_tracks = _flatten_release_tracks(mb_release_payload)
        if _has_divergent_track_credits(album_ac, all_tracks):
            return True

    return False


# === Inline-at-enqueue orchestrator (U4) ================================
#
# ``resolve_all`` is the single entry point the web add path
# (``web/routes/pipeline.py::post_pipeline_add``) and the CLI add path
# (``scripts/pipeline_cli/album_requests.py::cmd_add``) call after they have a freshly
# inserted ``album_requests`` row id and the upstream release payload(s)
# in hand. It:
#
#   * runs the four field resolvers in parallel via a
#     ``ThreadPoolExecutor`` with a 3-second wall-clock budget total
#     (so a single slow Discogs call cannot freeze the web UI on a
#     20-track album add — Discogs HTTP client has a 60s timeout);
#   * marks any resolver still running at budget exhaustion as
#     ``unresolved_timeout`` (the value lands NULL on the row, the
#     side-table records the timeout for triage);
#   * runs ``detect_va_compilation`` once with the already-fetched
#     payloads — synchronous, fast, no I/O.
#
# Side-table writes happen from the main thread only. Worker threads
# accumulate write attempts in a ``_DeferredRecorder``; the main thread
# flushes them to ``pdb`` after the budget loop. This keeps the real
# psycopg2 connection safe (single connection, not thread-safe per
# upstream docs) without modifying U2's resolvers.


_DEFAULT_BUDGET_SECONDS = 3.0


class ResolveAllResult(msgspec.Struct, kw_only=True):
    """Typed return shape of ``resolve_all``.

    Wire-boundary Struct so callers that re-encode the resolved values
    (audit, debug endpoints) get strict shape checking. ``track_artists``
    is a list of per-track-artist strings (or ``None`` for unresolved
    tracks); the caller decides how to surface partial resolution.
    """

    release_group_year: int | None = None
    release_group_id: str | None = None
    catalog_number: str | None = None
    track_artists: list[str | None] = msgspec.field(default_factory=list)
    is_va_compilation: bool = False
    # Total wall-clock seconds the orchestrator spent. Useful for the
    # operator triage surface and the latency-budget regression test.
    elapsed_seconds: float = 0.0
    # Names (the FIELD_* constants) of resolvers that hit the budget
    # ceiling and were marked as unresolved_timeout. Empty in the happy
    # path. The test guard reads this directly.
    timed_out_fields: list[str] = msgspec.field(default_factory=list)


class _DeferredRecorder:
    """Thread-safe queue of pending ``record_field_resolution`` calls.

    Workers route their inline ``_record()`` calls through here; the
    main thread flushes after ``resolve_all`` finishes. Required because
    psycopg2 connections are not thread-safe (the resolvers are called
    from worker threads). The lock guards the in-process list, not
    pdb — pdb is only touched from the main thread.
    """

    def __init__(self) -> None:
        self._records: list[tuple[int, str, str, str | None]] = []
        self._lock = threading.Lock()

    def record_field_resolution(
        self,
        request_id: int,
        field_name: str,
        status: str,
        reason_code: str | None,
    ) -> None:
        with self._lock:
            self._records.append(
                (int(request_id), field_name, status, reason_code),
            )

    def already_recorded(self, field_name: str) -> bool:
        """Has a row been queued for ``field_name`` already?

        Used by the main-thread timeout writer to avoid double-recording
        when a resolver completed *before* its future was cancelled (the
        completion still races the timeout writer in tight tests).
        """
        with self._lock:
            return any(fn == field_name for _rid, fn, _s, _rc in self._records)

    def flush_to(self, pdb: _PdbRecorder) -> None:
        """Write every queued record to the real pdb (main-thread only).

        Each call is wrapped in try/except so a single failed UPSERT
        does not lose the rest of the batch — same best-effort discipline
        as U2's ``_record()`` helper.
        """
        with self._lock:
            pending = list(self._records)
            self._records.clear()
        for request_id, field_name, status, reason_code in pending:
            try:
                pdb.record_field_resolution(
                    request_id=request_id,
                    field_name=field_name,
                    status=status,
                    reason_code=reason_code,
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "deferred record_field_resolution failed for "
                    "request=%d field=%s status=%s; dropping",
                    request_id, field_name, status,
                )


def resolve_all(
    request: dict[str, Any],
    pdb: _PdbRecorder,
    *,
    mb_release_payload: dict[str, Any] | None = None,
    discogs_release_payload: dict[str, Any] | None = None,
    mb_release_group_payload: dict[str, Any] | None = None,
    budget_seconds: float = _DEFAULT_BUDGET_SECONDS,
    mb_get_release_group_year: MBReleaseGroupYearFn | None = None,
    discogs_get_master_year: DiscogsMasterYearFn | None = None,
    mb_get_release: MBReleaseFn | None = None,
    discogs_get_release: DiscogsReleaseFn | None = None,
) -> ResolveAllResult:
    """Run all field resolvers inline at enqueue with a wall-clock budget.

    Called from ``web/routes/pipeline.py::post_pipeline_add`` and from
    ``scripts/pipeline_cli/album_requests.py::cmd_add`` after the new ``album_requests``
    row has been inserted (so ``request["id"]`` is real and the FK in
    ``album_request_field_resolutions`` is satisfiable). The caller then
    updates the row with the returned values (proceed-with-NULL where
    unresolved) and sets ``is_va_compilation`` once at enqueue.

    Wall-clock budget (default 3 seconds) applies to ALL resolvers
    combined. Each runs in its own worker thread. Any resolver still
    pending at the budget cutoff is left to settle in the background
    (no graceful cancellation in stdlib), but its slot in the result
    structure is filled with NULL + ``unresolved_timeout`` in the side
    table. This prevents a single slow upstream call (e.g. the 60s
    Discogs HTTP timeout) from freezing the add endpoint.

    Per the plan's key technical decision: VA detection is set ONCE at
    enqueue (or by the U3 backfill for legacy rows). The caller passes
    the already-fetched release/release-group payloads through; the
    detector runs synchronously and never touches the network.
    """
    request_id = int(request["id"])
    deferred = _DeferredRecorder()
    deadline = time.monotonic() + max(0.0, budget_seconds)
    start = time.monotonic()

    def _run_rg_year() -> ResolverResult:
        return resolve_release_group_year(
            request, deferred,
            mb_get_release_group_year=mb_get_release_group_year,
            discogs_get_master_year=discogs_get_master_year,
        )

    def _run_rg_id() -> ResolverResult:
        return resolve_release_group_id(
            request, deferred,
            mb_get_release=mb_get_release,
            discogs_get_release=discogs_get_release,
            mb_release_payload=mb_release_payload,
            discogs_release_payload=discogs_release_payload,
        )

    def _run_catno() -> ResolverResult:
        return resolve_catalog_number(
            request, deferred,
            mb_get_release=mb_get_release,
            discogs_get_release=discogs_get_release,
            mb_release_payload=mb_release_payload,
            discogs_release_payload=discogs_release_payload,
        )

    def _run_track_artists() -> list[ResolverResult]:
        return resolve_track_artists(
            request, deferred,
            mb_get_release=mb_get_release,
            discogs_get_release=discogs_get_release,
            mb_release_payload=mb_release_payload,
            discogs_release_payload=discogs_release_payload,
        )

    jobs: dict[str, tuple[str, Callable[[], Any]]] = {
        "rg_year": (FIELD_RELEASE_GROUP_YEAR, _run_rg_year),
        "rg_id": (FIELD_RELEASE_GROUP_ID, _run_rg_id),
        "catno": (FIELD_CATALOG_NUMBER, _run_catno),
        "track_artists": (FIELD_TRACK_ARTIST, _run_track_artists),
    }

    futures: dict[str, concurrent.futures.Future[Any]] = {}
    outputs: dict[str, Any] = {}
    timed_out: list[str] = []

    # Manage the pool by hand so we can return at budget exhaustion
    # without waiting on stuck workers (the default
    # ``ThreadPoolExecutor.__exit__`` blocks on ``shutdown(wait=True)``,
    # which defeats the wall-clock budget). ``wait=False`` +
    # ``cancel_futures=True`` releases the orchestrator immediately;
    # the long-running future is left to settle in the background
    # thread and its result is discarded.
    pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, len(jobs)),
        thread_name_prefix="resolve_all",
    )
    try:
        for key, (_field_name, fn) in jobs.items():
            futures[key] = pool.submit(fn)
        for key, fut in futures.items():
            field_name, _fn = jobs[key]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Budget already exhausted. BUT — if this future
                # already completed concurrently while a sibling was
                # being awaited, harvest its result anyway. The
                # original bug here (code-review finding #2) dropped
                # any future that finished AFTER the budget expired
                # but BEFORE we got to its slot in the iteration,
                # leaving its row NULL even though the work was done.
                if fut.done():
                    try:
                        outputs[key] = fut.result(timeout=0)
                        continue
                    except Exception:  # noqa: BLE001
                        # Fall through to the timeout-recording path
                        # below — a completed-but-raised future is
                        # equivalent to "we got nothing useful".
                        pass
                outputs[key] = None
                if not deferred.already_recorded(field_name):
                    deferred.record_field_resolution(
                        request_id=request_id,
                        field_name=field_name,
                        status="unresolved_timeout",
                        reason_code="budget_exhausted",
                    )
                    timed_out.append(field_name)
                continue
            try:
                outputs[key] = fut.result(timeout=remaining)
            except concurrent.futures.TimeoutError:
                outputs[key] = None
                if not deferred.already_recorded(field_name):
                    deferred.record_field_resolution(
                        request_id=request_id,
                        field_name=field_name,
                        status="unresolved_timeout",
                        reason_code="budget_exhausted",
                    )
                    timed_out.append(field_name)
            except Exception as exc:  # noqa: BLE001
                # The four resolvers handle their own exception
                # classification internally — anything escaping here is
                # either a transient that slipped past the resolver's
                # classifier (race, novel transport error) or a genuine
                # programmer error (e.g. KeyError on a payload shape).
                # Distinguishing the two matters: transients retry,
                # programmer errors don't. Reuse the same classifier the
                # per-field resolvers use; if it raises (programmer
                # error), tag the row ``unresolved_internal_error`` with
                # ``reason_code='bug_<ExcName>'`` so it sticks rather
                # than masquerading as a recoverable mirror outage.
                logger.exception(
                    "resolve_all: %s raised unexpectedly for request=%d: %s",
                    field_name, request_id, exc,
                )
                outputs[key] = None
                try:
                    status, reason = _classify_lookup_exception(exc)
                except BaseException:  # noqa: BLE001
                    status = "unresolved_internal_error"
                    reason = f"bug_{type(exc).__name__}"
                if not deferred.already_recorded(field_name):
                    deferred.record_field_resolution(
                        request_id=request_id,
                        field_name=field_name,
                        status=status,
                        reason_code=reason,
                    )
    finally:
        # Don't block on stuck workers — the budget is the point.
        # ``cancel_futures=True`` (Python 3.9+) cancels anything still
        # queued; running futures finish in the background and their
        # results are discarded.
        pool.shutdown(wait=False, cancel_futures=True)

    # Pull primitive values out of the ResolverResult shapes.
    rg_year_result = outputs.get("rg_year")
    release_group_year: int | None = None
    if isinstance(rg_year_result, ResolverResult) and rg_year_result.status == "resolved":
        v = rg_year_result.value
        if isinstance(v, int):
            release_group_year = v
        else:
            try:
                release_group_year = int(v) if v is not None else None
            except (TypeError, ValueError):
                release_group_year = None

    rg_id_result = outputs.get("rg_id")
    release_group_id: str | None = None
    if isinstance(rg_id_result, ResolverResult) and rg_id_result.status == "resolved":
        v = rg_id_result.value
        release_group_id = str(v) if v is not None else None

    catno_result = outputs.get("catno")
    catalog_number: str | None = None
    if isinstance(catno_result, ResolverResult) and catno_result.status == "resolved":
        v = catno_result.value
        catalog_number = str(v) if v is not None else None

    track_artist_results = outputs.get("track_artists")
    track_artists: list[str | None] = []
    if isinstance(track_artist_results, list):
        for entry in track_artist_results:
            if isinstance(entry, ResolverResult) and entry.status == "resolved":
                v = entry.value
                track_artists.append(str(v) if v is not None else None)
            else:
                track_artists.append(None)

    # VA detection — synchronous, payload-driven, no I/O.
    is_va = detect_va_compilation(
        request,
        mb_release_payload=mb_release_payload,
        discogs_release_payload=discogs_release_payload,
        mb_release_group_payload=mb_release_group_payload,
    )

    # Flush the deferred side-table writes once the budget loop has
    # settled. Main-thread only; safe against psycopg2's per-connection
    # threading constraint.
    deferred.flush_to(pdb)

    return ResolveAllResult(
        release_group_year=release_group_year,
        release_group_id=release_group_id,
        catalog_number=catalog_number,
        track_artists=track_artists,
        is_va_compilation=is_va,
        elapsed_seconds=time.monotonic() - start,
        timed_out_fields=timed_out,
    )


# === Apply helper — turn a ResolveAllResult into a DB update ============
#
# Single canonical implementation of the "result → update_request_fields"
# mapping. Web (``web/routes/pipeline_mutations.py::_resolve_and_update_after_add``)
# and CLI (``scripts/pipeline_cli/album_requests.py::_resolve_and_update_after_add``)
# previously each carried a near-identical copy. The transient deploy
# one-shot (``docs/search-plan-iter2-deploy.md`` § 3.2) also benefits —
# its inline ``updates`` dict-building is the same shape.
#
# The helper does NOT catch exceptions: the caller's logging style (web
# uses ``logger.exception``, CLI uses ``print(..., file=sys.stderr)``,
# the deploy heredoc just prints) is wrapper-specific and lives in the
# caller. Raising lets the wrapper decide.


class _ApplyResolveAllRecipient(Protocol):
    """Minimal DB surface ``apply_resolve_all_result`` needs."""

    def update_request_fields(
        self, request_id: int, **fields: Any,
    ) -> None: ...

    def update_track_artists(
        self, request_id: int, track_artists: list[str | None],
    ) -> None: ...


def apply_resolve_all_result(
    db: _ApplyResolveAllRecipient,
    req_id: int,
    result: ResolveAllResult,
    *,
    existing_mb_release_group_id: str | None = None,
) -> None:
    """Persist a ``ResolveAllResult`` into ``album_requests``.

    Writes ``is_va_compilation`` unconditionally (the immutability
    invariant at enqueue — the schema default is False, so writing the
    detector's verdict is what makes the column meaningful). Writes
    ``release_group_year`` / ``catalog_number`` only when the resolver
    actually produced a value. Writes ``mb_release_group_id`` only when
    the row didn't already have one (the resolver-derived value must
    never clobber a known-good upstream value).

    Pass ``existing_mb_release_group_id`` so the caller's "already
    known" decision lives outside the helper — the row may have come
    from a fresh add (None until the resolver fills it) or from a
    re-resolution where the column is already populated.
    """
    update_fields: dict[str, Any] = {
        "is_va_compilation": result.is_va_compilation,
    }
    if result.release_group_year is not None:
        update_fields["release_group_year"] = result.release_group_year
    if (
        result.release_group_id is not None
        and existing_mb_release_group_id is None
    ):
        update_fields["mb_release_group_id"] = result.release_group_id
    if result.catalog_number is not None:
        update_fields["catalog_number"] = result.catalog_number
    db.update_request_fields(req_id, **update_fields)
    # Per-track artists land in album_tracks (one column per track),
    # not album_requests. Done after the request-row update so a
    # failure here doesn't roll back the resolved scalar fields —
    # mirrors the resolver's proceed-with-NULL discipline (best-effort
    # log + continue).
    if result.track_artists:
        try:
            db.update_track_artists(req_id, list(result.track_artists))
        except Exception:  # noqa: BLE001
            logger.exception(
                "apply_resolve_all_result: update_track_artists failed "
                "for request %s; per-track artists not persisted",
                req_id,
            )
