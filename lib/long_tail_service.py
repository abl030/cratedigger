"""Long-tail worklist service — returns the ``wanted`` cohort with each
row pre-banded by on-disk quality (``Missing`` / a ``QualityRank`` band /
``Unknown``) and stamped with ``in_flight_rescue``.

This is the read backend for the Long-Tail Triage Console (U1). It is the
first UI consumer of the existing beets-library banding machinery —
``web.server.compute_library_rank`` via the beets-only banding core
factored out of ``web.routes._overlay`` — applied to the strict exact
identity derived from each ``album_requests`` row's MusicBrainz and
Discogs release fields.

Service-first (KTD2): the typed ``LongTailResult`` is the contract; the
HTTP route (``GET /api/pipeline/long-tail``) and the CLI
(``pipeline-cli long-tail``) are thin adapters that wrap this module and
map ``outcome`` onto status / exit codes. Both surfaces wrap the SAME
service method per CLI ⇄ API symmetry.

Banding rules (KTD1, fail-closed):

* ``CurrentBeetsMissing`` for the exact identity → ``Missing``.
* one exact current album whose coherent item snapshot cannot be ranked →
  ``Unknown`` (has audio, never ``Missing``).
* one exact rankable current album → the lowercase ``QualityRank`` band (``transparent`` /
  ``excellent`` / ``good`` / ``acceptable`` / ``poor``).
* every ambiguous topology or identity result aborts the whole projection;
  ambiguity is never presented as absence.

Missing, malformed, or conflicting request identities fail closed before a
Beets read. A valid identity omitted from the banding result also fails closed:
only an explicit Beets membership answer may claim that a pressing is missing.

The band labels are lowercase to match ``library_rank`` /
``badge-rank-*`` exactly so badge rendering comes for free.

``in_flight_rescue`` is stamped by the DB cohort query via the existing
``download_log`` predicate ``source='youtube' AND outcome='youtube_running'``
(inlined as an ``EXISTS`` in ``_RequestsMixin._LONG_TAIL_SELECT``), backed by
migration 037's partial unique index ``one_youtube_running_per_request`` —
never an N-query loop.

The banding ``band_fn`` collaborator is injected so tests drop in a
counting fake (the N+1 guard counts the cohort query plus one coherent Beets
resolver batch). Per the service-first pattern the service body never imports
``web.server`` — the route passes the concrete banding function in.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, Protocol

import msgspec

from lib.banding import BAND_MISSING as _BAND_MISSING
from lib.banding import (
    CurrentBeetsBandingAmbiguityError,
    CurrentBeetsBandingIdentityError,
    CurrentBeetsBandingUnavailableError,
)
from lib.beets_db import beets_authority_availability_category
from lib.release_identity import (
    ConflictingReleaseIdentityError,
    ReleaseIdentity,
)

BAND_MISSING = _BAND_MISSING


class LongTailIdentityError(ValueError):
    """A worklist request has no single strict exact release identity."""


class LongTailBandingUnavailableError(RuntimeError):
    """The Beets banding authority omitted a queried exact identity."""


class LongTailPublicError(msgspec.Struct, frozen=True):
    """Stable public classification for an expected Long Tail failure.

    The service continues to raise its typed exceptions. HTTP and CLI
    adapters call :func:`classify_long_tail_failure` and serialize this one
    payload, so their status/exit mappings cannot drift.
    """

    category: Literal["conflict", "unavailable"]
    error: Literal[
        "long_tail_authority_conflict",
        "long_tail_authority_unavailable",
    ]
    message: str

    @property
    def http_status(self) -> Literal[409, 503]:
        return 409 if self.category == "conflict" else 503

    @property
    def cli_exit_code(self) -> Literal[4, 5]:
        return 4 if self.category == "conflict" else 5


_LONG_TAIL_AUTHORITY_CONFLICT = LongTailPublicError(
    category="conflict",
    error="long_tail_authority_conflict",
    message="Long-tail exact release authority is ambiguous or invalid.",
)

_LONG_TAIL_AUTHORITY_UNAVAILABLE = LongTailPublicError(
    category="unavailable",
    error="long_tail_authority_unavailable",
    message="Current Beets authority is unavailable; retry later.",
)


def classify_long_tail_failure(exc: Exception) -> LongTailPublicError | None:
    """Classify only expected public failures; leave defects unhandled.

    Missing/locked Beets authority and incomplete resolver output are
    retryable. Ambiguous current topology or malformed/conflicting exact
    request identity is an integrity conflict. SQLite schema/programmer
    failures and unrelated exceptions intentionally return ``None`` so the
    route keeps its generic 500 and the CLI preserves the original exception.
    """
    if isinstance(exc, (
        CurrentBeetsBandingUnavailableError,
        LongTailBandingUnavailableError,
    )) or beets_authority_availability_category(exc) is not None:
        return _LONG_TAIL_AUTHORITY_UNAVAILABLE
    if isinstance(exc, (
        CurrentBeetsBandingAmbiguityError,
        CurrentBeetsBandingIdentityError,
        ConflictingReleaseIdentityError,
        LongTailIdentityError,
    )):
        return _LONG_TAIL_AUTHORITY_CONFLICT
    return None


class LongTailRow(msgspec.Struct, frozen=True):
    """One ``wanted`` request, pre-banded and rescue-stamped.

    ``band`` is one of ``"missing"`` / ``"unknown"`` / a lowercase
    ``QualityRank`` name. ``in_flight_rescue`` is the
    ``youtube_running`` flag stamped by the cohort query. The remaining
    columns are the operator-facing subset the console header renders
    (``min_bitrate``, ``target_format``, ``search_filetype_override``)
    plus the identity columns used by tab / search filtering client-side.

    ``track_count`` is the ``album_tracks`` row count for the request
    (the pressing's expected track count — the card meta renders it
    alongside year + MB/Discogs as the pressing-disambiguation triple).
    ``current_spectral_grade`` / ``current_spectral_bitrate`` are the
    denormalised on-disk spectral measurement, NULL when unknown
    (pre-2026-05-17 imports or lossy-source transcodes) — the expanded
    view surfaces them only when present ("if known").

    ``mb_release_group_id`` backs the console's accept-sibling control
    and siblings panel directly off the worklist row (#398) — no
    client-side stamp from the pipeline-detail fetch, so the single-row
    refetch-and-patch (KTD8) can never drop it. NULL for Discogs-sourced
    requests and legacy MB rows that predate the column.

    ``current_spectral_accusation_admissible`` /
    ``current_spectral_accusation_withheld`` are the audit-only display
    pair for the grade above them (issue #829 Phase 5 PR4) — derived, not
    stored, and both NULL on a request with no linked current evidence,
    which leaves the console chip on its historical accusing render.
    """

    id: int
    artist_name: str
    album_title: str
    year: int | None
    status: str
    source: str | None
    mb_release_id: str | None
    mb_release_group_id: str | None
    discogs_release_id: str | None
    target_format: str | None
    min_bitrate: int | None
    search_filetype_override: str | None
    unfindable_category: str | None
    track_count: int
    current_spectral_grade: str | None
    current_spectral_bitrate: int | None
    band: str
    in_flight_rescue: bool
    current_spectral_accusation_admissible: bool | None = None
    current_spectral_accusation_withheld: str | None = None


class LongTailResult(msgspec.Struct, frozen=True):
    """The full worklist payload.

    ``outcome`` is ``"ok"`` on success — the read has no error branch
    today (a bad ``band`` filter is rejected by the wrapper before the
    service runs), but the field keeps the result shape symmetric with
    the rest of the service layer's typed results and gives the wrappers
    a single field to map onto status / exit codes.

    ``band_filter`` echoes the optional ``band`` argument (``None`` for
    the unfiltered full-cohort fetch the UI uses).
    """

    outcome: str
    rows: list[LongTailRow]
    band_filter: str | None


# ``band_fn`` maps request rows to a complete ``{acquisition_release_id:
# band}`` dict in a bounded number of queries. It takes rows, not bare ids,
# because a request resolves over the union of its acquisition id and any
# MusicBrainz merge survivor (#1059) — the key stays the acquisition id,
# which is what the long-tail row displays. The route wires this to the exact
# Beets resolver banding core in ``web.routes._overlay``; tests inject a
# counting fake. Every queried row must be present in the result, including
# ids explicitly banded ``Missing``.
BandFn = Callable[[list[dict[str, Any]]], dict[str, str]]


class _PipelineDB(Protocol):
    """Duck-typed pipeline DB — service body never imports the concrete
    class so tests can drop in a ``FakePipelineDB`` without monkey-patching.
    """

    def get_long_tail_cohort(self) -> list[dict[str, Any]]: ...

    def get_long_tail_request(
        self, request_id: int,
    ) -> dict[str, Any] | None: ...


def list_long_tail(
    pdb: _PipelineDB,
    band_fn: BandFn,
    *,
    band: str | None = None,
) -> LongTailResult:
    """Return the ``wanted`` cohort pre-banded and rescue-stamped.

    Bounded query fan-out regardless of cohort size:

    1. ``get_long_tail_cohort`` — one Postgres query for the whole
       ``wanted`` set, each row carrying ``in_flight_rescue``.
    2. ``band_fn`` — the Beets exact-resolution core, batched once over the
       whole strict exact-identity list, never per row.

    ``band`` optionally filters the result to a single band (backs the
    CLI's ``--band``). The UI fetches unfiltered and filters client-side.
    """
    rows = pdb.get_long_tail_cohort()
    out_rows = _band_rows(rows, band_fn)
    if band is not None:
        out_rows = [r for r in out_rows if r.band == band]
    return LongTailResult(outcome="ok", rows=out_rows, band_filter=band)


def band_one_long_tail(
    pdb: _PipelineDB,
    band_fn: BandFn,
    request_id: int,
) -> LongTailRow | None:
    """Band a single ``wanted`` request by id.

    Backs the post-action single-row refetch (KTD8) and the single-id
    variant of the worklist read (R16). Returns ``None`` when the row
    doesn't exist OR is no longer ``wanted`` (the cohort query is
    ``status='wanted'`` only — an imported / replaced row is correctly
    absent from the worklist). Uses the same banding path as
    ``list_long_tail`` so the single-row band always agrees with the
    cohort band.
    """
    row = pdb.get_long_tail_request(int(request_id))
    if row is None:
        return None
    banded = _band_rows([row], band_fn)
    return banded[0] if banded else None


def _band_rows(
    rows: list[dict[str, Any]],
    band_fn: BandFn,
) -> list[LongTailRow]:
    """Band a cohort by one strict MB-or-Discogs identity per request.

    The strict identity is still derived here so a malformed row fails with
    the operator-facing ``LongTailIdentityError`` before Beets is consulted;
    ``band_fn`` then resolves each row over its own identity union.
    """
    identities = [_strict_request_identity(row) for row in rows]
    release_ids = [identity.release_id for identity in identities]
    bands = band_fn(rows) if rows else {}
    missing_results = [
        release_id
        for release_id in dict.fromkeys(release_ids)
        if release_id not in bands
    ]
    if missing_results:
        raise LongTailBandingUnavailableError(
            "Beets banding returned no result for exact release identities: "
            + ", ".join(missing_results)
        )
    return [
        _band_row(row, identity, bands)
        for row, identity in zip(rows, identities, strict=True)
    ]


def _strict_request_identity(row: dict[str, Any]) -> ReleaseIdentity:
    """Return one exact identity or fail before claiming Beets absence."""
    identity = ReleaseIdentity.from_strict_fields(
        row.get("mb_release_id"),
        row.get("discogs_release_id"),
    )
    if identity is None:
        raise LongTailIdentityError(
            f"request {int(row['id'])} has missing, malformed, or conflicting "
            "exact release identity fields"
        )
    return identity


def _band_row(
    row: dict[str, Any],
    identity: ReleaseIdentity,
    bands: dict[str, str],
) -> LongTailRow:
    # Deferred so the service module keeps importing nothing from the web
    # layer at load time (the same shape ``lib/mbid_replace_service.py``
    # uses for ``web.mb``). The audit-only accusation rule has exactly one
    # owner and both worklist surfaces — API and ``pipeline-cli
    # long-tail`` — reach it through this call rather than through an
    # injected collaborator neither caller could vary meaningfully.
    from lib.pipeline_db._shared import CURRENT_EVIDENCE_PREFIX
    from web.classify import evidence_column_accusation_flags

    flags = evidence_column_accusation_flags(
        row, prefix=CURRENT_EVIDENCE_PREFIX)
    return LongTailRow(
        id=int(row["id"]),
        artist_name=str(row.get("artist_name") or ""),
        album_title=str(row.get("album_title") or ""),
        year=_int_or_none(row.get("year")),
        status=str(row.get("status") or ""),
        source=row.get("source"),
        mb_release_id=row.get("mb_release_id"),
        mb_release_group_id=row.get("mb_release_group_id"),
        discogs_release_id=row.get("discogs_release_id"),
        target_format=row.get("target_format"),
        min_bitrate=_int_or_none(row.get("min_bitrate")),
        search_filetype_override=row.get("search_filetype_override"),
        unfindable_category=row.get("unfindable_category"),
        track_count=_int_or_none(row.get("track_count")) or 0,
        current_spectral_grade=row.get("current_spectral_grade"),
        current_spectral_bitrate=_int_or_none(
            row.get("current_spectral_bitrate")),
        band=bands[identity.release_id],
        in_flight_rescue=bool(row.get("in_flight_rescue")),
        current_spectral_accusation_admissible=flags.admissible,
        current_spectral_accusation_withheld=flags.withheld,
    )


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value)
    return None
