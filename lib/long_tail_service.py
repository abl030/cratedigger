"""Long-tail worklist service — returns the ``wanted`` cohort with each
row pre-banded by on-disk quality (``Missing`` / a ``QualityRank`` band /
``Unknown``) and stamped with ``in_flight_rescue``.

This is the read backend for the Long-Tail Triage Console (U1). It is the
first UI consumer of the existing beets-library banding machinery —
``web.server.compute_library_rank`` via the beets-only banding core
factored out of ``web.routes._overlay`` — applied to ``album_requests``
rows keyed by ``mb_release_id`` rather than to beets release rows keyed
by release id.

Service-first (KTD2): the typed ``LongTailResult`` is the contract; the
HTTP route (``GET /api/pipeline/long-tail``) and the CLI
(``pipeline-cli long-tail``) are thin adapters that wrap this module and
map ``outcome`` onto status / exit codes. Both surfaces wrap the SAME
service method per CLI ⇄ API symmetry.

Banding rules (KTD1, three-way):

* ``mb_release_id`` absent from the beets membership set → ``Missing``.
* present in the library but no detail row / ``compute_library_rank``
  returns ``"unknown"`` → ``Unknown`` (has audio, never ``Missing``).
* otherwise → the lowercase ``QualityRank`` band (``transparent`` /
  ``excellent`` / ``good`` / ``acceptable`` / ``poor``).

The band labels are lowercase to match ``library_rank`` /
``badge-rank-*`` exactly so badge rendering comes for free.

``in_flight_rescue`` is stamped by the DB cohort query via the existing
``download_log`` predicate ``source='youtube' AND outcome='youtube_running'``
(inlined as an ``EXISTS`` in ``_RequestsMixin._LONG_TAIL_SELECT``), backed by
migration 037's partial unique index ``one_youtube_running_per_request`` —
never an N-query loop.

The banding ``band_fn`` collaborator is injected so tests drop in a
counting fake (the N+1 guard counts both the cohort query AND the beets
membership + ``check_mbids_detail`` queries). Per the service-first
pattern the service body never imports ``web.server`` — the route passes
the concrete banding function in.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Protocol

import msgspec


# The ``Missing`` band sentinel — a release id absent from the beets
# membership set has no library copy to upgrade. ``"unknown"`` (in-library
# but unrankable) and the lowercase ``QualityRank`` names come straight
# from ``compute_library_rank`` via the injected ``band_fn``, so they
# aren't re-declared here.
BAND_MISSING = "missing"


class LongTailRow(msgspec.Struct, frozen=True):
    """One ``wanted`` request, pre-banded and rescue-stamped.

    ``band`` is one of ``"missing"`` / ``"unknown"`` / a lowercase
    ``QualityRank`` name. ``in_flight_rescue`` is the
    ``youtube_running`` flag stamped by the cohort query. The remaining
    columns are the operator-facing subset the console header renders
    (``min_bitrate``, ``target_format``, ``search_filetype_override``)
    plus the identity columns used by tab / search filtering client-side.
    """

    id: int
    artist_name: str
    album_title: str
    year: Optional[int]
    status: str
    source: Optional[str]
    mb_release_id: Optional[str]
    discogs_release_id: Optional[str]
    target_format: Optional[str]
    min_bitrate: Optional[int]
    search_filetype_override: Optional[str]
    unfindable_category: Optional[str]
    band: str
    in_flight_rescue: bool


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
    band_filter: Optional[str]


# ``band_fn`` maps a list of release ids (``mb_release_id`` values) to a
# ``{release_id: band}`` dict in a bounded number of queries. The route
# wires this to the beets-only banding core in ``web.routes._overlay``;
# tests inject a counting fake. Release ids absent from the returned dict
# band as ``Missing``.
BandFn = Callable[[list[str]], dict[str, str]]


class _PipelineDB(Protocol):
    """Duck-typed pipeline DB — service body never imports the concrete
    class so tests can drop in a ``FakePipelineDB`` without monkey-patching.
    """

    def get_long_tail_cohort(self) -> list[dict[str, Any]]: ...

    def get_long_tail_request(
        self, request_id: int,
    ) -> Optional[dict[str, Any]]: ...


def list_long_tail(
    pdb: _PipelineDB,
    band_fn: BandFn,
    *,
    band: Optional[str] = None,
) -> LongTailResult:
    """Return the ``wanted`` cohort pre-banded and rescue-stamped.

    Bounded query fan-out regardless of cohort size:

    1. ``get_long_tail_cohort`` — one Postgres query for the whole
       ``wanted`` set, each row carrying ``in_flight_rescue``.
    2. ``band_fn`` — the beets-only banding core, batched over the whole
       ``mb_release_id`` list (membership + ``check_mbids_detail``), never
       per row.

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
) -> Optional[LongTailRow]:
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
    """Band a cohort of request rows by ``mb_release_id`` in one batch."""
    release_ids = [
        str(r["mb_release_id"])
        for r in rows
        if r.get("mb_release_id")
    ]
    bands = band_fn(release_ids) if release_ids else {}
    return [_band_row(r, bands) for r in rows]


def _band_row(row: dict[str, Any], bands: dict[str, str]) -> LongTailRow:
    rid = row.get("mb_release_id")
    band = bands.get(str(rid), BAND_MISSING) if rid else BAND_MISSING
    return LongTailRow(
        id=int(row["id"]),
        artist_name=str(row.get("artist_name") or ""),
        album_title=str(row.get("album_title") or ""),
        year=_int_or_none(row.get("year")),
        status=str(row.get("status") or ""),
        source=row.get("source"),
        mb_release_id=row.get("mb_release_id"),
        discogs_release_id=row.get("discogs_release_id"),
        target_format=row.get("target_format"),
        min_bitrate=_int_or_none(row.get("min_bitrate")),
        search_filetype_override=row.get("search_filetype_override"),
        unfindable_category=row.get("unfindable_category"),
        band=band,
        in_flight_rescue=bool(row.get("in_flight_rescue")),
    )


def _int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
