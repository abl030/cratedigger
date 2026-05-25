#!/usr/bin/env python3
"""Dual-source field-resolution backfill — search-plan iteration 2 (U3).

Operator-invokable backfill that walks every existing request and resolves
the R15 fields via U2's ``lib/field_resolver_service``. Idempotent;
re-runnable. Writes every resolution attempt to the side table
``album_request_field_resolutions`` (migration 030) and updates the
parent ``album_requests`` / ``album_tracks`` columns when a value is
returned.

This script supersedes ``scripts/backfill_release_group_year.py`` — the
old script handles only ``release_group_year`` against the MB mirror and
ignores the side-table audit. The new script:

  * Dispatches across MB + Discogs via the dual-source resolver.
  * Backfills five fields: ``release_group_year``, ``release_group_id``,
    ``track_artist``, ``catalog_number``, ``is_va_compilation``.
  * Backfills the ``one_track_structural`` unfindable category at the
    same time (single-track requests get categorised without waiting for
    U13's periodic probes).
  * Filters by the U2 retry-window policy — ``unresolved_404`` and
    ``unresolved_field_missing_upstream`` are 30d sticky;
    ``unresolved_timeout`` / ``unresolved_mirror_unavailable`` retry
    after 1d; ``unresolved_malformed`` is permanent.
  * Writes every attempt's outcome to the side table via the resolver
    service — re-runs increment ``attempts`` and update ``status``.

Idempotency invariants:
  * ``--field=release_group_year`` (and the other resolver-backed
    fields): re-running on an already-resolved row skips it because the
    side-table filter excludes it.
  * ``--field=is_va_compilation``: re-running on an already-correct row
    is a no-op — the write only happens when the computed value differs.
  * ``--field=one_track_structural``: re-running is a no-op — only rows
    with ``unfindable_category IS NULL`` are eligible.

Plan reference: U3 of
``docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Protocol

import psycopg2.extras

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.field_resolver_service import (  # noqa: E402
    FIELD_CATALOG_NUMBER,
    FIELD_RELEASE_GROUP_ID,
    FIELD_RELEASE_GROUP_YEAR,
    FIELD_TRACK_ARTIST,
    MB_VA_ARTIST_MBID,
    DISCOGS_VA_ARTIST_ID,
    ResolverResult,
    detect_va_compilation,
    resolve_catalog_number,
    resolve_release_group_id,
    resolve_release_group_year,
    resolve_track_artists,
)

logger = logging.getLogger("backfill_field_resolutions")

# Progress-print cadence + commit batch boundary. The DB connection runs
# in autocommit mode (per `.claude/rules/pipeline-db.md`), so every row's
# write is durable independently; "batch" here means "log progress every
# N rows" rather than "wrap N rows in one transaction".
BATCH_SIZE = 100


# === Retry-window policy ================================================
#
# Mirrors the per-status retry policy pinned in
# ``lib/field_resolver_service.py`` and the U3 section of the plan. A
# side-table row is eligible for re-resolution iff
# ``resolved_at + retry_window(status) < NOW()``. ``resolved`` rows are
# never re-resolved by automated paths.

_INFINITE = timedelta(days=365 * 100)  # "never re-resolved" sentinel

_RETRY_WINDOWS: dict[str, timedelta] = {
    "resolved": _INFINITE,
    "unresolved_404": timedelta(days=30),
    "unresolved_field_missing_upstream": timedelta(days=30),
    "unresolved_timeout": timedelta(days=1),
    "unresolved_mirror_unavailable": timedelta(days=1),
    "unresolved_malformed": _INFINITE,
}


def _is_retry_eligible(
    *,
    status: str,
    resolved_at: datetime | None,
    now: datetime,
) -> bool:
    """Return True when the (status, resolved_at) pair is past its retry window.

    A row with no side-table entry (``resolved_at is None``) is always
    eligible — that's the "never tried" case.
    """
    if resolved_at is None:
        return True
    window = _RETRY_WINDOWS.get(status)
    if window is None:
        # Unknown status — treat as transient and retry conservatively
        # (1d). Pinning the table above is the single source of truth;
        # a status we don't recognise probably means a service-layer
        # change without a backfill update.
        window = timedelta(days=1)
    return (resolved_at + window) < now


# === Counters & result types ============================================


@dataclass
class FieldBackfillCounters:
    """Per-field outcome rollup. Returned from each ``run_*`` call."""

    field_name: str
    fetched: int = 0
    resolved: int = 0          # value returned & parent column written
    unresolved: int = 0        # resolver returned an unresolved_* status
    skipped: int = 0           # retry-window guarded; resolver not called
    errors: int = 0            # unexpected exception escaping the resolver

    def summary(self) -> str:
        return (
            f"{self.field_name}: fetched={self.fetched} resolved={self.resolved} "
            f"unresolved={self.unresolved} skipped={self.skipped} "
            f"errors={self.errors}"
        )


@dataclass
class BackfillSummary:
    """Aggregate result returned from ``main()``. Each field's counters
    appears in ``per_field``; the top-level totals are convenience
    aggregates so callers can branch on "did anything resolve?".
    """

    per_field: dict[str, FieldBackfillCounters] = field(default_factory=dict)
    va_examined: int = 0
    va_flipped_true: int = 0
    one_track_examined: int = 0
    one_track_categorised: int = 0


# === DB-access protocols ================================================
#
# The backfill is structured so a ``FakePipelineDB`` test can run the
# whole flow without a real PG instance. Production passes ``db`` (a
# ``PipelineDB``) directly; tests inject ``FakePipelineDB`` plus a
# matching ``Selector`` for rows.


class _DbLike(Protocol):
    """Minimal DB surface the backfill uses for parent-row writes.

    Both ``PipelineDB`` and ``FakePipelineDB`` satisfy this without any
    new methods -- everything below is already in U2's surface.
    """

    def update_request_fields(self, request_id: int, **fields: Any) -> None: ...
    def get_field_resolution(
        self, request_id: int, field_name: str,
    ) -> dict[str, Any] | None: ...
    def record_field_resolution(
        self, request_id: int, field_name: str,
        status: str, reason_code: str | None,
    ) -> None: ...


# Row selectors return raw dicts with the columns the resolvers read
# (id, source-related ids, etc.) plus the current value of the field
# being backfilled (so we can skip rows that already have it set, if
# applicable to the field).

Selector = Callable[[], Iterator[dict[str, Any]]]
"""Yield rows eligible for backfill, in id order. Production walks via
raw SQL; tests use an in-memory iterator over the fake's state."""


TrackArtistWriter = Callable[[int, int, int, str], None]
"""``writer(request_id, disc_number, track_number, artist)``. Production
runs raw SQL on ``db.conn``; tests pass a function that mutates the fake's
``_tracks``."""


# === Selectors (production = raw SQL, tests pass fakes) =================


def _select_requests_needing_resolution_factory(
    *,
    db: Any,
    field_name: str,
    parent_column: str | None,
    now: datetime,
    limit: int | None,
) -> Selector:
    """Build a Selector that yields rows where the side table says we
    should retry this field, or no side-table row exists yet.

    ``parent_column`` (when not None) restricts to rows where the parent
    column is currently NULL — so a resolver-backed field that already
    has a value is skipped on subsequent runs.
    """

    def _iter() -> Iterator[dict[str, Any]]:
        parts = ["SELECT ar.id, ar.mb_release_id, ar.mb_release_group_id, "
                 "       ar.mb_artist_id, ar.discogs_release_id, "
                 "       ar.artist_name, ar.album_title, ar.source, "
                 "       ar.year, ar.release_group_year, ar.is_va_compilation, "
                 "       ar.unfindable_category, "
                 "       arfr.resolved_at AS arfr_resolved_at, "
                 "       arfr.status AS arfr_status",
                 "FROM album_requests ar",
                 "LEFT JOIN album_request_field_resolutions arfr",
                 "  ON arfr.request_id = ar.id AND arfr.field_name = %s"]
        params: list[Any] = [field_name]

        wheres: list[str] = []
        # Parent column null gate. A row that has a value already wins
        # the retry-window race -- we don't re-fetch resolved fields.
        if parent_column is not None:
            wheres.append(f"ar.{parent_column} IS NULL")
        # We don't filter on the side-table status at SQL level -- we
        # evaluate the retry window in Python so the policy lives in
        # ONE constant table at the top of this module.
        if wheres:
            parts.append("WHERE " + " AND ".join(wheres))
        parts.append("ORDER BY ar.id")
        if limit is not None:
            parts.append(f"LIMIT {int(limit)}")

        sql = "\n".join(parts)
        # RealDictCursor is required — the row consumers below call
        # ``dict(row)`` and the production code path was unprotected by
        # tests (every test injects a Selector that yields dicts from
        # FakePipelineDB._requests, so a default tuple cursor would
        # crash on first row with ``TypeError: cannot convert dictionary
        # update sequence element #0 to a sequence``).
        cur = db.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(sql, tuple(params))
            yielded = 0
            for row in cur.fetchall():
                row_dict = dict(row)
                status = row_dict.get("arfr_status")
                resolved_at = row_dict.get("arfr_resolved_at")
                if status and not _is_retry_eligible(
                    status=str(status),
                    resolved_at=resolved_at,
                    now=now,
                ):
                    continue
                yield row_dict
                yielded += 1
                if limit is not None and yielded >= limit:
                    break
        finally:
            cur.close()

    return _iter


def _select_all_requests_factory(
    *,
    db: Any,
    limit: int | None,
    where_extra: str | None = None,
) -> Selector:
    """Build a Selector that yields every album_requests row in id order.

    Used by ``is_va_compilation`` (walks every row) and the
    ``one_track_structural`` categoriser (filters by single-track + null
    unfindable category).
    """

    def _iter() -> Iterator[dict[str, Any]]:
        sql_parts = [
            "SELECT ar.id, ar.mb_release_id, ar.mb_release_group_id,",
            "       ar.mb_artist_id, ar.discogs_release_id,",
            "       ar.artist_name, ar.album_title, ar.source,",
            "       ar.year, ar.release_group_year, ar.is_va_compilation,",
            "       ar.unfindable_category,",
            "       (SELECT COUNT(*) FROM album_tracks at",
            "         WHERE at.request_id = ar.id) AS track_count",
            "FROM album_requests ar",
        ]
        if where_extra:
            sql_parts.append(f"WHERE {where_extra}")
        sql_parts.append("ORDER BY ar.id")
        if limit is not None:
            sql_parts.append(f"LIMIT {int(limit)}")
        sql = "\n".join(sql_parts)
        # RealDictCursor — see the matching comment in
        # _select_requests_needing_resolution_factory above.
        cur = db.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(sql)
            for row in cur.fetchall():
                yield dict(row)
        finally:
            cur.close()

    return _iter


# === Track-artist writer (production = raw SQL, tests pass fake) ========


def _default_track_artist_writer(db: Any) -> TrackArtistWriter:
    """Build a writer that updates ``album_tracks.track_artist`` for one
    track via raw SQL on ``db.conn``.

    The script keeps this inline rather than introducing a new
    ``PipelineDB`` method — the U3 surface is intentionally tight; the
    per-row UPDATE is content-addressed by (request_id, disc_number,
    track_number) which is the natural key of ``album_tracks``.
    """

    def _write(
        request_id: int, disc_number: int, track_number: int, artist: str,
    ) -> None:
        cur = db.conn.cursor()
        try:
            cur.execute(
                """
                UPDATE album_tracks
                SET track_artist = %s
                WHERE request_id = %s
                  AND disc_number = %s
                  AND track_number = %s
                """,
                (artist, request_id, disc_number, track_number),
            )
        finally:
            cur.close()

    return _write


# === Field runners ======================================================


def run_release_group_year(
    *,
    db: _DbLike,
    selector: Selector,
    resolver: Callable[..., ResolverResult] = resolve_release_group_year,
    dry_run: bool = False,
    batch_size: int = BATCH_SIZE,
) -> FieldBackfillCounters:
    """Backfill ``release_group_year`` for every eligible request."""
    c = FieldBackfillCounters(field_name=FIELD_RELEASE_GROUP_YEAR)
    for row in selector():
        c.fetched += 1
        try:
            result = resolver(row, db)
        except Exception:  # noqa: BLE001
            c.errors += 1
            logger.exception(
                "release_group_year resolver crashed for request=%s",
                row.get("id"),
            )
            _progress(c)
            continue
        if result.status == "resolved" and result.value is not None:
            c.resolved += 1
            if not dry_run:
                db.update_request_fields(
                    int(row["id"]), release_group_year=int(result.value),
                )
        else:
            c.unresolved += 1
        _progress(c, batch_size=batch_size)
    return c


def run_release_group_id(
    *,
    db: _DbLike,
    selector: Selector,
    resolver: Callable[..., ResolverResult] = resolve_release_group_id,
    dry_run: bool = False,
    batch_size: int = BATCH_SIZE,
) -> FieldBackfillCounters:
    """Backfill ``mb_release_group_id`` for every eligible request."""
    c = FieldBackfillCounters(field_name=FIELD_RELEASE_GROUP_ID)
    for row in selector():
        c.fetched += 1
        try:
            result = resolver(row, db)
        except Exception:  # noqa: BLE001
            c.errors += 1
            logger.exception(
                "release_group_id resolver crashed for request=%s",
                row.get("id"),
            )
            _progress(c)
            continue
        if result.status == "resolved" and result.value is not None:
            c.resolved += 1
            if not dry_run:
                db.update_request_fields(
                    int(row["id"]),
                    mb_release_group_id=str(result.value),
                )
        else:
            c.unresolved += 1
        _progress(c, batch_size=batch_size)
    return c


def run_catalog_number(
    *,
    db: _DbLike,
    selector: Selector,
    resolver: Callable[..., ResolverResult] = resolve_catalog_number,
    dry_run: bool = False,
    batch_size: int = BATCH_SIZE,
) -> FieldBackfillCounters:
    """Backfill ``catalog_number`` -- but there's no column for it today.

    The plan reserves a ``catalog_number`` slot at the generator (R2 in
    Phase 2). For now, the resolver records the side-table row so the
    enqueue-time path (U4) and the generator (PR2) can consume the
    value when added. No parent column write happens.
    """
    c = FieldBackfillCounters(field_name=FIELD_CATALOG_NUMBER)
    for row in selector():
        c.fetched += 1
        try:
            result = resolver(row, db)
        except Exception:  # noqa: BLE001
            c.errors += 1
            logger.exception(
                "catalog_number resolver crashed for request=%s",
                row.get("id"),
            )
            _progress(c)
            continue
        if result.status == "resolved":
            c.resolved += 1
            # No parent column to write yet -- the side table carries
            # the value via reason_code for now. The Phase 2 generator
            # will consume it.
        else:
            c.unresolved += 1
        _progress(c, batch_size=batch_size)
    return c


def run_track_artist(
    *,
    db: _DbLike,
    selector: Selector,
    track_artist_writer: TrackArtistWriter,
    get_tracks: Callable[[int], list[dict[str, Any]]],
    resolver: Callable[..., list[ResolverResult]] = resolve_track_artists,
    dry_run: bool = False,
    batch_size: int = BATCH_SIZE,
) -> FieldBackfillCounters:
    """Backfill per-track ``track_artist`` for every eligible request.

    Per-track writes go through ``track_artist_writer`` so production
    and test paths can differ. The resolver returns one
    ``ResolverResult`` per track in upstream order; we align against
    the request's existing ``album_tracks`` rows (ordered by
    ``(disc_number, track_number)``).
    """
    c = FieldBackfillCounters(field_name=FIELD_TRACK_ARTIST)
    for row in selector():
        c.fetched += 1
        request_id = int(row["id"])
        try:
            results = resolver(row, db)
        except Exception:  # noqa: BLE001
            c.errors += 1
            logger.exception(
                "track_artist resolver crashed for request=%s", request_id,
            )
            _progress(c)
            continue

        tracks = get_tracks(request_id)
        if not results:
            c.unresolved += 1
            _progress(c, batch_size=batch_size)
            continue

        # Resolver returns one result per track in payload order; align
        # against the request's existing track rows. Use min() so a
        # length mismatch (upstream has more/fewer tracks than the DB)
        # doesn't crash -- partial backfill is better than zero.
        n_written = 0
        for track, result in zip(tracks, results):
            if result.status != "resolved" or result.value is None:
                continue
            if dry_run:
                n_written += 1
                continue
            track_artist_writer(
                request_id,
                int(track.get("disc_number") or 1),
                int(track["track_number"]),
                str(result.value),
            )
            n_written += 1

        if n_written > 0:
            c.resolved += 1
        else:
            c.unresolved += 1
        _progress(c, batch_size=batch_size)
    return c


def run_is_va_compilation(
    *,
    db: _DbLike,
    selector: Selector,
    detect: Callable[..., bool] = detect_va_compilation,
    mb_release_payload_fn: Callable[[str], dict[str, Any] | None] | None = None,
    mb_release_group_payload_fn: (
        Callable[[str], dict[str, Any] | None] | None
    ) = None,
    discogs_release_payload_fn: (
        Callable[[str], dict[str, Any] | None] | None
    ) = None,
    dry_run: bool = False,
    batch_size: int = BATCH_SIZE,
) -> tuple[FieldBackfillCounters, int, int]:
    """Walk every request and update ``is_va_compilation`` when the
    detector disagrees with the current value.

    The column is ``NOT NULL DEFAULT FALSE`` so we can't filter to NULL
    rows. ``detect_va_compilation`` takes optional payload kwargs; for
    the backfill we lean on Rule 1 (primary-artist-credit identity --
    works off the row's ``mb_artist_id`` alone) plus optional payload
    fetchers when supplied. The live-DB ground truth (25 VA rows
    credited to canonical "Various Artists" MBID) is Rule 1, so the
    25-row regression guard test passes against the no-payload path.
    """
    c = FieldBackfillCounters(field_name="is_va_compilation")
    examined = 0
    flipped_true = 0
    for row in selector():
        c.fetched += 1
        examined += 1
        request_id = int(row["id"])

        # Optionally fetch payloads. Each fetcher returns None on miss /
        # unsupported source so the detector falls back to Rule 1.
        mb_release_payload = None
        mb_release_group_payload = None
        discogs_release_payload = None
        try:
            if mb_release_payload_fn and row.get("mb_release_id"):
                mb_release_payload = mb_release_payload_fn(
                    str(row["mb_release_id"])
                )
            if mb_release_group_payload_fn and row.get("mb_release_group_id"):
                mb_release_group_payload = mb_release_group_payload_fn(
                    str(row["mb_release_group_id"])
                )
            if discogs_release_payload_fn and row.get("discogs_release_id"):
                discogs_release_payload = discogs_release_payload_fn(
                    str(row["discogs_release_id"])
                )
        except Exception:  # noqa: BLE001
            # Payload fetch failure is recoverable -- fall back to
            # Rule 1 with no payload. Count the error so operators see
            # mirror trouble; don't abort the row.
            c.errors += 1
            logger.exception(
                "is_va_compilation payload fetch failed for request=%s",
                request_id,
            )

        try:
            computed = detect(
                row,
                mb_release_payload=mb_release_payload,
                mb_release_group_payload=mb_release_group_payload,
                discogs_release_payload=discogs_release_payload,
            )
        except Exception:  # noqa: BLE001
            c.errors += 1
            logger.exception(
                "is_va_compilation detector crashed for request=%s",
                request_id,
            )
            _progress(c)
            continue

        current = bool(row.get("is_va_compilation"))
        if computed == current:
            # Idempotent: never write when the row already matches.
            c.unresolved += 1  # "no change made"
        else:
            c.resolved += 1
            if computed and not current:
                flipped_true += 1
            if not dry_run:
                db.update_request_fields(
                    request_id, is_va_compilation=bool(computed),
                )
        _progress(c, batch_size=batch_size)
    return c, examined, flipped_true


def run_one_track_structural(
    *,
    db: _DbLike,
    selector: Selector,
    dry_run: bool = False,
    batch_size: int = BATCH_SIZE,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> tuple[FieldBackfillCounters, int, int]:
    """Categorise every single-track / unfindable_category=NULL request
    as ``one_track_structural``.

    Detection is structural (track count = 1), not probe-driven, so the
    backfill is the one-shot path -- U13's periodic probes don't need
    to re-derive it. Multi-track requests untouched.
    """
    c = FieldBackfillCounters(field_name="one_track_structural")
    examined = 0
    categorised = 0
    for row in selector():
        c.fetched += 1
        examined += 1
        request_id = int(row["id"])
        track_count = int(row.get("track_count") or 0)

        if track_count != 1:
            c.unresolved += 1  # not eligible, but the row was inspected
            _progress(c, batch_size=batch_size)
            continue
        if row.get("unfindable_category") is not None:
            # Already categorised -- idempotent. Don't overwrite an
            # operator-set category with the structural label.
            c.unresolved += 1
            _progress(c, batch_size=batch_size)
            continue

        c.resolved += 1
        categorised += 1
        if not dry_run:
            db.update_request_fields(
                request_id,
                unfindable_category="one_track_structural",
                unfindable_categorised_at=now_fn(),
            )
        _progress(c, batch_size=batch_size)
    return c, examined, categorised


# === Progress logging ===================================================


def _progress(c: FieldBackfillCounters, *, batch_size: int = BATCH_SIZE) -> None:
    """Log progress every ``batch_size`` rows. Cheap; no DB hit."""
    if c.fetched == 0:
        return
    if c.fetched % batch_size == 0:
        logger.info(c.summary())


# === Top-level orchestration ============================================


_ALL_FIELDS = (
    FIELD_RELEASE_GROUP_YEAR,
    FIELD_RELEASE_GROUP_ID,
    FIELD_TRACK_ARTIST,
    FIELD_CATALOG_NUMBER,
    "is_va_compilation",
    "one_track_structural",
)

FIELD_CHOICES = ("all", *_ALL_FIELDS)


def run_backfill(
    *,
    db: _DbLike,
    field_name: str,
    limit: int | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
    # Test-only injection: callers pass custom selectors / writers /
    # payload fetchers; production paths use the defaults defined below.
    selector_factory: (
        Callable[..., Selector] | None
    ) = None,
    track_artist_writer: TrackArtistWriter | None = None,
    get_tracks_fn: Callable[[int], list[dict[str, Any]]] | None = None,
    mb_release_payload_fn: Callable[[str], dict[str, Any] | None] | None = None,
    mb_release_group_payload_fn: (
        Callable[[str], dict[str, Any] | None] | None
    ) = None,
    discogs_release_payload_fn: (
        Callable[[str], dict[str, Any] | None] | None
    ) = None,
) -> BackfillSummary:
    """Run the backfill for one or all fields. Returns rollup counters.

    ``selector_factory`` and ``get_tracks_fn`` default to PG-backed
    implementations (raw SQL on ``db.conn``); tests pass in-memory
    versions so the whole flow runs against ``FakePipelineDB``.
    """
    now = now or datetime.now(timezone.utc)
    summary = BackfillSummary()

    fields_to_run = (
        list(_ALL_FIELDS)
        if field_name == "all" else [field_name]
    )

    # Default selector factories. Each call wires the right SQL filter
    # for the field being backfilled.
    def _default_resolver_selector(
        f_name: str, parent_column: str | None,
    ) -> Selector:
        return _select_requests_needing_resolution_factory(
            db=db, field_name=f_name, parent_column=parent_column,
            now=now, limit=limit,
        )

    def _default_all_selector(where_extra: str | None = None) -> Selector:
        return _select_all_requests_factory(
            db=db, limit=limit, where_extra=where_extra,
        )

    if selector_factory is None:
        sel_resolver = _default_resolver_selector
        sel_all = _default_all_selector
    else:
        sel_resolver = lambda f, p: selector_factory(  # noqa: E731
            field_name=f, parent_column=p,
        )
        sel_all = lambda where_extra=None: selector_factory(  # noqa: E731
            field_name="__all__", parent_column=None,
            where_extra=where_extra,
        )

    track_artist_writer_eff = (
        track_artist_writer or _default_track_artist_writer(db)
    )

    if get_tracks_fn is None:
        def _default_get_tracks(rid: int) -> list[dict[str, Any]]:
            return db.get_tracks(rid)  # type: ignore[attr-defined]
        get_tracks_fn_eff = _default_get_tracks
    else:
        get_tracks_fn_eff = get_tracks_fn

    for f in fields_to_run:
        if f == FIELD_RELEASE_GROUP_YEAR:
            summary.per_field[f] = run_release_group_year(
                db=db,
                selector=sel_resolver(f, "release_group_year"),
                dry_run=dry_run,
            )
        elif f == FIELD_RELEASE_GROUP_ID:
            summary.per_field[f] = run_release_group_id(
                db=db,
                selector=sel_resolver(f, "mb_release_group_id"),
                dry_run=dry_run,
            )
        elif f == FIELD_CATALOG_NUMBER:
            # No parent column to filter on -- the side table is the
            # only gate. Pass parent_column=None so the selector skips
            # the IS NULL clause.
            summary.per_field[f] = run_catalog_number(
                db=db,
                selector=sel_resolver(f, None),
                dry_run=dry_run,
            )
        elif f == FIELD_TRACK_ARTIST:
            summary.per_field[f] = run_track_artist(
                db=db,
                selector=sel_resolver(f, None),
                track_artist_writer=track_artist_writer_eff,
                get_tracks=get_tracks_fn_eff,
                dry_run=dry_run,
            )
        elif f == "is_va_compilation":
            counters, examined, flipped = run_is_va_compilation(
                db=db,
                selector=sel_all(None),
                mb_release_payload_fn=mb_release_payload_fn,
                mb_release_group_payload_fn=mb_release_group_payload_fn,
                discogs_release_payload_fn=discogs_release_payload_fn,
                dry_run=dry_run,
            )
            summary.per_field[f] = counters
            summary.va_examined = examined
            summary.va_flipped_true = flipped
        elif f == "one_track_structural":
            counters, examined, categorised = run_one_track_structural(
                db=db,
                selector=sel_all("unfindable_category IS NULL"),
                dry_run=dry_run,
            )
            summary.per_field[f] = counters
            summary.one_track_examined = examined
            summary.one_track_categorised = categorised
        else:
            raise ValueError(f"unknown --field value: {f!r}")

    return summary


# === CLI ================================================================


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Dual-source field-resolution backfill (U3 of search-plan "
            "iteration 2). Walks every existing album_requests row and "
            "resolves R15 fields via the MB or Discogs mirror; records "
            "every attempt to album_request_field_resolutions; updates "
            "parent columns when a value is returned."
        ),
    )
    p.add_argument(
        "--field",
        choices=FIELD_CHOICES,
        default="all",
        help="Which field to backfill (default: all)",
    )
    p.add_argument(
        "--dsn",
        default=os.environ.get(
            "PIPELINE_DB_DSN",
            "postgresql://cratedigger@192.168.100.11:5432/cratedigger",
        ),
        help="PostgreSQL DSN for the pipeline DB",
    )
    p.add_argument(
        "--limit", type=int, default=None,
        help="Process at most this many rows total per field (staging)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Walk + classify but do not write back to the DB",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="DEBUG-level logging",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from lib.pipeline_db import (
        ADVISORY_LOCK_NAMESPACE_BACKFILL,
        PipelineDB,
    )
    from web import mb as _mb
    from web import discogs as _discogs

    def _mb_release_payload(release_mbid: str) -> dict[str, Any] | None:
        try:
            return _mb.get_release(release_mbid)
        except Exception:  # noqa: BLE001
            logger.exception("mb.get_release failed for %s", release_mbid)
            return None

    def _mb_release_group_payload(
        rg_mbid: str,
    ) -> dict[str, Any] | None:
        try:
            # web.mb.get_release_group returns a stripped {type, ...}
            # shape, but the VA detector expects ``primary-type``. Build
            # a synthetic payload so Rule 2 still fires when the
            # underlying group is a Compilation.
            stripped = _mb.get_release_group(rg_mbid)
            if not isinstance(stripped, dict):
                return None
            return {"primary-type": stripped.get("type", "")}
        except Exception:  # noqa: BLE001
            logger.exception(
                "mb.get_release_group failed for %s", rg_mbid,
            )
            return None

    def _discogs_release_payload(
        release_id: str,
    ) -> dict[str, Any] | None:
        try:
            stripped = _discogs.get_release(int(release_id))
            if not isinstance(stripped, dict):
                return None
            # The stripped Discogs shape carries ``artist_id`` at the
            # top level. The VA detector expects ``artists[0].id``;
            # build a synthetic payload so Rule 1 fires for Discogs.
            artist_id = stripped.get("artist_id")
            artists = [{"id": artist_id}] if artist_id else []
            return {"artists": artists}
        except Exception:  # noqa: BLE001
            logger.exception(
                "discogs.get_release failed for %s", release_id,
            )
            return None

    db = PipelineDB(args.dsn)
    try:
        # Belt-and-braces against accidental concurrent writers to
        # album_requests during the backfill window. The deploy runbook
        # also stops cratedigger.service / cratedigger-importer.service /
        # cratedigger-web.service; this lock catches anything outside
        # that procedure (an operator-run pipeline-cli command, a stray
        # connection from another host, etc.). Singleton — key = 0.
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_BACKFILL, 0,
        ) as acquired:
            if not acquired:
                logger.error(
                    "Could not acquire backfill advisory lock — another "
                    "backfill process appears to be running. Aborting.",
                )
                return 5
            summary = run_backfill(
                db=db,
                field_name=args.field,
                limit=args.limit,
                dry_run=args.dry_run,
                mb_release_payload_fn=_mb_release_payload,
                mb_release_group_payload_fn=_mb_release_group_payload,
                discogs_release_payload_fn=_discogs_release_payload,
            )
    finally:
        db.close()

    for f, counters in summary.per_field.items():
        logger.info(counters.summary())
    if "is_va_compilation" in summary.per_field:
        logger.info(
            "is_va_compilation: examined=%d flipped_true=%d "
            "(canonical MB MBID=%s, Discogs id=%s)",
            summary.va_examined, summary.va_flipped_true,
            MB_VA_ARTIST_MBID, DISCOGS_VA_ARTIST_ID,
        )
    if "one_track_structural" in summary.per_field:
        logger.info(
            "one_track_structural: examined=%d categorised=%d",
            summary.one_track_examined, summary.one_track_categorised,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
