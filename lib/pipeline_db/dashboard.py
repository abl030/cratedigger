"""Pipeline dashboard metrics, cycle telemetry, peer roster counters."""
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict

from lib.cycle_counters import COUNTER_NAMES, CycleCounters, counter_values
from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import (
    CACHE_ATTRIBUTION_CYCLE_ONLY,
    DASHBOARD_WANTED_BACKLOG_STATUSES,
    DASHBOARD_WANTED_TREND_WINDOWS,
    DASHBOARD_WINDOWS,
    SearchLogOutcome,
    _float_or_none,
    _isoformat_or_none,
    _peer_hash,
    pg_execute_values,
)

#: The ``search_log.outcome`` values both dashboard search panels roll up
#: as "errors" / "problem" — attempts that failed before they could say
#: anything about the release, as opposed to ``no_match`` / ``no_results``
#: / ``exhausted``, which are real answers. Exported so the in-memory twin
#: filters on the same vocabulary instead of hand-copying it twice; the two
#: SQL sites keep their literals, and
#: ``TestSharedOutcomeVocabularies`` binds them by round-tripping the whole
#: search-outcome vocabulary through real PostgreSQL.
SEARCH_ERROR_OUTCOMES: tuple[SearchLogOutcome, ...] = (
    "timeout", "error", "empty_query",
)

#: Every ``cycle_metrics`` column one completed cycle writes, in the order
#: ``record_cycle_metrics`` supplies them. The counter columns come from
#: ``lib.cycle_counters`` rather than a second hand-kept list, so a new
#: counter is persisted the moment it is declared. ``id`` is serial and
#: the remaining column, ``created_at``, takes ``completed_at``.
_CYCLE_METRIC_COLUMNS: tuple[str, ...] = (
    "started_at", "created_at", "cycle_total_s", *COUNTER_NAMES,
    "wanted_total",
)
_INSERT_CYCLE_METRICS = f"""
    INSERT INTO cycle_metrics ({", ".join(_CYCLE_METRIC_COLUMNS)})
    VALUES ({", ".join(["%s"] * len(_CYCLE_METRIC_COLUMNS))})
    RETURNING id
"""


class UnfindableRunMetricsRow(TypedDict):
    """One ``unfindable_run_metrics`` row (#1112), raw column values.

    ``candidates_processed`` is every attempted candidate -- the six
    ``*_count`` fields partition it exactly. ``probes_attempted`` is the
    narrower subset that actually fired a Soulseek search: it excludes
    ``not_due_count`` and ``request_not_found_count``, decided before any
    probe (review round 1, F7).
    """

    id: int
    created_at: datetime
    cohort_total: int
    due_backlog_at_start: int
    batch_limit: int
    candidates_processed: int
    probes_attempted: int
    categorised_count: int
    downgraded_count: int
    no_change_count: int
    probe_failed_count: int
    not_due_count: int
    request_not_found_count: int
    breaker_tripped: bool
    duration_seconds: float


class UnfindableRunMetricsPresentation(TypedDict):
    """JSON-safe rendering of one ``UnfindableRunMetricsRow`` for the
    dashboard payload (``created_at`` isoformatted)."""

    id: int
    created_at: str | None
    cohort_total: int
    due_backlog_at_start: int
    batch_limit: int
    candidates_processed: int
    probes_attempted: int
    categorised_count: int
    downgraded_count: int
    no_change_count: int
    probe_failed_count: int
    not_due_count: int
    request_not_found_count: int
    breaker_tripped: bool
    duration_seconds: float


#: Everything below this banner is a pure post-fetch derivation the real
#: adapter and ``tests/fakes/pipeline_db/dashboard.py`` BOTH call.
#:
#: Much of the dashboard IS computed in SQL — the search/cycle windows, the
#: coverage summary and the match-rate series are all server-side
#: aggregates, and the fake reimplements those in Python because it has no
#: server. These functions are the other half: row serializers, the
#: unfindable panel, the wanted-backlog trend arithmetic, and the envelope,
#: which are already plain Python over already-fetched values on the
#: production side too. A fake that re-implemented THOSE was mirroring by
#: hand what it could simply call (issue #1278 item 7). Each adapter keeps
#: only its own fetch and timezone normalization; these shapes live here,
#: once.


def serialize_unfindable_run_row(
    row: UnfindableRunMetricsRow,
) -> UnfindableRunMetricsPresentation:
    """JSON-safe rendering of one unfindable-detection run row."""
    return UnfindableRunMetricsPresentation(
        id=row["id"],
        created_at=_isoformat_or_none(row["created_at"]),
        cohort_total=row["cohort_total"],
        due_backlog_at_start=row["due_backlog_at_start"],
        batch_limit=row["batch_limit"],
        candidates_processed=row["candidates_processed"],
        probes_attempted=row["probes_attempted"],
        categorised_count=row["categorised_count"],
        downgraded_count=row["downgraded_count"],
        no_change_count=row["no_change_count"],
        probe_failed_count=row["probe_failed_count"],
        not_due_count=row["not_due_count"],
        request_not_found_count=row["request_not_found_count"],
        breaker_tripped=row["breaker_tripped"],
        duration_seconds=row["duration_seconds"],
    )


def unfindable_panel(
    rows: list[UnfindableRunMetricsPresentation],
) -> dict[str, list[UnfindableRunMetricsPresentation] | dict[str, object]]:
    """Recent unfindable-detection run health + backlog trend (#1112).

    ``rows`` arrives newest-first. ``recent_runs`` gives latest-run-first
    operator facts (when, attempted, per-outcome counts, breaker).
    ``backlog_trend`` is the due-backlog/probes series across those same
    runs, shaped like the ``wanted_trend_panel`` idiom (a ``current_*``
    snapshot, a ``latest_sample_at``, and a chronological ``series``) but
    at the daily run grain rather than a live-computed window/ETA — the
    detection job fires once a day, so there is no "last 6h" signal to
    bucket.
    """
    chronological = list(reversed(rows))
    series: list[dict[str, object]] = [
        {
            "sampled_at": r["created_at"],
            "due_backlog_at_start": r["due_backlog_at_start"],
            # candidates_processed, not probes_attempted (review round
            # 2, R nit): the truer per-run capacity number -- every
            # candidate the batch actually touched, not just the
            # subset that fired a probe.
            "candidates_processed": r["candidates_processed"],
        }
        for r in chronological
    ]
    latest = rows[0] if rows else None
    return {
        "recent_runs": rows,
        "backlog_trend": {
            "current_backlog": (
                latest["due_backlog_at_start"] if latest else None
            ),
            "latest_sample_at": latest["created_at"] if latest else None,
            "series": series,
        },
    }


def serialize_dashboard_heavy_query_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """One peer-browse heavy-query row, JSON-safe."""
    return {
        "search_log_id": int(row["search_log_id"]),
        "request_id": int(row["request_id"]),
        "mb_release_id": row.get("mb_release_id"),
        "artist_name": row.get("artist_name"),
        "album_title": row.get("album_title"),
        "status": row.get("status"),
        "created_at": _isoformat_or_none(row.get("created_at")),
        "query": row.get("query"),
        "variant": row.get("variant"),
        "outcome": row.get("outcome"),
        "result_count": int(row.get("result_count") or 0),
        "elapsed_s": _float_or_none(row.get("elapsed_s")),
        "browse_time_s": float(row.get("browse_time_s") or 0.0),
        "match_time_s": float(row.get("match_time_s") or 0.0),
        "peers_browsed": int(row.get("peers_browsed") or 0),
        "peers_browsed_lazy": int(row.get("peers_browsed_lazy") or 0),
        "peer_dirs": int(row.get("peer_dirs") or 0),
        "fanout_waves": int(row.get("fanout_waves") or 0),
    }


def serialize_dashboard_cycle_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """One ``cycle_metrics`` row, JSON-safe.

    ``cycle_searches_watchdog_killed`` is renamed to ``watchdog_kills``,
    the cache hit/miss and ``wanted_total`` columns are deliberately NOT
    emitted, and both timestamps are isoformatted.
    """
    return {
        "id": int(row["id"]),
        "started_at": _isoformat_or_none(row.get("started_at")),
        "created_at": _isoformat_or_none(row.get("created_at")),
        "cycle_total_s": float(row["cycle_total_s"]),
        "browse_time_s": float(row["browse_time_s"]),
        "match_time_s": float(row["match_time_s"]),
        "search_time_s": float(row["search_time_s"]),
        "watchdog_kills": int(row["cycle_searches_watchdog_killed"]),
        "find_download_queued": int(row["find_download_queued"]),
        "find_download_completed": int(row["find_download_completed"]),
        "find_download_drain_time_s": float(row["find_download_drain_time_s"]),
        "cache_errors": int(row["cache_errors"]),
        "cache_write_errors": int(row["cache_write_errors"]),
        "cache_fuse_tripped": int(row["cache_fuse_tripped"]),
        "peers_browsed": int(row["peers_browsed"]),
        "peers_browsed_lazy": int(row["peers_browsed_lazy"]),
        "fanout_waves": int(row["fanout_waves"]),
    }


def serialize_dashboard_request_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """One backlog request row for the loop-suspect / stale-wanted panels."""
    return {
        "request_id": int(row["request_id"]),
        "artist_name": row["artist_name"],
        "album_title": row["album_title"],
        "status": row["status"],
        "last_search_at": _isoformat_or_none(row.get("last_search_at")),
        "searches_24h": int(row.get("searches_24h") or 0),
        "searches_6h": int(row.get("searches_6h") or 0),
        "found_24h": int(row.get("found_24h") or 0),
        "no_match_24h": int(row.get("no_match_24h") or 0),
        "no_results_24h": int(row.get("no_results_24h") or 0),
        "reset_24h": int(row.get("reset_24h") or 0),
        "problem_24h": int(row.get("problem_24h") or 0),
    }


def wanted_trend_window(
    label: str,
    hours: int,
    *,
    samples: list[tuple[datetime, int]],
    now: datetime,
    current_wanted: int,
) -> dict[str, object]:
    """Delta / drain-rate / ETA for one wanted-backlog trend window."""
    window_start = now - timedelta(hours=hours)
    window_samples = [(at, wanted) for at, wanted in samples if at >= window_start]
    if not window_samples:
        return {
            "label": label,
            "hours": hours,
            "sample_count": 0,
            "start_sample_at": None,
            "end_sample_at": now.isoformat(),
            "start_wanted": None,
            "end_wanted": current_wanted,
            "delta": None,
            "delta_per_hour": None,
            "drain_per_hour": None,
            "eta_hours": None,
            "trend": "unknown",
        }

    start_at, start_wanted = window_samples[0]
    elapsed_hours = (now - start_at).total_seconds() / 3600
    delta = current_wanted - start_wanted
    if elapsed_hours <= 0:
        delta_per_hour = None
        drain_per_hour = None
        eta_hours = None
        trend = "unknown"
    else:
        delta_per_hour = delta / elapsed_hours
        drain_per_hour = max(-delta_per_hour, 0.0)
        eta_hours = (
            current_wanted / drain_per_hour
            if drain_per_hour > 0 and current_wanted > 0
            else None
        )
        trend = "down" if delta < 0 else "up" if delta > 0 else "flat"

    return {
        "label": label,
        "hours": hours,
        "sample_count": len(window_samples),
        "start_sample_at": start_at.isoformat(),
        "end_sample_at": now.isoformat(),
        "start_wanted": start_wanted,
        "end_wanted": current_wanted,
        "delta": delta,
        "delta_per_hour": delta_per_hour,
        "drain_per_hour": drain_per_hour,
        "eta_hours": eta_hours,
        "trend": trend,
    }


def wanted_trend_panel(
    samples: list[tuple[datetime, int]],
    *,
    current_wanted: int,
    now: datetime,
) -> dict[str, Any]:
    """The wanted-backlog trend panel over ``samples``.

    ``samples`` is the ``(created_at, wanted_total)`` series from the last
    7 days of ``cycle_metrics``, chronological and already normalized to
    UTC by the caller — each adapter owns its own fetch and timezone
    handling; the arithmetic lives here.
    """
    series_24h: list[dict[str, Any]] = [
        {"sampled_at": sample_at.isoformat(), "wanted_total": wanted}
        for sample_at, wanted in samples
        if sample_at >= now - timedelta(hours=24)
    ]
    series_24h.append({
        "sampled_at": now.isoformat(),
        "wanted_total": current_wanted,
        "synthetic": True,
    })
    return {
        "current_wanted": current_wanted,
        "latest_sample_at": samples[-1][0].isoformat() if samples else None,
        "series_24h": series_24h,
        "windows": [
            wanted_trend_window(
                label,
                hours,
                samples=samples,
                now=now,
                current_wanted=current_wanted,
            )
            for label, hours in DASHBOARD_WANTED_TREND_WINDOWS
        ],
    }


def dashboard_envelope(
    *,
    generated_at: datetime,
    search_windows: Sequence[Mapping[str, object]],
    cycle_windows: Sequence[Mapping[str, object]],
    recent_cycles: Sequence[Mapping[str, object]],
    outlier_cycles: Sequence[Mapping[str, object]],
    coverage: Mapping[str, object],
    peers: Mapping[str, object],
    plan_readiness: Mapping[str, object],
    unfindable: Mapping[str, object],
) -> dict[str, object]:
    """Assemble the Pipeline dashboard payload from its computed panels."""
    return {
        "generated_at": generated_at.isoformat(),
        "searches": {"windows": search_windows},
        "cycles": {
            "windows": cycle_windows,
            "recent": recent_cycles,
            "outliers": outlier_cycles,
        },
        "coverage": coverage,
        "peers": peers,
        "plan_readiness": plan_readiness,
        "unfindable": unfindable,
    }


class _DashboardMixin(_PipelineDBBase):
    """Pipeline dashboard metrics, cycle telemetry, peer roster counters."""


    # -- Pipeline dashboard telemetry ----------------------------------------

    def record_cycle_metrics(
        self,
        *,
        cycle_total_s: float,
        counters: CycleCounters | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        wanted_total: int | None = None,
    ) -> int:
        """Persist one completed cratedigger cycle's runtime counters.

        ``counters`` defaults to an all-zero value so a caller that only
        cares about cycle duration says so by omission, exactly as the
        sixteen zero-defaulted keyword arguments this replaced allowed.
        ``completed_at`` lands in the ``created_at`` column: the row is
        written when the cycle ends, and that column has been the cycle's
        completion stamp since migration 011.
        """
        completed = completed_at or datetime.now(UTC)
        wanted_snapshot = (
            self._current_wanted_total() if wanted_total is None
            else max(0, int(wanted_total))
        )
        cur = self._execute(_INSERT_CYCLE_METRICS, (
            started_at, completed, cycle_total_s,
            *counter_values(counters or CycleCounters()),
            wanted_snapshot,
        ))
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return int(row["id"])


    def _current_wanted_total(self) -> int:
        cur = self._execute("""
            SELECT COUNT(*)::int AS wanted_total
            FROM album_requests
            WHERE status = ANY(%s)
        """, (list(DASHBOARD_WANTED_BACKLOG_STATUSES),))
        row = cur.fetchone()
        return int((row.get("wanted_total") if row else None) or 0)


    # -- Unfindable-detection run telemetry (#1112) --------------------------

    def record_unfindable_run_metrics(
        self,
        *,
        cohort_total: int,
        due_backlog_at_start: int,
        batch_limit: int,
        candidates_processed: int,
        probes_attempted: int,
        breaker_tripped: bool,
        duration_seconds: float,
        categorised_count: int = 0,
        downgraded_count: int = 0,
        no_change_count: int = 0,
        probe_failed_count: int = 0,
        not_due_count: int = 0,
        request_not_found_count: int = 0,
    ) -> int:
        """Persist one completed ``cratedigger-unfindable.service`` run.

        Mirrors ``record_cycle_metrics``: one compact row per run so the
        dashboard can show run health without scraping journal logs.
        Written for both a fully classified run AND a breaker-tripped
        (incomplete) run -- a failed/partial run is exactly the signal an
        operator needs to see (issue #1112). NOT written for a run that
        aborts before any probe work starts (missing slskd config, or a
        behind/missing DB schema) -- there is no cohort/backlog reading to
        report yet, and a behind schema may not even have this table.

        The six ``*_count`` fields are one column per
        ``lib.unfindable_detection_service.RESULT_*`` outcome constant --
        the exact taxonomy ``categorise_due_batch`` can return for an
        attempted candidate -- and partition ``candidates_processed``
        exactly. ``probes_attempted`` is the narrower subset that
        actually fired a Soulseek search (review round 1, F7): the
        caller derives it as
        ``candidates_processed - not_due_count - request_not_found_count``.
        """
        cur = self._execute("""
            INSERT INTO unfindable_run_metrics (
                cohort_total, due_backlog_at_start, batch_limit,
                candidates_processed, probes_attempted, categorised_count,
                downgraded_count, no_change_count, probe_failed_count,
                not_due_count, request_not_found_count, breaker_tripped,
                duration_seconds
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            cohort_total, due_backlog_at_start, batch_limit,
            candidates_processed, probes_attempted, categorised_count,
            downgraded_count, no_change_count, probe_failed_count,
            not_due_count, request_not_found_count, breaker_tripped,
            duration_seconds,
        ))
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return int(row["id"])


    def get_unfindable_run_metrics(
        self, *, limit: int = 30,
    ) -> list[UnfindableRunMetricsRow]:
        """Return the most recent unfindable-detection run rows, newest
        first. Raw column values (real ``datetime``/``bool``) -- the
        dashboard's ``_dashboard_unfindable`` serializes these to the
        JSON-safe envelope; this is the Rule A round-trip surface."""
        cur = self._execute("""
            SELECT id, created_at, cohort_total, due_backlog_at_start,
                   batch_limit, candidates_processed, probes_attempted,
                   categorised_count, downgraded_count, no_change_count,
                   probe_failed_count, not_due_count,
                   request_not_found_count, breaker_tripped,
                   duration_seconds
            FROM unfindable_run_metrics
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """, (int(limit),))
        return [
            UnfindableRunMetricsRow(
                id=int(r["id"]),
                created_at=r["created_at"],
                cohort_total=int(r["cohort_total"]),
                due_backlog_at_start=int(r["due_backlog_at_start"]),
                batch_limit=int(r["batch_limit"]),
                candidates_processed=int(r["candidates_processed"]),
                probes_attempted=int(r["probes_attempted"]),
                categorised_count=int(r["categorised_count"]),
                downgraded_count=int(r["downgraded_count"]),
                no_change_count=int(r["no_change_count"]),
                probe_failed_count=int(r["probe_failed_count"]),
                not_due_count=int(r["not_due_count"]),
                request_not_found_count=int(r["request_not_found_count"]),
                breaker_tripped=bool(r["breaker_tripped"]),
                duration_seconds=float(r["duration_seconds"]),
            )
            for r in cur.fetchall()
        ]


    def _dashboard_unfindable(
        self,
    ) -> dict[str, list[UnfindableRunMetricsPresentation] | dict[str, object]]:
        """Recent unfindable-detection run health + backlog trend (#1112)."""
        return unfindable_panel([
            serialize_unfindable_run_row(r)
            for r in self.get_unfindable_run_metrics(limit=14)
        ])


    def record_peer_observations(
        self,
        usernames: Iterable[str],
        *,
        observed_at: datetime | None = None,
    ) -> int:
        """Persist hashed peer observations and return the new-peer count.

        Each username represents a peer whose share we cold-browsed this
        cycle. Raw usernames are never stored — only the stable hash —
        and the roster keeps exactly one row per distinct peer ever seen.
        """
        unique = sorted({str(u) for u in usernames if u})
        if not unique:
            return 0

        observed = observed_at or datetime.now(UTC)
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)

        hashes = [_peer_hash(username) for username in unique]
        existing_cur = self._execute(
            """
            SELECT username_hash
            FROM peer_observations
            WHERE username_hash = ANY(%s)
            """,
            (hashes,),
        )
        existing = {row["username_hash"] for row in existing_cur.fetchall()}

        self._ensure_conn()
        with self.conn.cursor() as cur:
            pg_execute_values(
                cur,
                """
                INSERT INTO peer_observations (
                    username_hash, first_seen_at, last_seen_at
                )
                VALUES %s
                ON CONFLICT (username_hash) DO UPDATE
                SET last_seen_at = GREATEST(
                    peer_observations.last_seen_at,
                    EXCLUDED.last_seen_at
                )
                """,
                [(h, observed, observed) for h in hashes],
            )
        self.conn.commit()
        return len(set(hashes) - existing)


    def get_peer_metrics(self, days: int = 14) -> dict[str, Any]:
        """Return distinct-peer roster metrics for the dashboard.

        The roster holds one row per distinct peer ever seen (~40K rows
        as of 2026-06), so everything is computed live: lifetime totals
        in one pass plus a Perth-local per-day growth curve whose
        ``total_peers`` column is the cumulative roster size at the end
        of each day (carried forward across days with no new peers).
        """
        clamped_days = max(1, min(int(days), 90))

        totals_cur = self._execute("""
            SELECT
                COUNT(*)::int AS known_peers,
                COUNT(*) FILTER (
                    WHERE first_seen_at >= NOW() - INTERVAL '24 hours'
                )::int AS new_24h,
                COUNT(*) FILTER (
                    WHERE last_seen_at >= NOW() - INTERVAL '24 hours'
                )::int AS seen_24h,
                MIN(first_seen_at) AS tracked_since
            FROM peer_observations
        """)
        totals_row = totals_cur.fetchone()

        days_cur = self._execute(
            """
            WITH per_day AS (
                SELECT
                    (first_seen_at AT TIME ZONE 'Australia/Perth')::date
                        AS day,
                    COUNT(*)::int AS new_peers
                FROM peer_observations
                GROUP BY 1
            ),
            cumulative AS (
                SELECT
                    day,
                    new_peers,
                    SUM(new_peers) OVER (ORDER BY day)::int AS total_peers
                FROM per_day
            ),
            day_series AS (
                SELECT generate_series(
                    (NOW() AT TIME ZONE 'Australia/Perth')::date
                        - (%s - 1),
                    (NOW() AT TIME ZONE 'Australia/Perth')::date,
                    INTERVAL '1 day'
                )::date AS day
            )
            SELECT
                ds.day,
                COALESCE(c.new_peers, 0)::int AS new_peers,
                COALESCE(
                    (
                        SELECT MAX(c2.total_peers)
                        FROM cumulative c2
                        WHERE c2.day <= ds.day
                    ),
                    0
                )::int AS total_peers
            FROM day_series ds
            LEFT JOIN cumulative c ON c.day = ds.day
            ORDER BY ds.day DESC
            """,
            (clamped_days,),
        )
        day_dicts = [
            {
                "date": row["day"].isoformat(),
                "new_peers": int(row["new_peers"]),
                "total_peers": int(row["total_peers"]),
            }
            for row in days_cur.fetchall()
        ]

        return {
            "days": day_dicts,
            "totals": {
                "known_peers": int(
                    (totals_row.get("known_peers") if totals_row else None) or 0
                ),
                "new_24h": int(
                    (totals_row.get("new_24h") if totals_row else None) or 0
                ),
                "seen_24h": int(
                    (totals_row.get("seen_24h") if totals_row else None) or 0
                ),
                "tracked_since": _isoformat_or_none(
                    totals_row.get("tracked_since") if totals_row else None
                ),
            },
        }


    def get_pipeline_dashboard_metrics(
        self,
        *,
        plan_generator_id: str | None = None,
    ) -> dict[str, Any]:
        """Return DB-derived metrics for the Pipeline dashboard.

        Redis status is owned by the web cache layer; this method intentionally
        covers only persisted Postgres state: searches, cycles, and active
        request coverage.

        ``plan_generator_id`` selects the search-plan generator id used to
        bucket wanted rows in the plan-readiness panel. Defaults to
        ``lib.search.SEARCH_PLAN_GENERATOR_ID`` so the dashboard tracks
        whatever the running pipeline considers current. Tests can pin a
        different id without monkey-patching the constant.
        """
        if plan_generator_id is None:
            from lib.search import SEARCH_PLAN_GENERATOR_ID
            plan_generator_id = SEARCH_PLAN_GENERATOR_ID
        peers = self.get_peer_metrics()
        peers["heavy_queries"] = self._dashboard_peer_browse_heavy_queries()
        peers["heavy_query_hours"] = 24
        return dashboard_envelope(
            generated_at=datetime.now(UTC),
            search_windows=[self._dashboard_search_window(label, hours)
                            for label, hours in DASHBOARD_WINDOWS],
            cycle_windows=[self._dashboard_cycle_window(label, hours)
                           for label, hours in DASHBOARD_WINDOWS],
            recent_cycles=self._dashboard_cycle_rows(outliers=False, limit=12),
            outlier_cycles=self._dashboard_cycle_rows(outliers=True, limit=8),
            coverage=self._dashboard_coverage(),
            peers=peers,
            plan_readiness=self.get_search_plan_readiness(plan_generator_id),
            unfindable=self._dashboard_unfindable(),
        )


    def _dashboard_peer_browse_heavy_queries(
        self,
        *,
        hours: int = 24,
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        """Return recent search rows that generated the most peer/dir work."""
        clamped_hours = max(1, min(int(hours), 168))
        clamped_limit = max(1, min(int(limit), 50))
        cur = self._execute("""
            SELECT
                sl.id AS search_log_id,
                sl.request_id,
                ar.mb_release_id,
                ar.artist_name,
                ar.album_title,
                ar.status,
                sl.created_at,
                sl.query,
                sl.variant,
                sl.outcome,
                sl.result_count,
                sl.elapsed_s,
                sl.browse_time_s,
                sl.match_time_s,
                sl.peers_browsed,
                sl.peers_browsed_lazy,
                sl.fanout_waves,
                (sl.peers_browsed + sl.peers_browsed_lazy)::int AS peer_dirs
            FROM search_log sl
            JOIN album_requests ar ON ar.id = sl.request_id
            WHERE sl.created_at >= NOW() - %s::interval
              AND (sl.peers_browsed + sl.peers_browsed_lazy) > 0
            ORDER BY
                (sl.peers_browsed + sl.peers_browsed_lazy) DESC,
                sl.fanout_waves DESC,
                sl.created_at DESC,
                sl.id DESC
            LIMIT %s
        """, (f"{clamped_hours} hours", clamped_limit))
        return [
            serialize_dashboard_heavy_query_row(dict(row))
            for row in cur.fetchall()
        ]


    def _dashboard_search_window(self, label: str, hours: int) -> dict[str, Any]:
        # ``exhausted`` is HISTORICAL ONLY after the persisted-search-plans
        # cutover -- new code never writes ``outcome='exhausted'`` rows.
        # ``cursor_wraps`` is the plan-driven equivalent: a search-log row
        # with ``cursor_update_status='wrapped'`` is what increments
        # ``plan_cycle_count``. Together the two fields let dashboards
        # diff "old reset signal" vs "new wrap signal" during the rollout.
        cur = self._execute("""
            SELECT
                COUNT(*)::int AS searches,
                COUNT(DISTINCT request_id)::int AS distinct_requests,
                AVG(elapsed_s)::double precision AS avg_elapsed_s,
                (percentile_cont(0.5) WITHIN GROUP (ORDER BY elapsed_s)
                    FILTER (WHERE elapsed_s IS NOT NULL))::double precision AS median_elapsed_s,
                (percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_s)
                    FILTER (WHERE elapsed_s IS NOT NULL))::double precision AS p95_elapsed_s,
                MAX(elapsed_s)::double precision AS max_elapsed_s,
                COUNT(*) FILTER (WHERE outcome = 'found')::int AS found,
                COUNT(*) FILTER (WHERE outcome = 'no_match')::int AS no_match,
                COUNT(*) FILTER (WHERE outcome = 'no_results')::int AS no_results,
                COUNT(*) FILTER (WHERE outcome = 'exhausted')::int AS exhausted,
                COUNT(*) FILTER (WHERE outcome IN ('timeout', 'error', 'empty_query'))::int AS errors,
                COUNT(*) FILTER (WHERE cursor_update_status = 'wrapped')::int AS cursor_wraps,
                COUNT(*) FILTER (WHERE cursor_update_status = 'stale')::int AS stale_completions,
                COUNT(*) FILTER (WHERE attempt_consumed = false)::int AS non_consuming
            FROM search_log
            WHERE created_at >= NOW() - %s::interval
        """, (f"{hours} hours",))
        row = cur.fetchone()
        def _get(key: str) -> int | float | None:
            return row.get(key) if row else None
        searches = int(_get("searches") or 0)
        return {
            "label": label,
            "hours": hours,
            "searches": searches,
            "distinct_requests": int(_get("distinct_requests") or 0),
            "searches_per_hour": searches / hours if hours else 0,
            "searches_per_24h": (searches / hours * 24) if hours else 0,
            "avg_elapsed_s": _float_or_none(_get("avg_elapsed_s")),
            "median_elapsed_s": _float_or_none(_get("median_elapsed_s")),
            "p95_elapsed_s": _float_or_none(_get("p95_elapsed_s")),
            "max_elapsed_s": _float_or_none(_get("max_elapsed_s")),
            "outcomes": {
                "found": int(_get("found") or 0),
                "no_match": int(_get("no_match") or 0),
                "no_results": int(_get("no_results") or 0),
                # Historical only -- preserved so legacy rows still render
                # in their existing position. Any non-zero count for rows
                # newer than the persisted-search-plans deploy timestamp is
                # a regression; see docs/persisted-search-plans-rollout.md.
                "exhausted": int(_get("exhausted") or 0),
                "errors": int(_get("errors") or 0),
            },
            # Plan-driven cycle metrics. ``cursor_wraps`` replaces the
            # ``exhausted`` reset signal: it is one-per-cycle per request
            # and increments ``plan_cycle_count``. ``stale_completions``
            # are post-regeneration log-only rows. ``non_consuming`` are
            # pre-attempt setup failures that did not advance the cursor.
            "cursor_wraps": int(_get("cursor_wraps") or 0),
            "stale_completions": int(_get("stale_completions") or 0),
            "non_consuming": int(_get("non_consuming") or 0),
            # Cache attribution honesty: surface that ``search_log`` has
            # no per-search cache columns today; only cycle-level counters
            # exist. See ``CACHE_ATTRIBUTION_CYCLE_ONLY``.
            "cache_attribution_level": CACHE_ATTRIBUTION_CYCLE_ONLY,
        }


    def _dashboard_cycle_window(self, label: str, hours: int) -> dict[str, Any]:
        cur = self._execute("""
            SELECT
                COUNT(*)::int AS cycles,
                AVG(cycle_total_s)::double precision AS avg_cycle_s,
                (percentile_cont(0.5) WITHIN GROUP (ORDER BY cycle_total_s)
                    FILTER (WHERE cycle_total_s IS NOT NULL))::double precision AS median_cycle_s,
                (percentile_cont(0.95) WITHIN GROUP (ORDER BY cycle_total_s)
                    FILTER (WHERE cycle_total_s IS NOT NULL))::double precision AS p95_cycle_s,
                MAX(cycle_total_s)::double precision AS max_cycle_s,
                (percentile_cont(0.5) WITHIN GROUP (ORDER BY search_time_s)
                    FILTER (WHERE search_time_s IS NOT NULL))::double precision AS median_search_s,
                SUM(cycle_searches_watchdog_killed)::int AS watchdog_kills,
                SUM(find_download_queued)::int AS find_download_queued,
                SUM(find_download_completed)::int AS find_download_completed,
                SUM(cache_errors)::int AS cache_errors,
                SUM(cache_write_errors)::int AS cache_write_errors,
                SUM(cache_fuse_tripped)::int AS cache_fuse_tripped,
                SUM(peers_browsed)::int AS peers_browsed,
                SUM(peers_browsed_lazy)::int AS peers_browsed_lazy,
                SUM(fanout_waves)::int AS fanout_waves
            FROM cycle_metrics
            WHERE created_at >= NOW() - %s::interval
        """, (f"{hours} hours",))
        row = cur.fetchone()
        def _get(key: str) -> int | float | None:
            return row.get(key) if row else None
        return {
            "label": label,
            "hours": hours,
            "cycles": int(_get("cycles") or 0),
            "avg_cycle_s": _float_or_none(_get("avg_cycle_s")),
            "median_cycle_s": _float_or_none(_get("median_cycle_s")),
            "p95_cycle_s": _float_or_none(_get("p95_cycle_s")),
            "max_cycle_s": _float_or_none(_get("max_cycle_s")),
            "median_search_s": _float_or_none(_get("median_search_s")),
            "watchdog_kills": int(_get("watchdog_kills") or 0),
            "find_download_queued": int(_get("find_download_queued") or 0),
            "find_download_completed": int(_get("find_download_completed") or 0),
            "cache_errors": int(_get("cache_errors") or 0),
            "cache_write_errors": int(_get("cache_write_errors") or 0),
            "cache_fuse_tripped": int(_get("cache_fuse_tripped") or 0),
            "peers_browsed": int(_get("peers_browsed") or 0),
            "peers_browsed_lazy": int(_get("peers_browsed_lazy") or 0),
            "fanout_waves": int(_get("fanout_waves") or 0),
        }


    def _dashboard_cycle_rows(
        self,
        *,
        outliers: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        cur = self._execute("""
            SELECT
                id, started_at, created_at, cycle_total_s, browse_time_s,
                match_time_s, search_time_s, cycle_searches_watchdog_killed,
                find_download_queued, find_download_completed,
                find_download_drain_time_s, cache_errors, cache_write_errors,
                cache_fuse_tripped, peers_browsed, peers_browsed_lazy,
                fanout_waves
            FROM cycle_metrics
            WHERE NOT %s OR created_at >= NOW() - INTERVAL '24 hours'
            ORDER BY
                CASE WHEN %s THEN cycle_total_s END DESC,
                CASE WHEN NOT %s THEN created_at END DESC,
                id DESC
            LIMIT %s
        """, (outliers, outliers, outliers, limit))
        return [serialize_dashboard_cycle_row(dict(row))
                for row in cur.fetchall()]


    def _dashboard_coverage(self) -> dict[str, Any]:
        summary = self._dashboard_coverage_summary()
        top_suspects = self._dashboard_loop_suspects()
        active_searches_24h = int(summary.get("active_wanted_searches_24h") or 0)
        top_10_searches = sum(int(r["searches_24h"]) for r in top_suspects[:10])
        top_10_share = (
            top_10_searches / active_searches_24h if active_searches_24h else 0
        )
        return {
            **summary,
            "wanted_trend": self._dashboard_wanted_trend(
                int(summary.get("wanted_total") or 0),
            ),
            "match_rate_series_24h": self._dashboard_match_rate_series(24),
            "match_rate_series_28d": self._dashboard_daily_match_rate_series(28),
            "top_10_share_24h": top_10_share,
            "top_loop_suspects": top_suspects,
            "stale_wanted": self._dashboard_stale_wanted(),
        }


    def _dashboard_wanted_trend(self, current_wanted: int) -> dict[str, Any]:
        cur = self._execute("""
            SELECT created_at, wanted_total
            FROM cycle_metrics
            WHERE wanted_total IS NOT NULL
              AND created_at >= NOW() - INTERVAL '7 days'
            ORDER BY created_at ASC, id ASC
        """)
        samples: list[tuple[datetime, int]] = []
        for row in cur.fetchall():
            created_at = row.get("created_at")
            if not isinstance(created_at, datetime):
                continue
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            else:
                created_at = created_at.astimezone(UTC)
            samples.append((created_at, int(row.get("wanted_total") or 0)))
        return wanted_trend_panel(
            samples, current_wanted=current_wanted, now=datetime.now(UTC))


    def _dashboard_match_rate_series(self, hours: int) -> list[dict[str, Any]]:
        clamped_hours = max(1, min(int(hours), 168))
        cur = self._execute("""
            WITH buckets AS (
                SELECT generate_series(
                    date_trunc('hour', NOW())
                        - ((%s::int - 1) * INTERVAL '1 hour'),
                    date_trunc('hour', NOW()),
                    INTERVAL '1 hour'
                ) AS bucket_start
            ),
            found AS (
                SELECT
                    date_trunc('hour', created_at) AS bucket_start,
                    COUNT(*)::int AS matches
                FROM search_log
                WHERE outcome = 'found'
                  AND created_at >= date_trunc('hour', NOW())
                    - ((%s::int - 1) * INTERVAL '1 hour')
                GROUP BY 1
            )
            SELECT
                buckets.bucket_start,
                COALESCE(found.matches, 0)::int AS matches
            FROM buckets
            LEFT JOIN found ON found.bucket_start = buckets.bucket_start
            ORDER BY buckets.bucket_start
        """, (clamped_hours, clamped_hours))
        return [
            {
                "bucket_start": _isoformat_or_none(row["bucket_start"]),
                "matches": int(row["matches"] or 0),
                "matches_per_hour": int(row["matches"] or 0),
            }
            for row in cur.fetchall()
        ]


    def _dashboard_daily_match_rate_series(self, days: int) -> list[dict[str, Any]]:
        clamped_days = max(1, min(int(days), 90))
        cur = self._execute("""
            WITH buckets AS (
                SELECT generate_series(
                    date_trunc('day', NOW())
                        - ((%s::int - 1) * INTERVAL '1 day'),
                    date_trunc('day', NOW()),
                    INTERVAL '1 day'
                ) AS bucket_start
            ),
            found AS (
                SELECT
                    date_trunc('day', created_at) AS bucket_start,
                    COUNT(*)::int AS matches
                FROM search_log
                WHERE outcome = 'found'
                  AND created_at >= date_trunc('day', NOW())
                    - ((%s::int - 1) * INTERVAL '1 day')
                GROUP BY 1
            )
            SELECT
                buckets.bucket_start,
                COALESCE(found.matches, 0)::int AS matches
            FROM buckets
            LEFT JOIN found ON found.bucket_start = buckets.bucket_start
            ORDER BY buckets.bucket_start
        """, (clamped_days, clamped_days))
        return [
            {
                "bucket_start": _isoformat_or_none(row["bucket_start"]),
                "matches": int(row["matches"] or 0),
                "matches_per_day": int(row["matches"] or 0),
            }
            for row in cur.fetchall()
        ]


    def _dashboard_coverage_summary(self) -> dict[str, Any]:
        cur = self._execute("""
            WITH wanted AS (
                SELECT id
                FROM album_requests
                WHERE status = ANY(%s)
            ),
            per_request AS (
                SELECT
                    request_id,
                    MAX(created_at) AS last_search_at,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS searches_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS searches_6h
                FROM search_log
                GROUP BY request_id
            ),
            match_rates AS (
                SELECT
                    COUNT(*) FILTER (
                        WHERE outcome = 'found'
                          AND created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS matches_24h,
                    COUNT(*) FILTER (
                        WHERE outcome = 'found'
                          AND created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS matches_6h
                FROM search_log
            )
            SELECT
                COUNT(*)::int AS wanted_total,
                COUNT(*) FILTER (
                    WHERE pr.last_search_at >= NOW() - INTERVAL '24 hours'
                )::int AS wanted_searched_24h,
                COUNT(*) FILTER (
                    WHERE pr.last_search_at >= NOW() - INTERVAL '6 hours'
                )::int AS wanted_searched_6h,
                COUNT(*) FILTER (WHERE pr.last_search_at IS NULL)::int
                    AS wanted_never_searched,
                COALESCE(SUM(pr.searches_24h), 0)::int
                    AS active_wanted_searches_24h,
                COALESCE(SUM(pr.searches_6h), 0)::int
                    AS active_wanted_searches_6h,
                MIN(pr.last_search_at) FILTER (WHERE pr.last_search_at IS NOT NULL)
                    AS oldest_last_search_at,
                COALESCE(MAX(match_rates.matches_24h), 0)::int AS matches_24h,
                COALESCE(MAX(match_rates.matches_6h), 0)::int AS matches_6h
            FROM wanted w
            LEFT JOIN per_request pr ON pr.request_id = w.id
            CROSS JOIN match_rates
        """, (list(DASHBOARD_WANTED_BACKLOG_STATUSES),))
        row = cur.fetchone()
        def _get(key: str) -> int | float | None:
            return row.get(key) if row else None
        oldest_last_search_at = row.get("oldest_last_search_at") if row else None
        wanted_total = int(_get("wanted_total") or 0)
        searched_24h = int(_get("wanted_searched_24h") or 0)
        searched_6h = int(_get("wanted_searched_6h") or 0)
        matches_24h = int(_get("matches_24h") or 0)
        matches_6h = int(_get("matches_6h") or 0)
        return {
            "wanted_total": wanted_total,
            "wanted_searched_24h": searched_24h,
            "wanted_searched_6h": searched_6h,
            "wanted_unsearched_24h": max(wanted_total - searched_24h, 0),
            "wanted_unsearched_6h": max(wanted_total - searched_6h, 0),
            "wanted_never_searched": int(_get("wanted_never_searched") or 0),
            "active_wanted_searches_24h": int(
                _get("active_wanted_searches_24h") or 0
            ),
            "active_wanted_searches_6h": int(
                _get("active_wanted_searches_6h") or 0
            ),
            "oldest_last_search_at": _isoformat_or_none(
                oldest_last_search_at
            ),
            "matches_24h": matches_24h,
            "matches_6h": matches_6h,
            "matches_per_hour_24h": matches_24h / 24,
            "matches_per_hour_6h": matches_6h / 6,
        }


    def _dashboard_loop_suspects(self) -> list[dict[str, Any]]:
        cur = self._execute("""
            WITH wanted AS (
                SELECT id, artist_name, album_title, status
                FROM album_requests
                WHERE status = ANY(%s)
            ),
            per_request AS (
                SELECT
                    request_id,
                    MAX(created_at) AS last_search_at,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS searches_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS searches_6h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome = 'found'
                    )::int AS found_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome = 'no_match'
                    )::int AS no_match_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome = 'no_results'
                    )::int AS no_results_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome = 'exhausted'
                    )::int AS reset_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                          AND outcome IN ('timeout', 'error', 'empty_query')
                    )::int AS problem_24h
                FROM search_log
                GROUP BY request_id
            )
            SELECT
                w.id AS request_id, w.artist_name, w.album_title, w.status,
                pr.last_search_at,
                COALESCE(pr.searches_24h, 0)::int AS searches_24h,
                COALESCE(pr.searches_6h, 0)::int AS searches_6h,
                COALESCE(pr.found_24h, 0)::int AS found_24h,
                COALESCE(pr.no_match_24h, 0)::int AS no_match_24h,
                COALESCE(pr.no_results_24h, 0)::int AS no_results_24h,
                COALESCE(pr.reset_24h, 0)::int AS reset_24h,
                COALESCE(pr.problem_24h, 0)::int AS problem_24h
            FROM wanted w
            JOIN per_request pr ON pr.request_id = w.id
            WHERE COALESCE(pr.searches_24h, 0) > 0
            ORDER BY pr.searches_24h DESC, pr.searches_6h DESC, w.id ASC
            LIMIT 12
        """, (list(DASHBOARD_WANTED_BACKLOG_STATUSES),))
        return [serialize_dashboard_request_row(dict(row))
                for row in cur.fetchall()]


    def _dashboard_stale_wanted(self) -> list[dict[str, Any]]:
        cur = self._execute("""
            WITH wanted AS (
                SELECT id, artist_name, album_title, status, created_at
                FROM album_requests
                WHERE status = ANY(%s)
            ),
            per_request AS (
                SELECT
                    request_id,
                    MAX(created_at) AS last_search_at,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                    )::int AS searches_24h,
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '6 hours'
                    )::int AS searches_6h
                FROM search_log
                GROUP BY request_id
            )
            SELECT
                w.id AS request_id, w.artist_name, w.album_title, w.status,
                pr.last_search_at,
                CASE
                    WHEN pr.last_search_at IS NULL THEN NULL
                    ELSE EXTRACT(EPOCH FROM (NOW() - pr.last_search_at)) / 3600.0
                END AS hours_since_search,
                COALESCE(pr.searches_24h, 0)::int AS searches_24h,
                COALESCE(pr.searches_6h, 0)::int AS searches_6h
            FROM wanted w
            LEFT JOIN per_request pr ON pr.request_id = w.id
            ORDER BY pr.last_search_at ASC NULLS FIRST, w.created_at ASC, w.id ASC
            LIMIT 12
        """, (list(DASHBOARD_WANTED_BACKLOG_STATUSES),))
        return [
            {
                **serialize_dashboard_request_row(dict(row)),
                "hours_since_search": _float_or_none(row["hours_since_search"]),
            }
            for row in cur.fetchall()
        ]
