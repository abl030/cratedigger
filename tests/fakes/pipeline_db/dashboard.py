"""FakePipelineDB dashboard cluster — mirrors ``lib/pipeline_db/dashboard.py``.

Pipeline dashboard telemetry aggregation.
"""
from __future__ import annotations

from collections.abc import (
    Iterable,
)
from datetime import UTC, date, datetime, timedelta
from typing import (
    Any,
)

from lib.cycle_counters import COUNTER_NAMES, CycleCounters, counter_values
from lib.pipeline_db import (
    UnfindableRunMetricsPresentation,
    UnfindableRunMetricsRow,
)
from lib.pipeline_db._shared import (
    CACHE_ATTRIBUTION_CYCLE_ONLY,
    DASHBOARD_WANTED_BACKLOG_STATUSES,
    DASHBOARD_WINDOWS,
    _isoformat_or_none,
)
from lib.pipeline_db.dashboard import (
    SEARCH_ERROR_OUTCOMES,
    dashboard_envelope,
    serialize_dashboard_cycle_row,
    serialize_dashboard_heavy_query_row,
    serialize_dashboard_request_row,
    serialize_unfindable_run_row,
    unfindable_panel,
    wanted_trend_panel,
)
from tests.fakes._shared import _PERTH_TZ, _as_datetime, _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase


class _FakeDashboardMixin(_FakePipelineDBBase):
    """Pipeline dashboard telemetry aggregation."""

    def record_cycle_metrics(
        self,
        *,
        cycle_total_s: float,
        counters: CycleCounters | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        wanted_total: int | None = None,
    ) -> int:
        wanted_snapshot = (
            self._current_wanted_total() if wanted_total is None
            else max(0, int(wanted_total))
        )
        row = {
            "id": len(self.cycle_metrics) + 1,
            "started_at": started_at,
            "created_at": completed_at or _utcnow(),
            "cycle_total_s": cycle_total_s,
            # Same source as production's INSERT column list, so a counter
            # cannot reach one store and not the other.
            **dict(zip(
                COUNTER_NAMES,
                counter_values(counters or CycleCounters()),
                strict=True,
            )),
            "wanted_total": wanted_snapshot,
        }
        self.cycle_metrics.append(row)
        return int(row["id"])

    def _current_wanted_total(self) -> int:
        return sum(1 for req in self._requests.values()
                   if req.get("status") in DASHBOARD_WANTED_BACKLOG_STATUSES)

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
        partition_sum = (
            categorised_count + downgraded_count + no_change_count
            + probe_failed_count + not_due_count + request_not_found_count
        )
        if partition_sum != candidates_processed:
            # Mirror unfindable_run_metrics_partition_check (migration
            # 077, #1112 review round 2 R5) -- a fake that accepts any
            # combination shipped a row production rejects (#146-style
            # fake-mirrors-CHECK precedent).
            import psycopg2.errors

            raise psycopg2.errors.CheckViolation(
                'new row for relation "unfindable_run_metrics" violates '
                'check constraint '
                '"unfindable_run_metrics_partition_check" '
                f'(sum={partition_sum}, '
                f'candidates_processed={candidates_processed})'
            )
        expected_probes_attempted = (
            candidates_processed - not_due_count - request_not_found_count
        )
        if probes_attempted != expected_probes_attempted:
            # Mirror unfindable_run_metrics_probes_attempted_check
            # (migration 077, #1112 review round 2 R5).
            import psycopg2.errors

            raise psycopg2.errors.CheckViolation(
                'new row for relation "unfindable_run_metrics" violates '
                'check constraint '
                '"unfindable_run_metrics_probes_attempted_check" '
                f'(probes_attempted={probes_attempted}, '
                f'expected={expected_probes_attempted})'
            )
        row = UnfindableRunMetricsRow(
            id=len(self.unfindable_run_metrics) + 1,
            created_at=_utcnow(),
            cohort_total=cohort_total,
            due_backlog_at_start=due_backlog_at_start,
            batch_limit=batch_limit,
            candidates_processed=candidates_processed,
            probes_attempted=probes_attempted,
            categorised_count=categorised_count,
            downgraded_count=downgraded_count,
            no_change_count=no_change_count,
            probe_failed_count=probe_failed_count,
            not_due_count=not_due_count,
            request_not_found_count=request_not_found_count,
            breaker_tripped=breaker_tripped,
            duration_seconds=duration_seconds,
        )
        self.unfindable_run_metrics.append(row)
        return row["id"]

    def get_unfindable_run_metrics(
        self, *, limit: int = 30,
    ) -> list[UnfindableRunMetricsRow]:
        rows = sorted(
            self.unfindable_run_metrics,
            key=lambda r: (self._as_utc(r["created_at"]), r["id"]),
            reverse=True,
        )
        return list(rows[:limit])

    def _dashboard_unfindable(
        self,
    ) -> dict[str, list[UnfindableRunMetricsPresentation] | dict[str, object]]:
        return unfindable_panel([
            serialize_unfindable_run_row(r)
            for r in self.get_unfindable_run_metrics(limit=14)
        ])

    def _dashboard_wanted_trend(self, current_wanted: int) -> dict[str, object]:
        """Collect the 7-day sample series, then delegate the arithmetic.

        Production fetches the same series with a ``WHERE created_at >=
        NOW() - INTERVAL '7 days'`` + ``ORDER BY created_at ASC`` query;
        this walks seeded ``cycle_metrics`` instead. Everything after the
        sample list — windows, deltas, drain rates, ETA — is
        ``wanted_trend_panel``, shared with the real adapter.
        """
        now = _utcnow()
        samples: list[tuple[datetime, int]] = []
        for row in sorted(self.cycle_metrics, key=lambda r: r["created_at"]):
            if row.get("wanted_total") is None:
                continue
            created_at = self._as_utc(row["created_at"])
            if created_at >= now - timedelta(days=7):
                samples.append((created_at, int(row["wanted_total"])))
        return wanted_trend_panel(
            samples, current_wanted=current_wanted, now=now)

    def record_peer_observations(
        self,
        usernames: Iterable[str],
        *,
        observed_at: datetime | None = None,
    ) -> int:
        from lib.pipeline_db import _peer_hash

        observed = observed_at or _utcnow()
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        unique = sorted({str(u) for u in usernames if u})
        new_count = 0
        for username in unique:
            username_hash = _peer_hash(username)
            row = self.peer_observations.get(username_hash)
            if row is None:
                self.peer_observations[username_hash] = {
                    "username_hash": username_hash,
                    "first_seen_at": observed,
                    "last_seen_at": observed,
                }
                new_count += 1
            else:
                row["last_seen_at"] = max(row["last_seen_at"], observed)
        return new_count

    def get_peer_metrics(self, days: int = 14) -> dict[str, Any]:
        """Mirror ``PipelineDB.get_peer_metrics``: live totals plus a
        Perth-local per-day growth curve with cumulative ``total_peers``."""
        clamped_days = max(1, min(int(days), 90))
        rows = list(self.peer_observations.values())

        today_perth = _utcnow().astimezone(_PERTH_TZ).date()
        window_start = today_perth - timedelta(days=clamped_days - 1)

        new_by_day: dict[date, int] = {}
        for row in rows:
            ts = row["first_seen_at"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            day = ts.astimezone(_PERTH_TZ).date()
            new_by_day[day] = new_by_day.get(day, 0) + 1

        day_dicts: list[dict[str, Any]] = []
        cursor = today_perth
        while cursor >= window_start:
            day_dicts.append({
                "date": cursor.isoformat(),
                "new_peers": new_by_day.get(cursor, 0),
                "total_peers": sum(
                    count for day, count in new_by_day.items()
                    if day <= cursor
                ),
            })
            cursor = cursor - timedelta(days=1)

        now = _utcnow()
        return {
            "days": day_dicts,
            "totals": {
                "known_peers": len(rows),
                "new_24h": sum(
                    1 for row in rows
                    if row["first_seen_at"] >= now - timedelta(hours=24)
                ),
                "seen_24h": sum(
                    1 for row in rows
                    if row["last_seen_at"] >= now - timedelta(hours=24)
                ),
                "tracked_since": _isoformat_or_none(
                    min((row["first_seen_at"] for row in rows), default=None)
                ),
            },
        }

    def _dashboard_search_window(
        self, label: str, hours: int, now: datetime,
    ) -> dict[str, object]:
        cutoff = now - timedelta(hours=hours)
        rows = [e for e in self.search_logs
                if self._as_utc(e.created_at) >= cutoff]
        # Errors bucket mirrors the SQL FILTER exactly: only timeout /
        # error / empty_query count — an unknown outcome counts toward
        # ``searches`` but no bucket.
        outcomes = {
            "found": sum(1 for e in rows if e.outcome == "found"),
            "no_match": sum(1 for e in rows if e.outcome == "no_match"),
            "no_results": sum(
                1 for e in rows if e.outcome == "no_results"),
            "exhausted": sum(1 for e in rows if e.outcome == "exhausted"),
            "errors": sum(
                1 for e in rows if e.outcome in SEARCH_ERROR_OUTCOMES),
        }
        elapsed = sorted(
            e.elapsed_s for e in rows if e.elapsed_s is not None)

        def _pct(p: float) -> float | None:
            if not elapsed:
                return None
            return elapsed[min(len(elapsed) - 1, int(len(elapsed) * p))]

        return {
            "label": label,
            "hours": hours,
            "searches": len(rows),
            "distinct_requests": len({e.request_id for e in rows}),
            "searches_per_hour": len(rows) / hours,
            "searches_per_24h": len(rows) / hours * 24,
            "avg_elapsed_s": (sum(elapsed) / len(elapsed)) if elapsed else None,
            "median_elapsed_s": _pct(0.5),
            "p95_elapsed_s": _pct(0.95),
            "max_elapsed_s": elapsed[-1] if elapsed else None,
            "outcomes": outcomes,
            "cursor_wraps": sum(
                1 for e in rows if e.cursor_update_status == "wrapped"),
            "stale_completions": sum(
                1 for e in rows if e.cursor_update_status == "stale"),
            "non_consuming": sum(
                1 for e in rows if e.attempt_consumed is False),
            "cache_attribution_level": CACHE_ATTRIBUTION_CYCLE_ONLY,
        }

    def _dashboard_cycle_window(
        self, label: str, hours: int, now: datetime,
    ) -> dict[str, object]:
        cutoff = now - timedelta(hours=hours)
        rows = [r for r in self.cycle_metrics
                if self._as_utc(r["created_at"]) >= cutoff]
        totals = sorted(float(r["cycle_total_s"]) for r in rows)
        searches = sorted(float(r["search_time_s"]) for r in rows)

        def _pct(values: list[float], p: float) -> float | None:
            if not values:
                return None
            return values[min(len(values) - 1, int(len(values) * p))]

        return {
            "label": label,
            "hours": hours,
            "cycles": len(rows),
            "avg_cycle_s": (sum(totals) / len(totals)) if totals else None,
            "median_cycle_s": _pct(totals, 0.5),
            "p95_cycle_s": _pct(totals, 0.95),
            "max_cycle_s": totals[-1] if totals else None,
            "median_search_s": _pct(searches, 0.5),
            "watchdog_kills": sum(
                int(r["cycle_searches_watchdog_killed"]) for r in rows),
            "find_download_queued": sum(
                int(r["find_download_queued"]) for r in rows),
            "find_download_completed": sum(
                int(r["find_download_completed"]) for r in rows),
            "cache_errors": sum(int(r["cache_errors"]) for r in rows),
            "cache_write_errors": sum(
                int(r["cache_write_errors"]) for r in rows),
            "cache_fuse_tripped": sum(
                int(r["cache_fuse_tripped"]) for r in rows),
            "peers_browsed": sum(int(r["peers_browsed"]) for r in rows),
            "peers_browsed_lazy": sum(
                int(r["peers_browsed_lazy"]) for r in rows),
            "fanout_waves": sum(int(r["fanout_waves"]) for r in rows),
        }

    def _dashboard_coverage(self, now: datetime) -> dict[str, object]:
        """Mirror the production coverage CTEs: backlog = wanted +
        downloading + processing; suspects = searched-in-24h rows ordered
        (searches_24h DESC, searches_6h DESC, id ASC) LIMIT 12 with
        reset_24h counting the HISTORICAL ``exhausted`` outcome and
        problem_24h restricted to timeout/error/empty_query;
        stale_wanted = ALL backlog rows ordered last_search_at ASC
        NULLS FIRST LIMIT 12 (recently-searched rows included); the
        match-rate series are DENSE generate_series mirrors (24 hourly /
        28 daily zero-filled buckets); matches_* ride the wanted CTE
        cross-join, so an empty backlog reports 0 matches even when
        found rows exist."""
        backlog = {
            int(r["id"]): r for r in self._requests.values()
            if r.get("status") in DASHBOARD_WANTED_BACKLOG_STATUSES
        }

        # One pass over search_log per request: rollup of windowed
        # outcome counts + last_search_at.
        rollup: dict[int, dict[str, Any]] = {}
        cutoff_24h = now - timedelta(hours=24)
        cutoff_6h = now - timedelta(hours=6)
        for e in self.search_logs:
            at = self._as_utc(e.created_at)
            r = rollup.setdefault(e.request_id, {
                "last_search_at": None, "searches_24h": 0, "searches_6h": 0,
                "found_24h": 0, "no_match_24h": 0, "no_results_24h": 0,
                "reset_24h": 0, "problem_24h": 0,
            })
            if r["last_search_at"] is None or at > r["last_search_at"]:
                r["last_search_at"] = at
            if at >= cutoff_24h:
                r["searches_24h"] += 1
                if e.outcome == "found":
                    r["found_24h"] += 1
                elif e.outcome == "no_match":
                    r["no_match_24h"] += 1
                elif e.outcome == "no_results":
                    r["no_results_24h"] += 1
                elif e.outcome == "exhausted":
                    r["reset_24h"] += 1
                elif e.outcome in SEARCH_ERROR_OUTCOMES:
                    r["problem_24h"] += 1
            if at >= cutoff_6h:
                r["searches_6h"] += 1

        searched_24h = sum(
            1 for rid in backlog
            if rollup.get(rid, {}).get("searches_24h", 0) > 0)
        searched_6h = sum(
            1 for rid in backlog
            if rollup.get(rid, {}).get("searches_6h", 0) > 0)
        active_24h = sum(
            rollup.get(rid, {}).get("searches_24h", 0) for rid in backlog)
        active_6h = sum(
            rollup.get(rid, {}).get("searches_6h", 0) for rid in backlog)

        found_rows = [e for e in self.search_logs if e.outcome == "found"]
        if backlog:
            matches_24h = sum(
                1 for e in found_rows
                if self._as_utc(e.created_at) >= cutoff_24h)
            matches_6h = sum(
                1 for e in found_rows
                if self._as_utc(e.created_at) >= cutoff_6h)
        else:
            # Production's summary SQL cross-joins match_rates against
            # the wanted CTE — zero backlog rows mean the aggregates
            # COALESCE to 0 regardless of found rows.
            matches_24h = 0
            matches_6h = 0

        # Dense bucket mirrors of generate_series + LEFT JOIN.
        hour_anchor = now.replace(minute=0, second=0, microsecond=0)
        hourly_counts: dict[datetime, int] = {}
        day_anchor = now.replace(hour=0, minute=0, second=0, microsecond=0)
        daily_counts: dict[datetime, int] = {}
        for e in found_rows:
            at = self._as_utc(e.created_at)
            hourly_counts[at.replace(minute=0, second=0, microsecond=0)] = (
                hourly_counts.get(
                    at.replace(minute=0, second=0, microsecond=0), 0) + 1)
            daily_counts[at.replace(
                hour=0, minute=0, second=0, microsecond=0)] = (
                daily_counts.get(at.replace(
                    hour=0, minute=0, second=0, microsecond=0), 0) + 1)
        series_24h = []
        for i in range(23, -1, -1):
            bucket = hour_anchor - timedelta(hours=i)
            n = hourly_counts.get(bucket, 0)
            series_24h.append({
                "bucket_start": _isoformat_or_none(bucket),
                "matches": n,
                "matches_per_hour": n,
            })
        series_28d = []
        for i in range(27, -1, -1):
            bucket = day_anchor - timedelta(days=i)
            n = daily_counts.get(bucket, 0)
            series_28d.append({
                "bucket_start": _isoformat_or_none(bucket),
                "matches": n,
                "matches_per_day": n,
            })

        def _outcome_columns(rid: int) -> dict[str, int]:
            """The per-outcome 24h breakdown, computed by ONE panel query.

            ``_dashboard_top_loop_suspects`` selects these; the
            stale-wanted query below does not select them at all.
            """
            r = rollup.get(rid, {})
            return {
                "found_24h": r.get("found_24h", 0),
                "no_match_24h": r.get("no_match_24h", 0),
                "no_results_24h": r.get("no_results_24h", 0),
                "reset_24h": r.get("reset_24h", 0),
                "problem_24h": r.get("problem_24h", 0),
            }

        def _request_row(
            rid: int, outcomes: dict[str, int],
        ) -> dict[str, Any]:
            """The raw joined row a production panel SELECT returns.

            ``outcomes`` is passed explicitly — empty for the stale-wanted
            panel, whose query projects identity plus the two search
            counts only, so every per-outcome column falls through
            ``serialize_dashboard_request_row``'s ``or 0`` default. The
            two panels used to share the full breakdown, which handed
            stale rows counters production always reports as 0 (issue
            #1278 item 7).
            """
            req = backlog[rid]
            r = rollup.get(rid, {})
            return serialize_dashboard_request_row({
                "request_id": rid,
                "artist_name": req.get("artist_name"),
                "album_title": req.get("album_title"),
                "status": req.get("status"),
                "last_search_at": r.get("last_search_at"),
                "searches_24h": r.get("searches_24h", 0),
                "searches_6h": r.get("searches_6h", 0),
                **outcomes,
            })

        suspects = [
            _request_row(rid, _outcome_columns(rid))
            for rid in sorted(
                (rid for rid in backlog
                 if rollup.get(rid, {}).get("searches_24h", 0) > 0),
                key=lambda rid: (
                    -rollup[rid]["searches_24h"],
                    -rollup[rid]["searches_6h"],
                    rid,
                ),
            )
        ][:12]

        def _stale_sort_key(rid: int):
            at = rollup.get(rid, {}).get("last_search_at")
            req = backlog[rid]
            created = self._as_utc(_as_datetime(req.get("created_at")))
            # NULLS FIRST: never-searched rows sort before everything.
            return (at is not None, at or created, created, rid)

        stale = []
        for rid in sorted(backlog, key=_stale_sort_key)[:12]:
            row = _request_row(rid, {})
            at = rollup.get(rid, {}).get("last_search_at")
            row["hours_since_search"] = (
                (now - at).total_seconds() / 3600 if at else None)
            stale.append(row)

        top_10_searches = sum(r["searches_24h"] for r in suspects[:10])
        top_10_share = (top_10_searches / active_24h) if active_24h else 0

        oldest: str | None = None
        searched_ats = [
            rollup[rid]["last_search_at"] for rid in backlog
            if rid in rollup and rollup[rid]["last_search_at"] is not None
        ]
        if searched_ats:
            oldest = _isoformat_or_none(min(searched_ats))

        return {
            "wanted_total": len(backlog),
            "wanted_searched_24h": searched_24h,
            "wanted_searched_6h": searched_6h,
            "wanted_unsearched_24h": max(len(backlog) - searched_24h, 0),
            "wanted_unsearched_6h": max(len(backlog) - searched_6h, 0),
            "wanted_never_searched": sum(
                1 for rid in backlog
                if rollup.get(rid, {}).get("last_search_at") is None),
            "active_wanted_searches_24h": active_24h,
            "active_wanted_searches_6h": active_6h,
            "oldest_last_search_at": oldest,
            "matches_24h": matches_24h,
            "matches_6h": matches_6h,
            "matches_per_hour_24h": matches_24h / 24,
            "matches_per_hour_6h": matches_6h / 6,
            "match_rate_series_24h": series_24h,
            "match_rate_series_28d": series_28d,
            "wanted_trend": self._dashboard_wanted_trend(
                self._current_wanted_total()),
            "top_10_share_24h": top_10_share,
            "top_loop_suspects": suspects,
            "stale_wanted": stale,
        }

    def _dashboard_heavy_queries(self, now: datetime) -> list[dict[str, object]]:
        """Mirror ``_dashboard_peer_browse_heavy_queries``' selection: rows
        with (peers_browsed + peers_browsed_lazy) > 0 in the last 24h,
        ordered (peer_dirs DESC, fanout_waves DESC, created_at DESC,
        id DESC), LIMIT 12. The row SHAPE — every int/float coercion and
        the isoformatted timestamp — is production's own serializer."""
        cutoff = now - timedelta(hours=24)
        rows = [e for e in self.search_logs
                if self._as_utc(e.created_at) >= cutoff
                and (e.peers_browsed + e.peers_browsed_lazy) > 0]
        rows.sort(key=lambda e: (
            -(e.peers_browsed + e.peers_browsed_lazy),
            -e.fanout_waves,
            -self._as_utc(e.created_at).timestamp(),
            -e.id,
        ))
        out: list[dict[str, object]] = []
        for e in rows[:12]:
            req = self._requests.get(e.request_id, {})
            out.append(serialize_dashboard_heavy_query_row({
                "search_log_id": e.id,
                "request_id": e.request_id,
                "mb_release_id": req.get("mb_release_id"),
                "artist_name": req.get("artist_name"),
                "album_title": req.get("album_title"),
                "status": req.get("status"),
                "created_at": self._as_utc(e.created_at),
                "query": e.query,
                "variant": e.variant,
                "outcome": e.outcome,
                "result_count": e.result_count,
                "elapsed_s": e.elapsed_s,
                "browse_time_s": e.browse_time_s,
                "match_time_s": e.match_time_s,
                "peers_browsed": e.peers_browsed,
                "peers_browsed_lazy": e.peers_browsed_lazy,
                "peer_dirs": (e.peers_browsed or 0) + (e.peers_browsed_lazy or 0),
                "fanout_waves": e.fanout_waves,
            }))
        return out

    def get_pipeline_dashboard_metrics(
        self,
        *,
        plan_generator_id: str | None = None,
    ) -> dict[str, Any]:
        """Python mirror of the production dashboard read-model.

        Aggregates real seeded telemetry (``search_logs``,
        ``cycle_metrics``, ``peer_observations``, request rows) into the
        same envelope ``PipelineDB.get_pipeline_dashboard_metrics``
        emits, with every timestamp isoformatted exactly like the
        production ``_isoformat_or_none`` boundary (datetimes leaking
        here 500 the dashboard route's json.dumps). Percentiles use a
        simple nearest-rank cut — close enough for contract tests; the
        production SQL is the authority on exact statistics.
        """
        if plan_generator_id is None:
            from lib.search import SEARCH_PLAN_GENERATOR_ID
            plan_generator_id = SEARCH_PLAN_GENERATOR_ID
        now = _utcnow()
        peers = self.get_peer_metrics()
        peers["heavy_queries"] = self._dashboard_heavy_queries(now)
        peers["heavy_query_hours"] = 24
        return dashboard_envelope(
            generated_at=now,
            search_windows=[
                self._dashboard_search_window(label, hours, now)
                for label, hours in DASHBOARD_WINDOWS
            ],
            cycle_windows=[
                self._dashboard_cycle_window(label, hours, now)
                for label, hours in DASHBOARD_WINDOWS
            ],
            # Production: ORDER BY created_at DESC, id DESC LIMIT 12 — NOT
            # insertion order (rows seeded with explicit completed_at
            # values must sort by their timestamps).
            recent_cycles=[
                serialize_dashboard_cycle_row(r)
                for r in sorted(
                    self.cycle_metrics,
                    key=lambda row: (
                        self._as_utc(row["created_at"]),
                        int(row["id"]),
                    ),
                    reverse=True,
                )[:12]
            ],
            # Production restricts outliers to the last 24 hours and
            # orders by cycle_total_s DESC, id DESC.
            outlier_cycles=[
                serialize_dashboard_cycle_row(r)
                for r in sorted(
                    (row for row in self.cycle_metrics
                     if self._as_utc(row["created_at"])
                     >= now - timedelta(hours=24)),
                    key=lambda row: (
                        float(row["cycle_total_s"]),
                        int(row["id"]),
                    ),
                    reverse=True,
                )[:8]
            ],
            coverage=self._dashboard_coverage(now),
            peers=peers,
            plan_readiness=self.get_search_plan_readiness(plan_generator_id),
            unfindable=self._dashboard_unfindable(),
        )

