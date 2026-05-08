-- 015_peer_dir_daily_aggregates.sql - Lazy-fill cache for completed-day
-- Perth-local peer-dir aggregates.
--
-- The Pipeline dashboard's per-day breakdown (`get_peer_dir_daily_metrics`)
-- aggregates `peer_dir_observations` by Perth-local day. Once a Perth-local
-- day is over its `(new_combos, new_peers, new_dirs)` tuple is frozen --
-- `first_seen_at` is wall-clock-stamped at insert and no late-arriving rows
-- can land in a past day. This table caches those completed-day tuples so
-- subsequent dashboard reads hit cheap PK lookups instead of re-scanning
-- the full observations table.
--
-- Immutability invariant: any row in this table is, by definition, for a
-- Perth-local day strictly less than today's Perth-local date at the time
-- the row was written. Completed days never change, so there is no
-- staleness detection, no `computed_at`, no TTL. Today's row is always
-- recomputed live and is never stored here.
--
-- The cache PK is the Perth-local date, matching the bucketing used by the
-- read-side query `(first_seen_at AT TIME ZONE 'Australia/Perth')::date`.
-- Using a UTC date instead would silently mis-bucket rows by the 8-hour
-- Perth/UTC offset.
--
-- Population path is lazy-fill on dashboard read: the application detects
-- missing completed days, aggregates them in one bounded `GROUP BY` query,
-- and bulk-inserts via `INSERT ... ON CONFLICT (day) DO NOTHING` under
-- autocommit. Each row is independently idempotent, so concurrent
-- dashboard requests cannot corrupt or duplicate cache entries.

CREATE TABLE peer_dir_daily_aggregates (
    day DATE PRIMARY KEY,
    new_combos INTEGER NOT NULL,
    new_peers INTEGER NOT NULL,
    new_dirs INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT peer_dir_daily_aggregates_new_combos_nonneg_check
        CHECK (new_combos >= 0),
    CONSTRAINT peer_dir_daily_aggregates_new_peers_nonneg_check
        CHECK (new_peers >= 0),
    CONSTRAINT peer_dir_daily_aggregates_new_dirs_nonneg_check
        CHECK (new_dirs >= 0)
);
