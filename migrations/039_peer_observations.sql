-- 039_peer_observations.sql - Replace peer/dir combo tracking with a
-- distinct-peer roster (issue #227).
--
-- The peer/dir observation experiment (migrations 012 + 015) tracked every
-- distinct (peer, directory) combo browsed cold. The combo table grew
-- ~500K rows/week and its lifetime-totals dashboard query was a 4.6s full
-- scan at 2.4M rows. The only number the operator actually watches is the
-- distinct-peer count ("how big is Soulseek") -- ~40K peers, which fits in
-- a table small enough that every dashboard read can be computed live.
--
-- Raw Soulseek usernames are intentionally not stored. `username_hash` is
-- the same `_stable_hash("peer-dir-user", username)` digest the combo
-- table used, so the backfill below dedupes correctly against all future
-- upserts from the new write path.

CREATE TABLE peer_observations (
    username_hash TEXT PRIMARY KEY CHECK (length(username_hash) = 64),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_peer_observations_first_seen
    ON peer_observations(first_seen_at DESC);

-- Backfill the roster from the combo table. MIN(first_seen_at) preserves
-- the post-truncate epoch (2026-05-08) and the per-peer discovery dates
-- that feed the dashboard growth curve.
INSERT INTO peer_observations (username_hash, first_seen_at, last_seen_at)
SELECT username_hash, MIN(first_seen_at), MAX(last_seen_at)
FROM peer_dir_observations
GROUP BY username_hash;

DROP TABLE peer_dir_observations;
DROP TABLE peer_dir_daily_aggregates;
