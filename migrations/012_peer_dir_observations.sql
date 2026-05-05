-- 012_peer_dir_observations.sql - Track first-seen peer/directory pairs.
--
-- Raw Soulseek usernames and directory paths are intentionally not stored in
-- Postgres. The pipeline hashes each peer/directory observation before insert
-- and this table keeps only aggregate-friendly identifiers for dashboard
-- trend views.

CREATE TABLE peer_dir_observations (
    combo_hash TEXT PRIMARY KEY CHECK (length(combo_hash) = 64),
    username_hash TEXT NOT NULL CHECK (length(username_hash) = 64),
    dir_hash TEXT NOT NULL CHECK (length(dir_hash) = 64),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    seen_count INTEGER NOT NULL DEFAULT 1 CHECK (seen_count > 0)
);

CREATE INDEX idx_peer_dir_observations_first_seen
    ON peer_dir_observations(first_seen_at DESC);
CREATE INDEX idx_peer_dir_observations_last_seen
    ON peer_dir_observations(last_seen_at DESC);
CREATE INDEX idx_peer_dir_observations_username_hash
    ON peer_dir_observations(username_hash);
