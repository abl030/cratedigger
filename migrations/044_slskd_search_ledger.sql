-- Write-ahead ledger for slskd search ids cratedigger creates (issue #576).
--
-- Root cause: the slskd search id was never persisted anywhere (only held
-- in memory during a cycle), so any process death between POST /searches
-- and the happy-path delete() in execute_search's finally leaked the
-- search on slskd forever -- slskd has no retention configured and never
-- garbage-collects on its own. Verified live: 1,677 resident searches,
-- 1,673 Completed/TimedOut/Errored, oldest since 2026-04-03. Dominant leak
-- path is a SIGTERM mid-cycle (deploys, OOM, reboots); minor paths are a
-- submit-retry's earlier half-created attempt, a submit error after slskd
-- already accepted the POST, and a post-accept collection crash.
--
-- Fix: every creation site inserts a row here BEFORE issuing the POST
-- (write-ahead -- I2), so a kill at ANY point after the POST still leaves
-- a durable record a later cycle's sweep can act on (I1, kill-proof). The
-- sweep NEVER touches an slskd search whose id isn't in this table (I3 --
-- #571 good-citizen doctrine: a human could be sharing the instance).
--
-- `search_id` is stored as TEXT, not the postgres `uuid` type: no other
-- table in this schema uses `uuid` and psycopg2 list-parameter adaptation
-- for the sweep's bulk `mark_search_ids_deleted` is friction-free against
-- TEXT (`= ANY(%s)` needs no explicit array cast), matching every other
-- identifier column in this schema. The value is always a UUID string
-- minted via `uuid.uuid4()`; nothing depends on the postgres type.
--
-- `request_id` carries no FK constraint -- requests can be superseded
-- (Replace) or the row can outlive its request's own lifecycle; this
-- column is forensic only (which request a search was for), never joined
-- for correctness.

CREATE TABLE slskd_search_ledger (
    search_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    purpose TEXT NOT NULL,
    request_id BIGINT,
    deleted_at TIMESTAMPTZ
);

-- The sweep scans undeleted rows past the GRACE window every cycle; a
-- partial index keeps that scan cheap regardless of how many already-
-- swept (and not-yet-pruned) rows accumulate.
CREATE INDEX idx_slskd_search_ledger_unswept
    ON slskd_search_ledger (created_at)
    WHERE deleted_at IS NULL;
