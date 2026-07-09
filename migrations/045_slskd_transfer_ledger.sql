-- Write-ahead ownership ledger for slskd transfers cratedigger creates
-- (issue #571 good-citizen doctrine, enabler PR).
--
-- Root cause: cratedigger currently infers ownership of an slskd transfer
-- NEGATIVELY. converge_slskd_orphans (lib/slskd_transfers.py) cancels any
-- LIVE transfer not backed by a currently-'downloading' album_requests
-- row; reap_disk_orphans deletes any 7-day-old completed file it can't
-- attribute; remove_completed_downloads bulk-purges every completed
-- transfer record slskd reports at the end of a cycle. None of these
-- checks a durable "cratedigger created this" record -- once a download
-- completes and its request leaves 'downloading' (imported, replaced, or
-- reset to wanted after a retry), there is nothing left distinguishing a
-- transfer/file cratedigger created from one a human sharing the same
-- slskd instance created. #571's doctrine: cratedigger may only destroy
-- what it can positively prove it created.
--
-- Fix: every production enqueue call site inserts one row per file here
-- BEFORE issuing the slskd `POST /api/v0/transfers/downloads/{username}`
-- call (write-ahead -- T1), so a process death at ANY point after
-- submission still leaves a durable ownership record a future
-- reaper/convergence pass can act on. lib/slskd_events.py's event
-- ingestion stamps `local_path` + `completed_at` onto the matching row in
-- the SAME pass it already stamps `active_download_state` (T2), using the
-- same (username, remote filename) matching key. This migration and its
-- write paths are the enabler only -- converge_slskd_orphans,
-- reap_disk_orphans, and remove_completed_downloads are NOT changed by
-- this migration; teaching them to consult this ledger instead of
-- negative inference is three separate follow-up PRs.
--
-- `username`/`filename` are TEXT, matching every other soulseek identifier
-- column in this schema; `filename` is the raw remote path slskd itself
-- uses as its matching key (see lib/slskd_events.py's (username, remote
-- filename) rationale) -- not a local filesystem path.
--
-- `request_id` carries no FK constraint, mirroring migration 044's
-- rationale: a request can be superseded (Replace) or reset back to
-- 'wanted' after a retry, and this column is forensic only (which
-- request an enqueue was for). It is NOT NULL (unlike 044's optional
-- artist-probe case) -- every production slskd transfer enqueue in this
-- pipeline is for a specific album_requests row; there is no
-- request-less transfer analogous to 044's artist_probe searches.
--
-- `transfer_id` is nullable and, in this PR, always NULL: the write-ahead
-- insert happens BEFORE the POST that would return slskd's transfer id,
-- so it is never known yet at insert time. A future PR may choose to
-- backfill it once an enqueue reconciles; nothing in this PR reads it
-- back to a non-NULL value.
--
-- `attempt_fingerprint` is nullable and populated whenever the enqueue
-- flow has computed one (lib/processing_paths.py::attempt_fingerprint) --
-- lets a future reaper flip derive the canonical processing folder
-- (issue #550 phase 2) directly from ledger rows instead of re-deriving
-- it from live active_download_state.

CREATE TABLE slskd_transfer_ledger (
    id BIGSERIAL PRIMARY KEY,
    request_id BIGINT NOT NULL,
    username TEXT NOT NULL,
    filename TEXT NOT NULL,
    transfer_id TEXT,
    attempt_fingerprint TEXT,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    local_path TEXT,
    completed_at TIMESTAMPTZ
);

-- T2's event-stamp lookup: every DownloadFileComplete event is matched to
-- ledger rows by the same (username, remote filename) key production
-- already matches active_download_state by.
CREATE INDEX idx_slskd_transfer_ledger_username_filename
    ON slskd_transfer_ledger (username, filename);

-- A future reaper/convergence flip's "still open" scan (rows with no
-- completion stamp yet) stays cheap regardless of how many completed
-- rows accumulate.
CREATE INDEX idx_slskd_transfer_ledger_open
    ON slskd_transfer_ledger (enqueued_at)
    WHERE completed_at IS NULL;

-- A future disk-reaper flip's "is this local_path mine?" lookup; partial
-- because most rows are still open (local_path IS NULL) at any moment.
CREATE INDEX idx_slskd_transfer_ledger_local_path
    ON slskd_transfer_ledger (local_path)
    WHERE local_path IS NOT NULL;

-- T3's prune query joins back to album_requests.status to decide whether
-- a request is still active (never prune those rows regardless of age).
CREATE INDEX idx_slskd_transfer_ledger_request_id
    ON slskd_transfer_ledger (request_id);
