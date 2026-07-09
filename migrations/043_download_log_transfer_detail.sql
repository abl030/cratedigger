-- Per-file transfer failure detail audit blob (issue #564 C7).
--
-- The composed error_message (issue #564 C5) already names the
-- deduplicated evidence summary for operators, but the underlying
-- per-file detail (which peer, which file, what exact state/exception,
-- how many bytes, how many retries) was previously discarded entirely.
-- Persisted alongside the timeout row so it's queryable without
-- re-deriving it from JSONB active_download_state history that no
-- longer exists once the request self-heals back to 'wanted'.

ALTER TABLE download_log ADD COLUMN transfer_detail JSONB;
