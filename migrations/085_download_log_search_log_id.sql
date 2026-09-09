-- Issue #811: link a grab-originated download_log row to the exact
-- search_log row whose ``found`` outcome produced that grab.
--
-- The link is stamped once, inside
-- ``PipelineDB.record_consumed_search_attempt``'s existing transaction,
-- onto ``album_requests.active_download_state``; every later
-- download_log row written from that persisted state carries it through
-- ``GrabListEntry`` -> ``DownloadInfo`` / ``TerminalDownloadAudit``.
--
-- NULL is the honest value for every row that is not a grab outcome:
-- the enqueue-time ``user_offline`` row (written before any search row
-- exists), merge/delete audits, YouTube queue rows, force/local-import
-- rows (whose grab state is long gone), and every historical row.
-- ON DELETE SET NULL because search_log rows are forensic and may be
-- pruned; losing the link must never delete the audit row.
ALTER TABLE download_log
    ADD COLUMN search_log_id INTEGER REFERENCES search_log(id) ON DELETE SET NULL;

CREATE INDEX idx_download_log_search_log_id
    ON download_log (search_log_id)
    WHERE search_log_id IS NOT NULL;
