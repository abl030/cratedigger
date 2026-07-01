-- Issue #146 phase 1: cursor for the slskd events feed poller.
--
-- Single-row table recording the newest slskd event we have processed.
-- The poller pages /api/v0/events (newest-first) from offset 0 until it
-- sees last_event_id (or an event older than last_event_timestamp) and
-- stamps DownloadFileComplete localFilename values onto
-- active_download_state. Timestamps are stored as the raw ISO-8601
-- strings slskd emits (7-digit fractional seconds) — comparison happens
-- in Python with a tolerant parser.

CREATE TABLE IF NOT EXISTS slskd_event_cursor (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_event_id TEXT NOT NULL,
    last_event_timestamp TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
