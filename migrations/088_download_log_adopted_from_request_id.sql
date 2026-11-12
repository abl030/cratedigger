-- Issue #1453: Replace --adopt moves a rejected download_log row from the
-- superseded request onto the successor that adopts its folder. The
-- rejection was decided against the ORIGINAL request, and that request's
-- source_denylist row still cites it as authority. This column keeps the
-- provenance so the world audit can bind the source request's denylist row
-- to the rejection that now lives on the successor. Set only by
-- reassign_wrong_match_source; NULL on every row that was never adopted.
ALTER TABLE download_log
    ADD COLUMN adopted_from_request_id INTEGER
        REFERENCES album_requests(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_download_log_adopted_from_request_id
    ON download_log(adopted_from_request_id)
    WHERE adopted_from_request_id IS NOT NULL;
