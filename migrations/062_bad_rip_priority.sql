ALTER TABLE album_requests
ADD COLUMN priority_started_at TIMESTAMPTZ;

COMMENT ON COLUMN album_requests.priority_started_at IS
    'Most recent operator urgency window; Bad Rip stamps this without rewriting created_at';
