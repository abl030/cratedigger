-- Issue #1241: operator-set mark that a request's installed copy is
-- incomplete (missing declared program). NULL = unmarked. Set/cleared only
-- by the operator (pipeline-cli mark-incomplete / POST
-- /api/pipeline/mark-incomplete) and cleared automatically when a terminal
-- import acceptance's candidate was proven whole by beets. Never written by
-- measurement — the daily library-completeness census only informs the
-- operator's decision.
ALTER TABLE album_requests ADD COLUMN marked_incomplete_at TIMESTAMPTZ;
