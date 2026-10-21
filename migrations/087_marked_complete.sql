-- Operator-set mark that a request's installed copy is complete despite the
-- library-completeness census reporting findings for it (e.g. a segued
-- Discogs composite the silence-gap instrument cannot see into).
-- marked_complete_findings records the exact census findings the operator
-- reviewed; the dashboard honours the mark only while the census still
-- reports exactly those findings, so a new or changed finding resurfaces.
-- Set/cleared only by the operator (pipeline-cli mark-complete / POST
-- /api/pipeline/mark-complete). Mutually exclusive with marked_incomplete_at.
ALTER TABLE album_requests
    ADD COLUMN marked_complete_at TIMESTAMPTZ,
    ADD COLUMN marked_complete_findings JSONB;
