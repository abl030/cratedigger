-- Index on discogs_release_id for efficient lookups when adding/checking
-- Discogs-sourced albums. Supports issue #69 (Discogs as first-class citizen).
CREATE INDEX idx_requests_discogs_release ON album_requests(discogs_release_id);
