-- A direct Add / new-row Upgrade is not runnable until its canonical tracks,
-- field-resolution audit, and initial plan outcome are durable. Existing rows
-- are already published, so no data migration is needed.

ALTER TABLE album_requests
    DROP CONSTRAINT album_requests_status_check;

ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_status_check
    CHECK(status IN (
        'initializing', 'wanted', 'downloading', 'imported', 'unsearchable',
        'replaced'
    ));
