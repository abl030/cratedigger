-- Rename the operator-owned search stop and remove its unused reason field.
-- Updating only status deliberately preserves updated_at on affected rows.

ALTER TABLE album_requests
    DROP CONSTRAINT album_requests_status_check;

UPDATE album_requests
SET status = 'unsearchable'
WHERE status = 'manual';

ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_status_check
    CHECK(status IN (
        'wanted', 'downloading', 'imported', 'unsearchable', 'replaced'
    ));

ALTER TABLE album_requests DROP COLUMN manual_reason;
