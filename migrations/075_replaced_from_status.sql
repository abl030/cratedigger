-- Durable #278 authority for Replace-tail staging cleanup.
ALTER TABLE album_requests
    ADD COLUMN replaced_from_status TEXT;

ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_replaced_from_status_valid
    CHECK (
        replaced_from_status IS NULL
        OR replaced_from_status IN (
            'initializing', 'wanted', 'downloading', 'processing', 'imported',
            'unsearchable'
        )
    );

COMMENT ON COLUMN album_requests.replaced_from_status IS
    'Locked source status atomically captured by Replace. NULL is historical/unknown and must fail safe for staging cleanup.';
