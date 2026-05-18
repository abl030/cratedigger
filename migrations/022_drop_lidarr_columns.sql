-- 022_drop_lidarr_columns.sql — drop vestigial lidarr_* columns
--
-- `album_requests.lidarr_album_id` and `album_requests.lidarr_artist_id`
-- are vestigial from the soularr-fork era. There are no readers or writers
-- in the codebase; only `001_initial.sql` declares them and they have no
-- foreign-key constraints. Drop them.

ALTER TABLE album_requests
    DROP COLUMN lidarr_album_id,
    DROP COLUMN lidarr_artist_id;
