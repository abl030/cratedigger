-- Current library location is resolved from the exact Beets identity.
-- A path copied onto album_requests is a stale cache and cannot be authority.
ALTER TABLE album_requests DROP COLUMN imported_path;
