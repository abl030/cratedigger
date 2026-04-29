-- 008_bad_audio_hashes.sql - Curator-reported bad-rip audio-content hashes
--
-- Per-track raw SHA-256 of compressed audio frames (tags + artwork stripped),
-- written by the ban-source route when a curator clicks "Bad rip" on a library
-- row. The pre-import gate (lib/preimport.py) hashes future search candidates
-- and rejects on match — bad bytes "ripple" through Soulseek harmlessly.
--
-- request_id is FK with ON DELETE SET NULL so request lifecycle changes don't
-- destroy the audit trail. reported_username is NULL when no successful
-- download_log was resolvable at click time (E1.1 in the plan). The (hash_value,
-- audio_format) UNIQUE serves both as the dedupe key and as the lookup index
-- for the validation-time gate (B-tree leftmost prefix covers hash_value alone).

CREATE TABLE bad_audio_hashes (
    id              BIGSERIAL PRIMARY KEY,
    hash_value      BYTEA NOT NULL,
    audio_format    TEXT NOT NULL,
    request_id      INTEGER REFERENCES album_requests(id) ON DELETE SET NULL,
    reported_username TEXT,
    reason          TEXT,
    reported_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (hash_value, audio_format)
);
