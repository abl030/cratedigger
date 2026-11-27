-- 089_evidence_file_audio_sha256.sql
--
-- Issue #1471 gap 1. Bad Rip (ban-source) used to hash only the library
-- copy. When the import was converted (FLAC -> Opus), every stored hash was
-- Opus, while the pre-import gate hashes candidates as the peer shares them
-- (FLAC). The lookup keys on (hash, audio_format), so the banned rip could
-- come straight back from any other peer.
--
-- The preview measurement already hashes every staged candidate file for
-- the bad-hash gate. This column keeps those digests on the candidate
-- evidence file rows, so a later Bad Rip can read them back through
-- download_log.candidate_evidence_id and record the source-format hashes.
--
-- Nullable: legacy rows, current-library evidence, unsupported formats and
-- per-file hashing failures carry no hash. Not part of the snapshot
-- fingerprint, so content addressing is unchanged.

ALTER TABLE album_quality_evidence_files
    ADD COLUMN audio_sha256 BYTEA
        CHECK (audio_sha256 IS NULL OR octet_length(audio_sha256) = 32);
