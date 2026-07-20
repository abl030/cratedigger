-- Preserve the exact decoder diagnostic alongside candidate integrity facts.
-- Per-file decode_ok identifies which files failed; this album-level text keeps
-- ffmpeg's actionable stderr available after the preview worker exits.
ALTER TABLE album_quality_evidence
    ADD COLUMN audio_error TEXT;
