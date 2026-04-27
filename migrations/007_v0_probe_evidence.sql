-- 007_v0_probe_evidence.sql - V0 probe audit and comparable source state
--
-- Per-attempt probe fields live on download_log for history/audit display.
-- Only the current lossless-source probe fields on album_requests are used as
-- the comparison baseline for suspect lossless-source provisional grind-up.

ALTER TABLE download_log
    ADD COLUMN IF NOT EXISTS v0_probe_kind TEXT CHECK (
        v0_probe_kind IS NULL OR v0_probe_kind IN (
            'lossless_source_v0',
            'native_lossy_research_v0',
            'on_disk_research_v0'
        )
    ),
    ADD COLUMN IF NOT EXISTS v0_probe_min_bitrate INTEGER,
    ADD COLUMN IF NOT EXISTS v0_probe_avg_bitrate INTEGER,
    ADD COLUMN IF NOT EXISTS v0_probe_median_bitrate INTEGER,
    ADD COLUMN IF NOT EXISTS existing_v0_probe_kind TEXT CHECK (
        existing_v0_probe_kind IS NULL OR existing_v0_probe_kind IN (
            'lossless_source_v0',
            'native_lossy_research_v0',
            'on_disk_research_v0'
        )
    ),
    ADD COLUMN IF NOT EXISTS existing_v0_probe_min_bitrate INTEGER,
    ADD COLUMN IF NOT EXISTS existing_v0_probe_avg_bitrate INTEGER,
    ADD COLUMN IF NOT EXISTS existing_v0_probe_median_bitrate INTEGER;

ALTER TABLE album_requests
    ADD COLUMN IF NOT EXISTS current_lossless_source_v0_probe_min_bitrate INTEGER,
    ADD COLUMN IF NOT EXISTS current_lossless_source_v0_probe_avg_bitrate INTEGER,
    ADD COLUMN IF NOT EXISTS current_lossless_source_v0_probe_median_bitrate INTEGER;
