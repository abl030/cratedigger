-- 005_import_preview_opt_in_default.sql - keep async import previews opt-in
--
-- Migration 004 introduced preview state with a database default of `waiting`.
-- Application code now writes `waiting` only when the preview gate is enabled.
-- Keep raw/older enqueue paths backward-compatible by making omitted preview
-- columns importable immediately.

ALTER TABLE import_jobs
    ALTER COLUMN preview_status SET DEFAULT 'would_import',
    ALTER COLUMN preview_message SET DEFAULT 'Preview gate disabled',
    ALTER COLUMN preview_completed_at SET DEFAULT NOW(),
    ALTER COLUMN importable_at SET DEFAULT NOW();
