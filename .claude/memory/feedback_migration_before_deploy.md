---
name: Run DB migrations before deploying code
description: Always ALTER TABLE on production BEFORE committing code that uses new columns — never deploy code referencing columns that don't exist yet
type: feedback
---

When adding new DB columns, run the ALTER TABLE on production BEFORE committing/deploying the code that references them.

**Why:** On 2026-03-31, commit e17e91f added `on_disk_spectral_grade`/`on_disk_spectral_bitrate` to the code but the migration was never run on prod. Every import succeeded in beets but then crashed on the DB update, leaving ~20 albums stuck at `wanted` status overnight. The pipeline silently failed for 12+ hours.

**How to apply:** Whenever touching `pipeline_db.py`'s migration list or adding kwargs to `update_status()` that reference new columns: (1) run the ALTER TABLE on prod via psql first, (2) then commit the code. Never assume the migration code in `__init__` will run in time.
