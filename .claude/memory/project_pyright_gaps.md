---
name: Pyright third-party gaps
description: 21 remaining pyright errors in cratedigger.py — all from third-party type stubs, not our code
type: reference
---

All 21 remaining pyright errors in cratedigger.py are at third-party library boundaries:

**slskd_api (6 errors)**: `directory["files"]` typed as slice access instead of dict. The slskd_api package (0.2.3) has incomplete/wrong type stubs for API response objects. Lines 399-400, 621-624.

**psycopg2 RealDictRow (12 errors)**: `.get("key")` typed with `bytes` key parameter. Lines 899, 905, 940-941, 966, 1208, 1283. The psycopg2 stubs don't properly type RealDictRow as a string-keyed dict.

**sqlite3 Row (2 errors)**: `row[2]` access in _check_quality_gate beets DB queries. Lines around 890-900.

**pipeline_db_source (1 error)**: `.get()` after a None check that pyright can't follow across function boundaries. Line 1283.

**Why not fixed:** These are upstream type stub issues. The code is correct at runtime (280 tests prove it). Fixing would require per-line `# type: ignore` or `cast()` wrappers — noise without benefit.

**How to apply:** When adding new code that touches slskd API responses or psycopg2 rows, expect pyright complaints. Use `cast()` or `# type: ignore` if it blocks you, but don't chase zero on these.
