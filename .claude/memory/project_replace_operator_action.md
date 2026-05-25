---
name: project-replace-operator-action
description: "Replace operator action shipped 2026-05-18 — supersede model, three button surfaces, lazy-resolve for legacy null-RG rows, known gaps"
metadata: 
  node_type: memory
  type: project
  originSessionId: 1db8a72d-014c-43a5-bc58-d65e264e80cc
---

The Replace operator action shipped 2026-05-18 via PRs #279 (feature) + #280 (UI wiring fix). Dogfooded successfully against Pet Grief request 4194 (MBID `72988560-...` merged upstream → replaced with canonical `18056805-...`).

**Fact:** The action uses the supersede model — old row → `status='replaced'` (frozen audit; only `imported_path` cleared), new row INSERTed with `replaces_request_id` lineage. No MBID is ever rewritten on an existing row.

**Why:** Avoids first-of-its-kind identity mutation. The frozen old row IS the audit trail; lineage column links forward.

**How to apply:** When debugging any post-Replace question, query both rows in the chain via `replaces_request_id`. The OLD row keeps all its history (`download_log`, `album_quality_evidence`, `source_denylist`) intact. The NEW row starts clean. Service is `lib/mbid_replace_service.py`; CLI is `pipeline-cli replace <id> --to <mbid>`; API is `POST /api/pipeline/<id>/replace`.

**Known gaps (filed as issues):**
- #281: picker rows can't be expanded to show tracklists — operator can't easily disambiguate pressings by track listing alone.
- #282: Discogs-pathway requests not supported (no MB release-group anchor) — needs brainstorm for cross-pathway identity model.
- #278: general slskd-orphan convergence — Replace leaves in-flight transfers orphan, intentionally.

**The 4001 legacy-null-RG row class is gone** as of 2026-05-18 — backfill script ran once on doc2, populated 3876 rows (124 Discogs-numeric rows skipped per #282, 1 transient error). Script removed from repo post-run. The lazy-resolve endpoint at `POST /api/pipeline/<id>/resolve-rg` handles any future null-RG row at picker-open time.

**Two new CLAUDE.md invariants** were enshrined in the same PR:
- "The pipeline self-heals — the request is the source of truth, everything else is derived."
- "Don't duplicate convergence — reuse the cleanup paths that already exist."
