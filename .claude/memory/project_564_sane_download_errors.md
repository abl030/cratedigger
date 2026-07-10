---
name: project-564-sane-download-errors
description: Issue
metadata: 
  node_type: memory
  type: project
  originSessionId: 5535dfdf-fd91-485b-8a97-799f98ea51bf
---

Issue #564 ("slskd disappearing tracks error message") COMPLETE 2026-07-09: PR #569 (9 commits) merged and deployed to doc2 via fleet-update, tagged v2026.07.09-2. Orchestrated: sonnet implementer → opus review (SHIP, minor+nit folded in) → orchestrator verify. Suite 4948 OK, pyright 0/0, fuzz burst green.

Four root causes fixed: (1) `TransferSnapshot` now decodes slskd's `exception` field; (2) persistence gate split from progress detection (`state_dirty` vs `progress_made`) so terminal-error observations persist; (3) `harvest_terminal_transfer_evidence()` runs immediately before the end-of-cycle `remove_completed_downloads()` purge (keep this ordering — AST-pinned); (4) enqueue-failure HTTP bodies captured as `SlskdEnqueueOutcome.reason`, stamped into per-file `last_exception`. Messages composed from evidence (`summarize_file_failures` in lib/download.py); classify renders "Download failed: …" for `outcome=timeout` and a `user_offline` badge. Migration 043 added `download_log.transfer_detail` JSONB (per-file `FileFailureDetail`) — queryable for per-peer failure-mode triage.

Deploy verified: migration 43 in schema_migrations, post-deploy cycle clean, web 200. Enriched timeout messages appear as the downloading cohort ages (remote_queue_timeout 3600s) — spot-check `download_log.error_message`/`transfer_detail` on the next timeout if curious. The 14-day slskd failure-mode catalogue lives in the issue's 2026-07-09 comment.

Related: [[project-550-complete]] (same slskd-ownership subsystem), [[project-548-generated-testing]] (pin+property method used throughout).
