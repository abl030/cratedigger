---
name: project-501-510-refactor-batch
description: The
metadata: 
  node_type: memory
  type: project
  originSessionId: 694f77c4-3beb-46a5-bb35-bc94ebb68d7e
---

COMPLETE 2026-07-05. Five open refactor issues implemented, reviewed, merged to cratedigger main, and deployed to doc2 (tag `v2026.07.05`, rev 109f7ad):

- **#501** (PR #527) post-#282 replace consolidation: server-side `/api/release-group/<id>` numeric→Discogs-master dispatch, typed `reason` on `ReplaceResult` (→msgspec.Struct) + shared `lib/replace_status.py`, MB-arm `_mb_lookup_or_error` parity.
- **#507** (PR #529) typed the slskd `get_all_downloads()` envelope (`DownloadUser`/`DownloadDirectory` Structs + `parse_downloads_envelope`), tolerant drop-decode.
- **#508** (PR #525) removed dead slskd surface (`get_download` network path, no-snapshot branch, fake `get_downloads`).
- **#509** (PR #528) unified the two staged-path resume deciders behind `_evaluate_staged_path_readiness`.
- **#510** (PR #526) `ActiveDownloadState.from_raw()` dedup (item 2 correctly landed docs-only — the unified job-outcome mapper already exists as `process_claimed_job`).

**Workflow that worked well (reuse it):** Sonnet subagents implement one issue each in isolated worktrees (TDD, all gates green, open PR), explicitly FORBIDDEN from spawning any reviewers. Orchestrator drives the review loop with a fresh Opus reviewer per PR (read-only, off the pushed branch, re-runs `nix-shell pyright` itself since the live IDE diagnostics run non-nix and are noise). NITs that hit a hard rule (scope.md "no fallback that never fires", a real undocumented behavior divergence in the self-heal path) get one fix round-trip via SendMessage to the same implementer; cosmetic NITs are surfaced, not looped. Conflicting pairs (#507↔#508 on slskd_transfers, #509↔#510 on download.py) sequenced in waves; the second of each pair merges origin/main and re-runs the FULL suite on the COMBINED tree before merge (both-green-apart ≠ green-together).

**Deploy footgun hit + fixed:** the deploy blocked on a pre-existing latent wrapper break (cratedigger 604da00 beets option consolidation, undeployed since v2026.07.04) — see [[project_nixosconfig_wrapper_tracks_module_options]].

**Follow-up filed:** #530 (render beets-distance badges for Discogs siblings — UUID-anchored `/api/beets-distance` regex). De-duped: the test-order flake surfaced = existing #511; the pyright-crawls-worktrees noise = existing #520.
