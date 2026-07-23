---
name: stale-transfer-success-laundering
description: "SHIPPED 2026-07-22 (req 4190 Pieces of Eight): harvest-without-not_before laundered a prior attempt's Succeeded record over the current Errored → false \"Download complete\"; fixed PR #821, deployed + cycle-verified, #820 closed, follow-ups #822"
metadata: 
  node_type: memory
  type: project
  originSessionId: 1b88fe17-3094-4346-abc5-df49a52ea663
  modified: 2026-07-22T05:07:35.194Z
---

2026-07-22 RCA (request 4190, dl 37923, The Pictures – Pieces of Eight, HumDrum), blind-verified by an independent subagent:

- Track 09 errored terminally in slskd ("reported as failed by HumDrum", 0 bytes) mid-cycle; no DownloadFileComplete event, file never on disk.
- `harvest_terminal_transfer_evidence` (`lib/download.py:569-570`) matches with un-guarded `match_transfer` (no `not_before`), over an `includeRemoved=True` snapshot (`lib/slskd_transfers.py:487-506`); `_transfer_priority` (`:414-425`) prefers success over recency → the May-18 attempt's `Completed, Succeeded` record for the SAME (username, filename) shadowed today's `Completed, Errored` and was stamped into `active_download_state`.
- Poll path's `match_transfer_for_attempt` (`:465-484`) filters only the single ranked-best candidate and returns None instead of falling back — so it never surfaced the genuine errored record either. Both seams had to line up; fixing either prevents it.
- Reducer then saw 12× Succeeded → `complete` → materialization demanded event-stamped local_path for all 12 → EVENT-PATH MISSING for 3600s → outcome=failed, reset to wanted, NO cooldown/denylist → loop can repeat every time the same peer is re-picked while its stale success record exists.
- FIXED + DEPLOYED 2026-07-22: PR #821 (merge 1727e17a) — filter-before-rank in `match_transfer_for_attempt`, attempt-scoped harvest via `state.enqueued_at`; 3 invariants as pin+property PAIRs with known-bad self-tests ([[feedback-pin-and-fuzz-pair]]); Opus review zero findings; nixosconfig pin 41f76a43; live cycle verified on the new store. #820 closed; contract-narrowing follow-ups in #822 (make all-history matching a private seam of `match_transfer_id`; fail-closed timestampless terminals; cooldown-on-materialize-reset decision).
- Side-finding: May spectral rejects of 4190 wrote no denylist rows (banned=[] ×3); the one recorded ban (violenthectarez) has no surviving row — `repair-spectral` (`scripts/pipeline_cli/quality.py:529-535`) deletes spectral denylist rows and plausibly removed it in the #815-era repairs.
- Forensics gotcha: live slskd app dir is `/mnt/virtio/slskd/data/` (microVM virtiofs share); `/var/lib/slskd` is a STALE pre-2026-07-19 copy.
