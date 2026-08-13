---
name: 1077-wrong-match-routing-complete
description: Issue #1077 shipped/deployed/closed 2026-08-12 — Wrong Matches routing rules, D10 sweep of 83 GiB, and the review-round lessons
metadata:
  type: project
---

Issue #1077 CLOSED 2026-08-12 (PR #1121, nixosconfig pin `123dca48`, cycle
`cf263364` verified). The settled rules (authority: decisions comment D1–D10 on
the issue): kept ⟺ banned, kept ⟹ visible, delete only under proof
(reducer redundancy / verified-lossless-parent / proven-corrupt bad rip);
lane-B world failures ban + keep + show; no-folder failures never ban;
delete-eligibility is the allowlist {extra_tracks, high_distance,
mbid_not_found, no_choose_match}; force-import success consumes its source.
Events table: `docs/rejection-routing.md`.

D10 one-shot deleted 228 unreferenced quarantine folders / 83.10 GiB across
all four roots (slskd pair held exactly the issue's 158; processing pair held
70 more incl. the DICE force-import leftovers); the 4 worklist-referenced
folders survive. `pipeline-cli triage quarantine` scans ONLY the slskd roots —
the processing-root blind spot is #1122 item 1.

**Why:** future Wrong Matches work must not re-derive these decisions, and the
DICE arc is the canonical proof the cleanup reducer's evidence-based authority
is correct (kept while library inferior, deleted only post-import) — do not
re-litigate binding deletion to denylist state. The `source_denylist` is
permanent per (request_id, username), NOT a cooldown (`user_cooldowns` is the
separate timed table); I shipped a false "expiring cooldown" claim to the
issue before the operator corrected it.

**How to apply:** when touching rejection routing, start from
`docs/rejection-routing.md` + the D1–D10 comment; remember the auto lane
persists `scenario='strong_match'` (real reason in `import_result->>'decision'`).
Review lesson from 4 rounds: filesystem movers on the reject lane must
complete-the-world (audit row + ban + requeue) regardless of FS errors — never
raise post-mutation; and property oracles must call the production predicate
(`wrong_match_row_is_visible`), not a proxy field. Residuals tracked in #1122.
