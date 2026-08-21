---
name: feedback_alert_blast_radius
description: Don't fix a silent-failure path by making every consumer louder — enumerate the failure cases and fix only the broken one
metadata:
  type: feedback
---

When a notification path silently swallows a failure, do NOT "fix" it by paging
unconditionally. On #1233 I proposed making the shared
`nixosconfig/modules/nixos/lib/negative-alert.nix` page Gotify on every alert
instead of only when the RCA webhook POST fails. The operator's reply: **"no i
hate it, you've instantly doubled my alert fatigue."**

That module is shared by seven consumers (domain-monitor, komga-sync, kopia,
ops-sync, gwm-archiver, alert-bridge, cratedigger-daily-checks). The fix would
have made all of them louder to repair one silent path.

**Why:** the RCA-agent indirection exists precisely to absorb noise — an agent
triages and only pages when it matters. Unconditional paging deletes the
feature along with the bug.

**How to apply:** enumerate the failure cases first and fix only the broken one.
On #1233 there were three — (1) Hermes down -> POST fails -> existing fallback
already works; (2) Hermes ACKs then dies internally (HTTP 429 from its LLM
provider) -> the only broken case; (3) Hermes handles it -> working as intended.
Case 2 was entirely observable inside Hermes, so it belonged there, not in the
shared caller. Accepted answer: Hermes self-pages, without AI, on its own
terminal RCA failure.

Corollary the operator also endorsed: prefer **paging on state transition, not
on state** (green->red, or a changed failure signature), so a repeated identical
failure produces one page rather than one per night — the same new/changed-member
model the world-audit debt gate already uses.

Related: [[feedback_measured_evidence_only]], [[feedback_no_homelab_specific_process_rules]].
