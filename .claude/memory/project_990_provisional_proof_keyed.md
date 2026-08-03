---
name: 990-provisional-proof-keyed
description: "2026-08-03: #990 shipped — provisional-lossless lane entry keys on proof absence (will_be_verified), killing the request-2066 equal-copy churn; V5/L5 invariant amended on the record"
metadata:
  type: project
---

2026-08-03: Issue #990 / PR #991 merged + deployed (nixosconfig `759c49cd`,
cycle `54e0ceb2`), issue closed with live proof: dl 39207's candidate evidence
vs request 2066's current evidence decides `suspect_lossless_downgrade` on the
deployed decider — the 95-download equal-copy churn is dead.

Non-obvious lessons this session proved:

- **The as-persisted decision-differential arm caught what pins, fuzz, and
  review round 1 all missed** (40 pre-PR3-proof rows re-routed): for ROUTING
  changes run BOTH arms — the docs framed the counterfactual arm as the
  revealing one, which holds only for proof-GATE changes. Encoded as #993
  item 2.
- **Amending a shipped generated invariant needs a citable issue-comment
  decision BEFORE implementing** (V5/L5 "leg never selects the lane" → "leg
  reaches the lane only through the proof"); the old decoy self-test survived
  scoped to the V0-override core, and the pre-fix production router itself
  became a new known-bad decoy.
- Genuine grade ≠ proof: the churn cohort was genuine-by-a-knife-edge
  (suspect_pct exactly 50.0) plus ultrasonic-denied (71.3 dB vs 59.5), living
  in the grade-keyed/proof-keyed gap PR3 created.
- Operator ack'd at merge: no-anchor unproven imports may displace a
  better-measured anchor-less copy; the anchored probe-less reject denylists
  the peer. Follow-ups ranked in #993 (measure that cohort before changing
  policy).
