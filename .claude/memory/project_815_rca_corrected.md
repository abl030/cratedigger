---
name: project-815-rca-corrected
description: "Issue #815 COMPLETE (2026-07-22): adoption-proxy RCA, fail-closed fix PR #818 deployed, 423-row one-shot (414 confirmed / 3 fixed / 6 world-broken)"
metadata: 
  node_type: memory
  type: project
  originSessionId: c72155f6-b68d-4393-b1d9-5f106f3dce74
  modified: 2026-07-22T04:02:48.615Z
---

CLOSED 2026-07-22. Corrected RCA (supersedes issue body; full chain in
https://github.com/abl030/cratedigger/issues/815#issuecomment-5040518013): the stale HAVE grade
was the 2026-05-12 adoption proxy (`_persist_spectral_state` wrote the rejected candidate's
spectral as request HAVE state), laundered into evidence row 1649 by the May-16 seeder and
stamped 'measured' by migration 055; #723 exonerated; analyzer drift rejected (MP3 path
unchanged since May 8).

Fix (PR #818, merge f0d431b3, deployed v-source k37h1dw0..., cycle eae99f06 verified):
adoption branch deleted (bail on absent HAVE audit — operator fail-closed doctrine, #762/#723
family); fresh-audit-wins persistence (fill-only-if-NULL guard dropped; R19
preserve + failed-audit fail-soft intact); invariants shipped as pin+property+known-bad pairs
incl. the dl-37742 outcome-flip pin (HAVE genuine/160 → reject vs stale 128 → import).

One-shot (production-code heredoc on doc2, R19-excluded, fingerprint-guarded): cohort 423
linked wanted/downloading installed/'measured' rows measured_at < 2026-07-12 → 414 confirmed
(contamination was real but narrow), 3 corrected, 6 fingerprint-mismatched left untouched
(self-heal via #723 rebuild at next preview; HEAD seeder no longer copies request scalars).

Lessons filed as issue #819: (1) masked-pipe rule generalization (fuzz burst `| tail` masked
exit + buffered output → false completion, double burst); (2) decision-consequence pins must
assert the decided-outcome flip through the real decider (genuine/None routed via
import_no_exist and hid the flip — reviewer disproved implementer rationale empirically).
Request 4351's genuine 192 is unrecoverable (destroyed pre-fix); only re-sourcing helps.
Related: [[project-812-spectral-tie-fix]].
