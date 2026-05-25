---
name: project_converge_operator_authority
description: Converge button uses operator-authority deletion; do not route through cleanup_wrong_match classifier
metadata: 
  node_type: memory
  type: project
  originSessionId: b3b7d4aa-ded4-43ab-8a4b-f9fa0e2a80ae
---

The Wrong Matches **Converge** button (`web/js/wrong-matches.js::convergeWrongMatches` → `web/routes/imports.py::post_wrong_match_converge`) has an explicit operator-authority deletion contract:

1. Walk the release's Wrong Matches rows; split by distance into green (≤ threshold) and unmatched.
2. Enqueue force-import jobs for green rows.
3. **Delete unmatched rows unconditionally** — via `lib/wrong_match_delete_service.delete_wrong_match`, NOT via `cleanup_wrong_match`. The advisory lock + active-jobs safety scaffolding is preserved; the evidence classifier is NOT.

**Why:** The operator has already reviewed the candidates and chosen the green ones. The classifier (which would honour `kept_would_import` and `stale-evidence` skip outcomes) silently keeps rows the operator explicitly asked to delete. That contradicts the converge UX.

**How to apply:** Never route converge deletions through `cleanup_wrong_match` or any evidence-based gate. If you find yourself doing that, re-read the docstring on `post_wrong_match_converge` — it has a permanent ⚠ marker explaining the regression history.

**Regression context (2026-05-17):** Found during #268 follow-up. Converge had been using `cleanup_wrong_match` for the unmatched deletion path, which silently skipped rows the reducer classified as `kept_would_import`. The #268 fix made cleanup's classifier accurate, surfacing the long-standing bug in converge. Fixed by routing through `delete_wrong_match` instead.

Related: [[feedback_never_defer_work]] (the original architecture put the per-row delete behind a "safety" gate; we ripped it out without offering to defer).
