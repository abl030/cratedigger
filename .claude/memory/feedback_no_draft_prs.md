---
name: no-draft-prs
description: Never open draft PRs — a reviewed PR is ready; merge and deploy without stopping
metadata: 
  node_type: memory
  type: feedback
  originSessionId: a5dc7f56-01ae-4fb9-a402-90bafee866f3
---

Never open PRs as drafts (2026-07-11, PR #610: "why are we doing drafts all of a sudden, i never want that, if it's reviewed then its good. merge and deploy").

**Why:** The house pipeline already gates quality before the PR exists — full suite + pyright, screenshot loop for UI, opus review pass with findings fixed. A draft adds a pointless approval round-trip for a single-operator repo.

**How to apply:** After the review loop is clean, `gh pr create` (no `--draft`), then merge via "Create a merge commit" ([[gh-merge-from-worktree-gotcha]] — verify with `gh pr view --json state`, don't retry on exit 1 from a worktree) and run the full deploy sequence ([[forgejo-cutover-deploy-flow]]). Don't stop to ask between review-clean and deployed.
