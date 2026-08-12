---
name: no-homelab-specific-process-rules
description: Don't propose deploy-runbook/rule/skill steps that encode this homelab's specifics — they over-correct on one incident and make no sense to an outside contributor
metadata:
  type: feedback
---

When a near-miss or incident happens on doc1/doc2, do NOT reflexively propose a
new step in `.claude/rules/deploy.md`, `.claude/skills/deploy/SKILL.md`, or a
similar process surface that only makes sense for this deployment. Weigh first
whether the rule would read as sensible to someone else submitting a PR against
this repo.

**Why:** 2026-08-12, issue #1079. A #1075 near-miss (adding `mbsync` to
`REQUIRED_PLUGINS` while the deployment's nixosconfig-owned Beets config had no
`mbsync`, which would have hard-failed every worker on the next switch) led to a
proposed pre-deploy step: run the new revision's config checker against doc2's
rendered Beets config before `fleet-deploy`. The operator closed it unbuilt:
"i thought it would be over-correcting on my specific instance and make no sense
if anyone else had to provide a PR." The in-repo gates (`moduleVm`, the checks
matrix, `examples/`) are the generalizable surface; the deployment's own config
is out-of-repo by design and verifying it is an operator act, not a repo rule.

**How to apply:**
- Repo-carried process rules should be true for any installation of the module.
  Deployment-specific verification stays in the operator's hands (or in chat).
- Prefer strengthening an in-repo gate over adding a runbook step that names
  doc2, `/var/lib/cratedigger/...`, or `~/nixosconfig`.
- This is the process-side corollary of `.claude/rules/scope.md`'s
  "the module is still distributed and other installations exist".
- Contrast with [[single-operator-no-backfill-scripts]]: single-operator removes
  compat/multi-tenant *machinery from product code*; it does not license
  homelab-shaped *process rules*.
