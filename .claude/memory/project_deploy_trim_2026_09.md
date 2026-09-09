---
name: project-deploy-trim-2026-09
description: "2026-09-09 (#1276, #1378): merged main ships on the nightly rolling update; deploy by hand only for user-facing live proof via scripts/deploy.sh; the strict-hold helper, cycle verifier and pin receipts were deleted; the orchestration skills no longer deploy"
metadata:
  type: project
---

The operator decided on 2026-09-09 that deploying is no longer part of shipping:
"i think we just remove deploy from orchestration as it is, and pretty much
anywhere, unless theres some user facing feaature or issue we are fixing there's
no need to deploy all the time, the overnight will handle it. and crucially when
we do deploy we don't need to baby it at all". Both deletions were confirmed:
"yes i agree with 1. delete. 2. a agree, simplify is always better."

Merged `main` is production the next morning: doc1's `rolling-flake-update`
(23:00) pins `cratedigger-src` to the tip and pushes a signed commit to Forgejo,
doc2's `nixos-upgrade.timer` (04:00, up to 1h jitter) applies it, migrations
included, and the daily gate (05:05) runs against that pinned source. To ship
sooner, `scripts/deploy.sh` from the shared checkout on doc1 is the whole
runbook; then check the change itself. Deleted with the ceremony:
`scripts/cratedigger_deploy_hold.py`, `scripts/verify_cratedigger_cycle.sh`,
`scripts/pin_nixosconfig.sh`'s receipt machinery, their tests and fakes, and
the deploy-hold scenarios in the module VM test.

**Why:** every deploy loaded a 40 KB skill and narrated six shell blocks,
waited two timer cycles, and grepped store paths, for a system that already
ships itself nightly. Two things were kept because they caught real incidents:
the exact `--override-input` pin (#1203) and the `last-verified-rev` anchor
check (2026-06-11, the frozen-GitHub-rev rebuild).

**How to apply:** close non-user-facing issues on merge. A migration over live
lifecycle rows (066-class) must apply against a running world; there is no
quiesce hold any more. The mutant runner works on a `git archive` snapshot
under the job tmp dir, never a live worktree. Related:
[[project-1355-register-closed]].
