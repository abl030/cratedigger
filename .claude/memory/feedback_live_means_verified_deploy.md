---
name: live-means-verified-deploy
description: Memory says "merged" until something has actually deployed it; #1293/#1294 were recorded "live" while undeployed. Since 2026-09-09 the nightly roll deploys main, so "merged" is the ordinary end state and "live" needs a deploy.sh run or a next-morning check
metadata:
  type: feedback
---

2026-08-31: session memory recorded #1293/#1294 as "live" when they were merged-but-undeployed - the nixosconfig lock still pinned #1292's revision. The next deploy's drop-detector caught and swept them.

**Why:** "live" written at merge time poisons later sessions - they skip verification, misread the deployed baseline, and attribute prod behavior to code that is not running.

**How to apply:** write "merged" after merge. Write "live" only after `scripts/deploy.sh` returned success (its anchor check is the proof) or after the nightly roll has demonstrably applied it (nixosconfig `flake.lock`'s `cratedigger-src.locked.rev` on Forgejo master, then doc2's `last-verified-rev`). When a memory says "live", the nixosconfig flake.lock pin is the authority, not the memory. Since [[project-deploy-trim-2026-09]] most work is never deployed by hand at all, so "merged" is the normal, complete end state for non-user-facing PRs. Related: [[1278-wx3-dashboard-composer]].
