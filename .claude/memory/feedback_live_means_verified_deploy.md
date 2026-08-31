---
name: live-means-verified-deploy
description: Memory must say "merged" until the deploy chain verifies; #1293/#1294 were recorded "live" while undeployed
metadata:
  type: feedback
---

2026-08-31: session memory recorded #1293/#1294 as "live" when they were merged-but-undeployed - the nixosconfig lock still pinned #1292's revision. The next deploy's drop-detector (deploy skill step 2, the #1203 pattern) caught and swept them.

**Why:** "live" written at merge time poisons later sessions - they skip verification, misread the deployed baseline, and attribute prod behavior to code that is not running.

**How to apply:** write "merged" after merge; write "live"/"shipped" only after the deploy chain verifies (fleet anchor + migrate invocation + source grep + verified cycle). When a memory says "live", the nixosconfig flake.lock pin is the authority, not the memory. Related: [[1278-wx3-dashboard-composer]].
