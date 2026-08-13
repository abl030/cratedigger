---
name: feedback-heavy-gates-serial-on-epi
description: doc1 OOM'd running suite+moduleVm concurrently; run heavy gates serially, prefer epi (wake via `epi` alias, ssh epi, 64GB, no contention)
metadata:
  type: feedback
---

Running the canonical suite (~12 PG clusters) CONCURRENTLY with the moduleVm QEMU check on doc1 OOM'd the whole box (2026-08-13), killing the Claude session repeatedly — which read as "harness restarts" and provoked relaunch-stacking, including a setsid'd suite that survived its parent and kept grinding a thrashing host.

**Why:** doc1 has neither the RAM nor isolation for two heavy gates plus parallel sessions.
**How to apply:** heavy gates run SERIALLY, one at a time. Prefer epimetheus: `wakeonlan 18:c0:4d:65:86:e8` (alias `epi`), `ssh epi`, clone must be FULL (`git fetch --unshallow` — a shallow clone breaks test_decision_corpus_export's `git archive` of historical base commits), `loginctl enable-linger abl030` before detached runs (suite lives on /run/user/1000). epi has no /dev/kvm: moduleVm runs TCG (~25 min, and see issue #1130). Never auto-relaunch a died gate — diagnose why it died first; repeated harness death IS the memory-pressure signal. Subagents must run gates in the FOREGROUND — backgrounded suite runs die silently with harness restarts and the agent waits forever on a completion notification that never comes.
