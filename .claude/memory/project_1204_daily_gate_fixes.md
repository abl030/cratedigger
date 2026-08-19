---
name: 1204-daily-gate-fixes
description: "#1204 COMPLETE 2026-08-19: SELF-gate clamp licence + moduleVm timer quiesce shipped; run the canonical gate from a CLEAN worktree, never the shared checkout; OOM-killed suites leak cratedigger-tests.* scratch that poisons later runs"
metadata:
  type: project
---

2026-08-19: issue #1204 CLOSED — both daily-gate reds fixed, deployed (nixosconfig `47a279fb`, cycle `fc218835` verified), PRs #1205 + #1206 merged.

- **PR #1206 (quality core):** `_one_sided_spectral_bitrates` now licenses on the classed side's own consumed label via ONE vocabulary (`_family_from_label`) — the SELF gate. The originally planned CROSS gate (raw side's interpreted family) was proven fail-open on the R19 converted-lineage cohort by independent review and dropped via an issue-comment invariant amendment. Live differential: 0 changed rows / 17,230 decided, both arms.
- **PR #1205 (dev gate):** moduleVm quiesces `cratedigger-unfindable.timer`+service through the pg_dump snapshot phase, re-quiescing after each `switch-to-configuration` call (reconciliation restarts stopped timers), with a permanently backdated stamp (deterministic eager-timer world) and a fail-closed timer+service guard in `_pipeline_data_snapshot()`.

**Operational lessons (cost ~2h of failed deploy gates):**

- **CORRECTED 2026-08-20 (#1208 item 3): the worktree pile was a bystander.** The shared-checkout gate blowups (3.5 CPU-hours / 25.5G / kernel OOM vs ~150s clean) were stray `.nixpkgs-src` symlinks under `docs/research/calibration-data/*` — planted by nix-shells entered from subdirectories (July #829 calibration) — which the root-anchored pyright excludes could not see, so pyright analyzed the entire nixpkgs Python corpus. Fixed: shellHook now anchors GC roots at the repo top level; pyright excludes are `**/`-globbed and cover nix `result*` links. See [[repo-walkers-exclude-claude-worktrees]].
- **An OOM/SIGKILL-killed suite leaks its `cratedigger-tests.*` scratch tree on the 3.1G RAM root forever** (reaper prefix gap, #1208 item 1), and the debris then deterministically fails `test_a_real_worker_exception_is_classified_by_live_measured_headroom` on every later run while passing in isolation. Check `du -sh /run/user/1000/cratedigger-tests.*` before diagnosing that test as a code defect; `rm -rf` the dead trees.
- **Verify a suspect "orphan" process's parentage before killing it** — this session killed the beets-tip canary's suite (its ExecStart PID owned the process group) after misattributing it to a dead gate run; the canary's 2026-08-19 failure is that, not a Beets-tip regression.
- **After an OOM-killed suite, let swap drain before retrying** — each immediate retry inherited ~14G of undrained swap and thrashed into the next OOM.
- Review-ladder note: both blocking design errors in this series (CROSS gate fail-open; wrong clock-excursion mechanism in the issue body) originated in the orchestrator's own verified-looking RCA and were caught only by fresh uninvolved reviewers — [[orchestrator-briefs-become-defects]] held again, in the planning layer.
