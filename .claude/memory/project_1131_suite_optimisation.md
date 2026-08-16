---
name: project-1131-suite-optimisation
description: 2026-08-14 #1131 closed — 7 PRs cut the canonical suite ~30% on doc1; poles were ceremony subprocesses, and raising workers FIRST made it worse
metadata:
  type: project
---

2026-08-14: issue #1131 closed. PRs #1147, #1150, #1148, #1152, #1151, #1154, #1155.
doc1 canonical suite 148-189s -> 107-126s; python phase 128-165s -> 103.0s; workers 12 -> 22
(`recommended_worker_count` is now `cpu*3//4`, no ceiling). epi: 245s -> 196s at 12 workers.

- **Order matters and the obvious order is wrong.** Raising `--jobs` BEFORE cutting the poles
  made doc1 worse: 230s -> 295s at 24 workers, RAM root 96% full, 3 targets broken, because one
  target (`test_nix_module`) was 60-80% of the phase and more workers just multiplied concurrent
  nixpkgs evaluations. After the poles were cut the phase became work-bound and more workers paid.
  Diagnose floor-bound vs work-bound before touching a worker count.
- **The poles were ceremony, not work.** Nested `nix-shell` re-entering the shell already active
  (six full Nix evals per run); a full MDCT/FFT AAC-lattice measurement whose value no assertion
  reads; ~28 Python interpreter startups per fake `git` shim call; the same NixOS worlds evaluated
  five times. Profile before assuming the cost is real work.
- **RAM was the enabler.** Disposable PG clusters ran stock settings on tmpfs. Trimmed to
  datadir -61% / `pg_wal` -90% (`wal_level=minimal`, tiny WAL ceiling, `shared_buffers=16MB`,
  `fsync=off`, `initdb --no-sync --wal-segsize=1`) with no wall-time cost. That is what made 22
  workers fit doc1's 3.2 GB root. **`max_connections` must exceed
  `run_world_model_burst.IN_PROCESS_JOB_CAP`** — 20 broke the nightly burst and no test covered it.
- **`nohup` changes which OS worlds a test can construct.** `SIG_IGN` and blocked-signal masks are
  inherited across fork+exec and CPython's `subprocess` clears neither (bar
  SIGPIPE/SIGXFZ/SIGXFSZ via `restore_signals`). A fixture doing `kill -1 $$` silently no-ops under
  `nohup`, exits 0, and the property asserts a world that never happened (#1154, latent since the
  file was written). SIGKILL/SIGSTOP are the only signals POSIX forbids ignoring or blocking — a
  fixture that needs a signal delivered must use SIGKILL. Relaunching a gate under `nohup` after a
  dropped SSH pipe is how this surfaced.
- **Test-infra changes are deterministic-only** (`code-quality.md`): no pin+property pair, no
  Hypothesis, for runners/schedulers/fixtures. Briefs must state this or implementers add one.
- **Five of seven PRs shipped a false claim caught only by the next independent read**, including
  two of mine as orchestrator: an unverified contention number that propagated into a code
  docstring as the justification for a design choice, and a suggested correction naming `SIGXFZ`
  (nonexistent on Linux). Never relay a measured number into a brief unverified
  ([[feedback-orchestrator-briefs-become-defects]]); every correction round needs a verification
  review.
- Residuals: sustained utilisation ~15-17/30 cores, not 25/30 — next lever is splitting
  `webAuthMatrix`'s 39 worlds (~65s of `test_nix_module`'s ~100s) to drop the floor to ~35-40s;
  headroom floor bounds admission not peak; no RAM guard at all (OOM shows as `BrokenProcessPool`
  classified as an ordinary test failure); epi's knee is ~16 workers.
