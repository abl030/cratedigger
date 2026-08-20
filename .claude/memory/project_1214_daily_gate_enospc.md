---
name: 1214-daily-gate-enospc
description: "#1214 CLOSED-OUT + #1156 items 2/3/6 2026-08-20: daily-gate ENOSPC was addCleanup-inside-@given leaking one tmpfs world per Hypothesis example; tmpfs is cgroup-charged so it read as a memory problem; 4 PRs"
metadata:
  type: project
---

2026-08-20: `cratedigger-daily-checks` failed on tmpfs ENOSPC (fuzz burst aborted
1368/4586, unit peak 26.4G). Four PRs merged — #1215 (root cause), #1216 (#1208
items 1+4 reaper), #1217 (monitor), #1218 (#1156 items 2/3/6). Combined `main`
`387fbaea` re-gated green. No deploy: all test-infra/dev-shell/docs.

Durable lessons:

- **`addCleanup` inside a `@given` body fires once per test METHOD; Hypothesis
  re-runs the body once per EXAMPLE.** Every example leaks its resource until
  the method returns. Measured: 2491 live `BeetsContractWorld`s = 1.59 GB from
  ONE test method at 2500 examples; after the fix, 1. 42 direct sites + 3
  helper-mediated. Guarded by `tests/test_given_body_cleanup_audit.py` (bounded
  fail-closed AST audit, two clauses — registered-cleanup AND bare construction
  with no cleanup, which is strictly worse because it strands an
  operator-unremovable `/dev/shm` dir).
- **cgroup v2 `memory.peak` counts tmpfs pages, so a scratch leak presents as a
  RAM problem.** 69% of the fuzz phase's 21.4 G "memory" peak was the tmpfs
  itself. Always split anon vs shmem before blaming processes.
- **Never diagnose from the headline unit peak.** It was flat 19.5-23.8 G for 13
  nights with no trend; the scratch had sat at 79-86% of its 16 GiB ceiling for
  two weeks and finally tipped. #1156's "19.3 GB" was one phase, not the unit.
- **PostgreSQL is NOT the suspect** (measured 42,314,016 B/datadir, flat across a
  full-budget target; 24 clusters = ~1 GB = 7% of the fuzz tmpfs). #1131's diet
  worked. **The real anon pole is Pyright**: 12 workers x ~860 MB = 8.54 GB PSS,
  co-resident with 22 test workers by design.
- **Instrumentation must not store state on the resource it measures.**
  `daily_resource_monitor.sh` kept its state on the tmpfs it sampled, so it died
  of the same ENOSPC — zero telemetry for the one run that overflowed. Fixed by
  fsid comparison (not path convention) against `$XDG_RUNTIME_DIR`.
- **Bare `exec {fd}<&- 2>/dev/null` (no command after `exec`) applies the
  redirection to the CURRENT SHELL permanently** — it had been silently sending
  the script's own stderr to `/dev/null`, so the gate diagnostic had never fired
  in production. Use `{ exec {fd}<&-; } 2>/dev/null`.
- **The scratch reaper must prove death, never infer staleness.** The earlier
  attempt used dir mtime and reaped LIVE trees. Now `.owner` = `"<pid> <ticks>"`
  written by `test_tmpfs.sh`, verified pid-reuse-safe; every ambiguity (missing/
  unreadable/malformed marker, any non-ENOENT `/proc` error) fails closed. Only
  SIGKILL defeats the shell EXIT trap — SIGTERM/INT/HUP all clean up. Trees
  created by a pre-merge shell are markerless and therefore never reaped.
- **`TRUNCATE ... CASCADE` per test costs 19x the wall time of `DELETE FROM`** and
  accrues unreclaimed catalog bloat because ephemeral clusters run
  `autovacuum=off` (measured 2000 cycles: catalog 2.0->12.5 MiB and 11.3 s, vs
  +0.0 MiB and 0.6 s). NOT done — `DELETE` doesn't reset sequences and needs
  FK-correct ordering. Real remaining wall-time opportunity.

Also swept 4.6 GB from `/var/lib/cratedigger-daily-checks/fuzz-failures` (20 run
dirs back to 2026-07-21, never pruned; operator-authorized).

Still open on #1156: items 1 (shard `test_nix_module`'s 39 `webAuthMatrix`
worlds), 4, 5 — all wall-time, deliberately out of scope.

See [[mutation-testing-pycache-trap]] and [[correction-rounds-mint-false-claims]].
