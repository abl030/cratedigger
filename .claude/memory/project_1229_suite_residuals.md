---
name: project_1229_suite_residuals
description: "#1229 CLOSED 2026-08-20: nix develop hits Nix's eval cache (5.2s->0.5s); LPT queue ordering from cached timings; gate 109s->93s; serial overhead is worth 22x per-test CPU"
metadata:
  type: project
---

2026-08-20, PR #1231 (merged `05ae4592`), issue #1229 closed. Follows [[project_1226_suite_wall_clock]].

**The ranking error worth remembering.** #1229 ranked its items by core-seconds,
which made per-test savings look comparable to serial/packing ones. They are not:
at 22 workers, **a 5-core-second test saving is 0.23s of wall; a 5-second serial
saving is 5 seconds — ~22x.** Three of the six items were per-test CPU and worth
~1s combined; the two that mattered were packing and serial, worth ~9.5s. Classify
a suite saving as serial / packing / per-test CPU BEFORE estimating its value.

**`nix develop` hits Nix's own eval cache; `nix-shell` cannot.** `flake.nix`'s
`devShells.default` IS `./nix/shell.nix`, so the environment is identical, but only
the flake path evaluates a LOCKED flake. Measured: `nix-shell --run true` 5181ms vs
`nix develop --command true` **481ms** warm on a clean tree (2824ms if a TRACKED
file is dirty; 490ms with only untracked changes — untracked files aren't part of
the flake source). `scripts/test.sh` and `run_final_gate.sh` now use it.
The planned fix was to hand-roll a `nix print-dev-env` cache with a custom
invalidation key; the real fix was **to stop bypassing the cache that already
existed**. Look for that shape.

**Queue ordering: frontload MEMBERSHIP is saturated.** Replaying a real 464-target
duration map through the real scheduler, every membership variant lands 81.0-82.3s
against an LPT bound of 76.2s. Closed the gap with a per-target duration cache in
the private runtime tmpfs (merged, not replaced, so `--test` partial runs refine
it). Cold 79.3s -> warm 73.7/75.2s. It is a HINT: `assert_exact_target_schedule`
compares by name as a SET, so ordering can never change coverage.
Trap found by its own test: unknown targets must sort as `math.inf`, not
`max(known)` — a stable sort keeps ties in incoming order, so `max` left unknown
targets BEHIND the dearest known one.

**Measured and rejected — do not redo:**
- Pyright thread count is already optimal: 8 threads -> 108s wall (pyright grows to
  43.8s, LENGTHENING the overlap window), 16 -> 119s, current 12 -> ~103.5s.
  Window length dominates instantaneous oversubscription.
- Per-child hypothesis import: only 259 of 422 test modules would skip it (163
  import it anyway) -> 1.35s ceiling.
- deploy-pin's ~1680 shim spawns: already at the Python floor (7.8ms; `-I` is
  faster but implies `-P`, which breaks `import _shim`) -> 0.6s wall.
- beets_destructive: child import floor 0.046s, all 24 spawns ~1.4 core-s. The
  2-3s per cell is the real beets work.

Result: gate wall **109s -> 93s** (97/95/93 across three passing gates). The suite
is now ~74s of irreducible work plus ~12% deliberate process isolation; the
remaining lever is cores, not code.

Also fixed: `TestMeasureTempdirAvailableBytesTracksDiskUsage` measured free bytes on
the tmpfs root SHARED by 22 workers, so a sibling freeing space offset its payload
(real gate failure). Retried, not loosened — the `.total` mutant never moves.
