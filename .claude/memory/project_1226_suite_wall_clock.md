---
name: project_1226_suite_wall_clock
description: "#1226 CLOSED 2026-08-20: every lib.nixosSystem in tests needed `nixpkgs.pkgs = modulePkgs` — 20 redundant nixpkgs instantiations per eval; suite wall -10.4%"
metadata:
  type: project
---

2026-08-20, PR #1228 (merged `32a0f024`), issue #1226 closed, residuals in #1229.

**The win nobody had looked for.** Every `lib.nixosSystem` in `tests/test_nix_module.py`
and `tests/test_web_auth_mode_generated.py` let the NixOS `nixpkgs` module build its
OWN pkgs instance, instead of reusing the one the expression's preamble already built
for `beetsPackage`. Adding `{ nixpkgs.pkgs = modulePkgs; }` as each system's first
module (12 sites) cut the three heavy evals 54-63% with **byte-identical**
`nix eval --json` stdout. `flake.nix` already used this idiom in `runtimeSrcPin` /
`packageSetPin` / `moduleAssertions` — the test expressions were the odd ones out.
**When a repo's own flake does something the tests don't, that asymmetry is the lead.**

**Measure the assumed-expensive thing first.** #1156/#1131 spent four PRs splitting
these evals to amortise a shared preamble measured at... 0.19s. A bare `lib.nixosSystem`
reading `.config.assertions` is 0.21s marginal; each real world was ~1.4s. The ~1.2s gap
was the per-world nixpkgs instantiation, invisible to every previous round because
nobody measured the preamble in isolation.

**Regime change is the real finding.** Removing the pole moved the phase from
pole-bound to throughput-bound. After that the largest target (47.9s) sits under the
perfect-packing floor (1673 core-seconds / 22 workers = 76.0s), so *no further
splitting of any target can move the phase*. Diagnose the regime before picking a lever.

Measured and REJECTED (don't redo these):
- **More workers**: 26 workers inflated total core-seconds 11% (1673->1860) for 2% wall.
  doc1 is CPU-saturated at 22. The "headroom" #1131 anticipated does not exist.
- **Sharding heavy targets**: `test_aac_lattice` costs 120.2s at method granularity vs
  30.9s as one target — those modules rebuild per-process fixtures. Sharding buys
  packing and pays far more. Always check per-test cost before sharding a module.

**Scheduling**: `schedule_modules` orders non-frontloaded modules by `_line_weight`
(LINE COUNT). Replaying a 464-target duration map: perfect 76.0s, actual order 88.1s,
LPT 76.2s — ~12s of pure ordering loss. Frontloading 4 measured-heavy modules got
89.6s->82.6s. Deliberately did NOT build a per-target cost table (464 drifting numbers
for the last ~5s).

Result (doc1, balanced ABBA, full `run_tests.sh`): suite wall 115.5s -> 103.5s (-10.4%);
python phase 109.9s -> 98.2s; standalone phase 98.6s -> 82.6s (-16.2%).

Technique worth reusing: replay a real per-target duration map through the REAL
scheduler to simulate makespan under alternative orderings — it predicted 81.5s vs a
measured 82.6s, so scheduling changes can be evaluated without a suite run each.

Also measured: `nix-shell` entry is ~5.2s and is ALL Nix evaluation (`nix-instantiate`
alone is 5218ms), paid once per suite run — 0 tests spawn a nested nix-shell. See
[[project_1156_suite_perf_complete]].
