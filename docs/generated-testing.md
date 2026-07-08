# Generated (property-based) testing

Issue #548. Hypothesis-driven generated tests assert **policy invariants**
over large generated state spaces instead of hand-picked examples. They are
ordinary members of the unittest suite — no separate runner, no seed
bookkeeping, no standing service.

## Modules

| Module | Target | Properties |
|--------|--------|------------|
| `tests/test_quality_generated.py` | the decision twins (`full_pipeline_decision` / `full_pipeline_decision_from_evidence`) | decisions are definitive (totality); raw verified-lossless FLAC never replaced by lossy; transparent lossy never accepts obvious downgrades; **twin parity** over the shared world language; evidence-only integrity facts (corrupt / bad-hash / nested / mixed) always reject in priority order; classification layer (`classify_full_pipeline_decision` / `evidence_decision_name` — cleanup eligibility) coherent with every generated decision; incomplete evidence fails closed |
| `tests/test_evidence_generated.py` | `ensure_current_evidence_for_action` | converted current evidence requires source V0 (fix `6cf26a4`): never `loaded` without a V0 metric, scalar state backfills, otherwise fail closed |
| `tests/test_slskd_events_generated.py` | `ingest_download_file_events` (event stamping — the ONLY source of completed-file locations) | stamping oracle (newest decodable event per key in the new-events window, nothing else); totality + exactly-once over wild feeds (dup ids, garbage timestamps, undecodable payloads, pruned/absent cursors, rows leaving `downloading` mid-ingest); duplicate-id invariance (mid-pagination shape) |
| `tests/test_request_lifecycle_generated.py` | `transitions.finalize_request` + `supersede_request_mbid` driven as a `RuleBasedStateMachine` over random operation sequences | statuses stay in the legal set; `replaced` rows are terminal and byte-frozen; identity (mbid/source/created_at) immutable; every replaced row has exactly one linked descendant; `active_download_state` only on `downloading` rows; the DB guards (claim, downloading→wanted) no-op on ineligible rows |
| `tests/test_multidisc_manifest_generated.py` | `try_multi_enqueue` with the REAL `check_for_match` matcher over generated multi-disc worlds | **the #550 coverage law**: an accepted multi-disc grab has no duplicate transfer identities, one distinct source folder per disc, unique coverage == track count. This harness **found the live #550 defect-#1 bug** (partial-disc manifests) before the production MANIFEST-TRACE window captured it |

Every generated module also carries **known-bad self-tests** proving each
invariant checker trips on a planted violating decision — the RED/GREEN
guarantee that the harness detects what it claims to.

## Three tiers, one knob

`tests/_hypothesis_profiles.py` registers three profiles, selected by
`CRATEDIGGER_HYPOTHESIS_PROFILE`:

- **`suite`** (default) — deterministic (`derandomize=True`, no example
  database), bounded examples. Runs on every `scripts/run_tests.sh`,
  identical on every machine. This is the merge gate.
- **`push`** — quick randomized burst (2k examples) that `scripts/pre-push`
  runs on every `git push`, before the flake check. Fresh entropy per push
  means exploration accumulates over time with zero operator effort; a
  push-found failure is remembered in `.hypothesis/` and replays first in
  dev. Escape hatch, as for the whole hook: `git push --no-verify`.
- **`fuzz`** — deep randomized burst (20k examples) for local exploration.
  Fresh entropy per run, local example database (`.hypothesis/`,
  gitignored) so found failures replay first on the next burst,
  `print_blob=True` for exact reproduction.

Run a deep burst whenever quality policy changes:

```bash
nix-shell --run "CRATEDIGGER_HYPOTHESIS_PROFILE=fuzz \
    python3 -m unittest discover -s tests -t . -p 'test_*_generated.py' -v"
```

It is pure and safe: no prod DB, no slskd, no beets, no network; the only
filesystem writes are per-example tempdirs and the local `.hypothesis/`
database. Repeat runs add entropy; there is nothing to resume and no seed
cursor — coverage grows by improving strategies and invariants, not by
consuming more seeds.

## Promotion policy — failures become named tests, not artifacts

When the fuzz tier finds a real failure, Hypothesis **shrinks** it to a
minimal world and prints it (plus a `@reproduce_failure` blob). Promote it:

1. Reproduce and fix (or conclude the invariant/world-mapping is wrong and
   fix the test — say which in the commit).
2. Commit the minimized world as a named `@example(...)` pin on the
   property, or as a full named scenario in the album test set
   (`tests/test_quality_classification.py::TestLiveBugReproductions` + its
   evidence-pipeline parity twin) when the shape deserves prose.

Never check in opaque failure artifacts (JSON corpora, seed logs). They
freeze `asdict()` snapshots of dataclasses that churn, and a seed's meaning
changes whenever a strategy changes — named examples evolve with the schema
and stay readable.

## The parity property

`TestGeneratedParity` machine-checks "quality decisions live in ONE place":
for every world expressible in the twins' **common language**, the simulator
twin and the production evidence decider must produce identical outcomes.
The world→evidence mapping is the same shared builder the hand-written
parity tests use (`tests/helpers.py::build_parity_candidate_evidence` /
`build_parity_current_evidence`), so a divergence can't hide behind two
different encodings. The common-language constraints (candidate V0 probes
only on FLAC candidates, derived `is_vbr`, explicit conversion facts) are
documented at the strategy definition.

## Coverage steering — measure before generating more

Random generation from a fixed strategy saturates: after the first burst,
more examples stop buying new behavior. When deciding where the *next*
property should aim, measure which branches the generated tests actually
execute (this is the functional-coverage idea from CPU verification —
steer generation at the holes, don't bookkeep seeds):

```bash
# Scope --source to the production code the properties target — this
# example covers the decision twins; use --source=lib to also see
# lib/slskd_events.py and lib/import_evidence.py (module files need the
# package dir, not the .py path).
nix-shell --run "CRATEDIGGER_HYPOTHESIS_PROFILE=fuzz \
    coverage run --branch --source=lib/quality \
    -m unittest discover -s tests -t . -p 'test_*_generated.py' \
  && coverage report --show-missing"
```

Read the misses critically: config parsing, wire helpers, and
simulator-only shims belong to other tests — the actionable holes are
unexecuted **decision-policy** branches. (The 2026-07-08 run found exactly
one: the classification layer, now covered by
`test_decision_classification_is_coherent`.)

## Qualifying the harness — fault injection

"Does this suite actually constrain the code?" is an empirical question,
answered the way hardware verification qualifies a testbench: **plant
mutants in production code and count kills** against the generated tests
only. How to pick mutants:

- revert a real past bug fix (the strongest single check);
- break each adapter derivation the parity property claims to pin;
- flip decision comparisons; remove early-exit guards and readiness gates;
- for each property, plant the exact violation it claims to catch.

Interpret results per mutant: **killed** = the property works; **killed
only at push/fuzz entropy** = suite-budget miss, acceptable because the
pre-push hook runs the entropy tier on every push; **survived all tiers** =
either a missing invariant (add it, with a known-bad self-test) or a world
the strategies rarely make decisive (pin the decisive world as an
`@example`). The mutation driver is an operator/agent one-shot — never
committed (`.claude/rules/scope.md`); record the kill matrix in the
issue/PR.

Canonical run (issue #548, 2026-07-08): 13 mutants — including reverting
fix `6cf26a4`, which the generated lifecycle property killed independently
of its hand-written regression tests — 10 killed outright, 1 at push-tier
entropy, 2 survivors fixed in PR #555 (`assert_below_gate_never_stops_search`
and the `_SPECTRAL_OVERRIDE_DECISIVE_WORLD` parity pin).

## Writing new generated tests

- **Invariants come first.** New features with a generated-testable surface
  (pure decisions, lifecycles/state machines, wire or event ingestion)
  write their policy invariants down in the issue/plan before
  implementation, and ship the generated properties in the same PR — see
  `.claude/rules/code-quality.md` § "Testing — Red/Green TDD".
- Strategies generate anything the **schema** can express — no plausibility
  filters. The V0-evidence bug lived in a state a plausible-worlds-only
  generator would have skipped. If a state is truly impossible, fail-closed
  handling of it is itself the invariant.
- Invariant checkers are module-level functions so a known-bad self-test
  can prove each one trips. Every checker owes one — a property that has
  never failed anything is unfalsifiable until proven otherwise.
- Import `tests._hypothesis_profiles` for the side effect before using
  `@given` — that is what wires the module into the suite/push/fuzz tiers.
- Reuse the shared fakes/builders (`tests/fakes/`, `tests/helpers.py`)
  per `.claude/rules/code-quality.md`; leaf-seam mock rules apply to
  generated tests like any other test.
