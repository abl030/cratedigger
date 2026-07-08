# Generated (property-based) testing

Issue #548. Hypothesis-driven generated tests assert **policy invariants**
over large generated state spaces instead of hand-picked examples. They are
ordinary members of the unittest suite — no separate runner, no seed
bookkeeping, no standing service.

## Modules

| Module | Target | Properties |
|--------|--------|------------|
| `tests/test_quality_generated.py` | the decision twins (`full_pipeline_decision` / `full_pipeline_decision_from_evidence`) | decisions are definitive (totality); raw verified-lossless FLAC never replaced by lossy; transparent lossy never accepts obvious downgrades; **twin parity** over the shared world language; evidence-only integrity facts (corrupt / bad-hash / nested / mixed) always reject in priority order; incomplete evidence fails closed |
| `tests/test_evidence_generated.py` | `ensure_current_evidence_for_action` | converted current evidence requires source V0 (fix `6cf26a4`): never `loaded` without a V0 metric, scalar state backfills, otherwise fail closed |

Every generated module also carries **known-bad self-tests** proving each
invariant checker trips on a planted violating decision — the RED/GREEN
guarantee that the harness detects what it claims to.

## Two tiers, one knob

`tests/_hypothesis_profiles.py` registers two profiles, selected by
`CRATEDIGGER_HYPOTHESIS_PROFILE`:

- **`suite`** (default) — deterministic (`derandomize=True`, no example
  database), bounded examples. Runs on every `scripts/run_tests.sh`,
  identical on every machine. This is the merge gate.
- **`fuzz`** — randomized burst for local exploration. Fresh entropy per
  run, big example budget, local example database (`.hypothesis/`,
  gitignored) so found failures replay first on the next burst,
  `print_blob=True` for exact reproduction.

Run a burst whenever quality policy changes:

```bash
nix-shell --run "CRATEDIGGER_HYPOTHESIS_PROFILE=fuzz \
    python3 -m unittest tests.test_quality_generated tests.test_evidence_generated -v"
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

## Writing new generated tests

- Strategies generate anything the **schema** can express — no plausibility
  filters. The V0-evidence bug lived in a state a plausible-worlds-only
  generator would have skipped. If a state is truly impossible, fail-closed
  handling of it is itself the invariant.
- Invariant checkers are module-level functions so a known-bad self-test
  can prove each one trips.
- Import `tests._hypothesis_profiles` for the side effect before using
  `@given` — that is what wires the module into the suite/fuzz tiers.
- Reuse the shared fakes/builders (`tests/fakes/`, `tests/helpers.py`)
  per `.claude/rules/code-quality.md`; leaf-seam mock rules apply to
  generated tests like any other test.
