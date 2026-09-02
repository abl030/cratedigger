# Code Quality Standards

## Quality decisions live in ONE place

**`full_pipeline_decision_from_evidence`** in `lib/quality/pipeline.py` (and its
flat-kwargs simulator twin `full_pipeline_decision`) is the single source of
truth for every importer decision: the five folder/audio-integrity facts
(`audio_corrupt`, `bad_audio_hash`, `nested_layout`, `empty_fileset`,
`mixed_source`) AND
quality (spectral, codec rank, V0 probe, provisional lossless, verified
lossless, transcode detection, quality gate). **Never re-create import
decisions elsewhere.** If a code path needs to know "should this be
imported", it must call the full pipeline — not invent its own narrower
check.

This bit us twice. First (PR #257): a parallel `preimport_decide` spectral
branch fell back to existing container bitrate when spectral evidence was
missing on one side, rejecting legitimate FLAC provisional-lossless upgrades
(request 4514). Second (evidence-canonical-cleanup, U11): `preimport_decide`
still owned five folder/audio-integrity branches alongside the full
pipeline. That asterisk on "quality decisions live in ONE place" was
hair-splitting; the branches were folded into
`full_pipeline_decision_from_evidence` as early exits.

**Preview produces evidence. Importer decides.** The two-worker contract:

- **Preview worker** (`lib/import_preview.py`): measures via
  `measure_preimport_state` (in `lib/measurement.py`), persists
  `AlbumQualityEvidence`, marks the job `evidence_ready` or
  `measurement_failed`. Never emits a verdict. Never decides accept/reject.
  Never writes the denylist.
- **Importer worker** (`lib/dispatch/entry_points.py::dispatch_import_from_db`
  → `lib/dispatch/core.py::dispatch_import_core`):
  reads persisted evidence, decides via `full_pipeline_decision_from_evidence`.
  All rejects route through one helper
  (`_reject_import_from_evidence_decision` in `lib/dispatch/outcome_actions.py`)
  with one denylist policy. The
  five folder/audio-integrity reject reasons are listed in
  `_PREIMPORT_FACT_REJECT_DECISIONS`; that frozenset is the shared generated-
  test taxonomy, not a production router. The dispatch caller owns requeue
  policy. Terminal persistence applies the common quality/search policy while
  preserving operator search state on every non-accepting outcome when that
  state is current as the request row is locked. Every terminal ``wanted``
  transition uses that arbitration, including rejection and local-completion
  bundles; policy fields and attempt/backoff accounting still apply without
  clearing the stop. A successful exact-release terminal acceptance instead
  records ``imported``.
  Authority: "A successful exact-release terminal import acceptance supersedes
  an operator-owned `unsearchable` search stop and records the request as
  `imported`." — https://github.com/abl030/cratedigger/issues/737#issuecomment-5013436918
  CLI/API lifecycle actions retry a stale compare-and-set against the
  post-terminal status so an operator command queued behind the row lock is not
  lost.

**The album test set is the contract.** Live-bug scenarios go in
`tests/test_quality_classification.py::TestLiveBugReproductions` (one test
per real-world album that exercised a quality decision). Every scenario MUST
also be exercised through the production decider via
`TestLiveBugReproductionsThroughEvidencePipeline` — the parity contract is
that the simulator and the evidence pipeline produce the same outcome on the
same album. If you change quality policy, update the album test set first;
the live code follows.

**Red flag phrases that mean you're about to re-invent the decision:** "let me
add a quick spectral check here", "the importer needs to handle this case
upstream", "I'll just compare bitrates before calling the pipeline", "this
gate should reject obviously-bad candidates early". All of these are wrong.
Call the full pipeline.

## Type Safety

# PYRIGHT CLEAN ALWAYS

`scripts/run_tests.sh` owns both complementary typing contracts and they must
have zero errors: whole-repository Pyright through `pyrightconfig.json`, plus
production strict mode through `pyrightconfig.production.json`. One concurrent
`scripts/run_pyright_checks.py` phase runs both contracts and combines their
status without hiding either output. Its hardware-aware allocation is capped at
the measured useful 8 whole-tree + 4 strict threads. Focused file checks provide
faster development feedback but never replace or narrow the canonical suite.
Never accept a "pre-existing" error in either contract.

- Use typed dataclasses (not dicts) for structured data crossing module boundaries
- **No dual-interface types.** Never add `__getitem__`, `.get()`, or `isinstance(x, dict)` dispatch to a dataclass. If a function receives both dicts and dataclasses, that is a type error — fix the callers, not the receiver. Temporary bridges become permanent bugs.
- If a function parameter is untyped and accepts multiple representations (dict or dataclass), type it and fix all callers to pass the correct type
- Inner data structures must also be typed — no `list[dict]` when a dataclass exists

### Typing enforcement

Production passes Pyright strict, enforced by `scripts/run_tests.sh`.
`reportPrivateUsage`, `reportUnusedFunction`, and `reportUnusedClass` stay off
because cross-module private imports are the house convention (PR #775).
Tests remain on the whole-repo standard config because their deliberate
protocol-conformance checks conflict with production strict rules.

`tests/test_typing_ratchet.py` requires the LIVE scanned count of explicit
`Any`, `cast(...)`, `# type: ignore`, and bare `# pyright: ignore` for every
file to match the checked-in baseline (`tests/_typing_ratchet_baseline.py`,
`tests/_tests_typing_ratchet_baseline.py`) EXACTLY — a straight dict-equality
check (`if live == TESTS_TYPING_RATCHET_BASELINE:` at
`tests/test_typing_ratchet.py:181`), not a monotonic comparison. What that
enforces: any escape hatch added WITHOUT regenerating the baseline fails
the test immediately, because the live count no longer equals the
committed one. What no code enforces:
`tests/_typing_ratchet_scanner.py`'s regeneration path
(`render_baseline_module`/`render_tests_baseline_module`) prints whatever the
tree currently contains — it never reads the baseline it is about to
overwrite — so a change that adds an escape hatch AND regenerates the
baseline in the same PR produces a green, growing baseline. "Only decrease"
is a property of the checked-in baseline file's git history that a reviewer
must notice in the diff (the baseline's numbers going up), not a property any
test enforces. Scoped `# pyright: ignore[rule]` is the sanctioned escape
hatch. When a baseline reaches zero, delete its generated baseline/regeneration
path and retain a direct zero-tolerance check; delete the scanner only when a
configured tool enforces the same syntax.

Before adding a new escape hatch in tests, or before widening the tests
baseline to admit one, check the existing typed bridges first
(`tests/dispatch_helpers.py` for the import/dispatch lane,
`tests/helpers.py` for context wiring). The `db=<FakePipelineDB>` kwarg gap used to be the largest single
cluster of frozen tests-side `type_ignore` debt; issue #1277 removed most of
it by narrowing the production annotation instead of bridging the call site.
Measured 2026-08-31, after the follow-up that closed the last genuine
cluster (the #1278 preview-lane series narrowed `measure_preimport_state`
to the `BadHashGateDB` port and deleted the dead `_persist_spectral_state`
writer, removing every fake-vs-concrete `db` hatch — the census's 15
`db=` sites in `tests/test_force_import_gates.py` and
`tests/test_integration_slices.py`, plus `tests/test_measurement.py`'s
positional-`db` ignores and casts): the only `type_ignore` findings still
sharing that shape are deliberate wrong-type injections a narrow port
would not fix
(`_refresh_current_evidence_after_import(db=None)` ×2,
`_check_quality_gate_core(db=SimpleNamespace())` ×1). The whole
`dispatch_import_core` cluster (34 findings, reached through an
`Any`-accepting `dispatch_import_with_fake_db` bridge) is gone: dispatch
now takes `lib/dispatch/types.py::DispatchDB`, a Protocol covering exactly
the methods `lib/dispatch/` calls, and `FakePipelineDB` satisfies it
structurally — so the bridge was deleted rather than reused.

**Narrowing the annotation is the preferred remedy; a bridge is the
fallback.** Two typed bridges exist for this family —
`tests/dispatch_helpers.py::finalize_claimed_dispatch` (its `db` is the
one `Any`-typed seam from a `FakePipelineDB` fixture into the
`PipelineDB`-typed `process_claimed_job`; `job` and `outcome` carry real
types) and `tests/helpers.py::make_ctx_with_fake_db` (typed at the DB
seam: `FakePipelineDB` in, `CratediggerContext` out, the fake wrapped in
`FakePipelineDBSource`; `cfg`/`slskd` stay `Any` on purpose) — and neither
covers the deliberate injections above. So for a new hatch of
this shape: first ask whether the production function actually needs the
concrete `PipelineDB` (usually it does not — ~40 narrow DB Protocols
already exist); if it genuinely does, reuse an existing bridge where the
call site matches one, or EXTEND the bridge set with a new one — never a
bare `# type: ignore`, and never a baseline sweep. The aim for a NEW test
file is zero escape hatches, not a fresh baseline entry.

When either typing ratchet trips, do not stop at making it green. For every
affected file and finding kind, finish with the committed count at least ten
below the baseline at the start of the change, or at zero if fewer than ten
remain, then regenerate the baseline. That obligation is the same on every
trip, whatever caused it: hatches added, hatches removed, or a laundering swap
that nets a decrease. An improvement fails the same exact-equality check an
addition does, so a file improved by one still owes ten below the baseline it
started at, or zero if fewer than ten remain. There is no added-hatches-only
reading and no partial credit. The rule converges the count toward zero; it
does not price the diff.
Authority: "it's not added, it's just always. we should make the wording
explicit here. it's a convergence mechanism, stop trying to find a way out!" —
https://github.com/abl030/cratedigger/issues/1313#issuecomment-5503374358
The ten-below obligation is deliberately a convergence rule,
not another checker: do not add stable-site, diff-aware, or history-aware
machinery to catch delete-and-add laundering (deleting one escape hatch of a
kind and adding a DIFFERENT one of the same kind elsewhere in the same file).
Both ratchet tests return early, silently, ONLY on exact dict equality
between the live scan and the baseline; any other outcome builds
`regressions`/`improvements` lists and calls `self.fail(...)` unconditionally.
That means only the EQUAL-count case of laundering is genuinely invisible to
the check — it hits the same early-return the unchanged case does, because
the (file, key) count is identical to the baseline either way. A laundering
edit that nets a DECREASE is not silent: it fails, with the "escape hatches
removed — tighten the baseline" message, exactly like a real improvement
would — the residual risk there is a reviewer who reads the celebratory
framing and regenerates the smaller number without checking WHICH specific
occurrence changed. Catching either shape for real would need per-SITE
identity (a stable fingerprint for each occurrence, tracked across commits),
not just the aggregate count the baseline already holds — that per-site
tracking is exactly the stable-site/diff-aware/history-aware machinery this
rule forbids building. This is a distinct escape from the upward-regeneration
one above — that one is caught by noticing the baseline's count go UP in the
diff; the equal-count laundering case never shows a diff at all.

## HTTP request bodies — use `pydantic.BaseModel`

Inbound HTTP bodies use a route-local `pydantic.BaseModel` and the shared
`web/routes/_pydantic.py::parse_body` adapter; the canonical example is
`PipelineAddRequest` in `web/routes/pipeline.py`. The adapter owns structured
`ValidationError → 400` responses, and `tests/test_pydantic_route_audit.py`
rejects direct body reads. Pydantic stops at the route: query strings,
responses, and internal types keep their existing contracts. Use
`Field(strict=True)` where JSON booleans must not be coerced.

## Wire-boundary types — use `msgspec.Struct`, not `@dataclass`
Any type that **crosses JSON** — harness stdout, an HTTP response, a JSONB blob written to or read from the DB, a subprocess's stdout — is a `msgspec.Struct`. **Same policy both directions:** encode via `msgspec.json.encode` (or `msgspec.to_builtins` when a dict is needed), decode via `msgspec.convert`. The declared Struct is the single contract that validates type drift at the boundary. Pyright does not see inside `dict.get()` — only runtime validation catches int-vs-str drift, mis-typed fields, or missing required data. This is the lesson of issue #99 / PR #98 (every Discogs validation silently logged `mbid_not_found` because a dataclass said `str` but the wire carried `int`) and the pre-#141 asymmetry (the old "dataclass if re-encoded, Struct if decoded only" split let docstrings lie about which side was strict).

- **Use `msgspec.Struct`** for: harness/subprocess JSON messages, external API responses, DB JSONB rows we read back and type-check, any type that is ever encoded back out to JSON. Reference implementations: `HarnessItem`, `HarnessTrackInfo`, `TrackMapping`, `CandidateSummary`, `ChooseMatchMessage`, `ImportResult`, `PostflightInfo`, `ConversionInfo`, `SpectralDetail`, `AudioQualityMeasurement`, `MovedSibling`, `ValidationResult`, `ActiveDownloadState`, `DisambiguationFailure` (all re-exported from `lib.quality`; defined across `lib/quality/`).
- **Keep `@dataclass`** for: types we construct entirely from our own typed Python code, inputs never crossing JSON (`QualityRankConfig`, `CratediggerConfig`, `DispatchAction`). Their inputs are already typed — the strict boundary buys nothing.
- **Encode symmetrically.** `ImportResult.to_json()` is `msgspec.json.encode(self).decode()`, not `json.dumps(asdict(self))`. Route payloads that need a dict use `msgspec.to_builtins(struct)`, not `dataclasses.asdict(struct)` (which doesn't recurse into Structs anyway). Do NOT re-introduce `asdict` on a Struct — Pyright will let it through and it'll return the Struct instance unchanged, failing at `json.dumps`.
- **Decode at exactly one site.** The wire boundary is the one place the untyped blob becomes a typed object. After that, every downstream consumer works with the Struct directly — no defensive coercion, no `dict.get()`, no re-validation. If you find yourself writing a `_coerce_x` helper on the consumer side, the boundary is in the wrong place.
- **Strict ≠ coerce.** Declare fields as the type you want (`str`, not `str | int`). `msgspec.ValidationError` at the boundary is the detector. Do not use `strict=False` to silently coerce away real type drift.
- **Normalise early if the external source is untidy.** Harness-side `_id_str` in `harness/beets_harness.py` coerces int IDs to str *before* emitting — so the wire is always clean, and the downstream Struct validation never trips in the happy path. Keep normalisation at the source, not at the consumer.
- Decoders already in the repo: `lib/beets.py::beets_validate` via `msgspec.convert(msg, type=ChooseMatchMessage)`; `ImportResult.from_dict` / `ValidationResult.from_dict` via `msgspec.convert(d, type=cls)`.
- Tests owe: at least one RED test that feeds the wrong type at the boundary and asserts `msgspec.ValidationError`. This is the regression guard that makes the boundary worth having.
- **Narrowing an ALREADY-decoded value (not a fresh wire decode) → `lib/json_narrow.py`, never a re-`convert`.** When strict pyright needs a concrete type for a JSONB row, a `to_builtins` result, or a plain untyped dict/list local, use the shared graceful helpers (`json_dict`/`json_list` degrade to `{}`/`[]`; `is_dict_like`/`is_list_like`/`is_container_like` check without narrowing the caller; `is_str_object_dict`/`is_object_list` are the `TypeGuard` narrowers) — or just annotate the assignment target (`x: dict[str, object] = msgspec.to_builtins(...)`; `to_builtins -> Any` is strict-clean and does nothing at runtime). A raw `msgspec.convert(value, dict[...])` mid-data-path is NOT identity — it re-validates and reconstructs, so it can reshape or raise (#804: a re-converted terminal-outcome payload stranded rescue imports at `wanted`). Do NOT hand-roll a private `_json_dict`/`_is_dict_like` copy; #809 consolidated ~10 of them into that one module.

## Testing — Red/Green TDD

- **Start every feature by writing its invariants down.** Before
  implementing, state the policy invariants the feature must uphold, in the
  issue or plan ("replaced rows are frozen", "the event stamp is the only
  location source", "a below-gate import never stops the search"). The
  invariants decide which tests you owe — code follows the tests, tests
  follow the invariants.
- Write tests FIRST (RED), then implement (GREEN). This applies to BOTH
  tiers:
  - **Standard tests** (unit / seam / orchestration / slice, per the
    taxonomy below) — RED reproduction or contract first.
  - **Generated tests** (Hypothesis, `docs/generated-testing.md`) — if a
    production feature has a generated-testable surface (pure decisions,
    lifecycles / state machines, wire or event ingestion), the property +
    strategy ship in the SAME PR as the feature, not as a follow-up. An
    invariant that only lives in prose is not an invariant.
- **A production invariant ships as a PAIR: one deterministic pin AND one
  generated property — same PR.** The pin proves the exact scenario;
  the property patrols the world space around it. Finding and defining an
  invariant and then only pinning it is 90% of the race and then sitting
  down (lesson: PR #560 shipped the #550-phase-2 isolation invariant with
  a deterministic pin only; PR #561 had to retrofit the property — which
  a single-point mutant immediately proved was the load-bearing half).
  Subagent implementation briefs state the pair requirement verbatim and
  never offer a deterministic-only alternative.
- **A regression pin is accepted only with planted-mutant evidence in
  both directions** — the pin must fail on the defect and pass on the
  fix, proven with an actual planted mutant, not asserted from reading
  the pin. Earned three times in the #1088/#1090 series, none caught by
  the suite: a pin whose fixture raised `RuntimeError` where only
  `AttributeError` distinguishes fixed from defective code (inert
  against the exact mutant it named); a test whose name promised a
  True-case assertion its body never made; and an exit-code distinctness
  test comparing a constant to a hand-typed literal.
- **Never property-test the test machinery.** Hypothesis and
  `test_*_generated.py` are reserved for production behavior and
  production-facing operator tools. Test runners and schedulers, suite and
  final-gate coordinators, target selectors, profile wiring, tmpfs helpers,
  fixtures/fakes/strategies, invariant checkers, and static or test-tree
  audits get deterministic tests only. A deterministic integration test may
  construct or invoke a tiny Hypothesis property when the boundary under test
  is Hypothesis integration itself, but that fixture must not be a discovered
  fuzz target or inherit the daily depth. A generated production test may use
  test helpers and fakes; classify the subject being asserted, not its leaf
  dependencies. A regression in test infrastructure gets an exact pin and an
  end-to-end deterministic contract, not a pin/property pair.
- **"Agree by construction" stops at the outermost real adapter, never at a
  shared library function.** Name every claimed surface's outermost real
  adapter. Each named adapter must be exercised by the invariant's
  deterministic pin and generated property through its actual boundary — SQL
  row aliases and a projection, a formatter, or the JavaScript-facing payload,
  as applicable — and must have at least one known-bad mutant at that adapter.
  A mutant at one adapter does not qualify any other claimed adapter. V4 in
  `tests/test_verdict_tiers_generated.py` is the pattern:
  `proof_verdict_from_evidence` and `proof_verdict_from_facts` agree, then
  `web.classify.proof_gate_projection` receives the aliases the browser's
  render path really gets. The adapter is the function production actually
  calls to read or write the value — not a sibling that happens to answer
  a similar question (``_matching_album_ids`` is not
  ``resolve_current_releases``). PR #973 proved why the shared-library
  stop is insufficient: the faulty lineage-gated input
  shape occurred on 26,503 live rows, but `storage_format` covered it, so the
  measured live verdict impact was zero; a generated counterexample proved the
  potential divergence while the common library functions remained in lockstep.
  Do not stop the property at the common function and call that parity, and do
  not answer this requirement with a semantic source scanner; the real
  adapter-driving pin, property, and mutant are the evidence.
- **A decision-consequence pin must assert the decided outcome, not a proxy
  field.** If a pin claims "this world changes what the pipeline decides,"
  it must drive the real decider and assert the *decided outcome*
  (import/reject/requeue) flipping between the bug world and the fixed
  world — a rank, grade, or other intermediate field is not the
  consequence, however plausible-looking. When the flip doesn't reproduce,
  the probe world is wrong (check missing-value routing such as
  `import_no_exist`) — verify empirically before writing a rationale that
  it "can't flip". Lesson (#815 review): an implementer pinned a proxy
  metric (`existing_rank` good→acceptable) because their probe world used
  `genuine/None`, which routed through `import_no_exist` and hid the flip;
  the real fresh-audit value (`genuine/160`) flips `imported` False↔True
  through `full_pipeline_decision_from_evidence` — the actual consequence
  and the strictly stronger regression guard.
- **Every invariant checker owes a known-bad self-test — per CLAUSE, not
  per checker.** A property that has never failed anything is unfalsifiable
  until proven otherwise, and a checker with eight `raise` sites and one
  self-test has proven exactly one of them. Three questions per clause, in
  order. **Q1: does the clause trip at all?** Build the minimal world that
  makes that clause's condition true while every EARLIER clause passes,
  feed it straight to the checker, and assert **that clause's message**
  with `assertRaisesRegex`. Bare `assertRaises(AssertionError)` is not
  proof — `raise`-style checkers short-circuit, so a world violating
  several clauses only ever exercises the first, while the test name goes
  on claiming the rest. **Q2: can the strategy reach that world?** Plant a
  production mutant that makes the condition true through the real path and
  require the generated property to fail. A survivor means the world set
  cannot produce a counterexample — the guard is unfalsifiable rather than
  satisfied, which is how four defects shipped green in the #1063 series.
  **Q3: does the clause stay quiet where production is right?** Q1 and Q2
  both push in the firing direction. A clause reads some of the world's
  dimensions and ignores the rest, and every ignored dimension is somewhere
  production may legitimately answer differently from what the clause
  demands — so name a world where the condition looks true and production
  is correct anyway, feed it to the checker, and assert no violation.
  Deterministic like Q1, no mutant: the defect is in the checker. This is
  the shape #1332 shipped and had to correct in `10fc9f74`: two clauses
  read `world.resolution` and neither read `run.authority_raises`, so four
  producible cells accused correct code. It fails as a nightly-gate
  red on someone else's PR rather than as a green property — 37 violating
  draws in 500 at fuzz depth while the suite tier stayed green by draw
  luck. Keep checkers as module-level functions so the self-test can call them
  directly (pattern: `TestInvariantCheckersTripOnViolations` in
  `tests/test_quality_generated.py`). The remedy for a survivor is to
  **widen the strategy, not delete the clause**: a guard legislates for
  future writers (#859), so a clause no world reaches today may be correct
  fail-closed legislation — `assert_quarantine_verdict_is_earned` in
  `tests/test_path_authority_generated.py` is that pattern. Delete only
  when the world is impossible by construction, and say so. In NEW
  checkers prefer an accumulating `list[str]` of violations over a
  short-circuiting `raise` chain: every clause evaluates, so ordering
  cannot mask one (`mode_selection_violations` in
  `tests/test_web_auth_mode_generated.py`). **Record which tier killed the
  mutant:** `suite` is `derandomize=True`, so a mutant that dies only under
  `fuzz` is not killed for gating purposes — pin that world as an
  `@example` and re-measure. **Standing scope:** a PR adding or changing a
  checker clause audits that checker's clauses as part of the change, and
  records the result — the named world and the killed mutant, per clause —
  in the PR's Fault injection section (`.github/pull_request_template.md`);
  one clause fits in a sentence, a checker with many clauses is a short
  list, one line per clause. The audit examines test
  machinery, so its artifacts are deterministic-only, and its evidence is a
  named world plus a killed mutant — never a scanner inferring reachability
  from source (issue #1094). Procedure: `docs/generated-testing.md`
  § "Per-clause proof".
- **Qualify the harness by fault injection when in doubt.** "Do these tests
  actually constrain the code?" is an empirical question: plant mutants in
  production code (revert a real past fix; break an adapter derivation;
  flip a decision comparison; remove an early-exit guard; target each
  property's claimed coverage) and run the relevant tests against each. A
  surviving mutant is either a missing invariant (add it) or an entropy
  budget miss (pin the decisive world as an `@example`). The driver is an
  operator/agent one-shot — never committed (`scope.md`). Report what you
  tried and what happened in one sentence (a short list, one line per
  clause, when the change is per-clause proof against a many-clause
  checker), in the PR's Fault injection section
  (`.github/pull_request_template.md`) — name the mutant and the
  test, not just "planted a mutant, confirmed RED". This used to be a
  mandatory per-diff-site table; it is a short account now because the
  table is where confabulation happened: PR #1209's matrix claimed "RED at the
  property, not just a pin" for the exact property later proved
  agree-by-construction, while the regression-pin and adapter mutant rules
  above — which caught real defects — stay unconditional, alongside the
  Standing scope per-clause obligation just above them. A mutant that
  kills test A does not qualify test B. Canonical run: issue #548,
  2026-07-08 — 13 mutants, incl. reverting fix `6cf26a4`, led to PR #555.
  Lesson (#1110): the implementer reported reverting each fix and
  watching its own pin go red — true only of `renderConvergeControls`;
  mutants at the three `deleteWrongMatchGroup` restore paths and at
  `removeWrongMatchEntry` survived every JS assertion, and one recreated
  the very dead end the PR existed to remove.
- **Run the catalog breadth pass with mutmut during convergence** (issue
  #1317, operator decision 2026-09-01). Before review handoff on a diff
  with mutable Python production surface: materialize the per-PR
  `[tool.mutmut]` config in your own worktree, run
  `nix-shell nix/mutmut-shell.nix --run "mutmut run"`, fix each survivor
  or dismiss it as equivalent with a one-line rationale, and report the
  machine tally plus the dismissal list in the PR's Fault injection
  section. The tally is machine evidence — the #1209 confabulation shape
  cannot occur for catalog mutants — but each DISMISSAL is a claim the
  review reader audits. The committed tooling is `nix/mutmut-shell.nix`
  alone; the per-run config and `mutants/` are never committed, and the
  aimed-mutant driver above stays an uncommitted one-shot (`scope.md`).
  Catalog breadth does not discharge any aimed obligation:
  the evaluation's replay found real gaps 40 aimed mutants missed AND
  could not express the one real survivor those aimed mutants found
  (`to_json_dict() → {}` is outside the catalog). Full runbook, scoping
  rules, and the real-PG `--max-children 1` requirement:
  `docs/mutation-testing.md`.
- **A log-only branch earns a pin only when the log IS the evidence.** A log
  line earns a pin iff it is the sole operator-visible evidence of a decision
  or failure the operator would act on, such as Recents audit evidence or a
  refusal reason. Progress and trace logging doesn't. So a survivor that only
  rewords progress output is a dismissal with that rationale written down, and
  one that silences the only record of a refusal is a real gap that owes the
  assertion. The criterion is the same for aimed mutants and catalog
  survivors, and
  mutmut produces the catalog ones by the dozen, so cite this rather than
  re-deciding per survivor (`docs/mutation-testing.md` § "Triage").
  Authority: "I accept your definitoin" —
  https://github.com/abl030/cratedigger/issues/1313#issuecomment-5503374358

### Generated-test performance is a coverage contract

- Classify a generated surface as **finite** only with an independently checked
  cardinality and canonical representation. Use
  `tests.finite_domain.finite_generated_domain`; its proof runs during isolated
  discovery, its exact budget is runner metadata, and the scheduler refuses to
  multiply it into entropy shards. Keep edge `@example` pins and a known-bad
  collapsed-domain checker. Never infer finiteness from a green `SHALLOW` report
  or cap an arbitrary strategy because examples repeated.
- A generated property must not launch Node once per example. Use
  `tests.node_jsonl_worker.NodeJsonlWorker`: one strict JSON-lines child per
  isolated Python target, with request IDs, typed frames, timeout/EOF/malformed
  output detection, poisoned-worker failure, and target teardown. The bounded
  AST audit in `tests/test_generated_node_worker_audit.py` rejects the historical
  literal `subprocess.run(["node", ...])` shape.
- Optimize from production-depth target timings, preserving the exact daily
  budget, property count, edge pins, and target isolation. Benchmark the changed
  target first, then run the complete burst after a material critical-path
  improvement. Stop when the next cost is domain work rather than repeated
  harness overhead, or when added protocol/scheduler complexity outweighs the
  measured wall-time return. Detailed workflow: `docs/generated-testing.md`.

## Test execution, evidence, and hooks

Choose validation timing and depth during development using engineering
judgment based on the change, current evidence, and concrete risk. Focused
tests, whole-tree Pyright, the complete deterministic suite, and relevant
surface-specific checks are all available whenever they add useful feedback:

```bash
bash scripts/test.sh tests.test_X                        # target + adjacent/ambient gates
nix-shell --run "python3 scripts/run_pyright_checks.py"  # both typing contracts
nix-shell --run "bash scripts/run_tests.sh"              # complete suite
```

The targeted entrypoint pairs deterministic and generated siblings, selects
tests adjacent to changed production surfaces, discovers every `test_*_audit`
module plus explicitly named ratchets, and reuses the canonical JavaScript,
Pyright, Ruff, and Vulture phases. With no explicit selector, changed paths are
the target source. A changed shared `tests/**.py` module resolves via an
`scripts/targeted_test_selection.py::EXACT_PATH_NEIGHBOURS` entry, one of the
few remaining directory prefix rules, or an admitted gap in
`SHARED_MODULES_WITHOUT_COVERAGE` (`tests/test_negative_coverage_audit.py`
mechanically enforces that no module under `tests/` imports it, not just
review); one with none of those fails the whole
entrypoint closed (`scripts/test.sh` exit code 2) before any phase runs —
silent under-selection there is worse than a loud refusal. A changed `lib/**/*.py`
module gets the same fail-closed treatment on the OTHER side of the same gap
(issue #1199): if its full resolution — the same `EXACT_PATH_NEIGHBOURS`
entry/prefix-rule mechanism, plus `_direct_test_candidates`'s `tests.test_<
stem>`/`tests.test_<stem>_generated` probes, which are keyed on basename only
and so miss real coverage filed under the file's full path (e.g.
`lib/dispatch/core.py`) — yields zero test modules, selection fails the same
way unless the path is admitted in `LIB_MODULES_WITHOUT_SELECTION_COVERAGE`
(measured fresh at 31 files on 2026-08-31,
`tests/test_selection_coverage_audit.py`
proves the registry exact in both directions: no stale admission, no
unregistered zero-neighbour file). Unlike the tests/-side registry, an
admitted lib/ gap does not early-return at all — the full resolution already
ran normally before this check ever sees it; nothing short-circuits. When
that resolution comes back genuinely empty AND the path is registered, this
branch only prints a loud stderr line naming the admitted gap and falls
through to the SAME return every other path takes — never a silent cap, and
a registration that later gains real coverage is selected immediately with
no code change here (only the now-stale registry entry needs deleting,
which the audit demands). A changed `scripts/**/*.py` file gets the SAME
non-early-return, admitted-gap treatment (issue #1248) via
`SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE` — the scripts/ mirror of
the lib/ case above, including the same nested-subdirectory basename-only
miss (`scripts/pipeline_cli/
cli.py` probes only `tests.test_cli`, ignoring the `pipeline_cli/`
component) — and, since #1278 item 9, so does a changed
`scripts/**/*.sh`: the shell wrappers had no fail-closed story at all
(`scripts/run_final_gate.sh`'s entry was added by item 6's PR2, the same
commit that made it a wrapper — through item 6's PR1 and everything
earlier, editing that file selected nothing), and they now resolve through
the same `tests.test_<stem>` basename probe or fail closed like their `.py`
siblings. All three roots are one `ROOT_COVERAGE_RULES` table — root,
policed suffixes, registry, whether admitted gaps early-return, and the
unmapped-path message — audited by one parameterized
`tests/test_selection_coverage_audit.py` that derives its rows from that
table (with the scope-deciding columns anchored outside it) and proves the
`lib/`/`scripts/` registries exact in both directions; the `tests/`
registry's exactness stays with `tests/test_targeted_test_selection.py` and
`tests/test_negative_coverage_audit.py`. That audit also polices
`EXACT_PATH_NEIGHBOURS` itself: every named
module exists, no entry is fully redundant with what the path resolves
without it, and every entry whose deletion no fail-closed rule would catch
(because a basename probe or prefix rule still resolves something, or
because no rule polices that root and suffix) carries an explicit
`MASKABLE_ENTRY_PINS` pin — single-place deletion can never be silent,
while deleting an entry AND its pin together stays review-owned, the same
boundary the typing ratchet has.

The basename conventions and the directory prefix rules are themselves one
table since issue #1313, `SELECTION_RULES` (5 basename rows read by
`_direct_test_candidates`, 10 directory rows read by the resolver), so the
modules they name are ordinary data that contract A checks like any
`EXACT_PATH_NEIGHBOURS` entry — they were inline literals inside two
if-chains, and a nonexistent one failed only downstream at the first
selection that hit the rule. Since #1331 residual 1 those rows also carry
the deletion-visibility contract the entries have: `MASKABLE_RULE_PINS`
pins every row whose deletion at least one file it matches would not
report, measured by removing the row through the resolver's own DI seam and
asking the real fail-closed contract what happens. 12 of the 15 rows are in
that state. Over an unpoliced root (`migrations/`, `nix/`, `web/`,
`harness/`, the top level) nothing can raise at all, so the loss is silent
even when the file drops to zero neighbours. Over a policed root it is
silent at the files something else still resolves for — a basename probe,
another prefix rule, or a hand-authored entry — which is how
`scripts/phase_parsers/pyright_checks.py` would quietly fall back to a
module written for a different script. A pin names sample paths and what
each stops selecting, one per contribution channel the row can silently
lose, and `MASKABLE_RULE_MATCHERS` freezes the same row's path conditions,
because a narrowed matcher shrinks the population the measurement judges
the row against rather than showing a loss. A row that matches no real
file, or contributes to none of the files it matches, is refused outright,
since either would measure clean while selecting nothing. Before adding a hand-authored entry, ask what
the path already resolves:
`nix-shell --run "python3 scripts/targeted_test_selection.py explain <path>"`
names the mechanism behind every selected module, every module a rule
looked for and did not find on disk, and whether any root rule polices the
path. This is development feedback; the full
suite remains the exhaustive pre-review boundary.

Always use `nix-shell --run` for Python (`.claude/rules/nix-shell.md`). Direct
Nix-shell runs are ordinary development feedback; fix their failures in the
current convergence loop. They do not mint final receipts. A green complete
suite does not replace generated, live-boundary, browser, corpus, VM, or other
specialized evidence required by the change. Before independent-review handoff,
use the `check` skill for one receipt-backed whole-tree confirmation once the
converged tree is committed and clean; it owns the receipt and unchanged-tree
no-replay rules. If review changes nothing, that same receipt is the final
pre-push confirmation.

`run_tests.sh` exhausts JavaScript, the concurrent complementary Pyright phase,
Ruff, Vulture, and the complete Python scheduler
before returning one aggregate status. `run_final_gate.sh` can only execute
that exact suite; it adds clean-commit receipt semantics, not another selection
of checks. The suite's terminal output is a compact complete failure index; the
printed private-tmpfs bundle contains `summary.json`, `summary.md`, and every
complete phase log.

Each phase's `PhaseSpec` names the callable that reads its own log, and that
callable lives in the module named after the wrapper whose output it decodes
(`scripts/run_ruff.sh` → `scripts/phase_parsers/ruff.py`, and so on; issue
#1313). `scripts/phase_parsers/__init__.py` is the contract they share and
holds no dialect: the `CheckFailure` the bundle persists, the `PhaseLog` a
parser is handed, the `PhaseFailures` it may return, and the `PARSER_ERRORS`
it may raise. The coordinator runs a command and indexes what comes back —
adding a phase means writing a parser beside its wrapper, never editing
`scripts/run_test_suite.py`'s decoding, because it has none. Dialect tests
live in `tests/test_phase_parsers.py` and are deterministic-only like every
other test-infrastructure test.

**Admission control on the shared test RAM root** (issue #1111): the fixed-size
tmpfs backing `TMPDIR` has no capacity of its own to arbitrate concurrent
suites, so `scripts/run_test_suite.py::run_suite` takes an advisory
`fcntl.flock` (`acquire_suite_admission`, in `scripts/test_substrate.py`; lockfile
`<runtime>/.cratedigger-test-admission.lock`) before running any phase, held
for the whole run — never per-phase. Scoped to `run_suite` itself, not to
every `nix-shell` shellHook entry, which would also serialize interactive
dev shells that never call `run_suite` at all. `scripts/test.sh` targeted
runs DO take this same lock: `scripts/run_targeted_tests.py` calls the
identical `run_suite` with no `runtime_dir` override, so it resolves the
same shared root and contends for the same lockfile as the canonical suite
— deliberately, since #1111's own incident record includes a
`scripts/test.sh` collision (`BrokenProcessPool` at host load ~46) and the
reap-safety argument below depends on every caller serializing through this
one lock. A second concurrently-launched `run_suite` call waits, bounded
(`DEFAULT_ADMISSION_TIMEOUT_SECONDS`, a
`run_suite(admission_timeout_seconds=...)` kwarg — no environment override,
unlike the headroom minimum below; expiry is exit-2-indistinguishable from a
genuine infrastructure failure, a stated residual), printing progress naming
the contended lockfile and, only when a stored pid+start-ticks pair is BOTH
present AND verified currently live (`_proc_start_ticks(pid) == ticks`, the
same liveness comparison the final gate's own receipt identities use — one
implementation since #1278 item 6 moved that gate into this same module), the
current holder's identity — never an unverified one, since the identity is
cleared on release but a write can itself be lost to the very exhaustion
this PR exists for, so a stale or unreadable value falls back to the plain
lockfile-path message rather than confidently naming a dead process
(`_write_lock_holder_identity` / `_read_lock_holder_identity`, mirroring
the final gate's own helper/gate identity precedent — `scripts/run_final_gate.sh`
is a thin wrapper over `scripts/test_substrate.py`'s `final-gate` subcommand
since #1278 item 6, so both identities are written and read by one module,
never by a bash copy). Since the shellHook's own
entry-time headroom check (`scripts/test_tmpfs.sh`) runs BEFORE this lock is
even reached, a naive second suite would still die at shell entry under
contention with the old unnamed message instead of queueing — so every
automated launcher of the canonical suite (`scripts/test.sh`, the final
gate behind `scripts/run_final_gate.sh` — which since #1278 item 6 sets the
var in the environment of the `nix develop` child it launches from Python
rather than shelling out through an `env` prefix,
`scripts/daily_flake_update.sh`'s
`deterministic_suite` stage, and `scripts/daily_beets_tip_update.sh`, which
runs the same suite through `nix develop .#tip` — grep for BOTH
`nix-shell --run` and `nix develop`, since only the daily-gate stages still
enter the shell the legacy way) sets
`CRATEDIGGER_SUITE_OWNS_HEADROOM=1` for the dev-shell entry it drives,
which tells that shellHook check to skip only its free-bytes refusal
(everything else in `setup_cratedigger_test_tmpfs` still runs); `run_suite`'s
own post-lock headroom precondition is then the single enforcement point for
every suite run launched this way. Issue #1229 moved the first two of those
launchers from `nix-shell --run` to `nix develop --command` — the same
derivation either way (`flake.nix`'s `devShells.default` IS `./nix/shell.nix`),
but only the flake path gets Nix's own eval cache, worth ~4.7s per invocation
on a clean tree. `daily_flake_update.sh`'s stages deliberately did NOT move:
they are unattended nightly jobs where the saving is irrelevant, and moving
them would mean teaching that script's fake `nix` shim a new subcommand for no
operator-visible gain (`daily_beets_tip_update.sh` already runs the same suite
through `nix develop .#tip`, with no shim in its test at all). The documented DIRECT command
(`nix-shell --run "bash scripts/run_tests.sh"`, CLAUDE.md/README.md/this
file's own code block above) deliberately stays as-is: a human running it
interactively gets the entry guard on purpose, since nothing there is
launching it as an unattended, contention-prone automation.

It is NOT only interactive entries that differ: the var is
an inherited process-environment setting, so a NESTED `nix-shell` a test
spawns as its own subprocess also inherits it from the enclosing suite and
skips the same refusal — deliberately, since the enclosing `run_suite`
already owns headroom enforcement for the whole run and a nested shell
re-imposing its own would be redundant, not protective. (`tests/test_decision_corpus_export.py` used to be this case, nesting six such
subprocesses per run to invoke a copied `scripts/decision_differential.py`
against historical base-ref archives; issue #1131 removed the nesting
entirely — `sys.executable` was already the enclosing shell's own
interpreter with an identical `PYTHONPATH`, so re-entering `nix-shell` paid
a full Nix evaluation per call for zero additional capability. The six
calls now invoke `sys.executable` directly, so no test currently spawns a
NESTED `nix-shell` — but this is not dead machinery: the top-level
free-bytes skip itself is taken on EVERY suite run launched by
`scripts/test.sh`, the final gate behind `scripts/run_final_gate.sh`,
`scripts/daily_flake_update.sh`, and `scripts/daily_beets_tip_update.sh`,
and is pinned by
`tests/test_test_tmpfs.py::test_suite_owns_headroom_skips_only_the_free_bytes_refusal`.
Only the nested-shell-inherits-the-var form has no current example — the
mechanism remains in `scripts/test_tmpfs.sh` for the next test that
legitimately needs to nest.) Only a genuinely interactive `nix-shell` entry,
started outside any suite run, never has this var set and keeps its entry
guard.

Once admitted, `run_suite` first retires eligible final-gate receipts
(`reap_stale_final_gate_receipts`, issue #1208 item 4), THEN best-effort
reaps scratch directories nothing can still be writing
(`reap_stale_check_bundles`, prefix set `_REAPABLE_PREFIXES`:
`cratedigger-checks.*` bundles, this test-infra change's own
`cratedigger-{suite,admission,reap,headroom}-test-*` fixture directories,
and the dev shell's OWN main scratch `TMPDIR` (`SCRATCH_TREE_PREFIX`,
`cratedigger-tests.*`, issue #1208 item 1) idle past
`DEFAULT_STALE_BUNDLE_MAX_AGE_SECONDS` — safe for another `run_suite`'s own
bundles specifically, because reaping happens exclusively under the same
lock every `run_suite` call takes; it is NOT a claim about every possible
creator of a matching prefix — `tests/test_final_gate_receipt.py` constructs
a `cratedigger-checks.*` fixture directly, outside `run_suite` and outside
the lock, and the age gate (not lock exclusivity) is what protects it.

`SCRATCH_TREE_PREFIX` is the one prefix the age gate alone does NOT protect:
`scripts/test_tmpfs.sh` creates it at `nix-shell` entry, outside the
admission lock, and cleans it up only via a shell EXIT trap — SIGTERM,
SIGINT, and SIGHUP all run that trap correctly; only SIGKILL (the OOM
killer, `kill -9`, host loss) skips it and leaks the tree forever, and a
busy suite's own scratch root can go quiet (old mtime) for hours while very
much in use, so mtime cannot distinguish "abandoned" from "just idle". A
prior attempt reaped this prefix on mtime alone and was reverted after
review found it reaping LIVE trees. The shipped design instead requires
`_scratch_tree_owner_dead` to PROVE the owning shell process is gone before
the age gate ever applies to a `cratedigger-tests.*` entry:
`test_tmpfs.sh`'s shell hook has the marker written immediately after
`mktemp` (since #1278 item 6 by calling `scripts/test_substrate.py`'s
`write-owner-marker`, so the `.owner` format — `"<pid> <ticks>\n"`, the same
content shape as the admission-lock holder identity below — and the `/proc`
read behind it are spelled beside the reader, not in bash), and
`_scratch_tree_owner_dead` verifies it with the same pid-reuse-safe
`_proc_start_ticks` comparison. A live owner is never touched regardless of
age; a missing or malformed marker fails closed (treated as "unknown,
never reap", not "abandoned") rather than falling back to age alone.
Known residual (issue #1208 review D7): the reaper resolves its runtime
root from `XDG_RUNTIME_DIR` only (`private_runtime_dir`), while
`check_suite_headroom` below and `scripts/test_tmpfs.sh`'s own scratch-
tree parent both honor a `CRATEDIGGER_TEST_RAM_ROOT` override — set that
override and the reaper looks in a different directory than the scratch
trees it exists to reap. Latent: nothing in this repository sets that
override outside tests and one doc recipe.

Two more known residuals of the same design (issue #1208 review D-F4,
D-F6):

- The recursive walk (`_scratch_tree_last_activity`) only helps when a
  descendant WRITES within `max_age_seconds`. A descendant that is alive
  but genuinely quiet — a shell blocked on `read`/`sleep`, holding the
  tree open with nothing touching any mtime — falls back to exactly the
  mtime heuristic this design otherwise argues against; the owner-death
  marker distinguishes "abandoned" from "idle" for the SHELL itself, but
  says nothing about a live-but-quiet descendant of an already-dead
  owner. Reproduced directly: a real SIGKILLed owner whose `bash`/`sleep`
  descendants were still alive and holding `$TMPDIR` was reaped anyway,
  because they were quiet.
- **Operationally important**: every `cratedigger-tests.*` tree created
  by a shell running BEFORE this fix lands has no `.owner` marker at
  all — fail-closed means such a tree is never reaped, forever, by this
  mechanism (a missing marker is "unknown", never "abandoned"). The
  founding incident's own leaked-tree cohort is exactly this shape.
  Deploying this fix does not retroactively clean up that cohort; it
  only stops the leak going forward. A one-time manual sweep of
  markerless `cratedigger-tests.*` trees whose owning shell is
  genuinely gone (per scope.md: an operator/agent-run one-shot, never
  committed machinery) clears the pre-existing backlog once, after this
  lands.

The 4-hour floor protects the bundle's detailed EVIDENCE (per-phase logs,
summary.md), not receipt reuse itself: `run_final_gate.sh status` checks a
receipt's `bundle` FILE (a path string) and now separately stats the
directory it names, failing visibly when it is gone rather than silently
reporting `pass` over evidence that no longer exists — a receipt's own
`terminal` verdict was always durable regardless of this floor. Before the
age gate ever applies, `_receipt_protected_bundles` excludes any bundle path
still named by a `cratedigger-final-gate.*/bundle` file — so a bundle a
still-present receipt still references survives regardless of age,
preserving both the verdict and its evidence for a long-running review
(issue #1111 review m13). Receipts are no longer permanently unreaped:
`reap_stale_final_gate_receipts` deletes one once its `terminal` file
exists OR its recorded helper/gate pid+start-ticks identities are
conclusively dead — never one still live — AND it is older than
`DEFAULT_RECEIPT_RETIREMENT_MAX_AGE_SECONDS` (a fixed 7-day constant, not
an env-overridable knob). A retired receipt is gone from disk before
`_receipt_protected_bundles` glob-scans `cratedigger-final-gate.*` on this
same admitted pass, so retirement lapses that receipt's own bundle
protection immediately, with no second cleanup path: the now-unprotected
bundle ages out through this function's ordinary path like any other. A
receipt whose protected bundle turns out to be missing anyway is a
genuinely dangling receipt; the protection does not paper over that —
`status`'s own stat check surfaces it, and the honest response is to
re-run.

`run_suite` then checks headroom (`check_suite_headroom`, same
`CRATEDIGGER_TEST_RAM_MIN_BYTES` env var and 1 GiB default as
`scripts/test_tmpfs.sh`'s own shell-entry guard, and honoring the same
`CRATEDIGGER_TEST_RAM_ROOT` override when set) BEFORE creating a bundle.
Insufficient headroom raises `RamRootExhaustedError` immediately, with no
phase run and no bundle created — the whole suite fails once, with its real
reason, instead of a phase deep into the run tripping the same guard after
earlier phases already passed. Mid-run,
`scripts/run_python_tests.py::_collapse_disk_full_failures` folds every
disk-full-classified target/test into ONE `test RAM root exhausted`
`CheckFailureMarker` (`TEST_RAM_ROOT_EXHAUSTED`, defined in
`scripts/test_substrate.py`) instead of N separately-indexed disguises
(`FileNotFoundError` on `raw-output.log`, a Hypothesis `FlakyFailure`, ...);
classification measures free bytes at the moment a worker's own exception is
caught (`_classify_target_infrastructure_failure`, same configured
`CRATEDIGGER_TEST_RAM_MIN_BYTES` floor `run_suite` itself enforces, not the
running-test classifier's much lower internal 64 MiB fallback) or a running
test's own ENOSPC-shaped exception fires (`_classify_test_infrastructure_error`,
unchanged) — never a scan of log text, per the semantic-source-scanner
prohibition above. Stated honestly: a genuine exhaustion event whose
measured moment happens to read above the configured floor still shows as an
ordinary failure — a known miss, never a false green, since nothing here
ever hides a real defect, only relabels ones it can prove are environmental.
A target whose failures are ENTIRELY disk-full returns
`TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE` so the phase (and the suite) reports
`infrastructure-failure`, not an ordinary `failed`, without touching
`scripts/run_test_suite.py`'s generic phase-state derivation.
`scripts/fuzz_burst.sh` (`scripts/run_fuzz_tests.py`) and
`scripts/run_world_model_burst.py` each now run their own analogous
headroom precondition (issue #1156 item 3) — `headroom_floor_bytes`/
`check_suite_headroom`, called once before any work and again once per
admission CYCLE inside their own admission loop (a cycle can admit up to
`worker_count` targets at once, so this is not a per-target check).
Honestly stated (independent review F6/F7): in the daily gate specifically
this preflight is a near-duplicate of a check that already ran seconds
earlier — `scripts/daily_flake_update.sh` scopes
`CRATEDIGGER_SUITE_OWNS_HEADROOM=1` to its `deterministic_suite` stage
only, so the `world_model`/`generated_fuzz`/`mirror_harness` stages still
hit `scripts/test_tmpfs.sh`'s shell-entry guard on the SAME root with the
SAME env var and the SAME flat 1 GiB default before either coordinator's
own `main` ever runs. The coordinator-level preflight's real added value
is for a SECOND invocation inside an already-entered shell (interactive
dev use, or any caller that skips the wrapper `.sh` and its shellHook),
where nothing else re-checks headroom between shell entry and that call.
Neither burst takes `acquire_suite_admission`'s exclusive lock or
`reap_stale_check_bundles`'s scratch reaping. Independent review B-4 (third
round, issue #1156): these are two SEPARATE decisions, and only the lock one
is where this paragraph previously claimed both were -- each `main`'s own
docstring records the lock reasoning (both are long-running, variable-
duration bursts that would starve or force raising the timeout on the
bounded queue an ordinary `scripts/test.sh` dev-loop run waits on); neither
docstring mentions `reap_stale_check_bundles` at all. The reaping decision
is simpler and stated here instead: `reap_stale_check_bundles` only reaps
`cratedigger-checks.*`/fixture-prefixed scratch under the ADMISSION LOCK
(`scripts/run_test_suite.py::run_suite`, called for neither burst), so a
burst that never takes that lock has no reap call to make in the first
place -- there is no burst-specific tradeoff to document. Both pass
`worker_count=1` to `headroom_floor_bytes`
(the flat, override-respecting `DEFAULT_MIN_HEADROOM_BYTES` floor), not a
worker-scaled one: neither burst has a MEASURED per-worker tmpfs footprint
the way the deterministic suite's #1131 model does — a worker-scaled
attempt sized by the fuzz burst's own default worker count (up to 60 on a
30-core host) demanded EXACTLY 4 GiB (256 MiB base + 64 MiB/worker * 60
workers = 4096 MiB, independent review B7 item 2 — "over 4 GiB" overstated
it), more than a normal interactive dev
tmpfs actually has free (see `headroom_floor_bytes`'s docstring). Tripping
either guard stops ADMISSION only — already-running targets keep
consuming the reserve regardless, so this is a correct label and exit code
on the way out, not prevention, and the abort is one-way with no
hysteresis. `recommended_fuzz_jobs` separately gained a ceiling
(`MAX_FUZZ_JOBS` = 64, issue #1214 "Contributing gaps" item 1, NOT #1156
item 1): it is inert on hosts measured to date (doc1 at 30 cores computes
60, well under 64) and would not by itself have prevented the 2026-08-20
incident (60 was already the number that overflowed) — it is fail-closed
legislation for a future, larger host. Separately,
`run_python_tests.py`'s worker-death classifier
(`_classify_target_infrastructure_failure`) now also folds a worker killed
by the OOM killer — exception shape `BrokenProcessPool`, corroborated by
measured `/proc/meminfo` `MemAvailable` below the env-overridable
`CRATEDIGGER_TEST_MEMORY_MIN_BYTES` (mirroring `CRATEDIGGER_TEST_RAM_MIN_BYTES`'s
own override pattern) — into ONE named `test host memory exhausted` marker
(`TEST_HOST_MEMORY_EXHAUSTED` / `_collapse_memory_exhausted_failures`), the
same N-disguises fix `TEST_RAM_ROOT_EXHAUSTED` above already gave
disk-full. A DIFFERENT resource (system memory, not the tmpfs RAM root)
gets a DIFFERENT identity and exit code
(`TEST_HOST_MEMORY_EXHAUSTED_EXIT_CODE`), never folded into the disk-full
bucket. If BOTH a disk-full marker and a memory-exhausted marker occur in
the same run (different targets), the phase reports an ordinary failed
rather than promoting to infrastructure-failure — a known, accepted
residual (independent review F10), not a claim that the two never
co-occur.

**Generated (property-based) production tests**
(`tests/test_*_generated.py`, Hypothesis)
run deterministically in the suite. After changing quality policy, run the
randomized fuzz burst: `nix-shell --run "bash scripts/fuzz_burst.sh"` (one
process per generated module, parallelised to the host's cores — Hypothesis is
single-threaded, so never run the burst serially). Failures shrink to minimal
worlds — promote them to named `@example` pins or album-test-set scenarios,
never JSON artifacts. New production features start by writing their invariants
down, and every production invariant ships as a pair — deterministic pin +
generated property — in the same PR, with known-bad self-tests. Test
infrastructure remains deterministic-only as defined above. When in doubt that the harness
constrains anything, qualify it by fault injection. See
`docs/generated-testing.md`.

### Skipped tests are an anti-pattern

**A test either runs or it doesn't exist.** `tests/test_skip_audit.py` rejects
known unittest skip markers without an allowlist; the same policy forbids
environment gates. Supply dependencies through Nix, construct synthetic
fixtures, use fakes, or remove the test.

### Hooks

- Pre-commit (`ln -sf ../../scripts/pre-commit .git/hooks/pre-commit`): threaded
  Pyright on staged `.py` and syntax checks on staged JavaScript.
- There is no pre-push hook and CI does not run the suite. The agent owns the
  local validation and final pre-push confirmation described above.
- Repository releases are not tagged. The deployed Git commit, signed
  nixosconfig pin, and live verification evidence identify the running state.

## Authority for exceptions and bypasses

Plans translate product authority; they do not create it. Any plan KTD,
approach item, registry entry, or implementation note that grants an exception,
bypass, mode/status-specific permission, or authority for a destructive or
scope-affecting action must carry both:

- a stable link to the exact operator or issue-thread decision; and
- the controlling sentence quoted verbatim next to that link.

Use the form `Authority: "<verbatim decision>" — <stable URL>`. A plan-internal
identifier, summary, inferred rationale, or earlier implementation is not
authority. If the citation does not exist or conflicting decisions remain, the
item is an open question for the operator, not a settled decision. When a later
decision supersedes an earlier exception, cite the superseding decision at the
old site and remove or explicitly correct every stale grant.

Canonical counterexample: issue #711's KTD4/U7 widened force-import into a
verified-lossless proof-lock bypass without a thread decision. Decision 21
reversed that planning-time grant; the plausible rationale did not make the
grant authoritative. This rule records issue #737 item 5's process guard.

## Semantic source scanners are prohibited

When an invariant can be enforced by narrowing the production contract, make
the allowed code shape small and explicit. Prefer one typed owner, one
canonical call or SQL form, and a fail-closed audit that rejects everything
outside that grammar.

Do not add repository-wide AST, data-flow, SQL, or control-flow analyzers that
attempt to infer runtime semantics from arbitrary source. Registries of
conditions or call sites, alias tracking, and scanners extended syntax case by
syntax case are prohibited. Static audits may enforce a local syntactic fact
with a deliberately bounded grammar; they must not substitute for Python or a
database parser.

**Good enough is a valid stopping condition.** When the production code states
the boundary plainly, removes the dangerous input from the decision surface,
and direct behavior tests pin the known failure modes, stop unless review can
name a concrete remaining counterexample. Do not replace a rejected semantic
scanner with a speculative typed policy layer or other abstraction whose only
purpose is to make hypothetical future misunderstanding impossible. Clear code,
behavior tests, and review are real guardrails; an issue must not stay open
solely because the code could theoretically be made harder to misuse. Further
centralization must identify a current failure or bypass that it would prevent.

Qualify the narrow contract with known-bad variants and at least one real
production-path test. Any non-canonical or unresolved construction must fail
closed unless it is an explicitly reviewed, tightly bounded seam. If a real
risk cannot be enforced at a typed/API/schema boundary plus concrete behavior
tests, record that unsolved risk in a GitHub issue instead of building a
semantic source scanner.

## API Contract Tests
- Every API endpoint consumed by the frontend must have a contract test in `tests/web/` — one `test_*.py` per `web/routes/*.py` module (e.g. `tests/web/test_routes_pipeline.py` for `web/routes/pipeline.py`)
- Use `_FakeDbWebServerCase` and its `_get`/`_post` helpers with seeded
  `FakePipelineDB` state; never configure DB mock returns or copy the harness.
- Define the frontend-consumed fields in `REQUIRED_FIELDS` and assert them with
  `_assert_required_fields`; add a needed field to the contract first (RED).
- Declare every route `classified=True`; `tests/web/test_route_audit.py`
  enforces the registry directly.
- **Mock data must mirror production row shape — synthetic int/str dicts are NOT acceptable.** When a contract test mocks a DB-row producer (any `PipelineDB`/`BeetsDB`/`psycopg2.extras.DictRow` source), at least one scenario must populate rows with production-shaped values: `datetime.datetime` for timestamps, `uuid.UUID` for UUIDs, the typed dataclass/`msgspec.Struct` for JSONB columns. Synthetic dicts of `str`/`int` values pass Pyright (`Dict[str, Any]` is permissive) and pass the contract test (mock matches assertion shape) but 500 on the first real call when the JSON encoder hits an unserializable type. This rule has bitten more than once — see `docs/solutions/testing/contract-test-mocks-must-mirror-production-shape.md` (search-plan-history datetime 500) and `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md` (search-by-id MB drift). The escape hatch when row-shape mocking is impractical: pair the contract test with an integration slice in `tests/test_integration_slices.py` that round-trips through real serialization. Every contract test that returns DB rows owes either a production-shaped mock OR a slice — never neither.

## Logging & Auditability
- Every download outcome (success, rejection, timeout, crash) MUST create a download_log row
- Use typed JSON dataclasses (`ImportResult`, `ValidationResult`) — never raw dicts
- Store the full JSON in JSONB columns for SQL queryability
- Never throw away data the harness or subprocess provides — log everything

## Decision Logic
- All quality/import decisions must be pure functions in `lib/quality/`
- No decision logic inline in cratedigger.py — call the pure function, branch on result
- Every pure function must have direct unit tests (not just tested through integration)

## Production Bug Hunting — Generated-First (the house method)

Proven on #550 (2026-07-08): a live production bug that static analysis and
disk forensics could NOT reproduce was found, reproduced, RCA'd and fixed in
one session by a generated harness driving the real code path. This is how
production bugs are hunted here — reach for it BEFORE log-trawling, before
speculative instrumentation, before reading code until a theory falls out.
Test-infrastructure defects are outside this method: reproduce them with an
exact deterministic contract and do not feed the test machinery back into the
generated-test scheduler.

1. **Write down the invariant the symptom violates** ("the manifest that
   reaches validation covers every file grabbed"). If you can't state one,
   you don't understand the symptom yet.
2. **Probe the cheapest suspicious seam with real production functions**
   (a throwaway nix-shell heredoc driving e.g. the real matcher over a
   seeded cache — minutes, not committed).
3. **Build/extend a generated harness** in `tests/test_*_generated.py`:
   strategies over the world space (no plausibility filters), the invariant
   as a checker, REAL production entry points, fakes/leaf-seam stubs only
   at the allowlisted edges. Let Hypothesis find and shrink the
   reproduction.
4. **RED → fix → GREEN** in one PR: the shrunk world becomes a
   deterministic regression pin, the invariant becomes a permanent
   property, a must-still-work guard proves the fix doesn't fail-closed
   legitimate behavior, and a message-asserting known-bad self-test proves
   each of the checker's clauses trips.
5. **Qualify when in doubt** — plant a mutant reverting your fix; the
   property must kill it.

Tools within the method, for quality-decision bugs specifically:

- **Simulator scenarios** (`pipeline-cli quality <id>`,
  `tests/test_simulator_scenarios.py`): the flat-kwargs twin is the
  canonical scenario language — add the failing scenario to the album test
  set and run the simulator against real albums in the live DB to verify.
  The simulator must show the full rejection cycle (import/reject →
  spectral propagation → backfill → next tiers), not the import decision
  in isolation.
- **Real-code-path orchestration repros**: when a bug lives in component
  interactions (propagation → decision → DB write), drive the actual
  orchestration function (`measure_preimport_state`,
  `dispatch_import_from_db`) with state matching the live scenario — pure
  decision tests alone miss state mutations and ordering. Guard both
  directions: the fixed case AND the still-valid original behavior.

## Invariants live at the widest boundary the change touches

The #853→#859 incident pair is the canonical lesson: two consecutive P0s
shipped back-to-back with thousands of passing tests, clean Pyright, and
adversarial review finding nothing — because every invariant and test lived
at module scope while both defects lived in the COMPOSITION of two workers
sharing one filesystem namespace (`processing/albums/`). The preview
worker's sidecar write was legal by every module-scope test; the importer's
exact-manifest guard was correct by every module-scope test; their
composition stalled every automation import in production.

- **Scope invariants to the widest boundary the change touches.** When a
  change writes to or guards a namespace that another worker, process, or
  phase also reads or writes — a filesystem tree, a DB table, a queue, a
  wire format — state the invariant at that shared-namespace level, and
  the pin+property pair must COMPOSE the real writer with the real
  guard/consumer over the same real resource. Module-scope tests alone are
  insufficient evidence for such a change, whatever their count.
- **Never mock our own writers in a composed test.** The leaf-seam mock
  rule already forbids mocking our own logic; this is its composition
  corollary: a test that claims writer→guard coverage must run the REAL
  writer and the REAL guard against the same real resource (tmp processing
  root, real PG, …). #858's tests proved normalization ordering with the
  sidecar writer absent from the composition — that absence was the escape
  route for #859.
- **A guard over a shared namespace ships with a patrolling property.**
  A guard (manifest equality, schema check, fingerprint match) implicitly
  legislates for every OTHER writer of that namespace, present and future.
  Ship the guard together with a generated property that drives the real
  writers and asserts the guarded invariant — otherwise the legislation is
  unenforced until production enforces it. Canonical patrol:
  `tests/test_preview_manifest_generated.py` (canonical processing albums
  remain exact media manifests).
- **Review must cross-reference the namespace, not just the diff.** For a
  PR touching writers or guards of a shared namespace, enumerate (grep,
  not an analyzer) every reader and writer of that namespace and check
  each pair against the change. The colliding code is usually NOT in the
  diff — that is precisely why diff-scoped adversarial review converged
  with no findings on #858.

## Frontend and server-route conventions

JavaScript and `web/routes/*.py` conventions live in `.claude/rules/web.md`
(`paths: web/**`), which loads whenever you touch the web surface.

## Finish What You Start
- Don't build infrastructure without wiring it up. Every new function, dataclass, or mode must be called from production code. If it's only reachable via manual config nobody sets, it's dead code.
- Before marking any feature complete, trace the full path from trigger to effect. Ask: "Does this actually run in production without manual intervention?" If not, it's not done.
- A new dataclass that nothing constructs, a config option nobody sets, a fallback that never triggers — these are all incomplete work, not shipped features.
- Comments and docstrings describe contracts and capabilities, not brittle
  cardinality. Do not write claims such as "the seven methods" when the
  surface can grow; name the methods only when each identity matters, or
  describe the shared contract the open-ended set implements.

## No Parallel Code Paths
- Never create a second function that calls the same subprocess (import_one.py, beets_harness.py, etc.). If a new entry point needs the pipeline, write an adapter that constructs the existing function's inputs and delegates. If the interface makes this painful, fix the interface — don't route around it.
- Never construct `CratediggerConfig` with positional/keyword args for a subset of fields. Always use `CratediggerConfig.from_ini()` with the runtime config file. Partial configs silently diverge when new config fields are added.
- Before adding a new function that "does roughly what X does but simpler," check if X can be called with an adapter. The adapter may be ugly — that's a signal to improve X's interface, not to duplicate X.

## CLI ⇄ API Surface Symmetry
- Every operator action must exist on **both** `pipeline-cli` and the web API. Adding only one is a contract drift waiting to happen — operators expect parity and will trip when it isn't there.
- Both surfaces use exactly one canonical execution path. Normally they wrap the same service-layer method (e.g. `SearchPlanService.advance_for_request`); when an existing web route is the canonical mutation boundary, the CLI may instead be a thin HTTP adapter to that route. In either shape, never duplicate logic or add a direct-DB fallback, and preserve matched outcome → exit-code / outcome → status-code mappings.
- Reference layout (worked example: `search-plan advance` in PR for the cursor-advance feature):
  - `lib/<thing>_service.py` — service method + typed `Result` dataclass (one outcome string per branch)
  - `lib/pipeline_db/` — atomic mutation with `FOR UPDATE` row lock
  - `scripts/pipeline_cli/<family>.py` — CLI subcommand wrapping the service (the CLI is a package split by command family, issue #495; ``cli.py`` registers the handler in the dispatch dict, ``routes_meta.py`` adds the argparse subparser)
  - `web/routes/<thing>.py` — HTTP endpoint wrapping the service
  - `tests/test_<thing>_service.py` — authoritative coverage of every outcome branch
  - `tests/test_pipeline_cli.py` — CLI wrapper test (exit-code mapping)
  - `tests/web/test_routes_<module>.py` — API contract test (status-code mapping); classification is `classified=True` on the route's `RouteRegistration`, enforced by `TestRouteContractAudit` (`tests/web/test_route_audit.py`)
- Status/exit-code convention to match: `200/0` success, `400/3` input validation (API only — CLI argparse covers this), `404/2` not found, `409/4` wrong state, `422/3` semantic violation, `503/5` transient/retryable. The convention's code home is `lib/surface_outcomes.py`: a service owns ONE outcome → HTTP-status table and derives its exit-code map via `exit_codes_from_http`; thin CLI HTTP adapters map live response statuses via `exit_code_for_http_status`. Service-owned maps are registered and audited in `tests/test_surface_outcomes.py`; route-/CLI-side ladders not yet behind a service table are the remaining #1278 item-3 work and sit outside that audit's bounded sweep.
- See `CLAUDE.md` § "CLI ⇄ API surface symmetry" for the full pattern table.

## New Work Checklist (read this first)

Before writing any new code, decide which test types you owe and what infrastructure you'll reuse:

| You're adding... | You owe... | Use this infrastructure |
|------------------|-----------|-------------------------|
| A new pure decision function in `lib/quality/` | A subTest table covering every branch | `tests/test_quality_decisions.py` patterns |
| A new dispatch / orchestration path | An orchestration test asserting domain state + an integration slice | `FakePipelineDB`, `patch_dispatch_externals()`, `tests/test_integration_slices.py` |
| A new web API endpoint | A contract test with `REQUIRED_FIELDS` AND `classified=True` on the route's `RouteRegistration` (enforced by `TestRouteContractAudit`) AND a paired `pipeline-cli` subcommand (CLI ⇄ API symmetry) | `_FakeDbWebServerCase` + `_assert_required_fields` from `tests/web/_harness.py`, the matching `tests/web/test_routes_<module>.py`, `scripts/pipeline_cli/` |
| A new operator action (CLI subcommand or API endpoint) | A service-layer method with a typed `Result`, BOTH a CLI subcommand AND an API endpoint, exit-code and status-code tests for each; the service owns ONE outcome → HTTP map with the exit map derived via `lib/surface_outcomes.py::exit_codes_from_http`, registered in `tests/test_surface_outcomes.py` | `tests/test_<service>.py` for the authoritative coverage; CLI/API tests check the wrapper only |
| A new slskd interaction | An orchestration test using `FakeSlskdAPI` | `FakeSlskdAPI` from `tests/fakes/` |
| A new JS-rendered operator control (card / button / onclick surface in `web/js/`) | An mjs assertion on the rendered handler wiring — the exact `window.*` handler with its exact arguments, plus the render path that places the control — because a control nothing asserts ships green while an argument-inversion mutant survives (escaped twice: #1110, #1241 review F3) | `tests/test_js_*.mjs` on the shared `tests/js_harness.mjs` (see `.claude/rules/web.md` for the idiom; `tests/test_js_suite_audit.py` rejects a bespoke one); precedent: `test_js_pipeline_dashboard.mjs`'s retag-card `window.recheckRetagDivergenceAlbum(...)` and mark-incomplete `window.toggleMarkIncomplete(...)` assertions |
| A new typed dataclass | A pure test of construction + serialization, and a builder in `tests/helpers.py` if it crosses test boundaries | `tests/helpers.py` |
| A new `PipelineDB` method | An equivalent stub on `FakePipelineDB`, in the `tests/fakes/pipeline_db/` module named after the production cluster that owns it, with a self-test in that cluster's `tests/test_fakes_<cluster>.py` (or `tests/test_fakes.py` when the cluster has no sibling yet) | `tests/fakes/pipeline_db/`, `tests/test_fakes*.py` |
| A new `BeetsDB` method | Either (a) an equivalent stub on `FakeBeetsDB` with a self-test in `tests/test_fakes_beets.py::TestFakeBeetsDB`, OR (b) drive the test against a real test SQLite DB if it's a read-only query | `tests/fakes/`, `tests/test_fakes_beets.py` |
| A feature with policy invariants (pure decisions, lifecycle / state machine, wire or event ingestion) | Generated properties + strategies in the same PR, a message-asserting known-bad self-test for every CLAUSE of every invariant checker; invariants written down FIRST | `tests/_hypothesis_profiles.py`, checker/strategy patterns in `tests/test_*_generated.py`, `docs/generated-testing.md` § "Per-clause proof" |
| A documented surface (a module option, a beets plugin, an operator action / CLI subcommand, the permission/ownership model, or a subsystem's documented behavior) | The doc update in the SAME PR (README / `docs/` / `examples/` / CLAUDE.md) — docs are part of done, not a follow-up | `tests/test_docs_audit.py` (structural coverage: plugins, CLI, dead-links, option descriptions); the relevant `docs/*.md` |

Routes are the strictest gate: `TestRouteContractAudit` will fail at test time if you add a route to `web/routes/` without classifying it. This is intentional — it prevents shipping endpoints the frontend can rely on without contract coverage.

**CLAUDE.md has a hard 32 KiB budget** — Codex's instruction limit, enforced by
`tools/generate-ai-adapters.py` and surfaced as a `tests.test_ai_portability`
failure, which is how you normally discover it: as a late, confusing red. New
always-loaded material therefore defaults to `.claude/rules/` (loaded anyway,
unbudgeted), and CLAUDE.md keeps only what earns a slot there. #1161 had to
revert its CLAUDE.md addition outright to stay green; the 2026-08-16
compaction pass bought roughly 3 KiB of headroom, not a reprieve. Check
`wc -c CLAUDE.md` before adding to it, and condense in the same PR if you do.

**Before writing a test, answer:** *What one-line change to production would make this test fail? If the answer is a function this test does not call, the test patrols a bystander.* That question catches agree-by-construction and wrong-reader pins at authoring time (issue #1143).

## Test Taxonomy

Four categories of tests. Each has different rules for what's acceptable. **All four categories already have established patterns and shared infrastructure in this repo — use them. Do not invent parallel approaches.**

### 1. Pure function tests
- Assert direct input → output. No mocks unless unavoidable for environment.
- Should be exhaustive for decision logic (`dispatch_action`, `quality_gate_decision`, etc.).
- **Use `subTest()` tables for decision matrices.** See `TestSpectralImportDecision`, `TestImportQualityDecision`, `TestTranscodeDetection`, `TestQualityGateDecision`, `TestDispatchAction`, `TestIsVerifiedLossless` in `tests/test_quality_decisions.py` as reference patterns. Pattern: `CASES = [(desc, ...args, expected), ...]` then one `test_X` method using `for ... in self.CASES: with self.subTest(desc=desc):`. Each new branch is one row, not one method.

### 2. Seam / adapter tests
- Protect interface boundaries: subprocess argv, config-to-flag wiring, SQL query shape, route contract fields, serialization formats.
- Implementation assertions (call args, payload shape) are **acceptable and encouraged** here.
- Examples: `--force` flag forwarded, `--override-min-bitrate` derived correctly, route returns required fields.
- These are legitimate tests — do not delete them to satisfy an "assert behavior not implementation" rule.
- For dispatch tests, use `patch_dispatch_externals()` from `tests/dispatch_helpers.py` — it patches the 4 external edges (`sp.run`, `_cleanup_staged_dir`, `trigger_plex_scan`, `trigger_jellyfin_scan`) and yields a `SimpleNamespace` with mock references. Add your own test-specific patches inside the `with` block.

### 3. Orchestration tests
- Must assert **domain outcomes**, not only helper call shapes.
- At least one assertion per test must target persisted state or observable output:
  - request status after the operation (`db.request(42)["status"]`)
  - `download_log` rows (`db.download_logs[0].outcome`, or `db.assert_log(self, 0, outcome="success")`)
  - denylist entries written (`db.denylist[0].username`)
  - retry / requeue behavior (status transitions via `db.status_history`)
  - attempt counters incremented (`row["validation_attempts"]`)
  - `validation_result` / `import_result` preserved
  - filesystem side effects (cleanup, staging)
- Mocking is allowed for external edges (subprocess and media-server clients), but the assertion target must be domain state.
- **Use `FakePipelineDB` from `tests/fakes/` for stateful collaborators instead of MagicMock.** It records request rows, download_logs, denylist entries, cooldowns, status history, spectral state updates. Since #1313 it is a package under `tests/fakes/pipeline_db/` with one module per `lib/pipeline_db/` cluster, composed in `_db.py` the way production composes `PipelineDB`; shared state and the cross-cluster contract live in `_base.py`. Each cluster's own self-tests live in `tests/test_fakes_<cluster>.py`, named so selection derives them; `tests/test_fakes.py` keeps the fake-to-production signature contract, the shared builders, and whatever has no sibling module to fall back from, which today is `_base`, `_core` and `source`.
- **Use `FakeSlskdAPI` from `tests/fakes/` for slskd interactions.** Stateful `transfers` and `users` fakes with `add_transfer()`, `queue_download_snapshots()`, `set_directory()`, `set_directory_error()`, configurable errors, and call recording.
- Use `make_ctx_with_fake_db(fake_db)` from `tests/helpers.py` to wire `FakePipelineDB` into a `CratediggerContext`.
- Use builders from `tests/helpers.py` — never hand-roll 20-field dicts.

### 4. Integration slice tests
- Use real code paths with lightweight fakes or temp resources.
- Patch only external edges that are truly expensive or unsafe (subprocess, network, BeetsDB).
- Live in `tests/test_integration_slices.py`. Existing slices to model new ones on:
  - `TestDispatchThroughQualityGate` — runs dispatch_import_core → real parse_import_result → real _check_quality_gate_core
  - `TestQualityGateVerifiedLosslessBypass`, `TestQualityGateSpectralOverride`
  - `TestDispatchNoJsonResult`, `TestForceImportSlice`
  - `TestSpectralPropagationSlice` — runs `measure_preimport_state` end-to-end (audio + spectral)
- **Required for every new high-risk orchestration boundary.** If you add a new pipeline path (a new dispatch decision, a new quality gate branch, a new spectral state transition), add a slice that exercises it with real code.

### Shared test infrastructure inventory

Always use these instead of inventing parallel scaffolding:

**`tests/helpers.py`** — general builders + context wiring:
- `make_request_row(**overrides)` — full album_requests row dict
- `make_import_result(decision=..., new_min_bitrate=..., ...)` — `ImportResult` dataclass
- `make_validation_result(**overrides)` — `ValidationResult` dataclass
- `make_download_info(...)` — `DownloadInfo` dataclass
- `make_download_file(...)` — real `DownloadFile` (not MagicMock)
- `make_grab_list_entry(...)` — real `GrabListEntry`
- `make_ctx_with_fake_db(fake_db)` — `CratediggerContext` wired to a fake
- `make_cycle_collaborators(...)` / `make_worker_collaborators(...)` — the two `lib/context.py` collaborator worlds with test defaults (#1313). The production types require ALL their fields (a construction site cannot forget one; that requiredness replaced the hand-registered construction audit), so the defaults live here: a test says "I don't care about this collaborator" by omitting it, while production must name every one. Build a context as `CratediggerContext(collaborators=make_cycle_collaborators(...), <scratch kwargs>)`
- `rebind_collaborators(ctx, *, download_ownership=..., ...)` — swap one collaborator on an existing context. `ctx.download_ownership = w` no longer works (the value is frozen); this is the test-only replacement, and there is deliberately no production equivalent
- `make_web_runtime(base=None, *, db=None, beets=None)` — the one place a `FakePipelineDB`/`FakeBeetsDB` is placed into `web/runtime.py::WebRuntime`'s production-typed handle fields (#1313). Two scoped `pyright: ignore`s mark that gap; everything else about a runtime is varied with plain `dataclasses.replace`, which Pyright does NOT check (measured against pyright 1.1.412: it rejects neither an unknown field name nor a wrong-typed value), so the checked surfaces are constructor calls and attribute access
- plus the slskd envelope/event builders, `make_candidate_summary`,
  `delete_all_rows`/`REQUEST_CASCADE_RESET_TABLES`, `make_socket_file`,
  `hermetic_beets_config_defaults`, `own_transfer_keys`,
  `seed_visible_wrong_match`/`SeededWrongMatch`, `make_requests_http_error`

**`tests/dispatch_helpers.py`** — dispatch/import-lane support (split out of
`tests/helpers.py`, #1278):
- `make_dispatch_request(**overrides)` — the `DispatchRequest` every `dispatch_import_core` / `_reject_import_from_evidence_decision` test constructs through (#1277). Its optional defaults ARE the dataclass's own, pinned field-by-field by `tests/test_dispatch_request.py`.
- `finalize_claimed_dispatch(db, job, outcome)` — applies a computed dispatch outcome (or a raising `BaseException`) through the production queue owner
- `claim_next_import_job` / `claim_next_import_preview_job` — one-shot claim conveniences over the production candidate-scan API
- `handoff_automation_owner(db, request_id)` — the real `wanted -> downloading -> processing` transcript; never inserts an owner directly
- `pinned_dispatch_authority` / `make_database_source_with_fake_db` — fixture→production bridges
- `patch_dispatch_externals()` — context manager for the 4 dispatch external patches
- `noop_quality_gate(**kwargs) -> None` — drop-in `quality_gate_fn` stub for dispatch tests that don't care about the post-import gate. Pair with `dispatch_import_core(..., quality_gate_fn=noop_quality_gate)`.
- `RecordingQualityGate()` — recorder `quality_gate_fn` with `assert_called_once()` / `assert_not_called()` / `call_count` / `calls` (list of kwargs). For tests that assert the gate ran with specific args.

**`tests/evidence_helpers.py`** — `AlbumQualityEvidence`-family builders
(split out of `tests/helpers.py`, #1278):
- `make_album_quality_evidence(...)` — production-shaped content-addressed evidence row (fingerprint computed by the canonical helper)
- `build_parity_candidate_evidence` / `build_parity_current_evidence` — the canonical simulator-world → evidence-row mapping shared by the hand-written parity tests and the generated parity property
- `make_aac_lattice_capture(...)` — AAC-lattice capture through the production derivation
- `make_audio_corrupt_validation_report(...)`, `PROVISIONAL_LANE_DECISIONS`

**`tests/fakes/`** — stateful fakes:
- `FakePipelineDB` — full PipelineDB stand-in: requests, download_logs, denylist, cooldowns, status history, spectral state, attempt counters. Includes `assert_log()` helper. Has `queue_execute_results(*cursors)` + `execute_calls` recording for tests driving raw-SQL CLI paths.
- `FakeBeetsDB` — minimal BeetsDB stand-in: `album_exists`, `get_album_info(mb_release_id, cfg)`, `get_all_album_ids_for_release`, `get_item_paths`, `get_album_path_by_id`, `close` + context-manager + per-method call recorders + seed helpers (`set_album_exists`, `set_album_info`, `set_album_ids_for_release`, `set_item_paths`, `set_album_path_by_id`). Each method also has a `_default` field for "any key returns the same value" tests. Extend the surface only when a test exercises a new BeetsDB method.
- `BeetsContractWorld` (`tests/fakes/beets_contract.py`) — real immutable config/include/state/library authority fixture, shared by deterministic, generated, startup, harness, and web-boundary tests. Includes tree snapshots and the canonical safe path constants.
- `tests/beets_config_startup_support.py` — shared real-entrypoint restart/admission helpers used by deterministic, generated, importer, and web startup-boundary tests.
- `FakeSlskdAPI` — stateful slskd client: `transfers` (enqueue, get_all_downloads, cancel_download, queued snapshots), `users` (directory with per-directory results and errors), call recording.
- `FakePipelineDBSource` — typed PipelineDBSource fake wrapping a `FakePipelineDB`. Use via `make_ctx_with_fake_db(fake_db)` rather than constructing directly.

**`tests/web/`** — per-route-module contract tests mirroring `web/routes/*.py`. Shared harness in `tests/web/_harness.py` (`_FakeDbWebServerCase` with a per-test bare `FakePipelineDB` as `self.db`, `_get`/`_post` helpers, `_assert_required_fields`, `_fresh_triage_runner`); `TestRouteContractAudit` guard in `tests/web/test_route_audit.py`.

**`tests/js_harness.mjs`** — the one JavaScript test harness (#1313). `suite(import.meta.url)` returns the checker every `tests/test_js_*.mjs` file uses; `stubGlobals`/`domStub`/`element` replace hand-written browser-global stubs. One exit path (`done()`) emits one `CRATEDIGGER_JS_FAILURE` marker per failed assertion, so a JS failure lands in the suite's failure index named per assertion instead of "this file failed". Idiom and rules: `.claude/rules/web.md`; enforced by `tests/test_js_suite_audit.py`.

### General test rules

#### Mocks: leaf-seam only

Use `MagicMock` and `patch(...)` only at external process, network, filesystem,
time, or third-party edges. Stateful collaborators use the repository's
`FakePipelineDB`, `FakeBeetsDB`, and `FakeSlskdAPI`; pure decisions run with
real inputs. `tests/test_mock_audit.py` rejects the bounded syntax it can
identify, while review owns semantically equivalent evasions.

A repository wrapper is allowlistable only when it is at most ten lines and
mostly forwards to an external edge. Give every allowlist entry a one-line
rationale; never allowlist a pure decision.

**Picking a strategy when you'd otherwise want to patch our own code:**

1. **Real inputs (best).** Construct values that produce the branch you need. Borrow fixtures from the decision's dedicated unit tests.
2. **Kwarg-DI seam.** Mid-tier helpers can accept the dependency as a kwarg with the production function as the default. Canonical examples in this repo: `try_enqueue(match_fn=)`, `dispatch_import_core(quality_gate_fn=)`, `_handle_valid_result(dispatch_fn=)`, `check_for_match(album_match_fn=, cross_check_fn=)`, `_collect_issues(find_orphaned_fn=, find_blocked_recovery_fn=)`.
   **Definition-time defaults are injected, never patched.** When a dependency is captured in a function default, tests must pass the replacement explicitly (for example, `try_enqueue(..., match_fn=recorder)`) and assert the fake or recorder's call contract. Patching the module binding later does not replace Python's captured default and is forbidden. Enforce this in review and concrete behavior tests; do not add a structural AST or dataflow audit that tries to reproduce Python binding or execution semantics.
3. **Module-local DI seam (only for URL or argparse dispatchers).** When the entry point can't take a kwarg, bind the dependency at the calling module's top: `finalize_request = transitions.finalize_request`. Tests patch the module attribute. Allowlist the binding. Canonical examples: `web.routes.pipeline_mutations.finalize_request`, `scripts.pipeline_cli.album_requests.finalize_request` (and its twin `scripts.pipeline_cli.quality.finalize_request` — the #495 CLI package split the single binding into one per command-family module that calls it), `scripts.repair._collect_issues`.
4. **Allowlist (last resort).** Only if the target is a thin wrapper around an external boundary.

**Other test rules:**
- **Equivalence proof when removing a test.** Note in the commit message what behaviour was covered, where it's covered now, what branch is still protected.
- **Short docstrings.** One line is fine. Long `NOTE:` paragraphs justifying a test's existence are a smell — restructure the test or move the explanation to the PR.
- **Builders for structured data.** Hand-rolled dicts with many fields drift silently when the schema evolves.
- **No new bespoke harnesses.** If existing fakes/builders/helpers don't fit, extend them and update this rule. Don't write a one-off.

## Pre-Commit Review Gate
- For non-trivial changes (new structs, refactored function signatures, new pipeline paths), review the complete diff before committing.
- Check correctness bugs, test gaps, missed callers, type errors, unfinished wiring, and production-shape drift. Use the active agent's native review capability; no specialist review workflow is required.
- Docs freshness: does this diff make any README / `docs/` / `examples/` / CLAUDE.md statement wrong or incomplete, or ship a documented surface (a new option / plugin / CLI subcommand / behavior) undocumented? `test_docs_audit.py` catches structural gaps; the reviewer catches stale prose the audit can't see.
  - **Search prose by the artifact, not only by the identifier.** Docs name a
    thing the way an operator meets it — a secret path, a URL, a systemd unit,
    a header — not by the option or symbol that governs it. A grep for the new
    identifier therefore returns clean while a paragraph about the same
    behaviour goes stale. Grep the artifacts the change touches too, and treat
    an empty result as "wrong pattern" until one search has actually hit the
    file you expect. Incident: #924's `web.externalAuth` cleared a grep for
    `basicAuthFile|enableInsecure|externalAuth` across README, `docs/`, and
    `examples/`, while `README.md` told every operator to provision
    `/run/secrets/cratedigger.htpasswd` before the first switch — untrue for
    the new mode, and found only by grepping `htpasswd`.
  - A symlink does not widen grep scope: `grep -r` and `rg` both skip symlinks
    during recursive traversal, so `AGENTS.md` never matches on `CLAUDE.md`'s
    content without an explicit `-R`/`-L`. Do not rely on one for coverage.
- **No round catches its own false claims.** A commit's author must
  re-read every claim (comment, docstring, PR prose) that commit itself
  added before committing it, then the next independent read — human or
  agent — must re-read it again. This matters most on a correction
  commit, where confidence is highest and scrutiny lowest right where a
  new false claim can hide. Earned four times in one batch (#1101,
  #1102, #1107, #1110); in each, the false claim was caught only by the
  next independent read, never by the round that wrote it.
- **Briefs are an unreviewed claim surface.** Every other artifact in this
  pipeline gets an independent read — a commit, a PR, a test. The brief an
  orchestrator hands an implementer does not; the implementer treats it as
  settled fact and builds on it. Two things fix it: a brief separates what
  its author MEASURED (ran it, read the actual output) from what they
  INFERRED (a plausible causal claim resting on a measurement); and the
  implementer verifies a brief's load-bearing claims before building on
  them, rather than taking them as given. Three false claims originated in
  briefs during the #1211 series (issue #1246,
  `.claude/memory/feedback_orchestrator_briefs_become_defects.md`). Two
  shipped and were caught only by an independent reviewer: a real count
  (71 `db=… # type: ignore[arg-type]` sites) with a false causal claim
  attached (that two `tests/helpers.py` bridges "exist to replace" them —
  zero of the 71 sit at either bridge's target); and a citation naming an
  identifier (`BASELINE`) that exists nowhere, at a line that was not the
  mechanism being described. The third — an instruction to write "the
  other seven sites reference it by name" when the true count was five —
  never shipped: the implementer verified the count before writing it and
  caught the error. That third instance is the rule working as intended,
  not another failure of it.
- **Independent review of a non-trivial PR is TWO agents with disjoint
  jobs, not one agent doing both.** Operator decision 2026-08-25, after
  PR #1257's review round supplied the evidence
  (https://github.com/abl030/cratedigger/pull/1257#issuecomment-5404791514):
  the round's one real production bug was found by close reading (a
  missing carve-out spotted by comparing adjacent arguments on the same
  call), while every mutant finding was a TEST-lattice gap — and a single
  agent's mutant quota taxes exactly the reading budget that finds bugs.
  The roles:
  - **The reader** reads, thinks, and prods production code: re-derives
    the PR's load-bearing claims, cross-references the touched namespaces
    (readers/writers, per the widest-boundary rule), checks composition
    seams, re-reads every claim the series itself added, greps docs by
    artifact, and audits the author's mutmut survivor dismissals — each
    "equivalent" verdict re-derived like any other claim (#1317). It
    plants NO mutants — its entire budget is thought. It MAY
    name suspect tests ("this looks like a bystander — prove it
    constrains X") as extra targets for the runner. Output: ranked
    findings, each labeled MEASURED or INFERRED. Model tier: the
    quality-core reader runs fable; opus elsewhere.
  - **The mutant runner** plants at least two mutants PER new or changed
    test, aimed at that test's named subject, not only at sites the
    author's Fault injection sentence already covers (issue #1143 /
    #1155), plus any suspects the reader handed over. Tests the author
    added to kill mutmut survivors are new tests and get the same two
    aimed mutants — no round certifies its own pins — and the author's
    mutmut catalog tally discharges none of this quota: aimed shapes
    (argument swaps, past-fix reverts, adapter derivations, JS) are
    outside the catalog (#1317). Mechanical
    discipline, not judgment: `PYTHONDONTWRITEBYTECODE=1`, its OWN
    isolated worktree (mutant planting mutates production files — a
    shared tree makes the reader read lies), every edit restored exactly
    and proven restored (`git status --porcelain` empty; when the file
    carries uncommitted work, restore by INVERSE EDIT — `git checkout
    <file>` restores from HEAD and silently wipes that work, #1270),
    and a final
    table where every row carries the actual command evidence for
    KILLED/SURVIVED — a prose claim of RED without output is the #1209
    confabulation shape and counts as no evidence. A SURVIVOR is a
    finding, never a footnote. The work needs no premium model tier.
  The two run in parallel against the same commit. One agent may still do
  both jobs on a small, low-risk diff — the split is mandatory where the
  old single-reviewer rule demanded mutants at all, i.e. non-trivial PRs
  with new or changed tests.
- Fix everything the review finds before committing. This is not
  optional. (Severity still orders the work: a reader finding on
  production code outranks a runner's minor survivor.)

## Commits & PRs
- One logical change per commit
- Non-trivial work goes on a feature branch with a PR (e.g. `feat/cooldowns`, `fix/spectral-race`)
- PRs are merged via GitHub **Create a merge commit** (not Rebase-and-merge, not Squash-and-merge). This keeps the PR attached to mainline history while preserving the individual commits, so write them well.
- Deploy and verify live after merging
- PR body follows `.github/pull_request_template.md`. The Fault injection section is a short account (one sentence, or a short list for per-clause proof against a many-clause checker) naming what you tried and what happened, not an exhaustive table; whether to run fault injection at all is the "when in doubt" judgment call in § "Testing — Red/Green TDD", not something every PR owes — but the obligations in that same section stay unconditional regardless: the regression-pin rule, the adapter mutant rule, and Standing scope (a PR adding or changing a checker clause records per-clause evidence there, not "N/A"). The mutant RUNNER's two-mutants-per-changed-test obligation (§ "Pre-Commit Review Gate", the two-reviewer split) is separate and always applies. A diff with mutable Python production surface also reports its mutmut breadth-pass tally and survivor dismissals in the same section (§ "Testing — Red/Green TDD"; runbook in `docs/mutation-testing.md`).
