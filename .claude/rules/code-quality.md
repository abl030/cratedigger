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

`tests/test_typing_ratchet.py` separately requires explicit `Any`, `cast(...)`,
`# type: ignore`, and bare `# pyright: ignore` debt to match the production and
tests baselines exactly and only decrease. Scoped
`# pyright: ignore[rule]` is the sanctioned form. When a baseline reaches zero,
delete its generated baseline/regeneration path and retain a direct
zero-tolerance check; delete the scanner only when a configured tool enforces
the same syntax.

When either typing ratchet trips, do not stop at making it green. For every
affected file and finding kind, finish with the committed count at least ten
below the baseline at the start of the change, or at zero if fewer than ten
remain, then regenerate the baseline. This is deliberately a convergence rule,
not another checker: do not add stable-site, diff-aware, or history-aware
machinery to catch delete-and-add laundering.

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

- **Use `msgspec.Struct`** for: harness/subprocess JSON messages, external API responses, DB JSONB rows we read back and type-check, any type that is ever encoded back out to JSON. Reference implementations: `HarnessItem`, `HarnessTrackInfo`, `TrackMapping`, `CandidateSummary`, `ChooseMatchMessage`, `ImportResult`, `PostflightInfo`, `ConversionInfo`, `SpectralDetail`, `AudioQualityMeasurement`, `MovedSibling`, `ValidationResult`, `ActiveDownloadState` (all re-exported from `lib.quality`; defined across `lib/quality/`), `BeetsOpFailure` in `lib/beets_album_op.py`.
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
  render path really gets. PR #973 proved why: the faulty lineage-gated input
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
  self-test has proven exactly one of them. Two questions per clause, in
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
  Keep checkers as module-level functions so the self-test can call them
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
  records the kill matrix in the PR. The audit examines test
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
  operator/agent one-shot — never committed (`scope.md`); record the kill
  matrix in the issue/PR. Canonical run: issue #548, 2026-07-08 — 13
  mutants, incl. reverting fix `6cf26a4`, led to PR #555. **When you
  inject at all, cover every site the diff adds, named individually — a
  killed mutant at one site does not qualify any other** (the "Agree by
  construction" rule above, generalized from claimed adapters to diff
  sites). Lesson (#1110): the implementer reported reverting each fix and
  watching its own pin go red — true only of `renderConvergeControls`;
  mutants at the three `deleteWrongMatchGroup` restore paths and at
  `removeWrongMatchEntry` survived every JS assertion, and one recreated
  the very dead end the PR existed to remove.

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
`SHARED_MODULES_WITHOUT_COVERAGE`; one with none of those fails the whole
entrypoint closed (`scripts/test.sh` exit code 2) before any phase runs —
silent under-selection there is worse than a loud refusal. This is development
feedback; the full suite remains the exhaustive pre-review boundary.

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

## Frontend (JavaScript)
- ES6 modules in `web/js/` — no inline `<script>` in HTML
- `// @ts-check` + JSDoc types on all exported functions
- Pure functions in `web/js/util.js` — testable via Node without DOM
- Shared state in `web/js/state.js` — no bare globals across modules
- Cross-module onclick handlers go through `window.*` bindings in `main.js`
- `node --check web/js/*.js` must pass (runs in pre-commit + CI)
- JS unit tests in `tests/test_js_util.mjs` — run with `node`, no npm
- Static JS served at `/js/*.js` by server.py

## Backend (Server Routes)
- Route handlers in `web/routes/*.py` — server.py is routing/cache/main only
- Route functions take `(handler, params)` or `(handler, body)`, not `self`
- All beets queries go through `lib/beets_db.py` `BeetsDB` class — no raw `sqlite3.connect()` in handlers
- Route modules access server globals via `_server()` deferred import

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
- Status/exit-code convention to match: `200/0` success, `400/3` input validation (API only — CLI argparse covers this), `404/2` not found, `409/4` wrong state, `422/3` semantic violation, `503/5` transient/retryable.
- See `CLAUDE.md` § "CLI ⇄ API surface symmetry" for the full pattern table.

## New Work Checklist (read this first)

Before writing any new code, decide which test types you owe and what infrastructure you'll reuse:

| You're adding... | You owe... | Use this infrastructure |
|------------------|-----------|-------------------------|
| A new pure decision function in `lib/quality/` | A subTest table covering every branch | `tests/test_quality_decisions.py` patterns |
| A new dispatch / orchestration path | An orchestration test asserting domain state + an integration slice | `FakePipelineDB`, `patch_dispatch_externals()`, `tests/test_integration_slices.py` |
| A new web API endpoint | A contract test with `REQUIRED_FIELDS` AND `classified=True` on the route's `RouteRegistration` (enforced by `TestRouteContractAudit`) AND a paired `pipeline-cli` subcommand (CLI ⇄ API symmetry) | `_FakeDbWebServerCase` + `_assert_required_fields` from `tests/web/_harness.py`, the matching `tests/web/test_routes_<module>.py`, `scripts/pipeline_cli/` |
| A new operator action (CLI subcommand or API endpoint) | A service-layer method with a typed `Result`, BOTH a CLI subcommand AND an API endpoint, exit-code and status-code tests for each | `tests/test_<service>.py` for the authoritative coverage; CLI/API tests check the wrapper only |
| A new slskd interaction | An orchestration test using `FakeSlskdAPI` | `FakeSlskdAPI` from `tests/fakes/` |
| A new typed dataclass | A pure test of construction + serialization, and a builder in `tests/helpers.py` if it crosses test boundaries | `tests/helpers.py` |
| A new `PipelineDB` method | An equivalent stub on `FakePipelineDB`, with a self-test in `tests/test_fakes.py` | `tests/fakes/`, `tests/test_fakes.py` |
| A new `BeetsDB` method | Either (a) an equivalent stub on `FakeBeetsDB` with a self-test in `tests/test_fakes.py::TestFakeBeetsDB`, OR (b) drive the test against a real test SQLite DB if it's a read-only query | `tests/fakes/`, `tests/test_fakes.py` |
| A feature with policy invariants (pure decisions, lifecycle / state machine, wire or event ingestion) | Generated properties + strategies in the same PR, a message-asserting known-bad self-test for every CLAUSE of every invariant checker; invariants written down FIRST | `tests/_hypothesis_profiles.py`, checker/strategy patterns in `tests/test_*_generated.py`, `docs/generated-testing.md` § "Per-clause proof" |
| A documented surface (a module option, a beets plugin, an operator action / CLI subcommand, the permission/ownership model, or a subsystem's documented behavior) | The doc update in the SAME PR (README / `docs/` / `examples/` / CLAUDE.md) — docs are part of done, not a follow-up | `tests/test_docs_audit.py` (structural coverage: plugins, CLI, dead-links, option descriptions); the relevant `docs/*.md` |

Routes are the strictest gate: `TestRouteContractAudit` will fail at test time if you add a route to `web/routes/` without classifying it. This is intentional — it prevents shipping endpoints the frontend can rely on without contract coverage.

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
- For dispatch tests, use `patch_dispatch_externals()` from `tests/helpers.py` — it patches the 5 external edges (`sp.run`, `_cleanup_staged_dir`, `trigger_plex_scan`, `trigger_jellyfin_scan`, `cleanup_disambiguation_orphans`) and yields a `SimpleNamespace` with mock references. Add your own test-specific patches inside the `with` block.

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
- **Use `FakePipelineDB` from `tests/fakes/` for stateful collaborators instead of MagicMock.** It records request rows, download_logs, denylist entries, cooldowns, status history, spectral state updates. See `tests/test_fakes.py` for the full API.
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

**`tests/helpers.py`** — builders + helpers:
- `make_request_row(**overrides)` — full album_requests row dict
- `make_import_result(decision=..., new_min_bitrate=..., ...)` — `ImportResult` dataclass
- `make_validation_result(**overrides)` — `ValidationResult` dataclass
- `make_download_info(...)` — `DownloadInfo` dataclass
- `make_download_file(...)` — real `DownloadFile` (not MagicMock)
- `make_grab_list_entry(...)` — real `GrabListEntry`
- `make_ctx_with_fake_db(fake_db)` — `CratediggerContext` wired to a fake
- `patch_dispatch_externals()` — context manager for the 6 dispatch external patches
- `noop_quality_gate(**kwargs) -> None` — drop-in `quality_gate_fn` stub for dispatch tests that don't care about the post-import gate. Pair with `dispatch_import_core(..., quality_gate_fn=noop_quality_gate)`.
- `RecordingQualityGate()` — recorder `quality_gate_fn` with `assert_called_once()` / `assert_not_called()` / `call_count` / `calls` (list of kwargs). For tests that assert the gate ran with specific args.

**`tests/fakes/`** — stateful fakes:
- `FakePipelineDB` — full PipelineDB stand-in: requests, download_logs, denylist, cooldowns, status history, spectral state, attempt counters. Includes `assert_log()` helper. Has `queue_execute_results(*cursors)` + `execute_calls` recording for tests driving raw-SQL CLI paths.
- `FakeBeetsDB` — minimal BeetsDB stand-in: `album_exists`, `get_album_info(mb_release_id, cfg)`, `get_all_album_ids_for_release`, `get_item_paths`, `get_album_path_by_id`, `close` + context-manager + per-method call recorders + seed helpers (`set_album_exists`, `set_album_info`, `set_album_ids_for_release`, `set_item_paths`, `set_album_path_by_id`). Each method also has a `_default` field for "any key returns the same value" tests. Extend the surface only when a test exercises a new BeetsDB method.
- `BeetsContractWorld` (`tests/fakes/beets_contract.py`) — real immutable config/include/state/library authority fixture, shared by deterministic, generated, startup, harness, and web-boundary tests. Includes tree snapshots and the canonical safe path constants.
- `tests/beets_config_startup_support.py` — shared real-entrypoint restart/admission helpers used by deterministic, generated, importer, and web startup-boundary tests.
- `FakeSlskdAPI` — stateful slskd client: `transfers` (enqueue, get_all_downloads, cancel_download, queued snapshots), `users` (directory with per-directory results and errors), call recording.
- `FakePipelineDBSource` — typed PipelineDBSource fake wrapping a `FakePipelineDB`. Use via `make_ctx_with_fake_db(fake_db)` rather than constructing directly.

**`tests/web/`** — per-route-module contract tests mirroring `web/routes/*.py`. Shared harness in `tests/web/_harness.py` (`_FakeDbWebServerCase` with a per-test bare `FakePipelineDB` as `self.db`, `_get`/`_post` helpers, `_assert_required_fields`, `_fresh_triage_runner`); `TestRouteContractAudit` guard in `tests/web/test_route_audit.py`.

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
- Fix everything it finds before committing. This is not optional.

## Commits & PRs
- One logical change per commit
- Non-trivial work goes on a feature branch with a PR (e.g. `feat/cooldowns`, `fix/spectral-race`)
- PRs are merged via GitHub **Create a merge commit** (not Rebase-and-merge, not Squash-and-merge). This keeps the PR attached to mainline history while preserving the individual commits, so write them well.
- Deploy and verify live after merging
