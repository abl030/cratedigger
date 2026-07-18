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
  preserving operator search state current when the request row is locked.
  Every terminal ``wanted`` transition uses that arbitration, including
  rejection and local-completion bundles; policy fields and attempt/backoff
  accounting still apply without clearing the stop. CLI/API lifecycle actions
  retry a stale compare-and-set against the post-terminal status so an
  operator command queued behind the row lock is not lost.

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

**Never scope the final Pyright check to just the files you touched.** Run
`nix-shell --run "pyright --threads 4"` on the full repo before the first branch
push. Pre-existing errors are not someone else's problem — they accumulate as
drift the moment you decide they're not yours to fix. Fixing each one is cheap;
the expensive part is re-discovering them later and arguing about ownership.
Triaging "is this mine or pre-existing?" via `git checkout` costs more tokens
than just fixing it. **The repo is either 0-errors or it is not. Make it
0-errors.**

- All new dataclasses, functions, and module-level code must pass pyright with 0 errors
- Use typed dataclasses (not dicts) for structured data crossing module boundaries
- **No dual-interface types.** Never add `__getitem__`, `.get()`, or `isinstance(x, dict)` dispatch to a dataclass. If a function receives both dicts and dataclasses, that is a type error — fix the callers, not the receiver. Temporary bridges become permanent bugs.
- If a function parameter is untyped and accepts multiple representations (dict or dataclass), type it and fix all callers to pass the correct type
- Inner data structures must also be typed — no `list[dict]` when a dataclass exists
- For focused feedback during implementation, use
  `pyright --threads 4 <files>`; the final check still covers the whole repo

## HTTP request bodies — use `pydantic.BaseModel`

Inbound HTTP request bodies in `web/routes/*.py` go through `pydantic.BaseModel` (v2) at the handler entry. Pydantic stops at the route layer; internal types stay `msgspec.Struct` / `@dataclass` per the next section. Enforced by `tests/test_pydantic_route_audit.py` — any `post_*` handler that reads the raw `body` dict instead of going through `parse_body` fails the audit (small allowlist with a rationale per entry).

Why Pydantic at this seam: `ValidationError.errors()` gives structured field-path errors the frontend renders directly, and the `*Request` model declared above the handler is the operator-facing contract.

Pattern (canonical: `PipelineAddRequest` + `post_pipeline_add` in `web/routes/pipeline.py`):

```python
from pydantic import BaseModel, model_validator
from web.routes._pydantic import parse_body

class MyRouteRequest(BaseModel):
    foo: str
    bar: int | None = None

def post_my_route(h, body: dict) -> None:
    req = parse_body(h, body, MyRouteRequest)
    if req is None:
        return
    # req.foo / req.bar are typed; use them directly
```

`parse_body` (in `web/routes/_pydantic.py`) is the single adapter that turns `ValidationError → 400` with `{"error": "<field>: <msg>", "errors": [...]}`. Use it; do not catch `ValidationError` inline.

Scope:
- HTTP request bodies → Pydantic.
- Query strings, response shapes, internal types → unchanged.
- Pydantic v2's lax mode coerces `"true"`/`"false"` strings to bool; use `Field(strict=True)` when the route's contract is JSON-bool only.

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
  - **Generated tests** (Hypothesis, `docs/generated-testing.md`) — if the
    feature has a generated-testable surface (pure decisions, lifecycles /
    state machines, wire or event ingestion), the property + strategy ship
    in the SAME PR as the feature, not as a follow-up. An invariant that
    only lives in prose is not an invariant.
- **An invariant ships as a PAIR: one deterministic pin AND one generated
  property — same PR, no exceptions.** The pin proves the exact scenario;
  the property patrols the world space around it. Finding and defining an
  invariant and then only pinning it is 90% of the race and then sitting
  down (lesson: PR #560 shipped the #550-phase-2 isolation invariant with
  a deterministic pin only; PR #561 had to retrofit the property — which
  a single-point mutant immediately proved was the load-bearing half).
  Subagent implementation briefs state the pair requirement verbatim and
  never offer a deterministic-only alternative.
- **Every invariant checker owes a known-bad self-test**: a planted
  violating decision/state proving the checker trips. A property that has
  never failed anything is unfalsifiable until proven otherwise. Keep
  checkers as module-level functions so the self-test can call them
  directly (pattern: `TestInvariantCheckersTripOnViolations` in
  `tests/test_quality_generated.py`).
- **Qualify the harness by fault injection when in doubt.** "Do these tests
  actually constrain the code?" is an empirical question: plant mutants in
  production code (revert a real past fix; break an adapter derivation;
  flip a decision comparison; remove an early-exit guard; target each
  property's claimed coverage) and run the relevant tests against each. A
  surviving mutant is either a missing invariant (add it) or an entropy
  budget miss (pin the decisive world as an `@example`). The driver is an
  operator/agent one-shot — never committed (`scope.md`); record the kill
  matrix in the issue/PR. Canonical run: issue #548, 2026-07-08 — 13
  mutants, incl. reverting fix `6cf26a4`, led to PR #555.
- During implementation, run focused modules:
  `nix-shell --run "python3 -m unittest tests.<module> -v"`
- After the final tree is reviewed and committed, run
  `nix-shell --run "pyright --threads 4"` and then
  `nix-shell --run "bash scripts/run_tests.sh"` exactly once before the first
  branch push. Both must pass on the pushed tree.
- If final validation finds a problem, fix it, reconverge with focused tests,
  commit and review the new tree, then restart the final sequence. Do not replay
  the final suite for an unchanged tree after push or merge.

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
- Contract tests use the real `_FakeDbWebServerCase` harness (HTTPServer on a random port + a fresh bare `FakePipelineDB` installed as `web.server.db` per test) — see existing `TestPipelineRouteContracts`, `TestBrowseRouteContracts`, etc. as reference patterns. Seed state (`self.db.seed_request(...)`, `self.db.log_download(...)`) and assert against the fake's real query semantics — never configure mock returns (#430; the `WEB_HARNESS_MOCK_BASELINE` ratchet in `tests/_mock_audit_scanner.py` is permanently empty and bans `mock_db` references in tests/web outright)
- Define a `REQUIRED_FIELDS` set per endpoint — the fields the frontend JS relies on
- Assert every returned dict includes all required fields via `_assert_required_fields(self, payload, REQUIRED_FIELDS, "label")`
- When adding a field the frontend needs, add it to `REQUIRED_FIELDS` first (RED), then fix the backend (GREEN)
- **Every new route MUST be declared `classified=True` on its `RouteRegistration`** — the `route(...)`/`pattern_route(...)` entry in the module's `ROUTES` list. Since #496 there is NO hand-maintained `CLASSIFIED_ROUTES` set: `TestRouteContractAudit` (`tests/web/test_route_audit.py`) introspects the merged `web.server.ALL_ROUTES` and fails, by name, on any route missing `classified=True`. The audit makes contract coverage self-enforcing — you cannot ship a route without classifying it, and there's no second list to drift because the classification lives on the route declaration itself.
- The harness in `tests/web/_harness.py` exposes `self._get(path)` and `self._post(path, body)` helpers that hit the real server. Reuse these (subclass `_FakeDbWebServerCase`; set `DB_FACTORY` to a typed `FakePipelineDB` subclass for failure injection) instead of building your own harness — standalone per-class harness copies are exactly the drift #408 removed.
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

## Bug Hunting — Generated-First (the house method)

Proven on #550 (2026-07-08): a live production bug that static analysis and
disk forensics could NOT reproduce was found, reproduced, RCA'd and fixed in
one session by a generated harness driving the real code path. This is how
bugs are hunted here — reach for it BEFORE log-trawling, before speculative
instrumentation, before reading code until a theory falls out.

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
   legitimate behavior, and a known-bad self-test proves the checker trips.
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
- Both surfaces wrap the same service-layer method (e.g. `SearchPlanService.advance_for_request`). The CLI subcommand and the HTTP handler are thin adapters with matched outcome → exit-code / outcome → status-code mappings. Never duplicate logic across the two; route everything through the service.
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
| A feature with policy invariants (pure decisions, lifecycle / state machine, wire or event ingestion) | Generated properties + strategies in the same PR, each invariant checker with a known-bad self-test; invariants written down FIRST | `tests/_hypothesis_profiles.py`, checker/strategy patterns in `tests/test_*_generated.py`, `docs/generated-testing.md` |
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
- `FakeSlskdAPI` — stateful slskd client: `transfers` (enqueue, get_all_downloads, cancel_download, queued snapshots), `users` (directory with per-directory results and errors), call recording.
- `FakePipelineDBSource` — typed PipelineDBSource fake wrapping a `FakePipelineDB`. Use via `make_ctx_with_fake_db(fake_db)` rather than constructing directly.

**`tests/web/`** — per-route-module contract tests mirroring `web/routes/*.py`. Shared harness in `tests/web/_harness.py` (`_FakeDbWebServerCase` with a per-test bare `FakePipelineDB` as `self.db`, `_get`/`_post` helpers, `_assert_required_fields`, `_fresh_triage_runner`); `TestRouteContractAudit` guard in `tests/web/test_route_audit.py`.

### General test rules

# MOCKS: LEAF-SEAM ONLY

`MagicMock` and `patch(...)` are for the **outermost edge** of the test — where our code calls something external we don't own. They are forbidden as a substitute for our own stateful types or our own pure-logic functions. Zero-tolerance: enforced by `tests/test_mock_audit.py` against the allowlist in `tests/_mock_audit_scanner.py`.

**Forbidden:**
- `MagicMock()` assigned to a variable named `db`, `mock_db`, `failing_db`, `pdb`, `ctx`, `context`, `beets`, `beets_db`, `source`, `pipeline_db`, `slskd`, `fake_db`. Use `FakePipelineDB` / `FakeBeetsDB` / `FakeSlskdAPI` from `tests/fakes/`.
- `patch("lib.X.our_function")` for any target not on the allowlist. If you're mocking your own logic, you're testing the mock.
- `patch("lib.beets_db.BeetsDB.<method>")`. The class constructor is allowlisted; per-method stubbing isn't — use `FakeBeetsDB`.
- Allowlisting a pure decision function. Drive the test with real inputs that produce the branch you care about — fixtures usually already exist in the decision's own coverage.

**Allowed leaf seams (mock freely, never tripped by the audit):**
Subprocess, urllib/requests, third-party libs (`music_tag`, `redis`), `os.path.*`, `shutil.*`, `time.sleep`, threading/signal primitives, fire-and-forget notifier helpers (`lib.util.trigger_*_scan`), module-level `logger` objects, and ergonomic envelopes (`args = MagicMock()` for parsed argparse args, `proc = MagicMock()` for subprocess return-value structs).

**Allowed thin seam-wrappers (allowlisted in `_mock_audit_scanner.py`):**
A function in `lib/` is a legitimate seam-wrapper iff its body is **≤10 lines AND mostly forwards to a process / network / filesystem boundary**. Fatter than that means pure logic with a side effect — drive it with a fake, not a patch.

When adding a wrapper to the allowlist, include a one-line rationale next to the regex. The list is the contract.

**Picking a strategy when you'd otherwise want to patch our own code:**

1. **Real inputs (best).** Construct values that produce the branch you need. Borrow fixtures from the decision's dedicated unit tests.
2. **Kwarg-DI seam.** Mid-tier helpers can accept the dependency as a kwarg with the production function as the default. Canonical examples in this repo: `try_enqueue(match_fn=)`, `dispatch_import_core(quality_gate_fn=)`, `_handle_valid_result(dispatch_fn=)`, `check_for_match(album_match_fn=, cross_check_fn=)`, `_collect_issues(find_orphaned_fn=, find_blocked_recovery_fn=)`.
   **Definition-time defaults are injected, never patched.** When a dependency is captured in a function default, tests must pass the replacement explicitly (for example, `try_enqueue(..., match_fn=recorder)`) and assert the fake or recorder's call contract. Patching the module binding later does not replace Python's captured default and is forbidden. Enforce this in review and concrete behavior tests; do not add a structural AST or dataflow audit that tries to reproduce Python binding or execution semantics.
3. **Module-local DI seam (only for URL or argparse dispatchers).** When the entry point can't take a kwarg, bind the dependency at the calling module's top: `finalize_request = transitions.finalize_request`. Tests patch the module attribute. Allowlist the binding. Canonical examples: `web.routes.pipeline_mutations.finalize_request`, `scripts.pipeline_cli.album_requests.finalize_request` (and its twin `scripts.pipeline_cli.quality.finalize_request` — the #495 CLI package split the single binding into one per command-family module that calls it), `scripts.repair._collect_issues`.
4. **Allowlist (last resort).** Only if the target is a thin wrapper around an external boundary.

**Adding a new `PipelineDB` / `BeetsDB` / `SlskdAPI` method:**
1. Add a state-respecting implementation to the corresponding `Fake*` in `tests/fakes/`.
2. Add a self-test in `tests/test_fakes.py`.
3. Tests consume the fake; they do NOT do `mock_db.new_method.return_value = ...`.

**Other test rules:**
- **Equivalence proof when removing a test.** Note in the commit message what behaviour was covered, where it's covered now, what branch is still protected.
- **Short docstrings.** One line is fine. Long `NOTE:` paragraphs justifying a test's existence are a smell — restructure the test or move the explanation to the PR.
- **Builders for structured data.** Hand-rolled dicts with many fields drift silently when the schema evolves.
- **No new bespoke harnesses.** If existing fakes/builders/helpers don't fit, extend them and update this rule. Don't write a one-off.

## Pre-Commit Review Gate
- For non-trivial changes (new structs, refactored function signatures, new pipeline paths), review the complete diff before committing.
- Check correctness bugs, test gaps, missed callers, type errors, unfinished wiring, and production-shape drift. Use the active agent's native review capability; no specialist review workflow is required.
- Docs freshness: does this diff make any README / `docs/` / `examples/` / CLAUDE.md statement wrong or incomplete, or ship a documented surface (a new option / plugin / CLI subcommand / behavior) undocumented? `test_docs_audit.py` catches structural gaps; the reviewer catches stale prose the audit can't see.
- Fix everything it finds before committing. This is not optional.

## Commits & PRs
- One logical change per commit
- Use focused tests while implementing. Review and commit the final tree, then
  run whole-repo threaded Pyright and the full suite once immediately before
  the first branch push.
- Non-trivial work goes on a feature branch with a PR (e.g. `feat/cooldowns`, `fix/spectral-race`)
- PRs are merged via GitHub **Create a merge commit** (not Rebase-and-merge, not Squash-and-merge). This keeps the PR attached to mainline history while preserving the individual commits, so write them well.
- Deploy and verify live after merging
