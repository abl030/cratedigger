# Code Quality Standards

## Type Safety
- All new dataclasses, functions, and module-level code must pass pyright with 0 errors
- Use typed dataclasses (not dicts) for structured data crossing module boundaries
- **No dual-interface types.** Never add `__getitem__`, `.get()`, or `isinstance(x, dict)` dispatch to a dataclass. If a function receives both dicts and dataclasses, that is a type error — fix the callers, not the receiver. Temporary bridges become permanent bugs.
- If a function parameter is untyped and accepts multiple representations (dict or dataclass), type it and fix all callers to pass the correct type
- Inner data structures must also be typed — no `list[dict]` when a dataclass exists
- Verify with: `pyright <files>` on every touched file before committing

## Wire-boundary types — use `msgspec.Struct`, not `@dataclass`
Any type whose **input comes from outside our Python code** (harness stdout, an HTTP response, a JSONB blob coming back from the DB, a subprocess's stdout) must be a `msgspec.Struct` so the deserializer validates the typed contract at the boundary. Pyright does not see inside `dict.get()` — only runtime validation catches int-vs-str drift, mis-typed fields, or missing required data. This is the lesson of issue #99 / PR #98 (every Discogs validation silently logged `mbid_not_found` because a dataclass said `str` but the wire carried `int`).

- **Use `msgspec.Struct`** for: harness/subprocess JSON messages, external API responses, DB JSONB rows we read back and type-check, any other "decode this blob into a typed object" site.
- **Keep `@dataclass`** for: types we construct entirely from our own typed Python code (`ImportResult`, `QualityRankConfig`, `SoularrConfig`, `ActiveDownloadState` when we're the only writer). Their inputs are already typed — the strict boundary buys nothing.
- **Decode at exactly one site.** The wire boundary is the one place the untyped blob becomes a typed object. After that, every downstream consumer works with the Struct directly — no defensive coercion, no `dict.get()`, no re-validation. If you find yourself writing a `_coerce_x` helper on the consumer side, the boundary is in the wrong place.
- **Strict ≠ coerce.** Declare fields as the type you want (`str`, not `str | int`). `msgspec.ValidationError` at the boundary is the detector. Do not use `strict=False` to silently coerce away real type drift.
- **Normalise early if the external source is untidy.** Harness-side `_id_str` in `harness/beets_harness.py` coerces int IDs to str *before* emitting — so the wire is always clean, and the downstream Struct validation never trips in the happy path. Keep normalisation at the source, not at the consumer.
- Reference implementation: `HarnessItem`, `HarnessTrackInfo`, `TrackMapping`, `CandidateSummary`, `ChooseMatchMessage` in `lib/quality.py`; decoded at `lib/beets.py::beets_validate` via `msgspec.convert(msg, type=ChooseMatchMessage)`.
- Tests owe: at least one RED test that feeds the wrong type at the boundary and asserts `msgspec.ValidationError`. This is the regression guard that makes the boundary worth having.

## Testing — Red/Green TDD
- Write tests FIRST (RED), then implement (GREEN)
- Every new function, dataclass, and decision branch needs test coverage
- Use `nix-shell --run "bash scripts/run_tests.sh"` for full suite
- Read `/tmp/soularr-test-output.txt` instead of re-running the 2-minute suite
- For single modules during dev: `nix-shell --run "python3 -m unittest tests.<module> -v"`

## API Contract Tests
- Every API endpoint consumed by the frontend must have a contract test in `test_web_server.py`
- Contract tests use a real `_WebServerCase` harness (HTTPServer on a random port + mocked DB) — see existing `TestPipelineRouteContracts`, `TestBrowseRouteContracts`, etc. as reference patterns
- Define a `REQUIRED_FIELDS` set per endpoint — the fields the frontend JS relies on
- Assert every returned dict includes all required fields via `_assert_required_fields(self, payload, REQUIRED_FIELDS, "label")`
- When adding a field the frontend needs, add it to `REQUIRED_FIELDS` first (RED), then fix the backend (GREEN)
- **Every new route MUST be added to `TestRouteContractAudit.CLASSIFIED_ROUTES`** — this is the guard test that introspects `Handler._FUNC_GET_ROUTES`/`_FUNC_POST_ROUTES`/`_FUNC_GET_PATTERNS` and fails if a registered route is unclassified or a stale entry is missing. The audit makes contract coverage self-enforcing — you cannot ship a route without classifying it.
- The `_WebServerCase` harness in `tests/test_web_server.py` exposes `self._get(path)` and `self._post(path, body)` helpers that hit the real server. Reuse these instead of building your own harness.

## Logging & Auditability
- Every download outcome (success, rejection, timeout, crash) MUST create a download_log row
- Use typed JSON dataclasses (`ImportResult`, `ValidationResult`) — never raw dicts
- Store the full JSON in JSONB columns for SQL queryability
- Never throw away data the harness or subprocess provides — log everything

## Decision Logic
- All quality/import decisions must be pure functions in `lib/quality.py`
- No decision logic inline in soularr.py — call the pure function, branch on result
- Every pure function must have direct unit tests (not just tested through integration)

## Pipeline Decision Debugging — Simulator-First TDD
- When debugging or changing import pipeline behavior (quality gate, backfill, spectral propagation, search tier selection), **always start with the CLI simulator** (`pipeline-cli quality <id>`).
- Add scenarios to the simulator FIRST that expose the bug or show the expected behavior. The simulator is the test suite for pipeline decisions — if you can't see the problem in the simulator output, you don't understand it yet.
- Only edit production code once the simulator scenarios clearly show what's wrong and what "right" looks like. The scenarios tell you what code to change.
- Run the simulator against real albums in the live DB (not mocked state) to verify. Pick albums that represent the edge case: e.g. CBR 320 with no spectral, verified lossless lo-fi, suspect FLAC transcodes.
- The simulator must show the full rejection cycle: import/reject decision → spectral propagation → backfill decision → next search tiers. Not just the import decision in isolation.

## Pipeline Bug Reproduction — Red/Green on Real Code Paths
- When a live pipeline bug involves **interactions between components** (spectral propagation → decision function → DB write → rejection), don't just test the pure decision function in isolation — write a unit test that calls the actual orchestration function (e.g. `lib.preimport.run_preimport_gates`) with mocked state matching the live scenario.
- **RED first**: reproduce the exact live scenario as a test. Mock up the album state from `pipeline-cli show <id>` (status, spectral fields, min_bitrate). Run the test and confirm it fails with the same symptom as production.
- **GREEN**: fix the production code, confirm the test passes.
- **Guard both directions**: add a test for the fixed case AND a test that the original valid behavior still works (e.g. propagation still works when an album IS on disk but lacks spectral data).
- This catches bugs that pure function tests miss — state mutations, propagation ordering, in-memory corruption before the decision function runs.

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

## No Parallel Code Paths
- Never create a second function that calls the same subprocess (import_one.py, beets_harness.py, etc.). If a new entry point needs the pipeline, write an adapter that constructs the existing function's inputs and delegates. If the interface makes this painful, fix the interface — don't route around it.
- Never construct `SoularrConfig` with positional/keyword args for a subset of fields. Always use `SoularrConfig.from_ini()` with the runtime config file. Partial configs silently diverge when new config fields are added.
- Before adding a new function that "does roughly what X does but simpler," check if X can be called with an adapter. The adapter may be ugly — that's a signal to improve X's interface, not to duplicate X.

## New Work Checklist (read this first)

Before writing any new code, decide which test types you owe and what infrastructure you'll reuse:

| You're adding... | You owe... | Use this infrastructure |
|------------------|-----------|-------------------------|
| A new pure decision function in `lib/quality.py` | A subTest table covering every branch | `tests/test_quality_decisions.py` patterns |
| A new dispatch / orchestration path | An orchestration test asserting domain state + an integration slice | `FakePipelineDB`, `patch_dispatch_externals()`, `tests/test_integration_slices.py` |
| A new web API endpoint | A contract test with `REQUIRED_FIELDS` AND an entry in `TestRouteContractAudit.CLASSIFIED_ROUTES` | `_WebServerCase`, `_assert_required_fields`, `tests/test_web_server.py` |
| A new slskd interaction | An orchestration test using `FakeSlskdAPI` | `FakeSlskdAPI` from `tests/fakes.py` |
| A new typed dataclass | A pure test of construction + serialization, and a builder in `tests/helpers.py` if it crosses test boundaries | `tests/helpers.py` |
| A new `PipelineDB` method | An equivalent stub on `FakePipelineDB`, with a self-test in `tests/test_fakes.py` | `tests/fakes.py`, `tests/test_fakes.py` |

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
- For dispatch tests, use `patch_dispatch_externals()` from `tests/helpers.py` — it patches the 6 external edges (`sp.run`, `_cleanup_staged_dir`, `trigger_meelo_scan`, `trigger_plex_scan`, `trigger_jellyfin_scan`, `cleanup_disambiguation_orphans`) and yields a `SimpleNamespace` with mock references. Add your own test-specific patches inside the `with` block.

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
- Mocking is allowed for external edges (subprocess, meelo, plex), but the assertion target must be domain state.
- **Use `FakePipelineDB` from `tests/fakes.py` for stateful collaborators instead of MagicMock.** It records request rows, download_logs, denylist entries, cooldowns, status history, spectral state updates. See `tests/test_fakes.py` for the full API.
- **Use `FakeSlskdAPI` from `tests/fakes.py` for slskd interactions.** Stateful `transfers` and `users` fakes with `add_transfer()`, `queue_download_snapshots()`, `set_directory()`, `set_directory_error()`, configurable errors, and call recording.
- Use `make_ctx_with_fake_db(fake_db)` from `tests/helpers.py` to wire `FakePipelineDB` into a `SoularrContext`.
- Use builders from `tests/helpers.py` — never hand-roll 20-field dicts.

### 4. Integration slice tests
- Use real code paths with lightweight fakes or temp resources.
- Patch only external edges that are truly expensive or unsafe (subprocess, network, BeetsDB).
- Live in `tests/test_integration_slices.py`. Existing slices to model new ones on:
  - `TestDispatchThroughQualityGate` — runs dispatch_import_core → real parse_import_result → real _check_quality_gate_core
  - `TestQualityGateVerifiedLosslessBypass`, `TestQualityGateSpectralOverride`
  - `TestDispatchNoJsonResult`, `TestForceImportSlice`
  - `TestSpectralPropagationSlice` — runs `run_preimport_gates` end-to-end (audio + spectral)
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
- `make_spectral_context(...)` — `SpectralContext`
- `make_ctx_with_fake_db(fake_db)` — `SoularrContext` wired to a fake
- `patch_dispatch_externals()` — context manager for the 6 dispatch external patches

**`tests/fakes.py`** — stateful fakes:
- `FakePipelineDB` — full PipelineDB stand-in: requests, download_logs, denylist, cooldowns, status history, spectral state, attempt counters. Includes `assert_log()` helper.
- `FakeSlskdAPI` — stateful slskd client: `transfers` (enqueue, get_all_downloads, get_download, cancel_download, queued snapshots), `users` (directory with per-directory results and errors), call recording.

**`tests/test_web_server.py`** — `_WebServerCase` harness with `_get`/`_post` helpers + `TestRouteContractAudit` guard.

### General test rules
- **Fakes over mocks for stateful collaborators.** Use `MagicMock` for leaf seams. Use `FakePipelineDB`/`FakeSlskdAPI` when the test reasons about state transitions over time.
- **Equivalence proof for deleted tests.** When removing a test, document in the commit message: what behavior was covered, where it's covered now, what branch is still protected.
- **Short docstrings.** One-line docstrings are fine. Long `NOTE:` paragraphs justifying a test's existence are a smell — extract a helper, move the explanation to the PR, or restructure the test.
- **Builders for structured data.** Hand-rolled dicts with many fields drift silently when the schema evolves.
- **No new bespoke harnesses.** If the existing fakes/builders/helpers don't fit your test, extend them (and update this rule). Don't write a one-off.

## Pre-Commit Review Gate
- For non-trivial changes (new dataclasses, refactored function signatures, new pipeline paths), spawn an Opus agent to review the diff before committing.
- The agent should check: correctness bugs, test gaps, callers you missed, type errors, unfinished wiring.
- Fix everything it finds before committing. This is not optional.

## Commits & PRs
- One logical change per commit
- Run full test suite + pyright before committing
- Non-trivial work goes on a feature branch with a PR (e.g. `feat/cooldowns`, `fix/spectral-race`)
- PRs are merged via **rebase merge** (squash and merge commits are disabled). This preserves individual commit messages on main, so write them well.
- Deploy and verify live after merging
