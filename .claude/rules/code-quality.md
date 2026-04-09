# Code Quality Standards

## Type Safety
- All new dataclasses, functions, and module-level code must pass pyright with 0 errors
- Use typed dataclasses (not dicts) for structured data crossing module boundaries
- **No dual-interface types.** Never add `__getitem__`, `.get()`, or `isinstance(x, dict)` dispatch to a dataclass. If a function receives both dicts and dataclasses, that is a type error — fix the callers, not the receiver. Temporary bridges become permanent bugs.
- If a function parameter is untyped and accepts multiple representations (dict or dataclass), type it and fix all callers to pass the correct type
- Inner data structures must also be typed — no `list[dict]` when a dataclass exists
- Verify with: `pyright <files>` on every touched file before committing

## Testing — Red/Green TDD
- Write tests FIRST (RED), then implement (GREEN)
- Every new function, dataclass, and decision branch needs test coverage
- Use `nix-shell --run "bash scripts/run_tests.sh"` for full suite
- Read `/tmp/soularr-test-output.txt` instead of re-running the 2-minute suite
- For single modules during dev: `nix-shell --run "python3 -m unittest tests.<module> -v"`

## API Contract Tests
- Every API endpoint consumed by the frontend must have a contract test in `test_web_server.py`
- Contract tests use a real in-memory SQLite beets DB (not mocks) to verify actual query results
- Define a `REQUIRED_FIELDS` set per endpoint — the fields the frontend JS relies on
- Assert every returned dict includes all required fields with non-empty values
- When adding a field the frontend needs, add it to `REQUIRED_FIELDS` first (RED), then fix the backend (GREEN)
- See `TestLibraryArtistContract` as the reference pattern

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
- When a live pipeline bug involves **interactions between components** (spectral propagation → decision function → DB write → rejection), don't just test the pure decision function in isolation — write a unit test that calls the actual orchestration function (e.g. `_apply_spectral_decision`) with mocked album state matching the live scenario.
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

## Test Taxonomy

Four categories of tests. Each has different rules for what's acceptable.

### 1. Pure function tests
- Assert direct input → output. No mocks unless unavoidable for environment.
- Should be exhaustive for decision logic (`dispatch_action`, `quality_gate_decision`, etc.).
- Use `subTest()` tables for decision matrices — less copy/paste, easier to audit completeness.

### 2. Seam / adapter tests
- Protect interface boundaries: subprocess argv, config-to-flag wiring, SQL query shape, route contract fields, serialization formats.
- Implementation assertions (call args, payload shape) are **acceptable and encouraged** here.
- Examples: `--force` flag forwarded, `--override-min-bitrate` derived correctly, route returns required fields.
- These are legitimate tests — do not delete them to satisfy an "assert behavior not implementation" rule.

### 3. Orchestration tests
- Must assert **domain outcomes**, not only helper call shapes.
- At least one assertion per test must target persisted state or observable output:
  - request status after the operation
  - `download_log` rows (outcome, fields present)
  - denylist entries written
  - retry / requeue behavior (status transitions)
  - attempt counters incremented
  - `validation_result` / `import_result` preserved
  - filesystem side effects (cleanup, staging)
- Mocking is allowed for external edges (subprocess, meelo, plex), but the assertion target must be domain state.
- Use `FakePipelineDB` from `tests/fakes.py` for stateful collaborators instead of MagicMock.
- Use builders from `tests/helpers.py` (`make_request_row`, `make_import_result`, etc.) — never hand-roll 20-field dicts.

### 4. Integration slice tests
- Use real code paths with lightweight fakes or temp resources.
- Patch only external edges that are truly expensive or unsafe (subprocess, network).
- Required for complex stateful flows and regressions.
- At least one per high-risk orchestration boundary (dispatch, quality gate, spectral propagation).

### General test rules
- **Fakes over mocks for stateful collaborators.** Use `MagicMock` for leaf seams. Use `FakePipelineDB` (or similar) when the test reasons about state transitions over time.
- **Equivalence proof for deleted tests.** When removing a test, document in the commit message: what behavior was covered, where it's covered now, what branch is still protected.
- **Short docstrings.** One-line docstrings are fine. Long `NOTE:` paragraphs justifying a test's existence are a smell — extract a helper, move the explanation to the PR, or restructure the test.
- **Builders for structured data.** Use `make_request_row()`, `make_import_result()`, `make_download_info()` from `tests/helpers.py`. Hand-rolled dicts with many fields drift silently when the schema evolves.

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
