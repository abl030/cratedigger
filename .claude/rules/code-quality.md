# Code Quality Standards

## Quality decisions live in ONE place

**`full_pipeline_decision_from_evidence`** in `lib/quality.py` (and its
flat-kwargs simulator twin `full_pipeline_decision`) is the single source of
truth for every importer decision: the four folder/audio-integrity facts
(`audio_corrupt`, `bad_audio_hash`, `nested_layout`, `empty_fileset`) AND
quality (spectral, codec rank, V0 probe, provisional lossless, verified
lossless, transcode detection, quality gate). **Never re-create import
decisions elsewhere.** If a code path needs to know "should this be
imported", it must call the full pipeline — not invent its own narrower
check.

This bit us twice. First (PR #257): a parallel `preimport_decide` spectral
branch fell back to existing container bitrate when spectral evidence was
missing on one side, rejecting legitimate FLAC provisional-lossless upgrades
(request 4514). Second (evidence-canonical-cleanup, U11): `preimport_decide`
still owned four folder/audio-integrity branches alongside the full
pipeline. That asterisk on "quality decisions live in ONE place" was
hair-splitting; the branches were folded into
`full_pipeline_decision_from_evidence` as early exits.

**Preview produces evidence. Importer decides.** The two-worker contract:

- **Preview worker** (`lib/import_preview.py`): measures via
  `measure_preimport_state` (in `lib/measurement.py`), persists
  `AlbumQualityEvidence`, marks the job `evidence_ready` or
  `measurement_failed`. Never emits a verdict. Never decides accept/reject.
  Never writes the denylist.
- **Importer worker** (`lib/import_dispatch.py::dispatch_import_from_db`):
  reads persisted evidence, decides via `full_pipeline_decision_from_evidence`.
  All rejects route through one helper
  (`_reject_import_from_evidence_decision`) with one denylist policy. The
  four folder/audio-integrity reject reasons are listed in
  `_PREIMPORT_FACT_REJECT_DECISIONS`; that frozenset gives them the
  "always self-heal back to wanted" invariant (force/manual paths normally
  pass `requeue_on_failure=False`, but the four facts override).

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

**Never run pyright on just the files you touched.** Run `nix-shell --run "pyright"` on the full repo, every time. Pre-existing errors are not someone else's problem — they accumulate as drift the moment you decide they're not yours to fix. Fixing each one is cheap; the expensive part is re-discovering them later and arguing about ownership. Triaging "is this mine or pre-existing?" via `git checkout` costs more tokens than just fixing it. **The repo is either 0-errors or it is not. Make it 0-errors.**

- All new dataclasses, functions, and module-level code must pass pyright with 0 errors
- Use typed dataclasses (not dicts) for structured data crossing module boundaries
- **No dual-interface types.** Never add `__getitem__`, `.get()`, or `isinstance(x, dict)` dispatch to a dataclass. If a function receives both dicts and dataclasses, that is a type error — fix the callers, not the receiver. Temporary bridges become permanent bugs.
- If a function parameter is untyped and accepts multiple representations (dict or dataclass), type it and fix all callers to pass the correct type
- Inner data structures must also be typed — no `list[dict]` when a dataclass exists
- Verify with: `pyright <files>` on every touched file before committing

## Wire-boundary types — use `msgspec.Struct`, not `@dataclass`
Any type that **crosses JSON** — harness stdout, an HTTP response, a JSONB blob written to or read from the DB, a subprocess's stdout — is a `msgspec.Struct`. **Same policy both directions:** encode via `msgspec.json.encode` (or `msgspec.to_builtins` when a dict is needed), decode via `msgspec.convert`. The declared Struct is the single contract that validates type drift at the boundary. Pyright does not see inside `dict.get()` — only runtime validation catches int-vs-str drift, mis-typed fields, or missing required data. This is the lesson of issue #99 / PR #98 (every Discogs validation silently logged `mbid_not_found` because a dataclass said `str` but the wire carried `int`) and the pre-#141 asymmetry (the old "dataclass if re-encoded, Struct if decoded only" split let docstrings lie about which side was strict).

- **Use `msgspec.Struct`** for: harness/subprocess JSON messages, external API responses, DB JSONB rows we read back and type-check, any type that is ever encoded back out to JSON. Reference implementations: `HarnessItem`, `HarnessTrackInfo`, `TrackMapping`, `CandidateSummary`, `ChooseMatchMessage`, `ImportResult`, `PostflightInfo`, `ConversionInfo`, `SpectralDetail`, `AudioQualityMeasurement`, `MovedSibling`, `ValidationResult` (all in `lib/quality.py`), `BeetsOpFailure` in `lib/beets_album_op.py`.
- **Keep `@dataclass`** for: types we construct entirely from our own typed Python code, inputs never crossing JSON (`QualityRankConfig`, `CratediggerConfig`, `DispatchAction`, `SpectralContext`, `ActiveDownloadState` where the custom `from_dict`/`to_json` helpers do the work). Their inputs are already typed — the strict boundary buys nothing.
- **Encode symmetrically.** `ImportResult.to_json()` is `msgspec.json.encode(self).decode()`, not `json.dumps(asdict(self))`. Route payloads that need a dict use `msgspec.to_builtins(struct)`, not `dataclasses.asdict(struct)` (which doesn't recurse into Structs anyway). Do NOT re-introduce `asdict` on a Struct — Pyright will let it through and it'll return the Struct instance unchanged, failing at `json.dumps`.
- **Decode at exactly one site.** The wire boundary is the one place the untyped blob becomes a typed object. After that, every downstream consumer works with the Struct directly — no defensive coercion, no `dict.get()`, no re-validation. If you find yourself writing a `_coerce_x` helper on the consumer side, the boundary is in the wrong place.
- **Strict ≠ coerce.** Declare fields as the type you want (`str`, not `str | int`). `msgspec.ValidationError` at the boundary is the detector. Do not use `strict=False` to silently coerce away real type drift.
- **Normalise early if the external source is untidy.** Harness-side `_id_str` in `harness/beets_harness.py` coerces int IDs to str *before* emitting — so the wire is always clean, and the downstream Struct validation never trips in the happy path. Keep normalisation at the source, not at the consumer.
- Decoders already in the repo: `lib/beets.py::beets_validate` via `msgspec.convert(msg, type=ChooseMatchMessage)`; `ImportResult.from_dict` / `ValidationResult.from_dict` via `msgspec.convert(d, type=cls)`.
- Tests owe: at least one RED test that feeds the wrong type at the boundary and asserts `msgspec.ValidationError`. This is the regression guard that makes the boundary worth having.

## Testing — Red/Green TDD
- Write tests FIRST (RED), then implement (GREEN)
- Every new function, dataclass, and decision branch needs test coverage
- Use `nix-shell --run "bash scripts/run_tests.sh"` for full suite
- Read `/tmp/cratedigger-test-output.txt` instead of re-running the 2-minute suite
- For single modules during dev: `nix-shell --run "python3 -m unittest tests.<module> -v"`

## API Contract Tests
- Every API endpoint consumed by the frontend must have a contract test in `test_web_server.py`
- Contract tests use a real `_WebServerCase` harness (HTTPServer on a random port + mocked DB) — see existing `TestPipelineRouteContracts`, `TestBrowseRouteContracts`, etc. as reference patterns
- Define a `REQUIRED_FIELDS` set per endpoint — the fields the frontend JS relies on
- Assert every returned dict includes all required fields via `_assert_required_fields(self, payload, REQUIRED_FIELDS, "label")`
- When adding a field the frontend needs, add it to `REQUIRED_FIELDS` first (RED), then fix the backend (GREEN)
- **Every new route MUST be added to `TestRouteContractAudit.CLASSIFIED_ROUTES`** — this is the guard test that introspects `Handler._FUNC_GET_ROUTES`/`_FUNC_POST_ROUTES`/`_FUNC_GET_PATTERNS` and fails if a registered route is unclassified or a stale entry is missing. The audit makes contract coverage self-enforcing — you cannot ship a route without classifying it.
- The `_WebServerCase` harness in `tests/test_web_server.py` exposes `self._get(path)` and `self._post(path, body)` helpers that hit the real server. Reuse these instead of building your own harness.
- **Mock data must mirror production row shape — synthetic int/str dicts are NOT acceptable.** When a contract test mocks a DB-row producer (any `PipelineDB`/`BeetsDB`/`psycopg2.extras.DictRow` source), at least one scenario must populate rows with production-shaped values: `datetime.datetime` for timestamps, `uuid.UUID` for UUIDs, the typed dataclass/`msgspec.Struct` for JSONB columns. Synthetic dicts of `str`/`int` values pass Pyright (`Dict[str, Any]` is permissive) and pass the contract test (mock matches assertion shape) but 500 on the first real call when the JSON encoder hits an unserializable type. This rule has bitten more than once — see `docs/solutions/testing/contract-test-mocks-must-mirror-production-shape.md` (search-plan-history datetime 500) and `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md` (search-by-id MB drift). The escape hatch when row-shape mocking is impractical: pair the contract test with an integration slice in `tests/test_integration_slices.py` that round-trips through real serialization. Every contract test that returns DB rows owes either a production-shaped mock OR a slice — never neither.

## Logging & Auditability
- Every download outcome (success, rejection, timeout, crash) MUST create a download_log row
- Use typed JSON dataclasses (`ImportResult`, `ValidationResult`) — never raw dicts
- Store the full JSON in JSONB columns for SQL queryability
- Never throw away data the harness or subprocess provides — log everything

## Decision Logic
- All quality/import decisions must be pure functions in `lib/quality.py`
- No decision logic inline in cratedigger.py — call the pure function, branch on result
- Every pure function must have direct unit tests (not just tested through integration)

## Pipeline Decision Debugging — Simulator-First TDD
- When debugging or changing import pipeline behavior (quality gate, backfill, spectral propagation, search tier selection), **always start with the CLI simulator** (`pipeline-cli quality <id>`).
- Add scenarios to the simulator FIRST that expose the bug or show the expected behavior. The simulator is the test suite for pipeline decisions — if you can't see the problem in the simulator output, you don't understand it yet.
- Only edit production code once the simulator scenarios clearly show what's wrong and what "right" looks like. The scenarios tell you what code to change.
- Run the simulator against real albums in the live DB (not mocked state) to verify. Pick albums that represent the edge case: e.g. CBR 320 with no spectral, verified lossless lo-fi, suspect FLAC transcodes.
- The simulator must show the full rejection cycle: import/reject decision → spectral propagation → backfill decision → next search tiers. Not just the import decision in isolation.

## Pipeline Bug Reproduction — Red/Green on Real Code Paths
- When a live pipeline bug involves **interactions between components** (spectral propagation → decision function → DB write → rejection), don't just test the pure decision function in isolation — write a unit test that calls the actual orchestration function (e.g. `lib.measurement.measure_preimport_state` + `dispatch_import_from_db`) with mocked state matching the live scenario.
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
- Never construct `CratediggerConfig` with positional/keyword args for a subset of fields. Always use `CratediggerConfig.from_ini()` with the runtime config file. Partial configs silently diverge when new config fields are added.
- Before adding a new function that "does roughly what X does but simpler," check if X can be called with an adapter. The adapter may be ugly — that's a signal to improve X's interface, not to duplicate X.

## CLI ⇄ API Surface Symmetry
- Every operator action must exist on **both** `pipeline-cli` and the web API. Adding only one is a contract drift waiting to happen — operators expect parity and will trip when it isn't there.
- Both surfaces wrap the same service-layer method (e.g. `SearchPlanService.advance_for_request`). The CLI subcommand and the HTTP handler are thin adapters with matched outcome → exit-code / outcome → status-code mappings. Never duplicate logic across the two; route everything through the service.
- Reference layout (worked example: `search-plan advance` in PR for the cursor-advance feature):
  - `lib/<thing>_service.py` — service method + typed `Result` dataclass (one outcome string per branch)
  - `lib/pipeline_db.py` — atomic mutation with `FOR UPDATE` row lock
  - `scripts/pipeline_cli.py` — CLI subcommand wrapping the service
  - `web/routes/<thing>.py` — HTTP endpoint wrapping the service
  - `tests/test_<thing>_service.py` — authoritative coverage of every outcome branch
  - `tests/test_pipeline_cli.py` — CLI wrapper test (exit-code mapping)
  - `tests/test_web_server.py` — API contract test (status-code mapping) + entry in `TestRouteContractAudit.CLASSIFIED_ROUTES`
- Status/exit-code convention to match: `200/0` success, `400/3` input validation (API only — CLI argparse covers this), `404/2` not found, `409/4` wrong state, `422/3` semantic violation, `503/5` transient/retryable.
- See `CLAUDE.md` § "CLI ⇄ API surface symmetry" for the full pattern table.

## New Work Checklist (read this first)

Before writing any new code, decide which test types you owe and what infrastructure you'll reuse:

| You're adding... | You owe... | Use this infrastructure |
|------------------|-----------|-------------------------|
| A new pure decision function in `lib/quality.py` | A subTest table covering every branch | `tests/test_quality_decisions.py` patterns |
| A new dispatch / orchestration path | An orchestration test asserting domain state + an integration slice | `FakePipelineDB`, `patch_dispatch_externals()`, `tests/test_integration_slices.py` |
| A new web API endpoint | A contract test with `REQUIRED_FIELDS` AND an entry in `TestRouteContractAudit.CLASSIFIED_ROUTES` AND a paired `pipeline-cli` subcommand (CLI ⇄ API symmetry) | `_WebServerCase`, `_assert_required_fields`, `tests/test_web_server.py`, `scripts/pipeline_cli.py` |
| A new operator action (CLI subcommand or API endpoint) | A service-layer method with a typed `Result`, BOTH a CLI subcommand AND an API endpoint, exit-code and status-code tests for each | `tests/test_<service>.py` for the authoritative coverage; CLI/API tests check the wrapper only |
| A new slskd interaction | An orchestration test using `FakeSlskdAPI` | `FakeSlskdAPI` from `tests/fakes.py` |
| A new typed dataclass | A pure test of construction + serialization, and a builder in `tests/helpers.py` if it crosses test boundaries | `tests/helpers.py` |
| A new `PipelineDB` method | An equivalent stub on `FakePipelineDB`, with a self-test in `tests/test_fakes.py` | `tests/fakes.py`, `tests/test_fakes.py` |
| A new `BeetsDB` method | Either (a) an equivalent stub on `FakeBeetsDB` with a self-test in `tests/test_fakes.py::TestFakeBeetsDB`, OR (b) drive the test against a real test SQLite DB if it's a read-only query | `tests/fakes.py`, `tests/test_fakes.py` |

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
- `make_spectral_context(...)` — `SpectralContext`
- `make_ctx_with_fake_db(fake_db)` — `CratediggerContext` wired to a fake
- `patch_dispatch_externals()` — context manager for the 6 dispatch external patches
- `noop_quality_gate(**kwargs) -> None` — drop-in `quality_gate_fn` stub for dispatch tests that don't care about the post-import gate. Pair with `dispatch_import_core(..., quality_gate_fn=noop_quality_gate)`.
- `RecordingQualityGate()` — recorder `quality_gate_fn` with `assert_called_once()` / `assert_not_called()` / `call_count` / `calls` (list of kwargs). For tests that assert the gate ran with specific args.

**`tests/fakes.py`** — stateful fakes:
- `FakePipelineDB` — full PipelineDB stand-in: requests, download_logs, denylist, cooldowns, status history, spectral state, attempt counters. Includes `assert_log()` helper. Has `queue_execute_results(*cursors)` + `execute_calls` recording for tests driving raw-SQL CLI paths.
- `FakeBeetsDB` — minimal BeetsDB stand-in: `album_exists`, `get_album_info(mb_release_id, cfg)`, `get_all_album_ids_for_release`, `get_item_paths`, `get_album_path_by_id`, `close` + context-manager + per-method call recorders + seed helpers (`set_album_exists`, `set_album_info`, `set_album_ids_for_release`, `set_item_paths`, `set_album_path_by_id`). Each method also has a `_default` field for "any key returns the same value" tests. Extend the surface only when a test exercises a new BeetsDB method.
- `FakeSlskdAPI` — stateful slskd client: `transfers` (enqueue, get_all_downloads, get_download, cancel_download, queued snapshots), `users` (directory with per-directory results and errors), call recording.
- `FakePipelineDBSource` — typed PipelineDBSource fake wrapping a `FakePipelineDB`. Use via `make_ctx_with_fake_db(fake_db)` rather than constructing directly.

**`tests/test_web_server.py`** — `_WebServerCase` harness with `_get`/`_post` helpers + `TestRouteContractAudit` guard.

### General test rules

# MOCKS: LEAF-SEAM ONLY

`MagicMock` and `patch(...)` are for the **outermost edge** of the test — the place where our code calls something external we don't own. They are forbidden as a substitute for our own stateful types or our own functions. Enforced by `tests/test_mock_audit.py` (issue #290).

**ALLOWED — leaf seams (mock freely):**
- Subprocess: `subprocess.run`, `subprocess.Popen`, `*.sp.run`, `*.sp.Popen`
- HTTP / URL: `urllib.request.urlopen`, `requests.get`, `requests.Session`
- External libraries we don't own: `music_tag`, `redis.Redis`, `slskd_api.SlskdClient` (the real one, at module import — see `_real_slskd_api` in conftest), MusicBrainz / Discogs API client objects
- Filesystem leaf seams: `os.path.isfile`, `os.path.isdir`, `os.path.exists`, `shutil.*`
- Time: `time.sleep`, `time.monotonic`
- Stdlib primitives: `threading.Event/Lock`, `signal.*`, `select.select`
- Notifier seams: `lib.util._meelo_*`, `lib.util.trigger_(meelo|plex|jellyfin)_scan` (one-way fire-and-forget)
- argparse `args` stubs: `args = MagicMock()` for CLI subcommand tests where args is a parsed-options struct
- subprocess return-value envelopes: `proc = MagicMock()` where you only set `returncode` / `stdout` / `stderr`
- `sys.modules["external_pkg"] = MagicMock()` at module top-level for import-time stubbing of optional deps
- Module-level `logger` objects (`lib.<module>.logger`, `harness.<module>.logger`) when tests assert on log records

**ALLOWED — thin seam-wrapper functions in `lib/` (the function IS the boundary):**
These are functions whose body is mostly "construct args and dispatch to a network / subprocess / filesystem call." Mocking them is the most ergonomic point to mock the underlying seam — patching `slskd_api` directly would require elaborate per-test setup for no extra coverage. **The exhaustive list lives in `tests/_mock_audit_scanner.py`** under "Thin seam-wrapper functions in lib/" with one rationale per entry. Today that includes (non-exhaustive):
- slskd network wrappers: `lib.enqueue._fanout_browse_users`, `lib.enqueue.slskd_do_enqueue`, `lib.enqueue.slskd_enqueue_with_outcome`, `lib.(download|enqueue).cancel_and_delete`
- Beets harness subprocess wrapper: `lib.beets.beets_validate`
- Sox / ffmpeg / mp3val wrappers: `lib.measurement.spectral_analyze`, `lib.measurement.inspect_local_files`, `lib.measurement.repair_mp3_headers`, `lib.measurement.measure_preimport_state` (and `lib.import_preview.*` / `lib.download.*` re-exports)
- Config loader: `lib.config.read_runtime_config`, `lib.config.CratediggerConfig.from_ini`
- `BeetsDB` class itself (the constructor — `BeetsDB.<method>` patches remain flagged because `FakeBeetsDB` is the right replacement)
- MB API fetch: `scripts.pipeline_cli.fetch_mb_release`, `lib.*.fetch_mb_release`
- DB reconnect: `web.server._try_reconnect_db`

When adding a new seam-wrapper function, the bar is: **its body must be ≤10 lines AND mostly forward to an external boundary.** Anything fatter is pure logic with a side effect, not a seam wrapper — drive it with a fake.

**ALLOWED — module-local DI seams (route/CLI dispatch only):**

When production code is dispatched by URL (web routes) or argparse subcommands (CLI), there's no kwarg path to inject a dependency through. The established pattern: bind the dependency at module-attribute scope and let tests patch the attribute.

```python
# lib/<module>.py at module top
from lib import transitions
finalize_request = transitions.finalize_request   # the DI seam

# tests patch the module attribute
@patch("lib.<module>.finalize_request")
def test_x(self, mock_finalize):
    ...
```

Examples currently allowlisted: `web.routes.pipeline.finalize_request`, `harness.import_one.finalize_request`, `scripts.pipeline_cli.finalize_request`, `scripts.repair.finalize_request`, `lib.import_dispatch.finalize_request`.

**This is only legitimate when the entry point cannot accept a kwarg.** A mid-tier private helper called from another production function CAN accept a kwarg — see `try_enqueue(..., match_fn=check_for_match)` and `dispatch_import_core(..., quality_gate_fn=_check_quality_gate_core)` as the canonical kwarg-DI examples. Module-local seams are the second-best option; kwarg DI is the first-best.

When you find yourself reaching for an in-module patch on a mid-tier function (e.g. `patch("lib.download._handle_valid_result")` from within a `process_completed_album` test), ask: could this function take the dependency as a kwarg? If yes, refactor. If no (URL or argparse dispatcher), allowlist with rationale.

**FORBIDDEN — stateful collaborators and pure-logic functions:**
- `MagicMock()` assigned to a variable named `db`, `mock_db`, `failing_db`, `pdb`, `ctx`, `context`, `beets`, `beets_db`, `source`, `pipeline_db`, `slskd`, `fake_db` — **use `FakePipelineDB`, `FakeBeetsDB`, `FakeSlskdAPI` from `tests/fakes.py`**.
- `patch("lib.enqueue.check_for_match")` — pure matching logic. Use the `try_enqueue(..., match_fn=)` kwarg DI seam, or drive the real function with seeded folder cache / track data.
- `patch("lib.transitions.finalize_request")` / `patch("lib.transitions.apply_transition")` on the ORIGIN module. The route / CLI / harness modules each expose a `finalize_request = transitions.finalize_request` module-local seam — patch THAT instead (e.g. `patch("web.routes.pipeline.finalize_request")`).
- `patch("lib.import_dispatch.parse_import_result")` is forbidden on lib.import_dispatch.parse_import_result — `lib.quality.parse_import_result` is a thin harness-stdout parser and is allowlisted; the `lib.import_dispatch` re-export is also allowlisted. Use one of those.
- `patch("lib.beets_db.BeetsDB.<method>")` — use `FakeBeetsDB`; the class itself is allowlisted but per-method stubbing is not.
- Any `patch("lib.<our-module>.<our-function>")` whose target is **not** explicitly on the seam-wrapper allowlist in `_mock_audit_scanner.py`. **If you're mocking your own logic, you're testing the mock, not the code.**

**Pure-decision allowlist debt (known cleanup):** Three pure-decision functions are currently allowlisted as orchestration test seams: `lib.import_dispatch.quality_gate_decision`, `lib.quality.full_pipeline_decision`, and `lib.import_preview.preview_import_from_values` (+ its `web.routes.pipeline` re-export). The rationale (orchestration tests stub the branch; the decision's own tests live elsewhere) is defensible scoping but the proper migration is to drive the tests with real measurement / value inputs that produce each branch. Tracked in the active follow-up issue — do not allowlist new pure-decision functions; migrate them.

**The rule of thumb:** does the thing you're about to mock cross a process / network / third-party boundary as its primary purpose? If yes, mock — and if the function is a thin wrapper around that boundary, add it to `_mock_audit_scanner.py`'s seam-wrapper list with a rationale. If the function is pure logic with a side effect, use a `Fake*` or drive the real function with constructed inputs.

**Adding a new `PipelineDB` method:**
1. Add the method to `tests/fakes.py::FakePipelineDB` (state-respecting implementation) — not `MagicMock`.
2. Add a self-test in `tests/test_fakes.py`.
3. Tests that use it consume the fake; they do NOT do `mock_db.new_method.return_value = ...`.

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
