# Generated (property-based) testing

Issue #548. Hypothesis-driven generated tests assert **policy invariants**
over large generated state spaces instead of hand-picked examples. They are
ordinary members of the unittest suite — no separate runner, no seed
bookkeeping, no standing service.

## Bug hunting — the house method

This is the primary bug-hunting workflow (proven on #550, where a live
production bug unreproducible by static analysis + disk forensics fell in
one session): **invariant → probe → generated harness → shrink → fix**.

1. Write down the invariant the symptom violates.
2. Probe the cheapest suspicious seam with REAL production functions (a
   throwaway nix-shell heredoc — minutes, never committed).
3. Build/extend a `tests/test_*_generated.py` harness: strategies over the
   world space, the invariant as a checker, real entry points, stubs only
   at allowlisted leaf seams. Hypothesis finds and shrinks the repro.
4. RED → fix → GREEN in one PR: shrunk world pinned, invariant permanent,
   must-still-work guard, known-bad self-test.
5. When in doubt, plant a mutant reverting the fix — the property must
   kill it.

Case study (#550 defect #1): invariant "an accepted multi-disc grab covers
every disc with unique transfer identities" → probe showed the real matcher
cross-matches sibling disc folders → harness drove real `try_multi_enqueue`
and reproduced `matched=True` with 16 entries / 11 unique files → two-layer
fix (source exclusion + fail-closed coverage gate) merged with the property
in PR #557. Full workflow rule: `.claude/rules/code-quality.md` § "Bug
Hunting — Generated-First".

## Modules

| Module | Target | Properties |
|--------|--------|------------|
| `tests/test_quality_generated.py` | the decision twins (`full_pipeline_decision` / `full_pipeline_decision_from_evidence`) | decisions are definitive (totality); raw verified-lossless FLAC never replaced by lossy; transparent lossy never accepts obvious downgrades; **twin parity** over the shared world language; evidence-only integrity facts (corrupt / bad-hash / nested / mixed) always reject in priority order; classification layer (`classify_full_pipeline_decision` / `evidence_decision_name` — cleanup eligibility) coherent with every generated decision; incomplete evidence fails closed |
| `tests/test_search_override_generated.py` | `rejection_backfill_override` over codec measurements and attempt-local HAVE audits | lossless-only search narrowing occurs exactly for canonically TRANSPARENT measurements with a completed genuine HAVE audit; excellent, unknown/lossless codecs, missing/failed audits, and non-genuine grades fail open |
| `tests/test_quality_lineage_generated.py` | target-quality contracts and measurement/evidence projection | `from_explicit_label` rejects bare MP3 across case/whitespace variants; `from_projection` requires a CBR/VBR boolean and preserves it for bare MP3; explicit V/numeric labels own their mode and gate policy even when the measured boolean contradicts them; exact single/equal/differing-track projections preserve mode; measurement-only early rejects never claim target policy |
| `tests/test_evidence_generated.py` | `ensure_current_evidence_for_action` | converted current evidence requires source V0 (fix `6cf26a4`): never `loaded` without a V0 metric, scalar state backfills, otherwise fail closed |
| `tests/test_slskd_events_generated.py` | `ingest_download_file_events` (event stamping — the ONLY source of completed-file locations) | stamping oracle (newest decodable event per key in the new-events window, nothing else); totality + exactly-once over wild feeds (dup ids, garbage timestamps, undecodable payloads, pruned/absent cursors, rows leaving `downloading` mid-ingest); duplicate-id invariance (mid-pagination shape) |
| `tests/test_completed_purge_generated.py` | `purge_completed_transfers` | a write-ahead intent becomes ownership only after POST acceptance; pending and foreign keys are never mutated; 1-N successor IDs for each confirmed `(username, filename)` key remain owned across every terminal state; terminal accounting conserves every row through successful removals, false returns, and exceptions (`removed + removal_failed + foreign`); failed removals remain resident, a successful second pass is idempotent, and every removal uses `remove=true` |
| `tests/test_transfer_ledger_generated.py` | enqueue write-ahead ownership + `prune_transfer_ledger` | every owned enqueue is ledgered before POST and gains destructive authority iff accepted; pending intents older than the strict cutoff are pruned regardless of request status, while old accepted evidence survives only for `wanted`/`downloading`; exact-cutoff rows survive |
| `tests/test_request_lifecycle_generated.py` | `transitions.finalize_request` + `supersede_request_mbid` driven as a `RuleBasedStateMachine` over random operation sequences | statuses stay in the legal set; `replaced` rows are terminal and byte-frozen; identity (mbid/source/created_at) immutable; every replaced row has exactly one linked descendant; `active_download_state` only on `downloading` rows; the DB guards (claim, downloading→wanted) no-op on ineligible rows |
| `tests/test_multidisc_manifest_generated.py` | `try_multi_enqueue` with the REAL `check_for_match` matcher over generated multi-disc worlds | **the #550 coverage law**: an accepted multi-disc grab has no duplicate transfer identities, one distinct source folder per disc, unique coverage == track count. This harness **found the live #550 defect-#1 bug** (partial-disc manifests) before the production MANIFEST-TRACE window captured it |
| `tests/test_import_manifest_generated.py` | `build_active_download_state` ⇄ `lib/download_reconstruction.py::reconstruct_grab_list_entry` + `check_audio_manifest` | manifest round-trip through persisted state never shrinks or grows (exact key/field fidelity); on-disk check oracle — pass iff disk audio set == tracked manifest set, extra/missing reported exactly |
| `tests/test_dispatch_outcomes_generated.py` | `dispatch_import_core` + every rejection writer (`_reject_import_from_evidence_decision`, `_record_rejection_and_maybe_requeue`, `DatabaseSource.reject_and_requeue`, `_reject_request_auto_import`, and the atomic abandoned-import writer) | the auditability law (every outcome writes a download_log row); full routing oracle vs `dispatch_action` across the decision universe + no-JSON crash path; U11 self-heal override for ALL 5 preimport facts (incl. `mixed_source`, missing from the hand-written table); every rejection writer preserves the `ValidationResult` distance/scenario projection exactly, including NULL and measured 0.0 |
| `tests/test_materialize_generated.py` | `lib/processing_paths.py::attempt_fingerprint` + `canonical_folder_for_row` / `canonical_processing_path`, and `lib/download_materialization.py::_materialize_processing_dir` | #550 phase 2 follow-up (PR #560 shipped deterministic-only): `attempt_fingerprint` is permutation-invariant, deterministic, distinguishes different `(username, filename)` sets, and the empty set is a stable defined digest; the fingerprint suffix is present iff the fingerprint is non-empty and the resulting basename stays ≤255 bytes even under adversarial generated unicode artist/title (r2 truncation guard); the materialize isolation law — two attempts for the same artist/title/year, driven through the real `_materialize_processing_dir` against a real tempdir, never blend files (attempt B's folder holds exactly B's manifest, A's folder is untouched) and identical manifests resolve to the same folder (resume stability) |
| `tests/test_quarantine_triage_generated.py` | `lib.quarantine_triage_service.list_unreferenced_quarantine_folders` | quarantine lifecycle law across request statuses: the deterministic result is exactly the immediate real album folders without a default-visible relative/absolute/descendant Wrong Matches reference; replaced frozen-audit rows and external references do not claim local folders, and the code-owned `bad_files` / `untracked_audio` roots are never surfaced or recursively traversed |
| `tests/test_wrong_match_policy_generated.py` | `lib.wrong_match_policy`, `lib.wrong_matches.wrong_match_row_is_visible`, and `FakePipelineDB.get_wrong_matches` | Wrong Matches remains a candidate/pressing review queue: all five folder/audio-integrity fact rejects plus `spectral_reject` stay excluded across generated scenario/status/history-view worlds, while arbitrary other or NULL scenarios remain visible unless the request is replaced in the default view; independently pinned oracles and known-bad checkers prevent the taxonomy, row predicate, and fake from drifting together |
| `tests/test_convergence_runner_generated.py` | `lib/convergence.py::run_convergence_steps` | every registered convergence step is attempted exactly once in declared order even when any arbitrary subset raises; Phase 0 order and the end-of-cycle harvest-before-purge constraint are pinned as registry data rather than source inspection; import failures are isolated like call failures |
| `tests/test_current_library_quality_generated.py` | `BeetsDB.check_mbids_detail` + `lib.banding.band_from_detail` | current beets projections preserve the positive-track minimum as floor data, expose the positive-track mean explicitly, and select that mean for codec-aware rank; the known-bad min-selected mutant is rejected |
| `tests/test_unused_import_audit_generated.py` | pinned Ruff `F401`/`F811` source-local analysis | an import is live only through its own binding in the importing module; same-named peer uses, parameter/comprehension shadows, and rebindings cannot mask it; exact intentional redundant-alias baselines reject expansions, duplicate identities, and stale entries even though Ruff accepts explicit re-exports; planted aggregate-name and baseline-delta faults qualify the checkers |
| `tests/test_suite_artifact_generated.py` | `scripts/test_artifact.py` exact-target provenance checker | a suite artifact is citeable iff its gate and synchronous output capture completed green, it started and ended clean at the expected HEAD, it ran every test in the exact discovered suite, and it records completed-output byte/SHA-256 integrity; deterministic pins exercise real concurrent allocations, delayed/failing capture processes, output tampering, and planted wrong-HEAD/dirty evidence |
| `tests/test_js_ast_generated.py` | flake-pinned tree-sitter JavaScript structural audits | supported direct payload literals produce exactly the independent field oracle across raw/escaped identifiers, shorthand, quoted/computed, nested, array, comment, string, template, Unicode, and ordering worlds; production payload fixtures use exact local aliases registered from the real renderer module, while raw renderer references, default/namespace/alternate imports, non-top-level or shadowed `__test__` registrations, computed `__test__` fixture calls, spreads, elisions, fixture indirection, and methods fail closed without attempting JavaScript dataflow inference; independent boundary worlds vary lexical scopes, repeated names, `let`/`const`, before/after member mutation, duplicate keys, registration/import shapes, unknown selectors, full browser-global-rooted semantic Object chains, and target expressions; unrelated modules and inert strings remain valid; emitted window handlers preserve ECMAScript raw/cooked escape semantics (including Unicode line continuations and lone surrogates), while bindings normalize escaped keys, treat full member chains rooted at `window`/`globalThis`/`self` as browser globals, reject every computed call rooted at semantic `Object` in a window-binding owner, and accept only exact direct `Object.assign(window, {...})` shapes across multiple blocks; planted quoted-key, template-interpolation, state-boundary, fail-open binding, and missing-binding mutants qualify the checkers |
| `tests/test_issue_reference_contract_generated.py` | `scripts/audit_issue_references.py` | implementation PR bodies and branch commit messages never use any GitHub auto-closing keyword with same-repo, cross-repo, or full-URL issue references across case, colon, whitespace, and issue-number worlds; canonical `Refs #N` and plain issue URLs remain valid, with the real premature-close shapes for issues #598 and #609 pinned as known-bad examples |
| `tests/test_deploy_pin_generated.py` | `scripts/pin_nixosconfig.sh` through deterministic process-level git/nix/token seams | a retry never creates a second signed pin across failures or signals after commit because the commit transaction advances a private pending ref; transient command failures and inconclusive signature status retain that ref while a definitively bad or unsigned candidate is discarded; every push follows promotion to the durable receipt ref and carries the token header only in a trace-sanitized environment; every started detached worktree gets a cleanup attempt across update, signature, post-commit recovery, push, and cleanup faults, with planted invalid-pending, two-signed-commits/one-receipt, push-before-ref, and missing-cleanup violations qualifying the checker |
| `tests/test_destructive_authority_generated.py` | `ban_source` + `delete_release_from_library` authority worlds plus pinned-Beets delete-manifest worlds varying track/art/sidecar presence, unknown bytes, no-op removers, partial I/O/enumeration faults, strict presence-probe faults at every progress phase, and lost subprocess/protocol acknowledgements | every authority rejection is zero mutation; confirmed success leaves every owned target and Beets/PG authority absent while preserving unknown bytes; cleanup, enumeration, and presence-probe failures retain Beets/PG authority and never notify; PG partial is explicitly album-gone/pipeline-present; every ambiguous child acknowledgement remains incomplete regardless of metadata state, preserves PG, skips notification, and retains operator recovery context. Planted omitted-art, omitted-sidecar, no-op-success, unknown-overdelete, early-Beets-delete, early-PG-delete, acknowledgement promotion/context-loss, enumeration/presence-success, authority-loss, and notification mutants qualify the checkers |
| `tests/test_library_delete_notifiers_generated.py` | `notify_library_delete` targeting and observation across generated Plex filesystem ancestry and Jellyfin lookup/refresh worlds | Plex submissions always target the nearest existing ancestor inside the configured Beets root, never the deleted or an out-of-root path; Jellyfin targets an exact former-path item when observable and otherwise falls back to the configured library, reports `submitted` only after exact-item absence is observed, and contains lookup/refresh failures as warnings without escaping the completed delete boundary. Planted deleted/out-of-root targets, stale 2xx success, wrong-target, hidden-failure, and escaping-exception mutants qualify the checkers |
| `tests/test_jellyfin_refresh_generated.py` | the real `trigger_jellyfin_scan` entry point with only `urllib` replaced at the network leaf | every generated imported album maps to one exact Jellyfin-visible path in a `POST /Library/Media/Updated` body, with POST/token/JSON/timeout intact and no collection refresh or broad fallback; transport/HTTP/runtime failures stay inside the best-effort notifier boundary |

Every reusable invariant checker also carries **known-bad self-tests** proving
it trips on a planted violating decision — the RED/GREEN guarantee that the
harness detects what it claims to. Modules such as
`tests/test_jellyfin_refresh_generated.py` that assert the property directly
do not add a second checker layer to self-test.

The finite `describeBeetsDeletion` partial-result rendering branch stays in
deterministic Node coverage (`tests/test_js_library.mjs`): it projects already
typed counts and warning strings and has no independent lifecycle state space
for Hypothesis to explore. The underlying delete ambiguity, enumeration,
presence-probe worlds, and notifier
target/observation worlds are generated in the two Python modules above; a
second JavaScript property stack would duplicate those policies rather than
exercise a new invariant.

## Three tiers, one knob

`tests/_hypothesis_profiles.py` registers three profiles, selected by
`CRATEDIGGER_HYPOTHESIS_PROFILE`:

- **`suite`** (default) — deterministic (`derandomize=True`, no example
  database), bounded examples. Runs on every `scripts/run_tests.sh`,
  identical on every machine. This is the merge gate.
- **`push`** — quick randomized burst (2k examples) that `scripts/pre-push`
  runs on every `git push`, before the flake check. The generated modules
  run as parallel single-module processes (wall-clock = slowest module,
  not the sum; the shared `.hypothesis/` database is multi-process safe).
  Fresh entropy per push means exploration accumulates over time with
  zero operator effort; a push-found failure is remembered in
  `.hypothesis/` and replays first in dev. Escape hatch, as for the whole
  hook: `git push --no-verify`.
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
