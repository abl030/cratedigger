# Generated (property-based) testing

Issue #548. Hypothesis-driven generated tests assert **policy invariants**
over large generated state spaces instead of hand-picked examples. The normal
pure/fake-backed modules are ordinary members of the unittest suite — no seed
bookkeeping or standing service. The deterministic cross-engine world model is
also part of the normal suite at its measured small budget. The deeper
randomized, mirror-backed, and long-fuzz runs execute together in the daily
unstable compatibility gate tracked by issue #498. The read-only live audit
joins that same run after issue #762 establishes current-path authority. The
public report groups findings by Cratedigger integrity, current-projection
health, and Beets/library health; issue #910's separate member-level debt gate
accepts only a complete exact stable or shrinking approved cohort.

## Daily unstable compatibility gate

`scripts/daily_flake_update.sh` is the Nixpkgs-reference unattended entry
point. It checks out current `main`, advances only the `nixpkgs` node in
`flake.lock`, and runs the deterministic suite (which owns both Pyright
contracts), the
`beetsStableCandidate` aggregate (every non-tip flake check plus the complete
reviewed Beets-release matrix), the default lifecycle hammer, the
20,000-example fuzz burst, and the mirror-harness smoke. The moving tip build,
contract, and full-repository Pyright canary are deliberately excluded from
this stable candidate and run only in the independent tip job. Independent
test stages all run so one notification contains the whole day's result. A
completely green candidate commits and pushes only `flake.lock`; a red
candidate pushes nothing. Scheduling, state paths, and notification belong to
the downstream nixosconfig service.

`scripts/daily_beets_tip_update.sh` is a separate serialized checks-only
canary. It advances only `beets-tip`, then requires the tip build, disposable
boundary contract, and tip-backed Pyright before making a lock-only candidate
commit. It never supplies the deployment-owned Beets runtime. The historical
matrix consumes the reviewed manifest; use
[`scripts/refresh_beets_compat_releases.py`](../scripts/refresh_beets_compat_releases.py)
with `--as-of YYYY-MM-DD --check` for a fixed-clock operator refresh check.

Each matrix member admits the deployment-shaped active Beets plugin profile,
loads that profile for the exact-delete boundary, and runs an incremental
import with a writable external state file while asserting that neither the
immutable configuration nor the source/library tree receives state artifacts.
An executable closure check also keeps the tip and historical Beets packages
out of normal packages, shells, apps, and the exported-module VM closure.

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

## Performance and expansion rulebook

Generated-test speed is subordinate to semantic coverage. The optimization
loop is therefore **classify → prove → remove harness repetition → measure**,
never "lower the daily budget until green". The unattended profile remains
20,000 examples for entropy-driven properties.

### Exact finite domains

A property is finite only when its module can prove the complete semantic
domain independently of Hypothesis. Do not infer finiteness from repeated
examples, a `SHALLOW` report, or a strategy that happens to contain bounded
primitives: projections, filters, dependent dimensions, and future expansion
can make an apparent product count wrong.

For a proved finite domain:

1. Derive cardinality from the actual independent dimensions in code.
2. Give every world one canonical representation. A bit mask over `n` optional
   members is the canonical powerset representation (`0 .. 2**n - 1`).
3. Write an independent verifier that enumerates the represented worlds and
   compares them with the intended domain. Keep a known-bad self-test that
   removes or collapses worlds and proves the verifier fails.
4. Keep named minimum, maximum, empty, full, and historical edge `@example`
   pins even when they duplicate generated endpoints.
5. Decorate the property with
   `@finite_generated_domain(cardinality=N, verify=verify_domain)`. The
   decorator runs the proof during every isolated import, inherits all active
   profile settings except `max_examples`, fixes the budget at exactly `N`, and
   publishes typed discovery metadata.
6. The fuzz scheduler validates `budget == cardinality` and schedules one
   target regardless of the entropy-shard count. A metadata mismatch aborts
   before admission; it never silently falls back to repeated shards.
7. Pin discovery in `tests/test_fuzz_runner_generated.py`: cardinality, explicit
   budget, one target, no profile override. Run the target under the production
   profile and inspect Hypothesis statistics for exactly the intended valid
   worlds.

This is explicit automation rather than strategy inference. Future expansion
changes the dimensions, so its verifier/cardinality test goes red and forces a
deliberate new proof before the scheduler changes work.

### Per-example external process startup

If the slow target launches the same interpreter, browser, compiler, or helper
for each Hypothesis example, first prove startup/module loading is the repeated
cost. Preserve fresh **Python target** isolation, but reuse the helper inside
that target when the operation itself can be request-local.

JavaScript-backed properties use `tests.node_jsonl_worker.NodeJsonlWorker`:

- one Node child is created in each test method's `setUp()` and registered with
  `addCleanup()`, so ordinary unittest execution never shares mutable worker
  state between methods;
- each request carries a monotone ID, operation, and complete payload;
- each response is exactly one typed JSON line with the matching ID;
- malformed/extra output, wrong IDs, EOF, timeout, JavaScript exceptions, and
  child loss poison the worker and fail closed with retained stderr;
- a poisoned worker repeats its original failure so Hypothesis cannot turn a
  harness death into inconsistent shrink noise;
- no child survives the Python fuzz target, and no mutable request state is
  shared by handlers.

`tests/test_generated_node_worker_audit.py` enforces the bounded historical
anti-pattern: a generated module cannot directly launch a literal `node` or
`nodejs` subprocess. Other real subprocess properties (FFmpeg, git, pinned
Beets) remain deliberate semantic boundaries and are not swept into a broad
source scanner.

### Measurement and stopping rule

1. Preserve the accepted full-depth log as baseline.
2. Benchmark the exact changed property/module with the production profile and
   canonical shard count. The automatic count is both host- and budget-bounded:
   each entropy child gets at least 250 examples before another process is worth
   starting. This proves the optimization before paying for the whole queue.
3. After a material critical-path change, run the complete relevant profile
   once and compare wall time, targets, tests, property depth, discards,
   infrastructure failures, ENOSPC, and slow-target rankings. Changes that can
   affect the unattended path also owe the 20,000-example burst.
4. Continue only while the next slow cohort is dominated by removable harness
   repetition or a separately proved finite domain. Domain computation,
   PostgreSQL serialization required by ownership, and real media decoding are
   semantic costs, not automatic optimization targets.
5. Stop when measured whole-run return is marginal relative to new protocol or
   scheduler complexity. Never raise the PostgreSQL cap or global worker count
   without private tmpfs/process-tree evidence.

## Modules

| Module | Target | Properties |
|--------|--------|------------|
| `tests/test_audio_validation_generated.py` | `lib.util.validate_audio` through the real pinned FFmpeg full-decode subprocess plus mocked process-outcome leaves | every generated FLAC audio-frame mutation fails the fixed audio-only strict policy even when the former command returns zero; a real unset STREAMINFO MD5 remains byte-identical and leaves no diagnostic; every positive FFmpeg exit on stable readable bytes is typed bad audio, while arbitrary exit-zero stderr has no policy or audit meaning; diagnostics remain bounded. The real-subprocess property executes 18 mutations in the suite and 96 fresh mutations in the randomized fuzz tier; the former `rc=0` contract is pinned as a known-bad checker input |
| `tests/test_test_tmpfs_generated.py` | the real `scripts/test_tmpfs.sh` setup subprocess | a low-headroom setup failure stays nonzero, prints no selected directory, and retains the exact headroom diagnostic for generated safe inherited `TMPDIR` values; zero-status, inherited-stdout, and diagnostic-loss inputs are independently qualified as known-bad checker inputs |
| `tests/test_suite_coordinator_generated.py` | `scripts.run_test_suite.run_suite` over generated all-phase outcome worlds through its real state/persistence loop and a process-free phase executor leaf | every scheduled phase executes exactly once and contributes its pass/fail state to the single terminal result; deterministic process pins cover simultaneous cross-tool failures, private typed bundles, exact failure identities and reruns, command-start infrastructure failure, and signal interruption; old fail-fast and false-green shapes qualify the checker |
| `tests/test_final_gate_receipt_generated.py` | the real `scripts/run_final_gate.sh` with a PATH-injected canonical-argv fake `nix-shell` over tiny clean Git fixtures | every generated ordinary command exit code has an atomically recorded matching pass/fail terminal state and is returned unchanged while the fake observes only `nix-shell --run "bash scripts/run_tests.sh"`; deterministic pins cover rejection of alternate gate labels, signal-shaped incomplete outcomes, exact active PID/start-tick recovery, changed-tree incomplete publication, output preservation, and the validated suite-bundle path; known-bad command, signal, stale-identity, missing-terminal, and tree-mismatch inputs qualify the checker |
| `tests/test_web_request_security_generated.py` | the pure request authorizer plus the real HTTP pre-dispatch boundary for the cache-writing YouTube resolver | generated channel, method, canonical-origin, Origin, and Referer worlds match the fail-closed authorization oracle; browser POSTs with missing or mismatched provenance return 403 before resolver dispatch or DB access. Missing-provenance, mismatched-origin, future-unsafe-method, resolver-call, and DB-touch mutants independently qualify the checkers |
| `tests/test_youtube_album_service_generated.py` | `lib.youtube_album_service.resolve_youtube_album` driven through the shared production Requests Session/HTTPAdapter and a loopback HTTP server | generated worlds cross every configured retryable status (429/500/502/503/504), GET/POST, independent refresh state, absent/empty/nonempty durable cache posture, and search/seed/one-sibling/all-siblings operation site. `total=3` produces four real attempts per exhausted request and the public `requests.exceptions.RetryError` never escapes: outer or all-sibling exhaustion returns `unresolved_mirror_unavailable` without a durable write, nonempty refresh returns the exact `ok` / `from_cache` matrix, one-sibling exhaustion excludes only that sibling and persists the exact remaining membership, and the existing HTTP/CLI maps remain authoritative. Escaped-exception, generic/wrong-result, fallback-drift, independent upsert/durable-rewrite, sibling-overreach, partial-membership-drift, and bypassed-attempt-count inputs qualify the checker |
| `tests/test_world_audit_debt_generated.py` | `lib.world_audit_debt.assess_world_audit_debt` over generated live-audit cohorts | a candidate passes exactly when it is an exact subset of the persisted member fingerprints; stable members remain, resolved members monotonically disappear, and any new member or changed cause for a known identity fails without advancing state. A same-count member replacement qualifies the count-only known-bad mutant |
| `tests/test_quality_generated.py` | the decision twins (`full_pipeline_decision` / `full_pipeline_decision_from_evidence`) | decisions are definitive (totality); raw verified-lossless FLAC never replaced by lossy; transparent lossy never accepts obvious downgrades; **twin parity** over the shared world language; evidence-only integrity facts (corrupt / bad-hash / nested / mixed) always reject in priority order; classification layer (`classify_full_pipeline_decision` / `evidence_decision_name` — cleanup eligibility) coherent with every generated decision; incomplete evidence fails closed |
| `tests/test_search_override_generated.py` | `rejection_backfill_override` over codec measurements and attempt-local HAVE audits, plus retained-import override resolution | lossless-only search narrowing occurs exactly for canonically TRANSPARENT measurements with a completed genuine HAVE audit; excellent, unknown/lossless codecs, missing/failed audits, and non-genuine grades fail open; a successful retained import never widens an existing lossless-only scope |
| `tests/test_search_scheduler_generated.py` | `FakePipelineDB.get_wanted_searchable` scheduler-parity boundary over generated cohort sizes, ages, eligibility states, attempt counts, and page sizes | selection contains no duplicates or ineligible rows, never wastes eligible page capacity, reserves a floor-rounded quarter share for sub-24-hour requests (four at production page size 16), preserves the established floor while established work is eligible, borrows unused capacity both ways, and ignores attempt counts when classifying aged requests |
| `tests/test_quality_lineage_generated.py` | target-quality contracts and measurement/evidence projection | `from_explicit_label` rejects bare MP3 across case/whitespace variants; `from_projection` requires a CBR/VBR boolean and preserves it for bare MP3; explicit V/numeric labels own their mode and gate policy even when the measured boolean contradicts them; exact single/equal/differing-track projections preserve mode; measurement-only early rejects never claim target policy |
| `tests/test_evidence_generated.py` | `ensure_current_evidence_for_action` | converted current evidence requires a linked source V0 fact (fix `6cf26a4`): never `loaded` without it, request scalar mutations cannot change the result, and missing evidence fails closed; fingerprint rebuilds carry only source-subject facts with `carried` provenance |
| `tests/test_preview_failure_evidence_generated.py` | `process_claimed_preview_job` plus failure-point current-evidence preparation/enrichment | every producer stage that terminates as `measurement_failed` reaches one lifecycle owner: request-owned failures always persist their job/audit and diagnostic; an exact readable installed release links a complete pre-attempt HAVE snapshot regardless of stage or job type; missing identity, absent/unreadable HAVE, and preparation/enrichment faults stay fail-soft without fabricating evidence |
| `tests/test_spectral_attempt_audit_generated.py` | production HAVE authorization, both attempt-audit adapters, and `process_claimed_preview_job` | candidate and HAVE authorization remain independent across automation and force jobs: each complete matching snapshot with a decision-usable grade projects its own persisted spectral fact without another scan, while changed, incomplete, or unusable HAVE evidence takes the existing measurement-and-persistence path; changed candidate evidence still runs full preview exactly once |
| `tests/test_lossless_lineage_check_generated.py` | migration 057's real PostgreSQL CHECK plus same-address FakePipelineDB upsert worlds | v4 installed-subject spectral is rejected exactly when source V0, verified-lossless proof, or a case-normalized lossless conversion source establishes lossless lineage; native installed facts, source-subject facts, M4A-container-only rows, and legacy v1/v3 rows remain valid; failures name the exact constraint; a same-key merge clears stale installed spectral when new lineage arrives while preserving a source-subject fact |
| `tests/test_slskd_events_generated.py` | `ingest_download_file_events` (event stamping — the ONLY source of completed-file locations) | stamping oracle (newest decodable event per key in the new-events window, nothing else); totality + exactly-once over wild feeds (dup ids, garbage timestamps, undecodable payloads, pruned/absent cursors, rows leaving `downloading` mid-ingest); duplicate-id invariance (mid-pagination shape); issue #898 PR1 current-incarnation classification across reused/different keys, event times before/at/after the exact `enqueued_at` witness, candidate shadowing, lost witnessed writes, cursor hold/replay, and idempotent ledger effects |
| `tests/test_download_incarnation_generated.py` | `FakePipelineDB.update_download_state_if_downloading` plus `lib.download._admit_download_incarnations` | issue #898 PR1 integrated invariant: after B replaces A at the same request/path, delayed enqueue/event/harvest/poll whole-state payloads carrying A's witness leave B and its row metadata byte-for-byte unchanged, while current B payloads succeed; post-event polling admits only refreshed exact `(request_id, enqueued_at)` pairs captured before the transfer snapshot, preserves refreshed order and original witness text, and excludes same-ID B replacements, departed/new rows, and missing/empty/malformed/invalid state. Missing status/stored-witness/outgoing-witness predicates and request-ID-only admission independently qualify the checkers. Event timing/cursor generation remains in `test_slskd_events_generated.py`; downstream processing, filesystem, importer, and terminal side-effect ownership is the explicit PR2 boundary |
| `tests/test_completed_purge_generated.py` | `purge_completed_transfers` | a write-ahead intent becomes ownership only after POST acceptance; pending and foreign keys are never mutated; 1-N successor IDs for each confirmed `(username, filename)` key remain owned across every terminal state; terminal accounting conserves every row through successful removals, false returns, and exceptions (`removed + removal_failed + foreign`); failed removals remain resident, a successful second pass is idempotent, and every removal uses `remove=true` |
| `tests/test_transfer_ledger_generated.py` | enqueue write-ahead ownership + `prune_transfer_ledger` | every owned enqueue is ledgered before POST and gains destructive authority iff accepted; pending intents older than the strict cutoff are pruned regardless of request status, while old accepted evidence survives only for `wanted`/`downloading`; exact-cutoff rows survive |
| `tests/test_request_lifecycle_generated.py` | `transitions.finalize_request` + `supersede_request_mbid` driven as a `RuleBasedStateMachine` over random operation sequences | statuses stay in the legal set; `replaced` rows are terminal and byte-frozen; identity (mbid/source/created_at) immutable; every replaced row has exactly one linked descendant; `active_download_state` only on `downloading` rows; the DB guards (claim, downloading→wanted) no-op on ineligible rows |
| `tests/test_multidisc_manifest_generated.py` | `try_multi_enqueue` with the REAL `check_for_match` matcher over generated multi-disc worlds | **the #550 coverage law**: an accepted multi-disc grab has no duplicate transfer identities, one distinct source folder per disc, unique coverage == track count. This harness **found the live #550 defect-#1 bug** (partial-disc manifests) before the production MANIFEST-TRACE window captured it |
| `tests/test_import_manifest_generated.py` | `build_active_download_state` ⇄ `lib/download_reconstruction.py::reconstruct_grab_list_entry`, `check_audio_manifest`, and `move_failed_import_curated` | manifest round-trip through persisted state never shrinks or grows (exact key/field fidelity); on-disk check oracle — pass iff disk audio set == tracked manifest set, extra/missing reported exactly; candidate/pressing rejects route to `wrong_matches/`, integrity/quality rejects remain in `failed_imports/`, and routed Wrong Match paths satisfy the cleanup safety boundary |
| `tests/test_import_job_payload_generated.py` | canonical prewrite validation plus `ImportJob.from_row` strict job-discriminated payload decoding | every generated valid force, automation, and YouTube input payload validates to the same JSON shape and decodes to its exact Struct; generated missing, `None`, zero, negative, boolean, empty, wrong-type, and extra-field defects fail both boundaries; discriminator/payload-class and invalid-world checkers reject planted known-bad inputs |
| `tests/test_track_replacement_generated.py` | real-PostgreSQL `PipelineDB.set_tracks` replacement | a later-row `NOT NULL` failure preserves the complete previous tracklist, including every generated field; an independent checker rejects a known-bad partial replacement |
| `tests/test_dispatch_outcomes_generated.py` | `dispatch_import_core` + every current rejection writer (`_reject_import_from_evidence_decision`, `_record_rejection_and_maybe_requeue`, `DatabaseSource.reject_and_requeue`, and `_reject_request_auto_import`) | the auditability law (every outcome writes a download_log row); full routing oracle vs `dispatch_action` across the decision universe + no-JSON crash path; U11 lifecycle policy for all 5 preimport facts (including `mixed_source`, missing from the hand-written table): automation self-heals to `wanted`, while terminal persistence preserves current operator-owned search state; every rejection writer preserves the `ValidationResult` distance/scenario projection exactly, including NULL and measured 0.0 |
| `tests/test_materialize_generated.py` | `lib/processing_paths.py::attempt_fingerprint` + `canonical_folder_for_row` / `canonical_processing_path`, and `lib/download_materialization.py::_materialize_processing_dir` | #550 phase 2 follow-up (PR #560 shipped deterministic-only): `attempt_fingerprint` is permutation-invariant, deterministic, distinguishes different `(username, filename)` sets, and the empty set is a stable defined digest; the fingerprint suffix is present iff the fingerprint is non-empty and the resulting basename stays ≤255 bytes even under adversarial generated unicode artist/title (r2 truncation guard); the materialize isolation law — two attempts for the same artist/title/year, driven through the real `_materialize_processing_dir` against a real tempdir, never blend files (attempt B's folder holds exactly B's manifest, A's folder is untouched) and identical manifests resolve to the same folder (resume stability) |
| `tests/test_quarantine_triage_generated.py` | `lib.quarantine_triage_service.list_unreferenced_quarantine_folders` | quarantine lifecycle law across request statuses: the deterministic result is exactly the immediate real album folders without a default-visible relative/absolute/descendant Wrong Matches reference; replaced frozen-audit rows and external references do not claim local folders, and the code-owned `bad_files` / `untracked_audio` roots are never surfaced or recursively traversed |
| `tests/test_wrong_match_policy_generated.py` | `lib.wrong_match_policy`, `lib.wrong_matches.wrong_match_row_is_visible`, and `FakePipelineDB.get_wrong_matches` | Wrong Matches remains a candidate/pressing review queue: all five folder/audio-integrity fact rejects plus `spectral_reject` stay excluded across generated scenario/status/history-view worlds, while arbitrary other or NULL scenarios remain visible unless the request is replaced in the default view; independently pinned oracles and known-bad checkers prevent the taxonomy, row predicate, and fake from drifting together |
| `tests/test_convergence_runner_generated.py` | `lib/convergence.py::run_convergence_steps` | every registered convergence step is attempted exactly once in declared order even when any arbitrary subset raises; Phase 0 order and the end-of-cycle harvest-before-purge constraint are pinned as registry data rather than source inspection; import failures are isolated like call failures |
| `tests/test_current_library_quality_generated.py` | `BeetsDB.check_mbids_detail` + `lib.banding.band_from_detail` | current beets projections preserve the positive-track minimum as floor data, expose the positive-track mean explicitly, and select that mean for codec-aware rank; the known-bad min-selected mutant is rejected |
| `tests/test_unused_import_audit_generated.py` | pinned repository-wide Ruff analysis plus exact production Vulture-whitelist freshness | an import is live only through its own binding in the importing module; same-named peer uses, parameter/comprehension shadows, and rebindings cannot mask it; exact intentional redundant-alias baselines reject expansions, duplicate identities, and stale entries even though Ruff accepts explicit re-exports; ordinary/generated/fake test modules share the production lint floor; unchanged Vulture entries remain valid while any generated source-location move is stale byte-for-byte; planted aggregate-name, baseline-delta, and name-only Vulture faults qualify the checkers |
| `tests/test_property_input_audit_generated.py` | the bounded drawn-input audit in `tests/test_property_input_audit.py`, which scans every Hypothesis property under `tests/` | a property's unused-input verdict equals an independent oracle across module/method/nested containers, keyword/positional `@given` and stateful `@rule` forms, and call/attribute/f-string/closure/comprehension uses versus `del`/never-mentioned/assign-only inputs; the checker trips exactly when an input is ignored and reports a stale allowlist entry otherwise; bare, `*args`, `**kwargs`, mixed, surplus-positional, unknown-keyword, and variadic shapes fail closed even when allowlisted; a `del`-only checker that misses never-mentioned inputs is known-bad. The #868 shape (a nested property that discards `album`/`download`) is pinned as an `@example` |
| `tests/test_import_failure_reason_generated.py` | `harness.import_one.run_import` through a real executable fake harness and real subprocess pipes | generated ordinary-exit and safe self-signal worlds always yield an observed human reason instead of the rc-only fallback; nonzero runs retain the last nonblank, non-filtered Beets stderr line when one exists, filtered/blank-only transcripts fall back to process status or signal, clean runs that never apply the requested release name that gap, and a planted reasonless terminal result qualifies the checker |
| `tests/test_import_one_argparse_generated.py` | the bounded `harness/import_one.py` argparse destination/direct-read contract | generated declared/read destination worlds pass exactly on equality and reject either-direction drift; the historical conditional `args.filetype` read is pinned, hyphen normalization and explicit `dest` values come from real argparse actions, and a union-based checker that would hide an undeclared read is known-bad |
| `tests/test_js_ast_generated.py` | flake-pinned tree-sitter JavaScript structural audits | supported direct payload literals produce exactly the independent field oracle across raw/escaped identifiers, shorthand, quoted/computed, nested, array, comment, string, template, Unicode, and ordering worlds; production payload fixtures use exact local aliases registered from the real renderer module, while raw renderer references, default/namespace/alternate imports, non-top-level or shadowed `__test__` registrations, computed `__test__` fixture calls, spreads, elisions, fixture indirection, and methods fail closed without attempting JavaScript dataflow inference; independent boundary worlds vary lexical scopes, repeated names, `let`/`const`, before/after member mutation, duplicate keys, registration/import shapes, unknown selectors, full browser-global-rooted semantic Object chains, and target expressions; unrelated modules and inert strings remain valid; emitted window handlers preserve ECMAScript raw/cooked escape semantics (including Unicode line continuations and lone surrogates), while bindings normalize escaped keys, treat full member chains rooted at `window`/`globalThis`/`self` as browser globals, reject every computed call rooted at semantic `Object` in a window-binding owner, and accept only exact direct `Object.assign(window, {...})` shapes across multiple blocks; planted quoted-key, template-interpolation, state-boundary, fail-open binding, and missing-binding mutants qualify the checkers |
| `tests/test_issue_reference_contract_generated.py` | `scripts/audit_issue_references.py` | implementation PR bodies and branch commit messages never use any GitHub auto-closing keyword with same-repo, cross-repo, or full-URL issue references across case, colon, whitespace, and issue-number worlds; canonical `Refs #N` and plain issue URLs remain valid, with the real premature-close shapes for issues #598 and #609 pinned as known-bad examples |
| `tests/test_deploy_pin_generated.py` | `scripts/pin_nixosconfig.sh` through deterministic process-level git/nix/token seams | a retry never creates a second signed pin across ordinary failures or signals after commit because the commit transaction advances a private pending ref; an interrupted pending candidate validates stable current master before either private-ref promotion or deletion, retaining both refs for an untrusted/incompatible remote. A divergent sibling receipt advances only when current Forgejo master has a verifiable allowed signature and preserves its exact target, otherwise it creates no commit, receipt update, or push. The sole recovery exception is a same-target candidate whose captured parent target is still on verified current master after its original push was rejected: one CAS replacement pin may be created from that master. Every push follows promotion to the durable receipt ref and carries the token header only in a trace-sanitized environment; every started detached worktree gets a cleanup attempt across update, signature, post-commit recovery, push, and cleanup faults, with planted invalid-pending, two-signed-commits/one-receipt, push-before-ref, divergent-untrusted pin, non-CAS receipt overwrite, and missing-cleanup violations qualifying the checker |
| `tests/test_deploy_cycle_verifier_generated.py` | `scripts/verify_cratedigger_cycle.sh` capture plus `verify-exact` through deterministic process-level systemd state, invocation-journal, and SSH-agent seams | capture preserves ordered starts after an exact journal cursor so a short failed target cannot disappear between state polls; only positive wrong-source evidence may be skipped, while a terminal manager-only start fails closed instead of yielding to a later green invocation; exact-target acceptance requires the expected deployed source token, application cycle completion, systemd successful deactivation, and a successful finished start job for the same invocation; every verifier SSH call disables the shared agent so a forced trigger key cannot consume verification; explicit failure evidence always rejects, rollover never substitutes the next invocation's evidence, and application-only success, next-invocation proof, and agent-eligible SSH calls qualify the checkers as known-bad |
| `tests/test_deploy_hold_generated.py` | `scripts/cratedigger_deploy_hold.py` through a state-respecting systemd/receipt backend | strict deploy acquisition first verifies the independently deployed main/YouTube controlled-start contract, survives interrupted atomic receipt publication, owns authoritative `system.control` masks for exactly the three trigger timers, drains every guarded service, and rejects every generated nonzero old-lifecycle preflight shape under the hold; controlled release creates receipt-owned main/YouTube inhibitors before releasing metadata, proves web/preview/importer readiness and overlapping resume exclusion, starts only main, retains YouTube until final release, and can recover every incomplete phase while removing only exact receipt-owned links/inhibitors. Planted low-precedence/service masks, surviving queued starts, unowned/tampered inhibitors, dirty preflight counts, and retained authority qualify the checkers |
| `tests/test_processing_owner_commands_generated.py`, `tests/test_import_execution_generated.py`, `tests/test_processing_cancellation_generated.py` | exact-owner DB commands, the shared execution-liveness decider, and real staged-tree/subprocess cancellation seams | every preview/import claim, evidence bind, heartbeat, path/launch mutation, and recovery action needs the request's exact owner, exact stage, and exact lease; changed boot or complete same-boot absence can prove death, while missing/contradictory/probe-error observations stay unknown; once pinned-session cancellation is observed, no later file move/remove or child process mutation starts. Owner-blind, heartbeat-only, PID-only, reconnecting, and preflight-only cancellation mutants qualify the independent checkers |
| `tests/test_cleanup_journal_generated.py`, `tests/test_processing_cleanup_generated.py` | real PostgreSQL journal owner/revision commands plus the real dirfd/no-follow cleanup executor over generated filesystem manifests | wrong owner or revision is zero-write; exact remove/quarantine/no-op uses one persisted source/destination manifest and collision-selected target, checkpoints every idempotent mutation, safely creates only the immediate journaled quarantine parent, resumes every boundary, and completes only with a typed receipt. Owner-blind, revision-blind, path-inference, allowlisted-debris, replace-existing, unsafe-parent, and receipt-free mutants are rejected |
| `tests/test_processing_visibility_generated.py`, `tests/test_operator_invalidation_generated.py` | production owner projection/status sets and canonical direct-delete/force/generic transition services | `processing_owner` exists iff the request is processing and must match the recorded pointer; acquisition/backlog include processing while transfer-ledger ownership does not; every processing owner rejects direct delete, force enqueue, and generic mutation with the same exact-owner conflict and unchanged request/files/audit. Latest-job projection, transfer-status broadening, and owner-blind deletion mutants qualify the checkers |
| `tests/test_destructive_authority_generated.py` | `ban_source`, Replace, and `delete_release_from_library` authority worlds plus pinned-Beets delete-manifest worlds varying source identity conflicts, track/art/sidecar presence, unknown bytes, no-op removers, partial I/O/enumeration faults, strict presence-probe faults at every progress phase, lost subprocess/protocol acknowledgements, and arbitrary child stdout prefixes/suffixes | every authority rejection is zero mutation; Replace rejects conflicting nonempty MB/Discogs source identities before lookup, locking, or Beets access; confirmed success leaves every owned target and Beets/PG authority absent while preserving unknown bytes; cleanup, enumeration, and presence-probe failures retain Beets/PG authority and never notify; ban-source reports completion iff the exact release is absent; PG partial is explicitly album-gone/pipeline-present; every ambiguous child acknowledgement remains incomplete regardless of metadata state, preserves PG, skips notification, and retains operator recovery context; a child response is valid only when stdout is exactly one canonical typed frame. Planted Replace fail-open, false ban completion, stdout contamination, omitted-art, omitted-sidecar, no-op-success, unknown-overdelete, early-Beets-delete, early-PG-delete, acknowledgement promotion/context-loss, enumeration/presence-success, authority-loss, and notification mutants qualify the checkers |
| `tests/test_beets_destructive_configs_generated.py` | real pinned Beets through the production `ban_source → current exact-release resolver → pinned exact-delete child` chain and the child directly; independently generated minimal/production plugin base, placeholder/readable/unreadable/invalid-UTF-8 secret mode, `importsource`, playlist, missing-plugin and include-level plugin override, MusicBrainz/Discogs identity, and 1/2/12-track axes | every valid configuration removes exactly the target metadata and owned files; invalid encoding, unreadable includes, and effectively configured missing plugins fail before mutation; separately owned import sources plus sibling album/item/file bytes survive; exact-delete stdout is one canonical typed frame even under Python/raw-fd diagnostics. The deterministic matrix executes 54 core cells; the generated property executes 18 real subprocess compositions in the suite and 96 fresh compositions in the randomized fuzz tier, with known-bad stdout contamination and false-completion self-tests |
| `tests/test_library_delete_notifiers_generated.py` | `notify_library_delete` targeting and observation across generated Plex filesystem ancestry and Jellyfin lookup/refresh worlds | Plex submissions always target the nearest existing ancestor inside the configured Beets root, never the deleted or an out-of-root path; Jellyfin targets an exact former-path item when observable and otherwise falls back to the configured library, reports `submitted` only after exact-item absence is observed, and contains lookup/refresh failures as warnings without escaping the completed delete boundary. Planted deleted/out-of-root targets, stale 2xx success, wrong-target, hidden-failure, and escaping-exception mutants qualify the checkers |
| `tests/test_jellyfin_refresh_generated.py` | the real `trigger_jellyfin_scan` entry point with only `urllib` replaced at the network leaf | every generated imported album maps to one exact Jellyfin-visible path in a `POST /Library/Media/Updated` body, with POST/token/JSON/timeout intact and no collection refresh or broad fallback; transport/HTTP/runtime failures stay inside the best-effort notifier boundary |
| `tests/test_world_invariants_generated.py` | the real world-invariant checkers plus `build_world_audit_report` over generated finding multisets | every emitted code has exactly one A/B/C owner, order and duplicates are preserved deterministically, unknown future codes fail closed to Bucket A, and only Bucket A drives integrity-failed status; known-bad omitted-code and flat-all-findings alarm shapes are rejected |
| `tests/test_world_audit_service_generated.py` | the real public owned and borrowed world-audit factory seams over generated resolver failures after an exact non-replaced request reaches the batch resolver | SQLite authority availability failures across both `DatabaseError` and `OperationalError` subclasses, including extended result codes, become one incomplete `observations_only` Bucket B `current_beets_authority_unavailable` report classified by primary code; SQLite schema errors and other unexpected exceptions propagate unchanged; owned handles close exactly once and borrowed handles never close. Planted complete-outage and OperationalError-only classifier mutants qualify the reusable checker |
| `tests/test_current_library_display_generated.py` | the production exact-identity Library artist and detail merges over generated current-Beets, acquisition-history, evidence, request-status, dual-tag attachment, request-cardinality, malformed-authority worlds, and direct request-detail routes over generated Beets read failures | presence, Captured history, current quality/proof, and tracking remain independent; zero matching requests keeps the primary observed identity untracked, one exact match owns the artist/detail action key, source, and detail history, while cross-source, duplicate modern Discogs, and modern-plus-legacy Discogs candidates fail closed as ambiguous instead of hiding a request; malformed, conflicting, and identityless pipeline-only rows remain visible but have no actionable release ID/source and never attach or suppress; sibling retags and deletions produce Captured plus Missing and held plus Untracked without identity inference; expected Beets availability failures return 503 and unexpected resolver defects return 500 without manufacturing current-library state or mutating the request; planted presence-to-capture, first-observation action-key, detached-source, detached-detail-history, first-wins overwrite, permissive-identity, and fabricated-unavailable-row mutants are rejected |
| `tests/test_long_tail_service_generated.py` | the public long-tail service over generated strict MusicBrainz, modern Discogs-only, and legacy duplicated-Discogs request cohorts plus the shared banding decision over generated `CurrentBeetsMissing`, `CurrentBeetsUnique`, every `CurrentBeetsAmbiguous` reason, mixed-format item orders, and production-shaped authority failures | every valid exact request identity is canonicalized into the single Beets batch; Missing alone emits `missing`, Unique ranks its exact item snapshot through canonical mixed-format precedence independent of item order, every ambiguity aborts without a payload, and FileNotFoundError or SQLite OperationalError escapes without fabricating Missing rows. MB-only selection, first-item format, and ambiguity-as-Missing mutants qualify the reusable checkers |
| `tests/test_long_tail_cache_lifecycle_generated.py` | the real DOM-free long-tail JavaScript load-failure transition, row renderer, and YouTube action over generated cached rows, selected bands, queries, console maps, request tokens, active Pipeline views, and exact MB/Discogs identity sources | only the current request failure invalidates cached rows and band selection, advances the console generation, and clears every console/action state while preserving the operator query; pre-failure work cannot recreate state or overwrite a new-generation result, stale failures remain no-ops, error paint is confined to the active Long Tail view, and either exact identity source retains its chip and drives the same resolver request body. Planted retained-cache, inactive-view-paint, stale-settle, MB-only-chip, and MB-only-action mutants qualify the reusable checkers |
| `tests/test_render_differential_generated.py` | `scripts/render_differential.py` — `summarize_render_diff`, the watched-field derivation over generated `msgspec.defstruct` output types, and the real Recents render path (`classify_log_entry` → `_project_current_library_have` → `_project_linked_import_evidence`) over generated `download_log` rows | the report is an exact census of two rendered corpora: every rendered field appears exactly once with zeros included, `changed_rows` and each per-field count equal independently recomputed differences, counts stay bounded by changed rows and changed rows by total rows, every sample is a real difference within budget, a corpus against itself reports nothing, and row order never matters; a row on one side only, a repeated id, or unsanctioned field-set drift fails closed, and an allowed drift names the unshared fields instead of dropping them. The derivation fails CLOSED — a field is unwatched only when its declared type is provably numeric/boolean/null, so strings, nested Structs, `object` and string-keyed mappings stay watched — and its converse holds on the real render path: no unwatched field ever holds text at runtime. A summarizer that silently drops the field that changed the most, plus undercount, understated-total, inflated-count, fabricated-sample, oversampled, text-left-unwatched, numeric-dragged-in, lost-field, and missing-rendered-field inputs, qualify the checkers |

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

## Heavy cross-engine world model

`tests/world_model/state_machine.py` drives real production request transitions
against a throwaway PostgreSQL database while a real pinned-Beets library adds,
removes, and moves generated tagged audio. Its rules cover request creation,
ordinary and force import, same-pressing upgrade/re-import, Replace, ban-source,
wrong-match deletion, and reset. After every rule it checks the shared
`lib/world_invariants.py` bank: folder exclusivity, replaced-row freezing,
status/membership, evidence/disk coherence, proof-lock terminality, search-tier
monotonicity, and denylist authority.

The canonical folder-exclusivity pin is the real Lisa Hannigan `Passenger`
shape from 2026-07-18: the existing ATO Records pressing is imported before a
same-key sibling whose label is empty, then the first pressing is upgraded. A
known-bad companion injects the exact historical `%aunique` template recovered
from commit `76ad5a0d`; the lifecycle must report `folder_shared`. The current
never-empty `path_disambig` policy must keep both exact pressings in distinct
folders through the upgrade.

Every generated world is initialized from the anonymized categorical corpus in
`tests/world_model/census_seeds.py`. The corpus was captured read-only from
doc2 on 2026-07-19 and retains only row-shape facts and capture-time counts:
status, MusicBrainz/dual identity shape, null/presence patterns, evidence
lineage, legacy search overrides, format casing, spectral provenance, and V0
presence. It contains no request/release IDs, artist/title metadata, peer names,
or paths. A deterministic pin proves that touching an installed lineage-1 seed
through the production import path rebuilds current lineage-4 evidence; the
state machine samples the coherent subset as its real initial state.

The read-only #743 fingerprint-drift census adds a second anonymized vocabulary
from all 238 live mismatches: 109 MP3→Opus replacements, two M4A→Opus
replacements, 119 same-codec filename renames, seven same-name size changes,
and one file-count change. Twenty-five of the codec replacements also changed
filenames, so the real-store mutation vocabulary includes both codec-only and
codec-plus-rename forms for each observed source container. It also contains
every linked fact shape (206 carried source/source, 25
carried-source/measured-source, two installed/installed, one
installed-without-V0, and four with neither fact). Deterministic real-store
pins execute every mutation and every fact shape. A generated property then
crosses those vocabularies with one to three unchanged action retries. Each
world mutates real tagged audio and the scratch Beets database without touching
its linked PostgreSQL evidence, proves the production invariant checker reports
`evidence_fingerprint_mismatch`, and requires the action loader to relink the
exact new snapshot without allowing an unenriched retry to become
authoritative. Ordinary and force-import pins then prove installed-only facts
produce the expected `have_analysis_error`, keep the request wanted, and
converge after the production enrichment path measures the exact new snapshot.

The #855 audit adds the separate 429-row `current_evidence_missing` cohort:
each row has one uniquely installed Beets album but no current-evidence FK.
Its anonymized marginal vocabularies retain historical origin, status,
MusicBrainz/Discogs identity, installed codec/container, operator search and
target intent, and legacy spectral/bitrate scalar presence. The real-store
state rule crosses those independent axes, first requiring the shared
invariant bank to report `current_evidence_missing`, then requiring the
production evidence loader to rebuild and link a lineage-4 snapshot without
mutating operator-owned request state. Explicit OGG/Vorbis and Windows
Media/WMA examples keep the native Beets format labels at that boundary.

Every lifecycle import — ordinary automation and force alike — hands its
enqueued job to the real preview worker (`process_claimed_preview_job`, the
shared `_run_preview_worker` step) before the importer may claim it, so
randomized sequences carry their accumulated candidate state through the same
ownership boundary production uses. A matching content snapshot takes the
front-gate reuse fast path (no full preview, the persisted candidate fact
projected without a second scan), and the importer then reads that
preview-produced evidence through the ordinary FK chain rather than a hand-forged
result. The reuse/measurement contract itself is probed on that same shared step
(`probe_candidate_preview_boundary`) across provider identity, codec, and spectral
grade: an unchanged snapshot must reach `candidate_status=reused` with no full
preview and no candidate analyzer call, while a changed snapshot cannot reach
importer ownership until exactly one full preview persists fresh evidence. The
installed HAVE side stays independently authorized: a complete matching
snapshot with a decision-usable grade is projected without analysis, while a
changed, incomplete, or unusable snapshot is measured and persisted before
dispatch. The explicit
unchanged force example uses the Rolling Stones incident identity; the exact
12-FLAC detail is pinned in the faster generated module.

The expanded lifecycle generator also shrank a separate counterexample from a
live-census seed: a successful unverified retained FLAC import widened an
existing lossless-only request back to the full tier set. Its deterministic
real-store pin and pure generated invariant now require retained-import search
policy to preserve the earlier narrowing.

Fault injection qualifies the retry property: temporarily removing the durable
gate makes the pinned installed/installed world become authoritative on its
first touch, and the generated test fails on that explicit example.

It is intentionally named outside unittest's `test*.py` discovery pattern so
raw discovery cannot accidentally inherit a randomized hammer environment.
The canonical `scripts/run_python_tests.py` runner adds it as an explicit
front-loaded queue target, fixes the six-example/eight-step deterministic
budget, and scrubs `TEST_DB_DSN`; `scripts/run_tests.sh` reaches it through
that runner on every normal suite. It can also be run directly while iterating:

```bash
nix-shell --run "python3 -m unittest tests.world_model.state_machine -v"
```

The temporary PostgreSQL, Beets SQLite database, library tree, generated audio,
and every other test scratch path live under one private per-shell tmpfs
directory beneath the operator's private runtime directory
(`/run/user/$UID` by default). That location is both RAM-backed and has
non-replaceable ancestry, so filesystem-authority tests can use the same
scratch root instead of escaping into the checkout. The dev shell fails closed
if that directory is unavailable, disk-backed, replaceable, or lacks headroom
rather than silently writing the suite's disposable workload to disk. This
runner never reads or mutates production. Normal EXIT handling still cleans the
current owned root automatically. A root left by a hard interruption requires
explicit operator cleanup after inspecting that exact path; the test
infrastructure has no automatic discovery or deletion authority over abandoned
roots.
The deterministic direct budget is six examples of eight stateful steps. On
doc1 on 2026-07-19, the initial census-seeded lifecycle module reported 10.431
test-seconds (excluding dev-shell startup). That is a historical baseline, not
a placement decision. With the #743 drift corpus, blocked/enriched import pins,
and retained-search counterexample added, the 14-test deterministic module
reported 20.396 test-seconds on doc1 on 2026-07-20. A 50-example drift-only pass
reported 23.197 test-seconds (24.80 seconds runner wall time). Override the
deterministic budget for a one-off run without changing code:

```bash
CRATEDIGGER_WORLD_EXAMPLES=20 CRATEDIGGER_WORLD_STEPS=20 \
  nix-shell --run "python3 -m unittest tests.world_model.state_machine -v"
```

The 20.396-second measured cost is about six percent of the 355-second normal
suite and was accepted for standard-suite coverage after issue #743 completed
its runtime qualification. Do not raise this deterministic budget in the
normal suite; deeper exploration belongs to the scheduled runners below.

### Randomized lifecycle hammer

The operator burst wrapper runs the same real-storage state machine with fresh
Hypothesis entropy, a persistent replay database, and a default budget of 25
worlds × 100 steps:

```bash
nix-shell --run "scripts/world_model_burst.sh"
nix-shell --run "scripts/world_model_burst.sh --examples 10 --steps 50"
nix-shell --run "scripts/world_model_burst.sh --print-config"
```

Every invocation unsets an ambient `TEST_DB_DSN` before importing the test
fixture, forcing a new ephemeral PostgreSQL instance. Beets SQLite, generated
audio, and the library tree remain per-world temporary state. The Hypothesis
database defaults to `.hypothesis/world-model` (gitignored): it replays a found
failure first on the next run, but is never a committed artifact. Randomized
failures print a reproduction blob and shrink to a minimal operation sequence.

On doc1 on 2026-07-19, the initial 3-world × 20-step randomized smoke completed
all six module tests in 8.470 test-seconds (10 seconds wrapper wall time). The
default 25 × 100 profile completed cleanly in 449.930 test-seconds (451 seconds
wrapper wall time). That measured depth is not part of `scripts/run_tests.sh`.
Its exact-revision doc1 schedule, alongside the long generated fuzz burst, is
the daily `scripts/daily_flake_update.sh` gate tracked by issue #498.

The default hammer uses the in-process production adapter that performs real
Beets model/database/filesystem mutations beneath the real dispatch services.
An opt-in profile crosses the real `import_one.py` →
`run_beets_harness.sh` subprocess boundary and performs exact-ID lookup against
an explicitly supplied MusicBrainz mirror origin:

```bash
nix-shell --run "scripts/world_model_burst.sh \
  --engine mirror-harness \
  --mirror-url http://192.168.1.43:5200 \
  --examples 2 --steps 5"
```

The mirror fixture is public catalogue metadata selected independently of the
production collection. Each synchronous subprocess receives a scratch runtime
config that names the per-world library DB, library root, and Beets config
directory; `BEETSDIR` and `BEETS_DB` match that scratch state as well. The
runtime config is the authority for zero-argument `BeetsDB()`, so masking it
or setting `BEETS_DB` alone is not sufficient. All environment values are
restored afterward; the pipeline database is still ephemeral. Only the mirror
is read. The profile
loads the shipped path template, exact-ID duplicate keys, inline field, match
policy, and MusicBrainz plugin; unrelated production fetchart/lyrics/scrub hooks
stay disabled so the boundary test cannot make external content requests. A
2-world × 5-step mirror smoke completed cleanly in 5 seconds on doc1 on
2026-07-19. It remains outside the normal suite and runs as a separately named
stage inside the daily issue-#498 gate, so mirror availability stays distinct
in the combined result.

When the hammer finds a defect:

1. Keep the replay database local and reproduce the shrunk operation sequence.
2. Fix the production boundary or the world mapping, stating which was wrong.
3. Promote the sequence to a named method on `TestPinnedLifecycleWorld`.
4. Run that named pin directly, then rerun the randomized profile that found it.

Never commit the Hypothesis database, a seed log, or an opaque JSON snapshot.
The readable named lifecycle is the durable regression artifact.

## Two tiers, one knob

`tests/_hypothesis_profiles.py` registers two profiles, selected by
`CRATEDIGGER_HYPOTHESIS_PROFILE`:

- **`suite`** (default) — deterministic (`derandomize=True`, no example
  database), bounded examples. Runs on every `scripts/run_tests.sh`,
  identical on every machine. This is part of the final local gate.
- **`fuzz`** — randomized burst for local exploration, with 500 examples by
  default. Fresh entropy per run, local example database (`.hypothesis/`,
  gitignored) so found failures replay first on the next burst,
  `print_blob=True` for exact reproduction. The unattended overnight gate
  explicitly sets `CRATEDIGGER_FUZZ_MAX_EXAMPLES=20000`.

Run a randomized burst whenever quality policy changes:

```bash
nix-shell --run "bash scripts/fuzz_burst.sh"                    # all generated modules
nix-shell --run "bash scripts/fuzz_burst.sh tests.test_quality_generated"  # subset
```

Loading a tier is an **import side effect**, so every module that uses
Hypothesis must import the profile module itself — at module level, and
**above the module's first `class`/`def`**:

```python
import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
```

A `@given`/`@settings(...)` resolves every knob it does not name from
`settings.default` **at decoration time**, i.e. while the class body runs.
That makes position load-bearing, not cosmetic: the same statement at the
BOTTOM of a module is a no-op for every decorator above it. A module without
the import (or with it too late) inherits whichever default happened to be
loaded when unittest imported it — the registered tier if an earlier module
pulled it in, stock Hypothesis defaults otherwise. Stock defaults are
randomized, example-database-backed and carry a 200ms deadline; a non-`None`
deadline is a hard failure in `scripts/run_fuzz_tests.py`'s discovery, so a
single unwired module stops the whole burst before it starts (issue #882).

Two independent gates enforce this:

- `tests/test_hypothesis_profile_audit.py` — the static half. That exact
  spelling in that position is the whole grammar; an alias, a `from`-import,
  a statement nested inside a function or an `if`, a canonical statement
  below the first definition, or no import at all all fail the suite. It
  scans **every** `.py` under `tests/` recursively, not the
  `test_*_generated.py` glob, because an unwired module outside that glob is
  invisible to the burst while still dragging stock defaults into the
  deterministic suite. `tests/_hypothesis_profiles.py` is the only exclusion.
- `scripts/run_python_tests.py::assert_hypothesis_deadlines_disabled` — the
  runtime half, run by every suite target child (and by fuzz discovery, which
  shares the same owner). It reads resolved settings rather than source, so
  no spelling or position can evade it. Scoped to `deadline` alone:
  `derandomize` and `database` are legitimately varied by
  `tests/world_model/state_machine.py` under `CRATEDIGGER_WORLD_RANDOMIZED=1`.

`scripts/fuzz_burst.sh` discovers the exact unittest IDs and effective
Hypothesis settings in every generated module. Ordinary deterministic pins run
once as a batch, and fixed per-test `@settings(max_examples=...)` budgets remain
single-run. Each property using the default fuzz profile divides its generated
examples exactly across independent entropy shards. The automatic fan-out is
bounded by both host cores and the effective budget discovered from the loaded
profile, with at least 250 examples per entropy child. The total budget does not
grow: on a 30-core host, the ordinary 500-example profile uses two processes of
250 examples, while the 20,000-example overnight gate keeps eight processes of
2,500. The child runner loads the owning module and selects the exact discovered
ID, so dynamically named state-machine tests can be sharded without repeating
the module's other properties or pins. An exact-budget check rejects any
schedule that omits a test, repeats a pin, changes a property's combined example
count, or invents an ID.

The queue uses every host core by default for ordinary targets. Targets whose
module boots an ephemeral PostgreSQL cluster are capped at two concurrent
processes, bounding their tmpfs footprint independently of the queue length;
eligible ordinary work bypasses a PostgreSQL-backed target waiting for that
resource. Any `ENOSPC`, PostgreSQL `DiskFull`, or unexpected loss of an
ephemeral database aborts further admission and reports that the property
verdict is invalid. Set `CRATEDIGGER_FUZZ_JOBS` to cap all concurrent
processes or `CRATEDIGGER_FUZZ_PROPERTY_SHARDS` to override the automatic
entropy fan-out. On doc1's 30-core VM on 2026-07-23, the complete 71-module,
769-test overnight burst at 20,000 examples completed in 607.8 seconds. The
worst subprocess-heavy generated module completed alone in 142.7 seconds; its
unsharded properties had still not completed after ten minutes.

On the same 30-core host on 2026-08-02, a matched 500-example run on one tree
completed 115 modules, 1,432 tests, and 433 properties in 180.1 seconds with
the former eight-shard fan-out (3,359 targets). The budget-aware two-shard
default completed the same inventory in 106.0 and 106.0 seconds (935 targets),
a 41.1% wall-time reduction. These timings justify the 250-example floor on
this host; they are measurements, not a universal performance promise. The
same tree's automatic 20,000-example run retained eight shards and completed
all 3,359 targets in 1,172.3 seconds.

### Per-property depth report

Issue #888 item 1. A budget is not depth: `@given(authorized=st.booleans(),
missing=st.booleans())` has four distinct worlds, so Hypothesis exhausts it in
four examples and stops — whether the budget is 150, 500, or 20,000. A mutant
that widens a quarantine authority boundary survived 215 tests across seven
modules for exactly that reason, and nothing in the burst output said so.

After the `SLOW` lines, every burst now prints what each property actually
generated, measured from Hypothesis' own `statistics` collector in the child
that ran it:

Measure depth only with the supported isolated runner/report. Never
import or load every generated module into one process: the result is
order-contaminated rather than a valid per-property depth measurement.

```
DEPTH 351 properties measured, 68 shallow (space exhausted below budget), 6 discarding at least 10% of their examples
SHALLOW 2 worlds vs 150 examples per shard (1 shard, 150 total) tests.test_pin_retention_generated.TestGeneratedPinRetention.test_every_status_is_pending_or_terminal
SHALLOW ... 48 more shallow
DISCARD 51% of 307 examples (157 discarded, 150 worlds) tests.test_world_invariants_generated.TestWorldInvariantGenerated.test_any_evidence_fingerprint_drift_is_rejected
```

- **SHALLOW** — the property ran out of distinct worlds before it ran out of
  budget (`stopped-because: nothing left to do`), ranked by world count
  ascending, top 20. Entropy shards are folded into one row first. That
  folding does not change WHICH properties are flagged — a shard that stops
  at its own budget reports `settings.max_examples=…`, never exhaustion, so
  an unfolded verdict picks out the same set. It de-duplicates eight
  identical rows out of the ranked list and reports the property's real total
  budget against one distinct-world bound: the largest single shard, since
  shards re-explore the same space.
- **DISCARD** — the property threw away at least 10% of its examples through
  `assume()` or a strategy filter, ranked by rate descending. This is a cost
  and shape signal, not a defect: `assume` marks a world invalid and
  Hypothesis refills the budget, which is exactly why it is the correct way
  to drop an unanswerable world.

Only properties are counted. A plain test whose body declares and calls its
own `@given` — the shape every known-bad self-test uses — emits statistics
too, under the enclosing test's id; those are dropped, so the denominator is
exactly the number of real `@given` property tests.

**It reports; it never gates.** A small strategy space is often exactly
right — the 36-world force-import authority property is correct — so a
threshold would either be trivially satisfiable or block legitimate work. Two
carve-outs keep the verdict honest: a run that reached an `interesting` case
stopped because it found its planted bug, and a property whose worlds reach
its budget wasted nothing. The first is defensive rather than load-bearing
today: no repository property reaches that state, precisely because the
known-bad self-tests plant their bug in a dropped inner `@given`.

**The ceiling is the per-child budget.** Only a space smaller than the
`N examples per shard` figure — 150 at the deterministic tier, 62 or 63 on a
30-core host for the default 500-example fuzz burst, and 2,500 for its
20,000-example overnight counterpart — can be observed at all. A world space
larger than the applicable ceiling never exhausts, so it is reported as deep,
and every non-shallow property's worlds are bounded by that per-shard budget
rather than by its real space. An empty SHALLOW section means "none found
below the ceiling", never "no shallow properties".

**It cannot see a bare `return`.** A `return` spends the example as a PASS, so
a vacuously-discarding property reads as a full budget of valid worlds. That
is unchanged by this report and is why the `assume`-not-`return` rule below
still has to be followed by hand.

Measured on doc1, 2026-07-26, deterministic tier, all 89 generated modules
(963 tests, 58.3s): 351 properties, 68 of them shallow — the smallest at two
worlds against 150 examples. Those are the properties for which a deeper
burst buys nothing at all; widening their strategies is separate work this
report exists to aim. Every figure here moves with each property added or
strategy widened, so re-run the burst rather than trusting these numbers.

All active logs, property tempdirs, and Hypothesis database writes stay in the
private per-shell tmpfs. A database named by
`HYPOTHESIS_STORAGE_DIRECTORY` seeds the run read-only. A green run discards
its active database and logs without writing persistent storage. On a property
failure, the active database is copied back so the shrunk example replays.
An infrastructure abort discards that contaminated database instead.
`CRATEDIGGER_FUZZ_OUTPUT_DIR` optionally receives the complete logs plus an
exact target manifest beneath a unique `run.*` directory for either kind of
failure. The ordinary burst uses 500 examples; the daily unattended gate
preserves the 20,000-example depth. The serial equivalent, when you need one
module's live output:

```bash
nix-shell --run "CRATEDIGGER_HYPOTHESIS_PROFILE=fuzz \
    python3 -m unittest tests.test_quality_generated -v"
```

It is pure and safe: no prod DB, no slskd, no beets, no network. Green runs
write disposable state only to tmpfs. Repeat runs add entropy; there is
nothing to resume and no seed cursor — coverage grows by improving strategies
and invariants, not by consuming more seeds.

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

Interpret results per mutant: **killed** = the property works; **killed only at
fuzz entropy** = the deterministic suite budget misses the decisive world, so
pin it as an `@example`; **survived both tiers** = either a missing invariant
(add it, with a known-bad self-test) or a world the strategies rarely make
decisive (again, pin the decisive world). The mutation driver is an
operator/agent one-shot — never committed (`.claude/rules/scope.md`); record
the kill matrix in the issue/PR.

Canonical run (issue #548, 2026-07-08): 13 mutants — including reverting
fix `6cf26a4`, which the generated lifecycle property killed independently
of its hand-written regression tests — 10 killed outright, 1 only under
deeper randomized entropy, 2 survivors fixed in PR #555
(`assert_below_gate_never_stops_search` and the
`_SPECTRAL_OVERRIDE_DECISIVE_WORLD` parity pin).

## Every property must use every input it draws

Issue #882 item 5. During the #868 series one generated property invoked no
production symbol at all: it `del`'d its generated input and then asserted a
relation between two test-local constants, under a "real materialize worlds,
real filesystem" docstring banner. It survived **0 of 7** planted mutants,
including one that deleted a containment check outright and failed 25 other
tests. A property that ignores its world patrols nothing, and nothing stopped
the next one.

`tests/test_property_input_audit.py` catches the detectable tell: it walks
every `.py` under `tests/` with the stdlib `ast` module and requires that each
parameter bound by `@given`, `@rule`, or `@initialize` is loaded somewhere in
that function's body. Never mentioned, only `del`'d, or only assigned over —
all three are the defect. `@invariant` is out of scope because it binds no
arguments at all (its pinned signature is
`invariant(*, check_during_init: bool = False)`).

**Read the guarantee in one direction only.** "Every drawn input is used" does
NOT mean "every property drives production". The audit run against the pre-fix
#868 module (`git show 20f309ac^:tests/test_materialize_evidence_generated.py`)
does flag that property — but on its unused third input, `leaf`. The primary
defect, indexing two test-local dicts instead of driving production, is
invisible to this criterion and to any criterion the repository is willing to
build (see the rejected criterion below). A property that passes this audit
may still patrol nothing; only review and mutant-kill counts show that.

- The audit **fails closed**: a decorator or signature shape it cannot map
  (bare decorator, `*args`, `**kwargs`, mixed positional/keyword, surplus
  positional strategies, a keyword that is not a parameter, a variadic
  signature) fails rather than passes, and the allowlist cannot excuse it.
  Aliasing a decorator import (`from hypothesis import given as g`) fails
  too, since discovery is by decorator name. A future DRY idiom such as
  `@given(**_COMMON_STRATEGIES)` is therefore a hard build break until the
  audit is extended — deliberate: an unmappable decorator must not pass.
- `PROPERTY_INPUT_ALLOWLIST` is **empty and armed**. The one property that
  used to flag — the planted-bad-decider self-test in
  `tests/test_quality_generated.py` — now passes its world to a decider that
  ignores it, which models "a decider that ignores its world" more faithfully
  than discarding the world did. A stale entry that no longer flags also fails
  the audit.
- It is a **bounded syntactic fact**, not a semantic scanner
  (`.claude/rules/code-quality.md` § "Semantic source scanners are
  prohibited"). It never infers what the body does with the value. The module
  docstring names the ceiling — all fail-open, all with zero live instances
  today, none to be closed case by case: a drawn input loaded only inside an
  assertion message or `subTest` label (the likeliest escape, and one that
  reads as better failure text); binding constructs that mask an unused input
  (`for x in ...`, `with ... as x`, `except E as x`, comprehension targets);
  non-import decorator aliases (`_g = given`, wrapper functions,
  `partial(given)`); `given(...)(func)` call form; and shadowing or rebinding
  before a load.
- The criterion issue #882 originally proposed — "every property must
  reference at least one production symbol" — was **measured and rejected**:
  44 of the 352 properties in `tests/test_*_generated.py` (12.5%) flag
  falsely, because they drive production through a module path inside a
  string passed to `node`, through a subprocessed script, or through
  `self.<attr>` in a state machine. Making it work needs a string-path
  registry plus subprocess argv analysis, which is the prohibited scanner.
  The module docstring records the measurement so it is not re-proposed.

## Writing new generated tests

- **Every drawn input must be used.** See the section above; the audit is
  automatic, so the practical rule is: if a property does not need an input,
  do not draw it.
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
- Import `tests._hypothesis_profiles` for the side effect **above the first
  `class`/`def`** — that is what wires the module into the suite/fuzz tiers,
  and both `tests/test_hypothesis_profile_audit.py` and the suite runner's
  own `assert_hypothesis_deadlines_disabled` fail without it.
- Discard an unanswerable world with `assume(...)`, never a bare `return`.
  A `return` spends the example as a PASS and silently shrinks the real
  budget; `assume` marks it invalid so Hypothesis refills. Issue #882 found
  a property burning 29% of a 20,000-example budget this way. If the world
  has a defined answer, assert it instead of discarding it. The burst's
  `DISCARD` lines price the `assume` cost; a `return` shows up nowhere, so
  this one is still enforced by hand.
- **Check the burst's `SHALLOW` lines for the property you just wrote.** A
  strategy space smaller than the budget is a fact worth knowing before the
  next mutant hunt reports a survivor.
- Reuse the shared fakes/builders (`tests/fakes/`, `tests/helpers.py`)
  per `.claude/rules/code-quality.md`; leaf-seam mock rules apply to
  generated tests like any other test.
