# Generated (property-based) testing

Issue #548. Hypothesis-driven generated tests assert **production policy
invariants** over large generated state spaces instead of hand-picked examples. The normal
pure/fake-backed modules are ordinary members of the unittest suite — no seed
bookkeeping or standing service. The deterministic cross-engine world model is
also part of the normal suite at its measured small budget. The deeper
randomized, mirror-backed, and long-fuzz runs execute together in the daily
unstable compatibility gate tracked by issue #498. The read-only live audit
joins that same run after issue #762 establishes current-path authority. The
public report groups findings by Cratedigger integrity, current-projection
health, and Beets/library health; issue #910's separate member-level debt gate
accepts only a complete exact stable or shrinking approved cohort.

Generated testing stops at the production boundary. Test runners, fuzz
schedulers, suite/final-gate coordinators, selectors, profile wiring, tmpfs
helpers, fixtures, strategies, invariant checkers, and static/test-tree audits
are test infrastructure and use deterministic tests only. They must not add a
`test_*_generated.py` companion or expose a property to the daily fuzz queue.
A deterministic integration pin may construct a tiny Hypothesis property when
testing Hypothesis integration itself; it runs at one exact local budget and is
never scheduled as generated coverage. Production properties may still use
fakes and helpers—the subject under assertion is the boundary.

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
   must-still-work guard, and a message-asserting known-bad self-test for
   every clause of the checker (§ "Per-clause proof").
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
7. Pin discovery deterministically in `tests/test_fuzz_burst.py`: cardinality,
   explicit budget, one target, no profile override. Run the production target
   under the production
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
| `tests/test_convergence_pipeline_db_generated.py` | the real migration-070 PostgreSQL request-local derivation and atomic stop CAS, differentially checked against `derive_convergence_signal` | genuine Hypothesis worlds cross direct/cross-walk attribution, structured peer arrays including comma and punctuation usernames, empty contributor sets, timestamp ties resolved by log ID, null/different cliff breaks, raw within-band spread, codec diversity, current-evidence replacement, and ineligible worlds; generated late-link, contributor, current-authority, and spectral mutations invalidate captured tokens before mutation, while one-observation/five-peer mosaic and cross-walk mutants qualify the checker. The bounded domain is sampled, not falsely certified as exhaustive |
| `tests/test_audio_validation_generated.py` | `lib.util.validate_audio` through the real pinned FFmpeg full-decode subprocess plus mocked process-outcome leaves | every generated FLAC audio-frame mutation fails the fixed audio-only strict policy even when the former command returns zero; a real unset STREAMINFO MD5 remains byte-identical and leaves no diagnostic; every positive FFmpeg exit on stable readable bytes is typed bad audio, while arbitrary exit-zero stderr has no policy or audit meaning; diagnostics remain bounded. The real-subprocess property executes 18 mutations in the suite and 96 fresh mutations in the randomized fuzz tier; the former `rc=0` contract is pinned as a known-bad checker input |
| `tests/test_web_request_security_generated.py` | the pure request authorizer plus the real HTTP pre-dispatch boundary for the cache-writing YouTube resolver | generated channel, method, canonical-origin, Origin, and Referer worlds match the fail-closed authorization oracle; browser POSTs with missing or mismatched provenance return 403 before resolver dispatch or DB access. Missing-provenance, mismatched-origin, future-unsafe-method, resolver-call, and DB-touch mutants independently qualify the checkers |
| `tests/test_youtube_album_service_generated.py` | `lib.youtube_album_service.resolve_youtube_album` driven through the shared production Requests Session/HTTPAdapter and a loopback HTTP server | generated worlds cross every configured retryable status (429/500/502/503/504), GET/POST, independent refresh state, absent/empty/nonempty durable cache posture, and search/seed/one-sibling/all-siblings operation site. `total=3` produces four real attempts per exhausted request and the public `requests.exceptions.RetryError` never escapes: outer or all-sibling exhaustion returns `unresolved_mirror_unavailable` without a durable write, nonempty refresh returns the exact `ok` / `from_cache` matrix, one-sibling exhaustion excludes only that sibling and persists the exact remaining membership, and the existing HTTP/CLI maps remain authoritative. Escaped-exception, generic/wrong-result, fallback-drift, independent upsert/durable-rewrite, sibling-overreach, partial-membership-drift, and bypassed-attempt-count inputs qualify the checker |
| `tests/test_world_audit_debt_generated.py` | `lib.world_audit_debt.assess_world_audit_debt` over generated live-audit cohorts | a candidate passes exactly when it is an exact subset of the persisted member fingerprints; stable members remain, resolved members monotonically disappear, and any new member or changed cause for a known identity fails without advancing state. A same-count member replacement qualifies the count-only known-bad mutant |
| `tests/test_quality_generated.py` | the decision twins (`full_pipeline_decision` / `full_pipeline_decision_from_evidence`) | decisions are definitive (totality); raw verified-lossless FLAC never replaced by lossy; transparent lossy never accepts obvious downgrades; **twin parity** over the shared world language; evidence-only integrity facts (corrupt / bad-hash / nested / mixed) always reject in priority order; classification layer (`classify_full_pipeline_decision` / `evidence_decision_name` — cleanup eligibility) coherent with every generated decision; incomplete evidence fails closed |
| `tests/test_search_override_generated.py` | `rejection_backfill_override` over codec measurements and attempt-local HAVE audits, plus retained-import override resolution | lossless-only search narrowing occurs exactly for canonically TRANSPARENT measurements with a completed genuine HAVE audit; excellent, unknown/lossless codecs, missing/failed audits, and non-genuine grades fail open; a successful retained import never widens an existing lossless-only scope |
| `tests/test_search_scheduler_generated.py` | `FakePipelineDB.get_wanted_searchable` scheduler-parity boundary over generated cohort sizes, ages, eligibility states, attempt counts, and page sizes | selection contains no duplicates or ineligible rows, never wastes eligible page capacity, reserves a floor-rounded quarter share for sub-24-hour requests (four at production page size 16), preserves the established floor while established work is eligible, borrows unused capacity both ways, and ignores attempt counts when classifying aged requests |
| `tests/test_quality_lineage_generated.py` | target-quality contracts, measurement/evidence projection, and the importer terminal boundary | `from_explicit_label` rejects bare MP3 across case/whitespace variants; `from_projection` requires a CBR/VBR boolean and preserves it for bare MP3; explicit V/numeric labels own their mode and gate policy even when the measured boolean contradicts them; exact single/equal/differing-track projections preserve mode; measurement-only early rejects never claim target policy; an invalid partial result is discarded and emits exactly one typed terminal crash acknowledgement |
| `tests/test_evidence_generated.py` | `ensure_current_evidence_for_action` | converted current evidence requires a linked source V0 fact (fix `6cf26a4`): never `loaded` without it, request scalar mutations cannot change the result, and missing evidence fails closed; fingerprint rebuilds carry only source-subject facts with `carried` provenance |
| `tests/test_evidence_media_identity_generated.py` | temporary Beets-shaped SQLite through `BeetsDB.get_album_info` → current-evidence propagation → final policy projection | the generated input inventory comes from the real native-format and alias producer surface, while an independently authored policy inventory fails closed on unknown canonical outputs; MP3, AAC, ALAC, FLAC, Opus, Vorbis, WAV, and WMA cross the outer adapter with a NULL analyzer generation and assert the final projected grade itself. Deterministic mixed M4A and OGG albums prove the aggregate format set withholds stale source grades. Known-bad final-policy bypass and unknown-alias inputs qualify both checkers. |
| `tests/test_preview_failure_evidence_generated.py` | `process_claimed_preview_job` plus failure-point current-evidence preparation/enrichment | every producer stage that terminates as `measurement_failed` reaches one lifecycle owner: request-owned failures always persist their job/audit and diagnostic; an exact readable installed release links a complete pre-attempt HAVE snapshot regardless of stage or job type; missing identity, absent/unreadable HAVE, and preparation/enrichment faults stay fail-soft without fabricating evidence |
| `tests/test_spectral_attempt_audit_generated.py` | production HAVE authorization, both attempt-audit adapters, preview writer/persistence round-trips, and real automation/force-import dispatch consumers | candidate and HAVE authorization remain independent across automation and force jobs: each complete matching snapshot with a decision-usable grade projects its own persisted spectral fact without another scan, while changed, incomplete, or unusable HAVE evidence takes the existing measurement-and-persistence path; the generated installed-codec matrix includes Beets' canonical OGG → Vorbis label; ordinary installed evidence needs the current analyzer generation, whereas a recognised preserved source grade may retain NULL/old/future provenance without scanning the derivative; blank/error/unknown grades never enter policy; the Iron & Wine witness drives automation dispatch and terminal cleanup, while a separate slice drives force-import dispatch |
| `tests/test_lossless_lineage_check_generated.py` | migration 073's real PostgreSQL lineage schema plus role-aware FakePipelineDB writer-order worlds | fresh candidate truth clears unowned legacy conversion lineage at the same content address without erasing a co-referenced current row; action-time, CLI, decision-differential, Recents, and Wrong Matches candidate projections all withhold current-only lineage from a shared row; current-evidence rebuilds preserve conversion lineage for an unchanged installed snapshot in either spectral/rebuild order; installed spectral measurements and output lineage may coexist; the exact manifest-aware R19 predicate alone determines whether a carried source spectral fact is reusable, failing closed for native, mixed, or unresolved output |
| `tests/test_slskd_events_generated.py` | `ingest_download_file_events` (event stamping — the ONLY source of completed-file locations) | stamping oracle (newest decodable event per key in the new-events window, nothing else); totality + exactly-once over wild feeds (dup ids, garbage timestamps, undecodable payloads, pruned/absent cursors, rows leaving `downloading` mid-ingest); duplicate-id invariance (mid-pagination shape); issue #898 PR1 current-incarnation classification across reused/different keys, event times before/at/after the exact `enqueued_at` witness, candidate shadowing, lost witnessed writes, cursor hold/replay, and idempotent ledger effects |
| `tests/test_download_incarnation_generated.py` | `FakePipelineDB.update_download_state_if_downloading` plus `lib.download._admit_download_incarnations` | issue #898 PR1 integrated invariant: after B replaces A at the same request/path, delayed enqueue/event/harvest/poll whole-state payloads carrying A's witness leave B and its row metadata byte-for-byte unchanged, while current B payloads succeed; post-event polling admits only refreshed exact `(request_id, enqueued_at)` pairs captured before the transfer snapshot, preserves refreshed order and original witness text, and excludes same-ID B replacements, departed/new rows, and missing/empty/malformed/invalid state. Missing status/stored-witness/outgoing-witness predicates and request-ID-only admission independently qualify the checkers. Event timing/cursor generation remains in `test_slskd_events_generated.py`; downstream processing, filesystem, importer, and terminal side-effect ownership is the explicit PR2 boundary |
| `tests/test_completed_purge_generated.py` | `purge_completed_transfers` | a write-ahead intent becomes ownership only after POST acceptance; pending and foreign keys are never mutated; 1-N successor IDs for each confirmed `(username, filename)` key remain owned across every terminal state; terminal accounting conserves every row through successful removals, false returns, and exceptions (`removed + removal_failed + foreign`); failed removals remain resident, a successful second pass is idempotent, and every removal uses `remove=true` |
| `tests/test_transfer_ledger_generated.py` | enqueue write-ahead ownership + `prune_transfer_ledger` | every owned enqueue is ledgered before POST and gains destructive authority iff accepted; pending intents older than the strict cutoff are pruned regardless of request status, while old accepted evidence survives only for `wanted`/`downloading`; exact-cutoff rows survive |
| `tests/test_request_lifecycle_generated.py` | `transitions.finalize_request` + `supersede_request_mbid` driven as a `RuleBasedStateMachine` over random operation sequences | statuses stay in the legal set; `replaced` rows are terminal and byte-frozen; identity (mbid/source/created_at) immutable; every replaced row has exactly one linked descendant; `active_download_state` only on `downloading` rows; the DB guards (claim, downloading→wanted) no-op on ineligible rows |
| `tests/test_multidisc_manifest_generated.py` | `try_multi_enqueue` with the REAL `check_for_match` matcher over generated multi-disc worlds | **the #550 coverage law**: an accepted multi-disc grab has no duplicate transfer identities, one distinct source folder per disc, unique coverage == track count. This harness **found the live #550 defect-#1 bug** (partial-disc manifests) before the production MANIFEST-TRACE window captured it |
| `tests/test_import_manifest_generated.py` | `build_active_download_state` ⇄ `lib/download_reconstruction.py::reconstruct_grab_list_entry`, `check_audio_manifest`, and `move_failed_import_curated` | manifest round-trip through persisted state never shrinks or grows (exact key/field fidelity); on-disk check oracle — pass iff disk audio set == tracked manifest set, extra/missing reported exactly; every scenario this mover ever sees — known (delete-eligible or excluded) or an arbitrary novel string — routes to the single `wrong_matches/` root (issue #1077, D1/D3/D6: the historical `failed_imports` split has no producer left), and every routed path satisfies the cleanup safety boundary |
| `tests/test_import_job_payload_generated.py` | canonical prewrite validation plus `ImportJob.from_row` strict job-discriminated payload decoding | every generated valid force, automation, and YouTube input payload validates to the same JSON shape and decodes to its exact Struct; generated missing, `None`, zero, negative, boolean, empty, wrong-type, and extra-field defects fail both boundaries; discriminator/payload-class and invalid-world checkers reject planted known-bad inputs |
| `tests/test_import_queue_generated.py` | candidate-evidence requeue gate through the real automation exact-owner and force terminal adapters | generated age, retained counter, reason, and owner-kind worlds either wait for the exact growing preview cadence or, at the one-hour budget, self-heal the exact automation owner to `wanted` with a Recents audit; non-owning force bails terminally by removing its private action copy without treating candidate quality as a Wrong Matches cleanup decision |
| `tests/test_track_replacement_generated.py` | real-PostgreSQL `PipelineDB.set_tracks` replacement | a later-row `NOT NULL` failure preserves the complete previous tracklist, including every generated field; an independent checker rejects a known-bad partial replacement |
| `tests/test_dispatch_outcomes_generated.py` | `dispatch_import_core` + every current rejection writer (`_reject_import_from_evidence_decision`, `_record_rejection_and_maybe_requeue`, `DatabaseSource.reject_and_requeue`, and `_reject_request_auto_import`) | the auditability law (every outcome writes a download_log row); full routing oracle vs `dispatch_action` across the decision universe + no-JSON crash path; U11 lifecycle policy for all 5 preimport facts (including `mixed_source`, missing from the hand-written table): automation self-heals to `wanted`, while terminal persistence preserves current operator-owned search state; every rejection writer preserves the `ValidationResult` distance/scenario projection exactly, including NULL and measured 0.0; for these four rejection writers specifically, every `source_denylist` row is traceable to one of the two terminal-outcome bundles rather than a separate `db.add_denylist` call outside either (issue #1355 item 3) — `lib/dispatch/quality_gate.py` and `lib/dispatch/post_import.py` still denylist through a separate `db.add_denylist` call outside any bundle when there is no pending job to attach to, and this property does not cover them |
| `tests/test_materialize_generated.py` | `lib/processing_paths.py::attempt_fingerprint` + `canonical_folder_for_row` / `canonical_processing_path`, and `lib/download_materialization.py::_materialize_processing_dir` | #550 phase 2 follow-up (PR #560 shipped deterministic-only): `attempt_fingerprint` is permutation-invariant, deterministic, distinguishes different `(username, filename)` sets, and the empty set is a stable defined digest; the fingerprint suffix is present iff the fingerprint is non-empty and the resulting basename stays ≤255 bytes even under adversarial generated unicode artist/title (r2 truncation guard); the materialize isolation law — two attempts for the same artist/title/year, driven through the real `_materialize_processing_dir` against a real tempdir, never blend files (attempt B's folder holds exactly B's manifest, A's folder is untouched) and identical manifests resolve to the same folder (resume stability) |
| `tests/test_quarantine_triage_generated.py` | `lib.quarantine_triage_service.list_unreferenced_quarantine_folders` | quarantine lifecycle law across request statuses: the deterministic result is exactly the immediate real album folders without a default-visible relative/absolute/descendant Wrong Matches reference; replaced frozen-audit rows and external references do not claim local folders, and the code-owned `bad_files` / `untracked_audio` roots are never surfaced or recursively traversed |
| `tests/test_wrong_match_policy_generated.py` | `lib.wrong_match_policy`, `lib.wrong_matches.wrong_match_row_is_visible`, and `FakePipelineDB.get_wrong_matches` | Wrong Matches remains a candidate/pressing review queue: all five folder/audio-integrity fact rejects plus `spectral_reject` stay excluded across generated scenario/status/history-view worlds, while arbitrary other or NULL scenarios remain visible unless the request is replaced in the default view; independently pinned oracles and known-bad checkers prevent the taxonomy, row predicate, and fake from drifting together |
| `tests/test_convergence_runner_generated.py` | `lib/convergence.py::run_convergence_steps` + `cratedigger.py::run_cycle` through fakes | every registered convergence step is attempted exactly once in declared order even when any arbitrary subset raises; Phase 0 order and the end-of-cycle harvest-before-purge constraint are pinned as registry data rather than source inspection; import failures are isolated like call failures; each registered step's failure message is reachable (per-step collaborator-down worlds); a complete `run_cycle` executes against `FakePipelineDB`/`FakeSlskdAPI`, and arbitrary subsets of step-touched DB methods raising never prevent the Phase 2 wanted scan or cycle completion |
| `tests/test_current_library_quality_generated.py` | `BeetsDB.check_mbids_detail` + `lib.banding.band_from_detail` | current beets projections preserve the positive-track minimum as floor data, expose the positive-track mean explicitly, and select that mean for codec-aware rank; the known-bad min-selected mutant is rejected |
| `tests/test_import_failure_reason_generated.py` | `harness.import_one.run_import` through a real executable fake harness and real subprocess pipes, plus the pure `_terminal_process_reason` producer | generated ordinary-exit and unignorable-signal worlds always yield an observed human reason instead of the rc-only fallback; nonzero runs retain the last nonblank, non-filtered Beets stderr line when one exists, filtered/blank-only transcripts fall back to the producer's status or signal wording, clean runs that never apply the requested release name that gap, and a planted reasonless terminal result qualifies the checker. Signal NUMBERS are patrolled process-free over generated signal and status worlds — a death and an exit status sharing digits never read alike — because only SIGKILL survives an ancestor holding `SIG_IGN`/`SIG_BLOCK` (see "Ask the OS only for worlds it guarantees") |
| `tests/test_deploy_pin_generated.py` | `scripts/pin_nixosconfig.sh` through deterministic process-level git/nix/token seams | a retry never creates a second signed pin across ordinary failures or signals after commit because the commit transaction advances a private pending ref; an interrupted pending candidate validates stable current master before either private-ref promotion or deletion, retaining both refs for an untrusted/incompatible remote. A divergent sibling receipt advances only when current Forgejo master has a verifiable allowed signature and preserves its exact target, otherwise it creates no commit, receipt update, or push. The sole recovery exception is a same-target candidate whose captured parent target is still on verified current master after its original push was rejected: one CAS replacement pin may be created from that master. Every push follows promotion to the durable receipt ref and carries the token header only in a trace-sanitized environment; every started detached worktree gets a cleanup attempt across update, signature, post-commit recovery, push, and cleanup faults, with planted invalid-pending, two-signed-commits/one-receipt, push-before-ref, divergent-untrusted pin, non-CAS receipt overwrite, and missing-cleanup violations qualifying the checker |
| `tests/test_deploy_cycle_verifier_generated.py` | `scripts/verify_cratedigger_cycle.sh` capture plus `verify-exact` through deterministic process-level systemd state, invocation-journal, and SSH-agent seams | capture preserves ordered starts after an exact journal cursor so a short failed target cannot disappear between state polls; only positive wrong-source evidence may be skipped, while a terminal manager-only start fails closed instead of yielding to a later green invocation; exact-target acceptance requires the expected deployed source token, application cycle completion, systemd successful deactivation, and a successful finished start job for the same invocation; every verifier SSH call disables the shared agent so a forced trigger key cannot consume verification; explicit failure evidence always rejects, rollover never substitutes the next invocation's evidence, and application-only success, next-invocation proof, and agent-eligible SSH calls qualify the checkers as known-bad |
| `tests/test_deploy_hold_generated.py` | `scripts/cratedigger_deploy_hold.py` through a state-respecting systemd/receipt backend whose clock advances by the real requested `sleep()` duration -- no production timeout constant is ever patched, so every property runs the exact bound production uses | strict deploy acquisition first verifies the independently deployed main/YouTube controlled-start contract, survives interrupted atomic receipt publication, owns authoritative `system.control` masks for exactly the three trigger timers, drains only the timer-driven producers (main/unfindable/watchdog -- never YouTube ingest, an always-on `Type=simple` daemon nothing before the hold ever stops) and waits (bounded, separately timed) for the still-running importer/preview to empty the automation queue before ever taking the metadata-gate hold (#1078), and rejects every generated nonzero old-lifecycle preflight shape under the hold — a drainable field alone waits with the hold never taken; an anomaly field, alone or combined with a drainable one (it takes precedence, since `recovery_required_jobs` is itself counted inside `active_automation_jobs`'s own SQL), fails immediately with the hold already established; controlled release creates receipt-owned main/YouTube inhibitors before releasing metadata, proves web/preview/importer readiness and overlapping resume exclusion, starts only main, retains YouTube until final release, and can recover every incomplete phase while removing only exact receipt-owned links/inhibitors. A second property drives real `acquire_hold`/`abort_hold` over the drainable-vs-anomaly field space, the queue-drain timeout, and the controlled-start contract: every failure is fully reversed (no owned link, hold, or inhibitor; no receipt) and `abort_hold` never touches an object it did not own -- refusing closed, before any mutation, the instant one is found -- across every known receipt phase. Planted low-precedence/service masks, surviving queued starts, unowned/tampered inhibitors, dirty preflight counts, retained authority, a reverted pre-#1078 acquire order, a YouTube-drained-pre-hold mutant, and an ownership-blind abort mutant qualify the checkers. A third property (`TestRecoverHeldWaitsOutActiveTimerDrivenProducers`, #1100 item 2) proves `recover_held`'s non-acquiring else branch -- which, unlike `acquire_hold`'s producer-drain-before-hold order, takes the gate hold with no producer drain first -- still waits out a genuinely active `cratedigger-unfindable.service`/watchdog through the post-hold `SERVICE_UNITS` drain rather than the gate, which never guards either; the `assert_recovery_waited_out_the_producer` checker (self-tested known-bad) requires the measured minimum poll count, and a `GATE_GUARDED_UNITS` wrongly widened to cover the unfindable service and a deleted post-hold drain each independently qualify it. Issue #1096's correction round found the fake itself was the gap: `FakeDeployHoldBackend.start_unit` unconditionally went active regardless of a present inhibitor file or an active gate-guarded hold, so the property space could never reach the two worlds (a `Type=oneshot` main service marked among the units a wait-for-active proves, and a still-inhibited unit starting under a hold the same adoption/abort call is mid-releasing) where the real systemd condition-skip semantics (`start` returns 0, unit stays down) turn a bounded wait into a permanent post-reboot dead end; `start_unit` now models that skip, and the two structural fixes it exposed (`_adopt_persistent_markers_or_refuse` and `abort_hold` both remove every marked/owned inhibitor file and release the hold before the single restart-and-prove pass, excluding `MAIN_SERVICE` from that wait and starting it unproven afterward, mirroring `prepare_controlled`) are what the fixed fake's RED-then-GREEN cycle qualifies. `TestNoInterruptionPointIsUnrecoverable` is `finite_generated_domain`-certified over 16 worlds from a named-string interruption-point set that now includes two mid-`prepare_controlled` points landing between the owned YouTube inhibitor and its unmark, so the domain can actually produce a rebooted world with both a manual marker and an inhibitor marker present together (the M1/M2 reproduction shape); a new finite (`cardinality=4`) property, `TestForeignObjectRefusesReceiptlessAdoption`, drives a `ForeignObjectWorld` NamedTuple domain and asserts receiptless adoption refuses a foreign (unmarked) manual hold before mutating any marker or inhibitor file. Reverting the ordering fix in either function reproduces the pre-correction hang against the fixed fake; reverting the S1 foreign-hold-name filter lets the foreign-object property remove an owned inhibitor file and attempt a unit start before its own refusal fires (measured: the persistent marker itself survives untouched, since it is only ever cleared at the very end of a successful run) |
| `tests/test_processing_owner_commands_generated.py`, `tests/test_import_execution_generated.py`, `tests/test_processing_cancellation_generated.py` | exact-owner DB commands, the shared execution-liveness decider, and real staged-tree/subprocess cancellation seams | every preview/import claim, evidence bind, heartbeat, path/launch mutation, and recovery action needs the request's exact owner, exact stage, and exact lease; changed boot or complete same-boot absence can prove death, while missing/contradictory/probe-error observations stay unknown; once pinned-session cancellation is observed, no later file move/remove or child process mutation starts. Owner-blind, heartbeat-only, PID-only, reconnecting, and preflight-only cancellation mutants qualify the independent checkers |
| `tests/test_cleanup_journal_generated.py`, `tests/test_processing_cleanup_generated.py` | real PostgreSQL journal owner/revision commands plus the real dirfd/no-follow cleanup executor over generated filesystem manifests | wrong owner or revision is zero-write; exact remove/quarantine/no-op uses one persisted source/destination manifest and collision-selected target, checkpoints every idempotent mutation, safely creates only the immediate journaled quarantine parent, resumes every boundary, and completes only with a typed receipt. Owner-blind, revision-blind, path-inference, allowlisted-debris, replace-existing, unsafe-parent, and receipt-free mutants are rejected |
| `tests/test_processing_visibility_generated.py`, `tests/test_operator_invalidation_generated.py` | production owner projection/status sets and canonical direct-delete/force/generic transition services | `processing_owner` exists iff the request is processing and must match the recorded pointer; acquisition/backlog include processing while transfer-ledger ownership does not; every processing owner rejects direct delete, force enqueue, and generic mutation with the same exact-owner conflict and unchanged request/files/audit. Latest-job projection, transfer-status broadening, and owner-blind deletion mutants qualify the checkers |
| `tests/test_destructive_authority_generated.py` | `ban_source`, Replace, and `delete_release_from_library` authority worlds plus pinned-Beets delete-manifest worlds varying source identity conflicts, track/art/sidecar presence, unknown bytes, no-op removers, partial I/O/enumeration faults, strict presence-probe faults at every progress phase, lost subprocess/protocol acknowledgements, and arbitrary child stdout prefixes/suffixes | every authority rejection is zero mutation; Replace rejects conflicting nonempty MB/Discogs source identities before lookup, locking, or Beets access; confirmed success leaves every owned target and Beets/PG authority absent while preserving unknown bytes; cleanup, enumeration, and presence-probe failures retain Beets/PG authority and never notify; ban-source reports completion iff the exact release is absent; PG partial is explicitly album-gone/pipeline-present; every ambiguous child acknowledgement remains incomplete regardless of metadata state, preserves PG, skips notification, and retains operator recovery context; a child response is valid only when stdout is exactly one canonical typed frame. Every checker clause owns one named known-bad world that satisfies each earlier clause and asserts that clause's own message, and the clause set is qualified by 20 production mutants driven through the real path, covering 27 of the 28 clauses: Replace fail-open (outcome, typed reason, mutated state, boundary reached), a delete aimed at a stale album id, missing or ambiguous cardinality reaching a mutation, ambiguous success without a mutation, false ban completion, searchability drift, stdout contamination, unknown-content sweep, survived-owned-path success, metadata removed before files, acknowledgement promotion, purge or notification on an incomplete delete, lost recovery context, and an injected pipeline-purge fault that makes the PG-partial arm reachable — pinned as an `@example`, since the suite tier's derandomized budget never draws that corner and the arm was otherwise measured only under fuzz entropy. The one deliberately unreachable clause is the unknown-outcome fall-through, which is fail-closed legislation against a future caller |
| `tests/test_beets_destructive_configs_generated.py` | real pinned Beets through the production `ban_source → current exact-release resolver → pinned exact-delete child` chain and the child directly; independently generated minimal/production plugin base, placeholder/readable/unreadable/invalid-UTF-8 secret mode, `importsource`, playlist, missing-plugin and include-level plugin override, MusicBrainz/Discogs identity, and 1/2/12-track axes | every valid configuration removes exactly the target metadata and owned files; invalid encoding, unreadable includes, and effectively configured missing plugins fail before mutation; separately owned import sources plus sibling album/item/file bytes survive; exact-delete stdout is one canonical typed frame even under Python/raw-fd diagnostics. The deterministic matrix executes 54 core cells; the generated property executes 18 real subprocess compositions in the suite and 96 fresh compositions in the randomized fuzz tier, with known-bad stdout contamination and false-completion self-tests |
| `tests/test_library_delete_notifiers_generated.py` | `notify_library_delete` targeting and report laws across generated Plex filesystem ancestry and a `finite_generated_domain`-certified 10-world Jellyfin report domain (found/absent/lookup-failure × real-adapter `urllib.error` exception kinds × Plex-configured) | Plex submissions always target the nearest existing ancestor inside the configured Beets root, never the deleted or an out-of-root path; the Jellyfin leg is detect-and-report for EVERY caller (#1221 item 1) and its outcome is lane-independent of `allow_escalation`: a found former-path item is a `warning` naming the item as NOT refreshed, a clean not-found is `skipped`, a lookup failure is a `warning` naming the failure without claiming a not-found it cannot know, `submitted` is itself a violation, and failures never escape the completed delete boundary. Per-clause known-bad worlds assert each clause's own message |
| `tests/test_jellyfin_refresh_generated.py` | the real `trigger_jellyfin_scan` entry point with only `urllib` replaced at the network leaf | every generated imported album maps to one exact Jellyfin-visible path in a `POST /Library/Media/Updated` body, with POST/token/JSON/timeout intact and no collection refresh or broad fallback; transport/HTTP/runtime failures stay inside the best-effort notifier boundary |
| `tests/test_world_invariants_generated.py` | the real world-invariant checkers plus `build_world_audit_report` over generated finding multisets | every emitted code has exactly one A/B/C owner, order and duplicates are preserved deterministically, unknown future codes fail closed to Bucket A, and only Bucket A drives integrity-failed status; known-bad omitted-code and flat-all-findings alarm shapes are rejected |
| `tests/test_world_audit_service_generated.py` | the real public owned and borrowed world-audit factory seams over generated resolver failures after an exact non-replaced request reaches the batch resolver | SQLite authority availability failures across both `DatabaseError` and `OperationalError` subclasses, including extended result codes, become one incomplete `observations_only` Bucket B `current_beets_authority_unavailable` report classified by primary code, whose derived `world_audit_outcome` is `beets_unavailable` with a decided CLI exit 5 / HTTP 503 (issue #1355 item 4); SQLite schema errors and other unexpected exceptions propagate unchanged; owned handles close exactly once and borrowed handles never close. Planted complete-outage, OperationalError-only classifier, and wrong-exit/status-map mutants qualify the reusable checker |
| `tests/test_current_library_display_generated.py` | the production exact-identity Library artist and detail merges over generated current-Beets, acquisition-history, evidence, request-status, dual-tag attachment, request-cardinality, malformed-authority worlds, and direct request-detail routes over generated Beets read failures | presence, Captured history, current quality/proof, and tracking remain independent; zero matching requests keeps the primary observed identity untracked, one exact match owns the artist/detail action key, source, and detail history, while cross-source, duplicate modern Discogs, and modern-plus-legacy Discogs candidates fail closed as ambiguous instead of hiding a request; malformed, conflicting, and identityless pipeline-only rows remain visible but have no actionable release ID/source and never attach or suppress; sibling retags and deletions produce Captured plus Missing and held plus Untracked without identity inference; expected Beets availability failures return 503 and unexpected resolver defects return 500 without manufacturing current-library state or mutating the request; planted presence-to-capture, first-observation action-key, detached-source, detached-detail-history, first-wins overwrite, permissive-identity, and fabricated-unavailable-row mutants are rejected |
| `tests/test_long_tail_service_generated.py` | the public long-tail service over generated strict MusicBrainz, modern Discogs-only, and legacy duplicated-Discogs request cohorts plus the shared banding decision over generated `CurrentBeetsMissing`, `CurrentBeetsUnique`, every `CurrentBeetsAmbiguous` reason, mixed-format item orders, and production-shaped authority failures | every valid exact request identity is canonicalized into the single Beets batch; Missing alone emits `missing`, Unique ranks its exact item snapshot through canonical mixed-format precedence independent of item order, every ambiguity aborts without a payload, and FileNotFoundError or SQLite OperationalError escapes without fabricating Missing rows. MB-only selection, first-item format, and ambiguity-as-Missing mutants qualify the reusable checkers |
| `tests/test_long_tail_cache_lifecycle_generated.py` | the real DOM-free long-tail JavaScript load-failure transition, row renderer, and YouTube action over generated cached rows, selected bands, queries, console maps, request tokens, active Pipeline views, and exact MB/Discogs identity sources | only the current request failure invalidates cached rows and band selection, advances the console generation, and clears every console/action state while preserving the operator query; pre-failure work cannot recreate state or overwrite a new-generation result, stale failures remain no-ops, error paint is confined to the active Long Tail view, and either exact identity source retains its chip and drives the same resolver request body. Planted retained-cache, inactive-view-paint, stale-settle, MB-only-chip, and MB-only-action mutants qualify the reusable checkers |
| `tests/test_render_differential_generated.py` | `scripts/render_differential.py` — `summarize_render_diff`, the watched-field derivation over generated `msgspec.defstruct` output types, and the real Recents render path (`_classify_log_entry` → `_project_current_library_have` → `_project_linked_import_evidence`) over generated `download_log` rows | the report is an exact census of two rendered corpora: every rendered field appears exactly once with zeros included, `changed_rows` and each per-field count equal independently recomputed differences, counts stay bounded by changed rows and changed rows by total rows, every sample is a real difference within budget, a corpus against itself reports nothing, and row order never matters; a row on one side only, a repeated id, or unsanctioned field-set drift fails closed, and an allowed drift names the unshared fields instead of dropping them. The derivation fails CLOSED — a field is unwatched only when its declared type is provably numeric/boolean/null, so strings, nested Structs, `object` and string-keyed mappings stay watched — and its converse holds on the real render path: no unwatched field ever holds text at runtime. A summarizer that silently drops the field that changed the most, plus undercount, understated-total, inflated-count, fabricated-sample, oversampled, text-left-unwatched, numeric-dragged-in, lost-field, and missing-rendered-field inputs, qualify the checkers |
| `tests/test_evidence_transition_matrix_generated.py`, `tests/test_decision_corpus_export.py` | the public measurement/import-result candidate producers, real persistence helper, canonical PostgreSQL upsert/FKs, cache/action loaders, unified evidence decider, and the v3 live-corpus ownership/evidence census plus disposable-PG transition replay | generated worlds cross candidate/current/dual canonical roles, null/old/current/future analyzer generations, source presence, lineage and subject/provenance shapes, same-address collisions, every structural early reject, and fresh measured/failed/absent attempts. The exact attempt receipt must survive persistence and FK reload, candidate-only replace must clear stale tuples atomically, exact irreplaceable converted-source current evidence must remain protected, and every ordinary current/dual source measurement must refresh to the new generation while candidate policy sees the receipt projection. Cache reuse stays generation-strict and action admission reaches only the unified decider. Known-bad unconditional-generation, stale-merge, and over-broad-current-preservation checkers prove the harness. The real-PG outer tests census all three evidence FKs in O(rows + links), retain unlinked/conflicted rows as audit records, pin canonical role/order output and exact sparse manifest mtimes, verify the v3 artifact pair, and prove transition replay cannot write its source database because it accepts no source DSN; a historical shape rejected by today's public producer is named `producer_refused` rather than injected below construction, while a preservation/refresh mismatch makes the transition report non-green. |

Every reusable invariant checker also carries **known-bad self-tests** proving
it trips on a planted violating decision — the RED/GREEN guarantee that the
harness detects what it claims to. One self-test per checker is not enough:
each **clause** owes its own world, asserting that clause's own message, per
§ "Per-clause proof". Modules such as
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
Hypothesis entropy, a persistent replay database, and a default logical budget
of 25 worlds × 100 steps:

```bash
nix-shell --run "scripts/world_model_burst.sh"
nix-shell --run "scripts/world_model_burst.sh --examples 10 --steps 50"
nix-shell --run "scripts/world_model_burst.sh --jobs 30 --seed 4242"
nix-shell --run "scripts/world_model_burst.sh --print-config"
```

The wrapper delegates to `scripts/run_world_model_burst.py`. The coordinator
discovers exact unittest IDs in a fresh interpreter, fails closed unless the
production census is exactly 19 deterministic pins plus five generated targets,
batches those pins exactly once, and partitions each generated property's
configured example budget across independent entropy shards. At the default
30-job/25-example profile, the dominant state machine gets 25 one-example
shards so its full 100-step worlds can occupy 25 cores. The four shorter
generated properties use six shards each. Admission is round-robin, so a short
property cannot hide the state-machine tail behind a block of earlier work.
Per-property shard budgets always sum back to 25; parallelism never multiplies or reduces logical depth.

The in-process engine defaults to the smaller of host CPUs and 30 jobs. One
coordinator-owned ephemeral PostgreSQL cluster is migrated once, then every
active target gets a distinct cloned database. Each target also runs in a fresh
interpreter with private Beets and Hypothesis paths. Ambient `TEST_DB_DSN` is
replaced with an owned clone DSN and children cannot stop the shared cluster.
The mirror-harness engine remains separately capped at two jobs. A shared
tmpfs-headroom precondition (`headroom_floor_bytes`/`check_suite_headroom`,
the same primitives `scripts/run_test_suite.py::run_suite` and the fuzz burst
use) runs once before any work and again once per admission CYCLE inside the
admission loop (a cycle can admit up to `jobs` targets at once, so this is
not a per-target check); either trip aborts the run under the `test RAM root
exhausted` identity rather than surfacing as an opaque coordinator crash
(issue #1156 item 3). Like the fuzz burst, it uses the flat, override-
respecting default floor (`CRATEDIGGER_TEST_RAM_MIN_BYTES`), not one scaled
by its own job count: no MEASURED per-worker tmpfs footprint exists for this
coordinator's own worker pool. Same daily-gate redundancy caveat as the fuzz
burst below: `scripts/daily_flake_update.sh` only sets
`CRATEDIGGER_SUITE_OWNS_HEADROOM=1` for its deterministic-suite stage, so
this coordinator's `world_model`/`mirror_harness` stages still hit
`scripts/test_tmpfs.sh`'s own shell-entry guard on the same root moments
earlier — the mid-run half, not the preflight half, is this change's
genuinely new protection.

Every run prints a random 64-bit root seed before storage starts; `--seed`
recreates that schedule. Target seeds derive only from the root seed, logical
test ID, shard index, and shard count—not PID, worker order, or timing. The
ignored output directory contains per-target logs plus an atomically written
`replay.json` with the root seed, jobs, budgets, target seeds, outcomes,
durations, not-started targets, admission state, and any coordinator-level
error. Active workers write private Hypothesis databases without using the
public `@seed` decorator, which would disable database replay. For each logical
property, only shard zero receives that property's primary, secondary, and
Pareto corpus keys; the other entropy shards start empty. A green run replaces
only those owned key directories with the private union. All old key backups
are retained until every swap succeeds, and any caught swap failure rolls back
every changed key, so one shard cannot resurrect another property's stale
examples or leave a partial process-level commit. A property failure preserves
and merges its failing corpus. Coordinator infrastructure failures retain a
terminal replay receipt and diagnostics; failures before a successful corpus
commit do not mutate the canonical corpus. An ownership marker rejects
destructive use of a non-empty unrelated `--database` path. After a failure,
new admission stops while active children drain cleanly.

The temporary PostgreSQL, Beets SQLite databases, library trees, generated
audio, and other scratch paths remain disposable and never read or mutate
production. Normal EXIT handling cleans the current owned root. A hard process
or host interruption during corpus replacement can leave `.staging` or `.backup`
directories beside the canonical database because no sequence of filesystem
renames can make a multi-directory swap crash-atomic. These and any active root
require explicit operator inspection and cleanup; the test infrastructure has no
automatic authority over unrelated roots.

The original six-test implementation completed 25 × 100 in 451 seconds on doc1
on 2026-07-19. After the module expanded to 24 logical tests, production serial
runs on 2026-08-04 and 2026-08-05 took 1,154 and 1,486 seconds. Those are the
current operational baseline. A reduced-depth real qualification of the new
coordinator ran 31 isolated targets (`6 × 1`, 30 jobs) cleanly in 8.7 seconds
after deterministic seed and replay-database verification. The optimized
canonical profile then ran all 50 targets at the exact 25 × 100 budget in 22.8
seconds coordinator wall time and 28.265 seconds end-to-end, averaging 1,375%
CPU. All 19 pinned regressions ran once; each of the five generated logical
targets summed to exactly 25 examples, every shard retained 100 steps, and all
50 outcomes passed. Against the 1,154–1,486 second serial production timings,
that is a 40.8×–52.6× end-to-end speedup. The canonical profile remains outside
`scripts/run_tests.sh` and runs in the daily issue-#498 gate.

The default hammer uses the in-process production adapter that performs real
Beets model/database/filesystem mutations beneath the real dispatch services.
An opt-in profile crosses the real `import_one.py` →
`run_beets_harness.sh` subprocess boundary and performs exact-ID lookup against
an explicitly supplied MusicBrainz mirror origin:

```bash
nix-shell --run "scripts/world_model_burst.sh \
  --engine mirror-harness \
  --mirror-url http://musicbrainz-mirror.internal:5200 \
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

**A related hazard has no audit, by design — this is a review concern, not a
checked one.** Both gates above assume the property is DISCOVERED at all.
Every runner in this repo (targeted selection, the full suite, the fuzz
burst) collects tests via `unittest.defaultTestLoader`, which finds only
`TestCase` methods — a module-level `@given`-decorated function is silently
never executed, with no error and no skip notice. The operator declined
building an audit for this shape specifically (unlike the profile-loading
hazard just above, which the two gates enforce mechanically), so treat a
bare module-level `@given` function as a review flag — and more generally,
any bare module-level `test_*` function outside a `TestCase`, `@given` or
not, since `unittest.defaultTestLoader` is blind to the shape itself, not
specifically to Hypothesis. The instructive history:
`tests/test_beets_candidate_coverage.py` shipped the broken shape at its
creation (`b1d5f7b5`, 2026-08-18) with TWO invisible module-level
functions — `test_generated_candidate_coverage_oracle` (`@given`) and
`test_generated_oracle_kills_count_only_mutant` (a plain known-bad
self-test, no Hypothesis at all) — and kept both through two later commits
that touched the same file without noticing (`2f887859`, `c33c6557`, same
day). Three days after that, `12e38e3c` (2026-08-21) added TWO MORE bare
module-level `@given` functions to the same file, growing the invisible
set to FOUR, and in that SAME commit also added a brand-new
`tests/test_composite_audio_gap.py` that used the correct wrapper pattern
from the start — wrapping its module-level properties in `test_*` methods
that call them. All four were finally wrapped the same way in the
`#1237` review-correction commit (`75e3a3b7`). The most damning of the
four: the known-bad self-test is exactly the artifact
`.claude/rules/code-quality.md`'s "Every invariant checker owes a
known-bad self-test" rule makes mandatory — the ONE test meant to prove a
checker clause actually trips never ran at all, invisibly, the whole time.
Nothing is known to be unreachable today.

**Gated correctly is not the same as fuzz-patrolled — name the two tiers
explicitly.** `scripts/run_fuzz_tests.py`'s own module discovery
(`_default_modules`) globs only `tests/test_*_generated.py`, narrower than
the profile audit's whole-`tests/`-tree sweep above and non-recursive, so a
module in a subdirectory never matches regardless of its name. A module can
pass both gates above — profile imported correctly, deadline disabled,
every property genuinely discoverable by `unittest` — and still never be
selected for a randomized burst, because its filename doesn't match the
pattern. As of this writing, 15 files outside the glob carry 39 `@given`
decorators (counted directly from each file's AST, not by grepping text —
the count includes every `@given` regardless of whether the decorated
function is a production-facing property or a deliberate Hypothesis-
integration self-test under the "Never property-test the test machinery"
rule in `.claude/rules/code-quality.md`, because BOTH shapes are equally
invisible to this specific glob and both are equally stuck at suite depth
by it):
`tests/test_convergence_service.py`, `tests/test_import_queue.py`,
`tests/web/test_server_endpoints.py`, and `tests/test_composite_audio_gap.py`
are four of them, running only at the deterministic suite's bounded budget
(150 examples, `derandomize=True`) and never at `scripts/fuzz_burst.sh`'s
daily depth. One exception worth naming: `tests/world_model/state_machine.py`
is one of the 15 — the four `@given` decorators contributing to its share of
the 39 are on ordinary `TestCase` methods
(`test_force_import_refreshes_relocated_candidate_authority:1208`,
`test_candidate_measurement_is_once_per_snapshot:1273`,
`test_live_drift_worlds_relink_without_retry_bypass:1346`,
`test_fresh_audit_overwrites_installed_spectral_landmine:1430`), unrelated to
the file's separate `LifecycleWorldMachine(RuleBasedStateMachine)` at `:942`,
which carries no `@given` at all — but `scripts/run_world_model_burst.py`'s
own discovery (`unittest.defaultTestLoader.loadTestsFromName`) targets the
WHOLE module, `tests.world_model.state_machine`, not the state machine
specifically — its own hard-coded `expected_generated = 5` and the fail-
closed count check against it (both in that script) are the evidence: all
five Hypothesis-testable units in the file — the four `@given` methods and
the one stateful machine — are discovered together. So unlike the other 14
files, none of this file's contribution to the 39 is actually starved of
randomized depth, only of THIS particular burst (`scripts/fuzz_burst.sh`).
A property that passes both gates above is wired correctly, not
fuzz-patrolled — the two are independent facts and neither implies the
other.

`scripts/fuzz_burst.sh` discovers the exact unittest IDs and effective
Hypothesis settings in every generated module. Ordinary deterministic pins run
once; modules already audited as deterministic-suite hotspots reuse that
runner's bounded class/method batches, and those hotspots are admitted before
ordinary modules so expensive real-tool work cannot become the tail. Every
randomized, non-finite property may divide its exact discovered budget across
independent entropy shards, including a fixed per-test
`@settings(max_examples=...)` budget. Derandomized and independently proved
finite properties remain one target. The real-Beets destructive matrix is also
one property target: multiplying its nested Beets startup/migration work causes
hard subprocess timeouts, so it is frontloaded instead of sharded.

Automatic fan-out is bounded by both host cores and the effective default
profile budget, with at least 250 examples per entropy child. The total budget
does not grow: on a 30-core host, the ordinary 500-example profile uses two
processes of 250 examples, while the 20,000-example overnight gate keeps eight
processes of 2,500. The child runner loads the owning module, selects the exact
discovered ID, and applies only that target's divided budget, so dynamically
named state-machine tests and explicit budgets can be sharded without repeating
the module's other properties or pins. An exact-budget check rejects any
schedule that omits a test, repeats a pin, changes a property's combined example
count, exceeds a resource shard limit, or invents an ID.

The queue defaults to twice the host's core count, capped at `MAX_FUZZ_JOBS`
(64) regardless of host size (issue #1214 "Contributing gaps" item 1 — NOT
#1156 item 1, a different, unrelated finding), because these targets mix
Python with subprocess and filesystem waits. PostgreSQL-backed targets have a
separate bounded lane (24 targets on doc1's 30-core host); admission fills that
lane before ordinary capacity so PostgreSQL work cannot accumulate into a
low-utilization tail. Any `ENOSPC`, PostgreSQL `DiskFull`, or unexpected loss of
an ephemeral database aborts further admission and reports that the property
verdict is invalid. A shared tmpfs-headroom precondition
(`headroom_floor_bytes`/`check_suite_headroom`, the same primitives
`scripts/run_test_suite.py::run_suite` uses) runs once before any work and
again once per admission CYCLE inside the admission loop (a cycle can admit
up to `worker_count` targets at once, so this is not a per-target check);
either trip aborts the same way, under the `test RAM root exhausted`
identity, before the burst can silently ENOSPC deep into a run (issue #1156
item 3). The MID-RUN half is the genuinely new protection; the PREFLIGHT
half is a near-duplicate in the daily gate specifically, where
`scripts/daily_flake_update.sh` only sets `CRATEDIGGER_SUITE_OWNS_HEADROOM=1`
for its deterministic-suite stage, so `scripts/test_tmpfs.sh`'s own
shell-entry guard already checked the SAME root with the SAME flat 1 GiB
default moments earlier — the preflight's real added value is a SECOND
invocation inside an already-entered shell. Unlike the deterministic suite's
own worker-scaled floor, the fuzz
burst uses the flat, override-respecting default
(`CRATEDIGGER_TEST_RAM_MIN_BYTES`): there is no equivalent measured
per-worker tmpfs footprint for its own, much larger and differently-shaped
worker pool. Tripping the guard stops ADMISSION only -- already-running
targets keep consuming the reserve regardless, so this is a correct label
and exit code on the way out, not prevention, and the abort is one-way with
no hysteresis (a single transient dip abandons every not-yet-started
target). Separately (and NOT part of this headroom precondition): when the
deterministic suite's own Python-phase worker pool runs a generated-test
module deterministically (`run_python_tests.py`'s own `ProcessPoolExecutor`
scheduling, not this randomized nightly burst -- the burst spawns each
target's own subprocess directly via `subprocess.run` and so can never
raise `BrokenProcessPool`), a worker killed by the OOM killer is classified
honestly rather than reported as an ordinary per-target failure:
`_classify_target_infrastructure_failure` recognizes the `BrokenProcessPool`
shape, corroborated by measured `/proc/meminfo` `MemAvailable` below the
env-overridable `CRATEDIGGER_TEST_MEMORY_MIN_BYTES` (mirroring
`CRATEDIGGER_TEST_RAM_MIN_BYTES`'s own override pattern), and folds every
such worker death into one `test host memory exhausted` marker instead of
N separately-indexed disguises (issue #1156 item 2). Set
`CRATEDIGGER_FUZZ_JOBS` to cap all concurrent processes,
`CRATEDIGGER_FUZZ_POSTGRES_JOBS` to cap the PostgreSQL lane, or
`CRATEDIGGER_FUZZ_PROPERTY_SHARDS` to override automatic entropy fan-out. On
doc1's 30-core VM on 2026-07-23, the complete 71-module,
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

The daily gate measures its existing top-level phases from inside the service's
own mount namespace and cgroup. `scripts/daily_resource_monitor.sh` reads the
unit's cgroup-v2 `memory.current`, monotonic `memory.peak`, swap, and
`memory.stat` categories while sampling the private scratch mount with
`statfs`. Each `CRATEDIGGER_DAILY_RESOURCE_PHASE` line retains the complete
time-correlated sample at that phase's largest observed `memory.current`, plus
its independent scratch byte/inode high-water. The terminal
`CRATEDIGGER_DAILY_RESOURCE_RECEIPT` names the first phase that established the
exact kernel memory peak. `shmem_at_memory_current_peak_bytes` and
`non_shmem_at_memory_current_peak_bytes` are from the same sample; never
subtract independently observed maxima to attribute working memory.

The monitor does not enumerate processes, enter the namespace from outside,
reset cgroup state, or change phase commands, worker counts, shard counts,
PostgreSQL admission, example budgets, or tmpfs limits. Missing cgroup files,
an unreachable scratch or state root, and a violated cross-sample invariant
(a scratch/inode limit of zero or one that changes mid-run, usage above the
limit, a cgroup peak below its own current value, an impossible memory
breakdown, or a regressing memory/swap peak) still produce one terminal
`status=invalid` receipt with no phase breakdown at all, rather than zeros. A
receipt that is not clean (`status=invalid`) also prints an explicit
`resource receipt invalid` line to stderr regardless of whether the
candidate gates themselves passed or failed (issue #1214 gap 4) — an
invalid receipt used to be silently absorbed into an already-nonzero exit
code on a failing run, exactly the run where the telemetry matters most.

**The invariant is binary, not quantified: a receipt must never say
`status=valid` if anything failed to record.** The monitor's own
bookkeeping (samples, the phase pointer, its lock) lives under a state
root tried in order — the caller's own `TMPDIR`, then `/tmp` — verified by
filesystem identity to be distinct from the private scratch tmpfs it
measures (`$XDG_RUNTIME_DIR`), never inside it: a full scratch tmpfs on
2026-08-20 took the monitor's own state down with it and erased the one
night's telemetry that would have diagnosed the overflow (issue #1214).
That is the whole fix. Issue #1214's rounds 2-4 (reviews F1-F9, C1-C9,
C-F1-C-F5) layered an increasingly elaborate loss-accounting system on top
of it — writer identity, per-sample sequence numbers, a fifo report
channel from the periodic loop, parent/loop drop counters, a
`dropped_samples`/`missing_phases`/`corrupted_history_lines` receipt
triple, a `degraded` status, a phase-history file — and each round's fix
for that layer re-introduced the same class of bug one call site over
(review C-F1 found the fourth such site: four bare marker-file writes
inside `daily_resource_monitor_set_phase` with no fallback of their own).
That was a design smell, not a testing gap, and the whole layer was
removed in a round-6 strip-back: once the state root is verified off the
measured filesystem, a write failure there is a rare, genuinely
exceptional event, not routine disk pressure, so it is handled the plain
way this monitor used before issue #1214's accounting rounds — a writer
that fails just says so, and the run is `invalid`. No counting.

`daily_resource_monitor_set_phase`'s own structural failures (an invalid
phase name, a stuck lock, a failed phase-pointer write, a failed unlock)
are reported through a plain in-process bash variable
(`_CRATEDIGGER_RESOURCE_FAILURE_REASON`), never a marker file: `set_phase`
and `daily_resource_monitor_finish` always run in the ONE parent process
(nothing here is forked except the periodic loop itself), so a bash
variable assignment — which cannot itself fail the way a filesystem write
can — is sufficient. The periodic loop is a genuinely separate, forked
process with no report channel of any kind: on its first failed sample
write it simply stops (matching this file's own pre-accounting shape),
and `daily_resource_monitor_finish`'s ordinary `wait` on its PID observes
that exit status directly — no inter-process signaling required. Either
path forces the terminal receipt to `status=invalid`, but — this is the
part rounds 2-4 existed to protect and the strip-back keeps — the
per-phase breakdown for whatever DID survive is still printed above it,
because losing that breakdown for the one run that needed it is the
entire reason this file exists.

A row that lands but is malformed (a real full filesystem does not reject
a write atomically — review F2, measured; a partial page can land and the
next append then concatenates onto its unterminated tail) is skipped and
the summary keeps going, rather than discarding every other row's
evidence for one corrupt line — this is what makes the receipt useful
under real ENOSPC pressure on the state store itself, the difference
between "we have the phase breakdown" and "we have nothing."

It is pure and safe: no prod DB, no slskd, no beets, no network. Green runs
write disposable state only to the measured scratch tmpfs; the monitor's
own bookkeeping deliberately does not (that is the whole fix). Repeat runs
add entropy; there is nothing to resume and no seed cursor — coverage grows
by improving strategies and invariants, not by consuming more seeds.

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
parity tests use (`tests/evidence_helpers.py::build_parity_candidate_evidence` /
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
only. This section is the AIMED procedure — hand-picked mutants at named
subjects. The separate catalog breadth pass (mutmut, implementer-side,
run during convergence via the committed `nix/mutmut-shell.nix`) is
`docs/mutation-testing.md`; its generic operators discharge none of the
aimed obligations below. How to pick aimed mutants:

- revert a real past bug fix (the strongest single check);
- break each adapter derivation the parity property claims to pin;
- flip decision comparisons; remove early-exit guards and readiness gates;
- for each property, plant the exact violation it claims to catch;
- when the diff adds several sites, mutate and name each one — a killed
  mutant at one site does not qualify any other
  (`.claude/rules/code-quality.md` § "Testing — Red/Green TDD").

Interpret results per mutant: **killed** = the property works; **killed only at
fuzz entropy** = the deterministic suite budget misses the decisive world, so
pin it as an `@example`; **survived both tiers** = either a missing invariant
(add it, with a known-bad self-test) or a world the strategies rarely make
decisive (again, pin the decisive world). The AIMED mutation driver is an
operator/agent one-shot — never committed (`.claude/rules/scope.md`; the
committed mutmut shell serves only the catalog pass); record
what you tried and what happened in the PR's Fault injection section
(`.github/pull_request_template.md`) — name the mutant and the test, not just
"planted a mutant, confirmed RED" — alongside the mutmut breadth-pass tally
and survivor dismissals when the diff has mutable Python production surface
(`docs/mutation-testing.md`). That section used to be a mandatory
per-diff-site table; it is a short account now — one sentence, or a short
list for per-clause proof against a many-clause checker
(`.claude/rules/code-quality.md` § "Testing — Red/Green TDD" has the
reasoning), while the regression-pin rule, the adapter mutant rule, and the
Standing scope per-clause obligation in that same section all stay
unconditional.

Canonical run (issue #548, 2026-07-08): 13 mutants — including reverting
fix `6cf26a4`, which the generated lifecycle property killed independently
of its hand-written regression tests — 10 killed outright, 1 only under
deeper randomized entropy, 2 survivors fixed in PR #555
(`assert_below_gate_never_stops_search` and the
`_SPECTRAL_OVERRIDE_DECISIVE_WORLD` parity pin).

## Per-clause proof — name the world that makes each clause fire

Issue #1094. Fault injection above qualifies a *suite*; this qualifies a
*checker*, one clause at a time. The two are the same instrument at
different resolutions, and the finer one exists because four defects in the
#1063 series hid behind green properties whose strategies could not reach
the violating world. In each case the invariant was written correctly. The
property was green because the world set could not produce a
counterexample, so the guard was unfalsifiable rather than satisfied.

**The practice:** for each clause of each invariant, name the world that
makes that clause fire, and plant a mutant that only that world kills. A
clause with no such world is either fail-closed legislation or decoration,
and you must say which.

Three questions per clause, in order. Q1 and Q3 are cheap and
deterministic; Q2 is the one that catches the #1063 shape. A clause can pass
Q1 and fail Q2, but there is no point asking Q2 about a clause that would
not fire if the world arrived. Q1 and Q2 both push in the firing direction;
Q3 asks the opposite question and is the only one of the three that can
catch a clause accusing correct production code.

**Q1 — does the clause trip at all?** Build the minimal world that makes
that clause's condition true *while every earlier clause in the same
function passes*, feed it directly to the checker, and assert that clause's
own message with `assertRaisesRegex`. Use a `subTest` mutant table when a
checker has many clauses.

Bare `assertRaises(AssertionError)` is not proof. `raise`-style checkers
short-circuit, so a self-test whose world violates several clauses only
ever exercises the first — and the test name goes on advertising the rest.
The canonical instance, found in minutes when this audit started:
`test_publication_checker_rejects_overwrite_source_loss` in
`tests/test_path_authority_generated.py` was named for the overwrite and
source-loss clauses, and its world really did set mismatched destination
names and plant an unpublished temp artifact. It also passed a wrong result
type, so the checker raised on the result-type clause and never evaluated
either clause the test was named for. Three clauses of that checker had no
proof at all.

**Q2 — can the strategy reach that world?** Plant a mutant in production
that makes the condition true through the real code path, run the module's
generated properties, and record KILLED or SURVIVED. Revert every mutant
before you finish; a left-behind mutant is the worst outcome this procedure
can produce. Target one module at a time —
`nix-shell --run "python3 scripts/run_fuzz_tests.py <module> --jobs N"`.

**Q3 — does the clause stay quiet where production is right?** A clause
reads some of the world's dimensions and ignores the rest, and every ignored
dimension is somewhere production may legitimately answer differently from
what the clause demands. So name a world where the clause's condition looks
true and production is nonetheless correct, feed it straight to the checker,
and assert it returns no violation. Deterministic, like Q1; there is no
mutant to plant, because the defect is in the checker, not in production.

This is the #1332 shape (issue #1313's residual sweep), and it is the one
that reaches the nightly gate rather than the PR. Two clauses in
`tests/test_beets_tag_sync_generated.py` held Beets to its own answer about
a release — an ambiguous resolution must refuse `not_unique`, a missing one
`not_found` — by reading `world.resolution`. Neither read
`run.authority_raises`, the counter the same commit added to record whether
the injected `world.authority_failure` actually fired, so when the authority
failed at the factory or at the resolution, production correctly returned
`beets_unavailable` while the clause went on demanding `not_found`.
Four producible cells, every one of them accusing correct code. Q1 passed
for both clauses: their self-tests hand-built `authority_raises=0`, so they
could not have seen it. Read that file for the shape and you will find the
corrected version — `10fc9f74` fixed both clauses the day after they landed.

Two things make Q3 worth its minute. The tell is usually already in the
file — `_write_authorized`, written for the same world in the same sitting,
read `run.authority_raises` for exactly this reason, and the asymmetry
between the two functions was visible without running anything. And the failure mode
is not a green property but a property that reds on a draw nobody chose: 37
violating draws in 500 at fuzz depth, while the suite tier stayed green
because the `@example` pins happened to shift the derandomized sweep off
them. Adding a pin, removing one, or bumping `max_examples` would have
flipped it red at a moment nobody was looking, on someone else's PR.

**Record which tier killed Q2's mutant.** A mutant that dies only under `fuzz` is not
killed for gating purposes: `suite` is `derandomize=True`, so a world the
deterministic budget misses is missed on every machine, forever. Pin that
world as an `@example` and re-measure. The #1094 first pass shipped this
defect and caught it in review: a widened `pg_partial` arm ran 9 times in
500 fuzz examples and **0** times in the suite tier, leaving a real
production fail-open mutant alive through the full suite and the final gate
until an `@example` pinned it.

**Editing a property body reshuffles its worlds — re-measure afterwards.**
`derandomize=True` seeds from the test function's own digest, so *any* edit
to a property's body changes the entire example sequence it draws. This is
not limited to widening a strategy: adding an assertion, renaming a local,
or reordering two statements can silently drop a decisive world out of the
gating tier. Measured during the #1094 second pass: a `tier4_only` world ran
**4 / 150** before the audit touched that property and **0 / 150** after,
removing a real ladder branch from every gating run — the round-one defect
arriving through a door nobody was watching. So after editing a property,
count how often each decisive arm actually executes under the default
profile, and pin any arm that reaches zero. Instrument by wrapping the
checker at the module attribute in a throwaway script; do not commit it.

**Clause ordering masks across checkers, not just within one.** A property
that calls checker A then checker B hides every clause of B that A also
rejects. In the same pass, `check_reserved_ceiling_tiers_unused` could never
fire: `check_tier_follows_fired_legs` ran first and re-derived the ladder,
so both reserved-tier mutants died on the *earlier checker's* message. The
fix is to order independent checker calls so each clause is attributed to
the checker that legislates it — which is a different operation from
reordering clauses inside one checker, and unlike that one it is legitimate.

**Count clauses by hand, not by one grep.** `grep -c "raise AssertionError"`
is a starting point, not an inventory: clauses are also written as bare
`assert cond, "message"`, and accumulating checkers append to a list instead
of raising. The #1094 second pass found a module whose real clause count was
34 against a grepped 26 — nine `assert x, msg` clauses and one `raise` that
belonged to an in-world stub rather than a checker. Derive the inventory from
the checker functions themselves and say so when your count disagrees with
the brief.

**A survivor is not a licence to delete.** The default remedy is to widen
the strategy until the world is producible. A guard over a shared namespace
legislates for every *other* writer of that namespace, present and future
(§ "Invariants live at the widest boundary" in
`.claude/rules/code-quality.md`), so a clause today's strategy cannot reach
may still be correct. Three dispositions, and the PR must say which applies:

- **widen** — the world is producible and the strategy was too narrow. This
  is the #1063 shape and the expected outcome.
- **fail-closed legislation** — no production caller can reach the world, and
  the clause exists so that a future one fails loudly instead of passing
  silently. Its only legitimate caller is its own Q1 world. Keep it and say
  why. Two shapes recur: a checker refusing an input it has no rule for
  (`assert_quarantine_verdict_is_earned` in
  `tests/test_path_authority_generated.py`, and the unknown-outcome
  fall-through in `assert_delete_postcondition`), and a clause whose world
  the platform forbids before production can build it — the NAME_MAX artifact
  clause there, where the kernel raises `ENAMETOOLONG` at `mkdir` so an
  over-length name can never be listed back. Both are unreachable for a
  reason outside the strategy's control, which is what separates them from a
  survivor you should have widened.
- **decoration** — the world is impossible by construction (type-impossible,
  not merely unreached) *and* the clause guards nothing a future writer could
  reintroduce. Delete it. This is the rarest of the three; prefer the two
  above unless you can name why no future caller could ever want the guard.

**A clause can also be unobservable by construction, independent of
reachability — say so and defer to a pin.** The three dispositions above are
about whether the WORLD is producible. A clause can clear that bar — the
world is real, the mutant is real — and still be unprovable by the property,
because the property's own output cannot tell the mutant from the fix.
`tests/test_path_authority_generated.py`'s `root_relative` world (lines
2237-2258) is the worked example already in-tree: bypassing the
root-absoluteness guard does not change what a fixed candidate like `/etc`
resolves to, because `_relative_to` calls `os.path.relpath` against the
current working directory either way, and the result is `..`-laden and
refused regardless of whether the guard fired — every refusal looks the same
to a property that only ever compares authorized-vs-refused. When you find
this shape, say so in a comment at the clause, the way that world's own
comment does, and defer the proof to a deterministic message-asserting pin
that asserts the exact exception type and message
(`test_relative_root_refuses_without_blaming_the_candidate` in
`tests/test_path_authority.py`) — not a property retrofit that cannot
actually observe the distinction. Keep the world in the generated strategy
anyway so a change that made it spuriously AUTHORIZE something is still
caught; that is a materially weaker, cheaper claim than "this property
patrols this clause," and the comment must not overstate it.

**A mutant killed only by the deferred pin does not clear the property.**
When a clause is deferred this way, reviewers must still plant the
clause-deletion mutant against the PROPERTY itself, not only against the
pin — a pin-only kill proves the pin works and says nothing about whether
the property is load-bearing for anything else at that clause. Record both
results.

**Prefer accumulating checkers in new code.** A checker that returns
`list[str]` violations evaluates every clause, so ordering cannot mask one
and each clause carries a distinct message the self-test can name
(`mode_selection_violations` in `tests/test_web_auth_mode_generated.py`,
`tests/test_cleanup_journal_generated.py`). Short-circuiting `raise` chains
are the structural cause of the masking above. This is a preference for new
checkers, not a licence for a blanket rewrite sweep: convert an existing
chain only where an audit shows ordering actually hides a clause.

**Scope and limits.** The standing rule is that a PR **adding or changing a
checker clause** audits that checker's clauses as part of the change — not
any PR that merely touches a generated module, which would trigger on adding
one `@example` and earn quiet non-compliance. A prioritised sweep of the
modules guarding the most expensive invariants runs separately, scoped module
by module. The audit examines test machinery, so its own
artifacts are deterministic-only — never schedule a generated audit of the
audit. Do not build a scanner that infers clause reachability from source
(`.claude/rules/code-quality.md` § "Semantic source scanners are
prohibited"); the evidence is the named world and the killed mutant,
recorded in the PR's Fault injection section exactly as the fault-injection
account above is — one clause fits in a sentence, several clauses are a
short list, one line per clause.

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

- **Production subjects only.** Never add generated coverage for test
  infrastructure. Use deterministic pins for runners, schedulers, selectors,
  receipts, fixtures, strategies, checkers, and audits.
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
  can prove each one trips. Every *clause* owes one, asserting that clause's
  own message — a property that has never failed anything is unfalsifiable
  until proven otherwise, and a checker with eight `raise` sites and one
  self-test has proven one clause (§ "Per-clause proof").
- Import `tests._hypothesis_profiles` for the side effect **above the first
  `class`/`def`** — that is what wires the module into the suite/fuzz tiers,
  and both `tests/test_hypothesis_profile_audit.py` and the suite runner's
  own `assert_hypothesis_deadlines_disabled` fail without it.
- **A resource built inside a `@given` body is example-scoped, not
  method-scoped — bind its lifetime with a `with` block, never
  `self.addCleanup(...)`/`self.enterContext(...)`.** Hypothesis re-executes
  the body once per EXAMPLE; `addCleanup`/`enterContext` fire once per test
  METHOD. A resource constructed directly in the body and registered there
  leaks one live copy per example until the method finally returns. Issue
  #1214: a fresh re-measurement found 2491 concurrently-live
  `BeetsContractWorld` fixtures, 1.59 GB, from ONE test method at the daily
  gate's real budget (issue #1214 itself first measured 2469 / 1576.99 MB
  from a slightly different run). That was the exposure that filled
  `cratedigger-daily-checks.service`'s tmpfs — `OSError: [Errno 28] No
  space left on device`, an ENOSPC exception, NOT a host OOM kill; issue
  #1214 states explicitly that no host OOM kill occurred.
  `tests/test_given_body_cleanup_audit.py` enforces the direct-construction
  shape of this mistake, but cannot see through a helper method the body
  merely calls by name (a bounded syntactic audit, not a call-graph tracer
  — `.claude/rules/code-quality.md` § "Semantic source scanners are
  prohibited"), so review still owns that shape. This is the OPPOSITE of
  `NodeJsonlWorker`'s pattern above — that worker is deliberately built
  ONCE per method in `setUp()` and REUSED across every example, so
  method-scoped `addCleanup` is exactly right there. The rule follows the
  resource's own intended lifetime, not a blanket ban on `addCleanup` near
  `@given`: a resource meant to be fresh per example needs a `with` block
  inside the body; a resource meant to be shared across the whole method's
  examples is correctly `addCleanup`-scoped in `setUp()`.
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
- Reuse the shared fakes/builders (`tests/fakes/`, `tests/helpers.py`,
  `tests/evidence_helpers.py`, `tests/dispatch_helpers.py`)
  per `.claude/rules/code-quality.md`; leaf-seam mock rules apply to
  generated tests like any other test.

### Ask the OS only for worlds it guarantees

A strategy that names an OS-level world must be able to CREATE that world
from any ancestry, or the property is asserting against a world the run did
not actually produce. Signal delivery is the sharp edge: `SIG_IGN`
dispositions and the blocked-signal mask are both inherited across
`fork`+`exec`, and CPython's `subprocess` clears only the signals CPython
itself ignores at startup (`restore_signals` — SIGPIPE and SIGXFSZ on
Linux, measured as a dying child; SIGHUP and SIGTERM ignored by the parent
still yield rc 0), never the mask and never the signals in play here — so
an ancestor nobody in the test controls decides whether a requested signal
lands:
`nohup` holds SIGHUP, a non-interactive shell starting a background job
holds SIGINT/SIGQUIT, a supervisor may hold SIGTERM. SIGKILL and SIGSTOP
are the only two signals POSIX forbids catching, blocking, or ignoring.

The failure shape is nasty because it accuses the wrong side: the fixture's
`kill -1 $$` silently no-ops, the child exits 0, production truthfully names
the status it observed — the clean-exit "never applied the release" gap —
and the property's branch assertion demands the signal wording instead. Note
which assertion fires. The reason was not lost, so the rc-only-fallback
checker PASSES; what fails is the branch-specific `assertEqual`, and a
debugger who starts at the checker starts in the wrong place. It reproduces
only under whichever launcher happened to hold the signal, which is why it
survived as a full-suite-only red. Ask for the guaranteed world
(`FAKE_HARNESS_FATAL_SIGNAL` in `tests/test_import_one_stages.py`), pin the
guarantee with a deterministic test that drives the shared fixture from a
parent ignoring AND blocking every signal an ancestor plausibly holds, and
cover the remaining numbers through the pure producer, where no ancestry can
intervene. The same question is worth asking of any generated world an
ancestor can veto: inherited umask and resource limits, a `CAP_*` the
sandbox drops, a filesystem that silently lacks the feature under test.
