# Code Review — refactor/711-quality-model-untangle (issue #711 quality-model untangle)

- **Scope:** branch `refactor/711-quality-model-untangle` @ `9730b628` vs base `3fa7ad7a` (origin/main). 92 files, +8269/-2671.
- **Plan (explicit):** `docs/plans/2026-07-16-001-refactor-quality-model-untangle-plan.md` (implementation-ready; R1-R33, U1-U11, KTD1-8, 5-family regression contract).
- **Intent:** verified-lossless proof as sole terminal boundary; gate_min_rank retired; one narrowing rule; loud analysis-failure aborts (have_analysis_error); Vorbis/WMA first-class + UNKNOWN bottom; two-axis evidence vocabulary (migration 055); scalar readers dead. Only the 5 enumerated change families (plus codec-label pins) may flip corpus behavior.
- **Panel:** correctness, testing, maintainability, project-standards, agent-native, learnings, data-migration, api-contract, reliability, adversarial, deployment-verification (all Opus) + decision-fidelity (Fable) + independent cross-model adversarial pass via Codex CLI (requested gpt-5.6-sol, high reasoning, `independence_verified: true`).
- **Validation:** 12 per-finding Opus validators (all interrupted once by a session-limit event, resumed with context intact; no verdicts lost) -> 11 confirmed, 1 rejected. 2 further findings verified directly by orchestrator inspection (mechanical facts).
- **Fixes applied:** NONE — operator instructed "don't fix; present findings" mid-run. All autofix-class labels below describe what a fix pass *would* do.

## Overall

The branch lands all 11 plan units with high fidelity: the terminal proof boundary, analysis-failure aborts, no-floor policy, codec model, two-axis vocabulary, and scalar-seam close are faithfully implemented with unusually disciplined test work (testing lane: every promised family flipped into named negative pins; pin+property+known-bad triads throughout; no mock-audit or skip-audit violations). Migrations 054/055 are mechanically correct and well-guarded. The learnings sweep found no documented scar re-opened and confirmed the #723 blind-HAVE seam closed.

Three confirmed P1s stand between this branch and merge: one policy regression on the post-import failure path (#1), one settled-decision deviation that was adapted around instead of surfaced (#2), and one data-losing migration seeding defect that must be amended before 055 ships (#3).

## Triage Groups

| Group | Findings | Type | Context | Preferred resolution |
|---|---|---|---|---|
| A. Post-import gate edge policy | #1, #8 | decision-gate | The U5 gate rewrite changed what happens off the happy path (evidence unavailable) and on operator paths (force/manual) — neither behavior is covered by a settled decision | Decide #1 first (failure-path contract: reopen WITHOUT denylist is the reviewers' consensus direction; optionally proof-aware short-circuit). Then confirm or reverse #8 and document it |
| B. Narrowing-rule direction | #2, #10, #12 | decision-gate | One decision resolves all three: does "transparent + independently genuine" narrowing key on installed-subject only (D3-independence reading, current code) or fire at import time on the carried genuine grade (AE6 letter, U5's explicit "New pins" directive)? | Decide the AE6 reading; align `filetypes.py` backfill and the gate to the SAME rule (#10); then post the complete flip enumeration addendum (#12) |
| C. Migration seeding | #3 | apply-queue | 055 is unshipped; amendment validated | Amend 055 (converted-row carve-out: subject='source', provenance='carried') + extend the migrator test. Must land before deploy |
| D. Mechanical fixes | #6, #7, #9, #11, #13, #14 | apply-queue | All validator-confirmed with concrete fixes; no decisions needed | Docs x2, preview guard + test, locked-decision renderer + pin, AE2 combined pin, vulture whitelist regen |

Ungrouped: #4 (structural design call), #5 (settled-conflict, report-only).

## Findings

### P1 — High

**#1. Post-import evidence-unavailable path reopens the request and permanently denylists the winning peer**
`lib/dispatch/quality_gate.py:178` (also :59, :243) — reviewers: adversarial + adversarial-codex (independent cross-model agreement, the strongest signal in the set); corroborated by correctness and decision-fidelity residuals. Confidence 100, validator CONFIRMED.
At base, `if not state:` was a genuine no-op (request stayed `imported`, no denylist). Now it routes through `post_import_search_action("requeue_upgrade")` -> status `wanted` + a permanent `source_denylist` INSERT (no expiry; `_get_denied_users` filters that peer from every future plan). Reachable on genuine local failure/race after a successful import (the `expected_current_evidence_id=0` sentinel when the refresh is non-'ready', evidence loads raising, rebuild-reasons non-empty). Proof is unknowable at that point, so a genuine verified-lossless copy can be demoted imported->wanted and its only source denylisted — an R1 violation on the failure path, and against the archivist frame ("the system never auto-decides anything irreversible"). The request may self-heal next cycle via the proof lock; the denylist never does. `test_missing_linked_evidence_reopens_full_tier_search` pins the behavior deliberately (with `verified_lossless=True`), so this was an implementation choice — but no settled decision covers it.
*Response needed:* operator decision. Consensus fix direction: `_evidence_unavailable_plan` reopens WITHOUT denylist entries (denylist only after a quality decision on successfully loaded evidence); optionally short-circuit to terminal accept when the linked row carries proof. Flip the pinning test accordingly.

**#2. AE6 narrowing is unreachable at import time; pins flipped outside the enumerated set**
`lib/quality/decisions.py:715` — reviewer: decision-fidelity (Fable). Confidence 75, validator CONFIRMED.
The narrowing branch requires `spectral_subject == 'installed'`, but every import-time producer of the gate's current evidence writes `'source'` (production propagate `lib/quality_evidence.py:874`, legacy `evidence_from_album_info` :594, simulator twin `lib/quality/pipeline.py:682-683`); `'installed'` is only written by a later-cycle HAVE re-measurement. So "genuine CBR-320 import -> wanted + lossless-only" (AE6, and U5's explicit "New pins: genuine CBR-320 -> wanted + lossless-only + denylisted") can never happen at import time — it converges one cycle later. The protected pins `test_cbr_320_genuine` / `test_cbr_256_genuine` were behaviorally flipped (requeue_lossless -> requeue_upgrade, denylisted False -> True) even though the plan's U5 family-3 list enumerates only the `*_no_spectral` siblings, and the docs were rewritten to describe the deferred behavior — the "adapt around it" move the plan's stop-condition clause forbids. The code faithfully encodes the D3/R4 "independently genuine installed copy" reading; it violates AE6's letter.
*Response needed:* operator decision between two readings: (a) AE6-letter — allow narrowing on the carried source-subject genuine grade at import time (then also align #10 and restore/replace the flipped pins); or (b) ratify the deferred D3-independence behavior — amend the plan/AE6, enumerate the pin flips, and add deferred-convergence pins. Either way add AE6-both-halves pins.

**#3. Migration 055 blanket 'installed' seed drops historical converted-row source grades on first touch**
`migrations/055_evidence_two_axis_vocabulary.sql:54` — reviewer: decision-fidelity (Fable). Confidence 75, validator CONFIRMED (including the amendment).
055 seeds `spectral_subject='installed'` for every row with a grade, but rows for lossless-source->lossy conversions (`was_converted_from IS NOT NULL`) carry a grade measured on the pre-conversion SOURCE bytes (base-code carry logic deliberately preserved them — the 6cf26a4 lineage). The new carry keys on subject: only `'source'` facts survive a rebuild (`backfill_current_evidence_from_album_info`; the pin `test_v3_touch_drops_ambiguous_and_installed_facts` proves installed-labeled facts drop to NULL). So the first v3->v4 touch of a converted row permanently drops the protective source grade, then re-derives spectral from the lossy derivative — R19 violated across the converted cohort (order ~7k imports).
*Response needed:* mechanical fix, must land before 055 deploys (it is unshipped, so amending is legal): seed `spectral_subject='source', spectral_provenance='carried'` WHERE `was_converted_from IS NOT NULL` (validated against the cross-product CHECK, which only forces measured for installed), else installed/measured as now; extend `TestEvidenceTwoAxisVocabularyMigration` with a converted-row case.

### P2 — Moderate

**#4. `lib/dispatch/core.py` crossed the structural threshold (955 -> 1078 lines)**
`lib/dispatch/core.py:187` — maintainability. Confidence 100 (fact verified directly). Recalibrated from the persona's P1: no codified repo rule, no runtime impact. Design call: extract the post-import policy cluster (`_resolve_post_import_search_policy`, `_apply_post_import_search_action`, the inline HAVE-abort branch) into a new post-import module under `lib/dispatch/`, or accept the size.

**#5. `have_analysis_error` feeds the offering peer's global cooldown streak** — SETTLED CONFLICT (decision 5 / R12)
`lib/dispatch/outcome_actions.py:651` — adversarial-codex (confidence 100). The code is FAITHFUL to your settled record: decision 5 and R12 explicitly mandate ordinary attempt bookkeeping including the global user-cooldown streak. Codex's objection (an our-side analyzer fault cools innocent peers — 5 outcomes = 3-day global cooldown, potentially hiding a rare peer in its only window) is recorded as the rejected alternative; report-only, never apply. Related residual worth knowing: the abort applies cooldown via two different mechanisms depending on `import_job_id` presence (pending-outcome `TerminalCooldown` vs direct `check_and_apply_cooldown`) — latent divergence.

**#6. `docs/quality-ranks.md:122-128` still documents the deleted transcode-detection bitrate fallback**
project-standards; validator CONFIRMED (stale block is 122-128, wider than first cited): claims `transcode_detection()` "continues to use min" and describes the `cfg.mp3_vbr.excellent` spectral-fallback threshold as active; the new signature is `transcode_detection(converted_count, *, spectral_grade=None)` — fail-closed, no bitrate read. Rewrite to mirror quality-verification.md's new fail-closed wording. (Adjacent advisory from decision-fidelity, anchor 50: while in this file, name the hardcoded V0 override 230/200 as the deliberate exception to the "every threshold lives in QualityRankConfig" claim near line 205, per decision 10.)

**#7. `docs/quality-verification.md:299` says "Version 3" (and :326 "v3 wire decoder/encoder") after the ImportResult schema bump to v4**
project-standards; validator CONFIRMED — line 299 was even edited in this diff ("five disjoint concerns") without fixing the version label. Distinct from the correctly-labeled evidence `lineage_version=4`. Change to Version 4 / v4.

**#8. Force/manual imports silently became terminal; the post-import gate is skipped**
`lib/dispatch/core.py:941` — decision-fidelity + testing. Validator CONFIRMED: base ran the gate unconditionally under `action.run_quality_gate`; the diff adds `not force and scenario not in FORCE_MANUAL_SCENARIOS`, and the orchestration pin flipped from `assert_called_once()` to `assert_not_called()` (unenumerated). A force/manual import of a lossy copy now lands terminal `imported` with no search-policy decision — touching the never-stop-searching invariant. (Precision note from validation: denylist still fires on these paths; the terminal/no-requeue core is what changed.) No settled decision covers it: U7's operator-bypass is scoped to the stage-2 proof lock, R4 describes the automatic decision.
*Response needed:* operator confirmation — either "operator imports are terminal" is intended (then document it and enumerate the pin flip) or run the proof-driven gate for force/manual too.

**#9. Read-only preview path turns an unprobeable M4A into HTTP 500**
`lib/import_preview.py:1951` — reliability. Validator CONFIRMED: this diff newly makes `has_supported_lossless_audio` raise `AudioCodecProbeError`; it propagates unguarded out of `preview_import_from_path` (the enclosing try at :1918 has only `finally: rmtree`), and the web handler catches only ValueError/TypeError/ValidationError -> server catch-all 500. The worker path degrades gracefully (`measurement_crashed`, :1478-1517). Fix: wrap the measure+persist block in try/except mirroring the worker guard, returning a `PREVIEW_VERDICT_UNCERTAIN` / `measurement_crashed` preview result; add a raising-probe test. The reachable population (wrong-match / failed-download triage) is exactly where broken m4a files live.

**#10. Two narrowing rules: rejection backfill ignores the subject gate**
`lib/quality/filetypes.py:129` — decision-fidelity. Validator CONFIRMED constructible: both sites read the SAME linked current-evidence row; the gate narrows only on subject='installed', the backfill (`linked_current_evidence` branch -> `download_rejection.py:304-308`) narrows subject-blind on genuine+TRANSPARENT. A genuine MP3-320 (stamped subject='source' at import — the stamp is not lossless-gated) gets full tiers from the gate but lossless-only from the next rejection's backfill. Order-dependent policy = two narrowing rules (violates decision 3's "one narrowing rule, everywhere"); the divergence is created by this diff (subject gate added to one site only). Fix direction follows the #2 decision — align both sites to the same rule.

**#11. The DoD-required fresh AE2 pin (Fred again.. boundary) is missing**
`tests/test_quality_decisions.py:1862` — decision-fidelity. Validator CONFIRMED: no test asserts the combined scenario (candidate V0 min=193/avg=256 vs anchor 248 -> provisional_lossless_upgrade AND stays unverified via the 200-floor). The two halves exist only separately under different values; the only request-5219 test is an unrelated propagation slice. Add the named combined pin.

**#12. Corpus flips shipped beyond the plan's enumerated flip-set**
`tests/test_simulator_scenarios.py:1408` — decision-fidelity (advisory). Validator CONFIRMED by sampling: real unenumerated behavioral flips exist (test_cbr_320/256_genuine; test_deloris_flac_vs_flac_same_bitrate_downgrades — downgrade -> verified_lossless_locked, U7 names only two other tests; test_suspect_spectral_triggers_requeue, test_step1_brandlos, test_step2_ceezles in test_integration_slices.py — a module U5 doesn't list). The original 15-name list is slightly over-inclusive (test_lofi_v0_still_imports, test_opus_64 were mechanical-only). Most extras look family-2/3/1-consistent — but the Verification Contract makes unenumerated flips a stop condition precisely so accidental flips can't hide among sanctioned ones. Action: post the complete name -> family -> old/new enumeration as a PR/issue addendum before merge; #2 and #8 (both flips outside the set) show the discipline is load-bearing.

**#13. `verified_lossless_locked` renders as a raw red "Rejected" in Recents**
`web/classify.py:1356` — agent-native. Validator CONFIRMED end-to-end: the proof-lock decline writes a download_log row (outcome='rejected', scenario='verified_lossless_locked' — proven by the dispatch pin), reaches Recents, misses the delegation set at :1298-1305, falls through to `return str(scenario)` with the red Rejected badge (:837). Its sibling `lossless_source_locked` has `_lossless_source_locked_verdict()` (:1201-1211); nothing pins the new decision's rendering. This undercuts KTD4's stated purpose (don't mislabel the audit trail). Fix: delegation entry + `_verified_lossless_locked_verdict()` (non-punitive copy: archival copy is proof-verified, candidate correctly declined, request stays imported) + a tests/web pin.

### P3 — Low

**#14. Stale vulture whitelist entries mask future dead-code detection**
`tools/vulture/whitelist.py:65-66` — maintainability. Confidence 100 (verified directly): `source_lineage` and `proof_origin` entries reference `lib/sidecar.py` fields this same diff renamed to subject/provenance (line 68 is now docstring text). Regenerate via `nix-shell --run "bash scripts/find_dead_code.sh"` on the final tree; watch for cascading orphans.

## Requirements Completeness (explicit plan)

Units: U1-U11 all present on this single branch (the plan's suggested 6-PR series landed as one 4-commit branch). U1-U4, U6-U9 met; **U5 partial** (#2 AE6), **U10 partial** (#3 seeding), **U11 partial by design** (the deploy one-shot + reconciliation are operator deploy-window steps, correctly NOT committed per scope.md).
Requirements: 27 of R1-R33 met. Partial: **R4** (AE6 import-time narrowing — #2), **R19** (converted-row carry — #3), **R29/R30** (flip-set discipline — #12, and #2/#8's unenumerated flips). **R23** pending deploy window (deliberate). Settled decisions: 22 of D1-D16+KTD1-8 faithful; deviations: **D3** (#2/#10), **KTD5 seeding** (#3).

## Rejected during validation

- **"Retry merges a new source into abandoned staging -> hybrid import" (adversarial-codex P1)** — REJECTED. Premises all true (the abort skips staged-dir cleanup; `stage_to_ai_path` is deterministic per request; `StagedAlbum.move_to` merges with exist_ok; and `/Incoming` staging is NOT in `reap_disk_orphans`' slskd-root scope — correcting an earlier in-panel assumption), but the harm is blocked: preview snapshots the clean fingerprinted canonical folder BEFORE the merge, and the harness runs `_validate_quality_evidence_action_snapshot` (full sorted path/size/codec set) before any beets mutation — a leftover extra file trips "refusing to mutate" (exit 5), never a hybrid import. Same leftover shape pre-exists on the no_json/timeout/exception paths. Kept as residual: the leftover dir can wedge subsequent attempts for that request into repeated fail-closed `quality_evidence_action_failed` mismatches until manually cleaned.

## Learnings & Past Solutions

No documented scar re-opened. Specifically honored: test-fidelity Rule A (real-PG round-trips for the five new evidence columns, production-shaped), the cohort-filter/value-map lesson (version-gated CHECKs + deliberately-unmapped-value test), the #723 blind-HAVE scar (absence- AND error-verifies both die; blank-path seam closed), Rule A fake-parity (outcome taxonomy self-verifying), palo-santo/asciify (no path-rendering or duplicate-resolution surface touched). One pointer confirmed clean by correctness: no consumer treats the repurposed `requeue_upgrade` label as a rank floor.

## Deployment Notes (Go/No-Go: GO conditional — full checklist in deployment-verification.md)

Blocking pre-deploy: (1) pg_dump backup — 055 is irreversible, code-rollback alone impossible after it (restore + redeploy base is the only rollback); (2) confirm nixosconfig wrapper sets no `gateMinRank` (checked clean on the local master clone at review time — re-confirm on doc1 at deploy); (3) `nix build .#checks.x86_64-linux.moduleVm`; (4) baseline audits saved. Plus finding #3's amendment MUST land in 055 before it ships.
Intra-window ordering is the real correctness gate: hold `cratedigger.timer` -> migrate -> U11 scalar one-shot -> record wanted-AND-verified reconciliation (expect 1 by scalar / 0 by evidence pre-one-shot) -> resume. Old-code in-flight cycle risk is bounded: evidence writers live only in restarting workers; migrator lock_timeout fails closed.
Recommended around deploy (read-only): `SELECT DISTINCT v0_subject, v0_provenance, verified_lossless_provenance` (value-map completeness rests on the audited live value set); count converted rows with `spectral_grade IS NULL` (each costs one have_analysis_error attempt before self-healing); post-deploy: columns present/absent, default=4, outcome CHECK, no row loss, v4 adoption climbing, `have_analysis_error` volume (broad spike = over-eager gate).

## Coverage

- 12 reviewer lanes + cross-model Codex pass, all completed; fast pass emitted 0 preliminary items (none withdrawn).
- Validators: 12 dispatched, 11 confirmed, 1 rejected; all 12 were interrupted once by a session-limit event and resumed with context intact — no degraded verdicts. 2 additional mechanical findings (#4, #14) verified by direct orchestrator inspection instead of validators.
- Settlement conflicts: 1 (#5, stamped `settled_conflict: decision-5/R12`, report-only). 1 anchor-50 advisory (V0-override doc exception) folded into #6. Maintainability #4 recalibrated P1 -> P2 (no codified repo rule; disagreement noted).
- Apply pass skipped by operator instruction; tree untouched and clean at `9730b628`.
- Residual risks (aggregate): evidence-unavailable known-bad coverage gap (a raised decision must reopen, not silently import — no test); leftover staged dir can wedge subsequent attempts (see Rejected); R9 fresh-HAVE requirement bypassed when `attempt_result.audit` is None (legacy/no-preview dispatch paths); library-row proofs all record provenance 'carried', collapsing the measured/carried Recents distinction onto candidate rows; two cooldown mechanisms keyed on import_job_id presence; v1/v3 rows with unmapped legacy strings hydrate off-Literal via direct Struct construction (msgspec only validates via convert) — tolerated by rebuild-on-touch; `_current_evidence_allows_action` branch at core.py:418 is now-unreachable shadow code (latent trap if the fail_closed=>FAILED invariant breaks); proof-lock result reports the candidate's `verified_lossless` on a current-proof-driven decision (parity holds, audit reads oddly); `expected_current_evidence_id=0` magic sentinel.
- Testing gaps (aggregate): R12 cooldown-streak asserted only on the force-path pin (not auto/manual, not in the property); mixed ALAC+AAC m4a folder unpinned; no ffprobe-timeout->None->AudioCodecProbeError pin; no real-ffprobe boundary test (probe is patched everywhere); no cross-cycle preview/importer interleave test; no known-bad for the evidence-unavailable except-block; converted-path spectral-None negative pin absent; no-proof FLAC-vs-FLAC downgrade lane lost its pin when the deloris test became the proof-lock pin.

---

## Operator decisions — 2026-07-17 (post-review settlement)

Settled in-session with the operator after the panel + validation pass. These resolve every open decision gate; the plan's Key Decisions gain D17-D20 and the issue #711 thread carries the same record.

1. **Finding #2 + #10 — AE6 stands; narrowing is subject-blind.** The transparent+genuine -> lossless-only rule fires at import time on the genuine grade; the subject label does not gate narrowing, and the post-import gate and rejection backfill share one identical condition. For an unconverted lossy import the source-subject grade describes the installed bytes (out-of-band mutation is outside the state model, decision 6). `test_cbr_320_genuine` / `test_cbr_256_genuine` restore to the U5-directed expectations; AE6 both-halves pins added.
2. **Finding #1 — the evidence-unavailable path reopens without blame.** When the post-import gate cannot load the linked current evidence, the request returns to `wanted` with NO denylist entry — a denylist attaches only after a quality decision on successfully loaded evidence. Local bookkeeping failure is never attributed to a peer. The pinning test flips to assert no denylist.
3. **Finding #8 — force-import overrides the beets distance and nothing else.** No separate code path, no separate state, never terminal: force/manual imports run the identical post-import quality-gate/search-policy decision as automatic imports. The only surviving operator exception is stage-2: force-import is not blocked by an existing proof lock (decision 1 / U7). The `FORCE_MANUAL_SCENARIOS` gate skip is removed; the gate-runs pin restores.
4. **Finding #4 — split now.** The post-import policy cluster extracts from `lib/dispatch/core.py` into its own module under `lib/dispatch/` in the same wave as the behavior fixes.
5. **Finding #5 — decision 5 reaffirmed.** `have_analysis_error` keeps feeding the global user-cooldown streak: the cooldown is good-citizen protection — do not hammer a slskd peer while the local install is busted — not punishment. Codex's alternative stays rejected. The two cooldown application mechanisms (pending-outcome vs no-job path) unify without policy change.

Finding #12's complete flip enumeration is produced against the post-fix tree (the #2 and #8 restorations shrink it).

## Appendix — complete corpus flip enumeration (finding #12; post-fix tree)

Every behavioral test flip on the branch, by authority. "Plan" = enumerated in U4/U5/U7/U9's
test-scenario lists; "D17"-"D19" = sanctioned by the 2026-07-17 operator decisions; "reverted" =
an unsanctioned flip the decision commits flipped back. Mechanical churn (field/vocabulary
renames, kwarg migrations with identical asserted behavior — e.g. `test_lofi_v0_still_imports`,
`test_opus_64`) is excluded.

**Family 1 + 5 (verified inversion; lossless re-import over proof) — plan:**
`test_genuine_flac_reimports_verified` -> proof-lock pin; `test_mp3_higher_than_lofi_imports` -> lock pin.
Family-consistent, enumerated here per #12: `test_deloris_flac_vs_flac_same_bitrate_downgrades`
(downgrade -> verified_lossless_locked; its no-proof FLAC-vs-FLAC downgrade coverage is a noted
testing gap).

**Family 2 (terminal lossy accept abolished) — plan:** `test_mp3_v0_240` (canonical),
`test_avg_bitrate_flows_into_stage3_rank`, the four accept-rows of `TestQualityGateDecision.CASES`,
`test_stage3_grade_aware_spectral_gate` genuine/marginal rows,
`test_quality_gate_reads_current_spectral_not_last_download`,
`test_genuine_v0_replacing_transcode_accepted`, `test_quality_gate_ignores_genuine_low_spectral`,
the "mp3 v0" subtest of `test_explicit_mp3_labels_ignore_contradictory_projected_modes`,
`test_explicit_mp3_label_owns_mode_and_gate_policy` (both branches), Opus 64/48 verified-target
re-pins. Family-consistent, enumerated here per #12:
`test_step2_ceezles_crosses_excellent_threshold_on_avg` -> `..._cannot_terminate_on_bitrate_rank`,
`test_import_quality_accept`/`_requeue_upgrade`/`_requeue_lossless`,
`test_transcode_upgrade_requeues_with_denylist` -> `..._full_tiers_with_denylist`,
`test_happy_path_acquires_lock_keyed_on_mbid_and_runs_import` -> `..._retains_unverified_import`,
`test_median_metric_accepts_outlier_album_end_to_end` (deleted; replaced by the D17 median pair).

**Family 3 (grade-blind CBR narrowing loosened) — plan:** the three CBR `requeue_lossless` rows in
`TestQualityGateDecision.CASES`, `test_cbr_320_no_spectral`/`test_cbr_256_no_spectral`,
`test_requeue_lossless_uses_intent`, the "mp3 320" subtest. Family-consistent, enumerated here:
`test_cbr_256_genuine` (EXCELLENT rank no longer narrows),
`test_suspect_spectral_triggers_requeue` -> `..._keeps_full_tier_search`,
`test_step1_brandlos_imports_transcode_over_genuine_on_avg_gain` (override cleared).
**Reverted by D17:** `test_cbr_320_genuine` (narrows + denylists again, per U5's directive).

**Family 4 (absence-verifies inverted) — plan:** `test_flac_no_spectral_is_verified`,
`test_lossless_no_spectral_is_verified`, the 8 spectral-None `TestTranscodeDetection` rows (+ the
two cfg-fallback tests, deleted with the branch), the error-grade negative pin.

**Codec-label pins (R27, additional to the families) — plan:**
`test_vorbis_folder_falls_back_to_mp3` -> asserts vorbis; `test_empty_folder_falls_back_to_mp3` ->
no-audio sentinel; the vorbis assertion in `test_unmapped_codec_returns_none`.

**D17 (subject-blind narrowing; new flips by the decision commits):** the "CBR 320 carried source
grade" `TestQualityGateDecision` row (full tiers -> narrows),
`test_lossless_narrowing_requires_installed_spectral_authority` ->
`..._is_subject_blind_for_genuine_transparent` (generated property),
`test_transparent_carried_source_grade_stays_on_full_tiers` -> `..._also_narrows`,
and six dispatch slices re-asserting the lossless override at import time
(`test_genuine_transparent_import_narrows_to_lossless`,
`test_genuine_cbr_320_import_narrows_at_import_time`, the median/AVG metric pair, the two
stale-field-clearing slices, the lock-contention happy path,
`test_import_with_unmeasured_distance_records_null`).

**D18 (no-blame reopen):** `test_missing_linked_evidence_reopens_full_tier_search`,
`test_linked_evidence_load_error_reopens_full_tier_search`,
`test_unavailable_refresh_sentinel_cannot_consume_old_proof`,
`test_suspect_spectral_keeps_full_tier_search` (stamps-only world), and the ceezles chain
denylist assertion — all now assert the peer stays available.

**D19 (force-import overrides distance only):**
`test_successful_force_and_manual_imports_skip_automatic_gate_and_notify` ->
`..._run_post_import_pipeline` (reverted to the pre-branch contract),
`test_force_and_manual_imports_skip_automatic_search_policy` ->
`..._run_the_same_post_import_gate`,
`test_operator_retained_import_decisions_stay_terminal` -> `..._requeue_like_automatic`,
`test_force_import_success` (force-imported unverified copy lands wanted, not imported).

---

## Verdict: NOT READY (as-is) — high-fidelity implementation, three confirmed P1s gate the merge

Two of the P1s are operator decisions the plan itself classifies as stop-conditions (surface, don't adapt around); one is a mechanical migration amendment that must land before 055 ships. Everything else is mechanical cleanup.

Fix order:
1. **Decide #2 (AE6 reading)** — settles #10 and part of #12's addendum.
2. **Decide #1 (failure-path contract)** and **#8 (operator-import terminality)** — group A.
3. **Apply #3** (055 amendment + migrator test) — before any deploy of this branch.
4. Mechanical queue: #9 preview guard, #13 locked-decision renderer, #11 AE2 pin, #6/#7 docs, #14 whitelist regen; then #12's flip-enumeration addendum; #4 at your discretion.
