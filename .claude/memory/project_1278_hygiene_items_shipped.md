# #1278 hygiene items (both) SHIPPED — 2026-08-31

PR #1308 (dead-code sweep) + PR #1309 (importer job-kind adapters), merged as
merge commits, combined main 55b7a221 re-gated, deployed via nixosconfig
4f2cfb6d, live-verified (fleet anchor, migrate invocation change, four workers
active, cycle 301a5900 from exact store ap9m0xrg…-source). Full close-out:
https://github.com/abl030/cratedigger/issues/1278#issuecomment-5479594564

Key facts a future session needs:

- **`match_transfer_id` was NOT deletable** — the scan's "deletable now" was
  false (two internal callers in `slskd_enqueue_with_outcome` since #822).
  Demoted to `_match_transfer_id`. Its docstring now says the underscore is a
  closed-seam marker, not an enforced boundary (`reportPrivateUsage` is off).
- `lib/util.py` is at **zero explicit `Any`** (ratchet entry deleted);
  `BeetsOpFailure` → `lib/quality/import_result_types.py::DisambiguationFailure`
  (single name, wire byte-identical); `lib/beets_album_op.py` gone.
- `tests/test_track_crosscheck.py` had ZERO collected tests (no base class)
  since inception; activated + de-vacuoused (literal asserts, tolerance
  boundary pair pinning the `//5` divisor both directions).
- `scripts/importer.py` dispatches all four job kinds through
  `_IMPORT_JOB_KINDS` (frozen per-kind descriptors; `_kind_for()` falls back to
  `_UNROUTED_JOB_KIND`). 14/15 branch sites converged; the survivor is
  `_automation_claim_is_current`'s identity conjunction (documented exception).
  Registry field is `execute_fn` NOT `execute` (the replaced-write audit's
  bounded grammar claims `.execute(` calls as SQL).
- The success-path committed-wrong-match gate is a stated accepted residual:
  reachable (decides `direct_attribution` evidence stamping on accepted rows)
  but a success outcome carrying a wrong-match scenario is impossible by
  construction (`_DispatchSettlement` has no scenario field). Pinning it needs
  an accept-path integration slice.
- **Preview-worker `db: Any` narrowing is DEFERRED with measurement** (its own
  scoped item): ten sites at zero ratchet margin, `_AutomationPreviewDB`'s
  `__getattr__` delegation fails any method-naming Protocol (probe-verified),
  plus a `hasattr` optional-method seam. A redesign, not annotations.
- Process: the series' dominant defect class was **counts in correction-round
  prose** (five instances); every one caught by the next independent read or by
  an implementer measuring before relaying. See
  [[feedback_correction_rounds_mint_false_claims]] and
  [[feedback_orchestrator_briefs_become_defects]] — both held for the fourth
  series running.

Remaining open #1278 work: strong candidate 1 (owned-key slskd module) plus the
residuals list in the close-out comment.
