---
name: check
description: Run Cratedigger's one canonical receipt-backed full suite on a clean committed tree before review handoff or first push.
---

# Canonical Quality Check

This skill wraps the single receipt-backed canonical suite. The underlying
checks may also be run directly through Nix-shell whenever they are useful
during development; direct runs provide feedback but do not mint final receipts
or satisfy this confirmation.

Invoke this skill after the implementer has self-reviewed and committed the
converged tree, before handing that exact commit to independent review. If
review changes nothing, the same receipt is the final pre-push confirmation. If
anything changes, commit the correction and run this skill again before another
review handoff.

## Steps

1. Run the canonical suite:
```bash
scripts/run_final_gate.sh
```

The command must exit zero and report `PASSED`. It runs whole-repository
Pyright, production-strict Pyright, JavaScript syntax and unit tests, Ruff,
Vulture, and the complete Python scheduler. Every independent phase still runs
after an earlier failure, and the command prints the private failure-bundle path
before returning its aggregate status. Investigate every indexed failure; do
not carry a chat-era "known issue" exemption forward without current repository
evidence.

2. The helper prints a mode-0700 receipt path under the private runtime tmpfs
before launching the one canonical underlying command
(`env CRATEDIGGER_SUITE_OWNS_HEADROOM=1 nix develop --command bash -c "bash scripts/run_tests.sh"`
since issue #1229 — the receipt's recorded `command` file still holds
`bash scripts/run_tests.sh`, so `status` compares it unchanged; only the
launcher around it moved). It saves complete output in
`output.log`, records the validated private suite-bundle path in `bundle`, then
atomically writes `terminal` only after the command exits. The bundle — while
it still exists — contains the typed summaries and complete per-phase logs.
A second concurrently-launched canonical suite on this shared host waits on
`run_suite`'s own admission lock rather than colliding with this one; once
admitted it first retires eligible receipts
(`scripts/test_substrate.py::reap_stale_final_gate_receipts`, issue #1208
item 4 — a receipt whose lifecycle is provably over, `terminal` present or
its recorded helper/gate process identities conclusively dead, AND older
than a fixed 7-day floor), then best-effort reaps check bundles idle past
~4 hours (`DEFAULT_STALE_BUNDLE_MAX_AGE_SECONDS`), but never one a still-
present receipt references — `status` (below) still stats the bundle path
and fails visibly rather than silently reporting `pass` over evidence that
no longer exists. A dangling receipt (bundle gone despite that protection)
means something genuinely unusual happened to it outside the normal reap
path — the honest response is to re-run the gate, not to trust the stale
`terminal` verdict.

If the client detaches, recover the exact invocation from the same committed clean
worktree:
```bash
scripts/run_final_gate.sh status /run/user/$UID/cratedigger-final-gate.XXXXXXXX
```
`exact-active` means the receipt's helper and gate PID/start-tick identities
still match; recover it rather than launching a duplicate. `pass` and `fail` are
terminal; `incomplete` means no terminal result was recorded. A matching `pass`
receipt prevents rerunning that unchanged gate. Never treat `fail` or
`incomplete` as green; choose the next action explicitly. Receipts are never
retried automatically, and a live or recent one is never deleted automatically
either — only a receipt that is both conclusively finished-or-dead AND older
than the 7-day retirement floor above is ever removed, and only by a later
admitted suite run's own reap pass (issue #1208 item 4).

The isolated final-gate worktree must remain exclusively owned for the entire
gate. The receipt rechecks its HEAD and clean state immediately before terminal
publication, but it is not a snapshot or protection against a concurrent writer
that changes and perfectly restores that worktree.

3. If the command passes and independent review leaves the commit unchanged,
push the branch once. If it fails, return the problem to the implementer for
ordinary convergence. Any tree change requires a new clean committed HEAD, a
new passing receipt, and review in proportion to the correction's risk. Do not
rerun a passing receipt for an unchanged tree after review, push, or merge.
