---
name: check
description: Run Cratedigger's final pre-push threaded type check and full test suite after focused implementation checks are complete.
---

# Final Pre-push Quality Check

Use focused test modules while implementing. Invoke this skill only after the
final tree is reviewed and committed, immediately before its first branch push.

## Steps

1. Run pyright on the full repository:
```bash
scripts/run_final_gate.sh pyright
```

Must be **0 errors**. Do not proceed if there are new errors
(psycopg2/slskd_api "could not be resolved" warnings are OK — they're C
extensions).

2. Run the full test suite exactly once:
```bash
scripts/run_final_gate.sh tests
```

The command must exit zero and report `OK` with no skipped tests. Investigate
every failure; do not carry a chat-era "known issue" exemption forward without
current repository evidence.

3. The helper prints a mode-0700 receipt path under the private runtime tmpfs before
launching each unchanged underlying command (`nix-shell --run "pyright --threads 4"`
or `nix-shell --run "bash scripts/run_tests.sh"`). It saves complete output in
`output.log`, then atomically writes `terminal` only after the command exits.

If the client detaches, recover the exact invocation from the same committed clean
worktree:
```bash
scripts/run_final_gate.sh status /run/user/$UID/cratedigger-final-gate.XXXXXXXX
```
`exact-active` means the receipt's helper and gate PID/start-tick identities still
match; `pass` and `fail` are terminal; `incomplete` means no terminal result was
recorded. A matching `pass` receipt prevents rerunning that unchanged gate. Never
treat `fail` or `incomplete` as green; choose the next action explicitly. Receipts
are never retried or deleted automatically.

The isolated final-gate worktree must remain exclusively owned for the entire
gate. The receipt rechecks its HEAD and clean state immediately before terminal
publication, but it is not a snapshot or protection against a concurrent writer
that changes and perfectly restores that worktree.

4. If both commands pass, push the branch once. If either fails, fix the problem,
run focused tests while reconverging, commit and review the new tree, then
restart this final sequence. Do not rerun it for an unchanged tree after push or
merge.
