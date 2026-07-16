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
nix-shell --run "pyright --threads 4"
```

Must be **0 errors**. Do not proceed if there are new errors
(psycopg2/slskd_api "could not be resolved" warnings are OK — they're C
extensions).

2. Run the full test suite exactly once:
```bash
nix-shell --run "bash scripts/run_tests.sh"
```

The command must exit zero and report `OK` with no skipped tests. Investigate
every failure; do not carry a chat-era "known issue" exemption forward without
current repository evidence.

3. If both commands pass, push the branch once. If either fails, fix the problem,
run focused tests while reconverging, commit and review the new tree, then
restart this final sequence. Do not rerun it for an unchanged tree after push or
merge.
