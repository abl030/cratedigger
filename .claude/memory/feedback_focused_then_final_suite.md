---
name: feedback-focused-then-final-suite
description: "Use focused tests while iterating, then run threaded whole-repo Pyright and one full suite on the final committed tree before its first push"
metadata:
  node_type: memory
  type: feedback
---

Keep implementation and review local while they converge. Run the smallest
relevant test modules during that loop. Once the exact tree is reviewed and
committed, run `nix-shell --run "pyright --threads 4"` and then
`nix-shell --run "bash scripts/run_tests.sh"` exactly once before its first
branch push.

Both final commands must pass on the tree that is pushed. If either finds a
problem, fix it, reconverge with focused tests, commit and review the new tree,
then restart the final sequence. Do not replay it after push, merge, or during
deploy when the revision is unchanged.

There is no pre-push hook or CI suite gate. Deterministic generated tests remain
in the full suite; the deep `fuzz` profile remains an explicit tool for relevant
policy changes and bug hunting.
