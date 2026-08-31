---
name: review-mutants-target-the-changed-expressions
description: Author mutants go at the diff's changed expressions first; record the failing TEST ID from output; subagents are pinned to the parent's worktree
metadata:
  type: feedback
---

Three lessons from the #1278 wx4 review round (2026-08-31):

1. In a verbatim-move refactor, the changed expressions are a tiny enumerable
   set — aim the FIRST author mutants there. Both reviewers independently
   converged on the one inlined wrapper (`job.to_json_dict()`) as the sole
   test gap; the author's mutants had targeted collaborator wiring instead.
2. When claiming "mutant X killed by test Y", read the failing TEST ID from
   the actual output — never infer Y from a failure count plus plausibility.
   The wx4 commit message named a killer test that could not distinguish the
   mutant (hardcoded `include_replaced=False` IS the default path); only the
   independent reader caught it. The #1209 confabulation shape.
3. Subagents spawned from a worktree-pinned session are PINNED to that same
   worktree — pre-made detached review worktrees go unused, and a mutant
   runner will plant mutants in the orchestrator's LIVE tree. Workable only
   with: orchestrator freezes all edits until the runner reports, runner
   restores by inverse edit with md5 proof, final `git status --porcelain`
   empty. Related: [[subagent-worktree-absolute-paths]].

**Why:** each shipped (or nearly shipped) a false claim or a live hazard that
only the next independent read caught.

**How to apply:** enumerate the diff's semantic changes before handoff and
mutate each; paste the failing test line into any kill claim; before spawning
a mutant runner, decide deliberately where it will actually run and freeze
that tree.
