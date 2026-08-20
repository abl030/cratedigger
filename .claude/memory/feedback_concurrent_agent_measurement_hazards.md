---
name: concurrent-agent-measurement-hazards
description: Parallel agents on one box corrupt perf measurements and share a repo-wide git stash — load-contaminated numbers understated two of three wins, and one agent popped a sibling's uncommitted work
metadata:
  type: feedback
---

Running several agents concurrently on doc1 is normal and productive, but two
hazards bite specifically when the work is **performance measurement** or
involves **git plumbing**. Both were hit in the 2026-08-20 #1156 series.

## `git stash` is repo-wide, never worktree-local

All worktrees of one repository share **one stash stack**. An agent's
`git stash push` / `pop` interleaves with a sibling's, and one agent
**popped a sibling's uncommitted changes into its own tree**. It recovered
correctly (caught it via `git status`, re-stashed the sibling's content
untouched with a "DO NOT DROP" label, recovered its own entry by explicit
index, then stopped using stash entirely) — but this is a data-loss shape.

**How to apply:** brief every agent that `git stash` is banned when siblings are
active. Toggle baseline/candidate with patch files (`git diff > f`,
`git apply`/`git apply -R`) or worktree-local commits instead.

## Load-contaminated measurements understate wins, and cost real time

Two of three perf agents reported numbers materially worse than reality because
siblings were loading the box:

- Item 7 reported its whole-module result as *"noise-dominated, 3/6 trials each
  way, went the wrong way"* (13.8s → 18.6s). On a quiet box it is **13.13s →
  9.74s, 26% faster, 6/6 paired trials.** The agent nearly recommended reverting
  a genuine win.
- Item 1 reported −13%; controlled paired probes measured **−20% to −29%**.
- The one **overstatement** came from an unbalanced run design, not load: a
  5-run B,A,A,B,A sequence under a falling load trend biased toward the second
  variant and reported −13% where a balanced **ABBA×2** gave **−8.5%**.

**How to apply, brief this verbatim to perf agents:**

1. Record `uptime` / load average **before and after every timing run** and
   report both — a wall-time number without load context is not evidence.
2. Wait (`sleep` loop) for the box to settle rather than timing through a
   sibling's run; baseline and candidate must be taken under comparable
   conditions, ideally back to back.
3. Use a **balanced** design (ABBA, repeated) so a monotone load trend cannot
   favour either side. Report the spread, not a single number.
4. Take the same discipline yourself as orchestrator before quoting any
   aggregate figure.

Note the `check` skill's suite takes an advisory admission lock, so canonical
runs already queue rather than overlap — it is the **ad hoc** timing runs that
are exposed. A related artefact: a `nix eval` reading the whole source tree
raced a sibling worker writing `tests/__pycache__/*` and failed a gate once
(TOCTOU, reproduced clean in isolation at load ~1, failed at load 22-30).

See [[1156-suite-perf-complete]] and [[mutation-testing-pycache-trap]].
