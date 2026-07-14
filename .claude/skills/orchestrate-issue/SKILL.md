---
name: orchestrate-issue
description: Orchestrate a substantial Cratedigger issue through scoped PRs, independent review, merge, deployment, live verification, tagging, and closure. Use for whole-issue delivery, especially when several workstreams can run concurrently. Do not use for a small patch, diagnosis-only request, or ordinary review.
---

# Orchestrate Issue

Own the outcome, not every keystroke. Keep scope and architecture coherent while
agents implement and review bounded pieces. Repository instructions remain the
authority; load the deploy skill when it is time to ship instead of repeating
its runbook here.

## Shape the work

Read the issue and comments, inspect the relevant code and live state, then make
a compact coverage ledger: requested outcome, PR, dependency, and evidence.
Use it to prevent forgotten edge items, not to rewrite the issue as bureaucracy.

Prefer small, coherent PRs in isolated worktrees. Use available agent slots for
independent work when it shortens the critical path; an idle slot is cheaper
than a duplicate agent. Serialize only for a concrete dependency, overlapping
ownership, or required merge order. Independent PRs may start from the same
current `main`; refresh it before final review.

## Implement and review

Compose agents around bounded jobs, not a fixed per-PR template. Implementation
may stay local, be delegated, or be split across disjoint surfaces. Review may
be broad or specialist, but must be independent of the code it assesses. Use
the fewest agents that give real coverage. Do not spend tokens on duplicate
generalist reviews or send several agents to answer the same question.

Treat tokens and context length as budgets. Brief agents with the issue slice,
worktree, exact SHA, and only the relevant rules; point to durable files instead
of pasting conversation history. Keep handoffs compact. Replace an agent whose
context is getting long at a coherent boundary. Never carry a subagent into the
next PR.

Let useful implementations and reviews overlap when safe. Agents should run
autonomously without reassurance pings or routine polling.

Keep work local through review. Implementers use focused tests while converging
and follow the repository's rules for tests, docs, and generated properties.
The orchestrator understands the diff and scope before handing the exact signed
local SHA over for independent review.

Review returns either one consolidated finding batch or `CLEAN`. Fix findings
locally and repeat on the new SHA. An independent `CLEAN` verdict is final. If
current `main` changes the reviewed tree, integrate it and review the new SHA.

Push the reviewed SHA with one ordinary branch push through the repository
pre-push hook, then open the PR. The hook is the sole release-grade code gate.
If a hook failure requires a code change, review that new local SHA before the
next push. Do not add post-push review, duplicate suites, artifact gates, Nix
replays, or post-merge code audits.

## Merge and advance

Confirm the PR head is the reviewed SHA and the coverage ledger is satisfied.
The PR body and every branch commit message must use canonical `Refs #N` or a
plain issue URL, never an auto-closing keyword; audit both with
`python3 scripts/audit_issue_references.py`.

Use the repository's merge method and expected-head guard. Confirm the merge
tree equals the reviewed PR-head tree, update the ledger, and keep independent
work moving. Do not rerun code gates after an unchanged reviewed tree merges.

## Ship

When the ledger is complete, use the deploy skill. Verify the exact deployed
source and the real user-facing behavior, including the post-switch successor
cycle where the service lifecycle requires it. Screenshots or live data should
prove the reported symptom, not merely that deployment commands succeeded.

Create the signed release tag at the verified merge. Push the signed tag with `--no-verify`;
it adds no code. Confirm the remote tag, then record the PR, review, deploy, and
live evidence. Only after the signed tag push succeeds, close the issue
deliberately.

## Keep perspective

The orchestrator owns scope, architectural judgment, merge authority, and live
evidence. Agents own bounded execution and independent challenge. Keep useful
slots busy, communicate only material milestones or blockers, and lead the
handoff with what shipped and how it was proven.
