---
name: orchestrate-issue
description: Orchestrate a substantial Cratedigger issue end to end: understand the scope, coordinate the work, converge efficiently, ship, verify live, and close. Use for issue-sized delivery, not small patches, diagnosis-only work, or ordinary review.
---

# Orchestrate Issue

You are the orchestrator. Own the issue outcome and keep the whole problem in
view. This skill defines the broad lifecycle and safety boundaries; use your
judgment for the route through them.

Repository instructions remain authoritative. Do not use Compound Engineering.
Load the deploy skill when it is time to ship instead of duplicating its
runbook here.

## Own the issue

Understand the issue, its comments, the relevant code, and any live state that
can change the diagnosis. Keep enough lightweight coverage tracking to know
that every requested outcome and invariant has an owner and convincing
evidence. The format does not matter.

Choose the delivery shape that best fits the work: one PR or several,
sequential or parallel, direct implementation or delegation. Agents,
worktrees, exact SHAs, detailed ledgers, and extra validation passes are tools
to use when they reduce a concrete risk or shorten the critical path. They are
not required ceremony.

The orchestrator retains responsibility for scope, architecture, integration,
merge decisions, deployment, and live proof even when work is delegated.

## Converge efficiently

Batch implementation failures, validation failures, and review findings. Apply
`CLAUDE.md`'s judgment-based development policy, including every mandatory
surface-specific contract, and fix related problems together. Treat failures
from direct whole-tree runs as ordinary convergence feedback.

Review the meaningful converged tree as a whole. Review again when corrections
materially change behavior or risk.

Integrate current `main` at sensible boundaries rather than continuously
chasing it. When the change is ready for its first push, invoke the `check`
skill; it owns the final clean committed-tree receipt mechanics.

Review should challenge the issue contract and real production path, not just
confirm that tests are green. Stop when the issue is covered, required checks
pass, and there is no concrete remaining counterexample.

## Ship and prove it

Keep issue references non-closing while deployment and live proof remain. Use
the repository's merge method, the deploy skill, and current downstream
instructions.

Verify the deployed system rather than only the deployment command. Exercise
the real CLI, API, or user-facing path and account for the service lifecycle
that loads the new code. Close the issue deliberately only after its scope is
covered and live evidence supports the result. Record unrelated follow-up work
separately, and clean up temporary worktrees when they are no longer useful.

## Communicate like an orchestrator

Lead with outcomes, decisions, material milestones, and blockers. Do not make
the operator follow routine agent pings, exact-SHA churn, or every intermediate
test failure. When evidence changes the diagnosis, say so and adjust.

Completion means the issue is covered, the converged change has been reviewed
in proportion to its risk, required final checks pass, the work is merged and
deployed, live behavior is verified, and the issue is closed with the evidence
that matters.
