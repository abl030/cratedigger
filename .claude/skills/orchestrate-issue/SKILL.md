---
name: orchestrate-issue
description: "Orchestrate a substantial Cratedigger issue end to end: understand the scope, coordinate the work, converge efficiently, ship, verify live, and close. Use for issue-sized delivery, not small patches, diagnosis-only work, or ordinary review."
---

# Orchestrate Issue

You are the orchestrator. Own the issue outcome and keep the whole problem in
view. This skill defines the broad lifecycle and safety boundaries; use your
judgment for the route through them.

Repository instructions remain authoritative. Do not use Compound Engineering.
Load the deploy skill when it is time to ship instead of duplicating its
runbook here. The independent-review gate below is an intentional, specific
exception to client compatibility mappings that otherwise serialize subagent
work in the main thread.

## Own the issue

Understand the issue, its comments, the relevant code, and any live state that
can change the diagnosis. Keep enough lightweight coverage tracking to know
that every requested outcome and invariant has an owner and convincing
evidence. The format does not matter.

Choose the delivery shape that best fits the work: one PR or several,
sequential or parallel, direct implementation or delegation. Agents,
exact SHAs, detailed ledgers, and extra validation passes are tools to use when
they reduce a concrete risk or shorten the critical path. Implementation
delegation is optional; the independent subagent review below is mandatory.

The orchestrator retains responsibility for scope, architecture, integration,
merge decisions, deployment, and live proof even when work is delegated.

## Converge efficiently

Batch implementation failures, validation failures, and review findings. Apply
`CLAUDE.md`'s judgment-based development policy, including every mandatory
surface-specific contract, and fix related problems together. Treat failures
from direct whole-tree runs as ordinary convergence feedback.

Focused tests are appropriate while implementation is changing. Before a
delegated implementer returns a converged candidate, it must self-review,
commit the tree, and invoke the `check` skill itself. Do not accept a handoff
until the one canonical complete suite is green. The handoff must name the
exact tested commit and receipt/bundle so independent review examines that same
tree. The implementer owns every suite failure before handoff; do not defer the
first complete run to the orchestrator or reviewer.

Review the meaningful converged tree as a whole. Review again when corrections
materially change behavior or risk.

Commission at least one independent, read-only subagent to review every
meaningful converged PR diff before its first push. The reviewer must be
previously uninvolved: it must not have implemented, planned, or diagnosed the
change. Give it the issue contract and raw diff or exact refs, but not the
orchestrator's conclusions; ask it to challenge the real production path,
invariants, migration/runtime compatibility, and missing tests. Require ranked
findings with exact evidence, or an explicit no-findings result with residual
risks.

The reviewer may run targeted probes and counterexamples, but it does not
repeat the identical deterministic suite when the exact reviewed commit already
has a valid receipt. Any correction invalidates the prior receipt: return it to
implementation, commit the corrected tree, run the complete suite again, then
review the new exact commit in proportion to the correction's behavior and
risk.

Triage every finding yourself. Fix valid findings in one convergence pass and
commission a fresh independent review when the corrections materially change
behavior or risk. Record a concrete rationale for rejected findings. A green
test suite or the orchestrator's own review does not substitute for this gate.
If an independent subagent cannot be started, stop before push and surface the
blocker; do not silently downgrade to self-review.

Integrate current `main` at sensible boundaries rather than continuously
chasing it. Before first push, require a passing `check` receipt for the exact
reviewed commit. Reuse the implementer's receipt when review and integration
left that commit unchanged; do not replay it as release ceremony.

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
