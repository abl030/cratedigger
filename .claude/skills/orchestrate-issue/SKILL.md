---
name: orchestrate-issue
description: Orchestrate a substantial Cratedigger GitHub issue from scope audit through multiple isolated implementation PRs, independent review loops, merge commits, deployment, live verification, tagging, closure, and post-ship reflection. Use when the user asks to complete or ship an entire issue, especially when it spans several workstreams or PRs and requires implementation agents to remain separate from review agents. Do not use for a single small patch, diagnosis-only request, or ordinary code review.
---

# Orchestrate Issue

Own the issue end to end. Delegate bounded execution and independent review,
but keep scope coverage, architectural decisions, merge authority, deployment,
and final verification in the orchestrator.

Do not use Compound Engineering or `ce-*` skills in this repository.

## 1. Establish the contract

1. Read the issue body and every comment. Read current repository instructions
   and every path-scoped rule that the issue may touch.
2. Inspect the relevant code and live state before deciding the PR breakdown.
   Do not merely restate the issue's suggested grouping.
3. Build a coverage ledger containing:
   - every named workstream and acceptance criterion;
   - every "small", optional-looking, or batchable item;
   - decisions the operator must make;
   - the planned PR and verification evidence for each item.
4. Create a native task plan. Keep one PR in progress at a time unless two
   tasks are genuinely independent and cannot mutate overlapping files.
5. State any interpretation that changes product behavior. Preserve the
   archivist invariants: strict pressing identity, perpetual searching,
   reversible/operator-owned decisions, and positive ownership before cleanup.

The coverage ledger is the defense against an implementer completing the main
idea while silently missing the edge items.

## 2. Isolate every PR

For each PR:

1. Fetch `origin/main`.
2. Create a fresh branch and worktree under a task-specific path outside the
   shared primary checkout.
3. Base the worktree on current `origin/main`, not on another unmerged feature
   branch, unless the issue explicitly requires a stack.
4. Never edit or clean the shared primary worktree. Treat unrelated dirt as
   another operator's work.
5. After the preceding PR merges, create the next worktree from the resulting
   new `main`. This keeps integration feedback inside each PR and prevents a
   long speculative stack.

Record the worktree, branch, base SHA, PR number, and current head SHA in the
plan or coverage ledger.

## 3. Delegate implementation with a hard role boundary

Create a fresh implementation agent for each PR. Never carry an implementation
agent into the next PR. Give it:

- the exact worktree and branch it may modify;
- the issue items assigned to this PR;
- repository invariants and relevant rules;
- expected tests, docs, generated properties, and live evidence;
- permission to commit, push, and open a non-draft PR;
- the PR-reference contract: whenever the PR body or a branch commit message
  references the issue, use canonical `Refs #N` or a plain issue URL, never a
  GitHub auto-closing keyword;
- an explicit ban on code review, self-issued CLEAN verdicts, merge,
  deployment, tagging, issue closure, and edits outside its worktree.

Every implementation-agent brief must state that reference contract. Require
the implementation agent to:

1. Inspect current code rather than trust stale issue line numbers.
2. Start from invariants and write the deterministic pin plus generated
   property required by repository rules. Qualify new harnesses with a
   known-bad/fault test.
3. Avoid compatibility shims, skipped tests, guessed schemas, and duplicated
   decision logic.
4. Run focused tests, randomized generated tests where relevant, pyright, the
   full suite, dead-code checks, pre-push shards, and Nix checks in proportion
   to risk.
5. Fetch current `main` before handoff. Integrate it semantically if it
   advanced, then rerun affected gates.
6. Push signed commits and report the exact head SHA, base SHA, gates, PR URL,
   reference audit, and clean worktree status.

The implementation report is evidence, not a review verdict.

## 4. Audit as the orchestrator

While implementation runs, independently understand the code path and prepare
the adversarial review brief. After handoff:

1. Fetch the branch and confirm the reported exact SHA.
2. Inspect `git status`, signatures, `git diff --check`, the full diff, and
   current-main compatibility.
3. Trace every remaining caller, import, patch target, wire field, and
   persistence writer affected by the change.
4. Run a small set of high-value focused tests yourself.
5. Compare the diff to the coverage ledger. Do not allow a green suite to
   substitute for an unimplemented issue item.

Pay special attention to:

- concern boundaries that point backward after an extraction;
- tests that claim "every writer" but omit one writer;
- generated tests that patch owned orchestration instead of leaf seams;
- fakes that pre-filter before the predicate supposedly under test;
- aggregate static analysis masking file-local unused imports;
- exact error-type and explicit-null behavior at wire boundaries;
- broad `Any`, `Callable[..., ...]`, `**kwargs`, or mock seams that let tests
  express behavior production cannot;
- documentation or compatibility exports that preserve the old owner.

## 5. Run an independent review loop

Only after a pushed implementation head exists, create a separate review
agent. Make the review read-only and give it:

- the exact PR head SHA and current base;
- the assigned coverage-ledger items;
- the product and ownership invariants at risk;
- known architectural seams to challenge;
- required adversarial tests and current-main merge audit;
- an explicit ban on editing, committing, pushing, merging, or deploying.

Require one of two outputs:

- `CLEAN`, tied to the exact reviewed SHA; or
- severity-ranked findings with file/line, concrete failure mode, and a
  specific correction.

Require the reviewer to finish one full exploratory pass and return a
consolidated final report. Treat interim findings as evidence, not as a
handoff: do not begin corrections while the reviewer is still searching.
Batching the complete pass prevents one finding from triggering a commit,
full gate run, and push before an equivalent bypass arrives.

When review is not clean:

1. Send the complete finding batch back to the implementation agent. Do not
   let the implementer declare itself clean.
2. During correction rounds, run focused tests and the smallest relevant fuzz
   or real-service qualification. Do not run the full suite, Nix gates,
   guarded push, or other release-grade gates after each finding.
3. When the batch is fixed and the worktree is quiescent, let the same reviewer
   make a read-only preflight pass over the uncommitted correction. The
   implementation agent must not edit concurrently with this pass.
4. Repeat the focused correction/preflight loop until the reviewer reports no
   provisional findings.
5. Only then create the signed correction commit, run the full required gates,
   update the PR body, push, and report the exact new SHA.
6. Send that exact pushed SHA to the reviewer for the binding `CLEAN` verdict.

If the PR reaches a third substantial correction round, rotate both roles at
the round boundary instead of extending already-long agent contexts. Do not
interrupt active edits. Give the fresh implementation agent and fresh reviewer
a compact durable handoff containing the worktree, base/head SHAs, coverage
ledger, consolidated finding history, current diff, and focused evidence.
They must rebuild understanding from repository and PR artifacts rather than
from a conversation transcript.

Do not merge on CI, mergeability, or the orchestrator's opinion alone.

## 6. Merge and advance deliberately

Before merging:

1. Confirm the reviewed SHA still equals the remote PR head.
2. Confirm current `main` is incorporated or semantically compatible.
3. Confirm signed commits, clean diff, required gates, and coverage-ledger
   completion for this PR.
4. Audit the PR body and every branch commit message before merge. Feed each
   surface through
   `nix-shell --run "python3 scripts/audit_issue_references.py"`; both must be
   clean. For example, pipe `gh pr view --json body --jq .body` and the
   base-to-head `git log --format=%B` output into separate invocations.
5. Merge with GitHub's merge-commit method and an expected-head guard.

After merging:

- record the merge SHA;
- verify the issue remains open (PR references must never decide closure);
- mark only the actually completed ledger items complete;
- create the next PR's fresh worktree from the new `origin/main`;
- remove no worktree until it is clean and no agent or operator needs it.

## 7. Prove the complete issue before deployment

After the final PR merges:

1. Re-read the original issue and comments line by line against the coverage
   ledger.
2. Search for stale old-owner imports, hardcoded parallel taxonomies,
   compatibility exports, TODOs, and deferred issue items.
3. Confirm the final combined tree passed a full suite after the latest
   concurrent `main` changes. A PR head whose tree equals the final merge is
   acceptable evidence; say why.
4. Comment on the issue with the PR-to-workstream map and verification summary.

Do not close the issue yet. Deployment, live evidence, and a signed release tag
are all part of done for a deploy-worthy Cratedigger series.

## 8. Deploy through the current control plane

Load the repository deploy skill, then independently read the current
downstream `nixosconfig` instructions. The downstream control-plane contract
wins if the two disagree.

For the current fleet topology:

1. Fast-forward clean `~/nixosconfig` on doc1.
2. Update only `cratedigger-src`, inspect the lock diff, create an SSH-signed
   commit, and push it to Forgejo without exposing the token.
3. Trigger doc2 from doc1 with `fleet-deploy doc2`. Do not bypass the
   forced-command boundary with direct host-side `fleet-update`.
4. Poll `nixos-upgrade.service` using `ActiveState`, `SubState`, and `Result`.
5. Verify dependent workers and the migration unit.
6. Resolve the exact active source store path from a running service wrapper or
   process command line. Do not accept a grep hit from an arbitrary old store
   closure.
7. Exercise the shipped feature through its real CLI/API/live path.
8. Because the main cycle has `restartIfChanged=false`, wait for the protected
   pre-switch cycle to finish and verify a post-switch successor cycle runs the
   new source before tagging.

If any documented deployment command conflicts with current downstream
instructions, stop, reconcile the contract, and record the discrepancy rather
than silently choosing the more permissive path.

## 9. Tag, close, and reflect

The release sequence is strict; do not reorder it:

1. Confirm deployment and post-switch successor cycle evidence from section 8.
2. On the exact final merge SHA in a clean worktree, run the full suite and
   retain its collision-free artifact directory. Verify that artifact against
   the exact final merge with the `verify` subcommand of
   `scripts/test_artifact.py`.
3. Create an SSH-signed `vYYYY.MM.DD` tag, using `-N` for another same-day
   release, at that exact final merge SHA.
4. Export the verified artifact path as `CRATEDIGGER_TEST_ARTIFACT` and push
   the signed tag exactly once with that environment variable set, so the
   pre-push gate verifies the peeled tag commit against the exact-final-HEAD
   suite evidence.
5. Confirm the remote tag's peeled commit and signature.
6. Only after the signed tag push succeeds, add the deploy, live successor,
   artifact, and tag evidence to the issue and close it as completed.
7. Perform the mandatory post-ship reflection:
   - findings reviewers caught manually;
   - fixes repeated across PRs;
   - boilerplate or signature duplication introduced by the series;
   - aggregate audits that masked file-local defects;
   - workflow or deploy instructions that proved stale.
8. Search all open issue bodies, not only titles. File one ranked covering issue
   with suggested PR grouping if new debt clears the bar; otherwise state that
   nothing does.

## 10. Communicate without losing the thread

- After dispatch, let implementation and review agents run autonomously. Do
  not send reassurance pings, request interim status, or list agents merely
  because time has passed.
- When no independent orchestrator work remains, wait for an agent blocker or
  completion notification for up to 15 minutes. If the wait times out, check
  status once and begin another 15-minute wait. Never approximate this cadence
  with repeated shorter polls.
- Do not send the operator routine updates that only say an agent or long gate
  is still running. Report material milestones, blockers that need a decision,
  corrections to prior claims, and completed handoffs.
- Correct premature status claims immediately when live process evidence
  contradicts them.
- Keep the final response self-contained: merged PRs, final merge, deploy pin,
  live evidence, tag, issue closure, and reflection issue.
- Lead with the outcome. Mention failed attempts only when they affect trust or
  explain the final verification path.

The orchestrator remains accountable throughout. Agents keep execution detail
bounded; they do not replace the orchestrator's understanding or authority.
