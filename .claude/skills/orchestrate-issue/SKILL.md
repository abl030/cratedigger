---
name: orchestrate-issue
description: Orchestrate a substantial Cratedigger GitHub issue from scope audit through multiple isolated implementation PRs, independent review loops, merge commits, deployment, live verification, tagging, and closure. Use when the user asks to complete or ship an entire issue, especially when it spans several workstreams or PRs and requires implementation agents to remain separate from review agents. Do not use for a single small patch, diagnosis-only request, or ordinary code review.
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
4. Create a native task plan with explicit dependencies and ownership surfaces.
   Fill available agent slots with independent implementation and review work.
   Serialize only when a concrete dependency, overlapping mutation surface, or
   required merge order makes concurrency unsafe; never serialize by default.
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
5. Create worktrees for independent PRs concurrently from the same current
   `origin/main`. Start a dependent PR only after its prerequisite merges.
   Every concurrent PR must refresh and integrate current `main` locally before
   its final review.

Record the worktree, branch, base SHA, PR number, and current head SHA in the
plan or coverage ledger.

## 3. Delegate implementation with a hard role boundary

Create a fresh implementation agent for each PR. Dispatch as many independent
implementation agents concurrently as available slots safely allow; a ready,
independent workstream must not sit idle behind another PR. Never carry an
implementation agent into the next PR. Give it:

- the exact worktree and branch it may modify;
- the issue items assigned to this PR;
- repository invariants and relevant rules;
- expected tests, docs, generated properties, and live evidence;
- permission to create signed local commits, but an explicit ban on pushing or
  opening the PR until an independent reviewer returns `CLEAN`;
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
4. Run focused tests that help implementation converge. Do not manually invoke
   broad release gates or duplicate the randomized and Nix work owned by the
   repository pre-push hook.
5. Fetch current `main` before handoff. Integrate it semantically if it
   advanced, then rerun only affected focused tests.
6. Report the exact signed local head SHA, base SHA, focused evidence, and
   clean worktree status for independent review. Do not push yet.

The implementation report is evidence, not a review verdict.

## 4. Audit as the orchestrator

While implementation runs, independently understand the code path and prepare
the adversarial review brief. After handoff:

1. Inspect the isolated local branch and confirm the reported exact SHA.
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

Only after a signed local implementation head exists, create a separate review
agent. Make the review read-only and give it:

- the exact local head SHA and current base;
- the assigned coverage-ledger items;
- the product and ownership invariants at risk;
- known architectural seams to challenge;
- required adversarial tests and current-main merge audit;
- an explicit ban on editing, committing, pushing, merging, or deploying.

Start that review as soon as a slot is available while other independent
implementations continue. Review different independent PRs concurrently when
slots allow; do not wait for an arbitrary issue-wide phase boundary.

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
   or real-service qualification. Do not invoke release-grade gates or push
   after each finding.
3. Commit the complete correction batch locally as a signed commit, stop all
   edits, and let the same reviewer inspect that exact local SHA.
4. Repeat the focused correction/review loop until the reviewer returns
   `CLEAN` on the exact local SHA.
5. A reviewer `CLEAN` is final. Do not request a second review, post-push
   review, or another release gate.
6. Push the reviewed SHA with one ordinary branch push through the repository
   pre-push hook, open the non-draft PR, and confirm the remote PR head still
   equals the reviewed SHA. If satisfying a real hook failure changes code,
   return the new local SHA to the reviewer before pushing again.

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
2. Confirm current `main` is an ancestor of the reviewed SHA. If `main`
   advanced, merge it locally, run only affected focused tests, and return the
   new signed local SHA to the reviewer before its one ordinary push.
3. Confirm signed commits, clean diff, successful pre-push hook, and
   coverage-ledger completion for this PR. Do not rerun its checks.
4. Audit the PR body and every branch commit message before merge. Feed each
   surface through
   `nix-shell --run "python3 scripts/audit_issue_references.py"`; both must be
   clean. For example, pipe `gh pr view --json body --jq .body` and the
   base-to-head `git log --format=%B` output into separate invocations.
5. Merge with GitHub's merge-commit method and an expected-head guard.

After merging:

- record the merge SHA;
- verify the merge tree equals the reviewed PR-head tree; stop if it differs;
- verify the issue remains open (PR references must never decide closure);
- mark only the actually completed ledger items complete;
- create the next PR's fresh worktree from the new `origin/main`;
- remove no worktree until it is clean and no agent or operator needs it.

## 7. Record the completed issue before deployment

After the final PR merges:

1. Confirm every coverage-ledger item was included in an independently
   reviewed PR.
2. Confirm every final merge tree equals its independently reviewed PR-head
   tree and that each reviewed head passed the repository pre-push hook.
3. Do not run a post-merge suite, focused-test replay, semantic audit, Nix gate,
   or post-push review. Review plus pre-push is the complete code gate.
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

## 9. Tag and close

The release sequence is strict; do not reorder it:

1. Confirm deployment and post-switch successor cycle evidence from section 8.
2. Create an SSH-signed `vYYYY.MM.DD` tag, using `-N` for another same-day
   release, at the recorded final merge commit.
3. The reviewed commit already passed the hook before merge and the merge tree
   was proven identical. Push the signed tag with `--no-verify`; a tag adds no
   code and must not replay the randomized or Nix gates.
4. Confirm the remote tag's peeled commit and signature.
5. Only after the signed tag push succeeds, add the review, pre-push, deploy,
   live successor, and tag evidence to the issue and close it as completed.

## 10. Communicate without losing the thread

- After dispatch, let implementation and review agents run autonomously. Do
  not send reassurance pings, request interim status, or list agents merely
  because time has passed.
- Keep useful agent slots occupied with ready independent implementation or
  review work. Do not wait for one PR to finish before dispatching another
  unless the dependency map says it must be serial.
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
  live evidence, tag, and issue closure.
- Lead with the outcome. Mention failed attempts only when they affect trust or
  explain the final verification path.

The orchestrator remains accountable throughout. Agents keep execution detail
bounded; they do not replace the orchestrator's understanding or authority.
