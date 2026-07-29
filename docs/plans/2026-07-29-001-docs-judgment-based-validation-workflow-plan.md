---
title: Judgment-Based Validation Workflow - Plan
type: docs
date: 2026-07-29
topic: judgment-based-validation-workflow
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-brainstorm
execution: code
---

# Judgment-Based Validation Workflow - Plan

## Goal Capsule

- **Objective:** Rewrite issue #919 around a repository-wide, judgment-based development loop without adding another validation stage.
- **Product authority:** The operator chooses one flexible development policy and retains one final receipt-backed confirmation phase before push.
- **Execution profile:** Documentation, shared agent guidance, tracked project memory, one GitHub issue edit, and one supported Codex-memory supersession note. No production code, test runner, receipt helper, CI, or deployment changes.
- **Stop conditions:** Stop if the repository changes would require a new checker, weaken the final clean-tree receipt contract, fork Claude and Codex behavior, or touch the active shared checkout.
- **Tail ownership:** The operator explicitly invoked `/lfg` for this delivery, overriding the repository’s default no-Compound-Engineering instruction for this run only. LFG owns formal review, commit, final clean-tree confirmation, the external corrections in U3, push, PR creation, and PR monitoring. It does not merge, deploy, close issue #919, or clean up the isolated worktree unless separately authorized.
- **Open blockers:** None.

---

## Product Contract

### Summary

Whole-tree tests and Pyright will become ordinary development feedback that agents run whenever engineering judgment says they are useful.
The existing receipt-backed clean-tree checks will remain the single final confirmation before the first push.

### Problem Frame

Issue #919 currently treats late structural failures as evidence that the repository needs a new fast preflight.
That diagnosis inherits a policy which reserves the complete suite for one final committed tree, so checks already present in the suite cannot provide feedback during ordinary convergence.

Two independent runs measured the complete suite at roughly 100 seconds for its test phase.
The proposed preflight would duplicate workflow stages to avoid using an existing affordable feedback loop.

The final-only policy is also repeated across repository instructions, skills, and memory.
Those copies can keep recreating the same ceremony even after one surface is corrected.

### Key Decisions

- **Use judgment throughout development.** (session-settled: user-directed — chosen over prescribed checkpoints or continuous execution: the suite is cheap enough for agents to decide when it adds value.) Governs R1-R3.
- **Keep one final receipt-backed confirmation phase.** (session-settled: user-directed — chosen over removing exact-tree confirmation: flexible development still ends with one hard pre-push phase containing the existing Pyright and test receipts.) Governs R4.
- **Consolidate policy ownership.** (session-settled: user-directed — chosen over surgical duplicated wording or deleting nearly all guidance: each durable rule should have one authoritative home.) Governs R5.
- **Rewrite issue #919 in place.** (session-settled: user-directed — chosen over closing or replacing the issue: the existing issue should preserve the corrected diagnosis.) Governs R6.
- **Remove stale cadence memory while preserving valid facts.** (session-settled: user-approved — chosen over retaining final-only guidance or deleting useful Nix and CI facts: memory must stop resurrecting obsolete choreography.) Governs R7.
- **Protect concurrent work.** (session-settled: user-directed — chosen over committing from the shared checkout: unrelated active work must remain untouched.) Governs R8.

### Requirements

**Development feedback**

- R1. Repository guidance must present focused tests, whole-tree Pyright, the complete deterministic suite, and relevant surface-specific checks as tools available throughout development.
- R2. Agents must choose validation timing and depth from the change, current evidence, and concrete risk rather than a mandated checkpoint schedule; a green complete suite does not replace generated, live-boundary, browser, corpus, VM, or other evidence when the change makes those useful.
- R3. Failures found by whole-tree checks during development must join the current convergence loop instead of triggering a special finalization restart.
- R10. The generic judgment-based cadence must not weaken existing change-specific validation obligations such as test-first development, generated pin/property pairs, fuzz bursts, VM checks, real-beets drift checks, UI screenshots, or live-corpus differentials.

**Push readiness**

- R4. The reviewed committed tree must still pass the existing receipt-backed whole-tree Pyright and complete-suite checks once before its first push.

**Authority and durable guidance**

- R5. General validation policy, final receipt procedure, issue orchestration, and memory must each own only their distinct concern and must not restate a shared cadence.
- R6. Issue #919 must be rewritten from “add a fast structural preflight” to “remove final-only development choreography and use existing whole-tree checks when useful.”
- R7. Dedicated final-only memory must be deleted, incidental cadence text must be removed from otherwise-valid memory, and client-local guidance must explicitly supersede the preflight recommendation.

**Delivery isolation**

- R8. Every repository read/write operation that can affect worktree state, including implementation, review, commit, and final gates, must use the isolated issue #919 worktree created from current `origin/main`.
- R9. GitHub issue and client-local memory mutations must resolve and verify their exact targets independently and must not alter the active shared checkout.

### Key Flows

- F1. Development convergence
  - **Trigger:** An agent is implementing or correcting a change.
  - **Steps:** The agent selects focused or whole-tree validation according to current risk, lets useful runs expose the failure set, and fixes those failures in the same convergence loop.
  - **Outcome:** Cross-tree regressions can be found early without a prescribed cadence or extra preflight.
  - **Covered by:** R1-R3

- F2. Final push confirmation
  - **Trigger:** The change is reviewed, committed, and ready for its first push.
  - **Steps:** The existing receipt-backed whole-tree checks run against the same clean committed HEAD. An `exact-active` receipt is recovered rather than duplicated; `fail` or `incomplete` blocks push; any change before the first push invalidates the old pair and returns the tree to convergence; matching `pass` receipts for an unchanged tree are not replayed.
  - **Outcome:** Push readiness retains one recoverable exact-tree confirmation phase without turning intermediate whole-tree runs into final authority.
  - **Covered by:** R4

- F3. Policy retrieval
  - **Trigger:** An agent or future session consults repository instructions, orchestration guidance, or memory.
  - **Steps:** It finds the general principle in one repository authority, final mechanics in the final-check authority, and no stale final-only cadence in memory.
  - **Outcome:** The old preflight proposal and final-only development loop do not reappear through duplicated guidance.
  - **Covered by:** R5-R7

- F4. GitHub issue correction
  - **Trigger:** The canonical repository wording is settled locally.
  - **Steps:** Resolve issue #919 by repository and number, preserve the original PR #918 structural-failure evidence, rewrite its title/body around the corrected diagnosis, and read it back.
  - **Outcome:** The same open issue no longer recommends a generic preflight and instead defines the authority-consolidation work.
  - **Covered by:** R6, R9

- F5. Codex-native memory supersession
  - **Trigger:** The repository wording and issue correction are stable.
  - **Steps:** Add one supported timestamped ad-hoc note naming the obsolete final-only/preflight guidance and pointing back to repository authority; do not edit the generated native memory registry or rollout summaries.
  - **Outcome:** Future Codex memory synthesis can supersede the stale recommendation without a client-specific policy fork.
  - **Covered by:** R7, R9

### Acceptance Examples

- AE1. **Covers R1-R3.** Given a structural edit that moves production code, when an agent judges the complete suite useful before review, then it may run the underlying Nix-shell suite command directly, fix Vulture or write-audit fallout, and continue converging without entering the final receipt sequence; that intermediate pass does not satisfy R4.
- AE2. **Covers R2.** Given a narrow change with convincing focused coverage, when broader validation would add no current information, then repository policy does not mandate an intermediate full-suite checkpoint.
- AE2a. **Covers R1-R2.** Given a UI, generated-policy, systemd, or live-integration change, when the complete suite is green but a surface-specific check addresses a remaining risk, then the agent may select that check without treating the complete suite as universal proof.
- AE3. **Covers R4.** Given a reviewed committed tree ready for its first push, when final validation begins, then both receipt-backed checks must pass on the same clean HEAD; `exact-active` is recovered, `fail`/`incomplete` block push, a changed pre-push tree requires a new pair, and an unchanged matching pass is not replayed.
- AE4. **Covers R5-R7.** Given a future agent investigating issue #919 or test cadence, when it reads repository guidance and memory, then it finds judgment-based development, one final confirmation, and no recommendation to add a structural preflight.
- AE5. **Covers R8-R9.** Given unrelated activity in the shared checkout, when repository or external-state changes are made, then repository operations occur only in the isolated issue #919 worktree and the shared checkout snapshot remains unchanged.
- AE6. **Covers R6, R9.** Given issue #919 before and after the edit, when its state is read back, then repository, number, URL, and open state are unchanged while the title, diagnosis, and acceptance criteria match the corrected scope.
- AE7. **Covers R7, R9.** Given stale Codex-native guidance, when the memory correction is delivered, then exactly one ad-hoc extension note names the obsolete claim while native `MEMORY.md` and rollout summaries remain unmodified.
- AE8. **Covers R10.** Given a change with a mandatory surface-specific contract, when an agent chooses the general validation cadence, then the specialized contract still runs and the complete suite is not treated as a substitute.

### Scope Boundaries

- No new preflight command, scanner, validation mode, continuous runner, or mandatory intermediate checkpoint.
- No new policy-enforcement audit; wording and existing portability/issue-reference checks are sufficient for this documentation change.
- No changes to test discovery, worker counts, suite semantics, CI coverage, or the final receipt implementation.
- No weakening of the existing clean-tree and terminal-result requirements for the final pre-push checks.
- No deletion of still-valid memory about Nix-shell execution, missing CI test coverage, or detached-session recovery.
- No weakening of mandatory change-specific validation contracts.
- No edits to `AGENTS.md`, `.agents/skills`, generated Codex adapters, or the active shared checkout; the existing shared-source symlinks provide client parity.
- No merge, deployment, issue closure, or broad historical-memory cleanup.

### Dependencies and Assumptions

- Measurement on 2026-07-29 at `e6d703978a5e82efe6c3e120d23bd3649460a2b8` found the complete suite affordable as ordinary feedback: the operator observed 7,819 tests in 106.7 seconds, and an independent local run observed 7,747 tests in 97.0 seconds (116.5 seconds wall time). These measurements justify removing the avoidance policy but are not a fixed performance requirement; future agents still use judgment if timing changes.
- CI still does not replace agent-owned local validation.
- The existing final receipt helper continues to provide recoverable pass, fail, active, and incomplete outcomes.

### Sources and Research

- GitHub issue #919, “Add a fast structural preflight before final-gate receipts.”
- `CLAUDE.md` testing policy.
- `.claude/rules/code-quality.md` review and commit policy.
- `.claude/skills/check/SKILL.md` final receipt contract.
- `.claude/skills/orchestrate-issue/SKILL.md` issue-convergence guidance.
- `README.md`, `.claude/rules/deploy.md`, `.claude/skills/deploy/SKILL.md`, and the living security audit as current cadence consumers.
- `scripts/run_tests.sh` complete-suite composition.
- `scripts/run_final_gate.sh` clean-tree receipt enforcement.
- `.claude/memory/feedback_focused_then_final_suite.md` and related memory entries carrying the final-only cadence.

**Product Contract preservation:** The session-settled decisions are unchanged by planning enrichment; R8 was narrowed to repository mutations, R9 was added for external targets that cannot live inside a Git worktree, and R10 makes explicit that the generic cadence change does not weaken specialized validation contracts.

---

## Planning Contract

### Key Technical Decisions

- **KTD1 — `CLAUDE.md` owns the general development policy.** Its “Running tests” section will say that agents choose focused, whole-tree, and surface-specific validation by judgment throughout convergence. `README.md`, `.claude/rules/code-quality.md`, `.claude/rules/deploy.md`, the deploy/orchestration skills, and living documentation may retain their distinct public, quality, deployment, lifecycle, or historical concerns but will not prescribe a duplicate focused-then-final cadence. This keeps the policy visible to both clients through the existing `AGENTS.md` symlink while leaving code-quality responsible for testing technique rather than timing. Supports R1-R5.
- **KTD2 — `check` owns only final confirmation mechanics.** `.claude/skills/check/SKILL.md` will retain clean committed-tree requirements, the two receipt-backed commands, receipt recovery states, exclusive-worktree ownership, failure handling, and no-replay behavior for an unchanged pushed tree. It will not imply that whole-tree checks are unavailable earlier without receipts. Supports R3-R5.
- **KTD3 — Memory records facts and lessons, not current workflow authority.** Delete the dedicated final-only memory and index entry; remove incidental cadence from otherwise-valid Nix-shell, CI, Pyright, and TDD memories while preserving their durable facts. The Codex-native cache is corrected now through the supported ad-hoc extension-note mechanism because the operator directly superseded the old recommendation in this session; the note records that user authority and the tracked issue/PR state without pretending the repository change is already merged. Supports R5, R7.
- **KTD4 — External mutations are identity-checked side effects.** Read issue #919 immediately before and after editing it, preserve its repository, number, URL, and open state, and change only its title/body. Create exactly one timestamped native-memory note at the documented extension path; never edit the native memory registry or rollout summaries directly. Supports R6-R7, R9.
- **KTD5 — Simplification must not erase risk-specific evidence.** “No preflight” means no new generic repository stage for issue #919. It does not prohibit generated tests, live corpus comparisons, screenshots, VM checks, rendered-unit inspection, or other targeted checks selected because a change exposes that risk, and it does not make existing mandatory surface contracts optional. Supports R1-R3, R10.

### Implementation Approach

1. Establish the canonical, concise judgment-based wording in `CLAUDE.md`.
2. Remove duplicate cadence from rule and skill surfaces while preserving their distinct technical contracts.
3. Surgically clean tracked project memory, using a repository-wide phrase search to catch direct contradictions without broad historical rewriting.
4. During U1/U2 convergence, select and time focused, whole-tree, and structural checks by judgment; before formal review, ensure the focused evidence required by the Verification Contract is green and inspect the complete diff.
5. Return the finished U1/U2 tree to the LFG tail. LFG performs formal review, commits the repository tree, and runs the existing receipt-backed final confirmation from the isolated worktree.
6. After both final receipts pass, LFG performs U3: rewrite issue #919, add the one authorized native-memory supersession note, read both targets back, verify the shared-checkout baseline, then push, open the PR, and watch its checks.

### Authority Map

- General judgment-based validation policy: `CLAUDE.md` → “Running tests.”
- Public contributor summary: `README.md` → “Development,” linked in meaning rather than duplicating a schedule.
- Testing invariants and quality techniques: `.claude/rules/code-quality.md`.
- Final pre-push receipt mechanics: `.claude/skills/check/SKILL.md` and `scripts/run_final_gate.sh`.
- Issue lifecycle and orchestration accountability: `.claude/skills/orchestrate-issue/SKILL.md`.
- Deployment sequencing policy: `.claude/rules/deploy.md`; `.claude/skills/deploy/SKILL.md` operationalizes that policy and remains unchanged.
- Historical facts and recall hints: `.claude/memory/` plus the supported Codex native-memory extension mechanism.

### Constraints and Sequencing

- U1 precedes U2 so memory cleanup can point at stable repository authority.
- U1 and U2 are reviewed, committed, and final-confirmed together before U3 mutates external state.
- Repository changes are one logical documentation commit unless review exposes a genuinely independent correction.
- External issue and native-memory changes are not smuggled into the Git commit; their read-back evidence is recorded in the PR description or handoff.
- Existing final-gate scripts and receipt tests are treated as unchanged dependencies, not implementation targets.

### Shared Checkout Baseline

The active shared checkout was captured read-only before plan implementation:

- branch `fix/898-pr1-rmw-fence`;
- HEAD `3f88098b5dfa77747b911325d92ce1faf6ed1429`;
- no staged or unstaged tracked diff (both SHA-256 values `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`);
- untracked `docs/plans/2026-07-28-002-fix-web-authentication-perimeter-plan.md` at SHA-256 `8cb30e3a844ea5f5ed9dffc79a62d894b07325896c6a612bd84024a6a13c4533`;
- untracked `u5-insecure-desktop-a11y.md` at SHA-256 `9f5eb42ff0c93a17bf3b60112b75d607116b3dc6b77931f1084927f0ccd982a1`.

U3 repeats the same branch/HEAD/status/diff-hash/untracked-file-hash manifest and fails closed on any difference. It never cleans, resets, stages, or rewrites the shared checkout.

---

## Implementation Units

### U1. Consolidate repository validation authority

- **Goal:** Make judgment-based development the one shared policy while retaining exact final mechanics in their proper owner.
- **Requirements:** R1-R5, R8, R10
- **Flows / examples:** F1-F3; AE1-AE3, AE5, AE8
- **Decisions:** KTD1, KTD2, KTD5
- **Files:**
  - `CLAUDE.md`
  - `README.md`
  - `.claude/rules/code-quality.md`
  - `.claude/rules/deploy.md`
  - `.claude/skills/check/SKILL.md`
  - `.claude/skills/orchestrate-issue/SKILL.md`
  - `docs/security-audit-2026-07-12.md`
- **Approach:**
  - Rewrite “Running tests” around engineering judgment: agents may use focused tests, whole-tree Pyright, the complete suite, and relevant specialized evidence whenever useful during development.
  - Reduce the README development section and living security-audit sentence to factual public/CI context without an independent checkpoint schedule.
  - Preserve one final clean committed-tree confirmation phase before first push, expressed as a reference to the `check` skill rather than duplicated command choreography.
  - Remove final-only or “restart convergence/review” prescriptions from code-quality, deploy, and orchestration prose.
  - Keep whole-repository Pyright scope, generated-test obligations, review quality, deployment constraints, receipt recovery, and unchanged-tree no-replay semantics intact.
  - Make the `check` description and opening explicit that the skill wraps final receipts; the underlying checks remain valid ordinary feedback when run directly.
  - Leave `.claude/skills/deploy/SKILL.md` unchanged as a thin operational consumer of the `check` skill.
- **Test scenarios:**
  - A reader can tell that whole-tree checks are available during development and that their timing is judgment-based.
  - A reader can find final receipt commands and recovery states in `check` without finding a second general cadence policy there.
  - A targeted surface-specific check remains clearly permitted even after the complete suite passes.
  - `AGENTS.md` and `.agents/skills` still resolve through their shared authored sources.
- **Verification:**
  - Inspect symlink targets with `readlink`.
  - Run `tests.test_ai_portability`, `tests.test_docs_audit`, and `tests.test_issue_reference_contract`.
  - Search normative surfaces for stale “focused then final,” “exactly once during development,” generic preflight, and mandatory restart wording.

### U2. Remove stale tracked-memory choreography

- **Goal:** Prevent shared recall from resurrecting final-only iteration while preserving still-valid operational facts.
- **Requirements:** R5, R7-R8
- **Flows / examples:** F3; AE4-AE5
- **Decisions:** KTD3
- **Files:**
  - Delete `.claude/memory/feedback_focused_then_final_suite.md`.
  - Remove its entry from `.claude/memory/MEMORY.md`.
  - Edit `.claude/memory/feedback_use_nix_shell.md`.
  - Edit `.claude/memory/project_ci_only_gitguardian.md`.
  - Edit `.claude/memory/feedback_pyright_full_repo.md`.
  - Edit `.claude/memory/feedback_tdd.md` only to replace its mandatory per-step complete-suite checkpoint with relevant-test verification.
  - Edit `.claude/memory/feedback_no_draft_prs.md` only to remove final-only cadence from the rationale while retaining the no-draft preference.
- **Approach:**
  - Remove cadence sentences rather than rewriting valid histories.
  - Preserve Nix-shell as the mandatory Python environment, CI’s GitGuardian-only limitation, whole-repository Pyright scope, the no-skips policy, receipt recovery facts, and strict test-first intent.
  - Update memory descriptions/index summaries only where they themselves carry the obsolete cadence.
- **Test scenarios:**
  - A search of active project memory finds no dedicated final-only development instruction.
  - Nix-shell, missing-CI, whole-repo-Pyright, and TDD facts remain discoverable and materially unchanged.
  - Historical references to real completed gates remain historical rather than being mass-edited.
- **Verification:**
  - Review the deletion and targeted diffs.
  - Run repository memory searches for cadence phrases and inspect every remaining match in context.
  - Let the full suite’s documentation and structural checks validate the resulting tree.

### U3. Correct issue #919 and Codex-native recall

- **Goal:** Align external issue scope and future Codex recall with the canonical repository policy.
- **Requirements:** R6-R7, R9
- **Flows / examples:** F4-F5; AE4, AE6-AE7
- **Dependencies:** U1 and U2 reviewed, committed, and passing both final receipts.
- **Decisions:** KTD3, KTD4
- **Targets:**
  - GitHub issue `abl030/cratedigger#919`.
  - One new timestamped note under `/home/abl030/.codex/memories/extensions/ad_hoc/notes/`.
- **Approach:**
  - Rewrite the issue title around judgment-based validation and removal of final-only choreography.
  - Preserve the useful PR #918 failure evidence, but correct the diagnosis: the existing complete suite is affordable ordinary feedback, so a new generic structural preflight is unnecessary.
  - Replace acceptance criteria with canonical policy consolidation, stale-memory cleanup, unchanged final receipt mechanics, and no new command/scanner.
  - Keep the issue open and preserve its identity.
  - Add one small native-memory supersession note requesting deletion/supersession of final-only and issue-919-preflight guidance while retaining valid receipt, Nix-shell, CI, and detached-session facts. State that the user superseded the recommendation in this session and identify the pending issue/PR rather than claiming the repository policy is already merged.
- **Test scenarios:**
  - Read-back shows issue number 919, URL, repository, and open state unchanged with corrected title/body.
  - The native-memory change is an additive extension note only; the registry and rollout files are untouched.
  - Neither external mutation changes the active shared checkout.
- **Verification:**
  - Use `gh issue view 919 --repo abl030/cratedigger --json number,title,state,url,body`.
  - Inspect the new note path and content without scanning or rewriting unrelated native-memory files.
  - Recompute and compare the shared checkout branch, HEAD, porcelain status, tracked-diff hashes, and untracked-file hashes with the recorded baseline.

---

## Verification Contract

### Focused validation

Run from the isolated worktree:

```bash
nix-shell --run "python3 -m unittest tests.test_ai_portability tests.test_docs_audit tests.test_issue_reference_contract -v"
```

Use direct text and symlink inspection to confirm:

- `AGENTS.md` still resolves to `CLAUDE.md`.
- `.agents/skills` still resolves to `.claude/skills`.
- no normative repository or active project-memory surface recommends a generic issue #919 preflight or reserves whole-tree checks exclusively for finalization;
- the final receipt helper, test runner, hooks, CI configuration, generated adapters, and production code have no diff.

### Judgment-based development validation

Run additional checks, including the existing underlying whole-tree commands, whenever the current diff or review makes them useful:

```bash
nix-shell --run "pyright --threads 4"
nix-shell --run "bash scripts/run_tests.sh"
```

These are ordinary development feedback when invoked directly. They do not mint final receipts or satisfy R4. This is deliberately not a checkpoint schedule, and all mandatory surface-specific checks still apply.

### Final pre-push confirmation

After review is complete and the repository tree is committed and clean, run once for that changed tree:

```bash
scripts/run_final_gate.sh pyright
scripts/run_final_gate.sh tests
```

Both terminal receipts must be `pass` for the same committed tree before its first push. Recover an `exact-active` receipt rather than minting a duplicate; `fail` and `incomplete` both block push and return the change to ordinary convergence. If the tree changes, fix and recommit, re-review in proportion to the correction, and run the final phase for the new tree. Do not replay a passing receipt for an unchanged pushed tree.

### External read-back

- Read back issue #919 and verify corrected content plus unchanged identity/open state.
- Verify exactly one native-memory extension note was added and no native registry or rollout file was edited.
- Verify the active shared checkout still matches the recorded branch/HEAD/status/diff-hash/untracked-file-hash manifest.

---

## Definition of Done

- **U1:** `CLAUDE.md` is the sole general cadence authority; README, quality, deploy, check, orchestration, and living-document surfaces own only their distinct concerns; mandatory surface-specific validation and Claude/Codex shared-source parity remain intact.
- **U2:** Dedicated final-only tracked memory is deleted, incidental stale cadence is removed, and valid Nix-shell, CI, Pyright, TDD, no-draft, and receipt facts remain.
- **U3:** Issue #919 is rewritten in place and remains open; one supported Codex-native supersession note exists; both external targets pass read-back verification.
- Focused portability/reference checks and judgment-selected development checks are green.
- The reviewed committed repository tree passes both existing final receipt-backed gates before its first push.
- The branch is pushed and a ready-for-review PR is open with the measured suite evidence and external side effects described; PR checks reach a terminal green state or LFG returns a concrete watcher handoff.
- No production code, runner, receipt helper, CI, hook, generated adapter, or active shared-checkout change appears in the diff.
- No abandoned policy draft, temporary mutation file, or unrelated work remains in the isolated branch.
