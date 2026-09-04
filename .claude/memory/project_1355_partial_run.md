---
name: project-1355-register-closed
description: "#1355 cold-code register CLOSED 2026-09-04: 20 PRs (#1357-#1377) live-verified as nixosconfig f2b62801 / cratedigger 4ecd9b98; WE7 declined; #1366 feature + #1378 reflection filed; three operator rulings; three-at-a-time cap; nightly rolling bot deploys main"
metadata:
  type: project
---

Issue #1355 (2026-09-02 architecture review, cold-code control) was meta-orchestrated
2026-09-02 to 2026-09-04 and CLOSED with everything shipped and live-verified: the eight
strong items, seven of eight Worth-exploring items (WE7 declined with a measured census),
and five residual-sweep batches (A, C, D, E, F), 20 PRs #1357 through #1377. Deployed as
nixosconfig `f2b62801` / cratedigger `4ecd9b98`, migrate invocation changed, cycle
`5779a68a` verified on `2r06w3ql…-source`. Post-ship reflection is #1378; the
cross-pathway Replace feature the run surfaced is #1366.

Operator rulings from the run, quoted on the issue: Q1 "it should stay stopped. i've marked
in unsearchable because slskd can't find it for some reason." (job-less rejection keeps an
`unsearchable` stop, shipped in #1377); Q2 became #1366 (R4/AE2 of the #282 plan were a
scope boundary, "i wouldn't have been saying we should never ever do this"); Q3 `is_cbr`
stays a bool, unproven CBR is treated as VBR. The quality-core merge hold was lifted for this
register ("no need to hokd merge for this.").

**Why:** what worked and what bit. Sonnet implementers with opus readers and runners was the
operator's token choice and held up; the fable reader on item 1 (quality core) caught a third
precedence site sonnet missed. Three implementers at a time is fine ("we never got jesr five
hour oimitbwith two"). Every correction round on nearly every PR minted a new false claim
caught only by the next read. Three implementers stalled by ending their turn to wait for a
spawned reviewer; one late reader finding never reached its implementer and shipped (#1371,
closed by #1376). A nixosconfig "nix bot" rolling update re-pins `cratedigger-src` to the
`main` tip nightly and doc2 deploys it at 04:00, so merged work goes live unattended before
any end-of-register deploy (#1378 item 2 asks the operator what to do about that).

**How to apply:** de-dupe new architecture findings against #1378 first. Brief boilerplate
(poll-never-wait, relay late reader findings, archive-snapshot runners, no `--edit-last`,
three concurrent) belongs in the orchestrate skills, which #1378 item 1 schedules. Sweep only
worktrees whose agent completion notification arrived. The deploy-gate worktree pattern
(detached at `origin/main`, run the final gate there, remove after) re-gates combined main
without dirtying the shared checkout.
