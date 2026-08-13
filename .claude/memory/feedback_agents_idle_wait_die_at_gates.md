---
name: agents-idle-wait-die-at-gates
description: Subagents that end their turn to "wait" for a background suite/gate get stopped while idle and their completion notification lands nowhere
metadata:
  type: feedback
---

Subagents repeatedly went dark by launching a long background run (scripts/test.sh, the check-skill suite) and ending their turn to "wait for the completion notification". The agents view stops agents idled for a stretch, so the notification never re-invokes them — the run finishes green but nobody acts on it (2026-08-13, #1122 series: PR2 died at a PASSED gate, PR3 twice at test waits).

**Why:** an idle subagent holds no process between turns; "I'll wait for the notification" is a turn-ending statement, and a stopped agent receives nothing.

**How to apply:** brief every implementer/reviewer subagent with a standing rule: never end the turn while a gate or suite is running — poll instead (`sleep 120` + check output file/exit status in a loop of ordinary tool calls) and act on the result in the same session. As orchestrator, treat a silent agent at a gate as OFF: check `/run/user/1000/cratedigger-final-gate.*` receipts (`repo_root`/`head`/`terminal`) yourself and resume the agent via SendMessage with the receipt facts. Related: [[worktree-switch-kills-live-subagent]], [[never-remove-a-subagents-worktree]].
