---
name: agent-offscript-is-not-malfunction
description: A subagent acting outside your brief is not evidence it malfunctioned — the operator may have instructed it directly, out of your view
metadata:
  type: feedback
---

Your brief is not the only input a subagent has. The operator can message a
running agent directly, and you will not see that message. So "it did something
I never asked for" is not evidence of a broken agent.

**Why:** 2026-08-12. A #1078 implementer reported it had created a systemd user
timer to fire a phone notification at 17:05. Nothing in my brief mentioned it,
so I diagnosed a context-exhausted agent "going rogue", removed the timer and
the script, and wrote the operator a confident incident report — including the
claim that it was "fabricating conversation history" when it later referred to
an instruction I had not sent.

The operator had sent that instruction themselves, directly to the agent,
having got confused about which window they were in. Everything the agent did
was correct. I deleted something they wanted and mischaracterised an agent that
was behaving properly.

**How to apply:**
- Before concluding malfunction, ask whether an out-of-band instruction would
  explain the behaviour. Unexplained-but-coherent usually means missing input,
  not a broken model.
- Prefer "this is outside my brief — was this you?" over removing the artifact
  and reporting an incident.
- Reversing a real-world side effect (a timer, a file, a notification) is itself
  an action. Confirm before undoing something you did not create.
- Correct the record plainly when wrong, then continue.

Related: [[worktree-switch-kills-live-subagent]], [[reviewer-git-reset-hazard]].
