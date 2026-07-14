---
name: feedback-review-clean-pre-push-only
description: "For orchestrated issues, parallelize independent PR work; independent review CLEAN is final and one ordinary branch push through pre-push is the only release-grade code gate"
metadata:
  node_type: memory
  type: feedback
---

Keep an orchestrated PR local while implementation and independent review
converge. Use focused tests during that loop. Once the reviewer returns
`CLEAN` on the exact signed local SHA, push it once through the repository
pre-push hook, open the PR, and merge that reviewed SHA.

Map real dependencies and overlapping mutation surfaces, then fill available
agent slots with independent implementations and reviews. Independent PRs may
start concurrently from the same current `origin/main`; each refreshes current
`main` before its final review. Serialize only where the dependency map proves
it necessary, never merely because the work belongs to one issue.

Do not add a binding or post-push review. Do not replay broad test, artifact,
randomized, Nix, or post-merge gates. Verify that the merge tree equals the
reviewed PR-head tree, then proceed to deployment and live verification. Push
the signed release tag with `--no-verify` because the tag changes no code and
the reviewed tree already passed pre-push.

Why: issue #695 took more than 40 minutes from its final correction push to
closure. The same tree saw two complete suite runs, repeated focused checks,
three randomized/Nix pre-push runs, and a post-push binding review. None found
a new defect; one accidental tag retry merely repeated the gates before GitHub
rejected the already-published tag.

How to apply: focused implementation evidence -> signed local head -> one
independent review-until-`CLEAN` loop -> one ordinary branch push -> merge-tree
identity check -> deploy/live verify -> signed tag with `--no-verify` -> close.
