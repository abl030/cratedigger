---
name: feedback_never_defer_work
description: "Never defer related fixes to follow-up issues; if a bug touches multiple surfaces, fix all of them in the same PR"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b3b7d4aa-ded4-43ab-8a4b-f9fa0e2a80ae
---

When a bug analysis surfaces a secondary issue (e.g. a misleading UI label that contributed to operator confusion, a small adjacent cleanup), don't propose to split it into a follow-up issue. Include it in the same brainstorm / plan / PR.

**Why:** Follow-ups get forgotten or starved. The user explicitly said "never defer work" when asked whether the secondary UI bug in #268 should be split out. The cost of doing both at once is small; the cost of leaving a known papercut on the floor is larger.

**How to apply:** During brainstorm scoping, do not ask "should we defer X to a separate PR?" If X is small and named in the issue, scope it in. Related: [[feedback_finish_the_job]] — same family of "wire it all up, ship it complete."
