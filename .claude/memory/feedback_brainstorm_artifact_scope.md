---
name: feedback-brainstorm-artifact-scope
description: "When user scopes a brainstorm to \"the artifact itself\" (e.g. an API), don't fish for downstream consumers — they're explicitly out of frame"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 740dc87f-bc63-476a-b5b2-3cb425f27455
---

When the user opens a brainstorm by describing an artifact (an API, a service, a function) and says it's "for lots of things" or "complex", don't probe Phase 1.2's specificity/evidence/counterfactual lenses at the *consumer* level. The downstream uses are deliberately out of frame — they want to design the artifact's own contract, not justify its existence via use cases.

**Why:** On 2026-05-27 the user opened a brainstorm for an MBID/Discogs → YouTube playlist API. The first probe asked where the link would surface (preview buttons, library enrichment, etc.) and what triggered the want. The reply: "this is all out of scope. it's going to be for lots of things but we don't care about those now." Phase 1.2's lenses pointed at consumers — the user wanted them pointed at the artifact (URL shape, matching strategy, caching, failure semantics).

**How to apply:** When the opening describes an artifact and the user signals "many consumers downstream", route Phase 1.2 lenses inward — apply the attachment lens to the artifact's *output shape*, not its *purpose*. Probe the contract: what does the URL/response look like, what's the input variance, what counts as success, what's persisted. The "why does this exist" frame is the wrong axis when the user has already decided it should.
