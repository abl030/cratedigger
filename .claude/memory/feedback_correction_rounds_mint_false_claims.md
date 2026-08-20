---
name: correction-rounds-mint-false-claims
description: Every correction round in the #1214 series minted a new false claim in its own prose, caught only by the NEXT independent read — and 5 rounds on one PR meant the design was wrong, not the tests
metadata:
  type: feedback
---

Across four PRs and ~11 correction rounds in the 2026-08-20 #1214/#1156 series,
**every correction round introduced at least one new false claim in its own
comments, docstrings or PR prose**, and in every case it was caught only by the
*next* independent read — never by the round that wrote it.

Concrete instances: a "Mutant proof (both directions)" docstring describing a
mechanism that doesn't happen (the real failure was a 30 s deadlock, not the
asserted assertion failure); "every new guard clause has a self-test" when two
clauses emitted the same message so one was unprovable; the incident called an
"OOM kill" when the issue states explicitly there were no host OOM kills (it was
ENOSPC) — and that false premise was the entire basis of a refusal; "the only
worker formula with no ceiling" when a sibling formula also had none; a fuzz cap
attributed to the wrong issue in four places; "untestable without root" when the
repo already does that exact `unshare --map-root-user` remap in its own fixtures.

**Why:** confidence is highest and scrutiny lowest exactly when writing a
correction. `code-quality.md` § "No round catches its own false claims" is real,
and this series is its strongest evidence.

**Also:** review value does NOT decay across rounds the way you'd expect — the
round-3 and round-4 reviews each found genuine blockers (a green fuzz burst
reported as a gate FAILURE; a `status=valid` receipt over a lost phase).

**How to apply:** brief every correction round to re-read the claims it itself
adds before committing, and commission each review from a *previously
uninvolved* agent. Never let an author self-certify a correction.

**The harder lesson:** when a component needs FIVE rounds and each fix
re-introduces the same class of bug one call site over, the *design* is wrong,
not the tests. The monitor's loss-accounting layer was stripped back to the one
idea that mattered on operator instruction ("just strip back C") — 32 tests -> 21,
and what remained was the part already verified. Reach for simplification after
round 3, not another patch.

See [[1214-daily-gate-enospc]] and [[mutation-testing-pycache-trap]].
