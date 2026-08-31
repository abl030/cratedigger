---
name: 1278-item7-fake-delegation
description: "#1278 item 7 shipped 2026-08-31: FakePipelineDB delegation series (PRs #1289/#1291/#1293), live-verified; extraction-vs-value-parity split"
metadata:
  type: project
---

#1278 item 7 complete 2026-08-31 (close-out: issue #1278 comment 5476015408). Three PRs merged via merge commits: #1289 clean lifts (evidence overlay + dashboard derivations to shared module functions), #1291 decision sub-parts (`lib/pipeline_db/decisions.py`: backoff/cursor-advance/readiness-bucket/saturation + six exported vocabularies), #1293 value-parity axis (`VALUE_PARITY_REGISTRY` in tests/read_projection_registry.py, tests-only). Production surfaces live under the item-8 session's pin 3cd0a874/nixosconfig 80aa21dd; #1293 rides the next behavior-bearing deploy.

**Why it matters later:**
- The item's "extract everything into pure functions" framing fit ~1/3 of the fake's re-derivers. The shipped pattern is a three-way split: clean lift (production already derives post-fetch) / decision sub-part (pure decision inside a SQL transaction; SQL body untouched) / value parity (truth inherently SQL — gate the output, never extract). The do-not-extract list is in #1293's close-out; do not reopen.
- #1291 fixed a latent live-reachable bug found IN the expression being unified: PG `POWER` is double precision, `30*POWER(2,n)` raises overflow at n>=1020 (not caps). Both SQL writers clamp via derived `SEARCH_BACKOFF_MAX_EXPONENT`.
- Six live fake<->production divergences fixed fake-side, incl. JSONB columns projected as `str` where psycopg2 returns `dict` (ten request-row mirrors leaked `active_download_state`).
- The two-role review split earned its cost: 8 mutant survivors across the series (incl. a serializer value-swap invisible to every existing test — the #1110/#1241 class — and a value-parity driver that never proved its "real" backend was real). Any future value-parity/registry addition should get an agree-by-construction probe: swap the real backend for a second fake; something must fail.
- Residual debt ranked in the close-out comment; top two: web/download_history_view.py's now-unproducible `str` import_result arm, and record_unfindable_run_metrics' Rule-A write-constraint mirror gap.
