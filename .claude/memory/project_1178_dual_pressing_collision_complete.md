---
name: 1178-dual-pressing-collision-complete
description: "#1178 COMPLETE 2026-08-18: PRs #1194/#1195 deployed+verified; reject-gate design killed in review (hidden-track cohort), shipped surface-not-reject instead"
metadata:
  type: project
---

Issue #1178 (two pressings, shared download collision, wrong-pressing import) closed 2026-08-18. PR #1194: render-time `track_length_warning` (amber chip + detail row, 60s bound over the persisted `is_target` mapping) — the ORIGINAL validation-time reject gate was fully implemented and then reverted after Opus review proved the >60s cohort is dominated by legitimate hidden-track/medley rips (85% single-pair, last-track; #1178's 222.6s sits below "Eskimo" 653s) and rejection would serially denylist peers offering correct rips. Surface-not-decide won; 124 historical imports are now permanently flagged in Recents (incl. Coil 2.0s-vs-1350s stub). PR #1195: cross-request enqueue guard (per-cycle shared registry + attempt-scoped ledger join, owners 'downloading' only per invariant 10).

**Why:** magnitude-family per-track length discriminators cannot separate wrong-pressing worlds from hidden-track appends — don't re-propose an import-time length gate without new evidence. The operator decision on beets album 19867 (Digital MBID fronting CD audio) was left pending at close.

**How to apply:** #1196 CLOSED 2026-08-18 (PRs #1197/#1198 live: attempt-fingerprint join scoping, conflict forensics marker, composite carve-out); #1199 also CLOSED 2026-08-19 via PR #1201: lib/ zero-neighbour selection now fails closed behind the audit-exact LIB_MODULES_WITHOUT_SELECTION_COVERAGE registry (35 admitted gaps, shrink-only), and the guard SQL is fingerprint-equality-else-fail-closed (clock fallback deleted). Series lesson held to the end: five correction rounds, five false confident sentences, every one caught only by the next independent read.. Orchestration shape that worked: [[opus-for-reviews]] with sonnet implementers, three review rounds, each round's false-docstring claim caught only by the next independent read — the existing no-round-catches-its-own-claims rule earned its keep three times.
