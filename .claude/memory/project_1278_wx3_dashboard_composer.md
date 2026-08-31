---
name: 1278-wx3-dashboard-composer
description: "#1278 worth-exploring item 3 shipped 2026-08-31: composer takes (navHtml, data, el); JS wiring residue closed; PR #1296 deployed + live-verified"
metadata:
  type: project
---

PR #1296 merged, deployed (nixosconfig pin 0bbbc89a -> cratedigger 82d4b16c), live-verified 2026-08-31: fleet anchor, migrate invocation, source-store grep, verified cycle, and a playwright dashboard smoke (zero console errors, all 14 cards). The pin swept #1293/#1294 (merged-but-undeployed, caught by the deploy drop-detector - see [[live-means-verified-deploy]]) and the parallel #1295.

- `renderPipelineDashboard(navHtml, data, el)`; `pipeline.js::renderDashboard()` is the one production caller. Composition byte-identical old-vs-new over 4 payload worlds (throwaway Node differential importing HEAD's module beside the new one over a shared state.js instance - reusable pattern for JS refactors).
- Composer order + payload-key selection pinned via sentinel-in-slice assertions; six pure-table cards got first assertions free. Wiring residue closed: toggleDetail (incl. acquisition YouTube arm where the two args diverge), openBrowseArtist, loadReleaseGroup (both ternary arms), openLabelDetailFromList (new tests/test_js_labels.mjs).
- 36 mutants (10 author + 26 runner), all killed. Both correction rounds minted exactly one false claim each; caught only by the next independent read.
- Test-design lesson: a fixture pre-seeding the exact fields whose absence drives a derivation short-circuits it out of the test (pre-seeded `matches_per_hour_*` let a delete-`withCoverageMatchRates` mutant survive until the reader named it).
- Residuals on #1278 comment 5476726495: no jsconfig so `@ts-check` lacks strictNullChecks; label-detail flow untested; `renderDashboard()` caller unasserted.
- Shared `.claude/memory/` copy NOT written (bg-session isolation guard); durable content lives in the issue comment + PR body.
