---
name: project-282-discogs-replace
description: COMPLETE 2026-07-04 — Discogs-pathway Replace shipped (PR #499), deployed, live-verified on doc2, tagged v2026.07.04-2, issue closed
metadata: 
  node_type: memory
  type: project
  originSessionId: 4b02a849-f0da-41d7-a281-f1c4de986e09
---

Issue #282 (Discogs-pathway Replace) implemented 2026-07-04 on branch `feat/discogs-pathway-replace`, PR https://github.com/abl030/cratedigger/pull/499 (plan: `docs/plans/2026-07-04-001-feat-discogs-pathway-replace-plan.md`).

Key facts worth keeping:
- Master anchor lives in `mb_release_group_id` (numeric Discogs master id) — established convention, no new column; legacy NULL-master rows lazy-resolve via the service's Discogs backfill arm or resolve-rg.
- Supersede dual-writes `mb_release_id` + `discogs_release_id`; the `mb_release_id` UNIQUE constraint is the concurrent-replace collision net.
- New outcome `mirror_unconfigured` → HTTP 503 / CLI exit 5.
- Deliberately deferred: cross-pathway replace (operator decision, delete-and-re-add covers it), inverted picker mode for Discogs, beets-distance badges on Discogs siblings (route is UUID-anchored — silent no-render is documented, not a bug).
- U6 live smoke DONE 2026-07-04: request 1870 (Susperia-Electrica) replaced into 8818 via `pipeline-cli replace` — dual identity, master 812637 carried, `replaces_request_id` back-link, search plan generated; masterless rejection verified on 1708 ("no master; nothing to swap to", exit 3, no mutation). Deployed via fleet-update, tagged `v2026.07.04-2`. Picker data pathway verified read-only against prod; browser DOM smoke deferred.
- Playwright-on-doc1 FIXED in PR #500 (2026-07-04): `scripts/mcp-playwright.sh` exec'd the wrong binary name (`mcp-server-playwright` npm vs nixpkgs `playwright-mcp`); wrapper now resolves either, headless auto-detect works, browsers come from the nix store (`PLAYWRIGHT_BROWSERS_PATH` — never `playwright install`). Stack verified via stdio initialize + headless navigate to music.ablz.au. Remaining one-time steps: copy `.mcp.json` from the tier2-packaging worktree to the primary checkout, and approve the playwright MCP server in the `claude` TUI (harness holds new MCP servers at "Pending approval"; agent-frontmatter mcpServers are gated by the same approval). Then re-run the picker DOM smoke.
- Pre-existing quirk found during review (out of scope, unfixed): `web/js/long_tail.js` sibling panel fires a doomed MB fetch for Discogs rows once their master is populated — graceful error, but contradicts its own docstring.
