---
name: ui-screenshot-loop
description: UI changes are verified with the live-db dev-server screenshot loop BEFORE pushing — photos of real data catch what tests+review miss
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2f201277-ea14-4a12-96d9-f567f6debb65
---

For any visible web-UI change, run the dev-server screenshot loop before pushing: PG tunnel → `scripts/web_dev_server.py --data live-db` on :8096 (with `--beets-db`) → CDP chromium (store binary, port 9222; playwright wrapper auto-attaches) → playwright agent screenshots in rounds → **the main agent Reads the PNGs itself** → fix → re-round. Full recipe + gotchas: `docs/solutions/ui-dev-server-screenshot-loop.md` (repo) — also pointed to from `.claude/rules/web.md`.

**Why:** on #575 PR2 the loop caught, in three rounds, defects that a green 5,153-test suite AND a clean Opus review both let through: a permanently-stuck `class="loading"` centering the whole tab, strips shipping without codec labels (JS mock seeded a field the API never sent), and the operator's exact broken card. The user's verdict: "photos are really the only way to get this good."

**How to apply:** treat "suite green + review clean" as insufficient for UI PRs; budget 2–3 screenshot rounds; resume the same playwright agent across rounds (keeps context); target the exact motivating row when working from an operator complaint; move screenshots out of the worktree before `git add -A`. Related: [[playwright-doc1-cdp-workaround]], [[project-575-ui-consolidation]].
