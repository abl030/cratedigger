---
name: 1278-pt4-beets-child-shipped
description: "#1278 item 4 (beets child module) shipped 2026-08-31 — PRs #1282/#1287 live-verified; lane facts and deferred debt"
metadata:
  type: project
---

Issue #1278 strong candidate 4 delivered 2026-08-31 as PRs #1282 (lib/beets_child.py + delete/retag/tag-sync lanes converge on run_pinned_beets_child/BeetsChildRun) and #1287 (lane A: HarnessSession protocol seam, spawn_harness_session, harness_session_argv with REQUIRED pretend kwarg; 22 lib.beets.sp.Popen patches → typed fakes). Deployed via nixosconfig b1340076; cycle c37f0097 verified from the new source.

Durable facts a later session needs:

- The exit-code doctrine is per-lane: "never SUCCESS evidence". `run_beets_delete` deliberately treats nonzero exit as refusal (its child is our own delete_album.py, exits 1 with no frame); retag/tag-sync decide from world re-reads only. Don't "unify" the delete lane onto the beets-CLI phrasing again.
- The mock-audit waiver for `lib.beets.beets_validate` stays on purpose: its ~9 users mock the whole validation seam from the CALLER side. The lane-internal seam is now the `spawn` kwarg.
- `text=True` beside `errors="replace"` in subprocess calls is belt-and-braces: `errors=` alone forces text mode (proven equivalent mutant). msgspec `strict=False` does NOT coerce int→str (proven by probe) — lax mode is not the PR-#98 hazard; consumer-side coercion is.
- Deferred debt, recorded on #1278: deleting dead `lib/util.py::beets_validate` costs driving lib/util.py to zero `Any` (ratchet baseline exactly 10; the other 9 are Jellyfin/MB JSON types); both lane generated properties are blind to the exit-code doctrine (RD1/SD1 killed by pins only).

Related: [[1277-dispatch-request-shipped]], [[worktree-isolated-git-boundaries]]
