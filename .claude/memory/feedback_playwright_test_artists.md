---
name: Playwright test artists
description: When briefing the playwright subagent for Discogs/MB browse-tab tests, pick small/obscure artists — not Radiohead/Taylor Swift. Big artists are slow on the mirror.
type: feedback
originSessionId: 445ade2d-e5b4-4bb1-b0ff-5f5785f198d3
---
When asking the playwright subagent to test the music.ablz.au Browse tab (especially Discogs source), pick **small or obscure artists** for queries — not Radiohead, Taylor Swift, or other megastars.

**Why:** The Discogs mirror's release search slows to ~15s on single-word queries that match thousands of releases (e.g. "Radiohead"). The cratedigger `_get()` timeout is 15s, so big-artist queries against `/api/discogs/search?type=release` regularly 500 with `read operation timed out`. Same with discography views — Radiohead has 158+ masters which makes the page slow to render and noisy to scan in a snapshot.

**How to apply:** When writing playwright agent prompts for browse/Discogs testing, use small/indie artists where the user knows there's data — e.g. "Blueline Medic" (the user's own collection has them), or any other artist with under ~30 releases. If you must verify a feature works for popular artists, use a multi-word query (artist + album title) to narrow results.
