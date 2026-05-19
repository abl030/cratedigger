---
name: project-plex-asciify-split
description: "2026-05-18 incident — asciify_paths + beet move split 1,178 Plex albums; fix is the Plex merge API, not Empty Trash"
metadata: 
  node_type: memory
  type: project
  originSessionId: 61665597-d040-4e9f-b5ed-f15fc9ad19d7
---

Enabling `asciify_paths = true` in beets and running a full-library `beet move`
renames thousands of file paths through `unidecode` (curly `’` → straight `'`,
`…` → `...`, `¥` → `Y`, etc.) but leaves ID3 tags and the beets DB untouched
by design. Plex's scanner doesn't reconcile mass renames — it spawns ghost
album rows for the renamed files alongside the original rows (which still
point at dead curly-path tracks). Affected ~12% of the library (1,178 albums)
on 2026-05-18.

Fixed via the Plex merge API (`scripts/plex_dupes_audit.py` +
`plex_dupes_merge.py`). 1,178 merges, all 200s, idempotent.

**Why:** Plex's `Empty Trash + Clean Bundles` makes it WORSE — it deletes
the dead track entries but leaves the ghost album rows, and on remote SMB
mounts (where Plex inotify is unreliable) the rescan it triggers surfaces
additional splits the partial scans had missed. The merge API is the only
clean fix that preserves play counts.

**How to apply:** Any future beets change that mutates rendered paths
(asciify, new `paths:` template, `path_sep_replace`) followed by `beet move`
will re-trigger this. Plan to run the merge scripts after the next full
Plex scan. Don't reach for Empty Trash first. Full incident write-up at
`docs/solutions/runtime-errors/plex-asciify-paths-album-split.md`; CLAUDE.md
"Resolved" section also references it.

The 63 residual diff-folder dupes left after cleanup are LEGITIMATE
multi-edition pressings (Aphex Twin Drukqs 2001 [Vertigo] vs 2018 reissue,
Arcade Fire Funeral Japan vs Merge Records, etc.) — invariant #5
("multiple editions are intentional"). Don't merge those.

Related: [[feedback_dict_boundary]], [[project_cratedigger_rename]].
