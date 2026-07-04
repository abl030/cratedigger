---
name: bak-file-bug-in-beets-library
description: "RESOLVED: .bak track files were created by mp3val -f; fixed with -nb flag. Rename repair in import_one.py deliberately removed — bad extensions are now detect-and-warn only."
metadata: 
  node_type: memory
  type: project
  originSessionId: deada68f-df61-4828-a3dd-87243539dee0
---

**RESOLVED (verified 2026-07-02).** The 24 albums with track files renamed to `.bak` (mostly track 01) were caused by `mp3val -f` creating backup files during audio repair. Fixed by adding `-nb` (commit `ad033f8`) — the call is now `mp3val -f -nb` in `lib/util.py:190`.

Follow-on design decision: the automatic post-import rename repair (ffprobe-probe + rename + beets SQLite path rewrite) was **deliberately deleted** (commit `1a239a6`). `_record_bad_extension_warnings` in `harness/import_one.py` now only detects and records bad extensions in `postflight.bad_extensions` with loud logging — it never renames or mutates beets. A bad extension appearing now is an upstream corruption signal, not a recovery path.

**How to apply:** don't re-add automatic extension-rename repair; don't re-investigate .bak as an unknown. Any leftover `.bak` files in the library are pre-fix residue: `ssh doc2 'beet ls -p path::.bak$'`.
