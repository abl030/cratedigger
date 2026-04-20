---
name: .bak file bug in beets library
description: 24 albums have track files renamed to .bak after import — systematic, mostly track 01. Root cause unknown.
type: project
---

24 albums in the beets library have a track (almost always track 01) with a .bak extension instead of .mp3. Meelo can't scan these files.

**What we know:**
- Beets' scrub plugin does NOT create .bak files (verified by reading source code)
- Mutagen, mediafile, music-tag, and our code do NOT create .bak files
- The harness logs show the files as .mp3 at validation time
- The .bak appears after beets import completes (between import and post-flight)
- 22 of 24 cases are track 01, 2 are track 04/05
- Pattern suggests a race condition or file locking issue, possibly virtiofs related

**Three actions needed (separate chat):**
1. Fix existing .bak files — rename to .mp3, update beets paths (user will do manually)
2. Add beets stderr logging to import_one.py — capture beets' internal operations during import to pin down when the rename happens. Currently we only log import_one's stderr, not beets' verbose output.
3. Add post-import verification — after import_one.py succeeds, check that all files in the beets DB have valid audio extensions. If .bak found, auto-rename and update. Could be a new step in import_one.py after postflight_verify().

**How to apply:** When working on import_one.py or post-import checks, remember this bug exists and these files need auditing.

**Affected albums query:**
```bash
ssh doc1 'beet ls -p path::.bak$'
```
