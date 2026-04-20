---
name: Refactoring phases
description: Status of the multi-phase refactoring of cratedigger.py — all phases through pyright cleanup
type: project
---

Multi-phase refactor of cratedigger.py:

- **Phase 1** (done): SoularrConfig dataclass in lib/config.py, replacing 50+ globals.
- **Phase 2a/2b** (done): Wire SoularrConfig into main(), migrate all globals to cfg.field.
- **Phase 3** (done): Extracted lib/quality.py and lib/search.py.
- **Phase 4** (done 2026-03-29): GrabListEntry dataclass in lib/grab_list.py. Bridge methods for backward compat.
- **Phase 5** (done 2026-03-29): DownloadFile dataclass in lib/grab_list.py. All file dict access converted to attributes.
- **Lidarr removal** (done 2026-03-29): Deleted all Lidarr code. Pipeline DB is sole source. -452 lines.
- **DevShell** (done 2026-03-29): shell.nix with postgresql, psycopg2, music-tag, sox, ffmpeg. 280 tests, 0 skips.
- **album_source bridge removal** (done 2026-03-29): Converted .get("_db_request_id") to .db_request_id in DatabaseSource methods.
- **beets_validate extraction** (done 2026-03-29): Moved to lib/beets.py as pure function (takes harness_path param). cratedigger.py has thin wrapper.
- **pyright cleanup** (done 2026-03-29): Typed globals (cfg: SoularrConfig, slskd: SlskdClient, pipeline_db_source: DatabaseSource). 79→8 errors. Remaining 8: slskd_api not in nixpkgs (2), spectral_check local import (1), music_tag Optional return (5).

**Known remaining issues:**
- `slskd-api` pip package not in nixpkgs — pyright can't resolve import
- `spectral_check` uses sys.path manipulation — pyright can't resolve
- `music_tag.load_file()` returns Optional — wrapped in try/except, safe at runtime

**What's left to type:**
- `AlbumRecord.from_db_row()` return dict — the "wanted record" shape. Read-only, lower priority.
