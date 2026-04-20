---
name: Dict boundary lesson
description: Critical lesson from production crashes — two dict shapes flow through cratedigger.py, only one is DownloadFile
type: feedback
---

There are TWO dict-shaped things in cratedigger.py — converting the wrong one crashes production:

1. **Raw slskd API dicts** — search results, directory["files"] items. Plain dicts with keys like filename, size, bitRate. Used in: verify_filetype(), download_filter(), album_track_num(), album_match(), check_ratio(), try_enqueue() (before slskd_do_enqueue).

2. **DownloadFile instances** — created in slskd_do_enqueue(), stored in GrabListEntry.files. Used in: monitor_downloads(), process_completed_album(), _build_download_info(), cancel_and_delete(), slskd_download_status().

3. **Raw from_db_row() dicts** — returned by AlbumRecord.from_db_row(). Passed to album_source.py methods AND to find_download/search_for_album. These have "_db_request_id" as a key. album_source.py MUST use .get("_db_request_id") not .db_request_id because it receives both raw dicts and GrabListEntry.

**Why:** Three production crashes on 2026-03-29 from converting raw-dict functions to attribute access.
**How to apply:** Before converting any file["key"] to file.key, verify the function receives DownloadFile/GrabListEntry, NOT a raw slskd/API dict. When in doubt, check `tests/test_integration.py` which tests both paths.
