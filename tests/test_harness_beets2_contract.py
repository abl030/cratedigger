"""Real-beets 2.12 API contract for the import harness.

The other harness unit tests mock ``beets`` in ``sys.modules``, so they cannot
catch beets-version API drift — which is exactly how the 2026-06-29 beets
2.11 → 2.12 bump broke every import undetected:

  * ``beets.ui.get_path_formats`` / ``get_replacements`` were removed and the
    1.x four-arg ``Library(path, dir, path_formats, replacements)`` form both
    fails to import and raises ``TypeError``; 2.x derives both from config and
    takes only ``(library, directory)``.
  * the 1.x duplicate-resolution hook (``ImportSession.resolve_duplicate`` +
    ``task.should_remove_duplicates = True``) was replaced by
    ``get_duplicate_action(task, found_duplicates) -> DuplicateAction``. The
    stale override was silently never called, so upgrade imports kept both
    album rows and failed the post-import "multiple beets album rows" guard.

Also guards issue #570 defect 2: ``AlbumInfo.MEDIA_FIELD_MAP`` (real beets,
not a mock) maps ``album_id -> mb_albumid`` / ``releasegroup_id ->
mb_releasegroupid``, so ``_neutralize_discogs_provider_ids`` must hold
against the REAL ``item_data`` a Discogs apply would write — the mocked
harness unit tests (tests/test_harness_discogs_neutralize.py /
tests/test_harness_discogs_neutralize_generated.py) can't see this mapping
at all.

This test runs the REAL harness against the REAL beets in the dev shell, in a
subprocess so the sibling harness tests' ``sys.modules`` beets mocks cannot
leak in. If a future beets bump breaks either API, this fails loudly instead of
in production.
"""

from __future__ import annotations

import os
import subprocess
import sys
import unittest

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Runs in a fresh interpreter with the real beets on the path (no mocks).
_CONTRACT = r'''
import os
import tempfile

import harness.beets_harness as h
from beets import library
from beets.importer.actions import DuplicateAction

# --- Breakage #1: 2-arg Library derives path_formats + replacements from config
with tempfile.TemporaryDirectory() as d:
    lib = library.Library(os.path.join(d, "lib.db"), d)
    assert lib.path_formats, "Library.path_formats is empty (config not derived)"
    assert lib.replacements is not None, "Library.replacements is None"
    print("LIBRARY_OK path_formats=%d replacements=%d"
          % (len(lib.path_formats), len(lib.replacements)))

# --- Breakage #2: get_duplicate_action override replaces 1.x resolve_duplicate
assert "get_duplicate_action" in vars(h.HarnessImportSession), \
    "HarnessImportSession does not override get_duplicate_action"
assert "resolve_duplicate" not in vars(h.HarnessImportSession), \
    "stale 1.x resolve_duplicate override still defined"

sess = h.HarnessImportSession.__new__(h.HarnessImportSession)

class _Task:
    paths = [b"/incoming/x"]
    cur_artist = "A"
    cur_album = "B"

sent = []
h._send = lambda m: sent.append(m)
decisions = iter([{"action": "remove"}, {"action": "skip"}, {}])
h._recv = lambda: next(decisions)

remove = h.HarnessImportSession.get_duplicate_action(sess, _Task(), [])
skip = h.HarnessImportSession.get_duplicate_action(sess, _Task(), [])
default = h.HarnessImportSession.get_duplicate_action(sess, _Task(), [])

assert remove is DuplicateAction.REMOVE, remove
assert skip is DuplicateAction.SKIP, skip
assert default is DuplicateAction.SKIP, default  # absent action -> defensive SKIP
assert sent and sent[0]["type"] == "resolve_duplicate", sent[:1]
print("CONTRACT_OK")

# --- Breakage #3 (issue #570): beets' AlbumInfo.MEDIA_FIELD_MAP maps
# album_id -> mb_albumid and releasegroup_id -> mb_releasegroupid. The
# Discogs plugin fills those with NUMERIC Discogs ids, so an
# un-neutralized apply writes a bare integer into mb_albumid /
# MUSICBRAINZ_ALBUMID (Jellyfin's `new Guid()` throws on it). Drive the
# REAL AlbumInfo.item_data (what apply_metadata actually consumes) through
# the harness's neutralizer to prove the fix holds against beets 2.12,
# not just the mocked-beets unit tests.
import types

from beets.autotag.hooks import AlbumInfo

discogs_info = AlbumInfo(
    tracks=[], album="X", album_id="1505049",
    releasegroup_id="99999", data_source="Discogs",
    discogs_albumid="1505049")
# Read item_data FIRST, before neutralizing, so beets' @cached_property is
# hot with the POISONED value below. This makes the cache-bust in
# _neutralize_discogs_provider_ids (the __dict__.pop("item_data"/"raw_data")
# calls) load-bearing for this test: without it, item_data would keep
# serving this stale snapshot after neutralization and the assertions below
# would still pass on the OLD poisoned data, not the new blanked one.
poisoned_item_data = dict(discogs_info.item_data)
assert poisoned_item_data.get("mb_albumid") == "1505049", \
    poisoned_item_data.get("mb_albumid")
did_neutralize = h._neutralize_discogs_provider_ids(
    types.SimpleNamespace(info=discogs_info))
discogs_item_data = dict(discogs_info.item_data)
assert did_neutralize is True, did_neutralize
assert not discogs_item_data.get("mb_albumid"), discogs_item_data.get("mb_albumid")
assert not discogs_item_data.get("mb_releasegroupid"), \
    discogs_item_data.get("mb_releasegroupid")
assert discogs_item_data.get("discogs_albumid") == "1505049", \
    discogs_item_data.get("discogs_albumid")

mb_info = AlbumInfo(
    tracks=[], album="Y",
    album_id="11111111-2222-3333-4444-555555555555",
    data_source="MusicBrainz")
did_neutralize_mb = h._neutralize_discogs_provider_ids(
    types.SimpleNamespace(info=mb_info))
mb_item_data = dict(mb_info.item_data)
assert did_neutralize_mb is False, did_neutralize_mb
assert mb_item_data.get("mb_albumid") == "11111111-2222-3333-4444-555555555555", \
    mb_item_data.get("mb_albumid")
print("DISCOGS_NEUTRALIZE_OK")
'''


class TestHarnessBeets2Contract(unittest.TestCase):
    def test_real_beets_import_library_and_duplicate_action(self):
        proc = subprocess.run(
            [sys.executable, "-c", _CONTRACT],
            cwd=_REPO,
            env={**os.environ,
                 "PYTHONPATH": _REPO + os.pathsep + os.environ.get("PYTHONPATH", "")},
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            proc.returncode, 0,
            f"real-beets contract subprocess failed\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )
        self.assertIn("LIBRARY_OK", proc.stdout)
        self.assertIn("CONTRACT_OK", proc.stdout)
        self.assertIn("DISCOGS_NEUTRALIZE_OK", proc.stdout)


if __name__ == "__main__":
    unittest.main()
