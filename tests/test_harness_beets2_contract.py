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


if __name__ == "__main__":
    unittest.main()
