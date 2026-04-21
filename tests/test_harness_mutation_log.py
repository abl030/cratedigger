"""Harness MBID-swap audit log.

Guards against silent cross-source mutations: when an external tool
(tagging-workspace fix_reissues/fix_undated, a manual `beet modify`, future
unknown drivers) uses the harness with `--search-id` to retag an existing
album, the swap is invisible to cratedigger's pipeline DB (different entry
path — download_log never sees it) and to beets' legacy import.log (the
harness uses the Python ImportSession API directly, bypassing the CLI logger).

The 2026-04-14 Lucksmiths "First Tape" retag took hours of forensics because
no single log captured it. This log is the single source of truth for
harness-driven mutations, regardless of caller.

Pure tests: `_mbid_swap_event` takes a task + candidate and returns the event
dict (or None). I/O is a separate helper, also tested here.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock


# beets isn't installed in the test nix-shell — mock it before import.
_beets_mocks = {
    "beets": MagicMock(),
    "beets.config": MagicMock(),
    "beets.library": MagicMock(),
    "beets.plugins": MagicMock(),
    "beets.importer": MagicMock(),
    "beets.importer.session": MagicMock(),
    "beets.importer.tasks": MagicMock(),
    "beets.ui": MagicMock(),
}
for name, mock in _beets_mocks.items():
    sys.modules.setdefault(name, mock)

setattr(sys.modules["beets.importer.session"], "ImportSession",
        type("ImportSession", (object,), {}))

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from harness import beets_harness  # noqa: E402


def _make_item(mb_albumid: str | None):
    """Stand-in for a beets Item with just the fields we read."""
    return SimpleNamespace(mb_albumid=mb_albumid)


def _make_task(items, path: bytes | str = b"/Beets/Artist/Album"):
    return SimpleNamespace(items=items, paths=[path])


def _make_candidate(album_id: str):
    info = SimpleNamespace(album_id=album_id)
    return SimpleNamespace(info=info)


class TestMbidSwapEvent(unittest.TestCase):

    def test_no_swap_when_mbids_match(self):
        task = _make_task([_make_item("aaa"), _make_item("aaa")])
        cand = _make_candidate("aaa")
        self.assertIsNone(beets_harness._mbid_swap_event(task, cand))

    def test_no_event_when_items_have_no_existing_mbid(self):
        # Fresh import of files that aren't in beets yet — no prior mbid to
        # diff against. Not a swap; a normal import.
        task = _make_task([_make_item(None), _make_item("")])
        cand = _make_candidate("new-mbid-123")
        self.assertIsNone(beets_harness._mbid_swap_event(task, cand))

    def test_no_event_when_candidate_has_no_album_id(self):
        task = _make_task([_make_item("aaa")])
        cand = _make_candidate("")
        self.assertIsNone(beets_harness._mbid_swap_event(task, cand))

    def test_swap_detected_returns_event(self):
        # The Lucksmiths case: existing items at d9b0ee01, candidate 5645221a.
        task = _make_task(
            [_make_item("d9b0ee01"), _make_item("d9b0ee01")],
            path=b"/Beets/The Lucksmiths/1996 - First Tape",
        )
        cand = _make_candidate("5645221a")
        ev = beets_harness._mbid_swap_event(task, cand)
        self.assertIsNotNone(ev)
        assert ev is not None  # narrow for pyright
        self.assertEqual(ev["event"], "harness_mbid_swap")
        self.assertEqual(ev["old_mb_albumid"], "d9b0ee01")
        self.assertEqual(ev["new_mb_albumid"], "5645221a")
        self.assertEqual(ev["path"], "/Beets/The Lucksmiths/1996 - First Tape")
        self.assertIn("ts", ev)
        self.assertIn("ppid", ev)
        self.assertEqual(ev["argv"], list(sys.argv))

    def test_swap_detected_with_mixed_existing_mbids(self):
        # Edge case: items somehow carry different mbids. Still a swap; we
        # log one deterministically so the event is stable.
        task = _make_task([_make_item("aaa"), _make_item("bbb")])
        cand = _make_candidate("ccc")
        ev = beets_harness._mbid_swap_event(task, cand)
        self.assertIsNotNone(ev)
        assert ev is not None
        self.assertIn(ev["old_mb_albumid"], {"aaa", "bbb"})
        self.assertEqual(ev["new_mb_albumid"], "ccc")

    def test_no_event_when_task_has_no_items(self):
        task = _make_task([])
        cand = _make_candidate("anything")
        self.assertIsNone(beets_harness._mbid_swap_event(task, cand))

    def test_handles_str_path_as_well_as_bytes(self):
        task = _make_task(
            [_make_item("old")], path="/literal/str/path",
        )
        cand = _make_candidate("new")
        ev = beets_harness._mbid_swap_event(task, cand)
        self.assertIsNotNone(ev)
        assert ev is not None
        self.assertEqual(ev["path"], "/literal/str/path")


class TestAppendMutationLog(unittest.TestCase):

    def test_appends_one_jsonl_line_per_event(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl",
                                          delete=False) as f:
            log_path = f.name
        try:
            beets_harness._append_mutation_log(
                {"event": "a", "x": 1}, log_path=log_path)
            beets_harness._append_mutation_log(
                {"event": "b", "x": 2}, log_path=log_path)
            with open(log_path) as f:
                lines = f.read().strip().split("\n")
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[0]), {"event": "a", "x": 1})
            self.assertEqual(json.loads(lines[1]), {"event": "b", "x": 2})
        finally:
            os.unlink(log_path)

    def test_unwritable_path_does_not_raise(self):
        # Audit log failure must not break the import.
        beets_harness._append_mutation_log(
            {"event": "x"}, log_path="/nonexistent/dir/log.jsonl")


if __name__ == "__main__":
    unittest.main()
