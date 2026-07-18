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

# --- Bad-rip derived-sidecar cleanup contract. ``beet remove -d`` delegates
# to Album.remove(delete=True), which prunes a directory only when every
# remaining file matches the exact clutter list. Our sidecar is derived state;
# an unknown sentinel must still block pruning.
from beets import config

def cleanup_world(*, foreign_file):
    with tempfile.TemporaryDirectory() as d:
        album_dir = os.path.join(d, "The Rolling Stones", "1964 - Album")
        os.makedirs(album_dir)
        audio_path = os.path.join(album_dir, "01.flac")
        with open(audio_path, "wb") as f:
            f.write(b"audio")
        sidecar_path = os.path.join(album_dir, "cratedigger.json")
        with open(sidecar_path, "w", encoding="utf-8") as f:
            f.write("{}")
        sentinel_path = os.path.join(album_dir, "operator.keep")
        if foreign_file:
            with open(sentinel_path, "w", encoding="utf-8") as f:
                f.write("keep")

        lib = library.Library(os.path.join(d, "lib.db"), d)
        item = library.Item(
            title="Track", artist="Artist", album="Album",
            albumartist="Artist", path=audio_path,
        )
        album = lib.add_album([item])
        config["clutter"].set(["cratedigger.json"])
        album.remove(delete=True)

        assert not os.path.exists(audio_path), audio_path
        if foreign_file:
            assert os.path.isdir(album_dir), album_dir
            assert os.path.isfile(sidecar_path), sidecar_path
            assert os.path.isfile(sentinel_path), sentinel_path
        else:
            assert not os.path.exists(album_dir), album_dir

cleanup_world(foreign_file=False)
cleanup_world(foreign_file=True)
print("BAD_RIP_CLEANUP_OK")
'''


# Fresh-interpreter sweep of the SHIPPED aunique config against real beets.
# The Passenger collision class (2026-07-18): beets' %aunique picks the first
# disambiguator field whose values are all-distinct across the same-key album
# set, then renders each album's OWN value — an album whose value for that
# field is EMPTY renders NO bracket and lands on the plain path, colliding
# with the sibling's sticky plain path (old album label='ATO Records', new
# album label='' → label is "all-distinct" → new album's bracket is empty).
# The invariant: under the shipped template + album_fields, two same-key
# albums with different release ids ALWAYS render distinct directories.
_AUNIQUE_CONTRACT = r'''
import itertools
import json
import os
import sys
import tempfile

import beets
from beets import config as bconfig
from beets import plugins as bplugins
from beets.library import Album, Library
from beets.util import functemplate

shipped = json.loads(os.environ["AUNIQUE_SHIPPED_CONFIG"])
TEMPLATE = shipped["template"]
ALBUM_FIELDS = shipped["album_fields"]

# The pre-2026-07-18 poisoned template — the planted known-bad the sweep
# must detect, proving the checker catches the class.
OLD_TEMPLATE = (
    "$albumartist/$year - $album%aunique{albumartist album,"
    "albumtype year label catalognum albumdisambig releasegroupdisambig "
    "short_mbid}/$track $title"
)

FIELD_STATES = [("", ""), ("X", ""), ("X", "X"), ("X", "Y")]
SWEEP_FIELDS = ("albumdisambig", "releasegroupdisambig", "catalognum", "label")


def find_collisions(lib, template, worlds):
    """Return violating pairs under the collision invariant.

    A violation is EITHER two same-key siblings rendering the same
    directory, OR any sibling rendering its PLAIN stem (the template
    with the %aunique call stripped) — the live hazard: the other
    sibling's sticky on-disk path IS the plain stem, so a plain-stem
    render lands the import inside the existing album's folder
    (Passenger, 2026-07-18)."""
    import re as _re

    tmpl = functemplate.template(template)
    stem_tmpl = functemplate.template(
        _re.sub(r"%aunique\{[^}]*\}", "", template))
    bad = []
    for a, b in worlds:
        da = a.evaluate_template(tmpl, True).rsplit("/", 1)[0]
        db = b.evaluate_template(tmpl, True).rsplit("/", 1)[0]
        stem_a = a.evaluate_template(stem_tmpl, True).rsplit("/", 1)[0]
        stem_b = b.evaluate_template(stem_tmpl, True).rsplit("/", 1)[0]
        if da == db or da == stem_a or db == stem_b:
            bad.append((da, db))
    return bad


with tempfile.TemporaryDirectory() as d:
    bconfig["directory"] = d
    for name, expr in ALBUM_FIELDS.items():
        bconfig["album_fields"][name] = expr
    bconfig["plugins"] = "inline"
    bplugins.load_plugins()
    lib = Library(os.path.join(d, "lib.db"), d)

    worlds = []
    n = 0
    for states in itertools.product(FIELD_STATES, repeat=len(SWEEP_FIELDS)):
        for year_b in (2011, 2012):
            n += 1
            fields_a = {f: s[0] for f, s in zip(SWEEP_FIELDS, states)}
            fields_b = {f: s[1] for f, s in zip(SWEEP_FIELDS, states)}
            a = Album(albumartist="Lisa Hannigan", album=f"Passenger {n}",
                      year=2011, albumtype="album",
                      mb_albumid="dd578a59-ef6d-46fa-9f28-1e19c456dac8",
                      **fields_a)
            lib.add(a)
            b = Album(albumartist="Lisa Hannigan", album=f"Passenger {n}",
                      year=year_b, albumtype="album",
                      mb_albumid="5e7a6000-ce08-4e7b-9773-22a26e0a2980",
                      **fields_b)
            lib.add(b)
            worlds.append((a, b))

    collisions = find_collisions(lib, TEMPLATE, worlds)
    if collisions:
        print("SHIPPED_TEMPLATE_COLLISIONS=%d" % len(collisions))
        print("first:", collisions[0])
        sys.exit(1)
    print("AUNIQUE_SHIPPED_OK worlds=%d" % len(worlds))

    # Known-bad: the poisoned historical template must trip the checker.
    old_collisions = find_collisions(lib, OLD_TEMPLATE, worlds)
    assert old_collisions, (
        "sweep failed to detect the known-bad empty-disambiguator "
        "collision in the pre-fix template — the checker is toothless"
    )
    print("AUNIQUE_KNOWN_BAD_DETECTED=%d" % len(old_collisions))
'''


def _shipped_aunique_config() -> dict:
    """Extract the shipped beets path template + inline album_fields from
    nix/module.nix — the test patrols what production actually renders."""
    import re

    src = open(os.path.join(_REPO, "nix", "module.nix")).read()
    m = re.search(r'default = "(\$albumartist[^"]+)";', src)
    assert m, "paths.default template not found in nix/module.nix"
    fields = dict(re.findall(r'album_fields\.(\w+) = "([^"]+)";', src))
    return {"template": m.group(1), "album_fields": fields}


class TestAuniqueCollisionContract(unittest.TestCase):
    def test_shipped_template_never_collides_same_key_siblings(self):
        import json as _json

        proc = subprocess.run(
            [sys.executable, "-c", _AUNIQUE_CONTRACT],
            cwd=_REPO,
            env={**os.environ,
                 "PYTHONPATH": _REPO + os.pathsep + os.environ.get("PYTHONPATH", ""),
                 "AUNIQUE_SHIPPED_CONFIG": _json.dumps(_shipped_aunique_config())},
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            proc.returncode, 0,
            f"aunique collision contract failed\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )
        self.assertIn("AUNIQUE_SHIPPED_OK", proc.stdout)
        self.assertIn("AUNIQUE_KNOWN_BAD_DETECTED", proc.stdout)


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
        self.assertIn("BAD_RIP_CLEANUP_OK", proc.stdout)


if __name__ == "__main__":
    unittest.main()
