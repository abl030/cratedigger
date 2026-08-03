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
import sys
import subprocess
import unittest

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Runs in a fresh interpreter with the real beets on the path (no mocks).
_CONTRACT = r'''
import os
import subprocess
import sys
import tempfile

import harness.beets_harness as h
from beets import library

# --- Configured Library keeps path/replacement policy in both API eras.
from beets import config
with tempfile.TemporaryDirectory() as d:
    config["paths"].set({"default": "$albumartist/Configured/$track $title"})
    config["replace"].set({"[ ]": "_"})
    config["library"].set(os.path.join(d, "lib.db"))
    config["directory"].set(d)
    lib = h.beets_compat.configured_library(config)
    assert lib.path_formats, "Library.path_formats is empty (config not derived)"
    assert lib.replacements is not None, "Library.replacements is None"
    print("LIBRARY_OK path_formats=%d replacements=%d"
          % (len(lib.path_formats), len(lib.replacements)))

# --- API eras: both hook names delegate to one structural adapter. The loaded
# Beets decides which one it calls; retaining both does not version-switch.
assert "get_duplicate_action" in vars(h.HarnessImportSession), \
    "HarnessImportSession does not expose the modern duplicate hook"
assert "resolve_duplicate" in vars(h.HarnessImportSession), \
    "HarnessImportSession does not expose the legacy duplicate hook"

sess = h.HarnessImportSession.__new__(h.HarnessImportSession)

class _Task:
    paths = [b"/incoming/x"]
    cur_artist = "A"
    cur_album = "B"

sent = []
h._send = lambda m: sent.append(m)
decisions = iter([{"action": "remove"}, {"action": "skip"}, {}])
h._recv = lambda: next(decisions)

report = h.beets_compat.capability_report()
if report["duplicate_era"] == "modern":
    from beets.importer.actions import DuplicateAction
    remove = h.HarnessImportSession.get_duplicate_action(sess, _Task(), [])
    skip = h.HarnessImportSession.get_duplicate_action(sess, _Task(), [])
    default = h.HarnessImportSession.get_duplicate_action(sess, _Task(), [])
    assert remove is DuplicateAction.REMOVE, remove
    assert skip is DuplicateAction.SKIP, skip
    assert default is DuplicateAction.SKIP, default
else:
    task = _Task()
    h.HarnessImportSession.resolve_duplicate(sess, task, [])
    assert task.should_remove_duplicates is True
    h.HarnessImportSession.resolve_duplicate(sess, task, [])
    assert task.should_remove_duplicates is False
    h.HarnessImportSession.resolve_duplicate(sess, task, [])
    assert task.should_remove_duplicates is False
assert sent and sent[0]["type"] == "resolve_duplicate", sent[:1]
print("CONTRACT_OK beets=%s era=%s" % (__import__("beets").__version__, report["era"]))

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
did_neutralize = h._neutralize_discogs_provider_ids(
    types.SimpleNamespace(info=discogs_info))
assert did_neutralize is True, did_neutralize
if hasattr(discogs_info, "item_data"):
    discogs_item_data = dict(discogs_info.item_data)
    assert not discogs_item_data.get("mb_albumid"), discogs_item_data.get("mb_albumid")
    assert not discogs_item_data.get("mb_releasegroupid"), \
        discogs_item_data.get("mb_releasegroupid")
    assert discogs_item_data.get("discogs_albumid") == "1505049", \
        discogs_item_data.get("discogs_albumid")
else:
    assert discogs_info.album_id == ""
    assert discogs_info.releasegroup_id == ""
    assert discogs_info.discogs_albumid == "1505049"

mb_info = AlbumInfo(
    tracks=[], album="Y",
    album_id="11111111-2222-3333-4444-555555555555",
    data_source="MusicBrainz")
did_neutralize_mb = h._neutralize_discogs_provider_ids(
    types.SimpleNamespace(info=mb_info))
assert did_neutralize_mb is False, did_neutralize_mb
if hasattr(mb_info, "item_data"):
    mb_item_data = dict(mb_info.item_data)
    assert mb_item_data.get("mb_albumid") == "11111111-2222-3333-4444-555555555555", \
        mb_item_data.get("mb_albumid")
else:
    assert mb_info.album_id == "11111111-2222-3333-4444-555555555555"
print("DISCOGS_NEUTRALIZE_OK")

# --- Exact delete uses Cratedigger's pinned-child operation, never Beets'
# selector/remove shortcut. Its owned sidecar must go, while an unknown
# operator sentinel must remain.
from beets import config

def cleanup_world(*, foreign_file):
    with tempfile.TemporaryDirectory() as d:
        beets_dir = os.path.join(d, "beets")
        os.makedirs(beets_dir)
        with open(os.path.join(beets_dir, "config.yaml"), "w", encoding="utf-8") as handle:
            handle.write(
                "library: %s\ndirectory: %s\nplugins: []\n"
                "clutter: [cratedigger.json]\n"
                "importsource:\n  suggest_removal: false\n"
                % (os.path.join(d, "lib.db"), d)
            )
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
            mb_albumid="11111111-2222-3333-4444-555555555555",
        )
        album = lib.add_album([item])
        assert album.id is not None
        child = """
from lib.beets_delete import BeetsDeleteCompleted, BeetsDeleteRequest, execute_pinned_beets_delete
outcome = execute_pinned_beets_delete(BeetsDeleteRequest(
    album_id=%d,
    expected_release_id=%r,
    library_db_path=%r,
    library_root=%r,
))
assert isinstance(outcome, BeetsDeleteCompleted), outcome
""" % (
            album.id,
            "11111111-2222-3333-4444-555555555555",
            os.path.join(d, "lib.db"),
            d,
        )
        proc = subprocess.run(
            [sys.executable, "-c", child],
            env={**os.environ, "BEETSDIR": beets_dir},
            text=True, capture_output=True, check=False,
        )
        assert proc.returncode == 0, proc.stderr

        assert not os.path.exists(audio_path), audio_path
        assert not os.path.exists(sidecar_path), sidecar_path
        if foreign_file:
            assert os.path.isdir(album_dir), album_dir
            assert os.path.isfile(sentinel_path), sentinel_path
        else:
            # Older Beets removes the owned media but leaves an empty parent;
            # current Beets prunes it. Both preserve the exact-delete safety
            # boundary (the foreign-file case above is the non-negotiable one).
            assert not os.path.exists(audio_path), audio_path

cleanup_world(foreign_file=False)
cleanup_world(foreign_file=True)
print("BAD_RIP_CLEANUP_OK")
'''


# Real wrapper contract for issue #862. This is deliberately a successful
# dry-run assertion only: it proves the completed ``--pretend`` path leaves
# the source manifest untouched, not that no transient create/delete event
# occurred while beets was running.
_PRETEND_SOURCE_PURITY_CONTRACT = r'''
import hashlib
import json
import os
import subprocess
import tempfile


def recursive_manifest(root):
    entries = []
    for current, dirs, files in os.walk(root):
        dirs.sort()
        files.sort()
        rel_dir = os.path.relpath(current, root)
        for dirname in dirs:
            entries.append(("dir", os.path.join(rel_dir, dirname)))
        for filename in files:
            path = os.path.join(current, filename)
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                for chunk in iter(lambda: handle.read(65536), b""):
                    digest.update(chunk)
            entries.append(("file", os.path.join(rel_dir, filename), digest.hexdigest()))
    return entries


with tempfile.TemporaryDirectory(prefix="cratedigger-pretend-purity-") as root:
    source = os.path.join(root, "source")
    library = os.path.join(root, "library")
    beetsdir = os.path.join(root, "beets")
    os.makedirs(source)
    os.makedirs(library)
    os.makedirs(beetsdir)

    flac = os.path.join(source, "01 - Source.flac")
    subprocess.run([
        "ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:duration=0.1", "-metadata", "artist=Purity Artist",
        "-metadata", "album=Purity Album", "-metadata", "title=Source",
        "-c:a", "flac", flac,
    ], check=True)
    with open(os.path.join(source, "cratedigger-sentinel.json"), "w", encoding="utf-8") as handle:
        json.dump({"must": "survive"}, handle)

    with open(os.path.join(beetsdir, "config.yaml"), "w", encoding="utf-8") as handle:
        handle.write("""library: %s\ndirectory: %s\nplugins: scrub\nimport:\n  copy: no\n  write: yes\n  move: yes\n  incremental: no\n  duplicate_keys:\n    album: [mb_albumid, discogs_albumid]\n    item: [artist, title]\n""" % (os.path.join(library, "library.db"), library))

    before = recursive_manifest(source)
    # The real wrapper must reach its normal candidate/choose path without
    # depending on the public MusicBrainz service. sitecustomize is limited
    # to this subprocess: it supplies one structurally valid provider result,
    # leaving Beets' importer, distance calculation, and protocol intact.
    shim = os.path.join(root, "shim")
    os.makedirs(shim)
    with open(os.path.join(shim, "sitecustomize.py"), "w", encoding="utf-8") as handle:
        handle.write("""\\
from beets.autotag import mb
from beets.autotag.hooks import AlbumInfo, TrackInfo

def match_album(artist, album, tracks, extra_tags):
    return [AlbumInfo(
        album="Purity Album", artist="Purity Artist",
        album_id="11111111-2222-3333-4444-555555555555",
        tracks=[TrackInfo(
            title="Source", artist="Purity Artist",
            track_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", index=1,
        )],
    )]

mb.match_album = match_album
""")
    env = {
        **os.environ,
        "BEETSDIR": beetsdir,
        "PYTHONPATH": shim + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    proc = subprocess.Popen(
        [os.environ.get("BASH", "bash"), os.environ["HARNESS_WRAPPER"], "--pretend", "--noincremental", source],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert proc.stdin is not None
    assert proc.stdout is not None
    saw_choose_match = False
    saw_session_end = False
    transcript = []
    for line in proc.stdout:
        message = json.loads(line)
        transcript.append(message)
        if message["type"] == "choose_match":
            saw_choose_match = True
            proc.stdin.write(json.dumps({"action": "skip"}) + "\n")
            proc.stdin.flush()
        elif message["type"] == "session_end":
            saw_session_end = True
    stderr = proc.stderr.read() if proc.stderr is not None else ""
    returncode = proc.wait()
    assert returncode == 0, (returncode, transcript, stderr)
    assert saw_choose_match, transcript
    assert saw_session_end, transcript
    after = recursive_manifest(source)
    assert after == before, (before, after)
    print("PRETEND_SOURCE_PURITY_OK")
'''

_STDOUT_PROTOCOL_CONTRACT = r'''
import os
import harness.beets_harness as h

h._protocol_stdout = h._reserve_protocol_stdout()
try:
    print("python diagnostic")
    os.write(1, b"raw diagnostic\\n")
    h._send({"type": "protocol_ok"})
finally:
    h._protocol_stdout.close()
    h._protocol_stdout = None
'''


# Fresh-interpreter sweep of the consumer-example aunique config against real Beets.
# The Passenger collision class (2026-07-18): beets' %aunique picks the first
# disambiguator field whose values are all-distinct across the same-key album
# set, then renders each album's OWN value — an album whose value for that
# field is EMPTY renders NO bracket and lands on the plain path, colliding
# with the sibling's sticky plain path (old album label='ATO Records', new
# album label='' → label is "all-distinct" → new album's bracket is empty).
# The invariant: under the consumer template + album_fields, two same-key
# albums with different release ids ALWAYS render distinct directories.
_AUNIQUE_CONTRACT = r'''
import itertools
import json
import os
import sys
import tempfile
import subprocess

import beets
from beets import config as bconfig
from beets import plugins as bplugins
from beets.library import Album, Library
from beets.util import functemplate

consumer = json.loads(os.environ["AUNIQUE_CONSUMER_CONFIG"])
TEMPLATE = consumer["template"]
ALBUM_FIELDS = consumer["album_fields"]

# The pre-2026-07-18 poisoned template — the planted known-bad the sweep
# must detect, proving the checker catches the class.
OLD_TEMPLATE = consumer["historical_passenger_template"]

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


# Real Beets incremental-import proof for the externally provisioned absolute
# statefile. The immutable BEETSDIR manifest must stay byte-identical while the
# separate state file records the completed source directory.
_EXTERNAL_STATEFILE_CONTRACT = r'''
import hashlib
import os
import pickle
import subprocess
import sys
from pathlib import Path

from lib.beets_config_contract import check_beets_config
from tests.fakes.beets_contract import BeetsContractWorld


def manifest(root):
    values = []
    for path in sorted(Path(root).rglob("*")):
        relative = str(path.relative_to(root))
        if path.is_file():
            values.append(("file", relative, hashlib.sha256(path.read_bytes()).hexdigest()))
        else:
            values.append(("dir", relative))
    return values


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def changed_paths(before, after):
    before_by_path = {entry[1]: entry for entry in before}
    after_by_path = {entry[1]: entry for entry in after}
    return {
        path
        for path in before_by_path.keys() | after_by_path.keys()
        if before_by_path.get(path) != after_by_path.get(path)
    }


world = BeetsContractWorld(role="importer")
try:
    world.state_file.write_bytes(
        pickle.dumps({"tagprogress": {}, "taghistory": set()})
    )
    world.unseal()
    world._write_main_config(**{
        "import": {
            "autotag": True,
            "move": True,
            "write": True,
            "incremental": True,
            "incremental_skip_later": True,
            "duplicate_keys": {
                "album": ["mb_albumid", "discogs_albumid"],
            },
        },
    })
    world._seal("importer")

    # The exact authority/config used by the real Beets run must first pass
    # the portable checker; this is one connected world, not a lookalike file.
    admitted = check_beets_config(world.cfg(), role="importer")
    assert admitted.ok, admitted.hard_failures

    source = world.root / "source"
    source.mkdir()
    audio = source / "01 - State Proof.flac"
    subprocess.run([
        "ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:duration=0.1", "-metadata", "artist=State Artist",
        "-metadata", "album=State Album", "-metadata", "title=State Proof",
        "-c:a", "flac", str(audio),
    ], check=True)
    before_beetsdir = manifest(world.beets_dir)
    before_runtime = digest(world.runtime_config)
    before_secret = digest(world.secret_include)
    before_source = manifest(source)
    before_state = world.state_file.read_bytes()
    before_database = digest(world.library_db)
    before_library = manifest(world.library_root)
    before_world = manifest(world.root)

    proc = subprocess.run(
        [
            sys.executable, "-m", "beets", "import", "-A", "-q",
            "--nocopy", "--nomove", "--nowrite", str(source),
        ],
        env={**os.environ, "BEETSDIR": admitted.authority.config_dir},
        text=True,
        capture_output=True,
        check=False,
    )
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert manifest(world.beets_dir) == before_beetsdir
    assert digest(world.runtime_config) == before_runtime
    assert digest(world.secret_include) == before_secret
    assert manifest(source) == before_source
    assert manifest(world.library_root) == before_library
    assert world.state_file.read_bytes() != before_state
    assert world.library_db.is_file(), world.library_db
    assert digest(world.library_db) != before_database
    after_world = manifest(world.root)
    assert changed_paths(before_world, after_world) == {
        str(world.library_db.relative_to(world.root)),
    }, changed_paths(before_world, after_world)
    with world.state_file.open("rb") as handle:
        state = pickle.load(handle)
    source_bytes = os.fsencode(str(source))
    assert any(source_bytes in paths for paths in state["taghistory"]), state
    print("EXTERNAL_STATEFILE_OK")
finally:
    world.close()
'''


def _consumer_aunique_config() -> dict:
    """Extract path policy from the deployment-owned consumer example."""
    from tests.beets_world import (
        HISTORICAL_PASSENGER_PATH_TEMPLATE,
        extract_consumer_beets_world_config,
    )

    consumer = extract_consumer_beets_world_config(_REPO)
    return {
        "template": consumer.default_path_template,
        "album_fields": dict(consumer.album_fields),
        "historical_passenger_template": HISTORICAL_PASSENGER_PATH_TEMPLATE,
    }


class TestAuniqueCollisionContract(unittest.TestCase):
    def test_checker_literals_match_the_real_collision_qualified_contract(self):
        proc = subprocess.run(
            [sys.executable, "-c", "from lib.beets_config_contract import SAFE_DEFAULT_PATH, SAFE_PATH_DISAMBIG; print(SAFE_DEFAULT_PATH); print(SAFE_PATH_DISAMBIG)"],
            cwd=_REPO, capture_output=True, text=True, check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        default, disambig = proc.stdout.splitlines()
        consumer = _consumer_aunique_config()
        self.assertEqual(default, consumer["template"])
        self.assertEqual(disambig, consumer["album_fields"]["path_disambig"])

    def test_consumer_template_never_collides_same_key_siblings(self):
        import json as _json

        proc = subprocess.run(
            [sys.executable, "-c", _AUNIQUE_CONTRACT],
            cwd=_REPO,
            env={**os.environ,
                 "PYTHONPATH": _REPO + os.pathsep + os.environ.get("PYTHONPATH", ""),
                 "AUNIQUE_CONSUMER_CONFIG": _json.dumps(_consumer_aunique_config())},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            proc.returncode, 0,
            f"aunique collision contract failed\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )
        self.assertIn("AUNIQUE_SHIPPED_OK", proc.stdout)
        self.assertIn("AUNIQUE_KNOWN_BAD_DETECTED", proc.stdout)


class TestHarnessBeets2Contract(unittest.TestCase):
    def test_help_stays_on_normal_stdout_and_protocol_is_private(self):
        help_proc = subprocess.run(
            [os.environ["CRATEDIGGER_BEETS_PYTHON"], os.path.join(_REPO, "harness", "beets_harness.py"), "--help"],
            cwd=_REPO, capture_output=True, text=True, check=False,
        )
        self.assertEqual(help_proc.returncode, 0, help_proc.stderr)
        self.assertIn("Beets interactive import harness", help_proc.stdout)
        self.assertEqual(help_proc.stderr, "")
        proc = subprocess.run(
            [sys.executable, "-c", _STDOUT_PROTOCOL_CONTRACT], cwd=_REPO,
            env={**os.environ, "PYTHONPATH": _REPO}, capture_output=True,
            text=True, check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout, '{"type": "protocol_ok"}\n')
        self.assertIn("python diagnostic", proc.stderr)
        self.assertIn("raw diagnostic", proc.stderr)

    def test_real_incremental_import_uses_external_statefile_only(self):
        proc = subprocess.run(
            [sys.executable, "-c", _EXTERNAL_STATEFILE_CONTRACT],
            cwd=_REPO,
            env={**os.environ,
                 "PYTHONPATH": _REPO + os.pathsep + os.environ.get("PYTHONPATH", "")},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"external Beets statefile contract failed\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )
        self.assertIn("EXTERNAL_STATEFILE_OK", proc.stdout)

    def test_real_harness_pretend_keeps_source_manifest_unchanged(self):
        """A completed pretend run leaves its source tree unchanged.

        This is not a claim about transient filesystem events during the run;
        it is the successful dry-run source-purity contract at the real
        wrapper boundary.
        """
        proc = subprocess.run(
            [sys.executable, "-c", _PRETEND_SOURCE_PURITY_CONTRACT],
            cwd=_REPO,
            env={
                **os.environ,
                "PYTHONPATH": _REPO + os.pathsep + os.environ.get("PYTHONPATH", ""),
                "HARNESS_WRAPPER": os.path.join(
                    _REPO, "harness", "run_beets_harness.sh"),
            },
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            proc.returncode, 0,
            f"real harness pretend source-purity contract failed\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )
        self.assertIn("PRETEND_SOURCE_PURITY_OK", proc.stdout)

    def test_real_beets_import_library_and_duplicate_action(self):
        proc = subprocess.run(
            [sys.executable, "-c", _CONTRACT],
            cwd=_REPO,
            env={**os.environ,
                 "PYTHONPATH": _REPO + os.pathsep + os.environ.get("PYTHONPATH", "")},
            capture_output=True,
            text=True,
            check=False,
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
