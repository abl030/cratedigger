"""Real-Beets contract for the import-time folder exclusivity guard (#1465).

Runs in a fresh interpreter (sibling harness tests mock ``beets`` in
``sys.modules``) against the dev shell's real Beets.
"""

from __future__ import annotations

import os
import subprocess
import sys
import unittest

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

_CONTRACT = r'''
import os
import shutil
import tempfile

from beets import config
from beets.library import Album, Item, Library
from beets.util import MoveOperation

from harness.folder_guard import find_folder_collision, planned_destinations

TEMPLATE = "$albumartist/$album%aunique{}/$track $title"


def world(d, *, replace=None):
    config["directory"].set(os.path.join(d, "library"))
    config["paths"].set({"default": TEMPLATE})
    config["replace"].set(replace or {})
    os.makedirs(os.path.join(d, "library"), exist_ok=True)
    return Library(os.path.join(d, "lib.db"), os.path.join(d, "library"))


counter = [0]


def stage(d, album, release_id, *, tracks=2, artist="Guard Artist", year=2001):
    counter[0] += 1
    src = os.path.join(d, "incoming", f"attempt-{counter[0]}")
    os.makedirs(src)
    items = []
    for n in range(1, tracks + 1):
        path = os.path.join(src, f"{n:02d}.flac")
        with open(path, "wb") as handle:
            handle.write(b"audio-%d-%d" % (counter[0], n))
        items.append(Item(
            title=f"Track {n}", track=n, artist=artist, albumartist=artist,
            album=album, year=year, mb_albumid=release_id, path=path,
        ))
    return items


def imported(lib, d, album, release_id, **kw):
    items = stage(d, album, release_id, **kw)
    added = lib.add_album(items)
    added.move(MoveOperation.MOVE)
    return added


def folder_of(album):
    return os.path.dirname(os.fsdecode(album.items()[0].path))


# 1. Equivalence: with the same-release duplicate excluded, the planned
#    destination is exactly where Beets puts the files after removal, and
#    the exclusion is load-bearing (with it visible %aunique brackets).
with tempfile.TemporaryDirectory() as d:
    lib = world(d)
    old = imported(lib, d, "Same Release", "rel-r")
    old_folder = folder_of(old)
    new_items = stage(d, "Same Release", "rel-r")
    new = lib.add_album(new_items)
    planned = planned_destinations(lib, new_items, {old.id})
    visible = tuple(os.fsdecode(i.destination()) for i in new_items)
    assert visible != planned, (visible, planned)
    assert find_folder_collision(lib, new, new_items, {old.id}) is None
    old.remove(delete=True)
    lib._memotable.clear()
    new.move(MoveOperation.MOVE)
    actual = tuple(os.fsdecode(i.path) for i in new.items())
    assert actual == planned, (actual, planned)
    assert os.path.dirname(planned[0]) == old_folder, (planned, old_folder)
print("EQUIVALENCE_OK")

# 2. Replace-rule collision: "Foo?" renders "Foo_", which a different
#    release already occupies. %aunique keys differ, so no bracket.
with tempfile.TemporaryDirectory() as d:
    lib = world(d, replace={r"[?]": "_"})
    occupant = imported(lib, d, "Foo_", "rel-x")
    new_items = stage(d, "Foo?", "rel-y")
    new = lib.add_album(new_items)
    collision = find_folder_collision(lib, new, new_items, set())
    assert collision is not None
    assert collision.folder == folder_of(occupant), collision
    assert [o.album_id for o in collision.occupants] == [occupant.id], collision
    assert collision.occupants[0].mb_albumid == "rel-x", collision
    assert collision.unowned_files == (), collision
    assert len(collision.planned_paths) == 2, collision
    wire = collision.to_wire()
    assert wire["occupants"][0]["beets_album_id"] == occupant.id, wire
    # A duplicate exclusion for an unrelated id must not hide the occupant.
    assert find_folder_collision(lib, new, new_items, {9999}) is not None
print("REPLACE_RULE_COLLISION_OK")

# 3. Unknown audio on disk in the planned folder counts as occupancy.
with tempfile.TemporaryDirectory() as d:
    lib = world(d)
    new_items = stage(d, "Lonely", "rel-z")
    new = lib.add_album(new_items)
    folder = os.path.dirname(planned_destinations(lib, new_items, {new.id})[0])
    os.makedirs(folder)
    stray = os.path.join(folder, "stray.mp3")
    with open(stray, "wb") as handle:
        handle.write(b"x")
    collision = find_folder_collision(lib, new, new_items, set())
    assert collision is not None
    assert collision.occupants == (), collision
    assert collision.unowned_files == (stray,), collision
    # 4. Non-audio leftovers alone are not occupancy.
    os.remove(stray)
    with open(os.path.join(folder, "cover.jpg"), "wb") as handle:
        handle.write(b"jpg")
    assert find_folder_collision(lib, new, new_items, set()) is None
    # ...unless one sits exactly at a planned destination.
    planned_file = planned_destinations(lib, new_items, {new.id})[0]
    shutil.copy(os.path.join(folder, "cover.jpg"), planned_file)
    collision = find_folder_collision(lib, new, new_items, set())
    assert collision is not None and collision.unowned_files == (planned_file,), collision
print("UNOWNED_FILES_OK")

# 5. A same-release duplicate sitting in the planned folder is not an
#    occupant (it is what Beets removes), but a third album there is.
with tempfile.TemporaryDirectory() as d:
    lib = world(d, replace={r"[?]": "_"})
    dup = imported(lib, d, "Bar_", "rel-dup")
    new_items = stage(d, "Bar_", "rel-dup")
    new = lib.add_album(new_items)
    assert find_folder_collision(lib, new, new_items, {dup.id}) is None
    lib.add_album(stage(d, "Bar?", "rel-third"))
    third = [a for a in lib.albums() if a.mb_albumid == "rel-third"][0]
    third.move(MoveOperation.MOVE)
    assert folder_of(third) == folder_of(dup), (folder_of(third), folder_of(dup))
    collision = find_folder_collision(lib, new, new_items, {dup.id})
    assert collision is not None
    assert [o.album_id for o in collision.occupants] == [third.id], collision
print("DUPLICATE_SET_OK")

# 6. A different release with identical %aunique keys is a sibling, not an
#    occupant: it brackets into its own folder. (Hiding the new album itself
#    from the sibling lookup would drop that bracket and refuse it.)
with tempfile.TemporaryDirectory() as d:
    lib = world(d)
    first = imported(lib, d, "Twin", "rel-first")
    new_items = stage(d, "Twin", "rel-second")
    new = lib.add_album(new_items)
    planned = planned_destinations(lib, new_items, set())
    assert os.path.dirname(planned[0]) != folder_of(first), (planned, folder_of(first))
    assert find_folder_collision(lib, new, new_items, set()) is None
    hidden_self = planned_destinations(lib, new_items, {new.id})
    assert os.path.dirname(hidden_self[0]) == folder_of(first), hidden_self
print("SIBLING_BRACKET_OK")
'''


_HARNESS_CONTRACT = r'''
import hashlib
import json
import os
import subprocess
import sys
import tempfile

from beets.library import Item, Library

from tests.harness_test_support import (
    CANDIDATE_INJECTION_ALBUM_ID,
    read_candidate_injection_receipt,
    write_candidate_injection_sitecustomize,
)

OCCUPIED = sys.argv[1] == "occupied"
OCCUPANT_RELEASE = "99999999-8888-7777-6666-555555555555"


def manifest(root):
    out = []
    for current, dirs, files in os.walk(root):
        dirs.sort()
        for name in sorted(files):
            path = os.path.join(current, name)
            with open(path, "rb") as handle:
                out.append((os.path.relpath(path, root),
                            hashlib.sha256(handle.read()).hexdigest()))
    return out


def flac(path, title, artist="Purity Artist", album="Purity Album"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    subprocess.run([
        "ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:duration=0.1", "-metadata", f"artist={artist}",
        "-metadata", f"album={album}", "-metadata", f"title={title}",
        "-c:a", "flac", path,
    ], check=True)


with tempfile.TemporaryDirectory(prefix="cratedigger-folder-guard-") as root:
    source = os.path.join(root, "source")
    library_root = os.path.join(root, "library")
    beetsdir = os.path.join(root, "beets")
    os.makedirs(library_root)
    os.makedirs(beetsdir)
    flac(os.path.join(source, "01 - Source.flac"), "Source")
    db_path = os.path.join(library_root, "library.db")
    with open(os.path.join(beetsdir, "config.yaml"), "w", encoding="utf-8") as handle:
        handle.write(
            "library: %s\ndirectory: %s\nplugins: scrub\n"
            "paths:\n  default: $albumartist/$album/$track $title\n"
            "import:\n  copy: no\n  write: yes\n  move: yes\n  incremental: no\n"
            "  duplicate_keys:\n    album: [mb_albumid, discogs_albumid]\n"
            % (db_path, library_root))

    lib = Library(db_path, library_root)
    duplicate_path = os.path.join(library_root, "Purity Artist", "Old Home", "01 Source.flac")
    flac(duplicate_path, "Source")
    duplicate = lib.add_album([Item.from_path(duplicate_path)])
    duplicate.mb_albumid = CANDIDATE_INJECTION_ALBUM_ID
    duplicate.store(inherit=True)
    occupant_path = os.path.join(library_root, "Purity Artist", "Purity Album", "01 Other.flac")
    occupant_id = None
    if OCCUPIED:
        flac(occupant_path, "Other", album="Purity Album")
        occupant = lib.add_album([Item.from_path(occupant_path)])
        occupant.mb_albumid = OCCUPANT_RELEASE
        occupant.store(inherit=True)
        occupant_id = occupant.id
    duplicate_id = duplicate.id
    lib._close()

    before_source = manifest(source)
    before_library = manifest(library_root)
    shim = os.path.join(root, "shim")
    receipt = os.path.join(root, "receipt.json")
    write_candidate_injection_sitecustomize(shim, receipt)
    env = {**os.environ, "BEETSDIR": beetsdir,
           "PYTHONPATH": shim + os.pathsep + os.environ.get("PYTHONPATH", "")}
    proc = subprocess.Popen(
        [os.environ.get("BASH", "bash"), os.environ["HARNESS_WRAPPER"],
         "--noincremental", source],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env=env,
    )
    transcript = []
    for line in proc.stdout:
        message = json.loads(line)
        transcript.append(message)
        if message["type"] == "choose_match":
            proc.stdin.write(json.dumps({"action": "apply", "candidate_index": 0}) + "\n")
            proc.stdin.flush()
        elif message["type"] == "resolve_duplicate":
            assert message["duplicate_album_ids"] == [duplicate_id], message
            proc.stdin.write(json.dumps({"action": "remove"}) + "\n")
            proc.stdin.flush()
        elif message["type"] == "should_resume":
            proc.stdin.write(json.dumps({"resume": False}) + "\n")
            proc.stdin.flush()
    stderr = proc.stderr.read()
    assert proc.wait() == 0, (transcript, stderr)
    read_candidate_injection_receipt(receipt)
    types = [m["type"] for m in transcript]
    assert "resolve_duplicate" in types, (transcript[-2:], stderr)
    collisions = [m for m in transcript if m["type"] == "folder_collision"]

    lib = Library(db_path, library_root)
    albums = {a.id: a for a in lib.albums()}
    library_files = [p for p, _ in manifest(library_root)]
    assert not any(".1." in os.path.basename(p) for p in library_files), library_files
    if OCCUPIED:
        assert len(collisions) == 1, (types, stderr)
        collision = collisions[0]
        assert [o["beets_album_id"] for o in collision["occupants"]] == [occupant_id], collision
        assert collision["folder"] == os.path.dirname(occupant_path), collision
        assert duplicate_id in collision["excluded_album_ids"], collision
        # Nothing moved, nothing removed: the duplicate survives with its
        # files, the occupant is untouched, the refused row is gone, and
        # the staged source is byte-identical.
        assert set(albums) == {duplicate_id, occupant_id}, albums
        assert manifest(library_root) == before_library or all(
            p == "library.db" for p, _ in
            set(manifest(library_root)) ^ set(before_library)
        ), (before_library, manifest(library_root))
        assert os.path.isfile(duplicate_path), duplicate_path
        assert os.path.isfile(occupant_path), occupant_path
        assert manifest(source) == before_source, (before_source, manifest(source))
        assert len(list(lib.items())) == 2, list(lib.items())
        print("HARNESS_REFUSAL_OK")
    else:
        assert collisions == [], collisions
        assert duplicate_id not in albums, albums
        assert not os.path.exists(duplicate_path), duplicate_path
        (new,) = albums.values()
        (item,) = new.items()
        assert os.path.dirname(os.fsdecode(item.path)) == os.path.dirname(occupant_path), item.path
        print("HARNESS_CONTROL_OK")
'''


class TestFolderGuardRealHarness(unittest.TestCase):
    def _run(self, mode: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-c", _HARNESS_CONTRACT, mode],
            cwd=_REPO,
            env={
                **os.environ,
                "PYTHONPATH": _REPO + os.pathsep + os.environ.get("PYTHONPATH", ""),
                "HARNESS_WRAPPER": os.path.join(_REPO, "harness", "run_beets_harness.sh"),
            },
            capture_output=True, text=True, check=False,
        )

    def test_occupied_folder_refuses_before_any_file_mutation(self) -> None:
        proc = self._run("occupied")
        self.assertEqual(
            proc.returncode, 0,
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
        self.assertIn("HARNESS_REFUSAL_OK", proc.stdout)

    def test_unoccupied_folder_imports_and_replaces_duplicate(self) -> None:
        proc = self._run("control")
        self.assertEqual(
            proc.returncode, 0,
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
        self.assertIn("HARNESS_CONTROL_OK", proc.stdout)


class TestFolderGuardRealBeets(unittest.TestCase):
    def test_folder_guard_against_real_beets(self) -> None:
        proc = subprocess.run(
            [sys.executable, "-c", _CONTRACT],
            cwd=_REPO,
            env={**os.environ,
                 "PYTHONPATH": _REPO + os.pathsep + os.environ.get("PYTHONPATH", "")},
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(
            proc.returncode, 0,
            f"folder guard contract failed\nSTDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}",
        )
        for marker in ("EQUIVALENCE_OK", "REPLACE_RULE_COLLISION_OK",
                       "UNOWNED_FILES_OK", "DUPLICATE_SET_OK",
                       "SIBLING_BRACKET_OK"):
            self.assertIn(marker, proc.stdout)


class TestFolderGuardAudioExtensions(unittest.TestCase):
    def test_mirror_matches_pipeline_audio_extensions(self) -> None:
        from harness.folder_guard import AUDIO_EXTENSIONS_DOTTED
        from lib.quality.filetypes import (
            AUDIO_EXTENSIONS_DOTTED as PIPELINE_AUDIO_EXTENSIONS_DOTTED,
        )

        self.assertEqual(AUDIO_EXTENSIONS_DOTTED, PIPELINE_AUDIO_EXTENSIONS_DOTTED)


if __name__ == "__main__":
    unittest.main()
