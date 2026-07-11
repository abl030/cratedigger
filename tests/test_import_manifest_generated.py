#!/usr/bin/env python3
"""Generated download-manifest lifecycle tests — issue #548 method.

Property-based tests over the download-manifest seams touched by #550:
the ``ActiveDownloadState`` persist/reconstruct round trip
(``lib/download.py::build_active_download_state`` /
``lib/download_reconstruction.py::reconstruct_grab_list_entry``) and the
on-disk manifest guard
(``lib/import_manifest.py::check_audio_manifest`` /
``tracked_audio_paths_for_downloads`` / ``audio_relative_paths``) used by
``_check_staged_audio_manifest`` at validation.

Two properties:

1. **State round-trip preserves the manifest** — a ``GrabListEntry`` with
   1..3 discs of ``DownloadFile`` rows (multiple ``file_dir``s, ``disk_no``
   set, unicode-ish names, some with ``local_path`` set) survives
   ``build_active_download_state`` -> ``to_json`` -> ``from_json`` ->
   ``reconstruct_grab_list_entry`` with the exact same ``(username,
   filename)`` coverage and the same ``file_dir``/``disk_no``/
   ``disk_count``/``size``/``local_path`` per file — coverage never shrinks
   or grows through persistence.
2. **On-disk check oracle** — for a manifest derived from
   ``tracked_audio_paths_for_downloads`` and a generated on-disk world
   (some manifest files present, some missing, extra audio files, non-audio
   noise), ``check_audio_manifest`` reports ``ok`` iff the on-disk audio set
   exactly equals the manifest set, and its ``extra_audio``/``missing_audio``
   lists match the world exactly. Generated relative paths are constrained
   to a safe vocabulary (posix ``/`` separators, no ``..``/absolute paths,
   no embedded backslashes) so the oracle can be computed directly from what
   the test wrote to disk instead of re-deriving ``_safe_relpath``'s
   normalization rules.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import os
import shutil
import sys
import tempfile
import unittest
from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given
from hypothesis import strategies as st

from lib.download import build_active_download_state
from lib.download_reconstruction import reconstruct_grab_list_entry
from lib.grab_list import DownloadFile
from lib.import_manifest import (
    ManifestCheck,
    audio_relative_paths,
    check_audio_manifest,
    tracked_audio_paths_for_downloads,
)
from lib.quality import ActiveDownloadState
from tests.helpers import make_grab_list_entry, make_request_row

# ============================================================================
# Property 1 — ActiveDownloadState persist/reconstruct round trip
# ============================================================================

_ROUND_TRIP_USERNAMES = ("peer0", "peer1", "péer♪2", "USER_X")
_ROUND_TRIP_FRAGMENTS = (
    "Artist", "Ártîst", "de Français", "日本語", "☆Star☆", "Ω_Beta", "plain_name",
)
_ROUND_TRIP_EXTS = ("flac", "mp3", "opus", "m4a", "wav")
_LAST_STATES = (None, "InProgress", "Completed, Succeeded", "Completed, Errored")


@st.composite
def _round_trip_files(draw) -> list[DownloadFile]:
    """1..3 discs of DownloadFile rows with unique (username, filename)
    keys by construction: each filename embeds its disc + track index, so
    no two generated files can collide regardless of the sampled username
    or fragment."""
    disc_count = draw(st.integers(min_value=1, max_value=3))
    multi = disc_count > 1
    files: list[DownloadFile] = []
    for disc in range(1, disc_count + 1):
        track_count = draw(st.integers(min_value=1, max_value=4))
        for idx in range(1, track_count + 1):
            username = draw(st.sampled_from(_ROUND_TRIP_USERNAMES))
            frag = draw(st.sampled_from(_ROUND_TRIP_FRAGMENTS))
            ext = draw(st.sampled_from(_ROUND_TRIP_EXTS))
            file_dir = f"{username}\\Music\\{frag}\\Disc{disc}"
            filename = f"{file_dir}\\{idx:02d} {frag}.{ext}"
            size = draw(st.integers(min_value=0, max_value=80_000_000))
            local_path = (
                f"/downloads/complete/{username}/{disc}-{idx}.{ext}"
                if draw(st.booleans()) else None
            )
            disk_no = disc if multi else draw(st.none() | st.just(disc))
            disk_count = (
                disc_count if multi else draw(st.none() | st.just(disc_count)))
            files.append(DownloadFile(
                filename=filename,
                id=f"tid-{disc}-{idx}",
                file_dir=file_dir,
                username=username,
                size=size,
                disk_no=disk_no,
                disk_count=disk_count,
                retry=draw(st.integers(min_value=0, max_value=6)),
                bytes_transferred=draw(st.integers(min_value=0, max_value=size)),
                last_state=draw(st.sampled_from(_LAST_STATES)),
                local_path=local_path,
            ))
    return files


def _manifest_snapshot(
    files: "list[DownloadFile]",
) -> dict[tuple[str, str], tuple]:
    """(username, filename) -> the fields the manifest round trip owes."""
    return {
        (f.username, f.filename): (f.file_dir, f.disk_no, f.disk_count, f.size,
                                    f.local_path)
        for f in files
    }


def assert_manifest_round_trip_preserved(
    expected: dict[tuple[str, str], tuple],
    actual: dict[tuple[str, str], tuple],
) -> None:
    """Round-trip checker (module-level for the known-bad self-test)."""
    if expected.keys() != actual.keys():
        raise AssertionError(
            "file coverage changed through persistence: "
            f"missing={sorted(expected.keys() - actual.keys())} "
            f"extra={sorted(actual.keys() - expected.keys())}")
    diffs = [
        f"{key}: expected={expected[key]!r} actual={actual[key]!r}"
        for key in expected if expected[key] != actual[key]
    ]
    if diffs:
        raise AssertionError(
            "manifest fields diverged through persistence:\n  "
            + "\n  ".join(diffs))


# Pinned regression: a 3-disc unicode grab spanning two peers, retries,
# in-flight and terminal states, and a mix of stamped/unstamped local paths
# — the full shape the property's strategy can produce, pinned so it always
# runs even at the bounded suite tier.
_PINNED_ROUND_TRIP_FILES = [
    DownloadFile(
        filename="péer♪2\\Music\\Ártîst\\Disc1\\01 Ártîst.flac", id="t1",
        file_dir="péer♪2\\Music\\Ártîst\\Disc1", username="péer♪2",
        size=30_000_000, disk_no=1, disk_count=3, retry=2,
        bytes_transferred=1000, last_state="InProgress", local_path=None,
    ),
    DownloadFile(
        filename="péer♪2\\Music\\日本語\\Disc2\\01 日本語.mp3", id="t2",
        file_dir="péer♪2\\Music\\日本語\\Disc2", username="péer♪2",
        size=8_000_000, disk_no=2, disk_count=3, retry=0,
        bytes_transferred=8_000_000, last_state="Completed, Succeeded",
        local_path="/downloads/complete/péer♪2/2-01.mp3",
    ),
    DownloadFile(
        filename="USER_X\\Music\\☆Star☆\\Disc3\\01 ☆Star☆.opus", id="t3",
        file_dir="USER_X\\Music\\☆Star☆\\Disc3", username="USER_X",
        size=5_000_000, disk_no=3, disk_count=3, retry=5,
        bytes_transferred=0, last_state="Completed, Errored", local_path=None,
    ),
]


class TestGeneratedManifestRoundTrip(unittest.TestCase):
    """Property 1: persist/reconstruct preserves the download manifest."""

    @given(files=_round_trip_files())
    @example(files=_PINNED_ROUND_TRIP_FILES)
    def test_state_round_trip_preserves_manifest(self, files):
        entry = make_grab_list_entry(
            album_id=42, files=files, filetype="flac",
            db_request_id=42, db_source="request",
            db_search_filetype_override=None, db_target_format=None,
        )
        state = build_active_download_state(entry)
        restored = ActiveDownloadState.from_json(state.to_json())
        request_row = make_request_row(id=42)

        reconstructed = reconstruct_grab_list_entry(request_row, restored)

        assert_manifest_round_trip_preserved(
            _manifest_snapshot(files), _manifest_snapshot(reconstructed.files))


# ============================================================================
# Property 2 — on-disk manifest check oracle
# ============================================================================
#
# check_audio_manifest's allowed_audio comes, in production, from
# tracked_audio_paths_for_downloads(album_data.files) (see
# lib/download_processing.py::_check_staged_audio_manifest). Relative paths
# are constrained to a safe vocabulary (posix separators, no ".."/absolute
# paths, no backslashes) so the "what's really on disk" oracle can be read
# straight off what the test wrote, instead of re-deriving _safe_relpath's
# normalization rules.

_DISK_TRACK_STEMS = (
    "01 Track", "02 Ütf-8 Track", "曲 三", "B-Side", "07 - Song", "Track (Live)",
)
_DISK_AUDIO_EXTS = ("flac", "mp3", "opus", "m4a", "wav", "ogg", "aac", "wma")
_DISK_NON_AUDIO_EXTS = ("jpg", "png", "nfo", "cue", "log", "m3u", "txt")


@st.composite
def _oracle_files(draw) -> list[DownloadFile]:
    disc_count = draw(st.integers(min_value=1, max_value=2))
    multi = disc_count > 1
    files: list[DownloadFile] = []
    for disc in range(1, disc_count + 1):
        track_count = draw(st.integers(min_value=1, max_value=4))
        for idx in range(1, track_count + 1):
            stem = draw(st.sampled_from(_DISK_TRACK_STEMS))
            ext = draw(st.sampled_from(_DISK_AUDIO_EXTS))
            filename = f"remote\\peer\\Album\\{idx:02d} {stem}.{ext}"
            files.append(DownloadFile(
                filename=filename, id="", file_dir="remote\\peer\\Album",
                username="peer", size=1000,
                disk_no=disc if multi else None,
                disk_count=disc_count if multi else None,
            ))
    return files


@dataclass(frozen=True)
class ManifestDiskWorld:
    """A manifest (real ``tracked_audio_paths_for_downloads`` output) plus
    an on-disk world: which manifest entries are actually present, which
    extra (unlisted) audio files exist, and which non-audio noise exists."""
    manifest: tuple[str, ...]
    present: tuple[str, ...]
    extra_audio: tuple[str, ...]
    noise: tuple[str, ...]


@st.composite
def manifest_disk_worlds(draw) -> ManifestDiskWorld:
    files = draw(_oracle_files())
    manifest = tuple(tracked_audio_paths_for_downloads(files))
    present = tuple(sorted(p for p in manifest if draw(st.booleans())))

    extra_audio: set[str] = set()
    for _ in range(draw(st.integers(min_value=0, max_value=3))):
        stem = draw(st.sampled_from(_DISK_TRACK_STEMS))
        ext = draw(st.sampled_from(_DISK_AUDIO_EXTS))
        subdir = draw(st.sampled_from((None, "Bonus", "Extras")))
        name = f"{stem} extra.{ext}"
        rel = name if subdir is None else f"{subdir}/{name}"
        if rel not in manifest:
            extra_audio.add(rel)

    noise: set[str] = set()
    for _ in range(draw(st.integers(min_value=0, max_value=3))):
        stem = draw(st.sampled_from(_DISK_TRACK_STEMS))
        ext = draw(st.sampled_from(_DISK_NON_AUDIO_EXTS))
        noise.add(f"{stem}.{ext}")

    return ManifestDiskWorld(
        manifest=manifest, present=present,
        extra_audio=tuple(sorted(extra_audio)), noise=tuple(sorted(noise)))


def _write_relative(root: str, rel: str) -> None:
    full = os.path.join(root, *rel.split("/"))
    os.makedirs(os.path.dirname(full), exist_ok=True)
    open(full, "wb").close()


def _run_disk_world(world: ManifestDiskWorld) -> tuple[ManifestCheck, set[str]]:
    root = tempfile.mkdtemp(prefix="cratedigger-manifest-gen-")
    try:
        for rel in world.present:
            _write_relative(root, rel)
        for rel in world.extra_audio:
            _write_relative(root, rel)
        for rel in world.noise:
            _write_relative(root, rel)

        check = check_audio_manifest(root, world.manifest)
        on_disk_audio = set(audio_relative_paths(root))
        return check, on_disk_audio
    finally:
        shutil.rmtree(root, ignore_errors=True)


def assert_manifest_check_oracle(
    world: ManifestDiskWorld,
    check: ManifestCheck,
    on_disk_audio: set[str],
) -> None:
    """On-disk check oracle (module-level for the known-bad self-test)."""
    expected_on_disk = set(world.present) | set(world.extra_audio)
    if on_disk_audio != expected_on_disk:
        raise AssertionError(
            "audio_relative_paths walk diverged from the materialized "
            f"world: found={sorted(on_disk_audio)} "
            f"expected={sorted(expected_on_disk)}")

    expected_extra = sorted(set(world.extra_audio))
    expected_missing = sorted(set(world.manifest) - set(world.present))
    if check.extra_audio != expected_extra:
        raise AssertionError(
            f"extra_audio diverged: check={check.extra_audio} "
            f"expected={expected_extra}")
    if check.missing_audio != expected_missing:
        raise AssertionError(
            f"missing_audio diverged: check={check.missing_audio} "
            f"expected={expected_missing}")
    expected_ok = not expected_extra and not expected_missing
    if check.ok != expected_ok:
        raise AssertionError(
            f"check.ok={check.ok} expected={expected_ok} "
            f"(extra={expected_extra}, missing={expected_missing})")


# Pinned regression: a two-track manifest with one file missing, one extra
# audio file nested under a bonus subdirectory, and non-audio noise at the
# root — the full mixed shape the property's strategy can produce.
_PINNED_DISK_WORLD = ManifestDiskWorld(
    manifest=("01 Track.flac", "Disk 2 - 01 Track.flac"),
    present=("01 Track.flac",),
    extra_audio=("Bonus/01 Track extra.mp3",),
    noise=("cover.jpg",),
)


class TestGeneratedManifestDiskOracle(unittest.TestCase):
    """Property 2: check_audio_manifest passes iff disk == manifest."""

    @given(world=manifest_disk_worlds())
    @example(world=_PINNED_DISK_WORLD)
    def test_check_passes_iff_disk_matches_manifest_exactly(self, world):
        check, on_disk_audio = _run_disk_world(world)
        assert_manifest_check_oracle(world, check, on_disk_audio)


# ============================================================================
# Property 3 — known-bad self-tests for the invariant checkers
# ============================================================================

class TestManifestCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: every checker above must trip on a planted
    violation of the invariant it claims to enforce."""

    def test_round_trip_checker_trips_on_dropped_file(self):
        key = ("peer0", "peer0\\Music\\01.flac")
        with self.assertRaises(AssertionError):
            assert_manifest_round_trip_preserved(
                {key: ("peer0\\Music", None, None, 100, None)}, {})

    def test_round_trip_checker_trips_on_invented_file(self):
        key = ("peer0", "peer0\\Music\\01.flac")
        with self.assertRaises(AssertionError):
            assert_manifest_round_trip_preserved(
                {}, {key: ("peer0\\Music", None, None, 100, None)})

    def test_round_trip_checker_trips_on_field_drift(self):
        key = ("peer0", "peer0\\Music\\01.flac")
        expected: dict[tuple[str, str], tuple] = {
            key: ("peer0\\Music", 1, 2, 100, "/downloads/x")}
        actual: dict[tuple[str, str], tuple] = {
            key: ("peer0\\Music", 1, 2, 999, "/downloads/x")}
        with self.assertRaises(AssertionError):
            assert_manifest_round_trip_preserved(expected, actual)

    def test_disk_oracle_checker_trips_on_walk_mismatch(self):
        world = ManifestDiskWorld(
            manifest=("01.flac",), present=("01.flac",),
            extra_audio=(), noise=())
        check = ManifestCheck(extra_audio=[], missing_audio=[])
        with self.assertRaises(AssertionError):
            # The materialized world wrote 01.flac, but the walk claims
            # nothing is there.
            assert_manifest_check_oracle(world, check, set())

    def test_disk_oracle_checker_trips_on_missed_extra_audio(self):
        world = ManifestDiskWorld(
            manifest=("01.flac",), present=("01.flac",),
            extra_audio=("bonus.flac",), noise=())
        check = ManifestCheck(extra_audio=[], missing_audio=[])  # wrongly "clean"
        with self.assertRaises(AssertionError):
            assert_manifest_check_oracle(
                world, check, {"01.flac", "bonus.flac"})

    def test_disk_oracle_checker_trips_on_missed_missing_audio(self):
        world = ManifestDiskWorld(
            manifest=("01.flac", "02.flac"), present=("01.flac",),
            extra_audio=(), noise=())
        check = ManifestCheck(extra_audio=[], missing_audio=[])  # wrongly "clean"
        with self.assertRaises(AssertionError):
            assert_manifest_check_oracle(world, check, {"01.flac"})

    def test_disk_oracle_checker_trips_on_wrong_ok_flag(self):
        """``ok`` is a derived property on the real ManifestCheck, so a
        genuine instance can never disagree with its own lists — this
        guards the checker's ``ok`` assertion against a future ``ok``
        property regression (e.g. only checking one of the two lists) via
        a duck-typed stand-in, not a real ManifestCheck."""
        world = ManifestDiskWorld(
            manifest=("01.flac",), present=("01.flac",),
            extra_audio=(), noise=())
        broken_check = SimpleNamespace(extra_audio=[], missing_audio=[], ok=False)
        with self.assertRaises(AssertionError):
            assert_manifest_check_oracle(
                world, cast(ManifestCheck, broken_check), {"01.flac"})


if __name__ == "__main__":
    unittest.main()
