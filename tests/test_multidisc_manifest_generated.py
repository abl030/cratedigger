#!/usr/bin/env python3
"""Generated multi-disc grab-manifest tests — issue #550 defect #1.

THE INVARIANT (the coverage law from #550's plan): an accepted multi-disc
grab must cover every disc with unique transfer identities — for a
``matched=True`` ``try_multi_enqueue`` attempt, the persisted manifest has
no duplicate ``(username, filename)`` keys and exactly one distinct source
folder per disc.

The bug this reproduces (found by this harness on 2026-07-08, before the
live MANIFEST-TRACE window produced a capture): the per-disc match loop
shares one candidate-directory pool and never excludes an already-assigned
``(username, file_dir)``. On releases whose track titles restart per disc
(radio-series shape — request 2812, The Mighty Boosh), disc N's tracks
strict-accept an EARLIER disc's folder via the real matcher. Every
downstream stage keys by ``(username, filename)``, so the duplicate
entries collapse: the all-or-nothing gates pass on entry count while
unique coverage is partial — the manifest reaching validation covers a
subset of discs, producing the "extra audio, no missing" false
``untracked_audio`` signature.

Drives the REAL production path — ``try_multi_enqueue`` with the real
``check_for_match`` matcher over a seeded ``ctx.folder_cache`` — with only
the two leaf seams patched (network fan-out browse; the slskd enqueue
HTTP wrapper, stubbed production-shaped).

Profiles / promotion / fault-injection: docs/generated-testing.md.
"""

import configparser
import os
import sys
import unittest
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.download_ownership import DownloadOwnershipWriter
from lib.enqueue import EnqueueAttempt, try_multi_enqueue
from lib.grab_list import DownloadFile
from lib.slskd_transfers import SlskdEnqueueOutcome
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI
from tests.helpers import make_request_row


@dataclass(frozen=True)
class MultiDiscWorld:
    """One multi-disc release + one peer serving per-disc folders."""
    disc_track_counts: tuple[int, ...]
    titles_restart_per_disc: bool  # radio-series shape (collision-prone)
    folder_scheme: str             # "CD 0{d}" | "Disk {d} - Series"
    folder_order: tuple[int, ...]  # search-result ordering of disc folders

    @property
    def total_tracks(self) -> int:
        return sum(self.disc_track_counts)


@st.composite
def multi_disc_worlds(draw) -> MultiDiscWorld:
    disc_count = draw(st.integers(min_value=2, max_value=3))
    counts = tuple(
        draw(st.integers(min_value=2, max_value=5)) for _ in range(disc_count))
    order = draw(st.permutations(tuple(range(disc_count))))
    return MultiDiscWorld(
        disc_track_counts=counts,
        titles_restart_per_disc=draw(st.booleans()),
        folder_scheme=draw(st.sampled_from(("CD 0{d}", "Disk {d} - Series"))),
        folder_order=tuple(order),
    )


def _title(world: MultiDiscWorld, disc: int, index: int) -> str:
    if world.titles_restart_per_disc:
        return f"Episode {index}"
    return f"Disc {disc} Episode {index}"


def _build_harness(world: MultiDiscWorld):
    """Seed cfg/ctx/db/folders for one world; returns everything the
    production call needs plus the folder map for the oracle."""
    ini = configparser.ConfigParser()
    ini["Search Settings"] = {
        "minimum_filename_match_ratio": "0.5",
        "ignored_users": "",
        "allowed_filetypes": "flac,mp3",
        "browse_parallelism": "4",
        "browse_top_k": "20",
        "browse_global_max_workers": "32",
    }
    cfg = CratediggerConfig.from_ini(ini)
    db = FakePipelineDB()
    db.seed_request(make_request_row(id=1, status="wanted"))
    ctx = CratediggerContext(
        cfg=cfg,
        slskd=FakeSlskdAPI(),
        pipeline_db_source=FakePipelineDBSource(db),
        user_upload_speed={"peer": 10_000},
    )
    ctx.download_ownership = DownloadOwnershipWriter(db_factory=lambda: db)
    ctx.current_album_cache[1] = SimpleNamespace(
        id=1, db_request_id=1,
        title="The Complete Radio Series",
        artist_name="The Mighty Boosh",
        release_date="2004-01-01T00:00:00Z",
        db_mb_release_id="mbid-boosh", db_source="request",
        db_search_filetype_override=None, db_target_format=None)

    base = "Music\\peer\\The Complete Radio Series"
    folders: dict[str, list[dict[str, Any]]] = {}
    discs = list(range(1, len(world.disc_track_counts) + 1))
    for disc, count in zip(discs, world.disc_track_counts):
        dir_name = f"{base}\\{world.folder_scheme.format(d=disc)}"
        folders[dir_name] = [
            {"filename": f"{i:02d} - {_title(world, disc, i)}.mp3",
             "size": 999}
            for i in range(1, count + 1)
        ]
    ctx.folder_cache["peer"] = {
        dir_name: {"directory": dir_name, "files": files}
        for dir_name, files in folders.items()
    }
    ordered_dirs = [list(folders)[i] for i in world.folder_order]
    results = {"peer": {"mp3": ordered_dirs}}
    all_tracks = cast(list, [
        {"albumId": 1, "title": _title(world, disc, i), "mediumNumber": disc}
        for disc, count in zip(discs, world.disc_track_counts)
        for i in range(1, count + 1)
    ])
    release = SimpleNamespace(
        media=[SimpleNamespace(medium_number=d) for d in discs],
        track_count=world.total_tracks)
    return db, ctx, release, all_tracks, results


def _production_shaped_enqueue_stub():
    """slskd HTTP wrapper stub: accepts and returns real DownloadFile rows,
    exactly as lib/slskd_transfers.py::slskd_enqueue_with_outcome does."""
    counter = [0]

    def _enqueue(*, username, files, file_dir, ctx, **kwargs):
        downloads = []
        for file in files:
            counter[0] += 1
            downloads.append(DownloadFile(
                username=username, filename=file["filename"],
                file_dir=file_dir, size=file["size"],
                id=f"t-{counter[0]}"))
        return SlskdEnqueueOutcome(status="accepted", downloads=downloads)

    return _enqueue


def _run_world(world: MultiDiscWorld) -> tuple[EnqueueAttempt, list[dict]]:
    db, ctx, release, all_tracks, results = _build_harness(world)
    with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
         patch("lib.enqueue.slskd_enqueue_with_outcome",
               _production_shaped_enqueue_stub()):
        attempt = try_multi_enqueue(release, all_tracks, results, "mp3", ctx)
    state = db.request(1)["active_download_state"]
    return attempt, (state["files"] if state else [])


def assert_manifest_covers_all_discs(
    world: MultiDiscWorld,
    attempt: EnqueueAttempt,
    manifest_files: list[dict],
) -> None:
    """The #550 coverage invariant. matched=False is always acceptable
    (fail-closed keep-searching is archivist-correct); an ACCEPTED grab
    must cover every disc with unique transfer identities."""
    if not attempt.matched:
        return
    keys = [(f["username"], f["filename"]) for f in manifest_files]
    if len(set(keys)) != len(keys):
        dupes = sorted({k for k in keys if keys.count(k) > 1})
        raise AssertionError(
            f"accepted multi-disc manifest holds duplicate transfer "
            f"identities (entries={len(keys)}, unique={len(set(keys))}): "
            f"{dupes[:3]}")
    source_dirs = {f["file_dir"] for f in manifest_files}
    if len(source_dirs) != len(world.disc_track_counts):
        raise AssertionError(
            f"accepted {len(world.disc_track_counts)}-disc grab sourced "
            f"from {len(source_dirs)} folder(s): {sorted(source_dirs)}")
    if len(set(keys)) != world.total_tracks:
        raise AssertionError(
            f"accepted manifest unique-file coverage "
            f"{len(set(keys))} != release track count {world.total_tracks}")


# The canonical reproduction: 3-disc radio-series box, per-disc episode
# numbering — the request-2812 shape, scaled down. Before the fix, disc 2
# matched disc 1's folder and the persisted manifest covered CD 01 + CD 03
# only (16 entries, 11 unique) while reporting matched=True.
_BOOSH_WORLD = MultiDiscWorld(
    disc_track_counts=(5, 5, 6),
    titles_restart_per_disc=True,
    folder_scheme="CD 0{d}",
    folder_order=(0, 1, 2),
)


class TestGeneratedMultiDiscManifest(unittest.TestCase):
    """The #550 defect-#1 coverage property over generated worlds."""

    @given(world=multi_disc_worlds())
    @example(world=_BOOSH_WORLD)
    def test_accepted_grab_covers_every_disc(self, world):
        attempt, manifest_files = _run_world(world)
        assert_manifest_covers_all_discs(world, attempt, manifest_files)

    def test_boosh_shape_reproduction(self):
        """Deterministic regression pin for the request-2812 shape."""
        attempt, manifest_files = _run_world(_BOOSH_WORLD)
        assert_manifest_covers_all_discs(_BOOSH_WORLD, attempt, manifest_files)

    def test_unique_titles_still_grab_all_discs(self):
        """Non-colliding worlds must keep matching (the fix must not turn
        legitimate multi-disc grabs into no-matches)."""
        world = MultiDiscWorld(
            disc_track_counts=(3, 3, 4),
            titles_restart_per_disc=False,
            folder_scheme="CD 0{d}",
            folder_order=(0, 1, 2),
        )
        attempt, manifest_files = _run_world(world)
        self.assertTrue(attempt.matched)
        assert_manifest_covers_all_discs(world, attempt, manifest_files)


class TestManifestCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests for the coverage checker."""

    def _accepted(self) -> EnqueueAttempt:
        return EnqueueAttempt(matched=True, downloads=[])

    def test_trips_on_duplicate_transfer_identities(self):
        files = [
            {"username": "peer", "filename": "a\\01.mp3", "file_dir": "a"},
            {"username": "peer", "filename": "a\\01.mp3", "file_dir": "a"},
        ]
        with self.assertRaises(AssertionError):
            assert_manifest_covers_all_discs(
                _BOOSH_WORLD, self._accepted(), files)

    def test_trips_on_missing_disc_folder(self):
        files = [
            {"username": "peer", "filename": f"a\\{i:02d}.mp3",
             "file_dir": "a"}
            for i in range(1, 17)
        ]
        with self.assertRaises(AssertionError):
            assert_manifest_covers_all_discs(
                _BOOSH_WORLD, self._accepted(), files)

    def test_trips_on_short_coverage(self):
        files = [
            {"username": "peer", "filename": f"{d}\\{i:02d}.mp3",
             "file_dir": str(d)}
            for d in (1, 2, 3) for i in range(1, 4)  # 9 < 16 tracks
        ]
        with self.assertRaises(AssertionError):
            assert_manifest_covers_all_discs(
                _BOOSH_WORLD, self._accepted(), files)


if __name__ == "__main__":
    unittest.main()
