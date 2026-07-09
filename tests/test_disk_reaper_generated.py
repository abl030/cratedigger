#!/usr/bin/env python3
"""Generated + pinned tests for the on-disk orphan reaper (issue #550 defect 3).

``lib/slskd_transfers.py::reap_disk_orphans`` reasons purely from
filesystem + DB state (``ctx.cfg.slskd_download_dir`` + the
``downloading`` rows) because a completed-but-unconsumed download has no
slskd-side handle left once ``remove_completed_downloads()`` purges the
transfer record at the end of the cycle. Seven invariants, each shipped
as a deterministic pin (``TestDiskReaperDeterministicPins`` /
``TestCanonicalDerivationParity``) AND a generated property
(``TestGeneratedDiskReaperInvariants`` /
``TestGeneratedCanonicalDerivationParity``):

1. **Within-root** — the reaper never removes anything outside
   ``slskd_download_dir``, and never removes the root itself.
2. **Ownership** — no file under a ``downloading`` row's canonical
   folder, and no stamped ``local_path``, is ever removed regardless of
   age.
3. **Quarantine** — nothing under ``failed_imports/`` is ever touched.
4. **Age** — no file younger than ``ORPHAN_MIN_AGE_DAYS`` is removed.
5. **Empty-dir-only pruning** — directories are only removed when empty
   (never a recursive ``rmtree``); an always-empty stale directory is
   pruned by its own mtime, a fresh one survives.
6. **Fail-closed ownership** — if ANY downloading row's
   ``active_download_state`` is missing or undecodable, the sweep
   aborts with ZERO deletions for the cycle: partial ownership
   knowledge must never make an unparseable row's files reap-eligible.
7. **Derivation parity** — the canonical folder the reaper protects is
   byte-identical to the folder materialize computes for the same row
   (real ``reconstruct_grab_list_entry`` +
   ``_canonical_import_folder_path`` on one side, the reaper's
   ``_protected_paths_for_downloading`` on the other), guarding the
   #546 projection-drift class.

Checkers are module-level functions with known-bad self-tests
(``TestDiskReaperCheckerTripsOnViolations``) per the house method
(CLAUDE.md "Bug Hunting — Generated-First" / code-quality.md Red/Green
TDD). Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import ast
import inspect
import os
import shutil
import sys
import tempfile
import time
import unittest
from dataclasses import dataclass
from enum import Enum
from typing import Any
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

from lib.download import build_active_download_state, reconstruct_grab_list_entry
from lib.download_processing import _canonical_import_folder_path
from lib.processing_paths import (
    attempt_fingerprint,
    canonical_processing_path,
    normalize_processing_path,
    sanitize_processing_folder_name,
)
from lib.quality import ActiveDownloadState
from lib.slskd_transfers import (
    ORPHAN_MIN_AGE_DAYS,
    DiskReapSummary,
    _protected_paths_for_downloading,
    reap_disk_orphans,
)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_ctx_with_fake_db,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
)

_OLD_DAYS = ORPHAN_MIN_AGE_DAYS + 3
_YOUNG_DAYS = 1  # well inside the grace window

_ACTIVE_ARTIST = "Active Artist"
_ACTIVE_TITLE = "Active Album"
_ACTIVE_YEAR = "2020"


# ============================================================================
# Shared fixtures
# ============================================================================

def _write_aged_file(path: str, *, age_days: float, size: int = 16) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"\0" * size)
    ts = time.time() - age_days * 86400
    os.utime(path, (ts, ts))


def _age_dir(path: str, *, age_days: float) -> None:
    ts = time.time() - age_days * 86400
    os.utime(path, (ts, ts))


def _downloading_row_and_canonical(
    *,
    request_id: int,
    artist: str,
    title: str,
    year: str,
    file_pairs: list[tuple[str, str]],
    root: str,
    local_paths: dict[tuple[str, str], str] | None = None,
) -> tuple[dict[str, Any], str]:
    """Build a ``downloading`` row + its canonical folder path the SAME
    way materialize computes it, for seeding into ``FakePipelineDB``.

    Uses the real ``build_active_download_state`` / ``attempt_fingerprint``
    / ``canonical_processing_path`` — never a reimplementation of the
    folder-name derivation.
    """
    local_paths = local_paths or {}
    files = []
    for username, filename in file_pairs:
        df = make_download_file(
            username=username, filename=filename, file_dir="peer\\Album")
        local_path = local_paths.get((username, filename))
        if local_path is not None:
            df.local_path = local_path
        files.append(df)

    entry = make_grab_list_entry(
        album_id=request_id, files=files,
        artist=artist, title=title, year=year)
    state = build_active_download_state(entry)
    raw_state = msgspec.to_builtins(state)
    row = make_request_row(
        id=request_id,
        status="downloading",
        artist_name=artist,
        album_title=title,
        year=int(year),
        active_download_state=raw_state,
    )
    fingerprint = attempt_fingerprint(file_pairs)
    canonical = canonical_processing_path(
        artist=artist, title=title, year=year,
        slskd_download_dir=root, attempt_fingerprint=fingerprint)
    return row, canonical


def _make_ctx(root: str, rows: list[dict[str, Any]] | None = None) -> Any:
    fake_db = FakePipelineDB()
    for row in rows or []:
        fake_db.seed_request(row)
    cfg = MagicMock()
    cfg.slskd_download_dir = root
    return make_ctx_with_fake_db(fake_db, cfg=cfg)


# ============================================================================
# Deterministic pins
# ============================================================================

class TestDiskReaperDeterministicPins(unittest.TestCase):
    def test_boosh_shape_orphans_reaped_active_canonical_untouched(self):
        """Orphaned multi-disc folders (CD 01/CD 03) from an abandoned
        attempt are reaped; a currently-downloading request's fingerprinted
        canonical folder in the SAME root is left alone."""
        with tempfile.TemporaryDirectory() as root:
            boosh_root = os.path.join(
                root, "The Mighty Boosh - The Mighty Boosh (2007)")
            _write_aged_file(
                os.path.join(boosh_root, "CD 01", "01 Track.flac"),
                age_days=_OLD_DAYS)
            _write_aged_file(
                os.path.join(boosh_root, "CD 03", "01 Track.flac"),
                age_days=_OLD_DAYS)

            row, canonical = _downloading_row_and_canonical(
                request_id=1, artist=_ACTIVE_ARTIST, title=_ACTIVE_TITLE,
                year=_ACTIVE_YEAR,
                file_pairs=[("peer1", "peer1\\Album\\01.flac")], root=root)
            _write_aged_file(
                os.path.join(canonical, "01.flac"), age_days=_OLD_DAYS)

            ctx = _make_ctx(root, rows=[row])
            summary = reap_disk_orphans(ctx)

            self.assertFalse(os.path.exists(boosh_root))
            self.assertTrue(os.path.isdir(canonical))
            self.assertTrue(os.path.exists(os.path.join(canonical, "01.flac")))
            self.assertEqual(summary.removed, 2)

    def test_stale_bare_canonical_folder_reaped(self):
        """A pre-PR#560 bare canonical folder (no fingerprint suffix) is
        an ordinary orphan — no current attempt computes that name."""
        with tempfile.TemporaryDirectory() as root:
            bare = os.path.join(root, "Old Artist - Old Album (2015)")
            _write_aged_file(os.path.join(bare, "01.flac"), age_days=_OLD_DAYS)

            ctx = _make_ctx(root)
            summary = reap_disk_orphans(ctx)

            self.assertFalse(os.path.exists(bare))
            self.assertEqual(summary.removed, 1)

    def test_failed_imports_quarantine_never_touched(self):
        """Wrong Match cards reference failed_imports/ paths — never reap,
        no matter how old."""
        with tempfile.TemporaryDirectory() as root:
            quarantined = os.path.join(root, "failed_imports", "Some Album")
            target = os.path.join(quarantined, "01.flac")
            _write_aged_file(target, age_days=_OLD_DAYS * 10)

            ctx = _make_ctx(root)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(target))
            self.assertEqual(summary.removed, 0)

    def test_fresh_orphan_folder_not_touched(self):
        """A folder younger than the grace window survives even with no
        DB owner at all."""
        with tempfile.TemporaryDirectory() as root:
            fresh = os.path.join(root, "Fresh Artist - Fresh Album (2026)")
            target = os.path.join(fresh, "01.flac")
            _write_aged_file(target, age_days=_YOUNG_DAYS)

            ctx = _make_ctx(root)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(target))
            self.assertEqual(summary.removed, 0)

    def test_blank_download_dir_is_a_no_op(self):
        """No configured root -> never walk anything (never walks '/')."""
        ctx = _make_ctx("")
        summary = reap_disk_orphans(ctx)
        self.assertEqual(summary, DiskReapSummary())

    def test_undecodable_ownership_aborts_sweep_fail_closed(self):
        """A downloading row whose state can't be decoded (or is missing)
        must abort the ENTIRE sweep — its files can't be told apart from
        orphans, so nothing anywhere gets deleted this cycle."""
        for desc, bad_state in (
            ("missing state", None),
            ("undecodable state", {"garbage": True}),
        ):
            with self.subTest(desc=desc):
                with tempfile.TemporaryDirectory() as root:
                    orphan = os.path.join(
                        root, "Orphan Artist - Orphan Album (2001)", "01.flac")
                    _write_aged_file(orphan, age_days=_OLD_DAYS)
                    stale_empty = os.path.join(root, "Empty Stale Folder")
                    os.makedirs(stale_empty)
                    _age_dir(stale_empty, age_days=_OLD_DAYS)
                    bad_row = make_request_row(
                        id=7, status="downloading",
                        active_download_state=bad_state)
                    ctx = _make_ctx(root, rows=[bad_row])

                    summary = reap_disk_orphans(ctx)

                    self.assertTrue(os.path.exists(orphan))
                    self.assertTrue(os.path.isdir(stale_empty))
                    self.assertTrue(summary.aborted)
                    self.assertEqual(summary.removed, 0)
                    self.assertEqual(summary.pruned_dirs, 0)

    def test_one_undecodable_row_aborts_despite_healthy_rows(self):
        """ANY undecodable downloading row aborts — a healthy sibling row
        with a decodable state doesn't rescue the sweep."""
        with tempfile.TemporaryDirectory() as root:
            orphan = os.path.join(
                root, "Orphan Artist - Orphan Album (2001)", "01.flac")
            _write_aged_file(orphan, age_days=_OLD_DAYS)
            good_row, _canonical = _downloading_row_and_canonical(
                request_id=1, artist=_ACTIVE_ARTIST, title=_ACTIVE_TITLE,
                year=_ACTIVE_YEAR,
                file_pairs=[("peer1", "peer1\\Album\\01.flac")], root=root)
            bad_row = make_request_row(
                id=7, status="downloading",
                active_download_state={"garbage": True})
            ctx = _make_ctx(root, rows=[good_row, bad_row])

            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(orphan))
            self.assertTrue(summary.aborted)
            self.assertEqual(summary.removed, 0)


# ============================================================================
# Generated property
# ============================================================================

class _Zone(Enum):
    ORPHAN = "orphan"
    PROTECTED = "protected"
    FAILED_IMPORTS = "failed_imports"
    STALE_BARE = "stale_bare"


@dataclass(frozen=True)
class _FileSpec:
    zone: _Zone
    folder: str      # top-level orphan folder name (ORPHAN zone only)
    nested: bool     # place under a "Disc 1" subfolder (ORPHAN zone only)
    old: bool
    size: int
    ext: str


@dataclass(frozen=True)
class DiskReaperWorld:
    files: tuple[_FileSpec, ...]
    stray_protected: bool
    empty_stale_dir: bool
    empty_fresh_dir: bool
    # A second downloading row with an undecodable state — the sweep must
    # abort fail-closed: everything (however old/orphaned) survives.
    undecodable_state: bool


_ORPHAN_FOLDER_NAMES = (
    "Some Artist - Some Album (2019)",
    "CD 01",
    "Weird [Folder]",
    "Another Artist - Another Album (2021)",
)
_EXTS = (".flac", ".mp3", ".jpg", ".nfo", ".cue")


@st.composite
def _disk_reaper_worlds(draw) -> DiskReaperWorld:
    files: list[_FileSpec] = []
    for _ in range(draw(st.integers(min_value=0, max_value=5))):
        files.append(_FileSpec(
            zone=_Zone.ORPHAN,
            folder=draw(st.sampled_from(_ORPHAN_FOLDER_NAMES)),
            nested=draw(st.booleans()),
            old=draw(st.booleans()),
            size=draw(st.sampled_from([0, 16, 4096])),
            ext=draw(st.sampled_from(_EXTS)),
        ))

    include_protected = draw(st.booleans())
    if include_protected:
        for _ in range(draw(st.integers(min_value=1, max_value=3))):
            files.append(_FileSpec(
                zone=_Zone.PROTECTED, folder="", nested=False,
                # Protection must hold regardless of age.
                old=draw(st.booleans()),
                size=draw(st.sampled_from([0, 16, 4096])),
                ext=draw(st.sampled_from(_EXTS)),
            ))

    if draw(st.booleans()):
        for _ in range(draw(st.integers(min_value=1, max_value=2))):
            files.append(_FileSpec(
                zone=_Zone.FAILED_IMPORTS, folder="", nested=False,
                # Quarantine must hold regardless of age too.
                old=draw(st.booleans()),
                size=draw(st.sampled_from([0, 16])),
                ext=draw(st.sampled_from(_EXTS)),
            ))

    if include_protected and draw(st.booleans()):
        files.append(_FileSpec(
            zone=_Zone.STALE_BARE, folder="", nested=False, old=True,
            size=16, ext=".flac"))

    stray_protected = include_protected and draw(st.booleans())
    empty_stale_dir = draw(st.booleans())
    empty_fresh_dir = draw(st.booleans())
    # Weighted low so most examples still exercise the deletion logic.
    undecodable_state = draw(
        st.sampled_from([False, False, False, True]))

    return DiskReaperWorld(
        files=tuple(files),
        stray_protected=stray_protected,
        empty_stale_dir=empty_stale_dir,
        empty_fresh_dir=empty_fresh_dir,
        undecodable_state=undecodable_state,
    )


@dataclass(frozen=True)
class DiskReaperRunResult:
    root: str
    summary: DiskReapSummary
    path_expected_survive: dict[str, bool]
    path_actual_survive: dict[str, bool]
    folder_expected_exists: dict[str, bool]
    folder_actual_exists: dict[str, bool]
    empty_stale_path: str | None
    empty_stale_expected_survive: bool
    empty_stale_survived: bool
    empty_fresh_path: str | None
    empty_fresh_survived: bool
    root_intact: bool
    expect_abort: bool


def _materialize_and_run(world: DiskReaperWorld) -> DiskReaperRunResult:
    parent = tempfile.mkdtemp(prefix="cratedigger-diskreap-gen-")
    root = os.path.join(parent, "slskd")
    os.makedirs(root)
    try:
        # Proves the within-root invariant on every example: a sentinel
        # living OUTSIDE slskd_download_dir (but under the same parent)
        # must never be touched.
        sentinel = os.path.join(parent, "sibling-sentinel.flac")
        _write_aged_file(sentinel, age_days=_OLD_DAYS * 10)
        path_expected: dict[str, bool] = {sentinel: True}

        active_file_pairs = [
            ("activepeer", f"activepeer\\Album\\p{i}.flac") for i in range(3)]
        local_paths: dict[tuple[str, str], str] = {}

        orphan_folder_names = sorted({
            spec.folder for spec in world.files if spec.zone == _Zone.ORPHAN})
        stray_path: str | None = None
        stray_folder_name: str | None = None
        if world.stray_protected:
            if orphan_folder_names:
                stray_folder_name = orphan_folder_names[0]
                stray_dir = os.path.join(root, stray_folder_name)
            else:
                stray_dir = os.path.join(root, "Stray Home")
            stray_path = os.path.join(stray_dir, "stray-protected.flac")
            stray_pair = ("strayuser", "strayuser\\Stray\\stray-protected.flac")
            active_file_pairs = active_file_pairs + [stray_pair]
            local_paths[stray_pair] = stray_path

        row, canonical = _downloading_row_and_canonical(
            request_id=1, artist=_ACTIVE_ARTIST, title=_ACTIVE_TITLE,
            year=_ACTIVE_YEAR, file_pairs=active_file_pairs, root=root,
            local_paths=local_paths)

        if stray_path is not None:
            # Aged old on purpose: file-level protection must hold
            # independent of directory-level protection AND independent
            # of age.
            _write_aged_file(stray_path, age_days=_OLD_DAYS)
            path_expected[stray_path] = True

        folder_has_survivor: dict[str, bool] = {
            name: False for name in orphan_folder_names}
        if stray_folder_name is not None:
            folder_has_survivor[stray_folder_name] = True

        for idx, spec in enumerate(world.files):
            if spec.zone == _Zone.ORPHAN:
                folder = os.path.join(root, spec.folder)
                if spec.nested:
                    folder = os.path.join(folder, "Disc 1")
                path = os.path.join(folder, f"track-{idx}{spec.ext}")
                _write_aged_file(
                    path,
                    age_days=_OLD_DAYS if spec.old else _YOUNG_DAYS,
                    size=spec.size)
                path_expected[path] = not spec.old
                if not spec.old:
                    folder_has_survivor[spec.folder] = True
            elif spec.zone == _Zone.PROTECTED:
                path = os.path.join(canonical, f"protected-{idx}{spec.ext}")
                _write_aged_file(
                    path,
                    age_days=_OLD_DAYS if spec.old else _YOUNG_DAYS,
                    size=spec.size)
                path_expected[path] = True  # always survives
            elif spec.zone == _Zone.FAILED_IMPORTS:
                path = os.path.join(
                    root, "failed_imports", "Quarantine",
                    f"quarantined-{idx}{spec.ext}")
                _write_aged_file(
                    path,
                    age_days=_OLD_DAYS if spec.old else _YOUNG_DAYS,
                    size=spec.size)
                path_expected[path] = True  # quarantine ignores age
            elif spec.zone == _Zone.STALE_BARE:
                bare_name = sanitize_processing_folder_name(
                    f"{_ACTIVE_ARTIST} - {_ACTIVE_TITLE} ({_ACTIVE_YEAR})")
                path = os.path.join(root, bare_name, f"legacy-{idx}{spec.ext}")
                _write_aged_file(path, age_days=_OLD_DAYS, size=spec.size)
                path_expected[path] = False  # ordinary orphan

        empty_stale_path: str | None = None
        if world.empty_stale_dir:
            empty_stale_path = os.path.join(root, "Empty Stale Folder")
            os.makedirs(empty_stale_path, exist_ok=True)
            _age_dir(empty_stale_path, age_days=_OLD_DAYS)

        empty_fresh_path: str | None = None
        if world.empty_fresh_dir:
            empty_fresh_path = os.path.join(root, "Empty Fresh Folder")
            os.makedirs(empty_fresh_path, exist_ok=True)
            # Freshly created -- mtime is already "now".

        rows = [row]
        if world.undecodable_state:
            rows.append(make_request_row(
                id=2, status="downloading",
                active_download_state={"garbage": True}))
            # Fail-closed: the sweep aborts, so EVERYTHING survives —
            # including old orphans and the stale empty directory.
            path_expected = {p: True for p in path_expected}
            folder_has_survivor = {
                name: True for name in folder_has_survivor}

        ctx = _make_ctx(root, rows=rows)
        summary = reap_disk_orphans(ctx)

        path_actual = {p: os.path.exists(p) for p in path_expected}
        folder_expected = dict(folder_has_survivor)
        folder_actual = {
            name: os.path.isdir(os.path.join(root, name))
            for name in folder_expected
        }
        # Captured BEFORE the `finally` cleanup below removes `parent`
        # (and therefore `root`, its child) -- checking os.path.isdir on
        # the returned path after this function returns would always
        # report False regardless of what the reaper actually did.
        root_intact = os.path.isdir(root)
        return DiskReaperRunResult(
            root=root,
            summary=summary,
            path_expected_survive=path_expected,
            path_actual_survive=path_actual,
            folder_expected_exists=folder_expected,
            folder_actual_exists=folder_actual,
            empty_stale_path=empty_stale_path,
            empty_stale_expected_survive=world.undecodable_state,
            empty_stale_survived=(
                empty_stale_path is not None
                and os.path.exists(empty_stale_path)),
            empty_fresh_path=empty_fresh_path,
            empty_fresh_survived=(
                empty_fresh_path is not None
                and os.path.exists(empty_fresh_path)),
            root_intact=root_intact,
            expect_abort=world.undecodable_state,
        )
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def assert_disk_reaper_invariants(result: DiskReaperRunResult) -> None:
    """Module-level checker (known-bad self-tests below)."""
    if result.expect_abort:
        if not result.summary.aborted:
            raise AssertionError(
                "sweep should have aborted fail-closed on an undecodable "
                "downloading row, but did not")
        if result.summary.removed or result.summary.pruned_dirs:
            raise AssertionError(
                f"an aborted sweep must delete NOTHING: {result.summary}")
    elif result.summary.aborted:
        raise AssertionError(
            "sweep aborted with no undecodable ownership planted")

    for path, expected in result.path_expected_survive.items():
        actual = result.path_actual_survive[path]
        if actual != expected:
            raise AssertionError(
                f"path survival diverged: path={path} "
                f"expected_survive={expected} actual_survive={actual}")

    for folder, expected in result.folder_expected_exists.items():
        actual = result.folder_actual_exists[folder]
        if actual != expected:
            raise AssertionError(
                f"orphan folder survival diverged: folder={folder} "
                f"expected_exists={expected} actual_exists={actual}")

    if result.empty_stale_path is not None and (
            result.empty_stale_survived
            != result.empty_stale_expected_survive):
        raise AssertionError(
            f"stale empty directory survival diverged: "
            f"path={result.empty_stale_path} "
            f"expected_survive={result.empty_stale_expected_survive} "
            f"actual_survive={result.empty_stale_survived}")

    if result.empty_fresh_path is not None and not result.empty_fresh_survived:
        raise AssertionError(
            f"fresh empty directory should NOT have been pruned: "
            f"{result.empty_fresh_path}")

    if not result.root_intact:
        raise AssertionError("root itself must never be removed")


# Pinned regression: a mixed-age orphan folder (one old file reaped, one
# young survivor keeping the folder alive), a protected file aged past
# the threshold (must still survive), a quarantined failed_imports file
# aged past the threshold (must still survive), a stale bare-canonical
# legacy folder, a stray protected file living inside an orphan folder,
# and both empty-directory scenarios -- the full mixed shape the
# strategy can produce.
_PINNED_WORLD = DiskReaperWorld(
    files=(
        _FileSpec(zone=_Zone.ORPHAN, folder="CD 01", nested=False,
                   old=True, size=16, ext=".flac"),
        _FileSpec(zone=_Zone.ORPHAN, folder="CD 01", nested=True,
                   old=False, size=0, ext=".jpg"),
        _FileSpec(zone=_Zone.PROTECTED, folder="", nested=False,
                   old=True, size=16, ext=".flac"),
        _FileSpec(zone=_Zone.FAILED_IMPORTS, folder="", nested=False,
                   old=True, size=16, ext=".flac"),
        _FileSpec(zone=_Zone.STALE_BARE, folder="", nested=False,
                   old=True, size=16, ext=".flac"),
    ),
    stray_protected=True,
    empty_stale_dir=True,
    empty_fresh_dir=True,
    undecodable_state=False,
)

# Pinned fail-closed world: the same full mixed shape, but with a second
# downloading row whose state can't be decoded — the sweep must abort and
# every path (however old and orphaned) must survive.
_PINNED_ABORT_WORLD = DiskReaperWorld(
    files=_PINNED_WORLD.files,
    stray_protected=True,
    empty_stale_dir=True,
    empty_fresh_dir=True,
    undecodable_state=True,
)


class TestGeneratedDiskReaperInvariants(unittest.TestCase):
    """Properties 1-6: within-root, ownership, quarantine, age,
    empty-dir-only pruning, fail-closed ownership -- patrolled together
    over generated worlds."""

    @given(world=_disk_reaper_worlds())
    @example(world=_PINNED_WORLD)
    @example(world=_PINNED_ABORT_WORLD)
    def test_reaper_respects_ownership_quarantine_age_and_root(self, world):
        result = _materialize_and_run(world)
        assert_disk_reaper_invariants(result)


# ============================================================================
# Known-bad self-tests for the invariant checker
# ============================================================================

class TestDiskReaperCheckerTripsOnViolations(unittest.TestCase):
    """Each planted violation must trip assert_disk_reaper_invariants."""

    def _base_result(self, **overrides: Any) -> DiskReaperRunResult:
        defaults: dict[str, Any] = dict(
            root=tempfile.gettempdir(),
            summary=DiskReapSummary(),
            path_expected_survive={},
            path_actual_survive={},
            folder_expected_exists={},
            folder_actual_exists={},
            empty_stale_path=None,
            empty_stale_expected_survive=False,
            empty_stale_survived=False,
            empty_fresh_path=None,
            empty_fresh_survived=False,
            root_intact=True,
            expect_abort=False,
        )
        defaults.update(overrides)
        return DiskReaperRunResult(**defaults)

    def test_trips_on_protected_or_young_path_wrongly_removed(self):
        result = self._base_result(
            path_expected_survive={"/tmp/x/owned.flac": True},
            path_actual_survive={"/tmp/x/owned.flac": False},
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_old_orphan_wrongly_kept(self):
        result = self._base_result(
            path_expected_survive={"/tmp/x/orphan.flac": False},
            path_actual_survive={"/tmp/x/orphan.flac": True},
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_orphan_folder_wrongly_surviving(self):
        result = self._base_result(
            folder_expected_exists={"CD 01": False},
            folder_actual_exists={"CD 01": True},
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_stale_empty_dir_wrongly_kept(self):
        result = self._base_result(
            empty_stale_path="/tmp/x/Empty Stale Folder",
            empty_stale_expected_survive=False,
            empty_stale_survived=True,
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_stale_empty_dir_wrongly_pruned_during_abort(self):
        result = self._base_result(
            summary=DiskReapSummary(aborted=True),
            expect_abort=True,
            empty_stale_path="/tmp/x/Empty Stale Folder",
            empty_stale_expected_survive=True,
            empty_stale_survived=False,
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_fresh_empty_dir_wrongly_removed(self):
        result = self._base_result(
            empty_fresh_path="/tmp/x/Empty Fresh Folder",
            empty_fresh_survived=False,
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_root_removed(self):
        result = self._base_result(root_intact=False)
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_missing_abort_flag(self):
        result = self._base_result(expect_abort=True)
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_deletions_during_abort(self):
        result = self._base_result(
            expect_abort=True,
            summary=DiskReapSummary(removed=1, aborted=True),
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_unexpected_abort(self):
        result = self._base_result(
            summary=DiskReapSummary(aborted=True))
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)


# ============================================================================
# Derivation parity: reaper protection == materialize's canonical folder
# ============================================================================
#
# The reaper computes its protected canonical folder from the leaf
# functions (attempt_fingerprint + canonical_processing_path); materialize
# computes the SAME folder through its wrappers
# (reconstruct_grab_list_entry -> _canonical_import_folder_path). If those
# wrappers ever drift (a different fingerprint input set, a different
# artist/title/year projection), the reaper would silently stop protecting
# the folder materialize actually writes into — the #546 projection-drift
# class. This parity test drives BOTH real production derivations from the
# same row and demands byte-equality.

def assert_canonical_derivation_parity(
    materialize_path: str,
    protected_dirs: set[str],
    root: str,
) -> None:
    """Module-level checker: the reaper's protected set contains exactly
    the quarantine dir plus the (normalized) folder materialize derives."""
    norm_expected = normalize_processing_path(materialize_path)
    quarantine = normalize_processing_path(
        os.path.join(root, "failed_imports"))
    canonical_entries = protected_dirs - {quarantine}
    if canonical_entries != {norm_expected}:
        raise AssertionError(
            "reaper protection diverged from materialize's canonical "
            f"derivation: materialize={norm_expected!r} "
            f"reaper={sorted(canonical_entries)!r}")


def _parity_row(
    artist: str,
    title: str,
    year: int | None,
    pairs: list[tuple[str, str]],
) -> dict[str, Any]:
    """Downloading row whose state round-trips the JSONB wire shape."""
    files = [
        make_download_file(
            username=username, filename=filename, file_dir="peer\\Album")
        for username, filename in pairs
    ]
    entry = make_grab_list_entry(
        album_id=9, files=files, artist=artist, title=title,
        year=str(year or ""))
    state = build_active_download_state(entry)
    return make_request_row(
        id=9, status="downloading", artist_name=artist, album_title=title,
        year=year, active_download_state=msgspec.to_builtins(state))


def _run_parity(row: dict[str, Any], root: str) -> None:
    # Materialize's derivation: the REAL wrappers production uses when
    # placing completed files.
    state = ActiveDownloadState.from_raw(row["active_download_state"])
    entry = reconstruct_grab_list_entry(row, state)
    materialize_path = _canonical_import_folder_path(entry, root)
    # The reaper's derivation.
    protected_dirs, _protected_files = _protected_paths_for_downloading(
        root, [row])
    assert_canonical_derivation_parity(materialize_path, protected_dirs, root)


class TestCanonicalDerivationParity(unittest.TestCase):
    """Invariant 7 pins: both real derivations agree byte-for-byte."""

    CASES: list[tuple[str, str, str, int | None, list[tuple[str, str]]]] = [
        ("ascii", "Test Artist", "Test Album", 2020,
         [("user1", "user1\\Music\\01.flac")]),
        ("unicode", "Sigur Rós", "Ágætis byrjun", 1999,
         [("péer", "péer\\Tónlist\\01 svefn-g-englar.flac"),
          ("péer", "péer\\Tónlist\\02 starálfur.flac")]),
        ("255-byte truncation", "The Artist", "T" * 300, 2001,
         [("user1", "user1\\Music\\01.flac")]),
        ("no year", "Test Artist", "Test Album", None,
         [("user1", "user1\\Music\\01.flac")]),
        ("empty fileset", "Test Artist", "Test Album", 2020, []),
    ]

    def test_reaper_protects_exactly_what_materialize_derives(self):
        for desc, artist, title, year, pairs in self.CASES:
            with self.subTest(desc=desc):
                _run_parity(_parity_row(artist, title, year, pairs),
                            "/downloads")


@st.composite
def _parity_inputs(draw) -> tuple[str, str, int | None, list[tuple[str, str]]]:
    artist = draw(st.text(min_size=1, max_size=60))
    title = draw(st.text(min_size=1, max_size=120))
    year = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=3000)))
    pairs = draw(st.lists(
        st.tuples(st.text(min_size=1, max_size=12),
                  st.text(min_size=1, max_size=40)),
        min_size=0, max_size=3))
    return artist, title, year, pairs


class TestGeneratedCanonicalDerivationParity(unittest.TestCase):
    """Invariant 7 property: parity holds across the input space (pure
    path computation — no filesystem)."""

    @given(inputs=_parity_inputs())
    @example(inputs=("Sigur Rós", "Ágætis byrjun", 1999,
                     [("péer", "péer\\Tónlist\\01 svefn-g-englar.flac")]))
    @example(inputs=("The Artist", "T" * 300, 2001,
                     [("user1", "user1\\Music\\01.flac")]))
    def test_derivations_agree_over_generated_inputs(self, inputs):
        artist, title, year, pairs = inputs
        _run_parity(_parity_row(artist, title, year, pairs), "/downloads")


class TestParityCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests for assert_canonical_derivation_parity."""

    def test_trips_when_canonical_folder_not_protected(self):
        with self.assertRaises(AssertionError):
            assert_canonical_derivation_parity(
                "/downloads/Artist - Album (2020) [deadbeef]",
                {normalize_processing_path("/downloads/failed_imports")},
                "/downloads")

    def test_trips_when_protected_folder_differs_by_one_byte(self):
        with self.assertRaises(AssertionError):
            assert_canonical_derivation_parity(
                "/downloads/Artist - Album (2020) [deadbeef]",
                {normalize_processing_path("/downloads/failed_imports"),
                 normalize_processing_path(
                     "/downloads/Artist - Album (2020) [deadbeee]")},
                "/downloads")


# ============================================================================
# Phase 0 wiring
# ============================================================================

class TestDiskReaperPhase0Wiring(unittest.TestCase):
    """reap_disk_orphans runs immediately after slskd orphan-transfer
    convergence, guarded by its own try/except so a sweep failure can't
    abort the cycle -- mirrors converge_slskd_orphans's existing Phase 0
    guard. There is no runnable end-to-end test of cratedigger.main() in
    this suite (nothing else in tests/ calls it either -- it would need a
    bespoke harness mocking the whole cycle); this is the same class of
    source-inspection seam test already established at
    tests/test_import_one_stages.py::test_no_preview_import_result_reuse_path_in_main,
    strengthened with an AST check of the try/except shape rather than
    plain string containment.
    """

    def _main_source(self) -> str:
        import cratedigger
        return inspect.getsource(cratedigger.main)

    def test_called_after_converge_slskd_orphans_before_phase1(self):
        source = self._main_source()
        converge_idx = source.index("converge_slskd_orphans(_module_ctx)")
        reap_idx = source.index("reap_disk_orphans(_module_ctx)")
        phase1_idx = source.index("Starting Phase 1")
        self.assertLess(converge_idx, reap_idx)
        self.assertLess(reap_idx, phase1_idx)

    def test_call_is_isolated_in_its_own_try_except_exception_block(self):
        tree = ast.parse(self._main_source())

        def _calls_reap(node: ast.AST) -> bool:
            return (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "reap_disk_orphans"
            )

        matches = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Try):
                continue
            in_body = any(
                _calls_reap(n) for stmt in node.body for n in ast.walk(stmt))
            if in_body:
                matches.append(node)

        # main() wraps its whole body in one outer crash-guard try, so a
        # bare "body contains the call" walk matches that ancestor too --
        # keep only the innermost (most specific) match.
        inner_matches = [
            m for m in matches
            if not any(
                other is not m and id(other) in {id(n) for n in ast.walk(m)}
                for other in matches
            )
        ]

        self.assertEqual(
            len(inner_matches), 1,
            "expected exactly one (innermost) try block calling "
            "reap_disk_orphans")
        node = inner_matches[0]
        self.assertTrue(node.handlers, "the try block must have a handler")
        for handler in node.handlers:
            self.assertIsInstance(
                handler.type, ast.Name,
                "handler must catch a bare exception type")
            assert isinstance(handler.type, ast.Name)
            self.assertEqual(
                handler.type.id, "Exception",
                "must catch Exception broadly so a sweep failure can't "
                "abort the cycle")
            in_handler = any(
                _calls_reap(n) for stmt in handler.body for n in ast.walk(stmt))
            self.assertFalse(
                in_handler,
                "reap_disk_orphans must not be called from its own handler")


if __name__ == "__main__":
    unittest.main()
