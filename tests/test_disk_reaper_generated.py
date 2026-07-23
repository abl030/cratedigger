#!/usr/bin/env python3
"""Generated + pinned tests for the on-disk orphan reaper (issue #550
defect 3, flipped to positive ledger ownership by issue #571's
good-citizen doctrine).

``lib/slskd_transfers.py::reap_disk_orphans`` reasons from filesystem +
DB state (``ctx.cfg.slskd_download_dir`` + the ``downloading`` rows +
the write-ahead transfer ledger, migration 045) because a
completed-but-unconsumed download's slskd-side handle is unreliable —
the end-of-cycle ``purge_completed_transfers`` removes stamped-owned
completed records, and unstamped/foreign ones persist indefinitely. Seven
invariants ship as deterministic pins and generated properties:

1. **Within-root** — the reaper never removes anything outside
   ``slskd_download_dir``, and never removes the root itself.
2. **Active protection** — no exact ``local_path`` stamped on a
   ``downloading`` row is removed regardless of age.
3. **Quarantine** — nothing under ``failed_imports/`` or ``wrong_matches/``
   is ever touched.
4. **Age** — no ledger-owned file younger than ``ORPHAN_MIN_AGE_DAYS``
   is removed.
5. **Empty-dir-only pruning** — only parents emptied by an exact owned
   file deletion are pruned; an already-empty directory always stays.
6. **Fail-closed ownership** — if ANY downloading row's
   ``active_download_state`` is missing or undecodable, the sweep
   aborts with ZERO deletions for the cycle: partial ownership
   knowledge must never make an unparseable row's files reap-eligible.
7. **Good-citizen positive ownership** — a file with no ledgered,
   event-stamped ``local_path`` is NEVER removed, whatever its age.

Invariant 8 is the flip's headline change and inverts the pre-#571
ORPHAN-zone doctrine this file used to pin: an aged file with NO
recognised owner used to be reaped (issue #550 defect 3's original
"anything unrecognised and old enough goes" rule) — now it must
SURVIVE, however old, and only a positively ledger-owned file is
reap-eligible. The equivalence proof for every pin this flip changed
lives in the PR/commit message.

Checkers are module-level functions with known-bad self-tests
(``TestDiskReaperCheckerTripsOnViolations``) per the house method
(CLAUDE.md "Bug Hunting — Generated-First" / code-quality.md Red/Green
TDD). Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

import inspect
import os
import shutil
import sys
import tempfile
import time
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

from lib.download import build_active_download_state
from lib.pipeline_db import TransferLedgerRow
from lib.processing_paths import (
    attempt_fingerprint,
    canonical_folder_for_row,
    canonical_processing_path,
    normalize_processing_path,
)
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

    Uses the real ``build_active_download_state`` /
    ``canonical_folder_for_row`` leaf — never a reimplementation of the
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
    canonical = canonical_folder_for_row(entry, root)
    return row, canonical


def _ledger_seed(
    fake_db: FakePipelineDB,
    *,
    request_id: int,
    file_pairs: list[tuple[str, str]],
    local_paths: dict[tuple[str, str], str] | None = None,
    with_fingerprint: bool = True,
) -> None:
    """Write-ahead ledger + completion-stamp seed mirroring production's
    two-write sequence (T1 ``record_transfer_enqueue``, T2
    ``stamp_transfer_completion`` — migration 045, issue #571) for one
    attempt's files. Every production enqueue call site ledgers
    write-ahead regardless of the row's eventual status, so a realistic
    disk-reaper world always seeds both alongside any downloading/
    request row.
    """
    fp = attempt_fingerprint(file_pairs) if with_fingerprint else None
    fake_db.record_transfer_enqueue([
        TransferLedgerRow(
            request_id=request_id, username=username, filename=filename,
            attempt_fingerprint=fp)
        for username, filename in file_pairs
    ])
    for username, filename in file_pairs:
        fake_db.confirm_transfer_enqueue(username, filename)
    for (username, filename), local_path in (local_paths or {}).items():
        fake_db.stamp_transfer_completion(
            username, filename, local_path)


def _seed_active_downloading(
    fake_db: FakePipelineDB,
    *,
    request_id: int,
    artist: str,
    title: str,
    year: str,
    file_pairs: list[tuple[str, str]],
    root: str,
    local_paths: dict[tuple[str, str], str] | None = None,
) -> str:
    """Seed a currently-``downloading`` row (active protection, the SAME
    real derivation ``_downloading_row_and_canonical`` builds) AND its
    write-ahead ledger rows -- returns the canonical folder path.

    Every production enqueue ledgers write-ahead BEFORE the row is even
    ``downloading``, so a realistic world always carries both; the
    active-protection guard and the ledger's positive-ownership record
    simply overlap for a row's CURRENT attempt.
    """
    row, canonical = _downloading_row_and_canonical(
        request_id=request_id, artist=artist, title=title, year=year,
        file_pairs=file_pairs, root=root, local_paths=local_paths)
    fake_db.seed_request(row)
    _ledger_seed(
        fake_db, request_id=request_id, file_pairs=file_pairs,
        local_paths=local_paths)
    return canonical


def _seed_owned_inactive(
    fake_db: FakePipelineDB,
    *,
    request_id: int,
    artist: str,
    title: str,
    year: str,
    file_pairs: list[tuple[str, str]],
    root: str,
    status: str = "imported",
) -> str:
    """Seed a request whose row is NOT ``downloading`` (contributes
    nothing to active protection) but whose files ARE write-ahead
    ledgered from a past attempt -- exactly the "abandoned attempt"
    shape ``_owned_paths_from_ledger`` recognises in production (a
    retry that moved on, an import that already completed, a request
    reset back to wanted). Returns the canonical folder ownership
    derives to.
    """
    fake_db.seed_request(make_request_row(
        id=request_id, status=status, artist_name=artist,
        album_title=title, year=int(year) if year else None))
    _ledger_seed(fake_db, request_id=request_id, file_pairs=file_pairs)
    fp = attempt_fingerprint(file_pairs)
    return canonical_processing_path(
        artist=artist, title=title, year=year,
        slskd_download_dir=root, attempt_fingerprint=fp)


def _make_ctx(
    root: str,
    rows: list[dict[str, Any]] | None = None,
    fake_db: FakePipelineDB | None = None,
) -> Any:
    if fake_db is None:
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
    def test_boosh_shape_exact_stamps_reaped_active_stamp_untouched(self):
        """R2 pin: only exact accepted event stamps authorize a reap.

        Re-expresses the pre-#571 'boosh shape' pin (which proved "any
        unrecognised old folder is reaped") under positive-ownership
        semantics: same physical shape, same removed count, but the
        reap is now driven by the write-ahead ledger recognising an
        abandoned attempt — not by mere unrecognition.
        """
        with tempfile.TemporaryDirectory() as root:
            fake_db = FakePipelineDB()
            boosh_pairs = [
                ("boosh-peer", "boosh-peer\\Album\\CD 01\\01 Track.flac"),
                ("boosh-peer", "boosh-peer\\Album\\CD 03\\01 Track.flac"),
            ]
            fake_db.seed_request(make_request_row(id=2, status="imported"))
            boosh_paths = {
                boosh_pairs[0]: os.path.join(root, "Boosh", "CD 01", "01 Track.flac"),
                boosh_pairs[1]: os.path.join(root, "Boosh", "CD 03", "01 Track.flac"),
            }
            _ledger_seed(
                fake_db, request_id=2, file_pairs=boosh_pairs,
                local_paths=boosh_paths, with_fingerprint=False,
            )
            for path in boosh_paths.values():
                _write_aged_file(path, age_days=_OLD_DAYS)

            active_pair = ("peer1", "peer1\\Album\\01.flac")
            active_path = os.path.join(root, "Active", "01.flac")
            _seed_active_downloading(
                fake_db, request_id=1, artist=_ACTIVE_ARTIST, title=_ACTIVE_TITLE,
                year=_ACTIVE_YEAR,
                file_pairs=[active_pair], root=root,
                local_paths={active_pair: active_path},
            )
            _write_aged_file(active_path, age_days=_OLD_DAYS)

            ctx = _make_ctx(root, fake_db=fake_db)
            summary = reap_disk_orphans(ctx)

            self.assertFalse(any(os.path.exists(path) for path in boosh_paths.values()))
            self.assertTrue(os.path.exists(active_path))
            self.assertEqual(summary.removed, 2)

    def test_unledgered_orphan_folder_survives_however_old(self):
        """R1 pin: the exact inversion of the pin above — the SAME
        multi-disc folder shape, but NEVER ledgered at all, must now
        SURVIVE no matter how old. This is the headline behavioral
        flip: pre-#571, this exact shape was reaped
        (test_boosh_shape_orphans_reaped_active_canonical_untouched);
        post-#571, it is never cratedigger's to delete.
        """
        with tempfile.TemporaryDirectory() as root:
            unowned_root = os.path.join(
                root, "The Mighty Boosh - The Mighty Boosh (2007)")
            target1 = os.path.join(unowned_root, "CD 01", "01 Track.flac")
            target2 = os.path.join(unowned_root, "CD 03", "01 Track.flac")
            _write_aged_file(target1, age_days=_OLD_DAYS * 10)
            _write_aged_file(target2, age_days=_OLD_DAYS * 10)

            ctx = _make_ctx(root)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(target1))
            self.assertTrue(os.path.exists(target2))
            self.assertEqual(summary.removed, 0)
            self.assertEqual(summary.unowned, 2)

    def test_stale_bare_canonical_folder_survives_unledgered(self):
        """Pre-#571 debris pin: a pre-#560 bare canonical folder (no
        fingerprint suffix) with no ledger row and no request row at
        all is now unowned debris that survives forever — flips
        test_stale_bare_canonical_folder_reaped. The operator clears
        any already-stranded pre-ledger debris in a one-off deploy
        pass (see PR body); this reaper never touches it going
        forward.
        """
        with tempfile.TemporaryDirectory() as root:
            bare = os.path.join(root, "Old Artist - Old Album (2015)")
            target = os.path.join(bare, "01.flac")
            _write_aged_file(target, age_days=_OLD_DAYS)

            ctx = _make_ctx(root)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(target))
            self.assertEqual(summary.removed, 0)
            self.assertEqual(summary.unowned, 1)

    def test_failed_imports_quarantine_never_touched(self):
        """Genuine failed imports are never reaped, no matter how old."""
        with tempfile.TemporaryDirectory() as root:
            quarantined = os.path.join(root, "failed_imports", "Some Album")
            target = os.path.join(quarantined, "01.flac")
            _write_aged_file(target, age_days=_OLD_DAYS * 10)

            ctx = _make_ctx(root)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(target))
            self.assertEqual(summary.removed, 0)

    def test_wrong_matches_quarantine_never_touched(self):
        """Wrong Match review sources retain their existing reaper protection."""
        with tempfile.TemporaryDirectory() as root:
            fake_db = FakePipelineDB()
            fake_db.seed_request(make_request_row(id=3, status="wanted"))
            quarantined = os.path.join(root, "wrong_matches", "Some Album")
            target = os.path.join(quarantined, "01.flac")
            pair = ("peer", "peer\\Some Album\\01.flac")
            _ledger_seed(
                fake_db,
                request_id=3,
                file_pairs=[pair],
                local_paths={pair: target},
                with_fingerprint=False,
            )
            _write_aged_file(target, age_days=_OLD_DAYS * 10)

            ctx = _make_ctx(root, fake_db=fake_db)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(target))
            self.assertEqual(summary.removed, 0)

    def test_fresh_orphan_folder_not_touched(self):
        """A folder younger than the grace window survives — doubly so
        now: it's both unowned AND too young."""
        with tempfile.TemporaryDirectory() as root:
            fresh = os.path.join(root, "Fresh Artist - Fresh Album (2026)")
            target = os.path.join(fresh, "01.flac")
            _write_aged_file(target, age_days=_YOUNG_DAYS)

            ctx = _make_ctx(root)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(target))
            self.assertEqual(summary.removed, 0)
            self.assertEqual(summary.unowned, 1)

    def test_blank_download_dir_is_a_no_op(self):
        """No configured root -> never walk anything (never walks '/')."""
        ctx = _make_ctx("")
        summary = reap_disk_orphans(ctx)
        self.assertEqual(summary, DiskReapSummary())

    def test_ledger_owned_stamped_file_reaped_when_aged(self):
        """R2 pin, file-level ownership mechanism: a file matching a
        ledgered, completion-stamped local_path is reaped once aged,
        even though it sits OUTSIDE any canonical-folder derivation —
        proves mechanism (a) (``get_owned_local_paths``) independent
        of mechanism (b) (canonical folder derivation)."""
        with tempfile.TemporaryDirectory() as root:
            fake_db = FakePipelineDB()
            fake_db.seed_request(make_request_row(id=3, status="imported"))
            path = os.path.join(root, "Weird Folder", "stray.flac")
            pair = ("op", "op\\stray.flac")
            _ledger_seed(
                fake_db, request_id=3, file_pairs=[pair],
                local_paths={pair: path}, with_fingerprint=False)
            _write_aged_file(path, age_days=_OLD_DAYS)

            ctx = _make_ctx(root, fake_db=fake_db)
            summary = reap_disk_orphans(ctx)

            self.assertFalse(os.path.exists(path))
            self.assertEqual(summary.removed, 1)

    def test_ledger_owned_file_survives_when_young(self):
        """Age gate still applies to owned files — R2 requires AGED,
        not merely owned."""
        with tempfile.TemporaryDirectory() as root:
            fake_db = FakePipelineDB()
            fake_db.seed_request(make_request_row(id=3, status="imported"))
            path = os.path.join(root, "Weird Folder", "stray.flac")
            pair = ("op", "op\\stray.flac")
            _ledger_seed(
                fake_db, request_id=3, file_pairs=[pair],
                local_paths={pair: path}, with_fingerprint=False)
            _write_aged_file(path, age_days=_YOUNG_DAYS)

            ctx = _make_ctx(root, fake_db=fake_db)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(path))
            self.assertEqual(summary.removed, 0)
            self.assertEqual(summary.skipped_young, 1)

    def test_same_request_retry_old_stamp_reaped_new_stamp_protected(self):
        """A retry protects only its current exact event stamp."""
        with tempfile.TemporaryDirectory() as root:
            fake_db = FakePipelineDB()
            old_pairs = [("peerA", "peerA\\Album\\01.flac")]
            new_pairs = [("peerB", "peerB\\Album\\01.flac")]

            fake_db.seed_request(make_request_row(id=5, status="imported"))
            old_path = os.path.join(root, "old-attempt", "01.flac")
            _ledger_seed(
                fake_db, request_id=5, file_pairs=old_pairs,
                local_paths={old_pairs[0]: old_path}, with_fingerprint=False,
            )
            _write_aged_file(old_path, age_days=_OLD_DAYS)

            # Second (live, current) attempt: this IS what
            # active_download_state reflects — actively protected.
            new_path = os.path.join(root, "new-attempt", "01.flac")
            _seed_active_downloading(
                fake_db, request_id=5, artist=_ACTIVE_ARTIST, title=_ACTIVE_TITLE,
                year=_ACTIVE_YEAR, file_pairs=new_pairs, root=root,
                local_paths={new_pairs[0]: new_path},
            )
            _write_aged_file(new_path, age_days=_OLD_DAYS)

            ctx = _make_ctx(root, fake_db=fake_db)
            summary = reap_disk_orphans(ctx)

            self.assertFalse(os.path.exists(old_path))
            self.assertTrue(os.path.exists(new_path))
            self.assertEqual(summary.removed, 1)

    def test_stale_empty_ledgered_dir_survives_without_a_file_stamp(self):
        """A ledger proves file ownership, never directory ownership."""
        with tempfile.TemporaryDirectory() as root:
            fake_db = FakePipelineDB()
            canonical = _seed_owned_inactive(
                fake_db, request_id=4, artist="Empty Artist",
                title="Empty Album", year="2019",
                file_pairs=[("op", "op\\01.flac")], root=root)
            os.makedirs(canonical)
            _age_dir(canonical, age_days=_OLD_DAYS)

            ctx = _make_ctx(root, fake_db=fake_db)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.isdir(canonical))
            self.assertEqual(summary.pruned_dirs, 0)

    def test_stale_empty_unowned_dir_survives(self):
        """A foreign already-empty stale directory (no ledger record
        at all) is NEVER pruned, however old — flips the pre-#571
        'prune any stale empty dir' doctrine."""
        with tempfile.TemporaryDirectory() as root:
            stale = os.path.join(root, "Empty Stale Folder")
            os.makedirs(stale)
            _age_dir(stale, age_days=_OLD_DAYS)

            ctx = _make_ctx(root)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.isdir(stale))
            self.assertEqual(summary.pruned_dirs, 0)

    def test_undecodable_ownership_aborts_sweep_fail_closed(self):
        """A downloading row whose state can't be decoded (or is
        missing) must abort the ENTIRE sweep — its files can't be told
        apart from a genuinely reap-eligible ledger-owned orphan, so
        nothing anywhere gets deleted this cycle. The planted "orphan"
        here is deliberately LEDGER-OWNED-INACTIVE (would be reaped
        under normal, non-aborted conditions) so its survival proves
        the abort — not mere unownership, which would survive either
        way post-#571."""
        for desc, bad_state in (
            ("missing state", None),
            ("undecodable state", {"garbage": True}),
        ):
            with self.subTest(desc=desc):
                with tempfile.TemporaryDirectory() as root:
                    fake_db = FakePipelineDB()
                    owned_canonical = _seed_owned_inactive(
                        fake_db, request_id=8, artist="Orphan Artist",
                        title="Orphan Album", year="2001",
                        file_pairs=[("op", "op\\Album\\01.flac")], root=root)
                    orphan = os.path.join(owned_canonical, "01.flac")
                    _write_aged_file(orphan, age_days=_OLD_DAYS)
                    stale_empty = os.path.join(root, "Empty Stale Folder")
                    os.makedirs(stale_empty)
                    _age_dir(stale_empty, age_days=_OLD_DAYS)
                    fake_db.seed_request(make_request_row(
                        id=7, status="downloading",
                        active_download_state=bad_state))
                    ctx = _make_ctx(root, fake_db=fake_db)

                    summary = reap_disk_orphans(ctx)

                    self.assertTrue(os.path.exists(orphan))
                    self.assertTrue(os.path.isdir(stale_empty))
                    self.assertTrue(summary.aborted)
                    self.assertEqual(summary.removed, 0)
                    self.assertEqual(summary.pruned_dirs, 0)

    def test_one_undecodable_row_aborts_despite_healthy_rows(self):
        """ANY undecodable downloading row aborts — a healthy sibling
        row with a decodable state doesn't rescue the sweep. Same
        ledger-owned-inactive "would normally be reaped" planted orphan
        as above."""
        with tempfile.TemporaryDirectory() as root:
            fake_db = FakePipelineDB()
            owned_canonical = _seed_owned_inactive(
                fake_db, request_id=8, artist="Orphan Artist",
                title="Orphan Album", year="2001",
                file_pairs=[("op", "op\\Album\\01.flac")], root=root)
            orphan = os.path.join(owned_canonical, "01.flac")
            _write_aged_file(orphan, age_days=_OLD_DAYS)
            _seed_active_downloading(
                fake_db, request_id=1, artist=_ACTIVE_ARTIST, title=_ACTIVE_TITLE,
                year=_ACTIVE_YEAR,
                file_pairs=[("peer1", "peer1\\Album\\01.flac")], root=root)
            fake_db.seed_request(make_request_row(
                id=7, status="downloading",
                active_download_state={"garbage": True}))

            ctx = _make_ctx(root, fake_db=fake_db)
            summary = reap_disk_orphans(ctx)

            self.assertTrue(os.path.exists(orphan))
            self.assertTrue(summary.aborted)
            self.assertEqual(summary.removed, 0)


# ============================================================================
# Generated property
# ============================================================================

class _Zone(Enum):
    ORPHAN = "orphan"            # never ledgered — always survives
    PROTECTED = "protected"      # active row's exact stamp — always survives
    FAILED_IMPORTS = "failed_imports"  # quarantine — always survives
    OWNED_DIR = "owned_dir"      # exact owned files under an ordinary folder
    OWNED_FILE = "owned_file"    # ledgered + stamped exact local_path, standalone


@dataclass(frozen=True)
class _FileSpec:
    zone: _Zone
    folder: str      # top-level orphan folder name (ORPHAN zone only)
    nested: bool     # place under a "Disc 1" subfolder (ORPHAN/OWNED_DIR zones)
    old: bool
    size: int
    ext: str


@dataclass(frozen=True)
class DiskReaperWorld:
    files: tuple[_FileSpec, ...]
    stray_protected: bool
    empty_stale_unowned_dir: bool
    empty_stale_owned_dir: bool
    empty_fresh_dir: bool
    # A second downloading row with an undecodable state — the sweep must
    # abort fail-closed: everything (however old/orphaned/owned) survives.
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

    if draw(st.booleans()):
        for _ in range(draw(st.integers(min_value=1, max_value=3))):
            files.append(_FileSpec(
                zone=_Zone.OWNED_DIR, folder="", nested=draw(st.booleans()),
                old=draw(st.booleans()),
                size=draw(st.sampled_from([0, 16, 4096])),
                ext=draw(st.sampled_from(_EXTS)),
            ))

    if draw(st.booleans()):
        for _ in range(draw(st.integers(min_value=1, max_value=3))):
            files.append(_FileSpec(
                zone=_Zone.OWNED_FILE, folder="", nested=False,
                old=draw(st.booleans()),
                size=draw(st.sampled_from([0, 16, 4096])),
                ext=draw(st.sampled_from(_EXTS)),
            ))

    stray_protected = include_protected and draw(st.booleans())
    empty_stale_unowned_dir = draw(st.booleans())
    empty_stale_owned_dir = draw(st.booleans())
    empty_fresh_dir = draw(st.booleans())
    # Weighted low so most examples still exercise the deletion logic.
    undecodable_state = draw(
        st.sampled_from([False, False, False, True]))

    return DiskReaperWorld(
        files=tuple(files),
        stray_protected=stray_protected,
        empty_stale_unowned_dir=empty_stale_unowned_dir,
        empty_stale_owned_dir=empty_stale_owned_dir,
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
    empty_stale_unowned_path: str | None
    empty_stale_unowned_survived: bool
    empty_stale_owned_path: str | None
    empty_stale_owned_expected_survive: bool
    empty_stale_owned_survived: bool
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

        fake_db = FakePipelineDB()

        protected_specs = [
            (idx, spec) for idx, spec in enumerate(world.files)
            if spec.zone == _Zone.PROTECTED
        ]
        active_file_pairs = [
            ("activepeer", f"activepeer\\Album\\protected-{idx}.flac")
            for idx, _spec in protected_specs
        ]
        local_paths = {
            pair: os.path.join(root, "active", os.path.basename(pair[1]))
            for pair in active_file_pairs
        }

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

        _seed_active_downloading(
            fake_db, request_id=1, artist=_ACTIVE_ARTIST, title=_ACTIVE_TITLE,
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

        # --- Inactive exact-stamp ownership under one ordinary folder ---
        has_owned_dir_files = any(
            spec.zone == _Zone.OWNED_DIR for spec in world.files)
        owned_dir: str | None = None
        owned_dir_rel_name: str | None = None
        if has_owned_dir_files:
            fake_db.seed_request(make_request_row(id=2, status="imported"))
            owned_dir = os.path.join(root, "Owned Inactive Files")
            owned_dir_rel_name = os.path.basename(owned_dir)
            folder_has_survivor[owned_dir_rel_name] = False

        # --- Standalone stamped-file ownership (OWNED_FILE zone) ---
        has_owned_file = any(
            spec.zone == _Zone.OWNED_FILE for spec in world.files)
        if has_owned_file:
            fake_db.seed_request(make_request_row(id=3, status="imported"))

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
                path_expected[path] = True  # never ledgered — always survives
                folder_has_survivor[spec.folder] = True
            elif spec.zone == _Zone.PROTECTED:
                pair = (
                    "activepeer",
                    f"activepeer\\Album\\protected-{idx}.flac",
                )
                path = local_paths[pair]
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
            elif spec.zone == _Zone.OWNED_DIR:
                assert owned_dir is not None
                assert owned_dir_rel_name is not None
                folder = owned_dir
                if spec.nested:
                    folder = os.path.join(folder, "Disc 1")
                path = os.path.join(folder, f"owned-{idx}{spec.ext}")
                pair = (
                    f"ownedpeer{idx}",
                    f"ownedpeer{idx}\\Album\\owned-{idx}{spec.ext}",
                )
                _ledger_seed(
                    fake_db, request_id=2, file_pairs=[pair],
                    local_paths={pair: path}, with_fingerprint=False,
                )
                _write_aged_file(
                    path,
                    age_days=_OLD_DAYS if spec.old else _YOUNG_DAYS,
                    size=spec.size)
                path_expected[path] = not spec.old  # ledger-owned, age-gated
                if not spec.old:
                    folder_has_survivor[owned_dir_rel_name] = True
            elif spec.zone == _Zone.OWNED_FILE:
                path = os.path.join(root, f"owned-file-{idx}{spec.ext}")
                pair = (
                    f"filepeer{idx}",
                    f"filepeer{idx}\\owned-file-{idx}{spec.ext}")
                _ledger_seed(
                    fake_db, request_id=3, file_pairs=[pair],
                    local_paths={pair: path}, with_fingerprint=False)
                _write_aged_file(
                    path,
                    age_days=_OLD_DAYS if spec.old else _YOUNG_DAYS,
                    size=spec.size)
                path_expected[path] = not spec.old  # ledger-owned, age-gated

        empty_stale_unowned_path: str | None = None
        if world.empty_stale_unowned_dir:
            empty_stale_unowned_path = os.path.join(root, "Empty Stale Folder")
            os.makedirs(empty_stale_unowned_path, exist_ok=True)
            _age_dir(empty_stale_unowned_path, age_days=_OLD_DAYS)

        empty_stale_owned_path: str | None = None
        if world.empty_stale_owned_dir:
            empty_stale_owned_path = os.path.join(root, "Empty Ledger Folder")
            os.makedirs(empty_stale_owned_path, exist_ok=True)
            _age_dir(empty_stale_owned_path, age_days=_OLD_DAYS)

        empty_fresh_path: str | None = None
        if world.empty_fresh_dir:
            empty_fresh_path = os.path.join(root, "Empty Fresh Folder")
            os.makedirs(empty_fresh_path, exist_ok=True)
            # Freshly created -- mtime is already "now".

        if world.undecodable_state:
            fake_db.seed_request(make_request_row(
                id=99, status="downloading",
                active_download_state={"garbage": True}))
            # Fail-closed: the sweep aborts, so EVERYTHING survives —
            # including old exact-owned files and empty directories.
            path_expected = {p: True for p in path_expected}
            folder_has_survivor = {
                name: True for name in folder_has_survivor}

        ctx = _make_ctx(root, fake_db=fake_db)
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
            empty_stale_unowned_path=empty_stale_unowned_path,
            empty_stale_unowned_survived=(
                empty_stale_unowned_path is not None
                and os.path.exists(empty_stale_unowned_path)),
            empty_stale_owned_path=empty_stale_owned_path,
            empty_stale_owned_expected_survive=True,
            empty_stale_owned_survived=(
                empty_stale_owned_path is not None
                and os.path.exists(empty_stale_owned_path)),
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

    if (result.empty_stale_unowned_path is not None
            and not result.empty_stale_unowned_survived):
        raise AssertionError(
            "a foreign already-empty stale directory must NEVER be "
            f"pruned: {result.empty_stale_unowned_path}")

    if result.empty_stale_owned_path is not None and (
            result.empty_stale_owned_survived
            != result.empty_stale_owned_expected_survive):
        raise AssertionError(
            f"owned already-empty stale directory survival diverged: "
            f"path={result.empty_stale_owned_path} "
            f"expected_survive={result.empty_stale_owned_expected_survive} "
            f"actual_survive={result.empty_stale_owned_survived}")

    if result.empty_fresh_path is not None and not result.empty_fresh_survived:
        raise AssertionError(
            f"fresh empty directory should NOT have been pruned: "
            f"{result.empty_fresh_path}")

    if not result.root_intact:
        raise AssertionError("root itself must never be removed")


# Pinned regression: a mixed-age orphan folder (never ledgered — always
# survives), a mixed-age owned-inactive folder (one old file reaped, one
# young survivor keeping the folder alive), a protected file aged past
# the threshold (must still survive), a quarantined failed_imports file
# aged past the threshold (must still survive), a standalone
# ledger-owned stamped file, a stray protected file living inside an
# orphan folder, and all three empty-directory scenarios (foreign,
# owned, fresh) -- the full mixed shape the strategy can produce.
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
        _FileSpec(zone=_Zone.OWNED_DIR, folder="", nested=False,
                   old=True, size=16, ext=".flac"),
        _FileSpec(zone=_Zone.OWNED_DIR, folder="", nested=True,
                   old=False, size=0, ext=".jpg"),
        _FileSpec(zone=_Zone.OWNED_FILE, folder="", nested=False,
                   old=True, size=16, ext=".flac"),
    ),
    stray_protected=True,
    empty_stale_unowned_dir=True,
    empty_stale_owned_dir=True,
    empty_fresh_dir=True,
    undecodable_state=False,
)

# Pinned fail-closed world: the same full mixed shape, but with a second
# downloading row whose state can't be decoded — the sweep must abort and
# every path (however old, orphaned, or owned) must survive.
_PINNED_ABORT_WORLD = DiskReaperWorld(
    files=_PINNED_WORLD.files,
    stray_protected=True,
    empty_stale_unowned_dir=True,
    empty_stale_owned_dir=True,
    empty_fresh_dir=True,
    undecodable_state=True,
)


class TestGeneratedDiskReaperInvariants(unittest.TestCase):
    """Properties 1-6 and 8: within-root, active protection, quarantine,
    age, owned-only empty-dir pruning, fail-closed ownership, and
    good-citizen positive ownership -- patrolled together over
    generated worlds."""

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
            empty_stale_unowned_path=None,
            empty_stale_unowned_survived=True,
            empty_stale_owned_path=None,
            empty_stale_owned_expected_survive=False,
            empty_stale_owned_survived=False,
            empty_fresh_path=None,
            empty_fresh_survived=False,
            root_intact=True,
            expect_abort=False,
        )
        defaults.update(overrides)
        return DiskReaperRunResult(**defaults)

    def test_trips_on_protected_or_young_path_wrongly_removed(self):
        """R1/active-protection known-bad self-test: a path that should
        have survived (protected, unowned, or too young) but was
        removed."""
        result = self._base_result(
            path_expected_survive={"/tmp/x/owned.flac": True},
            path_actual_survive={"/tmp/x/owned.flac": False},
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_old_owned_orphan_wrongly_kept(self):
        """R2 known-bad self-test: an aged, ledger-owned, non-active
        path that should have been reaped but survived."""
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

    def test_trips_on_foreign_empty_dir_wrongly_pruned(self):
        result = self._base_result(
            empty_stale_unowned_path="/tmp/x/Foreign Empty Folder",
            empty_stale_unowned_survived=False,
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_stale_owned_empty_dir_wrongly_kept(self):
        result = self._base_result(
            empty_stale_owned_path="/tmp/x/Owned Empty Folder",
            empty_stale_owned_expected_survive=False,
            empty_stale_owned_survived=True,
        )
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_stale_owned_empty_dir_wrongly_pruned_during_abort(self):
        result = self._base_result(
            summary=DiskReapSummary(aborted=True),
            expect_abort=True,
            empty_stale_owned_path="/tmp/x/Owned Empty Folder",
            empty_stale_owned_expected_survive=True,
            empty_stale_owned_survived=False,
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
            summary=DiskReapSummary(removed=1, aborted=True))
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)

    def test_trips_on_unexpected_abort(self):
        result = self._base_result(
            summary=DiskReapSummary(aborted=True))
        with self.assertRaises(AssertionError):
            assert_disk_reaper_invariants(result)


# ============================================================================
# Reaper authority wiring: exact event stamps, never canonical folders
# ============================================================================

def assert_exact_stamp_protected(
    stamp_path: str,
    protected_dirs: set[str],
    protected_files: set[str],
    root: str,
) -> None:
    """The active reaper boundary is quarantine roots plus one exact stamp."""
    quarantines = {
        normalize_processing_path(os.path.join(root, "failed_imports")),
        normalize_processing_path(os.path.join(root, "wrong_matches")),
    }
    if protected_dirs != quarantines:
        raise AssertionError(f"unexpected protected directories: {protected_dirs!r}")
    expected = {normalize_processing_path(stamp_path)}
    if protected_files != expected:
        raise AssertionError(f"unexpected protected files: {protected_files!r}")


def _row_with_stamp(stamp_path: str) -> dict[str, Any]:
    file = make_download_file(
        username="peer", filename="peer\\Album\\01.flac", file_dir="peer\\Album")
    file.local_path = stamp_path
    state = build_active_download_state(make_grab_list_entry(files=[file]))
    return make_request_row(
        id=9, status="downloading", active_download_state=msgspec.to_builtins(state))


def _run_exact_stamp_authority(stamp_path: str, root: str) -> None:
    protected_dirs, protected_files = _protected_paths_for_downloading(
        root, [_row_with_stamp(stamp_path)])
    assert_exact_stamp_protected(stamp_path, protected_dirs, protected_files, root)


class TestExactStampAuthority(unittest.TestCase):
    def test_reaper_protects_exactly_the_event_stamp(self):
        _run_exact_stamp_authority("/downloads/peer/01.flac", "/downloads")

    def test_reaper_does_not_derive_processing_folder_authority(self):
        source = inspect.getsource(_protected_paths_for_downloading)
        self.assertNotIn("canonical_processing_path", source)
        self.assertNotIn("reconstruct_grab_list_entry", source)


class TestGeneratedExactStampAuthority(unittest.TestCase):
    @given(parts=st.lists(st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-",
        min_size=1, max_size=16), min_size=1, max_size=4))
    def test_reaper_protects_generated_exact_stamp(self, parts):
        _run_exact_stamp_authority("/downloads/" + "/".join(parts), "/downloads")


class TestExactStampCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_stamp_is_not_protected(self):
        with self.assertRaises(AssertionError):
            assert_exact_stamp_protected(
                "/downloads/peer/01.flac",
                {
                    normalize_processing_path("/downloads/failed_imports"),
                    normalize_processing_path("/downloads/wrong_matches"),
                },
                set(),
                "/downloads")

    def test_trips_when_an_extra_directory_is_protected(self):
        with self.assertRaises(AssertionError):
            assert_exact_stamp_protected(
                "/downloads/peer/01.flac",
                {
                    normalize_processing_path("/downloads/failed_imports"),
                    normalize_processing_path("/downloads/wrong_matches"),
                    normalize_processing_path("/downloads/not-authority"),
                },
                {normalize_processing_path("/downloads/peer/01.flac")},
                "/downloads")


if __name__ == "__main__":
    unittest.main()
