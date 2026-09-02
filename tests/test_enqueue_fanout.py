"""Wave-based fan-out tests for try_enqueue / try_multi_enqueue (issue #198 U3).

The refactor replaces the sequential per-user iteration in `try_enqueue` and
`try_multi_enqueue` with: (1) chunk eligible users into waves of
`cfg.browse_top_k`, (2) parallel browse via `_fanout_browse_users`, (3) match
in upload-speed order against the now-warm folder cache, (4) exit on first
successful enqueue.

These tests pin:
  * top-K hit → only first wave fans out
  * lazy-tail hit → second wave fans out, third never
  * all-miss → every eligible user fans out, matched=False
  * 0 eligible (cooldown/denylist) → no fan-out
  * fewer than K eligible → single short wave
  * cached entries skipped from the work list
  * had_enqueue_failure tracking when enqueue raises
  * try_multi_enqueue: per-disc wave loop reuses populated cache
"""

from __future__ import annotations

import configparser
import json
import sys
import threading
import unittest
from collections.abc import Sequence
from dataclasses import replace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import msgspec

from cratedigger import TrackRecord
from lib.browse import BrowseManyResult
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.download_ownership import DownloadOwnershipWriter
from lib.enqueue import (
    ClaimedQueueKeysRegistry,
    DownloadOwnershipClaim,
    EnqueueAttempt,
    _enqueue_with_claim_outcome,
    _WorkerPipelineDBSource,
    get_album_tracks,
    prepare_find_download_context,
    try_enqueue,
    try_multi_enqueue,
)
from lib.grab_list import DownloadFile, GrabListEntry
from lib.matching import MatchResult
from lib.pipeline_db import TransferLedgerRow
from lib.processing_paths import attempt_fingerprint_of_files
from lib.quality import ActiveDownloadState, CandidateScore
from lib.slskd_transfers import SlskdEnqueueOutcome
from tests.fakes import (
    DenylistEntry,
    FakePipelineDB,
    FakePipelineDBSource,
    FakeSlskdAPI,
    FakeSlskdTransfers,
)
from tests.helpers import (
    make_cycle_collaborators,
    make_request_row,
    rebind_collaborators,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_cfg(
    *,
    browse_top_k: int = 20,
    browse_global_max_workers: int = 32,
) -> CratediggerConfig:
    """Build a CratediggerConfig with the fan-out knobs configurable."""
    ini = configparser.ConfigParser()
    ini["Search Settings"] = {
        "minimum_filename_match_ratio": "0.5",
        "ignored_users": "",
        "allowed_filetypes": "flac,mp3",
        "browse_parallelism": "4",
        "browse_top_k": str(browse_top_k),
        "browse_global_max_workers": str(browse_global_max_workers),
    }
    return CratediggerConfig.from_ini(ini)


def _make_ctx(
    cfg: CratediggerConfig,
    *,
    user_upload_speed: dict[str, int] | None = None,
    cooled_down_users: set[str] | None = None,
    denied_users: list[str] | None = None,
) -> CratediggerContext:
    """Build a context with controllable cooldowns and denylist."""
    db = FakePipelineDB()
    for username in denied_users or []:
        db.denylist.append(DenylistEntry(request_id=1, username=username))
    ctx = CratediggerContext(
        collaborators=make_cycle_collaborators(
            cfg=cfg,
            slskd=FakeSlskdAPI(),
            pipeline_db_source=FakePipelineDBSource(db),
        ),
        user_upload_speed=user_upload_speed or {},
        cooled_down_users=cooled_down_users or set(),
    )
    ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
    return ctx


def _make_results(users: list[str]) -> dict[str, dict[str, list[str]]]:
    """Build a search-results dict where each user has one flac dir."""
    return {u: {"flac": [f"Music\\{u}\\Album"]} for u in users}


def _make_tracks(album_id: int = 1) -> list[TrackRecord]:
    return cast(
        "list[TrackRecord]",
        [{"albumId": album_id, "title": "Track 1", "mediumNumber": 1}],
    )


def _multi_disc_tracks(
    *, album_id: int = 1, mediums: tuple[int, ...] = (1, 2),
) -> list[TrackRecord]:
    """Shared multi-disc TrackRecord builder -- one cast, reused by every
    try_multi_enqueue test that needs more than one medium (#1196 item 1
    review F1's composed pin reuses this rather than growing the file's
    frozen escape-hatch count with its own inline cast)."""
    return cast(
        "list[TrackRecord]",
        [
            {"albumId": album_id, "title": f"Disc{m} Track", "mediumNumber": m}
            for m in mediums
        ],
    )


def _album_with_request(request_id: int = 1) -> MagicMock:
    return MagicMock(
        id=request_id,
        db_request_id=request_id,
        title="Album",
        artist_name="Artist",
        release_date="2024-01-01T00:00:00Z",
        db_mb_release_id=f"mbid-{request_id}",
        db_source="request",
        db_search_filetype_override=None,
        db_target_format=None,
    )


def _ctx_with_download_ownership(
    *,
    cfg: CratediggerConfig,
    db: FakePipelineDB,
    slskd: FakeSlskdAPI | None = None,
    registry: ClaimedQueueKeysRegistry | None = None,
) -> CratediggerContext:
    ctx = _make_ctx(cfg, user_upload_speed={"u00": 10_000, "u01": 9_999})
    rebind_collaborators(
        ctx,
        slskd=slskd if slskd is not None else FakeSlskdAPI(),
    )
    rebind_collaborators(ctx, pipeline_db_source=FakePipelineDBSource(db))
    ctx.current_album_cache[1] = _album_with_request(1)
    rebind_collaborators(
        ctx,
        download_ownership=DownloadOwnershipWriter(db_factory=lambda: db),
    )
    # A fresh registry per call by default (one cycle == one registry);
    # cross-request tests pass a SHARED instance to model two candidates
    # evaluated within the same cycle (issue #1178 PR2 review F7).
    rebind_collaborators(
        ctx,
        claimed_queue_keys_registry=registry if registry is not None else ClaimedQueueKeysRegistry(),
    )
    return ctx


def _ledger_enqueue_attempt(
    db: FakePipelineDB,
    username: str,
    files: list[dict[str, object]],
    *,
    accepted: bool,
    attempt_fp: str | None = None,
    request_id: int = 1,
) -> None:
    """Mirror the ledger half of the real ``slskd_enqueue_with_outcome``.

    A fake standing in for that function must write what it writes, in
    both directions (Rule B, `.claude/rules/test-fidelity.md`):

    * the write-ahead row goes in BEFORE the POST, so it is written
      whatever the POST then returns -- a rejected enqueue still leaves
      pending intent behind (`_write_ahead_transfer_ledger` is called
      unconditionally at the top of the real function);
    * only an accepted POST is then confirmed, which is what turns that
      intent into destructive authority.

    Writing only the accepted half manufactures a world production
    cannot produce, in both directions: an ACCEPTED enqueue whose queue
    key nothing owns (which the #1278 ownership gate correctly refuses
    to act on, reading as a test failure rather than the fixture gap it
    is), and a REJECTED enqueue that leaves no pending row for the
    retention/promotion paths to find.

    ``attempt_fp`` exists because the real function stamps it onto every
    row and the cross-request enqueue guard joins on exactly that
    column. Every call site today leaves it ``None``: production does
    pass ``attempt_fp`` by name, but these fakes swallow it in
    ``**kwargs`` and never forward it into the row, and no assertion in
    them reads the column. A guard test written into this class later
    MUST pass it rather than inherit a fixture that cannot reproduce the
    join.
    """
    rows = [
        TransferLedgerRow(
            request_id=request_id,
            username=username,
            filename=str(file["filename"]),
            attempt_fingerprint=attempt_fp,
        )
        for file in files
    ]
    db.record_transfer_enqueue(rows)
    if not accepted:
        return
    for row in rows:
        db.confirm_transfer_enqueue(
            row.username, row.filename, request_id=row.request_id)


class TestEnqueueAttemptFingerprintPolicy(unittest.TestCase):
    """The ledger-side half of the empty-attempt `None` policy.

    `attempt_fingerprint_or_none`'s docstring says both sides of the
    cross-request guard's fingerprint equality are written by two
    callers: `lib.download.build_active_download_state` and this one.
    Only the first had a pin — a mutant swapping THIS site to the
    always-`str` variant survived the whole suite (#1278 review). An
    empty-files attempt that minted the empty-set digest here would
    equal any other empty attempt's state fingerprint, so the guard's
    exact-equality join would treat two unrelated file-less claims as
    the same attempt.
    """

    def _claim(self, files: list[DownloadFile]) -> DownloadOwnershipClaim:
        entry = GrabListEntry(
            album_id=1,
            files=files,
            filetype="flac",
            title="Album",
            artist="Artist",
            year="2020",
            mb_release_id="release-id",
            db_request_id=1,
        )
        return DownloadOwnershipClaim(
            entry=entry, request_id=1, attempted=True, claimed=True,
            enqueued_at="2026-08-26T00:00:00+00:00")

    def _attempt_fp_written(self, files: list[DownloadFile]) -> object:
        captured: dict[str, object] = {}
        db = FakePipelineDB()

        def fake_enqueue(*, username, files, attempt_fp, **_kwargs):
            captured["attempt_fp"] = attempt_fp
            # Rule B: the write-ahead row precedes the POST, so even this
            # rejecting fake writes the pending intent the real function
            # would have written — the contract `_ledger_enqueue_attempt`
            # above declares.
            _ledger_enqueue_attempt(
                db, username, files, accepted=False, attempt_fp=attempt_fp)
            return SlskdEnqueueOutcome(status="rejected")

        with patch(
            "lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue,
        ):
            _enqueue_with_claim_outcome(
                claim=self._claim(files),
                username="u00",
                files=[{"filename": "a.flac", "size": 1}],
                file_dir="Music\\Album",
                ctx=_make_ctx(_make_cfg()),
            )
        return captured["attempt_fp"]

    def test_file_less_attempt_writes_no_fingerprint(self):
        self.assertIsNone(self._attempt_fp_written([]))

    def test_attempt_with_files_writes_the_shared_derivation(self):
        files = [DownloadFile(
            filename="Music\\a.flac", id="", file_dir="Music",
            username="u00", size=1)]

        self.assertEqual(
            self._attempt_fp_written(files),
            attempt_fingerprint_of_files(files),
        )


def _request_active_state(db: FakePipelineDB) -> ActiveDownloadState:
    raw = db.request(1)["active_download_state"]
    if isinstance(raw, str):
        return ActiveDownloadState.from_json(raw)
    return msgspec.convert(raw, type=ActiveDownloadState)


def _install_same_path_attempt_b(
    db: FakePipelineDB,
    *,
    enqueued_at: str = "attempt-b",
) -> ActiveDownloadState:
    """Replace attempt A with same-path attempt B and return B's exact state."""
    replacement = ActiveDownloadState.from_json(
        _request_active_state(db).to_json(),
    )
    replacement.enqueued_at = enqueued_at
    replacement.last_progress_at = enqueued_at
    for file in replacement.files:
        file.last_state = "attempt-b-owned"
    db.request(1)["active_download_state"] = msgspec.to_builtins(replacement)
    return replacement


class _AttemptReplacingPipelineDB(FakePipelineDB):
    """Install attempt B immediately before one witnessed state write."""

    def __init__(self, *, replace_on_state_write: int = 1) -> None:
        super().__init__()
        self.replace_on_state_write = replace_on_state_write
        self.state_witnesses: list[tuple[str, str]] = []
        self.attempt_b: ActiveDownloadState | None = None

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool:
        outgoing = ActiveDownloadState.from_json(state_json)
        self.state_witnesses.append(
            (expected_enqueued_at, outgoing.enqueued_at),
        )
        if len(self.state_witnesses) == self.replace_on_state_write:
            self.attempt_b = _install_same_path_attempt_b(self)
        return super().update_download_state_if_downloading(
            request_id,
            state_json,
            expected_enqueued_at=expected_enqueued_at,
        )


class _ObservingCancelTransfers(FakeSlskdTransfers):
    """Record persisted state at cancel and optionally install attempt B."""

    def __init__(
        self,
        api: FakeSlskdAPI,
        db: FakePipelineDB,
        *,
        replace_with_attempt_b: bool = False,
    ) -> None:
        super().__init__(api)
        self.db = db
        self.replace_with_attempt_b = replace_with_attempt_b
        self.observed_before_cancel: list[ActiveDownloadState] = []
        self.writes_before_cancel: list[int] = []
        self.attempt_b: ActiveDownloadState | None = None

    def cancel_download(
        self,
        username: str,
        id: str,
        remove: bool = False,
    ) -> bool:
        self.observed_before_cancel.append(_request_active_state(self.db))
        self.writes_before_cancel.append(
            len(self.db.update_download_state_calls),
        )
        if self.replace_with_attempt_b:
            self.attempt_b = _install_same_path_attempt_b(self.db)
        return super().cancel_download(username=username, id=id, remove=remove)


def _enqueue_file_identity(
    files: Sequence[dict[str, object]],
) -> tuple[str, int]:
    filename = files[0]["filename"]
    size = files[0]["size"]
    assert isinstance(filename, str)
    assert isinstance(size, int)
    return filename, size


def _ranked_users(n: int) -> list[str]:
    """Return n usernames with descending upload speeds (fastest first)."""
    return [f"u{i:02d}" for i in range(n)]


def _upload_speeds(users: list[str]) -> dict[str, int]:
    """Map usernames to upload speeds so list order = upload-speed order desc."""
    return {u: 10_000 - i for i, u in enumerate(users)}


def _match_for(username: str, file_dir: str) -> MatchResult:
    """Build a MatchResult that matches strictly for one user."""
    return MatchResult(
        matched=True,
        directory={
            "directory": file_dir,
            "files": [{"filename": "01 - Track.flac", "size": 123}],
        },
        file_dir=file_dir,
        candidates=[],
    )


def _nomatch() -> MatchResult:
    return MatchResult(matched=False, directory={}, file_dir="", candidates=[])


def _always_nomatch(*_args, **_kwargs) -> MatchResult:
    """Stub match_fn that never matches.

    Replaces the legacy ``return_value=_nomatch()`` patch on the
    check_for_match module attribute."""
    return _nomatch()


def _const_match(result: MatchResult):
    """Stub match_fn that always returns ``result``.

    Replaces the legacy ``return_value=result`` patch on the
    check_for_match module attribute."""

    def _fn(*_args, **_kwargs) -> MatchResult:
        return result

    return _fn


class _RecordingMatchFn:
    """Recorder match_fn for tests that previously bound the
    check_for_match module attribute via patch and then asserted on
    call shape (``assert_not_called``, ``call_count``, ``call_args``).

    Wraps an inner stub and records each invocation's positional args so
    tests can assert call counts and arguments without mocking module
    globals.
    """

    def __init__(self, inner=_always_nomatch):
        self._inner = inner
        self.calls: list[tuple] = []

    def __call__(self, *args, **kwargs):
        self.calls.append(args)
        return self._inner(*args, **kwargs)

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def assert_not_called(self) -> None:
        if self.calls:
            raise AssertionError(
                f"expected match_fn never to be called, got {len(self.calls)} call(s)"
            )


class TestAdvertisedSizeAdmission(unittest.TestCase):
    """#1301 defense in depth at both outer enqueue boundaries."""

    def test_six_zero_byte_mp3s_never_claim_or_enqueue(self) -> None:
        cfg = _make_cfg()
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        username = "sevennorth"
        file_dir = "Music\\sevennorth\\Summer 2002 Tour EP"
        ctx.user_upload_speed[username] = 10_000
        filenames = (
            "01 - sea and the rhythm.mp3",
            "02 - jesus the mexican boy.mp3",
            "03 - red dust.mp3",
            "04 - someday the waves.mp3",
            "05 - overhead.mp3",
            "06 - dead man's will.mp3",
        )
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [
                    {"filename": filename, "size": 0, "bitRate": 216}
                    for filename in filenames
                ],
            },
            file_dir=file_dir,
            candidates=[],
        )
        results = {username: {"mp3": [file_dir]}}
        enqueue = MagicMock()

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", enqueue):
            attempt = try_enqueue(
                _make_tracks(), results, "mp3", ctx,
                match_fn=_const_match(match),
            )

        self.assertFalse(attempt.matched)
        enqueue.assert_not_called()
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])
        self.assertEqual(db.record_transfer_enqueue_calls, [])

    def test_multi_disc_zero_byte_audio_never_claims_or_enqueues(self) -> None:
        cfg = _make_cfg()
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        release = MagicMock()
        release.media = [
            MagicMock(medium_number=1),
            MagicMock(medium_number=2),
        ]
        results = {
            "u00": {"flac": ["u00\\Disc 1"]},
            "u01": {"flac": ["u01\\Disc 2"]},
        }

        def match_disc(tracks, _filetype, _dirs, username, _ctx):
            disc_no = tracks[0]["mediumNumber"]
            expected_user = "u00" if disc_no == 1 else "u01"
            if username != expected_user:
                return _nomatch()
            return MatchResult(
                matched=True,
                directory={
                    "directory": f"{username}\\Disc {disc_no}",
                    "files": [{
                        "filename": f"{disc_no:02d}.flac",
                        "size": 0 if disc_no == 1 else 1,
                    }],
                },
                file_dir=f"{username}\\Disc {disc_no}",
                candidates=[],
            )

        enqueue = MagicMock()
        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", enqueue):
            attempt = try_multi_enqueue(
                release,
                _multi_disc_tracks(),
                results,
                "flac",
                ctx,
                match_fn=match_disc,
            )

        self.assertFalse(attempt.matched)
        enqueue.assert_not_called()
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])
        self.assertEqual(db.record_transfer_enqueue_calls, [])


# ---------------------------------------------------------------------------
# Wave-shape tests
# ---------------------------------------------------------------------------


class TestWaveShape(unittest.TestCase):
    """Pin how many users land in each fan-out wave."""

    def test_top_k_hit_fans_out_once(self):
        """Match in top-5 of 30 users, K=20 → single fan-out wave covering top-20."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(30)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        winner = users[3]

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            if username == winner:
                return _match_for(winner, f"Music\\{winner}\\Album")
            return _nomatch()

        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan, \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(m_fan.call_count, 1, "expected a single fan-out wave for top-K hit")
        work = m_fan.call_args[0][0]
        wave_users = {u for (u, _d) in work}
        # Match was at rank 3, but the entire wave's work is submitted before
        # matching iterates — the work covers the top-K (20) by upload speed.
        self.assertEqual(len(wave_users), 20, f"expected 20 users in wave-1 work, got {len(wave_users)}")
        self.assertEqual(wave_users, set(users[:20]))

    def test_lazy_tail_hit_fans_out_two_waves(self):
        """Match at rank 35 of 50, K=20 → two fan-out waves, third never."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(50)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        winner = users[35]

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            if username == winner:
                return _match_for(winner, f"Music\\{winner}\\Album")
            return _nomatch()

        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan, \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(m_fan.call_count, 2, "expected exactly two fan-out waves")
        wave2_work = m_fan.call_args_list[1][0][0]
        wave2_users = {u for (u, _d) in wave2_work}
        self.assertEqual(wave2_users, set(users[20:40]))

    def test_all_peers_miss_fans_out_every_wave(self):
        """30 users, no match → ceil(30/20)=2 fan-outs, matched=False."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(30)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)

        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan, \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_always_nomatch,
            )

        self.assertFalse(attempt.matched)
        self.assertFalse(attempt.enqueue_failed)
        self.assertEqual(m_fan.call_count, 2)

    def test_zero_eligible_users_skips_fanout(self):
        """All users on cooldown or denylisted → no fan-out call at all."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(5)
        ctx = _make_ctx(
            cfg,
            user_upload_speed=_upload_speeds(users),
            cooled_down_users={users[0], users[1], users[2]},
            denied_users=[users[3], users[4]],
        )
        results = _make_results(users)

        m_match = _RecordingMatchFn()
        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan:
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=m_match,
            )

        self.assertFalse(attempt.matched)
        m_fan.assert_not_called()
        m_match.assert_not_called()

    def test_fewer_than_k_eligible_runs_single_short_wave(self):
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(10)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)

        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan:
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_always_nomatch,
            )

        self.assertEqual(m_fan.call_count, 1)
        work = m_fan.call_args[0][0]
        wave_users = {u for (u, _d) in work}
        self.assertEqual(wave_users, set(users))

    def test_cached_entries_skipped_from_work_list(self):
        """Pre-populate folder_cache for half the dirs → only uncached are submitted."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(4)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        # Pre-cache the first two users' directories.
        for u in users[:2]:
            ctx.folder_cache[u] = {f"Music\\{u}\\Album": {"directory": "x", "files": []}}

        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan:
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_always_nomatch,
            )

        self.assertEqual(m_fan.call_count, 1)
        work = m_fan.call_args[0][0]
        # Only the two un-cached users contribute work items.
        work_users = {u for (u, _d) in work}
        self.assertEqual(work_users, set(users[2:]))

    def test_primary_fanout_browse_time_is_recorded(self):
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(2)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)

        def fake_fanout(work, slskd, ctx, max_workers):
            import time

            time.sleep(0.001)
            for user, file_dir in work:
                ctx.folder_cache.setdefault(user, {})[file_dir] = {
                    "directory": file_dir,
                    "files": [],
                }

        with patch("lib.enqueue._fanout_browse_users", side_effect=fake_fanout):
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_always_nomatch,
            )

        self.assertGreater(ctx.counters.browse_time_s, 0.0)
        self.assertEqual(ctx.counters.fanout_waves, 1)
        self.assertEqual(ctx.counters.peers_browsed, 2)

    def test_primary_negative_skips_are_visible_to_matching(self):
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(1)
        user = users[0]
        file_dir = f"Music\\{user}\\Album"
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        browse_result = BrowseManyResult(
            negative_skips={(user, file_dir)},
            browse_attempts=0,
        )

        def fake_match(_tracks, _allowed_filetype, _dirs, username, ctx):
            self.assertEqual(username, user)
            self.assertIn((user, file_dir), ctx.peer_cache_negative_skips)
            return _nomatch()

        with patch("lib.enqueue._fanout_browse_users", return_value=browse_result):
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=fake_match,
            )

        self.assertEqual(ctx.counters.peers_browsed, 0)
        self.assertEqual(ctx.peer_cache_negative_skips, {(user, file_dir)})


# ---------------------------------------------------------------------------
# Per-cycle scope of broken_user
# ---------------------------------------------------------------------------


class TestBrokenUserPerCycle(unittest.TestCase):
    def test_broken_user_is_per_cycle_not_persistent(self):
        """A fresh CratediggerContext starts with empty broken_user."""
        cfg = _make_cfg()
        ctx = CratediggerContext(
            collaborators=make_cycle_collaborators(
                cfg=cfg,
                slskd=FakeSlskdAPI(),
                pipeline_db_source=FakePipelineDBSource(),
            ),
        )
        self.assertEqual(ctx.broken_user, set())


class _PrefetchSource(FakePipelineDBSource):
    """FakePipelineDBSource variant that returns a configurable stub list
    from ``get_tracks`` so the prefetch-contract test can assert on the
    exact rows the source delivered without the production negative-ID
    transform interfering."""

    def __init__(self, stub_tracks: list[TrackRecord]) -> None:
        super().__init__()
        self._stub_tracks = list(stub_tracks)

    def get_tracks(self, album_record: object) -> list[TrackRecord]:
        self.get_tracks_calls.append(album_record)
        return list(self._stub_tracks)


class TestFindDownloadWorkerContext(unittest.TestCase):
    def test_worker_context_snapshots_inputs_and_prefetches_db_data(self):
        cfg = _make_cfg()
        db = FakePipelineDB()
        db.denylist.append(DenylistEntry(request_id=1, username="blocked"))
        source = _PrefetchSource(
            stub_tracks=[{"albumId": 1, "title": "Track 1", "mediumNumber": 1}],
        )
        source.db = db
        ctx = CratediggerContext(
            collaborators=make_cycle_collaborators(
                cfg=cfg,
                slskd=FakeSlskdAPI(),
                pipeline_db_source=source,
                claimed_queue_keys_registry=ClaimedQueueKeysRegistry(),
            ),
            search_cache={1: {"fast": {"flac": ["dirA"]}}},
            user_upload_speed={"fast": 100},
            search_dir_audio_count={"fast": {"dirA": 1}},
            cooled_down_users={"cooled"},
        )
        album = MagicMock(id=1, db_request_id=1)

        search_result = MagicMock(
            cache_entries={"fast": {"flac": ["dirA"]}},
            upload_speeds={"fast": 100},
            dir_audio_counts={"fast": {"dirA": 1}},
        )

        worker_ctx = prepare_find_download_context(album, ctx, search_result)

        ctx.search_cache[1]["fast"]["flac"].append("dirB")
        ctx.user_upload_speed["fast"] = 1
        ctx.search_dir_audio_count["fast"]["dirA"] = 99
        ctx.cooled_down_users.add("late")

        self.assertEqual(worker_ctx.search_cache[1]["fast"]["flac"], ["dirA"])
        self.assertEqual(worker_ctx.user_upload_speed["fast"], 100)
        self.assertEqual(worker_ctx.search_dir_audio_count["fast"]["dirA"], 1)
        self.assertEqual(worker_ctx.cooled_down_users, {"cooled"})
        self.assertEqual(worker_ctx.denied_users_cache[1], {"blocked"})
        self.assertIs(worker_ctx.folder_cache, ctx.folder_cache)
        self.assertIs(worker_ctx.browse_coordinator, ctx.browse_coordinator)
        # The counters are the scratch that must NOT be shared (#1348).
        # A worker fills its own and the owner adds the totals back in
        # through FindDownloadMetrics, so an aliased value would count
        # this album's browse time, peers and waves twice, on every
        # album, in both the parallel and serial paths. This is the
        # adapter that decides it; a two-fresh-contexts test cannot.
        self.assertIsNot(worker_ctx.counters, ctx.counters)
        worker_ctx.counters.peers_browsed += 4
        self.assertEqual(ctx.counters.peers_browsed, 0)
        # #1178 PR2 review F1: the same-cycle registry MUST be the same
        # object, not a fresh one per worker -- a fresh registry per
        # worker silently degrades the guard to cross-cycle-only, which
        # does not reliably catch a genuine same-cycle collision (neither
        # sibling has an accepted ledger row yet when the other's guard
        # runs).
        self.assertIsNotNone(ctx.claimed_queue_keys_registry)
        self.assertIs(
            worker_ctx.claimed_queue_keys_registry,
            ctx.claimed_queue_keys_registry,
        )

        # After the prefetch, additional ``get_album_tracks`` calls must
        # not reach back to the source. Reset the call counter so we
        # measure only what happens after prepare_find_download_context.
        calls_before = len(source.get_tracks_calls)
        self.assertEqual(get_album_tracks(album, worker_ctx), [
            {"albumId": 1, "title": "Track 1", "mediumNumber": 1},
        ])
        self.assertEqual(len(source.get_tracks_calls), calls_before,
                         "get_tracks must not be re-invoked after prefetch")
        with self.assertRaises(AssertionError):
            worker_ctx.pipeline_db_source._get_db()

    def test_worker_db_sentinel_is_not_swallowed_by_denylist_lookup(self):
        from lib.enqueue import _get_denied_users

        ctx = CratediggerContext(
            collaborators=make_cycle_collaborators(
                cfg=_make_cfg(),
                slskd=FakeSlskdAPI(),
                pipeline_db_source=_WorkerPipelineDBSource(),
            ),
        )

        with self.assertRaises(AssertionError):
            _get_denied_users(1, ctx)


# ---------------------------------------------------------------------------
# Enqueue-failure path (had_enqueue_failure tracking)
# ---------------------------------------------------------------------------


class TestEnqueueFailureTracking(unittest.TestCase):
    def test_enqueue_exception_marks_flag_and_keeps_iterating(self):
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(3)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        # Every user matches; first user's enqueue raises, second returns None,
        # third also matches but enqueue should be tried until success or end.
        match_returns = {
            u: _match_for(u, f"Music\\{u}\\Album") for u in users
        }

        enqueue_calls: list[str] = []

        def fake_enqueue(*, username, files, file_dir, ctx, **_ledger_kwargs):
            enqueue_calls.append(username)
            if username == users[0]:
                raise RuntimeError("transient slskd hiccup")
            if username == users[1]:
                return None  # treated as failure, keep iterating
            return [MagicMock()]

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_do_enqueue", side_effect=fake_enqueue):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx,
                match_fn=lambda tracks, ft, dirs, u, ctx: match_returns[u],
            )

        # All three users were tried; final user succeeded.
        self.assertEqual(enqueue_calls, list(users))
        self.assertTrue(attempt.matched)

    def test_enqueue_failure_with_no_eventual_success_sets_flag(self):
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(2)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        match_returns = {
            u: _match_for(u, f"Music\\{u}\\Album") for u in users
        }

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=None):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx,
                match_fn=lambda tracks, ft, dirs, u, ctx: match_returns[u],
            )

        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)


def _candidate_score(username: str, dir_: str) -> CandidateScore:
    """One forensic score row, in the cheap sub-count-gate shape."""
    return CandidateScore(
        username=username, dir=dir_, filetype="flac",
        matched_tracks=0, total_tracks=1, avg_ratio=0.0,
        missing_titles=[], file_count=0,
    )


def _scored_match(
    username: str,
    *,
    matched: bool,
    candidates: int,
    skips: int,
    empty_after_filter: bool = False,
) -> MatchResult:
    """A MatchResult carrying forensics, matched or not.

    ``empty_after_filter`` is a match whose directory holds no enqueueable
    file — the world both lanes handle with their own "nothing remained
    after filtering and admission" return. Without it those two return
    sites are unreachable from any generated world (issue #1313 review,
    reader F3).
    """
    file_dir = f"Music\\{username}\\Album"
    files = (
        [] if empty_after_filter
        else [{"filename": "01 - Track.flac", "size": 123}]
    )
    return MatchResult(
        matched=matched,
        directory=(
            {"directory": file_dir, "files": files} if matched else {}
        ),
        file_dir=file_dir if matched else "",
        candidates=[
            _candidate_score(username, f"{file_dir}\\{i}")
            for i in range(candidates)
        ],
        pre_filter_skip_count=skips,
    )


def run_forensics_world(
    plan: list[tuple[bool, int, int]],
    *,
    enqueue_succeeds: bool = True,
    lane: str = "single",
    empty_after_filter: bool = False,
) -> tuple[EnqueueAttempt, list[tuple[int, int]]]:
    """Drive one real enqueue lane over a per-peer ``(matched, candidates,
    skips)`` plan, returning the attempt and what ``match_fn`` was asked for.

    ``consumed`` is recorded per call rather than derived from ``plan``, so
    the expectation follows the lane's real iteration — the single lane walks
    peers until one is kept, the multi lane takes each disc's first match.
    Shared by the deterministic pins here and the generated property in
    ``tests/test_enqueue_admission_generated.py`` (issue #1313 review).
    """
    cfg = _make_cfg(browse_top_k=20)
    users = _ranked_users(len(plan))
    ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
    results = _make_results(users)
    specs = dict(zip(users, plan, strict=True))
    consumed: list[tuple[int, int]] = []

    def match_fn(_tracks, _ft, _dirs, username, _ctx):
        matched, candidates, skips = specs[username]
        consumed.append((candidates, skips))
        return _scored_match(
            username, matched=matched, candidates=candidates, skips=skips,
            empty_after_filter=empty_after_filter,
        )

    with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
         patch(
             "lib.enqueue.slskd_do_enqueue",
             return_value=[MagicMock()] if enqueue_succeeds else None,
         ):
        if lane == "multi":
            release = MagicMock()
            release.media = [
                MagicMock(medium_number=1), MagicMock(medium_number=2)]
            attempt = try_multi_enqueue(
                release, _multi_disc_tracks(), results, "flac", ctx,
                match_fn=match_fn,
            )
        else:
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=match_fn,
            )
    return attempt, consumed


class TestAttemptReportsItsForensics(unittest.TestCase):
    """Whatever the outcome, an attempt reports what matching cost it.

    ``search_log.candidates`` and ``search_log.pre_filter_skip_count`` are
    the request's search history, and both are populated from the returned
    ``EnqueueAttempt`` regardless of whether anything matched. Before issue
    #1313 every one of the two impls' nineteen return sites repeated the two
    keyword arguments by hand. Dropping the skip count would have gone
    unnoticed by every test in the tree; dropping the candidates would have
    been caught only indirectly, by two integration slices asserting the
    persisted ``search_log`` row.
    """

    def _assert_reports_every_visit(self, attempt, consumed):
        self.assertEqual(
            len(attempt.candidates),
            sum(candidates for candidates, _ in consumed),
        )
        self.assertEqual(
            attempt.pre_filter_skip_count,
            sum(skips for _, skips in consumed),
        )

    def test_a_match_still_reports_the_peers_it_walked_past(self):
        """The decisive world: matching on the second peer must not throw
        away the first peer's skipped dirs, which is the whole reason the
        skip count is accumulated rather than read off the winner."""
        attempt, consumed = run_forensics_world(
            [(False, 2, 3), (True, 1, 1)])

        self.assertTrue(attempt.matched)
        self.assertEqual(len(consumed), 2)
        self.assertEqual(len(attempt.candidates), 3)
        self.assertEqual(attempt.pre_filter_skip_count, 4)
        self._assert_reports_every_visit(attempt, consumed)

    def test_an_attempt_that_matches_nothing_still_reports_them_all(self):
        attempt, consumed = run_forensics_world(
            [(False, 1, 2), (False, 3, 0)])

        self.assertFalse(attempt.matched)
        self.assertEqual(len(attempt.candidates), 4)
        self.assertEqual(attempt.pre_filter_skip_count, 2)
        self._assert_reports_every_visit(attempt, consumed)

    def test_a_failed_enqueue_reports_them_too(self):
        attempt, consumed = run_forensics_world(
            [(True, 2, 5)], enqueue_succeeds=False)

        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)
        self.assertEqual(len(attempt.candidates), 2)
        self.assertEqual(attempt.pre_filter_skip_count, 5)
        self._assert_reports_every_visit(attempt, consumed)

    def test_the_multi_disc_lane_reports_every_disc_it_walked(self):
        """The multi lane restarts matching per disc, so the same peers get
        asked twice and both walks count. Its fifteen return sites were
        reached by none of the pins above (issue #1313 review, reader F3)."""
        attempt, consumed = run_forensics_world(
            [(False, 1, 2), (True, 2, 1)], lane="multi")

        self.assertGreater(len(consumed), 2)
        self._assert_reports_every_visit(attempt, consumed)


    def _multi_disc_partial(self, discs_that_match: int):
        """Drive try_multi_enqueue where only some discs find a folder."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = _multi_disc_tracks()

        def match_fn(disc_tracks, _ft, _dirs, username, _ctx):
            disc = disc_tracks[0]["mediumNumber"]
            return _scored_match(
                username,
                matched=disc <= discs_that_match,
                candidates=1,
                skips=1,
            )

        def accept(*, username, files, file_dir, ctx, **_ledger_kwargs):
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id=f"transfer-{username}",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome", side_effect=accept,
             ):
            return try_multi_enqueue(
                release, tracks, _make_results(_ranked_users(2)), "flac", ctx,
                match_fn=match_fn,
            ), db

    def test_a_multi_disc_album_missing_a_disc_is_not_a_match(self):
        """The world no multi-disc test built, and the one that crashes.

        Disc 1 finds a folder, disc 2 does not, so the candidate is
        incomplete and the attempt must report ``matched=False``. Nothing
        constructed this world before, so both of the multi lane's
        not-every-disc returns could report ``matched=True`` with no
        downloads and the entire enqueue suite stayed green (issue #1313,
        mutant runner mutants 3 and 6). That is not a cosmetic wrong flag:
        ``_try_filetype`` asserts ``attempt.downloads is not None`` the
        moment ``matched`` is true, so it takes the cycle down with a bare
        AssertionError the next time an album is missing a disc.
        """
        attempt, db = self._multi_disc_partial(discs_that_match=1)

        self.assertFalse(attempt.matched)
        self.assertIsNone(attempt.downloads)
        self.assertFalse(attempt.enqueue_failed)
        # Still reports what the walk cost, on the incomplete-candidate path.
        self.assertTrue(attempt.candidates)
        self.assertTrue(attempt.pre_filter_skip_count)
        self.assertEqual(db.request(1)["status"], "wanted")

    def test_a_multi_disc_album_finding_every_disc_still_matches(self):
        """Must-still-work: the guard above must not refuse a whole album."""
        attempt, _db = self._multi_disc_partial(discs_that_match=2)

        self.assertTrue(attempt.matched)
        self.assertIsNotNone(attempt.downloads)


class TestDownloadOwnershipPreclaim(unittest.TestCase):
    @staticmethod
    def _two_disc_release_and_tracks() -> tuple[MagicMock, list[TrackRecord]]:
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = cast(
            "list[TrackRecord]",
            [
                {"albumId": 1, "title": "Disc1 Track", "mediumNumber": 1},
                {"albumId": 1, "title": "Disc2 Track", "mediumNumber": 2},
            ],
        )
        return release, tracks

    @staticmethod
    def _match_two_discs(
        tracks: Sequence[TrackRecord],
        _allowed_filetype: str,
        file_dirs: list[str],
        username: str,
        _ctx: CratediggerContext,
    ) -> MatchResult:
        disc_no = tracks[0]["mediumNumber"]
        expected_user = "u00" if disc_no == 1 else "u01"
        if username != expected_user:
            return _nomatch()
        file_dir = file_dirs[0]
        return MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{
                    "filename": f"d{disc_no}.flac",
                    "size": 111 * disc_no,
                }],
            },
            file_dir=file_dir,
            candidates=[],
        )

    def test_filtered_empty_match_does_not_claim_or_enqueue(self):
        cfg = replace(_make_cfg(browse_top_k=20), download_filtering=True)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        file_dir = "Music\\u00\\Album"
        results = {"u00": {"mp3": [file_dir]}}
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01 - Track 1.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        enqueue = MagicMock(return_value=SlskdEnqueueOutcome(
            status="accepted",
            downloads=[],
        ))
        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", enqueue):
            attempt = try_enqueue(
                _make_tracks(), results, "mp3", ctx, match_fn=_const_match(match),
            )

        self.assertFalse(attempt.matched)
        self.assertFalse(attempt.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])
        self.assertEqual(db.status_history, [])
        enqueue.assert_not_called()

    def test_multi_disc_filtered_empty_match_does_not_claim_or_enqueue(self):
        cfg = replace(_make_cfg(browse_top_k=20), download_filtering=True)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        file_dir = "Music\\u00\\Album"
        results = {"u00": {"mp3": [file_dir]}}
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = cast(
            "list[TrackRecord]",
            [
                {"albumId": 1, "title": "Disc1 Track", "mediumNumber": 1},
                {"albumId": 1, "title": "Disc2 Track", "mediumNumber": 2},
            ],
        )
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01 - Track 1.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        enqueue = MagicMock(return_value=SlskdEnqueueOutcome(
            status="accepted",
            downloads=[],
        ))
        with self.assertLogs("cratedigger", level="INFO") as log_ctx, \
             patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", enqueue):
            attempt = try_multi_enqueue(
                release, tracks, results, "mp3", ctx,
                match_fn=_const_match(match),
            )

        self.assertFalse(attempt.matched)
        self.assertFalse(attempt.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])
        self.assertEqual(db.status_history, [])
        enqueue.assert_not_called()
        self.assertTrue(any(
            "album_browse" in line and "matched=False" in line
            for line in log_ctx.output
        ))

    def test_claims_downloading_before_slskd_enqueue(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        users = ["u00"]
        results = _make_results(users)
        file_dir = "Music\\u00\\Album"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        # Capture the row as observed at the moment slskd is called. Assertions
        # must NOT live inside this closure: production wraps the enqueue call
        # in a broad try/except (lib/enqueue.py::_leave_claim_for_poll_recovery)
        # that swallows any exception once the claim has landed, so an
        # AssertionError raised here would be masked and the test would pass
        # regardless. Assert in the test body instead.
        observed: dict[str, Any] = {}

        def fake_enqueue(*, username, files, file_dir, ctx, **_ledger_kwargs):
            row = db.request(1)
            observed["status"] = row["status"]
            observed["state"] = json.loads(row["active_download_state"])
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id="transfer-1",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.status_history, [(1, "downloading")])
        self.assertEqual(db.request(1)["status"], "downloading")
        # The claim landed BEFORE slskd was called: fake_enqueue saw the row
        # already downloading with the planned state.
        self.assertEqual(observed["status"], "downloading")
        self.assertEqual(observed["state"]["filetype"], "flac")
        self.assertEqual(observed["state"]["files"][0]["username"], "u00")
        self.assertEqual(
            observed["state"]["files"][0]["filename"],
            "Music\\u00\\Album\\01.flac",
        )
        # current_path is unset at claim time (before slskd returns transfer
        # IDs). The msgspec encoder omits it when None (issue #467), so read it
        # via .get() as production does.
        self.assertIsNone(observed["state"].get("current_path"))

    def test_claim_enqueued_at_threads_as_reconciliation_not_before(self):
        """issue #822 item 3: the claim's own pre-POST timestamp
        (captured before writer.claim_downloading, strictly before the
        slskd POST — same source as the not_before already threaded to
        rederive_transfer_ids via ``_visible_claim_transfers``) is passed
        as slskd_enqueue_with_outcome's not_before boundary, so post-POST
        transfer-ID reconciliation is attempt-scoped rather than
        all-history."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        users = ["u00"]
        results = _make_results(users)
        file_dir = "Music\\u00\\Album"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        observed: dict[str, str | None] = {}

        def fake_enqueue(
            *, username, files, file_dir, ctx, not_before=None, **_ledger_kwargs,
        ):
            row = db.request(1)
            observed["not_before"] = not_before
            observed["state_enqueued_at"] = json.loads(
                row["active_download_state"])["enqueued_at"]
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id="transfer-1",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        self.assertIsNotNone(observed["not_before"])
        self.assertEqual(observed["not_before"], observed["state_enqueued_at"])

    def test_single_same_path_attempt_a_outcomes_do_not_escape_after_b_replaces_a(
        self,
    ):
        """A/EA outcomes are stale after same-path B/EB replaces the claim."""
        cases: list[tuple[str, SlskdEnqueueOutcome | Exception, bool]] = [
            (
                "accepted",
                SlskdEnqueueOutcome(
                    status="accepted",
                    downloads=[DownloadFile(
                        filename="Music\\u00\\Album\\01.flac",
                        id="attempt-a-transfer",
                        file_dir="Music\\u00\\Album",
                        username="u00",
                        size=123,
                    )],
                ),
                False,
            ),
            (
                "ambiguous",
                SlskdEnqueueOutcome(
                    status="unknown",
                    reason="attempt A response was uncertain",
                ),
                False,
            ),
            (
                "rejected_fallback_persistence",
                SlskdEnqueueOutcome(
                    status="rejected",
                    reason="attempt A was rejected",
                ),
                True,
            ),
            ("post_runtime_error", RuntimeError("attempt A POST failed"), False),
        ]

        for name, outcome_or_error, snapshot_fails in cases:
            with self.subTest(name=name):
                cfg = _make_cfg(browse_top_k=20)
                db = _AttemptReplacingPipelineDB()
                db.seed_request(make_request_row(id=1, status="wanted"))
                slskd = FakeSlskdAPI()
                if snapshot_fails:
                    slskd.transfers.get_all_downloads_error = RuntimeError(
                        "snapshot unavailable",
                    )
                ctx = _ctx_with_download_ownership(
                    cfg=cfg,
                    db=db,
                    slskd=slskd,
                )

                enqueue_side_effect = (
                    outcome_or_error
                    if isinstance(outcome_or_error, Exception)
                    else None
                )
                enqueue_return = (
                    outcome_or_error
                    if isinstance(outcome_or_error, SlskdEnqueueOutcome)
                    else None
                )
                with patch(
                    "lib.enqueue._fanout_browse_users",
                    return_value=set(),
                ), patch(
                    "lib.enqueue.slskd_enqueue_with_outcome",
                    return_value=enqueue_return,
                    side_effect=enqueue_side_effect,
                ):
                    attempt = try_enqueue(
                        _make_tracks(),
                        _make_results(["u00"]),
                        "flac",
                        ctx,
                        match_fn=_const_match(_match_for(
                            "u00",
                            "Music\\u00\\Album",
                        )),
                    )

                self.assertTrue(db.state_witnesses)
                expected_attempt_a = db.state_witnesses[0][0]
                self.assertNotEqual(expected_attempt_a, "attempt-b")
                self.assertEqual(
                    db.state_witnesses,
                    [(expected_attempt_a, expected_attempt_a)],
                )
                self.assertIsNotNone(db.attempt_b)
                self.assertEqual(
                    _request_active_state(db),
                    db.attempt_b,
                )
                self.assertEqual(db.request(1)["status"], "downloading")
                self.assertFalse(attempt.matched)
                self.assertTrue(attempt.enqueue_failed)
                self.assertIsNone(attempt.downloads)
                self.assertEqual(db.download_logs, [])
                self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_multi_same_path_attempt_a_outcomes_do_not_escape_after_b_replaces_a(
        self,
    ):
        """Whole-album A/EA results stay internal once B/EB owns the row."""
        for outcome_name in (
            "fully_accepted",
            "later_ambiguous",
            "later_exception",
        ):
            with self.subTest(outcome_name=outcome_name):
                cfg = _make_cfg(browse_top_k=20)
                db = _AttemptReplacingPipelineDB()
                db.seed_request(make_request_row(id=1, status="wanted"))
                slskd = FakeSlskdAPI()
                ctx = _ctx_with_download_ownership(
                    cfg=cfg,
                    db=db,
                    slskd=slskd,
                )
                enqueue_calls = 0

                def fake_enqueue(
                    *,
                    username: str,
                    files: list[dict[str, object]],
                    file_dir: str,
                    _outcome_name: str = outcome_name,
                    **_kwargs: object,
                ) -> SlskdEnqueueOutcome:
                    nonlocal enqueue_calls
                    enqueue_calls += 1
                    if enqueue_calls == 2:
                        if _outcome_name == "later_ambiguous":
                            return SlskdEnqueueOutcome(
                                status="unknown",
                                reason="attempt A response was uncertain",
                            )
                        if _outcome_name == "later_exception":
                            raise RuntimeError("attempt A POST failed")
                    filename, size = _enqueue_file_identity(files)
                    return SlskdEnqueueOutcome(
                        status="accepted",
                        downloads=[DownloadFile(
                            filename=filename,
                            id=f"attempt-a-transfer-{enqueue_calls}",
                            file_dir=file_dir,
                            username=username,
                            size=size,
                        )],
                    )

                release, tracks = self._two_disc_release_and_tracks()
                with patch(
                    "lib.enqueue._fanout_browse_users",
                    return_value=set(),
                ), patch(
                    "lib.enqueue.slskd_enqueue_with_outcome",
                    side_effect=fake_enqueue,
                ):
                    attempt = try_multi_enqueue(
                        release,
                        tracks,
                        _make_results(["u00", "u01"]),
                        "flac",
                        ctx,
                        match_fn=self._match_two_discs,
                    )

                self.assertTrue(db.state_witnesses)
                expected_attempt_a = db.state_witnesses[0][0]
                self.assertEqual(
                    db.state_witnesses,
                    [(expected_attempt_a, expected_attempt_a)],
                )
                self.assertIsNotNone(db.attempt_b)
                self.assertEqual(
                    _request_active_state(db),
                    db.attempt_b,
                )
                self.assertEqual(db.request(1)["status"], "downloading")
                self.assertFalse(attempt.matched)
                self.assertTrue(attempt.enqueue_failed)
                self.assertIsNone(attempt.downloads)
                self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_process_death_after_claim_leaves_planned_state_owned(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        users = ["u00"]
        results = _make_results(users)
        file_dir = "Music\\u00\\Album"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        row = db.request(1)
        self.assertEqual(row["status"], "downloading")
        state = json.loads(row["active_download_state"])
        self.assertEqual(state["files"][0]["filename"], "Music\\u00\\Album\\01.flac")

    def test_verified_no_acceptance_resets_to_wanted(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(
            cfg=cfg,
            db=db,
            slskd=FakeSlskdAPI(downloads=[]),
        )
        users = ["u00"]
        results = _make_results(users)
        file_dir = "Music\\u00\\Album"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])
        self.assertEqual(db.status_history, [(1, "downloading"), (1, "wanted")])
        self.assertEqual(db.recorded_attempts, [(1, "download")])
        self.assertIsNotNone(db.request(1)["next_retry_after"])

    def test_offline_presence_skips_enqueue_without_claim(self):
        """When ``users.status`` reports the matched peer as ``Offline``,
        ``try_enqueue`` must skip the enqueue entirely — no claim, no
        ``download_log`` row, no ``transfers.enqueue`` call."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[])
        slskd.users.set_status("u00", "Offline")
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00"]
        results = _make_results(users)
        match = _match_for("u00", "Music\\u00\\Album")
        enqueue_mock = MagicMock()

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", enqueue_mock):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        # Probe was consulted; enqueue was never called.
        self.assertEqual(slskd.users.status_calls, ["u00"])
        enqueue_mock.assert_not_called()
        # Request stayed wanted; no claim made; no log written.
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])
        self.assertEqual(db.download_logs, [])
        self.assertFalse(attempt.matched)

    def test_online_presence_proceeds_to_enqueue(self):
        """``Online`` presence is a no-op — enqueue runs as before."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[])
        slskd.users.set_status("u00", "Online")
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00"]
        results = _make_results(users)
        match = _match_for("u00", "Music\\u00\\Album")

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(
                     status="accepted",
                     downloads=[DownloadFile(
                         filename="Music\\u00\\Album\\01.flac",
                         id="tid-1",
                         file_dir="Music\\u00\\Album",
                         username="u00",
                         size=123,
                     )],
                 ),
             ):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertEqual(slskd.users.status_calls, ["u00"])
        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")

    def test_away_presence_treated_as_online(self):
        """``Away`` peers can still serve uploads — proceed to enqueue."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[])
        slskd.users.set_status("u00", "Away")
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00"]
        results = _make_results(users)
        match = _match_for("u00", "Music\\u00\\Album")

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(
                     status="accepted",
                     downloads=[DownloadFile(
                         filename="Music\\u00\\Album\\01.flac",
                         id="tid-1",
                         file_dir="Music\\u00\\Album",
                         username="u00",
                         size=123,
                     )],
                 ),
             ):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")

    def test_status_exception_falls_through_to_enqueue(self):
        """If the probe raises, fall through to enqueue. The user-offline
        classification in ``slskd_enqueue_with_outcome`` is the safety
        net for the actual offline case."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[])
        slskd.users.set_status_error("u00", RuntimeError("status endpoint flaky"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00"]
        results = _make_results(users)
        match = _match_for("u00", "Music\\u00\\Album")

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(
                     status="accepted",
                     downloads=[DownloadFile(
                         filename="Music\\u00\\Album\\01.flac",
                         id="tid-1",
                         file_dir="Music\\u00\\Album",
                         username="u00",
                         size=123,
                     )],
                 ),
             ):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")

    def test_offline_first_user_falls_through_to_online_second(self):
        """Two ranked users: A offline, B online. Probe both; enqueue only
        B; A never claimed."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[])
        slskd.users.set_status("u00", "Offline")
        slskd.users.set_status("u01", "Online")
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00", "u01"]
        results = _make_results(users)

        def match_per_user(_tracks, _ft, _dirs, username, _ctx):
            return _match_for(username, f"Music\\{username}\\Album")

        # Each user gets its own match.
        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(
                     status="accepted",
                     downloads=[DownloadFile(
                         filename="Music\\u01\\Album\\01.flac",
                         id="tid-1",
                         file_dir="Music\\u01\\Album",
                         username="u01",
                         size=123,
                     )],
                 ),
             ) as enq:
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=match_per_user,
            )

        self.assertEqual(slskd.users.status_calls, ["u00", "u01"])
        # enqueue called once, for u01
        self.assertEqual(enq.call_count, 1)
        called_username = enq.call_args.kwargs.get("username") or enq.call_args.args[0]
        self.assertEqual(called_username, "u01")
        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        # No download_log row written for the offline skip.
        self.assertEqual(db.download_logs, [])

    def test_verified_no_acceptance_writes_user_offline_download_log(self):
        """When ``slskd_enqueue_with_outcome`` returns ``rejected`` and
        verification confirms no transfer landed, ``try_enqueue`` must
        write a ``download_log`` row recording the failed attempt — so
        the failure is surfaced in the web UI / pipeline-cli immediately
        rather than silently disappearing into a status flip."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(
            cfg=cfg,
            db=db,
            slskd=FakeSlskdAPI(downloads=[]),
        )
        # Route pipeline_db_source -> same FakePipelineDB so the download_log
        # write is observable (in production both seams connect to the same
        # Postgres; in this fixture they're independent unless wired here).
        cast(FakePipelineDBSource, ctx.pipeline_db_source).db = db
        users = ["pooyork"]
        results = _make_results(users)
        file_dir = "musiclibrary\\Mercury Rev\\Deserter's Songs"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        # One log row, attributed to the rejected user.
        self.assertEqual(len(db.download_logs), 1)
        log = db.download_logs[0]
        self.assertEqual(log.request_id, 1)
        self.assertEqual(log.soulseek_username, "pooyork")
        self.assertEqual(log.filetype, "flac")
        self.assertEqual(log.outcome, "user_offline")
        assert log.error_message is not None
        self.assertIn("offline", log.error_message.lower())

    def test_verified_no_acceptance_user_offline_log_uses_captured_reason(self):
        """Issue #564 C4: when the offline classification captured a
        reason, the download_log error_message uses it directly instead
        of the generic fallback string."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=FakeSlskdAPI(downloads=[]),
        )
        cast(FakePipelineDBSource, ctx.pipeline_db_source).db = db
        users = ["pooyork"]
        results = _make_results(users)
        file_dir = "musiclibrary\\Mercury Rev\\Deserter's Songs"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(
                     status="rejected", reason="peer appears to be offline"),
             ):
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertEqual(len(db.download_logs), 1)
        log = db.download_logs[0]
        self.assertEqual(log.outcome, "user_offline")
        self.assertEqual(log.error_message, "peer appears to be offline")

    def test_rejected_enqueue_with_visible_transfer_does_not_log(self):
        """When the rejected outcome leaves a visible transfer (the
        residual-claim safety net), the request stays in ``downloading``
        and no ``download_log`` row should be written — the attempt is
        not yet a verified failure."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        file_dir = "Music\\u00\\Album"
        slskd = FakeSlskdAPI(downloads=[{
            "username": "u00",
            "directories": [{"directory": file_dir, "files": [
                {"filename": "Music\\u00\\Album\\01.flac", "id": "transfer-1"},
            ]}],
        }])
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00"]
        results = _make_results(users)
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        # Verified-no-acceptance failed; claim left for recovery — no log.
        self.assertEqual(db.download_logs, [])

    def test_rejected_enqueue_with_visible_transfer_stays_downloading(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        file_dir = "Music\\u00\\Album"
        slskd = FakeSlskdAPI(downloads=[{
            "username": "u00",
            "directories": [{"directory": file_dir, "files": [
                {"filename": "Music\\u00\\Album\\01.flac", "id": "transfer-1"},
            ]}],
        }])
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00"]
        results = _make_results(users)
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        state_raw = db.request(1)["active_download_state"]
        state = json.loads(state_raw) if isinstance(state_raw, str) else state_raw
        self.assertEqual(state["files"][0]["username"], "u00")
        self.assertEqual(state["files"][0]["filename"], "Music\\u00\\Album\\01.flac")
        self.assertEqual(db.status_history, [(1, "downloading")])
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True])

    def test_rejected_enqueue_snapshot_failure_stays_downloading(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI()
        slskd.transfers.get_all_downloads_error = RuntimeError("snapshot down")
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00"]
        results = _make_results(users)
        file_dir = "Music\\u00\\Album"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        self.assertEqual(db.status_history, [(1, "downloading")])

    def test_ambiguous_enqueue_failure_stays_downloading_for_poll_recovery(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        users = ["u00"]
        results = _make_results(users)
        file_dir = "Music\\u00\\Album"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="unknown"),
             ):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        state_raw = db.request(1)["active_download_state"]
        state = json.loads(state_raw) if isinstance(state_raw, str) else state_raw
        self.assertEqual(state["files"][0]["username"], "u00")

    def test_multi_disc_claim_contains_all_discs_before_first_enqueue(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        users = ["u00", "u01"]
        results = _make_results(users)
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = cast(
            "list[TrackRecord]",
            [
                {"albumId": 1, "title": "Disc1 Track", "mediumNumber": 1},
                {"albumId": 1, "title": "Disc2 Track", "mediumNumber": 2},
            ],
        )

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            disc_no = tracks[0]["mediumNumber"]
            if disc_no == 1 and username == "u00":
                file_dir = file_dirs[0]
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": file_dir,
                        "files": [{"filename": "d1.flac", "size": 111}],
                    },
                    file_dir=file_dir,
                    candidates=[],
                )
            if disc_no == 2 and username == "u01":
                file_dir = file_dirs[0]
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": file_dir,
                        "files": [{"filename": "d2.flac", "size": 222}],
                    },
                    file_dir=file_dir,
                    candidates=[],
                )
            return _nomatch()

        enqueue_calls = 0

        def fake_enqueue(*, username, files, file_dir, ctx, **_ledger_kwargs):
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 1:
                state = json.loads(db.request(1)["active_download_state"])
                self.assertEqual(len(state["files"]), 2)
                self.assertEqual(
                    [(f["username"], f["disk_no"], f["disk_count"])
                     for f in state["files"]],
                    [("u00", 1, 2), ("u01", 2, 2)],
                )
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id=f"transfer-{enqueue_calls}",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_multi_enqueue(
                release, tracks, results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.status_history, [(1, "downloading")])

    def test_multi_disc_first_ambiguous_outcome_stamps_all_planned_files(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        results = _make_results(["u00", "u01"])
        release, tracks = self._two_disc_release_and_tracks()
        reason = "first disc enqueue response was uncertain"

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(
                     status="unknown",
                     reason=reason,
                 ),
             ):
            attempt = try_multi_enqueue(
                release,
                tracks,
                results,
                "flac",
                ctx,
                match_fn=self._match_two_discs,
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        state_raw = db.request(1)["active_download_state"]
        state = json.loads(state_raw) if isinstance(state_raw, str) else state_raw
        self.assertEqual(len(state["files"]), 2)
        self.assertEqual(
            [file["last_exception"] for file in state["files"]],
            [f"enqueue failed: {reason}"] * 2,
        )

    def test_multi_disc_later_ambiguous_outcome_keeps_accepted_transfer(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI()
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        results = _make_results(["u00", "u01"])
        release, tracks = self._two_disc_release_and_tracks()
        reason = "second disc enqueue response was uncertain"
        enqueue_calls = 0

        def fake_enqueue(*, username, files, file_dir, ctx, **_ledger_kwargs):
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 2:
                return SlskdEnqueueOutcome(status="unknown", reason=reason)
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id="transfer-1",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 side_effect=fake_enqueue,
             ):
            attempt = try_multi_enqueue(
                release,
                tracks,
                results,
                "flac",
                ctx,
                match_fn=self._match_two_discs,
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        self.assertEqual(slskd.transfers.cancel_download_calls, [])
        state_raw = db.request(1)["active_download_state"]
        state = json.loads(state_raw) if isinstance(state_raw, str) else state_raw
        self.assertEqual(len(state["files"]), 2)
        self.assertEqual(
            [file["last_exception"] for file in state["files"]],
            [f"enqueue failed: {reason}"] * 2,
        )

    def test_partial_same_path_b_before_recovery_write_blocks_a_cancellation(
        self,
    ):
        """B/EB replacing A/EA before the recovery write makes A stale."""
        cfg = _make_cfg(browse_top_k=20)
        db = _AttemptReplacingPipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI()
        slskd.transfers.cancel_download_result = False
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        release, tracks = self._two_disc_release_and_tracks()
        enqueue_calls = 0

        def fake_enqueue(
            *,
            username: str,
            files: list[dict[str, object]],
            file_dir: str,
            **_kwargs: object,
        ) -> SlskdEnqueueOutcome:
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 2:
                # The write-ahead row precedes the POST, so a rejection
                # still leaves pending intent behind.
                _ledger_enqueue_attempt(db, username, files, accepted=False)
                return SlskdEnqueueOutcome(status="rejected")
            filename, size = _enqueue_file_identity(files)
            slskd.add_transfer(
                username=username,
                directory=file_dir,
                filename=filename,
                id="attempt-a-transfer-1",
            )
            _ledger_enqueue_attempt(db, username, files, accepted=True)
            return SlskdEnqueueOutcome(
                status="accepted",
                downloads=[DownloadFile(
                    filename=filename,
                    id="attempt-a-transfer-1",
                    file_dir=file_dir,
                    username=username,
                    size=size,
                    last_state="attempt-a-accepted",
                )],
            )

        with patch(
            "lib.enqueue._fanout_browse_users",
            return_value=set(),
        ), patch(
            "lib.enqueue.slskd_enqueue_with_outcome",
            side_effect=fake_enqueue,
        ):
            attempt = try_multi_enqueue(
                release,
                tracks,
                _make_results(["u00", "u01"]),
                "flac",
                ctx,
                match_fn=self._match_two_discs,
            )

        self.assertIsNotNone(db.attempt_b)
        self.assertEqual(_request_active_state(db), db.attempt_b)
        self.assertEqual(len(db.update_download_state_calls), 1)
        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)
        self.assertIsNone(attempt.downloads)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    def test_partial_post_cancel_stale_write_suppresses_attempt_a_recovery(
        self,
    ):
        """A recovery is persisted before cancel; a later B wins exactly."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI()
        cancel_transfers = _ObservingCancelTransfers(
            slskd,
            db,
            replace_with_attempt_b=True,
        )
        cancel_transfers.cancel_download_result = False
        slskd.transfers = cancel_transfers
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        release, tracks = self._two_disc_release_and_tracks()
        enqueue_calls = 0

        def fake_enqueue(
            *,
            username: str,
            files: list[dict[str, object]],
            file_dir: str,
            **_kwargs: object,
        ) -> SlskdEnqueueOutcome:
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 2:
                # The write-ahead row precedes the POST, so a rejection
                # still leaves pending intent behind.
                _ledger_enqueue_attempt(db, username, files, accepted=False)
                return SlskdEnqueueOutcome(status="rejected")
            filename, size = _enqueue_file_identity(files)
            slskd.add_transfer(
                username=username,
                directory=file_dir,
                filename=filename,
                id="attempt-a-transfer-1",
            )
            _ledger_enqueue_attempt(db, username, files, accepted=True)
            return SlskdEnqueueOutcome(
                status="accepted",
                downloads=[DownloadFile(
                    filename=filename,
                    id="attempt-a-transfer-1",
                    file_dir=file_dir,
                    username=username,
                    size=size,
                    last_state="attempt-a-accepted",
                    bytes_transferred=71,
                    retry=3,
                    last_exception="attempt-a-observed-error",
                )],
            )

        with patch(
            "lib.enqueue._fanout_browse_users",
            return_value=set(),
        ), patch(
            "lib.enqueue.slskd_enqueue_with_outcome",
            side_effect=fake_enqueue,
        ):
            attempt = try_multi_enqueue(
                release,
                tracks,
                _make_results(["u00", "u01"]),
                "flac",
                ctx,
                match_fn=self._match_two_discs,
            )

        self.assertEqual(len(cancel_transfers.observed_before_cancel), 1)
        self.assertEqual(cancel_transfers.writes_before_cancel, [1])
        observed_file = cancel_transfers.observed_before_cancel[0].files[0]
        self.assertEqual(
            (
                observed_file.last_state,
                observed_file.bytes_transferred,
                observed_file.retry_count,
                observed_file.last_exception,
            ),
            (
                "attempt-a-accepted",
                71,
                3,
                "attempt-a-observed-error",
            ),
        )
        attempt_b = cancel_transfers.attempt_b
        self.assertIsNotNone(attempt_b)
        assert attempt_b is not None
        self.assertNotEqual(
            cancel_transfers.observed_before_cancel[0].enqueued_at,
            attempt_b.enqueued_at,
        )
        self.assertEqual(
            _request_active_state(db),
            attempt_b,
        )
        self.assertEqual(len(db.update_download_state_calls), 2)
        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)
        self.assertIsNone(attempt.downloads)

    def test_multi_disc_partial_failure_resets_after_verified_cancel(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI()
        cancel_transfers = _ObservingCancelTransfers(slskd, db)
        slskd.transfers = cancel_transfers
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00", "u01"]
        results = _make_results(users)
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = _multi_disc_tracks()

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            disc_no = tracks[0]["mediumNumber"]
            if disc_no == 1 and username == "u00":
                file_dir = file_dirs[0]
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": file_dir,
                        "files": [{"filename": "d1.flac", "size": 111}],
                    },
                    file_dir=file_dir,
                    candidates=[],
                )
            if disc_no == 2 and username == "u01":
                file_dir = file_dirs[0]
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": file_dir,
                        "files": [{"filename": "d2.flac", "size": 222}],
                    },
                    file_dir=file_dir,
                    candidates=[],
                )
            return _nomatch()

        enqueue_calls = 0

        def fake_enqueue(*, username, files, file_dir, ctx, **_ledger_kwargs):
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 2:
                # The write-ahead row precedes the POST, so a rejection
                # still leaves pending intent behind.
                _ledger_enqueue_attempt(db, username, files, accepted=False)
                return SlskdEnqueueOutcome(status="rejected")
            slskd.add_transfer(
                username=username,
                directory=file_dir,
                filename=files[0]["filename"],
                id="transfer-1",
            )
            _ledger_enqueue_attempt(db, username, files, accepted=True)
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id="transfer-1",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                    last_state="accepted-before-cancel",
                    bytes_transferred=83,
                    retry=4,
                    last_exception="accepted-observed-error",
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_multi_enqueue(
                release, tracks, results, "flac", ctx, match_fn=fake_match,
            )

        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertEqual(db.status_history, [(1, "downloading"), (1, "wanted")])
        self.assertEqual(
            [(call.username, call.id) for call in slskd.transfers.cancel_download_calls],
            [("u00", "transfer-1")],
        )
        self.assertEqual(len(cancel_transfers.observed_before_cancel), 1)
        self.assertEqual(cancel_transfers.writes_before_cancel, [1])
        observed_file = cancel_transfers.observed_before_cancel[0].files[0]
        self.assertEqual(
            (
                observed_file.last_state,
                observed_file.bytes_transferred,
                observed_file.retry_count,
                observed_file.last_exception,
            ),
            (
                "accepted-before-cancel",
                83,
                4,
                "accepted-observed-error",
            ),
        )

    def test_multi_disc_first_rejected_with_visible_transfer_stays_owned(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        file_dir = "Music\\u00\\Album"
        slskd = FakeSlskdAPI(downloads=[{
            "username": "u00",
            "directories": [{"directory": file_dir, "files": [
                {"filename": "Music\\u00\\Album\\d1.flac", "id": "transfer-1"},
            ]}],
        }])
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00"]
        results = _make_results(users)
        release = MagicMock()
        release.media = [MagicMock(medium_number=1)]
        tracks = cast(
            "list[TrackRecord]",
            [{"albumId": 1, "title": "Disc1 Track", "mediumNumber": 1}],
        )

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            return MatchResult(
                matched=True,
                directory={
                    "directory": file_dir,
                    "files": [{"filename": "d1.flac", "size": 111}],
                },
                file_dir=file_dir,
                candidates=[],
            )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt = try_multi_enqueue(
                release, tracks, results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        self.assertEqual(db.status_history, [(1, "downloading")])

    def test_multi_disc_partial_failure_cancel_false_stays_owned(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI()
        slskd.transfers.cancel_download_result = False
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00", "u01"]
        results = _make_results(users)
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = _multi_disc_tracks()

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            disc_no = tracks[0]["mediumNumber"]
            if disc_no == 1 and username == "u00":
                file_dir = file_dirs[0]
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": file_dir,
                        "files": [{"filename": "d1.flac", "size": 111}],
                    },
                    file_dir=file_dir,
                    candidates=[],
                )
            if disc_no == 2 and username == "u01":
                file_dir = file_dirs[0]
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": file_dir,
                        "files": [{"filename": "d2.flac", "size": 222}],
                    },
                    file_dir=file_dir,
                    candidates=[],
                )
            return _nomatch()

        enqueue_calls = 0

        def fake_enqueue(*, username, files, file_dir, ctx, **_ledger_kwargs):
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 2:
                # The write-ahead row precedes the POST, so a rejection
                # still leaves pending intent behind.
                _ledger_enqueue_attempt(db, username, files, accepted=False)
                return SlskdEnqueueOutcome(status="rejected")
            slskd.add_transfer(
                username=username,
                directory=file_dir,
                filename=files[0]["filename"],
                id="transfer-1",
            )
            _ledger_enqueue_attempt(db, username, files, accepted=True)
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id="transfer-1",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_multi_enqueue(
                release, tracks, results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        self.assertEqual(db.status_history, [(1, "downloading")])
        self.assertEqual(
            [(call.username, call.id) for call in slskd.transfers.cancel_download_calls],
            [("u00", "transfer-1")],
        )

    def test_multi_disc_partial_failure_without_transfer_ids_stays_owned(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
        users = ["u00", "u01"]
        results = _make_results(users)
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = cast(
            "list[TrackRecord]",
            [
                {"albumId": 1, "title": "Disc1 Track", "mediumNumber": 1},
                {"albumId": 1, "title": "Disc2 Track", "mediumNumber": 2},
            ],
        )

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            disc_no = tracks[0]["mediumNumber"]
            if disc_no == 1 and username == "u00":
                file_dir = file_dirs[0]
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": file_dir,
                        "files": [{"filename": "d1.flac", "size": 111}],
                    },
                    file_dir=file_dir,
                    candidates=[],
                )
            if disc_no == 2 and username == "u01":
                file_dir = file_dirs[0]
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": file_dir,
                        "files": [{"filename": "d2.flac", "size": 222}],
                    },
                    file_dir=file_dir,
                    candidates=[],
                )
            return _nomatch()

        enqueue_calls = 0

        def fake_enqueue(*, username, files, file_dir, ctx, **_ledger_kwargs):
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 2:
                return SlskdEnqueueOutcome(status="rejected")
            return SlskdEnqueueOutcome(status="accepted", downloads=[
                DownloadFile(
                    filename=files[0]["filename"],
                    id="",
                    file_dir=file_dir,
                    username=username,
                    size=files[0]["size"],
                ),
            ])

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_multi_enqueue(
                release, tracks, results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        self.assertEqual(db.request(1)["status"], "downloading")
        state_raw = db.request(1)["active_download_state"]
        state = json.loads(state_raw) if isinstance(state_raw, str) else state_raw
        self.assertEqual(len(state["files"]), 2)
        self.assertEqual(db.status_history, [(1, "downloading")])


class TestTransferLedgerThroughRealEnqueuePath(unittest.TestCase):
    """T1 integration slice (issue #571): drives try_enqueue /
    try_multi_enqueue end-to-end WITHOUT patching
    slskd_enqueue_with_outcome — the real write-ahead seam runs against a
    real FakeSlskdAPI, proving the ledger row lands through the actual
    orchestration (wave matching, claim, enqueue), not just at the
    choke-point function in isolation."""

    def test_try_enqueue_writes_a_ledger_row_for_the_real_seam(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        file_dir = "Music\\u00\\Album"
        slskd = FakeSlskdAPI(downloads=[{
            "username": "u00",
            "directories": [{
                "directory": file_dir,
                "files": [{"filename": f"{file_dir}\\01.flac", "id": "tid-1"}],
            }],
        }])
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        results = {"u00": {"flac": [file_dir]}}
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        rows = db.record_transfer_enqueue_calls
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].username, "u00")
        self.assertEqual(rows[0].filename, f"{file_dir}\\01.flac")
        self.assertIsNotNone(rows[0].attempt_fingerprint)

    def test_try_multi_enqueue_shares_one_attempt_fingerprint_across_discs(self):
        """The attempt fingerprint is computed ONCE from the whole
        multi-disc manifest (claim.entry.files) — every disc's ledger
        row must carry the SAME value, matching what
        canonical_processing_path later derives from the same manifest
        (issue #550 phase 2)."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[
            {
                "username": "u00",
                "directories": [{
                    "directory": "u00\\Music",
                    "files": [{"filename": "u00\\Music\\d1.flac", "id": "tid-d1"}],
                }],
            },
            {
                "username": "u01",
                "directories": [{
                    "directory": "u01\\Music",
                    "files": [{"filename": "u01\\Music\\d2.flac", "id": "tid-d2"}],
                }],
            },
        ])
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00", "u01"]
        results = _make_results(users)
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = _multi_disc_tracks()

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            disc_no = tracks[0]["mediumNumber"]
            if disc_no == 1 and username == "u00":
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": "u00\\Music",
                        "files": [{"filename": "d1.flac", "size": 111}],
                    },
                    file_dir="u00\\Music",
                    candidates=[],
                )
            if disc_no == 2 and username == "u01":
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": "u01\\Music",
                        "files": [{"filename": "d2.flac", "size": 222}],
                    },
                    file_dir="u01\\Music",
                    candidates=[],
                )
            return _nomatch()

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt = try_multi_enqueue(
                release, tracks, results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        rows = db.record_transfer_enqueue_calls
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {r.username for r in rows}, {"u00", "u01"})
        fingerprints = {r.attempt_fingerprint for r in rows}
        self.assertEqual(
            len(fingerprints), 1,
            f"expected one shared attempt_fingerprint across discs, got {fingerprints!r}")
        self.assertIsNotNone(next(iter(fingerprints)))


# ---------------------------------------------------------------------------
# Multi-disc
# ---------------------------------------------------------------------------


class TestMultiDiscFanout(unittest.TestCase):
    def test_multi_disc_per_disc_uses_warm_cache_across_discs(self):
        """2 discs, disc 1 finds match in user X, disc 2 in user Y. Cache populated
        by disc 1's wave is reused for disc 2 — no duplicate (user, dir) work."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(5)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        # Each user has one dir; results dict shape: {user: {ft: [dirs]}}
        results = _make_results(users)

        # Build a 2-disc release.
        release = MagicMock()
        media1 = MagicMock(medium_number=1)
        media2 = MagicMock(medium_number=2)
        release.media = [media1, media2]

        all_tracks = cast(
            "list[TrackRecord]",
            [
                {"albumId": 1, "title": "Disc1 Track 1", "mediumNumber": 1},
                {"albumId": 1, "title": "Disc2 Track 1", "mediumNumber": 2},
            ],
        )

        # Side-effect: simulate the fan-out populating folder_cache for the
        # work items it received. Track which (user, dir) pairs were actually
        # submitted across calls so the test can assert no duplicates.
        seen_work: list[tuple[str, str]] = []

        def fake_fanout(work, slskd, ctx, max_workers):
            for u, d in work:
                seen_work.append((u, d))
                ctx.folder_cache.setdefault(u, {})[d] = {"directory": d, "files": []}
            return set()

        # disc 1 matches user X (rank 2), disc 2 matches user Y (rank 4).
        disc1_winner = users[2]
        disc2_winner = users[4]

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            disc_no = tracks[0]["mediumNumber"]
            if disc_no == 1 and username == disc1_winner:
                return _match_for(disc1_winner, file_dirs[0])
            if disc_no == 2 and username == disc2_winner:
                return _match_for(disc2_winner, file_dirs[0])
            return _nomatch()

        with patch("lib.enqueue._fanout_browse_users", side_effect=fake_fanout), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]), \
             patch("lib.enqueue.cancel_and_delete"):
            attempt = try_multi_enqueue(
                release, all_tracks, results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched, f"expected match, got {attempt!r}")
        # No (user, dir) duplicate across the per-disc passes — the cache from
        # disc 1's wave eliminates re-browsing for disc 2.
        self.assertEqual(
            len(seen_work), len(set(seen_work)),
            f"duplicate (user, dir) work across disc waves: {seen_work}",
        )


class TestAlbumBrowseLogContract(unittest.TestCase):
    """Contract test for the per-album `album_browse:` instrumentation line.

    The line is the data source for #198 wave-cap / peer-ranking analysis;
    its field set and shape are part of the operational interface.
    """

    REQUIRED_FIELDS = (
        "artist=",
        "album=",
        "filetype=",
        "kind=",
        "matched=",
        "match_wave=",
        "eligible=",
        "peers=",
        "waves=",
    )

    def _capture_album_browse(self, log_records: list[str]) -> list[str]:
        return [r for r in log_records if "album_browse:" in r]

    def test_match_in_first_wave_logs_match_wave_zero(self):
        """Top-K match → match_wave=0, peers/waves are this album's deltas."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(30)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        winner = users[3]

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            if username == winner:
                return _match_for(winner, file_dirs[0])
            return _nomatch()

        with self.assertLogs("cratedigger", level="INFO") as log_ctx, \
             patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        lines = self._capture_album_browse(log_ctx.output)
        self.assertEqual(len(lines), 1, f"expected one album_browse line, got {lines!r}")
        line = lines[0]
        for field in self.REQUIRED_FIELDS:
            self.assertIn(field, line, f"missing {field!r} in album_browse: {line}")
        self.assertIn("matched=True", line)
        self.assertIn("match_wave=0", line)
        self.assertIn("kind=single", line)
        self.assertIn("eligible=30", line)

    def test_match_in_second_wave_logs_match_wave_one(self):
        """Lazy-tail match at rank 35, K=20 → match_wave=1."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(50)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        winner = users[35]

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            if username == winner:
                return _match_for(winner, file_dirs[0])
            return _nomatch()

        with self.assertLogs("cratedigger", level="INFO") as log_ctx, \
             patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=fake_match,
            )

        line = self._capture_album_browse(log_ctx.output)[0]
        self.assertIn("matched=True", line)
        self.assertIn("match_wave=1", line)

    def test_no_match_logs_match_wave_none(self):
        """All peers miss → matched=False, match_wave=None."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(30)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)

        with self.assertLogs("cratedigger", level="INFO") as log_ctx, \
             patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_always_nomatch,
            )

        line = self._capture_album_browse(log_ctx.output)[0]
        self.assertIn("matched=False", line)
        self.assertIn("match_wave=None", line)

    def test_multi_disc_logs_one_line_per_disc(self):
        """try_multi_enqueue emits one album_browse line per disc with kind=multi-disc<n>."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(5)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)

        release = MagicMock()
        release.media = [
            MagicMock(medium_number=1),
            MagicMock(medium_number=2),
        ]
        all_tracks = cast(
            "list[TrackRecord]",
            [
                {"albumId": 1, "title": "Disc1 Track 1", "mediumNumber": 1},
                {"albumId": 1, "title": "Disc2 Track 1", "mediumNumber": 2},
            ],
        )

        def fake_fanout(work, slskd, ctx, max_workers):
            for u, d in work:
                ctx.folder_cache.setdefault(u, {})[d] = {"directory": d, "files": []}
            return set()

        disc1_winner = users[2]
        disc2_winner = users[4]

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            disc_no = tracks[0]["mediumNumber"]
            if disc_no == 1 and username == disc1_winner:
                return _match_for(disc1_winner, file_dirs[0])
            if disc_no == 2 and username == disc2_winner:
                return _match_for(disc2_winner, file_dirs[0])
            return _nomatch()

        with self.assertLogs("cratedigger", level="INFO") as log_ctx, \
             patch("lib.enqueue._fanout_browse_users", side_effect=fake_fanout), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]), \
             patch("lib.enqueue.cancel_and_delete"):
            try_multi_enqueue(
                release, all_tracks, results, "flac", ctx, match_fn=fake_match,
            )

        lines = self._capture_album_browse(log_ctx.output)
        self.assertEqual(len(lines), 2, f"expected 2 disc lines, got {lines!r}")
        self.assertIn("kind=multi-disc1", lines[0])
        self.assertIn("kind=multi-disc2", lines[1])
        self.assertIn("matched=True", lines[0])
        self.assertIn("matched=True", lines[1])


class TestClaimDownloadingTOCTOU(unittest.TestCase):
    """#2: ``claim_downloading`` must reject a stale plan even when a
    regenerate lands between plan selection and the UPDATE. The fix is a
    single atomic UPDATE (``set_downloading_if_plan_current``) whose WHERE
    clause encodes the plan_id / ordinal / cycle constraints. We simulate
    the TOCTOU race by regenerating the plan after the executor has
    captured its PlanExecutionContext."""

    def _build_plan_execution(self, db: FakePipelineDB, request_id: int):
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID, PlanExecutionContext
        plan_id = db.create_successful_search_plan(
            request_id=request_id,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="A B",
                canonical_query_key="a b")],
        )
        active = db.get_active_search_plan(request_id)
        assert active is not None
        item = active.items[0]
        return PlanExecutionContext(
            plan_id=plan_id,
            plan_item_id=item.id,
            plan_ordinal=0,
            plan_strategy="default",
            plan_canonical_query_key=item.canonical_query_key,
            plan_repeat_group=None,
            plan_generator_id=SEARCH_PLAN_GENERATOR_ID,
            plan_item_count=1,
            cycle_count_snapshot=0,
        )

    def test_atomic_check_rejects_stale_claim_when_plan_moves_after_check(self):
        """TOCTOU: a regenerate lands AFTER the executor captured its
        PlanExecutionContext but BEFORE the claim UPDATE. The atomic
        ``set_downloading_if_plan_current`` must refuse the stale write —
        its WHERE clause re-validates plan currentness."""
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        plan_exec = self._build_plan_execution(db, 1)

        # Simulate the regenerate that landed mid-flight: cycle bump,
        # cursor reset to a new plan. plan_exec still points at the old
        # plan; the atomic UPDATE must reject the claim.
        db.supersede_search_plan_with_replacement(
            request_id=1, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="N",
                canonical_query_key="n")],
        )

        writer = DownloadOwnershipWriter(db_factory=lambda: db)
        ok = writer.claim_downloading(
            1, '{"state":"planned"}', plan_execution=plan_exec,
        )

        self.assertFalse(ok, "stale claim must be rejected by the atomic UPDATE")
        self.assertEqual(db.request(1)["status"], "wanted")


class TestCrossRequestEnqueueGuard(unittest.TestCase):
    """Composition pins for issue #1178: two requests for different
    pressings of the same album that both browse to the SAME peer
    directory must not both claim and enqueue the SAME (username,
    filename) queue keys. Drives the REAL try_enqueue / try_multi_enqueue
    seam (matching, the guard, claim, the write-ahead ledger) against one
    shared FakePipelineDB and one shared DownloadOwnershipWriter,
    mirroring #1178's real-world composition (17 shared keys from peer
    TheBun, 240ms apart) with a smaller representative key set. Each test
    that models "two candidates in one cycle" constructs one
    ``ClaimedQueueKeysRegistry`` and passes it to both contexts -- a
    cycle-scoped object, not a module global (issue #1178 PR2 review F7)."""

    @staticmethod
    def _shared_candidate(file_dir: str, filenames: list[str]):
        slskd = FakeSlskdAPI(downloads=[{
            "username": "TheBun",
            "directories": [{
                "directory": file_dir,
                "files": [
                    {"filename": name, "id": f"tid-{i}"}
                    for i, name in enumerate(filenames, start=1)
                ],
            }],
        }])
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [
                    {"filename": name.rsplit("\\", 1)[-1], "size": 111 * i}
                    for i, name in enumerate(filenames, start=1)
                ],
            },
            file_dir=file_dir,
            candidates=[],
        )
        return slskd, match

    def test_second_request_is_skipped_not_enqueued(self):
        """Request 1 claims and enqueues the shared keys first (through
        the real write-ahead + confirm ledger path); request 2's
        candidate — matched from the SAME peer directory a moment later,
        in the SAME process/cycle — must be skipped (not enqueue_failed)
        with NOTHING written to the ledger for request 2, and request 2's
        status/state left untouched. This serial call order lets request
        1's real accepted ledger row settle before request 2's guard
        check runs, so the cross-cycle DB layer is what actually catches
        it here (it is consulted first); the same-cycle registry's OWN
        atomic guarantee against a true in-flight race is proven directly
        at the unit level by TestClaimedQueueKeysRegistry below."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        file_dir = "Music\\TheBun\\Album"
        filenames = [f"{file_dir}\\0{i}.flac" for i in (1, 2, 3)]
        slskd, match = self._shared_candidate(file_dir, filenames)
        registry = ClaimedQueueKeysRegistry()
        ctx1 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2.current_album_cache[2] = _album_with_request(2)
        results = {"TheBun": {"flac": [file_dir]}}
        tracks1 = _make_tracks()
        tracks2 = _make_tracks(album_id=2)

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt1 = try_enqueue(
                tracks1, results, "flac", ctx1, match_fn=_const_match(match),
            )
            with self.assertLogs("cratedigger", level="INFO") as log_ctx:
                attempt2 = try_enqueue(
                    tracks2, results, "flac", ctx2, match_fn=_const_match(match),
                )

        self.assertTrue(attempt1.matched)
        self.assertFalse(attempt2.matched)
        self.assertFalse(attempt2.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "downloading")
        self.assertEqual(db.request(2)["status"], "wanted")
        self.assertIsNone(db.request(2)["active_download_state"])
        self.assertEqual(db.status_history, [(1, "downloading")])
        rows = db.record_transfer_enqueue_calls
        self.assertEqual({r.request_id for r in rows}, {1})
        self.assertEqual(len(rows), 3)
        self.assertTrue(
            any("#1178" in line and "request(s) [1]" in line
                for line in log_ctx.output),
            log_ctx.output,
        )
        # Issue #1196 item 2: the skipped attempt carries the forensics
        # marker naming its conflicting owner; the winner carries none.
        self.assertEqual(attempt2.conflicting_request_ids, frozenset({1}))
        self.assertEqual(attempt1.conflicting_request_ids, frozenset())

    def test_two_candidates_conflicting_with_two_owners_carry_both(self):
        """The union across candidates, driven through the real guard.

        Every other assertion on ``conflicting_request_ids`` in this file
        checks a single-conflict outcome, so degrading the union to an
        assignment — losing the first candidate's ids — survived the whole
        enqueue suite (issue #1313, mutant runner finding 2). Two peers,
        each already owned by a different request, and one attempt that
        browses both: the guard's own docstring says every id it skipped
        for during the WHOLE call is carried, and this is what proves it.
        """
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        for request_id in (1, 2, 3):
            db.seed_request(make_request_row(id=request_id, status="wanted"))
        dirs = {
            "TheBun": "Music\\TheBun\\Album",
            "OtherPeer": "Music\\OtherPeer\\Album",
        }
        candidates = {
            peer: self._shared_candidate(
                file_dir, [f"{file_dir}\\0{i}.flac" for i in (1, 2)])
            for peer, file_dir in dirs.items()
        }
        registry = ClaimedQueueKeysRegistry()

        def context(request_id: int, peer: str) -> CratediggerContext:
            ctx = _ctx_with_download_ownership(
                cfg=cfg, db=db, slskd=candidates[peer][0], registry=registry)
            ctx.current_album_cache[request_id] = _album_with_request(
                request_id)
            ctx.user_upload_speed.update({"TheBun": 10_000, "OtherPeer": 9_999})
            return ctx

        results = {peer: {"flac": [d]} for peer, d in dirs.items()}

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            # Each owner takes one peer's keys, through the real ledger.
            first = try_enqueue(
                _make_tracks(), {"TheBun": results["TheBun"]}, "flac",
                context(1, "TheBun"),
                match_fn=_const_match(candidates["TheBun"][1]),
            )
            second = try_enqueue(
                _make_tracks(album_id=2), {"OtherPeer": results["OtherPeer"]},
                "flac", context(2, "OtherPeer"),
                match_fn=_const_match(candidates["OtherPeer"][1]),
            )
            # Request 3 browses both and can have neither.
            third = try_enqueue(
                _make_tracks(album_id=3), results, "flac",
                context(3, "TheBun"),
                match_fn=lambda _t, _f, _d, peer, _c: candidates[peer][1],
            )

        self.assertTrue(first.matched)
        self.assertTrue(second.matched)
        self.assertFalse(third.matched)
        self.assertFalse(third.enqueue_failed)
        self.assertEqual(third.conflicting_request_ids, frozenset({1, 2}))
        self.assertEqual(db.request(3)["status"], "wanted")

    def test_persisted_state_fingerprint_agrees_with_real_ledger_rows_single_disc(self):
        """#1196 item 1 (review F1): agreement-by-construction, driven at
        the REAL adapter (try_enqueue), never asserted from a shared
        test-local variable. The ``attempt_fingerprint`` persisted onto
        ``active_download_state`` by the real claim must equal the
        ``attempt_fingerprint`` on EVERY real ledger row this same
        attempt writes. Kills a mutant that diverges the ledger-side
        computation (``lib.enqueue._enqueue_with_claim_outcome``) from
        the state-side computation
        (``lib.download.build_active_download_state``) -- e.g.
        appending a stray character to the ledger-side value."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        file_dir = "Music\\TheBun\\Album"
        filenames = [f"{file_dir}\\0{i}.flac" for i in (1, 2, 3)]
        slskd, match = self._shared_candidate(file_dir, filenames)
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        results = {"TheBun": {"flac": [file_dir]}}
        tracks = _make_tracks()

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt = try_enqueue(
                tracks, results, "flac", ctx, match_fn=_const_match(match),
            )

        self.assertTrue(attempt.matched)
        state = _request_active_state(db)
        self.assertIsNotNone(state.attempt_fingerprint)
        ledger_fps = {
            row.attempt_fingerprint
            for row in db.record_transfer_enqueue_calls
        }
        self.assertEqual(
            ledger_fps, {state.attempt_fingerprint},
            "the persisted state fingerprint must equal every real "
            "ledger row's fingerprint for this attempt (single-disc)",
        )

    def test_persisted_state_fingerprint_agrees_with_real_ledger_rows_multi_disc(self):
        """Same F1 composed pin through ``try_multi_enqueue`` -- the
        plausible divergence site the review named: each disc issues
        its OWN enqueue call (and so its OWN ledger-side
        ``attempt_fingerprint`` computation in
        ``_enqueue_with_claim_outcome``), while the state is claimed
        ONCE up front from the whole multi-disc manifest. Both must
        still agree."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI(downloads=[
            {
                "username": "u00",
                "directories": [{
                    "directory": "u00\\Music",
                    "files": [
                        {"filename": "u00\\Music\\d1.flac", "id": "tid-d1"},
                    ],
                }],
            },
            {
                "username": "u01",
                "directories": [{
                    "directory": "u01\\Music",
                    "files": [
                        {"filename": "u01\\Music\\d2.flac", "id": "tid-d2"},
                    ],
                }],
            },
        ])
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        users = ["u00", "u01"]
        results = _make_results(users)
        release = MagicMock()
        release.media = [MagicMock(medium_number=1), MagicMock(medium_number=2)]
        tracks = _multi_disc_tracks()

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            disc_no = tracks[0]["mediumNumber"]
            if disc_no == 1 and username == "u00":
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": "u00\\Music",
                        "files": [{"filename": "d1.flac", "size": 111}],
                    },
                    file_dir="u00\\Music",
                    candidates=[],
                )
            if disc_no == 2 and username == "u01":
                return MatchResult(
                    matched=True,
                    directory={
                        "directory": "u01\\Music",
                        "files": [{"filename": "d2.flac", "size": 222}],
                    },
                    file_dir="u01\\Music",
                    candidates=[],
                )
            return _nomatch()

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt = try_multi_enqueue(
                release, tracks, results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        state = _request_active_state(db)
        self.assertIsNotNone(state.attempt_fingerprint)
        ledger_fps = {
            row.attempt_fingerprint
            for row in db.record_transfer_enqueue_calls
        }
        self.assertEqual(
            ledger_fps, {state.attempt_fingerprint},
            "the persisted state fingerprint must equal every real "
            "per-disc ledger row's fingerprint for the SAME "
            "whole-attempt manifest (multi-disc)",
        )

    def test_multi_disc_candidate_is_skipped_not_enqueued(self):
        """Same pin, through the try_multi_enqueue call site (issue #1178
        PR2 wires the guard at BOTH seams -- try_enqueue and
        try_multi_enqueue each call _claim_initial_download_ownership
        independently). Unlike try_enqueue's per-user wave loop,
        try_multi_enqueue takes the FIRST match per disc
        (``next(_iter_wave_matches(...))``) rather than iterating
        candidates -- a guard hit here skips the WHOLE multi-disc
        candidate (the request keeps searching next cycle), not just one
        peer. This is accepted, documented behaviour (review F4), not a
        bug: multi-disc candidates are already scarce and per-peer
        fallback would multiply the disc-matching cost combinatorially."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        file_dir = "Music\\TheBun\\Album"
        filenames = [f"{file_dir}\\0{i}.flac" for i in (1, 2)]
        slskd, match = self._shared_candidate(file_dir, filenames)
        registry = ClaimedQueueKeysRegistry()
        ctx1 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2.current_album_cache[2] = _album_with_request(2)
        results = {"TheBun": {"flac": [file_dir]}}
        release = MagicMock()
        release.media = [MagicMock(medium_number=1)]

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt1 = try_multi_enqueue(
                release, _make_tracks(album_id=1), results, "flac", ctx1,
                match_fn=_const_match(match),
            )
            with self.assertLogs("cratedigger", level="WARNING") as log_ctx:
                attempt2 = try_multi_enqueue(
                    release, _make_tracks(album_id=2), results, "flac", ctx2,
                    match_fn=_const_match(match),
                )

        self.assertTrue(attempt1.matched)
        self.assertFalse(attempt2.matched)
        self.assertFalse(attempt2.enqueue_failed)
        self.assertEqual(db.request(2)["status"], "wanted")
        rows = db.record_transfer_enqueue_calls
        self.assertEqual({r.request_id for r in rows}, {1})
        self.assertTrue(
            any("MULTI-DISC CROSS-REQUEST CONFLICT" in line
                for line in log_ctx.output),
            log_ctx.output,
        )
        # Issue #1196 item 2: same marker through the multi-disc seam.
        self.assertEqual(attempt2.conflicting_request_ids, frozenset({1}))
        self.assertEqual(attempt1.conflicting_request_ids, frozenset())

    def test_conflicted_peer_falls_through_to_next_peer(self):
        """#1178 PR2 review F4: a guard hit means "try the next peer", not
        "give up". Two eligible peers match in the same wave (u00 ranked
        first by upload speed); u00's candidate conflicts with a key
        another request already holds this cycle, so try_enqueue must
        fall through and claim u01's (unconflicted) candidate instead of
        reporting matched=False."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        registry = ClaimedQueueKeysRegistry()
        # A DIFFERENT request (99) already holds u00's key this cycle --
        # no DB row needed, the same-cycle registry never consults one.
        registry.register_or_conflicting_owners(
            [("u00", "Music\\u00\\Album\\01.flac")], request_id=99)

        users = ["u00", "u01"]
        results = _make_results(users)

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            file_dir = f"Music\\{username}\\Album"
            return MatchResult(
                matched=True,
                directory={
                    "directory": file_dir,
                    "files": [{"filename": "01.flac", "size": 123}],
                },
                file_dir=file_dir,
                candidates=[],
            )

        slskd = FakeSlskdAPI(downloads=[{
            "username": "u01",
            "directories": [{
                "directory": "Music\\u01\\Album",
                "files": [
                    {"filename": "Music\\u01\\Album\\01.flac", "id": "tid-1"},
                ],
            }],
        }])
        ctx = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)

        with self.assertLogs("cratedigger", level="INFO") as log_ctx, \
             patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=fake_match,
            )

        self.assertTrue(attempt.matched)
        self.assertFalse(attempt.enqueue_failed)
        downloads = attempt.downloads or []
        self.assertTrue(downloads)
        self.assertEqual({d.username for d in downloads}, {"u01"})
        self.assertEqual(db.request(1)["status"], "downloading")
        self.assertTrue(
            any("#1178" in line and "request(s) [99]" in line
                for line in log_ctx.output),
            log_ctx.output,
        )
        # Issue #1196 item 2: even a WINNING attempt carries the marker
        # for a peer it conflicted on and fell through past -- the
        # forensics fact "a guard skip happened during this search" is
        # independent of whether the search ultimately matched.
        self.assertEqual(attempt.conflicting_request_ids, frozenset({99}))

    def test_claim_refused_releases_registry_for_sibling(self):
        """#1178 PR2 review F5: request 1's row has drifted to
        'unsearchable' since it was picked up as a candidate (e.g. an
        operator search-stop landed concurrently), so its guard-cleared
        claim (a wanted -> downloading CAS) is refused. Those
        already-registered keys must be released, or request 2's
        IDENTICAL candidate -- evaluated a moment later in the SAME cycle
        -- would be wrongly blocked by a claim that was never actually
        granted."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="unsearchable"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        file_dir = "Music\\TheBun\\Album"
        filenames = [f"{file_dir}\\01.flac"]
        slskd, match = self._shared_candidate(file_dir, filenames)
        registry = ClaimedQueueKeysRegistry()
        ctx1 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2.current_album_cache[2] = _album_with_request(2)
        results = {"TheBun": {"flac": [file_dir]}}

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt1 = try_enqueue(
                _make_tracks(), results, "flac", ctx1,
                match_fn=_const_match(match),
            )
            attempt2 = try_enqueue(
                _make_tracks(album_id=2), results, "flac", ctx2,
                match_fn=_const_match(match),
            )

        self.assertFalse(attempt1.matched)
        self.assertTrue(attempt1.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "unsearchable")
        self.assertTrue(attempt2.matched)
        self.assertFalse(attempt2.enqueue_failed)
        rows = db.record_transfer_enqueue_calls
        self.assertEqual({r.request_id for r in rows}, {2})

    def test_peer_offline_release_frees_sibling(self):
        """#1178 PR2 review F2, site: try_enqueue peer-offline. Request
        1's guard clears and registers the shared key, then the matched
        peer is found offline -- released. Request 2's IDENTICAL
        candidate a moment later must reach ITS OWN peer-offline check
        (not the cross-request guard), proving the registration was
        released rather than left blocking an innocent sibling."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        file_dir = "Music\\TheBun\\Album"
        filenames = [f"{file_dir}\\01.flac"]
        slskd, match = self._shared_candidate(file_dir, filenames)
        slskd.users.set_status("TheBun", "Offline")
        registry = ClaimedQueueKeysRegistry()
        ctx1 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2.current_album_cache[2] = _album_with_request(2)
        results = {"TheBun": {"flac": [file_dir]}}

        with self.assertLogs("cratedigger", level="INFO") as log_ctx, \
             patch("lib.enqueue._fanout_browse_users", return_value=set()):
            attempt1 = try_enqueue(
                _make_tracks(), results, "flac", ctx1,
                match_fn=_const_match(match),
            )
            attempt2 = try_enqueue(
                _make_tracks(album_id=2), results, "flac", ctx2,
                match_fn=_const_match(match),
            )

        self.assertFalse(attempt1.matched)
        self.assertFalse(attempt2.matched)
        peer_offline_lines = [
            line for line in log_ctx.output
            if "peer offline at enqueue: skipping TheBun" in line
        ]
        conflict_lines = [
            line for line in log_ctx.output
            if "cross-request enqueue conflict" in line
        ]
        # Both requests must independently reach the peer-offline check --
        # a leftover (unreleased) registration would instead route request
        # 2 into the cross-request-conflict branch and it would never
        # reach its own offline probe.
        self.assertEqual(len(peer_offline_lines), 2, log_ctx.output)
        self.assertEqual(conflict_lines, [], log_ctx.output)

    def test_verified_no_acceptance_release_frees_sibling(self):
        """#1178 PR2 review F2, site: try_enqueue verified_no_acceptance.
        Request 1 claims, then slskd rejects the enqueue and verification
        confirms nothing landed -- the claim resets to 'wanted' and the
        registry claim must be released too. Request 2's IDENTICAL
        candidate must reach its OWN claim+enqueue attempt -- proven by
        its own verified_no_acceptance download_log row (the mocked
        ``slskd_enqueue_with_outcome`` bypasses the real write-ahead
        ledger insert, so that signal isn't available here) -- not be
        blocked by request 1's leftover registration."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        registry = ClaimedQueueKeysRegistry()
        ctx1 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=FakeSlskdAPI(downloads=[]),
            registry=registry)
        ctx2 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=FakeSlskdAPI(downloads=[]),
            registry=registry)
        ctx2.current_album_cache[2] = _album_with_request(2)
        users = ["pooyork"]
        results = _make_results(users)
        file_dir = "musiclibrary\\Mercury Rev\\Deserter's Songs"
        match = MatchResult(
            matched=True,
            directory={
                "directory": file_dir,
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir=file_dir,
            candidates=[],
        )

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt1 = try_enqueue(
                _make_tracks(), results, "flac", ctx1,
                match_fn=_const_match(match),
            )
            try_enqueue(
                _make_tracks(album_id=2), results, "flac", ctx2,
                match_fn=_const_match(match),
            )

        self.assertFalse(attempt1.matched)
        self.assertEqual(db.request(1)["status"], "wanted")
        # request 2 must have reached its OWN claim+enqueue attempt --
        # proven by its own verified_no_acceptance download_log row (the
        # real write-ahead ledger insert lives inside the mocked
        # slskd_enqueue_with_outcome, so it never runs in this fixture) --
        # rather than being blocked by request 1's leftover registration.
        self.assertIn(2, {log.request_id for log in db.download_logs})

    def test_multi_disc_claim_refused_release_frees_sibling(self):
        """#1178 PR2 review F2, site: try_multi_enqueue claim-refused.
        Same shape as the try_enqueue pin above, through the
        try_multi_enqueue call site."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="unsearchable"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        file_dir = "Music\\TheBun\\Album"
        filenames = [f"{file_dir}\\01.flac"]
        slskd, match = self._shared_candidate(file_dir, filenames)
        registry = ClaimedQueueKeysRegistry()
        ctx1 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=slskd, registry=registry)
        ctx2.current_album_cache[2] = _album_with_request(2)
        results = {"TheBun": {"flac": [file_dir]}}
        release = MagicMock()
        release.media = [MagicMock(medium_number=1)]

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt1 = try_multi_enqueue(
                release, _make_tracks(album_id=1), results, "flac", ctx1,
                match_fn=_const_match(match),
            )
            attempt2 = try_multi_enqueue(
                release, _make_tracks(album_id=2), results, "flac", ctx2,
                match_fn=_const_match(match),
            )

        self.assertFalse(attempt1.matched)
        self.assertTrue(attempt1.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "unsearchable")
        self.assertTrue(attempt2.matched)
        self.assertFalse(attempt2.enqueue_failed)
        rows = db.record_transfer_enqueue_calls
        self.assertEqual({r.request_id for r in rows}, {2})

    def test_multi_disc_verified_no_acceptance_release_frees_sibling(self):
        """#1178 PR2 review F2, site: try_multi_enqueue
        verified_no_acceptance -- same trigger as the try_enqueue pin
        above, through the try_multi_enqueue call site, but proven via
        request 2's status_history transition instead of a download_log
        row (try_multi_enqueue writes no download_log on this outcome,
        unlike try_enqueue)."""
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        file_dir = "Music\\TheBun\\Album"
        filenames = [f"{file_dir}\\01.flac"]
        _slskd, match = self._shared_candidate(file_dir, filenames)
        registry = ClaimedQueueKeysRegistry()
        ctx1 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=FakeSlskdAPI(downloads=[]),
            registry=registry)
        ctx2 = _ctx_with_download_ownership(
            cfg=cfg, db=db, slskd=FakeSlskdAPI(downloads=[]),
            registry=registry)
        ctx2.current_album_cache[2] = _album_with_request(2)
        results = {"TheBun": {"flac": [file_dir]}}
        release = MagicMock()
        release.media = [MagicMock(medium_number=1)]

        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt1 = try_multi_enqueue(
                release, _make_tracks(album_id=1), results, "flac", ctx1,
                match_fn=_const_match(match),
            )
            try_multi_enqueue(
                release, _make_tracks(album_id=2), results, "flac", ctx2,
                match_fn=_const_match(match),
            )

        self.assertFalse(attempt1.matched)
        self.assertTrue(attempt1.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "wanted")
        # request 2 must have reached its OWN claim (transiently flipping
        # to 'downloading' before verified_no_acceptance resets it back)
        # -- try_multi_enqueue writes no download_log on this outcome
        # (unlike try_enqueue), so the status_history transition is the
        # only observable proof it wasn't blocked by request 1's leftover
        # registration.
        self.assertIn((2, "downloading"), db.status_history)


class TestCrossRequestEnqueueGuardCrossCycle(unittest.TestCase):
    """Cross-cycle pin: no same-cycle in-process claim exists (a fresh
    ``ClaimedQueueKeysRegistry``, simulating a new process/cycle -- each
    ``_run`` call builds its own via ``_ctx_with_download_ownership``'s
    default), but the transfer ledger already holds an accepted row for
    these queue keys from a PRIOR cycle. The owner's CURRENT status
    decides: 'downloading' blocks (the owner is still actively working
    the same files); 'replaced' (Replace-lineage attempt sharing, e.g.
    requests 8781/8846), 'wanted', and 'imported' (already moved on) must
    never block."""

    def _run(self, owner_status: str) -> bool:
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        username, filename = "TheBun", "Music\\TheBun\\Album\\01.flac"
        db.seed_request(make_request_row(id=99, status=owner_status))
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=99, username=username, filename=filename),
        ])
        db.confirm_transfer_enqueue(username, filename, request_id=99)
        slskd = FakeSlskdAPI(downloads=[{
            "username": username,
            "directories": [{
                "directory": "Music\\TheBun\\Album",
                "files": [{"filename": filename, "id": "tid-1"}],
            }],
        }])
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
        match = MatchResult(
            matched=True,
            directory={
                "directory": "Music\\TheBun\\Album",
                "files": [{"filename": "01.flac", "size": 123}],
            },
            file_dir="Music\\TheBun\\Album",
            candidates=[],
        )
        results = {username: {"flac": ["Music\\TheBun\\Album"]}}
        with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
             patch("time.sleep"):
            attempt = try_enqueue(
                _make_tracks(), results, "flac", ctx, match_fn=_const_match(match),
            )
        return attempt.matched

    def test_downloading_owner_blocks(self):
        self.assertFalse(self._run("downloading"))

    def test_replaced_wanted_imported_owners_do_not_block(self):
        for status in ("replaced", "wanted", "imported"):
            with self.subTest(status=status):
                self.assertTrue(self._run(status))


class TestClaimedQueueKeysRegistry(unittest.TestCase):
    """Unit-level pins for the same-cycle registry
    (``lib.enqueue.ClaimedQueueKeysRegistry``): same-request re-claims
    never self-block (poll-loop / multi-wave retries), the check-then-
    register step is atomic under concurrent same-key claims (issue #1178
    PR2 m3: TOCTOU), and ``release`` only removes keys still owned by the
    releasing request (review F5)."""

    def test_same_request_reclaiming_its_own_keys_never_conflicts(self):
        registry = ClaimedQueueKeysRegistry()
        keys = [("p0", "a.flac"), ("p0", "b.flac")]

        first = registry.register_or_conflicting_owners(keys, request_id=1)
        second = registry.register_or_conflicting_owners(keys, request_id=1)

        self.assertEqual(first, set())
        self.assertEqual(second, set())

    def test_different_request_conflicts_on_shared_key(self):
        registry = ClaimedQueueKeysRegistry()
        registry.register_or_conflicting_owners(
            [("p0", "a.flac")], request_id=1)

        conflicting = registry.register_or_conflicting_owners(
            [("p0", "a.flac"), ("p0", "b.flac")], request_id=2)

        self.assertEqual(conflicting, {1})
        # A conflicting attempt must not partially register its OTHER,
        # unconflicted keys either -- the whole key set is atomic.
        self.assertNotIn(("p0", "b.flac"), registry._keys)

    def test_release_removes_only_keys_still_owned_by_the_releaser(self):
        """#1178 PR2 review F5: release is a no-op for a key this request
        never actually won (e.g. it conflicted at registration time and
        another request legitimately owns it), and removes exactly the
        keys it DID win."""
        registry = ClaimedQueueKeysRegistry()
        registry.register_or_conflicting_owners(
            [("p0", "a.flac"), ("p0", "b.flac")], request_id=1)
        # Request 2 never actually won "a.flac" (it's owned by 1) -- its
        # release call must not touch request 1's real claim.
        registry.release([("p0", "a.flac")], request_id=2)
        self.assertEqual(
            registry.register_or_conflicting_owners(
                [("p0", "a.flac")], request_id=3),
            {1},
        )
        # Request 1 releasing its OWN keys actually frees them.
        registry.release([("p0", "a.flac"), ("p0", "b.flac")], request_id=1)
        self.assertEqual(
            registry.register_or_conflicting_owners(
                [("p0", "a.flac"), ("p0", "b.flac")], request_id=3),
            set(),
        )

    def test_concurrent_same_key_claims_never_both_win(self):
        """TOCTOU proof: N threads race to claim the SAME key set under
        DIFFERENT request ids, against ONE shared registry (modelling one
        cycle's ThreadPoolExecutor). Exactly one must win (empty conflict
        set); every other thread must observe a conflict naming the
        winner. A check-then-register split (rather than one atomic
        lock-held check-and-register) would let more than one thread
        observe "unclaimed" and both win. ``sys.setswitchinterval`` is
        lowered for the race window only: CPython's GIL rarely preempts
        mid-critical-section at its 5ms default, which made a genuinely
        split check-then-register mutant pass this test's own kill-matrix
        proof (issue #1178 PR2 kill matrix, m3) even though the window
        was real -- a shorter interval makes a real gap reproducible
        without weakening the assertion the correct (single-lock) code
        must satisfy regardless of scheduling."""
        registry = ClaimedQueueKeysRegistry()
        keys = [("p0", "race.flac")]
        n = 64
        results: list[set[int]] = [set() for _ in range(n)]
        barrier = threading.Barrier(n)

        def worker(i: int) -> None:
            barrier.wait()
            results[i] = registry.register_or_conflicting_owners(
                keys, request_id=i)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        old_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        finally:
            sys.setswitchinterval(old_interval)

        winners = [i for i, r in enumerate(results) if not r]
        self.assertEqual(
            len(winners), 1,
            f"expected exactly one winner, got {winners} (results={results})",
        )
        winner = winners[0]
        for i, r in enumerate(results):
            if i == winner:
                continue
            self.assertEqual(r, {winner}, f"thread {i} result={r}")


if __name__ == "__main__":
    unittest.main()
