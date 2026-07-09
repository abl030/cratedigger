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
import unittest
from dataclasses import replace
from typing import Any, cast
from unittest.mock import MagicMock, patch

from cratedigger import TrackRecord
from lib.browse import BrowseManyResult
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.enqueue import (
    _WorkerPipelineDBSource,
    get_album_tracks,
    prepare_find_download_context,
    try_enqueue,
    try_multi_enqueue,
)
from lib.slskd_transfers import SlskdEnqueueOutcome
from lib.download_ownership import DownloadOwnershipWriter
from lib.grab_list import DownloadFile
from lib.matching import MatchResult
from tests.fakes import (
    DenylistEntry,
    FakePipelineDB,
    FakePipelineDBSource,
    FakeSlskdAPI,
)
from tests.helpers import make_request_row


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
        cfg=cfg,
        slskd=FakeSlskdAPI(),
        pipeline_db_source=FakePipelineDBSource(db),
        user_upload_speed=user_upload_speed or {},
        cooled_down_users=cooled_down_users or set(),
    )
    ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
    return ctx


def _make_results(users: list[str]) -> dict[str, dict[str, list[str]]]:
    """Build a search-results dict where each user has one flac dir."""
    return {u: {"flac": [f"Music\\{u}\\Album"]} for u in users}


def _make_tracks() -> list[TrackRecord]:
    return cast(
        "list[TrackRecord]",
        [{"albumId": 1, "title": "Track 1", "mediumNumber": 1}],
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
) -> CratediggerContext:
    ctx = _make_ctx(cfg, user_upload_speed={"u00": 10_000, "u01": 9_999})
    ctx.slskd = slskd if slskd is not None else FakeSlskdAPI()
    ctx.current_album_cache[1] = _album_with_request(1)
    ctx.download_ownership = DownloadOwnershipWriter(db_factory=lambda: db)
    return ctx


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

        self.assertGreater(ctx.browse_time_s, 0.0)
        self.assertEqual(ctx.fanout_waves, 1)
        self.assertEqual(ctx.peers_browsed, 2)

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

        self.assertEqual(ctx.peers_browsed, 0)
        self.assertEqual(ctx.peer_cache_negative_skips, {(user, file_dir)})


# ---------------------------------------------------------------------------
# Per-cycle scope of broken_user
# ---------------------------------------------------------------------------


class TestBrokenUserPerCycle(unittest.TestCase):
    def test_broken_user_is_per_cycle_not_persistent(self):
        """A fresh CratediggerContext starts with empty broken_user."""
        cfg = _make_cfg()
        ctx = CratediggerContext(
            cfg=cfg,
            slskd=FakeSlskdAPI(),
            pipeline_db_source=FakePipelineDBSource(),
        )
        self.assertEqual(ctx.broken_user, set())


class _PrefetchSource(FakePipelineDBSource):
    """FakePipelineDBSource variant that returns a configurable stub list
    from ``get_tracks`` so the prefetch-contract test can assert on the
    exact rows the source delivered without the production negative-ID
    transform interfering."""

    def __init__(self, stub_tracks: list[dict[str, object]]) -> None:
        super().__init__()
        self._stub_tracks = list(stub_tracks)

    def get_tracks(self, album_record: object) -> list[dict[str, object]]:  # type: ignore[override]
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
            cfg=cfg,
            slskd=FakeSlskdAPI(),
            pipeline_db_source=source,
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
            cfg=_make_cfg(),
            slskd=FakeSlskdAPI(),
            pipeline_db_source=_WorkerPipelineDBSource(),  # type: ignore[arg-type]
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

        def fake_enqueue(*, username, files, file_dir, ctx):
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


class TestDownloadOwnershipPreclaim(unittest.TestCase):
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

        def fake_enqueue(*, username, files, file_dir, ctx):
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
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
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

        def fake_enqueue(*, username, files, file_dir, ctx):
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

    def test_multi_disc_partial_failure_resets_after_verified_cancel(self):
        cfg = _make_cfg(browse_top_k=20)
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        slskd = FakeSlskdAPI()
        ctx = _ctx_with_download_ownership(cfg=cfg, db=db, slskd=slskd)
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

        def fake_enqueue(*, username, files, file_dir, ctx):
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 2:
                return SlskdEnqueueOutcome(status="rejected")
            slskd.add_transfer(
                username=username,
                directory=file_dir,
                filename=files[0]["filename"],
                id="transfer-1",
            )
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

        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertEqual(db.status_history, [(1, "downloading"), (1, "wanted")])
        self.assertEqual(
            [(call.username, call.id) for call in slskd.transfers.cancel_download_calls],
            [("u00", "transfer-1")],
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

        def fake_enqueue(*, username, files, file_dir, ctx):
            nonlocal enqueue_calls
            enqueue_calls += 1
            if enqueue_calls == 2:
                return SlskdEnqueueOutcome(status="rejected")
            slskd.add_transfer(
                username=username,
                directory=file_dir,
                filename=files[0]["filename"],
                id="transfer-1",
            )
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

        def fake_enqueue(*, username, files, file_dir, ctx):
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


if __name__ == "__main__":
    unittest.main()
