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
from typing import cast
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
from lib.download import SlskdEnqueueOutcome
from lib.download_ownership import DownloadOwnershipWriter
from lib.grab_list import DownloadFile
from lib.matching import MatchResult
from tests.fakes import FakePipelineDB, FakeSlskdAPI
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
    source = MagicMock()
    db = MagicMock()
    db.get_denylisted_users.return_value = [
        {"username": u} for u in (denied_users or [])
    ]
    source._get_db.return_value = db
    ctx = CratediggerContext(
        cfg=cfg,
        slskd=MagicMock(),
        pipeline_db_source=source,
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
        directory={"directory": file_dir, "files": []},
        file_dir=file_dir,
        candidates=[],
    )


def _nomatch() -> MatchResult:
    return MatchResult(matched=False, directory={}, file_dir="", candidates=[])


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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", return_value=_nomatch()), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

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

        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan, \
             patch("lib.enqueue.check_for_match") as m_match:
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertFalse(attempt.matched)
        m_fan.assert_not_called()
        m_match.assert_not_called()

    def test_fewer_than_k_eligible_runs_single_short_wave(self):
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(10)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)

        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan, \
             patch("lib.enqueue.check_for_match", return_value=_nomatch()):
            try_enqueue(_make_tracks(), results, "flac", ctx)

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

        with patch("lib.enqueue._fanout_browse_users", return_value=set()) as m_fan, \
             patch("lib.enqueue.check_for_match", return_value=_nomatch()):
            try_enqueue(_make_tracks(), results, "flac", ctx)

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

        with patch("lib.enqueue._fanout_browse_users", side_effect=fake_fanout), \
             patch("lib.enqueue.check_for_match", return_value=_nomatch()):
            try_enqueue(_make_tracks(), results, "flac", ctx)

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

        with patch("lib.enqueue._fanout_browse_users", return_value=browse_result), \
             patch("lib.enqueue.check_for_match", side_effect=fake_match):
            try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertEqual(ctx.peers_browsed, 0)
        self.assertEqual(ctx.peer_cache_negative_skips, {(user, file_dir)})


# ---------------------------------------------------------------------------
# Per-cycle scope of broken_user
# ---------------------------------------------------------------------------


class TestBrokenUserPerCycle(unittest.TestCase):
    def test_broken_user_is_per_cycle_not_persistent(self):
        """A fresh CratediggerContext starts with empty broken_user."""
        cfg = _make_cfg()
        ctx = CratediggerContext(cfg=cfg, slskd=MagicMock(), pipeline_db_source=MagicMock())
        self.assertEqual(ctx.broken_user, [])


class TestFindDownloadWorkerContext(unittest.TestCase):
    def test_worker_context_snapshots_inputs_and_prefetches_db_data(self):
        cfg = _make_cfg()
        source = MagicMock()
        db = MagicMock()
        db.get_denylisted_users.return_value = [{"username": "blocked"}]
        source._get_db.return_value = db
        source.get_tracks.return_value = [
            {"albumId": 1, "title": "Track 1", "mediumNumber": 1},
        ]
        ctx = CratediggerContext(
            cfg=cfg,
            slskd=MagicMock(),
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

        source.get_tracks.reset_mock()
        self.assertEqual(get_album_tracks(album, worker_ctx), [
            {"albumId": 1, "title": "Track 1", "mediumNumber": 1},
        ])
        source.get_tracks.assert_not_called()
        with self.assertRaises(AssertionError):
            worker_ctx.pipeline_db_source._get_db()

    def test_worker_db_sentinel_is_not_swallowed_by_denylist_lookup(self):
        from lib.enqueue import _get_denied_users

        ctx = CratediggerContext(
            cfg=_make_cfg(),
            slskd=MagicMock(),
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
             patch(
                 "lib.enqueue.check_for_match",
                 side_effect=lambda tracks, ft, dirs, u, ctx: match_returns[u],
             ), \
             patch("lib.enqueue.slskd_do_enqueue", side_effect=fake_enqueue):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch(
                 "lib.enqueue.check_for_match",
                 side_effect=lambda tracks, ft, dirs, u, ctx: match_returns[u],
             ), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=None):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)


class TestDownloadOwnershipPreclaim(unittest.TestCase):
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

        def fake_enqueue(*, username, files, file_dir, ctx):
            row = db.request(1)
            self.assertEqual(row["status"], "downloading")
            state = json.loads(row["active_download_state"])
            self.assertEqual(state["filetype"], "flac")
            self.assertEqual(state["files"][0]["username"], "u00")
            self.assertEqual(
                state["files"][0]["filename"],
                "Music\\u00\\Album\\01.flac",
            )
            self.assertIn("current_path", state)
            self.assertIsNone(state["current_path"])
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
             patch("lib.enqueue.check_for_match", return_value=match), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertTrue(attempt.matched)
        self.assertEqual(db.status_history, [(1, "downloading")])
        self.assertEqual(db.request(1)["status"], "downloading")

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
             patch("lib.enqueue.check_for_match", return_value=match), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", return_value=match), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertFalse(attempt.matched)
        self.assertTrue(attempt.enqueue_failed)
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])
        self.assertEqual(db.status_history, [(1, "downloading"), (1, "wanted")])
        self.assertEqual(db.recorded_attempts, [(1, "download")])
        self.assertIsNotNone(db.request(1)["next_retry_after"])

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
             patch("lib.enqueue.check_for_match", return_value=match), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", return_value=match), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", return_value=match), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="unknown"),
             ):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_multi_enqueue(release, tracks, results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_multi_enqueue(release, tracks, results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch(
                 "lib.enqueue.slskd_enqueue_with_outcome",
                 return_value=SlskdEnqueueOutcome(status="rejected"),
             ):
            attempt = try_multi_enqueue(release, tracks, results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_multi_enqueue(release, tracks, results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_enqueue_with_outcome", side_effect=fake_enqueue):
            attempt = try_multi_enqueue(release, tracks, results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]), \
             patch("lib.enqueue.cancel_and_delete"):
            attempt = try_multi_enqueue(release, all_tracks, results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", return_value=_nomatch()), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            try_enqueue(_make_tracks(), results, "flac", ctx)

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
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]), \
             patch("lib.enqueue.cancel_and_delete"):
            try_multi_enqueue(release, all_tracks, results, "flac", ctx)

        lines = self._capture_album_browse(log_ctx.output)
        self.assertEqual(len(lines), 2, f"expected 2 disc lines, got {lines!r}")
        self.assertIn("kind=multi-disc1", lines[0])
        self.assertIn("kind=multi-disc2", lines[1])
        self.assertIn("matched=True", lines[0])
        self.assertIn("matched=True", lines[1])


if __name__ == "__main__":
    unittest.main()
