"""Wave-based fan-out tests for try_enqueue / try_multi_enqueue (issue #198 U3).

The refactor replaces the sequential per-user iteration in `try_enqueue` and
`try_multi_enqueue` with: (1) chunk eligible users into waves of
`cfg.browse_top_k`, (2) parallel browse via `_fanout_browse_users`, (3) match
in upload-speed order against the now-warm folder cache, (4) exit on first
successful enqueue. A per-cycle browse budget short-circuits remaining work.

These tests pin:
  * top-K hit → only first wave fans out
  * lazy-tail hit → second wave fans out, third never
  * all-miss → every eligible user fans out, matched=False
  * 0 eligible (cooldown/denylist) → no fan-out
  * fewer than K eligible → single short wave
  * cached entries skipped from the work list
  * cycle budget short-circuit between albums and between waves
  * wave deadline trips → users land in broken_user
  * match-rate regression (high-rank user wins when low-rank users timed out)
  * per-cycle scope of broken_user
  * had_enqueue_failure tracking when enqueue raises
  * try_multi_enqueue: per-disc wave loop reuses populated cache
"""

from __future__ import annotations

import configparser
import unittest
from typing import cast
from unittest.mock import MagicMock, patch

from cratedigger import TrackRecord
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.enqueue import try_enqueue, try_multi_enqueue
from lib.matching import MatchResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_cfg(
    *,
    browse_top_k: int = 20,
    browse_wave_deadline_s: float = 20.0,
    browse_global_max_workers: int = 32,
    browse_cycle_budget_s: float = 240.0,
) -> CratediggerConfig:
    """Build a CratediggerConfig with the four U2 fan-out knobs configurable."""
    ini = configparser.ConfigParser()
    ini["Search Settings"] = {
        "minimum_filename_match_ratio": "0.5",
        "ignored_users": "",
        "allowed_filetypes": "flac,mp3",
        "browse_parallelism": "4",
        "browse_top_k": str(browse_top_k),
        "browse_wave_deadline_s": str(browse_wave_deadline_s),
        "browse_global_max_workers": str(browse_global_max_workers),
        "browse_cycle_budget_s": str(browse_cycle_budget_s),
    }
    return CratediggerConfig.from_ini(ini)


def _make_ctx(
    cfg: CratediggerConfig,
    *,
    user_upload_speed: dict[str, int] | None = None,
    cooled_down_users: set[str] | None = None,
    denied_users: list[str] | None = None,
    cycle_browse_time_s: float = 0.0,
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
        cycle_browse_time_s=cycle_browse_time_s,
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


# ---------------------------------------------------------------------------
# Cycle budget short-circuit
# ---------------------------------------------------------------------------


class TestCycleBudget(unittest.TestCase):
    def test_budget_exhausted_inter_album_skips_fanout(self):
        """If ctx.cycle_browse_time_s already > budget at entry, no fan-out."""
        cfg = _make_cfg(browse_cycle_budget_s=1.0)
        users = _ranked_users(5)
        ctx = _make_ctx(
            cfg,
            user_upload_speed=_upload_speeds(users),
            cycle_browse_time_s=2.0,  # already over budget
        )
        results = _make_results(users)

        with patch("lib.enqueue._fanout_browse_users") as m_fan, \
             patch("lib.enqueue.check_for_match") as m_match:
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertFalse(attempt.matched)
        self.assertFalse(attempt.enqueue_failed)
        m_fan.assert_not_called()
        m_match.assert_not_called()

    def test_budget_exhausted_inter_wave_stops_subsequent_waves(self):
        """First wave inflates cycle_browse_time_s above budget → no second wave."""
        cfg = _make_cfg(browse_top_k=20, browse_cycle_budget_s=1.0)
        users = _ranked_users(40)  # would normally take 2 waves
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)

        # Side effect: bump cycle_browse_time_s past budget on the first call.
        def bump_budget(*args, **kwargs):
            ctx.cycle_browse_time_s += 1.5
            return set()

        with patch("lib.enqueue._fanout_browse_users", side_effect=bump_budget) as m_fan, \
             patch("lib.enqueue.check_for_match", return_value=_nomatch()):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertFalse(attempt.matched)
        self.assertEqual(m_fan.call_count, 1, "second wave must be short-circuited")


# ---------------------------------------------------------------------------
# Wave deadline → broken_user updates
# ---------------------------------------------------------------------------


class TestWaveDeadline(unittest.TestCase):
    def test_timed_out_users_added_to_broken_user(self):
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(5)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)

        # First two users time out.
        slow_users = {users[0], users[1]}

        def fake_fanout(work, slskd, ctx, max_workers, deadline_s):
            return slow_users

        with patch("lib.enqueue._fanout_browse_users", side_effect=fake_fanout), \
             patch("lib.enqueue.check_for_match", return_value=_nomatch()) as m_match:
            try_enqueue(_make_tracks(), results, "flac", ctx)

        for u in slow_users:
            self.assertIn(u, ctx.broken_user)
        # Match was not invoked for the timed-out users (they were skipped).
        match_users = {call.args[3] for call in m_match.call_args_list}
        self.assertTrue(slow_users.isdisjoint(match_users))

    def test_broken_users_excluded_from_subsequent_wave_work_list(self):
        """Wave-1 timeouts must NOT be re-submitted in wave-2's work plan.

        Regression for the #198 code-review finding: the match loop already
        skips broken users, but the work-list builder didn't — paying another
        browse_wave_deadline_s to re-confirm dead peers each wave. With a
        2-wave album where wave-1 times out users, wave-2's submitted work
        must contain ZERO of those usernames.
        """
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(40)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        wave1_users = set(users[:20])

        call_history: list[set[str]] = []

        def fake_fanout(work, slskd, ctx, max_workers, deadline_s):
            wave_users = {u for (u, _d) in work}
            call_history.append(wave_users)
            # Wave 1: every user times out. Wave 2: nobody times out.
            if wave_users <= wave1_users:
                return wave_users
            return set()

        with patch("lib.enqueue._fanout_browse_users", side_effect=fake_fanout), \
             patch("lib.enqueue.check_for_match", return_value=_nomatch()):
            try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertEqual(len(call_history), 2, "expected 2 waves")
        wave2_users = call_history[1]
        self.assertTrue(
            wave2_users.isdisjoint(wave1_users),
            f"wave-2 must not re-submit wave-1's timed-out users; "
            f"overlap was {wave2_users & wave1_users}",
        )
        # And wave-2 should contain the actual rank 20-39 users.
        self.assertEqual(wave2_users, set(users[20:40]))

    def test_match_rate_regression_high_rank_user_wins(self):
        """Wave-1 users all time out, wave-2 user X is the only true match."""
        cfg = _make_cfg(browse_top_k=20)
        users = _ranked_users(40)
        ctx = _make_ctx(cfg, user_upload_speed=_upload_speeds(users))
        results = _make_results(users)
        winner = users[25]

        # Wave 1 (top-20): everyone times out.
        # Wave 2 (next 20): no timeouts.
        def fake_fanout(work, slskd, ctx, max_workers, deadline_s):
            wave_users = {u for (u, _d) in work}
            if wave_users <= set(users[:20]):
                return wave_users
            return set()

        def fake_match(tracks, allowed_filetype, file_dirs, username, ctx):
            if username == winner:
                return _match_for(winner, f"Music\\{winner}\\Album")
            return _nomatch()

        with patch("lib.enqueue._fanout_browse_users", side_effect=fake_fanout), \
             patch("lib.enqueue.check_for_match", side_effect=fake_match), \
             patch("lib.enqueue.slskd_do_enqueue", return_value=[MagicMock()]):
            attempt = try_enqueue(_make_tracks(), results, "flac", ctx)

        self.assertTrue(attempt.matched)
        for u in users[:20]:
            self.assertIn(u, ctx.broken_user, f"wave-1 user {u} should be broken")

    def test_broken_user_is_per_cycle_not_persistent(self):
        """A fresh CratediggerContext starts with empty broken_user."""
        cfg = _make_cfg()
        ctx = CratediggerContext(cfg=cfg, slskd=MagicMock(), pipeline_db_source=MagicMock())
        self.assertEqual(ctx.broken_user, [])


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

        def fake_fanout(work, slskd, ctx, max_workers, deadline_s):
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


if __name__ == "__main__":
    unittest.main()
