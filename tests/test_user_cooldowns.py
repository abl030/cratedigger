"""Orchestration pins for the registered per-cycle cooldown loader."""
from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from lib.user_cooldowns import load_user_cooldowns
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db


class TestLoadUserCooldowns(unittest.TestCase):
    def test_loads_active_cooldowns_onto_the_context(self):
        db = FakePipelineDB()
        db.add_cooldown(
            "slowpeer",
            cooldown_until=datetime.now(UTC) + timedelta(hours=1),
            reason="timeout")

        ctx = make_ctx_with_fake_db(db)
        loaded = load_user_cooldowns(ctx)

        self.assertEqual(loaded, {"slowpeer"})
        self.assertEqual(ctx.cooled_down_users, {"slowpeer"})

    def test_updates_the_context_set_in_place(self):
        # build_phase1_context forwards ctx.cooled_down_users by reference,
        # so the loader must mutate the SAME object (clear + update): every
        # alias, whenever captured, sees the loaded roster and never a
        # stale ghost. Replacing the object would strand pre-load aliases.
        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)
        alias = ctx.cooled_down_users
        alias.add("ghost")

        loaded = load_user_cooldowns(ctx)

        self.assertEqual(loaded, set())
        self.assertEqual(ctx.cooled_down_users, set())
        self.assertIs(ctx.cooled_down_users, alias)
        self.assertEqual(alias, set())

    def test_expired_cooldowns_are_not_loaded(self):
        db = FakePipelineDB()
        db.add_cooldown(
            "recovered",
            cooldown_until=datetime.now(UTC) - timedelta(minutes=1),
            reason="timeout")

        ctx = make_ctx_with_fake_db(db)

        self.assertEqual(load_user_cooldowns(ctx), set())


if __name__ == "__main__":
    unittest.main()
