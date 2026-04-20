"""Tests for lib/context.py — CratediggerContext dataclass."""

import unittest
from unittest.mock import MagicMock


class TestCratediggerContext(unittest.TestCase):
    """Test CratediggerContext construction and cache isolation."""

    def test_context_construction(self):
        from lib.context import CratediggerContext
        mock_cfg = MagicMock()
        mock_slskd = MagicMock()
        mock_db_source = MagicMock()

        ctx = CratediggerContext(
            cfg=mock_cfg,
            slskd=mock_slskd,
            pipeline_db_source=mock_db_source,
        )

        self.assertIs(ctx.cfg, mock_cfg)
        self.assertIs(ctx.slskd, mock_slskd)
        self.assertIs(ctx.pipeline_db_source, mock_db_source)
        self.assertIsInstance(ctx.search_cache, dict)
        self.assertIsInstance(ctx.folder_cache, dict)
        self.assertIsInstance(ctx.user_upload_speed, dict)
        self.assertIsInstance(ctx.broken_user, list)
        self.assertEqual(len(ctx.search_cache), 0)
        self.assertEqual(len(ctx.broken_user), 0)

    def test_context_cache_isolation(self):
        from lib.context import CratediggerContext
        mock_cfg = MagicMock()
        mock_slskd = MagicMock()
        mock_db_source = MagicMock()

        ctx1 = CratediggerContext(
            cfg=mock_cfg,
            slskd=mock_slskd,
            pipeline_db_source=mock_db_source,
        )
        ctx2 = CratediggerContext(
            cfg=mock_cfg,
            slskd=mock_slskd,
            pipeline_db_source=mock_db_source,
        )

        # Mutating one context's caches should not affect the other
        ctx1.search_cache[42] = {"user1": {}}
        ctx1.broken_user.append("bad_user")
        ctx1.user_upload_speed["user1"] = 50000

        self.assertEqual(len(ctx2.search_cache), 0)
        self.assertEqual(len(ctx2.broken_user), 0)
        self.assertEqual(len(ctx2.user_upload_speed), 0)


if __name__ == "__main__":
    unittest.main()
