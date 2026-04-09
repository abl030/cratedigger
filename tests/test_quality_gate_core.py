"""Tests for _check_quality_gate_core — quality gate with plain params.

Verifies that the extracted core function works identically to
_check_quality_gate but takes plain params + PipelineDB directly.
"""

import unittest
from unittest.mock import MagicMock, patch

from lib.quality import QUALITY_UPGRADE_TIERS, QUALITY_LOSSLESS
from tests.helpers import make_request_row


class TestCheckQualityGateCore(unittest.TestCase):
    """_check_quality_gate_core must match _check_quality_gate behavior."""

    def _run(self, gate_decision, files=None, **req_overrides):
        from lib.import_dispatch import _check_quality_gate_core
        db = MagicMock()
        merged = {"current_spectral_bitrate": None, "verified_lossless": False}
        merged.update(req_overrides)
        db.get_request.return_value = make_request_row(**merged)
        if files is None:
            files = [MagicMock(username="user1", filename="01 - Track.mp3")]

        with patch("lib.beets_db.BeetsDB") as mock_beets_cls, \
             patch("lib.quality.quality_gate_decision",
                   return_value=gate_decision):
            mock_beets = MagicMock()
            mock_beets.__enter__ = MagicMock(return_value=mock_beets)
            mock_beets.__exit__ = MagicMock(return_value=False)
            mock_beets.get_album_info.return_value = MagicMock(
                min_bitrate_kbps=192, is_cbr=True)
            mock_beets_cls.return_value = mock_beets
            _check_quality_gate_core(
                mb_id="test-mbid",
                label="Test Artist - Test Album",
                request_id=42,
                files=files,
                db=db,
            )
        return db

    def test_requeue_upgrade(self):
        db = self._run("requeue_upgrade")
        call_args = db.reset_to_wanted.call_args
        self.assertEqual(
            call_args.kwargs.get("search_filetype_override") or call_args[1].get("search_filetype_override"),
            QUALITY_UPGRADE_TIERS,
        )

    def test_requeue_upgrade_verified_lossless_accepts(self):
        db = self._run("requeue_upgrade", verified_lossless=True)
        db.reset_to_wanted.assert_not_called()
        db.add_denylist.assert_not_called()

    def test_requeue_lossless(self):
        db = self._run("requeue_lossless")
        call_args = db.reset_to_wanted.call_args
        self.assertEqual(
            call_args.kwargs.get("search_filetype_override") or call_args[1].get("search_filetype_override"),
            QUALITY_LOSSLESS,
        )

    def test_accept_clears_search_override(self):
        db = self._run("accept")
        call_args = db.update_status.call_args
        kwargs = call_args.kwargs if call_args.kwargs else call_args[1]
        self.assertIn("search_filetype_override", kwargs)
        self.assertIsNone(kwargs["search_filetype_override"])

    def test_no_mb_id_returns_early(self):
        """Empty mb_id should return without doing anything."""
        from lib.import_dispatch import _check_quality_gate_core
        db = MagicMock()
        _check_quality_gate_core(
            mb_id="", label="Test", request_id=42, files=[], db=db)
        db.get_request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
