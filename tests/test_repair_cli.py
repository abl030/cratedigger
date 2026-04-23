"""Tests for scripts/repair.py CLI wiring."""

from __future__ import annotations

import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.quality import OrphanInfo
from scripts import repair


class TestCmdFix(unittest.TestCase):
    @patch("scripts.repair._transition_request")
    @patch("scripts.repair._collect_issues")
    def test_reset_to_wanted_routes_through_shared_finalizer(
        self,
        mock_collect_issues,
        mock_transition,
    ) -> None:
        db = MagicMock()
        mock_collect_issues.return_value = [
            OrphanInfo(
                request_id=17,
                issue_type="orphaned_download",
                detail="transfers gone",
            )
        ]

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            repair.cmd_fix(db)

        mock_transition.assert_called_once_with(
            db,
            17,
            "wanted",
            from_status="downloading",
            message="Repair reset orphaned download to wanted",
        )
        self.assertIn("Reset to wanted", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
