#!/usr/bin/env python3
"""Tests for scripts/migrate_db.py CLI wiring."""

from __future__ import annotations

import io
import os
import sys
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts import migrate_db


class TestDefaultDsnFailsLoud(unittest.TestCase):
    """#479 item 2: no hardcoded fallback — fail loud instead."""

    @patch.object(migrate_db, "DEFAULT_DSN", None)
    def test_main_fails_loud_when_dsn_is_not_configured(self) -> None:
        with patch.object(sys, "argv", ["migrate_db.py"]):
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as cm:
                    migrate_db.main()

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("PIPELINE_DB_DSN", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
