"""Contract tests for cfg.search_max_inflight (issue #198 U4).

The hard-coded MAX_INFLIGHT=2 in `_search_and_queue_parallel` is replaced
with `cfg.search_max_inflight` (default 4). These tests pin:

  * The configured value is used to size the ThreadPoolExecutor and the
    initial seed loop — verified via the "Pipelined search: N albums,
    K in flight" log line.
  * Default (4) and a non-default override (1) both round-trip correctly.
"""

from __future__ import annotations

import configparser
import logging
import unittest
from dataclasses import replace
from unittest.mock import MagicMock

import cratedigger
from lib.config import CratediggerConfig


def _empty_cfg(**overrides) -> CratediggerConfig:
    """A real CratediggerConfig built from an empty INI (all defaults), then
    optionally overridden via dataclasses.replace."""
    cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


class TestSearchMaxInflightPipelineLog(unittest.TestCase):
    """The pipeline log line must report the configured value, not the
    legacy hard-coded 2."""

    def setUp(self) -> None:
        self._orig_cfg = cratedigger.cfg
        self._orig_slskd = cratedigger.slskd

    def tearDown(self) -> None:
        cratedigger.cfg = self._orig_cfg
        cratedigger.slskd = self._orig_slskd

    def _run_with(self, cfg: CratediggerConfig) -> str:
        """Run _search_and_queue_parallel with an empty album list, return
        the captured "Pipelined search" log line."""
        cratedigger.cfg = cfg
        cratedigger.slskd = MagicMock()
        ctx = MagicMock()
        ctx.cfg = cfg
        with self.assertLogs("cratedigger", level=logging.INFO) as captured:
            cratedigger._search_and_queue_parallel([], ctx)
        for record in captured.records:
            if "Pipelined search" in record.message:
                return record.message
        self.fail("Expected 'Pipelined search' log line not emitted")

    def test_default_search_max_inflight_is_four(self):
        cfg = _empty_cfg()
        self.assertEqual(
            cfg.search_max_inflight, 4,
            "default raised from legacy 2 to 4 (issue #198 U4)",
        )
        line = self._run_with(cfg)
        self.assertIn("4 in flight", line)

    def test_configured_value_is_used(self):
        cfg = _empty_cfg(search_max_inflight=1)
        line = self._run_with(cfg)
        self.assertIn("1 in flight", line)


if __name__ == "__main__":
    unittest.main()
