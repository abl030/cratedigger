"""Composed producer→browser contract for the search-plan detail page.

Every Python test of the producer passed and every JS test of the
renderer passed for months while the per-slot stats table rendered
``—``/``?`` on every row: ``web/js/search_plan.js`` read
``identity.plan_ordinal`` while ``lib/pipeline_db/search_plan.py``
writes stats identities as ``{"plan_id", "ordinal", "strategy"}``. Each
side's fixtures were hand-written to its own idea of the contract, so
nothing composed the two.

This test crosses that wire end to end: real PostgreSQL rows, the real
routes (``GET /api/pipeline/<id>/search-plan`` and its ``/history``
sibling, including their own serialisation), and the real
``renderDetailPage`` running under Node. Nothing between the two is a
fixture except the library payload, which belongs to a third route this
test does not exercise. Every seeded value is a sentinel unique to its
field, so a needle can never match a neighbour.

Deterministic by construction — no Hypothesis, no sampling.
"""

from __future__ import annotations

import configparser
import dataclasses
import os
import re
import sys
import unittest
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

import msgspec

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

from lib.config import CratediggerConfig
from lib.pipeline_db import (
    ConsumedAttemptInput,
    PipelineDB,
    SearchPlanItemInput,
)
from lib.quality import CandidateScore
from lib.search import SEARCH_PLAN_GENERATOR_ID
from tests.helpers import REQUEST_CASCADE_RESET_TABLES, delete_all_rows
from tests.node_jsonl_worker import NodeJsonlWorker
from tests.web._harness import _WebServerCase
from web.runtime import install_runtime, runtime

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_DSN = os.environ["TEST_DB_DSN"]

# One sentinel per field. Nothing here is a plausible production value:
# an assertion that matches must have matched the value this test seeded
# into that exact column.
ARTIST = "Wirecheck Artist"
ALBUM = "Wirecheck Album"
IN_SCOPE_TIER = "wiretier-lossless"
OFF_SCOPE_TIER = "wiretier-offscope"
CONFIGURED_TIER = "wiretier-configured"
STRATEGIES = ("wirestrat-alpha", "wirestrat-beta", "wirestrat-gamma")
QUERIES = ("wirequery alpha", "wirequery beta", "wirequery gamma")
FOUND_PEER = "wirepeer-found"
OTHER_PEER = "wirepeer-other"
GRAB_FILETYPE = "wireflac"
GRAB_ERROR = "wire grab error sentinel; retried 3 times"
GRAB_ERROR_FIRST_CLAUSE = "wire grab error sentinel"
HISTORY_PAGE_LIMIT = 5

_DETAIL_PAGE_WORKER = """
import { renderDetailPage } from './web/js/search_plan.js';

async function handle(operation, payload) {
  if (operation === 'detail') {
    return {
      html: renderDetailPage({
        inspection: payload.inspection,
        history: payload.history,
        nextBeforeId: payload.next_before_id,
        library: payload.library,
      }),
    };
  }
  throw new Error(`unknown operation ${operation}`);
}
"""

#: What ``GET /api/pipeline/<id>`` contributes to the page. That route is
#: not under test here, so it stays a literal in its own shape; the
#: renderer reports its "nothing resolved" state for it.
LIBRARY_PAYLOAD: dict[str, object] = {
    "current_library": {"state": "none"},
    "beets_tracks": [],
    "request": {"id": 0},
    "history": [],
}


def _cells(row_html: str) -> list[str]:
    """The text of every ``<td>`` in one rendered row, tags stripped."""
    return [
        re.sub(r"<[^>]*>", "", cell).strip()
        for cell in re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL)
    ]


def _plan_rows(html: str) -> dict[str, list[str]]:
    """Plan-table rows keyed by the strategy cell each one carries."""
    table = html.split('<table class="sp-plan-table">', 1)
    if len(table) != 2:
        return {}
    body = table[1].split("</table>", 1)[0]
    rows: dict[str, list[str]] = {}
    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", body, re.DOTALL):
        cells = _cells(row_html)
        if len(cells) >= 2:
            rows[cells[1]] = cells
    return rows


class TestSearchPlanDetailPageWire(_WebServerCase):
    """Real rows → real routes → real ``renderDetailPage``."""

    worker: ClassVar[NodeJsonlWorker]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.worker = NodeJsonlWorker(_DETAIL_PAGE_WORKER, cwd=REPO_ROOT)
        cls.addClassCleanup(cls.worker.close)

    def setUp(self) -> None:
        super().setUp()
        self.db = PipelineDB(TEST_DSN)
        self.addCleanup(self.db.close)
        delete_all_rows(self.db, REQUEST_CASCADE_RESET_TABLES)
        # The routes reach the DB through the installed runtime handle,
        # which is production-typed — a real PipelineDB needs no bridge.
        self.enterContext(install_runtime(
            dataclasses.replace(runtime(), shared_db=self.db)))
        cp = configparser.RawConfigParser()
        cp.read_string(
            "[Search Settings]\n"
            f"allowed_filetypes = {CONFIGURED_TIER}, {IN_SCOPE_TIER}\n")
        self.enterContext(patch(
            "lib.config.read_runtime_config",
            return_value=CratediggerConfig.from_ini(cp)))
        self.req_id = self._seed_request()
        self._seed_plan()
        self.grab_log_id = self._seed_attempts()

    # ── seeding ────────────────────────────────────────────────────
    def _seed_request(self) -> int:
        request_id = self.db.add_request(
            artist_name=ARTIST, album_title=ALBUM, source="request",
            mb_release_id="wirecheck-mbid",
        )
        self.db.update_request_fields(
            request_id, search_filetype_override=IN_SCOPE_TIER)
        return request_id

    def _seed_plan(self) -> int:
        return self.db.create_successful_search_plan(
            request_id=self.req_id,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[
                SearchPlanItemInput(
                    ordinal=ordinal, strategy=strategy, query=query,
                    canonical_query_key=f"key-{ordinal}",
                    repeat_group=f"group-{ordinal}",
                )
                for ordinal, (strategy, query) in enumerate(
                    zip(STRATEGIES, QUERIES, strict=True))
            ],
        )

    def _candidate(
        self, username: str, tier: str, matched: int, ratio: float,
    ) -> CandidateScore:
        return CandidateScore(
            username=username, dir=f"/{username}/album", filetype=tier,
            matched_tracks=matched, total_tracks=11, avg_ratio=ratio,
            missing_titles=[], file_count=matched,
        )

    def _consume(
        self, outcome: str, *, candidates: list[CandidateScore] | None = None,
        result_count: int = 0, elapsed_s: float = 2.0,
    ) -> int:
        """Consume the slot the cursor really schedules next, executor-style."""
        active = self.db.get_active_search_plan(self.req_id)
        assert active is not None
        item = next(
            i for i in active.items if i.ordinal == active.next_ordinal)
        result = self.db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=self.req_id,
            plan_id=active.plan.id,
            plan_item_id=item.id,
            plan_ordinal=item.ordinal,
            plan_strategy=item.strategy,
            plan_canonical_query_key=item.canonical_query_key,
            plan_repeat_group=item.repeat_group,
            plan_generator_id=active.plan.generator_id,
            query=item.query,
            outcome=outcome,
            result_count=result_count,
            elapsed_s=elapsed_s,
            candidates_json=(
                None if candidates is None
                else msgspec.json.encode(candidates).decode()),
            plan_item_count=len(active.items),
            cycle_count_snapshot=active.cycle_count,
        ))
        self.assertFalse(
            result.is_stale, "seeding must follow the cursor, never race it")
        return result.search_log_id

    def _seed_attempts(self) -> int:
        """Seven consumed attempts over three slots, then one linked grab.

        Per-slot tallies are deliberately uneven — 3/2/2 attempts with a
        different outcome mix each — so a row rendered against the wrong
        slot's stats cannot pass by carrying its neighbour's numbers.
        """
        self._consume("no_results")                                     # ord 0
        self._consume("no_match", candidates=[                          # ord 1
            self._candidate(OTHER_PEER, OFF_SCOPE_TIER, 4, 0.41)])
        self._consume("no_match", candidates=[                          # ord 2
            self._candidate(OTHER_PEER, IN_SCOPE_TIER, 9, 0.80)])
        self._consume("no_results")                                     # ord 0
        self._consume("no_match")                                       # ord 1
        found_id = self._consume("found", result_count=2, candidates=[  # ord 2
            self._candidate(FOUND_PEER, IN_SCOPE_TIER, 11, 0.98),
            self._candidate(OTHER_PEER, IN_SCOPE_TIER, 9, 0.80),
        ])
        self._consume("no_results")                                     # ord 0
        return self.db.log_download(
            self.req_id, soulseek_username=FOUND_PEER,
            filetype=GRAB_FILETYPE, outcome="timeout",
            error_message=GRAB_ERROR, search_log_id=found_id,
        )

    # ── the wire ───────────────────────────────────────────────────
    def _render(self) -> str:
        """Fetch both routes and render exactly what the browser renders."""
        status, inspection = self._get(
            f"/api/pipeline/{self.req_id}/search-plan")
        self.assertEqual(status, 200, inspection)
        status, history = self._get(
            f"/api/pipeline/{self.req_id}/search-plan/history"
            f"?limit={HISTORY_PAGE_LIMIT}")
        self.assertEqual(status, 200, history)
        self.history = history
        rendered = self.worker.request("detail", {
            "inspection": inspection,
            "history": history["rows"],
            "next_before_id": history["next_before_id"],
            "library": LIBRARY_PAYLOAD,
        })
        if not isinstance(rendered, dict):
            raise TypeError(f"worker returned {type(rendered).__name__}")
        html = rendered.get("html")
        if not isinstance(html, str):
            raise TypeError("worker returned no html")
        return html

    def _attempts_section(self, html: str) -> str:
        marker = '<table class="sp-attempts-table"'
        self.assertIn(marker, html)
        return html.split(marker, 1)[1]

    # ── assertions ─────────────────────────────────────────────────
    def test_plan_table_shows_each_slot_its_own_seeded_tallies(self) -> None:
        rows = _plan_rows(self._render())
        self.assertEqual(sorted(rows), sorted(STRATEGIES))
        # ordinal, strategy, query, tried, found, no match, empty
        self.assertEqual(
            rows[STRATEGIES[0]][:7],
            ["0", STRATEGIES[0], QUERIES[0], "3", "0", "0", "3"])
        self.assertEqual(
            rows[STRATEGIES[1]][:7],
            ["1", STRATEGIES[1], QUERIES[1], "2", "0", "2", "0"])
        self.assertEqual(
            rows[STRATEGIES[2]][:7],
            ["2", STRATEGIES[2], QUERIES[2], "2", "1", "1", "0"])
        # The mean-elapsed and last-seen cells prove the matched stats row
        # carried its own telemetry, not just a count.
        self.assertEqual(rows[STRATEGIES[0]][7], "2s")
        self.assertNotEqual(rows[STRATEGIES[0]][8], "")

    def test_scope_chips_light_the_seeded_override_only(self) -> None:
        html = self._render()
        self.assertIn(
            f'<span class="sp-tier sp-tier-on">{IN_SCOPE_TIER}</span>', html)
        self.assertIn(f'<span class="sp-tier">{CONFIGURED_TIER}</span>', html)
        self.assertIn('<span class="sp-tier">any</span>', html)
        self.assertIn(f"<code>{IN_SCOPE_TIER}</code>", html)
        self.assertIn("from the request search override", html)

    def test_override_checks_carry_the_seeded_tiers_and_last_found(
        self,
    ) -> None:
        html = self._render()
        self.assertIn(
            f"<strong>1</strong> of <strong>4</strong> candidates scored "
            f"outside the scope · 3 {IN_SCOPE_TIER} · 1 {OFF_SCOPE_TIER}"
            f" · off-scope: {OFF_SCOPE_TIER}",
            html)
        self.assertIn(
            f"<strong>1</strong> grabs · <strong>1</strong> {GRAB_FILETYPE}",
            html)
        self.assertIn(
            f'Last found: <span class="sp-check-peer">{FOUND_PEER}</span> '
            f"{IN_SCOPE_TIER} <strong>11/11</strong> via {STRATEGIES[2]} ",
            html)
        self.assertIn(f"grab timeout · {GRAB_ERROR_FIRST_CLAUSE}", html)
        # Every peer the search scored, each with its own best match and
        # its own attempt count -- the peers query's own aggregation.
        self.assertIn(
            f'Peers seen: <span class="sp-check-peer">{OTHER_PEER}</span> '
            f'{IN_SCOPE_TIER} 9/11 <span class="sp-check-when">×3</span> · '
            f'<span class="sp-check-peer">{FOUND_PEER}</span> '
            f'{IN_SCOPE_TIER} 11/11 <span class="sp-check-when">×1</span>',
            html)

    def test_history_rows_carry_their_own_grab_and_cursor(self) -> None:
        html = self._render()
        attempts = self._attempts_section(html)
        self.assertIn(f"grab timeout · {GRAB_ERROR_FIRST_CLAUSE}", attempts)
        self.assertIn(
            f"grab: {self.grab_log_id} timeout {GRAB_FILETYPE} {FOUND_PEER}",
            attempts)
        self.assertIn(f"2 of {HISTORY_PAGE_LIMIT} loaded", html)
        next_before_id = self.history["next_before_id"]
        self.assertIsInstance(next_before_id, int)
        self.assertIn(
            f"window.searchPlanLoadOlder({self.req_id}, {next_before_id})",
            html)


if __name__ == "__main__":
    unittest.main()
