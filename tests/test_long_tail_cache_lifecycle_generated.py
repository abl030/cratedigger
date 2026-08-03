"""Generated cache-failure lifecycle laws for the real long-tail JS seam."""

from __future__ import annotations

import json
import unittest
from dataclasses import asdict, dataclass
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.node_jsonl_worker import NodeJsonlWorker

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class LongTailCacheWorld:
    rows: list[dict[str, object]]
    band: str | None
    query: str
    console_ids: list[int]
    current_failure: bool
    pipeline_view: str
    identity_source: str


_LONG_TAIL_CACHE_WORKER = """
import {
  applyLongTailLoadFailure,
  renderLongTailRow,
  shouldPaintLongTailLoadFailure,
} from './web/js/long_tail.js';
import {
  checkYoutube,
  consoleCanStart,
  consoleStates,
  consoleSetYoutubeResultForGeneration,
  consoleOpen,
  consoleSetYoutubeResult,
  consoleYoutubeResult,
  longTailConsoleGeneration,
} from './web/js/long_tail_console.js';
import { state } from './web/js/state.js';

async function handle(operation, payload) {
  if (operation !== 'transition') {
    throw new Error(`unknown long-tail cache operation: ${operation}`);
  }
  payload = JSON.parse(payload);
  const longTail = {
    rows: payload.rows,
    band: payload.band,
    query: payload.query,
  };
  const consoles = new Map();
  for (const id of payload.console_ids) {
    consoleOpen(consoles, id);
    consoleCanStart(consoles, id, 'resolve');
    consoleSetYoutubeResult(consoles, id, {
      outcome: 'ok', youtube_releases: [{ browse_id: `cached-${id}` }],
    });
  }
  const snapshotConsoles = () => [...consoles.entries()].map(([id, entry]) => ({
    id,
    open: entry.open,
    token: entry.token,
    youtube_result: entry.youtubeResult,
    in_flight: [...entry.inFlight],
  }));
  const consolesBeforeFailure = snapshotConsoles();
  const pendingGeneration = longTailConsoleGeneration();
  const activeToken = 17;
  const failedToken = payload.current_failure ? activeToken : activeToken - 1;
  const applied = applyLongTailLoadFailure(
    failedToken, activeToken, longTail, consoles,
  );
  const consolesAfterFailure = snapshotConsoles();
  const settleId = 2000001;
  const staleSettleAccepted = consoleSetYoutubeResultForGeneration(
    consoles,
    settleId,
    { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'stale' }] },
    pendingGeneration,
  );
  const staleSettleCreatedState = consoles.has(settleId);
  const newGeneration = longTailConsoleGeneration();
  const newSettleAccepted = consoleSetYoutubeResultForGeneration(
    consoles,
    settleId,
    { outcome: 'ok', youtube_releases: [{ yt_browse_id: 'fresh' }] },
    newGeneration,
  );
  const currentResult = consoleYoutubeResult(consoles, settleId);
  const releaseFields = payload.identity_source === 'discogs'
    ? { mb_release_id: null, discogs_release_id: '12856590' }
    : {
        mb_release_id: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        discogs_release_id: null,
      };
  const sourceHtml = renderLongTailRow({
    id: 3000001,
    artist_name: 'Generated',
    album_title: 'Exact identity',
    band: 'missing',
    in_flight_rescue: false,
    track_count: 1,
    ...releaseFields,
  });
  const actionId = 3000002;
  let actionCalls = 0;
  let actionBody = null;
  const originalFetch = globalThis.fetch;
  state.longTail = {
    rows: [{ id: actionId, ...releaseFields }],
    band: null,
    query: '',
  };
  globalThis.fetch = async (_url, options) => {
    actionCalls += 1;
    actionBody = JSON.parse(options.body);
    return {
      status: 200,
      async json() {
        return { outcome: 'ok', youtube_releases: [] };
      },
    };
  };
  try {
    await checkYoutube(actionId);
  } finally {
    globalThis.fetch = originalFetch;
    consoleStates.delete(actionId);
  }
  return {
    applied,
    paint_failure: shouldPaintLongTailLoadFailure(applied, payload.pipeline_view),
    rows: longTail.rows,
    band: longTail.band,
    query: longTail.query,
    consoles_before_failure: consolesBeforeFailure,
    consoles_after_failure: consolesAfterFailure,
    stale_settle_accepted: staleSettleAccepted,
    stale_settle_created_state: staleSettleCreatedState,
    new_settle_accepted: newSettleAccepted,
    current_result: currentResult,
    source_chip_present: sourceHtml.includes(
      payload.identity_source === 'discogs' ? 'Discogs' : 'MusicBrainz'
    ),
    action_calls: actionCalls,
    action_identifier: actionBody && actionBody.identifier,
    action_refresh: actionBody && actionBody.refresh,
  };
}
"""


def _run_real_failure_transition(
    worker: NodeJsonlWorker,
    world: LongTailCacheWorld,
) -> dict[str, object]:
    # Node's readline treats U+2028/U+2029 as record separators. Keep the JSONL
    # frame ASCII while restoring the exact generated Unicode inside Node.
    payload = json.dumps(asdict(world), ensure_ascii=True)
    parsed = worker.request("transition", payload)
    if not isinstance(parsed, dict):
        raise TypeError("long-tail JS transition must return an object")
    return parsed


def assert_failure_transition(
    world: LongTailCacheWorld,
    result: dict[str, object],
) -> None:
    """Current failures invalidate all cache; stale failures mutate nothing."""
    if result.get("applied") is not world.current_failure:
        raise AssertionError("failure transition applied against the wrong token")
    if result.get("query") != world.query:
        raise AssertionError("failure transition changed the search query")

    if world.current_failure:
        if result.get("rows") is not None:
            raise AssertionError("current failure retained cached rows")
        if result.get("band") is not None:
            raise AssertionError("current failure retained the selected band")
        if result.get("consoles_after_failure") != []:
            raise AssertionError("current failure retained cached console state")
        return

    if result.get("rows") != world.rows:
        raise AssertionError("stale failure changed the newer cached rows")
    if result.get("band") != world.band:
        raise AssertionError("stale failure changed the newer selected band")
    if (
        result.get("consoles_after_failure")
        != result.get("consoles_before_failure")
    ):
        raise AssertionError("stale failure changed the newer console state")


def assert_failure_paint(world: LongTailCacheWorld, result: dict[str, object]) -> None:
    """Only a current failure in the active Long Tail view may paint."""
    expected = world.current_failure and world.pipeline_view == "long-tail"
    if result.get("paint_failure") is not expected:
        raise AssertionError("failure painted the wrong pipeline view")


def assert_pending_operation_fence(
    world: LongTailCacheWorld,
    result: dict[str, object],
) -> None:
    """Invalidation rejects old settles and accepts new-generation work."""
    old_should_settle = not world.current_failure
    if result.get("stale_settle_accepted") is not old_should_settle:
        raise AssertionError("pending operation crossed the failure generation")
    if result.get("stale_settle_created_state") is not old_should_settle:
        raise AssertionError("pending operation recreated invalidated console state")
    if result.get("new_settle_accepted") is not True:
        raise AssertionError("new-generation operation was rejected")
    current = result.get("current_result")
    assert isinstance(current, dict), "new-generation result was not cached"
    releases = current.get("youtube_releases")
    assert isinstance(releases, list) and releases, (
        "new-generation result lost its release payload"
    )
    first = releases[0]
    assert isinstance(first, dict), "new-generation release payload was malformed"
    if first.get("yt_browse_id") != "fresh":
        raise AssertionError("stale operation overwrote the new-generation result")


def assert_exact_source_chip(
    world: LongTailCacheWorld,
    result: dict[str, object],
) -> None:
    """Either exact request source renders its pressing-source chip."""
    if result.get("source_chip_present") is not True:
        raise AssertionError(
            f"{world.identity_source} exact identity lost its source chip"
        )


def assert_exact_resolver_action(
    world: LongTailCacheWorld,
    result: dict[str, object],
) -> None:
    """Either exact request source drives the same resolver request body."""
    expected_identifier = (
        "12856590"
        if world.identity_source == "discogs"
        else "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    )
    if result.get("action_calls") != 1:
        raise AssertionError("exact resolver action did not send one request")
    if result.get("action_identifier") != expected_identifier:
        raise AssertionError(
            f"{world.identity_source} exact identity did not drive resolver action"
        )
    if result.get("action_refresh") is not False:
        raise AssertionError("resolver action changed the refresh contract")


@st.composite
def _cached_rows(draw: st.DrawFn) -> dict[str, object]:
    return {
        "id": draw(st.integers(min_value=1, max_value=1_000_000)),
        "artist_name": draw(st.text(max_size=24)),
        "album_title": draw(st.text(max_size=24)),
        "band": draw(st.sampled_from((
            "missing", "unknown", "poor", "acceptable", "good", "lossless",
        ))),
    }


@st.composite
def _cache_worlds(draw: st.DrawFn) -> LongTailCacheWorld:
    return LongTailCacheWorld(
        rows=draw(st.lists(_cached_rows(), max_size=12)),
        band=draw(st.one_of(
            st.none(),
            st.sampled_from((
                "missing", "unknown", "poor", "acceptable", "good",
                "lossless", "future-band",
            )),
        )),
        query=draw(st.text(max_size=32)),
        console_ids=draw(st.lists(
            st.integers(min_value=1, max_value=1_000_000),
            unique=True,
            max_size=12,
        )),
        current_failure=draw(st.booleans()),
        pipeline_view=draw(st.sampled_from((
            "long-tail", "dashboard", "search-plan-detail",
        ))),
        identity_source=draw(st.sampled_from(("musicbrainz", "discogs"))),
    )


class TestLongTailCacheLifecycleGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = NodeJsonlWorker(_LONG_TAIL_CACHE_WORKER, cwd=ROOT)
        self.addCleanup(self.worker.close)

    @given(world=_cache_worlds())
    @example(LongTailCacheWorld(
        rows=[{
            "id": 501,
            "artist_name": "Cached Missing Artist",
            "album_title": "Cached Action Album",
            "band": "missing",
        }],
        band="missing",
        query="needle",
        console_ids=[501, 777],
        current_failure=True,
        pipeline_view="long-tail",
        identity_source="discogs",
    ))
    @example(LongTailCacheWorld(
        rows=[{
            "id": 502,
            "artist_name": "Newer Artist",
            "album_title": "Newer Album",
            "band": "good",
        }],
        band="good",
        query="newer",
        console_ids=[502],
        current_failure=False,
        pipeline_view="dashboard",
        identity_source="musicbrainz",
    ))
    def test_only_the_current_failure_invalidates_all_cached_state(
        self,
        world: LongTailCacheWorld,
    ) -> None:
        result = _run_real_failure_transition(self.worker, world)
        assert_failure_transition(world, result)
        assert_failure_paint(world, result)
        assert_pending_operation_fence(world, result)
        assert_exact_source_chip(world, result)
        assert_exact_resolver_action(world, result)

    def test_checker_rejects_the_retained_cache_mutant(self) -> None:
        world = LongTailCacheWorld(
            rows=[{
                "id": 501,
                "artist_name": "Cached Missing Artist",
                "album_title": "Cached Action Album",
                "band": "missing",
            }],
            band="missing",
            query="needle",
            console_ids=[501],
            current_failure=True,
            pipeline_view="long-tail",
            identity_source="discogs",
        )
        retained_cache = {
            "applied": True,
            "rows": world.rows,
            "band": world.band,
            "query": world.query,
            "consoles_before_failure": [{"id": 501}],
            "consoles_after_failure": [{"id": 501}],
        }

        with self.assertRaisesRegex(AssertionError, "retained cached rows"):
            assert_failure_transition(world, retained_cache)

    def test_paint_checker_rejects_the_inactive_view_mutant(self) -> None:
        world = LongTailCacheWorld(
            rows=[], band=None, query="", console_ids=[],
            current_failure=True, pipeline_view="dashboard",
            identity_source="musicbrainz",
        )
        with self.assertRaisesRegex(AssertionError, "wrong pipeline view"):
            assert_failure_paint(world, {"paint_failure": True})

    def test_generation_checker_rejects_the_stale_settle_mutant(self) -> None:
        world = LongTailCacheWorld(
            rows=[], band=None, query="", console_ids=[],
            current_failure=True, pipeline_view="long-tail",
            identity_source="musicbrainz",
        )
        mutant = {
            "stale_settle_accepted": True,
            "stale_settle_created_state": True,
            "new_settle_accepted": True,
            "current_result": {
                "youtube_releases": [{"yt_browse_id": "fresh"}],
            },
        }
        with self.assertRaisesRegex(AssertionError, "crossed the failure"):
            assert_pending_operation_fence(world, mutant)

    def test_source_checker_rejects_the_mb_only_chip_mutant(self) -> None:
        world = LongTailCacheWorld(
            rows=[], band=None, query="", console_ids=[],
            current_failure=False, pipeline_view="long-tail",
            identity_source="discogs",
        )
        with self.assertRaisesRegex(AssertionError, "discogs exact identity"):
            assert_exact_source_chip(world, {"source_chip_present": False})

    def test_action_checker_rejects_the_mb_only_identifier_mutant(self) -> None:
        world = LongTailCacheWorld(
            rows=[], band=None, query="", console_ids=[],
            current_failure=False, pipeline_view="long-tail",
            identity_source="discogs",
        )
        mutant = {
            "action_calls": 0,
            "action_identifier": None,
            "action_refresh": None,
        }
        with self.assertRaisesRegex(AssertionError, "did not send one request"):
            assert_exact_resolver_action(world, mutant)


if __name__ == "__main__":
    unittest.main()
