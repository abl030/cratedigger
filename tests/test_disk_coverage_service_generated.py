"""Generated patrol for Disk Coverage's exact-resolution classifier."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_db import CurrentBeetsAmbiguous, CurrentBeetsResolution
from lib.disk_coverage_service import (
    DiskCoverageAmbiguousResolution,
    DiskCoverageBeetsDB,
    disk_coverage,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from tests.node_jsonl_worker import NodeJsonlWorker

ROOT = Path(__file__).resolve().parents[1]

_PRESENTATION_WORKER = """
import { __test__ } from './web/js/pipeline_dashboard.js';

async function handle(operation, payload) {
  if (operation !== 'render_drift_row') {
    throw new Error(`unknown disk-coverage operation: ${operation}`);
  }
  return __test__.renderDriftRow(payload);
}
"""

ResolutionKind = Literal[
    "unique",
    "missing",
    "invalid",
    "multiple_matches",
    "conflicting_identity",
    "empty_topology",
    "invalid_path",
    "unresolved_relative_path",
    "split_topology",
]


@dataclass(frozen=True)
class ResolutionWorld:
    kind: ResolutionKind
    album_count: int = 1


@st.composite
def resolution_worlds(draw: st.DrawFn) -> ResolutionWorld:
    kind = draw(st.sampled_from((
        "unique", "missing", "invalid", "multiple_matches",
        "conflicting_identity", "empty_topology", "invalid_path",
        "unresolved_relative_path", "split_topology",
    )))
    album_count = (
        draw(st.integers(min_value=2, max_value=8))
        if kind == "multiple_matches" else 1
    )
    return ResolutionWorld(kind=kind, album_count=album_count)


def _mbid(index: int) -> str:
    return f"00000000-0000-4000-8000-{index:012d}"


def _render_drift_row(
    worker: NodeJsonlWorker,
    row: object,
) -> str:
    html = worker.request("render_drift_row", row)
    if not isinstance(html, str):
        raise TypeError(f"drift renderer returned {type(html).__name__}")
    return html


class _ConflictingIdentityBeetsDB:
    """Boundary stub for the resolver-only conflicting-identity outcome."""

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        return {
            identity: CurrentBeetsAmbiguous(
                identity=identity,
                album_ids=(10,),
                reason="conflicting_identity",
            )
            for identity in identities
        }

    def list_release_identities(self) -> list[dict[str, object]]:
        return []


def _seed_world(
    world: ResolutionWorld,
    *,
    mbid: str,
) -> tuple[FakePipelineDB, DiskCoverageBeetsDB]:
    db = FakePipelineDB()
    fields: dict[str, object] = {"mb_release_id": mbid}
    if world.kind == "invalid":
        fields["discogs_release_id"] = "1"
    elif world.kind == "conflicting_identity":
        # Production Discogs requests duplicate this numeric exact identity
        # into both columns; strict identity derivation must retain Discogs.
        fields = {"mb_release_id": "12345", "discogs_release_id": "12345"}
    db.seed_request(make_request_row(id=1, status="imported", **fields))

    if world.kind == "conflicting_identity":
        return db, _ConflictingIdentityBeetsDB()

    beets = FakeBeetsDB(
        library_root="" if world.kind == "unresolved_relative_path" else "/tmp/library",
    )
    if world.kind == "unique":
        beets.set_album_exists(mbid, True)
    elif world.kind == "multiple_matches":
        beets.set_album_ids_for_release(
            mbid, list(range(10, 10 + world.album_count)),
        )
    elif world.kind == "empty_topology":
        beets.set_album_ids_for_release(mbid, [10])
        beets.set_item_paths(mbid, [])
    elif world.kind == "invalid_path":
        beets.set_album_ids_for_release(mbid, [10])
        beets.set_item_paths(mbid, [(101, None)])
    elif world.kind == "unresolved_relative_path":
        beets.set_album_ids_for_release(mbid, [10])
        beets.set_item_paths(mbid, [(101, "01.flac")])
    elif world.kind == "split_topology":
        beets.set_album_ids_for_release(mbid, [10])
        beets.set_item_paths(mbid, [
            (101, "/tmp/library/one/01.flac"),
            (102, "/tmp/library/two/02.flac"),
        ])
    return db, beets


class TestDiskCoverageResolutionGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = NodeJsonlWorker(_PRESENTATION_WORKER, cwd=ROOT)
        self.addCleanup(self.worker.close)

    @example(ResolutionWorld(kind="missing"))
    @example(ResolutionWorld(kind="invalid"))
    @example(ResolutionWorld(kind="multiple_matches", album_count=2))
    @example(ResolutionWorld(kind="conflicting_identity"))
    @example(ResolutionWorld(kind="empty_topology"))
    @example(ResolutionWorld(kind="invalid_path"))
    @example(ResolutionWorld(kind="unresolved_relative_path"))
    @example(ResolutionWorld(kind="split_topology"))
    @given(world=resolution_worlds())
    def test_service_resolution_and_real_presenter_stay_truthful(
        self,
        world: ResolutionWorld,
    ) -> None:
        db, beets = _seed_world(world, mbid=_mbid(1))

        result = disk_coverage(db, beets)

        if world.kind == "unique":
            self.assertEqual(result.counts.on_disk_total, 1)
            self.assertEqual(result.counts.off_disk_total, 0)
            self.assertEqual(result.off_disk, [])
            return

        self.assertEqual(result.counts.on_disk_total, 0)
        self.assertEqual(result.counts.off_disk_total, 1)
        assert result.off_disk is not None
        row = result.off_disk[0]
        html = _render_drift_row(self.worker, msgspec.to_builtins(row))

        if world.kind in {"missing", "invalid"}:
            self.assertEqual(row.resolution.kind, "missing")
            self.assertIn('metric-bad">missing', html)
            if world.kind == "invalid":
                self.assertIsNone(row.source)
            return

        resolution = row.resolution
        assert isinstance(resolution, DiskCoverageAmbiguousResolution)
        self.assertEqual(resolution.reason, world.kind)
        expected_ids = (
            tuple(range(10, 10 + world.album_count))
            if world.kind == "multiple_matches" else (10,)
        )
        self.assertEqual(resolution.album_ids, expected_ids)
        label = (
            f"ambiguous ({world.album_count} "
            f"{'album' if world.album_count == 1 else 'albums'})"
        )
        self.assertIn(label, html)
        self.assertNotIn('metric-bad">missing', html)
        if world.kind == "conflicting_identity":
            self.assertEqual(row.source, "discogs")
            self.assertIn("ambiguous (1 album)", html)
            self.assertNotIn("Follow MB merge", html)


if __name__ == "__main__":
    unittest.main()
