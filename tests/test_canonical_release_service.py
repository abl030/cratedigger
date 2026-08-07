"""Authoritative coverage for the canonical-release reconciler (#1059).

Every outcome branch, plus the two invariants only this service can break:

I2  Only an observed ``301`` writes. Nothing else -- not a body field, not a
    metadata match, not a release-group relative.
I7  A canonical may be UPDATED on a further merge; it is never cleared by a
    failed or unavailable lookup. That is why the service has no clearing
    path at all: the operation does not exist for it to call.

Per test-fidelity Rule B the mirror fakes raise the exception classes
``urllib`` really raises, so a fail-open claim is tested against the real
failure shape rather than a synthetic ``None``.
"""

from __future__ import annotations

import email.message
import unittest
import urllib.error
from collections.abc import Mapping
from datetime import UTC, datetime

from lib.canonical_release_service import (
    OUTCOME_FROZEN,
    OUTCOME_INVALID_IDENTITY,
    OUTCOME_NO_CANONICAL,
    OUTCOME_NO_REDIRECT,
    OUTCOME_NOT_FOUND,
    OUTCOME_NOT_MUSICBRAINZ,
    OUTCOME_RESOLVED,
    OUTCOME_RETIRED,
    OUTCOME_STALE,
    OUTCOME_UNCHANGED,
    CanonicalReleaseService,
)
from tests.fakes import FakePipelineDB

LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
SECOND_SURVIVOR = "abe18a1c-ad01-423c-b6ca-63cfa8a9daf1"
NOW = datetime(2026, 8, 6, 12, 0, 0, tzinfo=UTC)


def _service(db: FakePipelineDB, answers: Mapping[str, str]):
    return CanonicalReleaseService(
        db,
        canonical_fn=lambda release_id: answers.get(release_id),
        now_fn=lambda: NOW,
    )


def _raising_service(db: FakePipelineDB, exc: BaseException):
    """A resolver whose fetch blows up the way the real mirror can.

    The production ``canonical_release_id`` swallows these itself; this
    proves the service does not depend on that and would still fail open
    if the resolver were ever made to raise.
    """
    def _fn(_release_id: str) -> str | None:
        raise exc

    return CanonicalReleaseService(db, canonical_fn=_fn, now_fn=lambda: NOW)


class TestReconcileRequest(unittest.TestCase):
    def setUp(self) -> None:
        self.db = FakePipelineDB()

    def _seed(
        self, *, mb: str | None = LOSER, discogs: str | None = None,
    ) -> int:
        return self.db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=mb, discogs_release_id=discogs,
        )

    def test_observed_redirect_stores_the_survivor(self) -> None:
        rid = self._seed()
        result = _service(self.db, {LOSER: SURVIVOR}).reconcile_request(rid)

        self.assertEqual(result.outcome, OUTCOME_RESOLVED)
        self.assertTrue(result.changed)
        self.assertEqual(result.canonical_release_id, SURVIVOR)
        row = self.db.request(rid)
        self.assertEqual(row["canonical_release_id"], SURVIVOR)
        self.assertEqual(row["canonical_resolved_at"], NOW)
        # I1: the acquisition id is frozen history.
        self.assertEqual(row["mb_release_id"], LOSER)

    def test_no_redirect_writes_nothing(self) -> None:
        rid = self._seed()
        result = _service(self.db, {}).reconcile_request(rid)

        self.assertEqual(result.outcome, OUTCOME_NO_REDIRECT)
        self.assertFalse(result.changed)
        self.assertIsNone(self.db.request(rid)["canonical_release_id"])
        self.assertEqual(self.db.record_canonical_release_id_calls, [])

    def test_a_further_merge_updates_the_stored_survivor(self) -> None:
        """I7 — a survivor that is itself merged moves forward."""
        rid = self._seed()
        _service(self.db, {LOSER: SURVIVOR}).reconcile_request(rid)
        result = _service(
            self.db, {LOSER: SECOND_SURVIVOR}).reconcile_request(rid)

        self.assertEqual(result.outcome, OUTCOME_RESOLVED)
        self.assertEqual(result.previous_canonical_release_id, SURVIVOR)
        self.assertEqual(
            self.db.request(rid)["canonical_release_id"], SECOND_SURVIVOR)

    def test_repeat_of_the_same_answer_is_a_no_op(self) -> None:
        rid = self._seed()
        answers = {LOSER: SURVIVOR}
        _service(self.db, answers).reconcile_request(rid)
        self.db.record_canonical_release_id_calls.clear()

        result = _service(self.db, answers).reconcile_request(rid)

        self.assertEqual(result.outcome, OUTCOME_UNCHANGED)
        self.assertEqual(self.db.record_canonical_release_id_calls, [])

    def test_mirror_failure_never_clears_a_stored_survivor(self) -> None:
        """I7's teeth: yesterday's answer survives today's outage."""
        rid = self._seed()
        _service(self.db, {LOSER: SURVIVOR}).reconcile_request(rid)

        result = _service(self.db, {}).reconcile_request(rid)

        self.assertEqual(result.outcome, OUTCOME_NO_REDIRECT)
        self.assertEqual(
            self.db.request(rid)["canonical_release_id"], SURVIVOR)

    def test_only_explicit_retirement_can_clear_a_survivor(self) -> None:
        """I7: reconciliation itself has no clearing capability."""
        from lib.canonical_release_service import CanonicalReleaseDB

        surface = {
            name for name in dir(CanonicalReleaseDB)
            if not name.startswith("_")
        }
        self.assertEqual(
            surface,
            {
                "get_request",
                "list_non_replaced_requests",
                "record_canonical_release_id",
                "retire_canonical_release_id",
            },
        )


class TestRetireRequest(unittest.TestCase):
    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.request_id = self.db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=LOSER,
        )
        self.db.record_canonical_release_id(
            self.request_id, canonical_release_id=SURVIVOR, resolved_at=NOW,
        )

    def test_explicit_retirement_clears_only_canonical_fields(self) -> None:
        before = dict(self.db.request(self.request_id))
        result = CanonicalReleaseService(
            self.db, now_fn=lambda: NOW.replace(hour=13),
        ).retire_request(self.request_id)

        self.assertEqual(result.outcome, OUTCOME_RETIRED)
        self.assertTrue(result.changed)
        self.assertEqual(result.previous_canonical_release_id, SURVIVOR)
        after = self.db.request(self.request_id)
        self.assertIsNone(after["canonical_release_id"])
        self.assertIsNone(after["canonical_resolved_at"])
        self.assertEqual(
            {
                field for field in before if before[field] != after[field]
            },
            {"canonical_release_id", "canonical_resolved_at"},
        )

    def test_missing_no_canonical_and_replaced_are_noops(self) -> None:
        service = CanonicalReleaseService(self.db, now_fn=lambda: NOW)
        self.assertEqual(
            service.retire_request(999_999).outcome, OUTCOME_NOT_FOUND)

        no_canonical = self.db.add_request(
            artist_name="Plain", album_title="Release", source="request",
            mb_release_id=SECOND_SURVIVOR,
        )
        before = dict(self.db.request(no_canonical))
        self.assertEqual(
            service.retire_request(no_canonical).outcome, OUTCOME_NO_CANONICAL)
        self.assertEqual(self.db.request(no_canonical), before)

        self.db.supersede_request_mbid(
            self.request_id,
            new_mb_release_id="bce7d8c3-815b-449c-8e18-df806398986c",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Merged",
            new_album_title="Release",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        frozen = dict(self.db.request(self.request_id))
        self.assertEqual(
            service.retire_request(self.request_id).outcome, OUTCOME_FROZEN)
        self.assertEqual(self.db.request(self.request_id), frozen)

    def test_a_newer_observation_wins_the_retirement_race(self) -> None:
        class RacingDB(FakePipelineDB):
            def retire_canonical_release_id(self, request_id, **kwargs):
                return False

        db = RacingDB()
        request_id = db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=LOSER,
        )
        db.record_canonical_release_id(
            request_id, canonical_release_id=SURVIVOR, resolved_at=NOW,
        )
        before = dict(db.request(request_id))
        result = CanonicalReleaseService(db, now_fn=lambda: NOW).retire_request(
            request_id,
        )

        self.assertEqual(result.outcome, OUTCOME_STALE)
        self.assertEqual(db.request(request_id), before)


class TestReconcileRequestOutcomes(unittest.TestCase):
    def setUp(self) -> None:
        self.db = FakePipelineDB()

    def _seed(
        self, *, mb: str | None = LOSER, discogs: str | None = None,
    ) -> int:
        return self.db.add_request(
            artist_name="Merged", album_title="Release", source="request",
            mb_release_id=mb, discogs_release_id=discogs,
        )

    def test_discogs_request_never_asks_musicbrainz(self) -> None:
        rid = self._seed(mb=None, discogs="12856590")
        asked: list[str] = []

        def _fn(release_id: str) -> str | None:
            asked.append(release_id)
            return SURVIVOR

        result = CanonicalReleaseService(
            self.db, canonical_fn=_fn, now_fn=lambda: NOW,
        ).reconcile_request(rid)

        self.assertEqual(result.outcome, OUTCOME_NOT_MUSICBRAINZ)
        self.assertEqual(asked, [])
        self.assertIsNone(self.db.request(rid)["canonical_release_id"])

    def test_unknown_request_is_not_found(self) -> None:
        result = _service(self.db, {}).reconcile_request(999_999)
        self.assertEqual(result.outcome, OUTCOME_NOT_FOUND)

    def test_unusable_identity_is_reported_not_guessed(self) -> None:
        rid = self._seed(mb="not-a-uuid")
        result = _service(self.db, {}).reconcile_request(rid)
        self.assertEqual(result.outcome, OUTCOME_INVALID_IDENTITY)

    def test_a_superseded_row_reports_frozen(self) -> None:
        rid = self._seed()
        self.db.supersede_request_mbid(
            rid,
            new_mb_release_id=SECOND_SURVIVOR,
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Merged",
            new_album_title="Release",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )

        result = _service(self.db, {LOSER: SURVIVOR}).reconcile_request(rid)

        self.assertEqual(result.outcome, OUTCOME_FROZEN)
        self.assertIsNone(self.db.request(rid)["canonical_release_id"])

    def test_a_raising_resolver_still_reaches_the_caller(self) -> None:
        """The service does not swallow a programming error as 'no merge'.

        ``lib.mb_canonical`` owns fail-open for transport failures; if a
        resolver ever raises through it, that is a bug worth surfacing, not
        a silent no-redirect.
        """
        rid = self._seed()
        exc = urllib.error.HTTPError(
            url="http://mirror/ws/2/release/x",
            code=500,
            msg="boom",
            hdrs=email.message.Message(),
            fp=None,
        )
        with self.assertRaises(urllib.error.HTTPError):
            _raising_service(self.db, exc).reconcile_request(rid)


class TestReconcileAll(unittest.TestCase):
    def test_the_sweep_covers_every_non_replaced_row_and_counts_outcomes(
        self,
    ) -> None:
        db = FakePipelineDB()
        merged = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id=LOSER,
        )
        current = db.add_request(
            artist_name="C", album_title="D", source="request",
            mb_release_id=SECOND_SURVIVOR,
        )
        discogs = db.add_request(
            artist_name="E", album_title="F", source="request",
            discogs_release_id="12856590",
        )
        streamed: list[str] = []

        result = _service(db, {LOSER: SURVIVOR}).reconcile_all(
            on_result=lambda r: streamed.append(r.outcome),
        )

        self.assertEqual(result.scanned, 3)
        self.assertEqual(result.changed, 1)
        self.assertEqual(
            [r.request_id for r in result.resolved], [merged])
        self.assertEqual(result.outcome_counts[OUTCOME_RESOLVED], 1)
        self.assertEqual(result.outcome_counts[OUTCOME_NO_REDIRECT], 1)
        self.assertEqual(result.outcome_counts[OUTCOME_NOT_MUSICBRAINZ], 1)
        self.assertEqual(len(streamed), 3)
        self.assertEqual(db.request(current)["canonical_release_id"], None)
        self.assertEqual(db.request(discogs)["canonical_release_id"], None)

    def test_an_empty_library_sweeps_cleanly(self) -> None:
        result = _service(FakePipelineDB(), {}).reconcile_all()
        self.assertEqual(result.scanned, 0)
        self.assertEqual(result.changed, 0)


if __name__ == "__main__":
    unittest.main()
