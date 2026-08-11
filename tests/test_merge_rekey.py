"""Deterministic pins for the import-time MusicBrainz merge rekey (#1059).

MusicBrainz editors merge release A into release B. The loser's MBID becomes a
permanent 301, and a request stored at A can never import again: Beets offers
candidate B, our matcher demands A, and every download is rejected
``mbid_not_found`` forever. Live instance — request 346, DICE "Midnight Zoo",
stored ``6b209cc5-…`` which redirects to ``9b59f78b-…``, whose album is already
installed under the old id.

The fix lives at exactly one seam: ``lib/download_validation.py``, on the
``mbid_not_found`` result, where the failure announces itself. The invariants
these pin — the generated siblings in ``tests/test_merge_rekey_generated.py``
patrol the world space around them:

M1  **Retag before rekey, always.** Beets keys album duplicate detection on
    ``mb_albumid`` (``duplicate_keys: album: [mb_albumid, discogs_albumid]``,
    combined as an ``AndQuery`` in ``beets/library/models.py``). A request
    rekeyed onto the survivor while the installed album is still filed under
    the merged-away id flags NO duplicate, so the import lands a SECOND album
    beside the first, and the existing-album lookup misses — routing the
    quality decision through ``import_no_exist`` and silently skipping the
    downgrade guard. The row must not move until the library observably has.
M2  **The mirror is asked only on ``mbid_not_found``, at most once.** The
    performance contract: ~8,500 healthy validations a cycle must never make a
    network call.
M3  **Every non-ready world keeps today's rejection, byte for byte**, and
    leaves the request runnable for the next cycle. Nothing is parked, no
    marker whose only exit is a human is written (invariant 11).
M4  **A survivor another request already holds fails closed.** Two requests
    are two curated pressings; merging or deleting either is the operator's
    call (invariant 5).
M5  **Exactly one place turns a candidate into a scenario.** The rekeyed
    result is re-derived by ``lib.beets.apply_candidate_scenario`` — the same
    function ``beets_validate`` uses — never by a second copy of the
    distance/extra-tracks branch.
M6  **The evidence moves with the identity.** Evidence is content-addressed
    by ``(mb_release_id, snapshot_fingerprint)``, so a rekey that leaves it
    behind strands the request's verified-lossless proof at an id nothing
    names any more — and the rebuilt HAVE row silently drops the proof lock,
    the quality gate loads no state and reopens full-tier search on the very
    import this seam exists to enable. The rekey is one identity change, so
    it is one transaction.
M7  **Both RELEASE advisory locks are held across the retag and the rekey.**
    The retag mutates two release identities at once, and
    ``lib/destructive_release_service.py`` fences Beets mutation per release
    from OTHER processes (web routes, ``pipeline-cli destructive``). An
    operator Bad Rip resolving "the one album at the survivor" mid-``mbsync``
    could otherwise bind to the album we just retagged onto that id.
    Contention is a typed non-ready outcome, never a wait.

``canonical_release_fn`` and ``retag_fn`` are definition-time defaults on
``_process_beets_validation``: these tests INJECT replacements and never patch
the module binding, because patching does not replace a captured default.
``lib.beets.beets_validate`` is the one patched name — it is the allowlisted
harness-subprocess wrapper, not our logic.
"""

from __future__ import annotations

import contextlib
import copy
import logging
import os
import tempfile
import unittest
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from unittest.mock import patch

from lib.beets_db import AlbumInfo
from lib.beets_retag import (
    RETAG_AMBIGUOUS,
    RETAG_FAILED,
    RETAG_READY_OUTCOMES,
    BeetsRetagResult,
    MbsyncRun,
    RetagOutcome,
    retag_merged_album,
)
from lib.config import CratediggerConfig
from lib.download_validation import (
    MERGE_NO_REDIRECT,
    MERGE_NOT_APPLICABLE,
    MERGE_NOT_OWNED,
    MERGE_REKEY_REFUSED,
    MERGE_REKEYED,
    MERGE_RELEASE_LOCKED,
    MERGE_RETAG_NOT_READY,
    MERGE_SURVIVOR_NOT_OFFERED,
    _follow_merged_release,
    _process_beets_validation,
)
from lib.grab_list import GrabListEntry
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    release_id_to_lock_key,
)
from lib.quality import (
    CandidateSummary,
    HarnessTrackInfo,
    ValidationResult,
    VerifiedLosslessProof,
)
from lib.quality_evidence import backfill_current_evidence_from_album_info
from lib.release_identity import ReleaseIdentity
from lib.staged_album import StagedAlbum
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    handoff_automation_owner,
    make_ctx_with_fake_db,
    make_grab_list_entry,
    make_request_row,
)

# The live merge probed on 2026-08-06: request 346, DICE — "Midnight Zoo".
MERGED = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
SURVIVOR = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"
UNRELATED = "11111111-2222-3333-4444-555555555555"

OLD = ReleaseIdentity(source="musicbrainz", release_id=MERGED)
NEW = ReleaseIdentity(source="musicbrainz", release_id=SURVIVOR)

REQUEST_ID = 346


@contextlib.contextmanager
def _silence_logs() -> Iterator[None]:
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous)


def candidate(
    mbid: str,
    *,
    distance: float = 0.04,
    extra_tracks: int = 0,
) -> CandidateSummary:
    """One beets candidate as the harness serializes it."""
    return CandidateSummary(
        mbid=mbid,
        artist="DICE",
        album="Midnight Zoo",
        distance=distance,
        extra_tracks=[HarnessTrackInfo() for _ in range(extra_tracks)],
    )


def mbid_not_found_result(
    *candidates: CandidateSummary,
    target: str = MERGED,
) -> ValidationResult:
    """Exactly the result ``beets_validate`` returns for a merged-away id."""
    return ValidationResult(
        valid=False,
        distance=None,
        scenario="mbid_not_found",
        detail=f"Target MBID {target} not in candidates",
        mbid_found=False,
        target_mbid=target,
        candidate_count=len(candidates),
        candidates=list(candidates),
    )


class RecordingCanonical:
    """A recording merge-survivor lookup. Never a network call in tests."""

    def __init__(self, survivor: str | None) -> None:
        self.survivor = survivor
        self.calls: list[str] = []

    def __call__(self, release_id: str) -> str | None:
        self.calls.append(release_id)
        return self.survivor


@dataclass
class RetagObservation:
    """What the world looked like at the moment the retag was invoked."""

    old_identity: ReleaseIdentity
    new_identity: ReleaseIdentity
    stored_release_id: str | None
    #: Every ``(namespace, key)`` acquired when the retag started. M7's
    #: instrument: "the lock was taken at some point" is not the invariant —
    #: "the lock was HELD while Beets was mutated" is.
    advisory_locks_held: tuple[tuple[int, int], ...] = ()


class RecordingRetag:
    """A recording library retag that snapshots the request row it ran under.

    ``stored_release_id`` is the M1 instrument: the ordering defect is
    invisible in the end state (both orders finish with the row at the
    survivor), and visible only from inside the retag.
    """

    def __init__(
        self,
        db: FakePipelineDB,
        result: BeetsRetagResult,
        *,
        request_id: int = REQUEST_ID,
    ) -> None:
        self._db = db
        self._request_id = request_id
        self.result = result
        self.observations: list[RetagObservation] = []

    def __call__(
        self,
        cfg: CratediggerConfig,
        *,
        old_identity: ReleaseIdentity,
        new_identity: ReleaseIdentity,
    ) -> BeetsRetagResult:
        del cfg
        row = self._db.request(self._request_id)
        self.observations.append(RetagObservation(
            old_identity=old_identity,
            new_identity=new_identity,
            stored_release_id=(
                None if row is None else row.get("mb_release_id")
            ),
            advisory_locks_held=tuple(self._db.advisory_lock_calls),
        ))
        return self.result


def real_retag_over(
    beets: FakeBeetsDB,
    *,
    moves: bool = True,
) -> Callable[..., BeetsRetagResult]:
    """Compose the REAL ``retag_merged_album`` with a real fake library.

    The seam's guard and the retag's own decision are the two halves that must
    agree, so the DICE pin drives the production retag rather than a stub —
    ``mbsync`` is the only thing standing in, and it mutates the fake library
    exactly as the real command mutates the real one.
    """

    def run_mbsync(query: str) -> MbsyncRun:
        del query
        if moves:
            beets.set_album_ids_for_release(MERGED, [])
            beets.set_album_ids_for_release(SURVIVOR, [7])
        return MbsyncRun(returncode=0, stdout="", stderr="")

    def retag(
        cfg: CratediggerConfig,
        *,
        old_identity: ReleaseIdentity,
        new_identity: ReleaseIdentity,
    ) -> BeetsRetagResult:
        del cfg
        return retag_merged_album(
            beets,
            old_identity=old_identity,
            new_identity=new_identity,
            run_mbsync=run_mbsync,
        )

    return retag


class _MergeWorld:
    """One request mid-import: ``processing``, exact automation owner, staged."""

    def __init__(
        self,
        stack: contextlib.ExitStack,
        *,
        status_owned: bool = True,
        stored_release_id: str = MERGED,
    ) -> None:
        self.tmpdir = stack.enter_context(tempfile.TemporaryDirectory())
        with open(os.path.join(self.tmpdir, "01 - Track.mp3"), "wb") as handle:
            handle.write(b"audio")
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=REQUEST_ID,
            mb_release_id=stored_release_id,
            artist_name="DICE",
            album_title="Midnight Zoo",
        ))
        if status_owned:
            job = handoff_automation_owner(
                self.db,
                REQUEST_ID,
                state={
                    "filetype": "mp3",
                    "enqueued_at": "2026-08-11T00:00:00+00:00",
                    "current_path": self.tmpdir,
                    "files": [],
                },
                canonical_path=self.tmpdir,
            )
            self.import_job_id = job.id
        else:
            # The YouTube-rescue shape: a real import job exists, but the
            # request keeps its operator lifecycle state and no owner.
            self.import_job_id = 4242
        self.cfg = CratediggerConfig(
            beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
            beets_distance_threshold=0.15,
            beets_staging_dir=os.path.join(self.tmpdir, "staging"),
            slskd_download_dir=self.tmpdir,
            pipeline_db_enabled=True,
        )
        self.ctx = make_ctx_with_fake_db(self.db, cfg=self.cfg)
        self.album_data: GrabListEntry = make_grab_list_entry(
            album_id=REQUEST_ID,
            artist="DICE",
            title="Midnight Zoo",
            mb_release_id=stored_release_id,
            db_source="request",
            db_request_id=REQUEST_ID,
        )
        self.staged_album = StagedAlbum(
            current_path=self.tmpdir, request_id=REQUEST_ID,
        )

    def stored_release_id(self) -> str | None:
        row = self.db.request(REQUEST_ID)
        return None if row is None else row.get("mb_release_id")


class TestMergeRedirectBranches(unittest.TestCase):
    """Every branch of the merge-redirect helper, on the real production path."""

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.world = _MergeWorld(self.stack)

    def _follow(
        self,
        bv_result: ValidationResult,
        *,
        canonical: RecordingCanonical,
        retag: Callable[..., BeetsRetagResult],
    ):
        with _silence_logs():
            return _follow_merged_release(
                self.world.album_data,
                bv_result,
                self.world.ctx,
                import_job_id=self.world.import_job_id,
                canonical_release_fn=canonical,
                retag_fn=retag,
            )

    def test_dice_shape_retags_then_rekeys_then_revalidates(self) -> None:
        """M1/M5 — the live request-346 world, end to end."""
        bv_result = mbid_not_found_result(candidate(SURVIVOR, distance=0.02))
        canonical = RecordingCanonical(SURVIVOR)
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="retagged album 7",
        ))

        outcome = self._follow(bv_result, canonical=canonical, retag=retag)

        self.assertEqual(outcome.status, MERGE_REKEYED)
        self.assertEqual(outcome.survivor, SURVIVOR)
        self.assertEqual(canonical.calls, [MERGED])
        # M1: the row had NOT moved while the library was being retagged.
        self.assertEqual(len(retag.observations), 1)
        self.assertEqual(retag.observations[0].stored_release_id, MERGED)
        self.assertEqual(retag.observations[0].old_identity, OLD)
        self.assertEqual(retag.observations[0].new_identity, NEW)
        # And it moved afterwards, in both the row and the in-flight entry.
        self.assertEqual(self.world.stored_release_id(), SURVIVOR)
        self.assertEqual(self.world.album_data.mb_release_id, SURVIVOR)
        self.assertEqual(
            self.world.db.update_request_release_for_merge_calls,
            [(REQUEST_ID, MERGED, SURVIVOR, self.world.import_job_id)],
        )
        # M5: the survivor's own candidate re-derived the scenario.
        self.assertTrue(bv_result.valid)
        self.assertEqual(bv_result.scenario, "strong_match")
        self.assertEqual(bv_result.distance, 0.02)
        self.assertTrue(bv_result.mbid_found)
        self.assertEqual(bv_result.target_mbid, SURVIVOR)
        self.assertTrue(bv_result.candidates[0].is_target)

    def test_no_redirect_changes_nothing(self) -> None:
        """M2/M3 — the overwhelmingly common answer costs one lookup."""
        bv_result = mbid_not_found_result(candidate(UNRELATED))
        before = copy.deepcopy(bv_result)
        canonical = RecordingCanonical(None)
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ))

        outcome = self._follow(bv_result, canonical=canonical, retag=retag)

        self.assertEqual(outcome.status, MERGE_NO_REDIRECT)
        self.assertEqual(canonical.calls, [MERGED])
        self.assertEqual(retag.observations, [])
        self.assertEqual(bv_result.to_json(), before.to_json())
        self.assertEqual(self.world.stored_release_id(), MERGED)
        self.assertEqual(self.world.album_data.mb_release_id, MERGED)
        self.assertEqual(self.world.db.update_request_release_for_merge_calls, [])

    def test_survivor_not_among_candidates_keeps_the_rejection(self) -> None:
        """M3 — the merge is real, this download still is not the survivor."""
        bv_result = mbid_not_found_result(candidate(UNRELATED))
        before = copy.deepcopy(bv_result)
        canonical = RecordingCanonical(SURVIVOR)
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ))

        outcome = self._follow(bv_result, canonical=canonical, retag=retag)

        self.assertEqual(outcome.status, MERGE_SURVIVOR_NOT_OFFERED)
        self.assertEqual(outcome.survivor, SURVIVOR)
        self.assertEqual(retag.observations, [])
        self.assertEqual(bv_result.to_json(), before.to_json())
        self.assertEqual(self.world.stored_release_id(), MERGED)
        self.assertEqual(self.world.db.update_request_release_for_merge_calls, [])

    def test_a_non_ready_retag_never_moves_the_row(self) -> None:
        """M1/M3 — gate on READY membership, never on ``!= failed``."""
        non_ready: tuple[tuple[RetagOutcome, str], ...] = (
            (RETAG_FAILED, "mbsync exited 0, but the library did not move"),
            (RETAG_AMBIGUOUS, "library holds both sides of the merge"),
        )
        for outcome_name, detail in non_ready:
            with self.subTest(retag_outcome=outcome_name):
                self.assertNotIn(outcome_name, RETAG_READY_OUTCOMES)
                world = _MergeWorld(self.stack)
                bv_result = mbid_not_found_result(candidate(SURVIVOR))
                before = copy.deepcopy(bv_result)
                retag = RecordingRetag(world.db, BeetsRetagResult(
                    outcome=outcome_name, detail=detail,
                ))

                with _silence_logs():
                    result = _follow_merged_release(
                        world.album_data,
                        bv_result,
                        world.ctx,
                        import_job_id=world.import_job_id,
                        canonical_release_fn=RecordingCanonical(SURVIVOR),
                        retag_fn=retag,
                    )

                self.assertEqual(result.status, MERGE_RETAG_NOT_READY)
                self.assertIn(outcome_name, result.detail)
                self.assertEqual(len(retag.observations), 1)
                self.assertEqual(bv_result.to_json(), before.to_json())
                self.assertEqual(world.stored_release_id(), MERGED)
                self.assertEqual(world.album_data.mb_release_id, MERGED)
                self.assertEqual(
                    world.db.update_request_release_for_merge_calls, [],
                )
                # Still runnable: no marker whose only exit is a human.
                row = world.db.request(REQUEST_ID)
                assert row is not None
                self.assertEqual(row["status"], "processing")
                self.assertEqual(
                    row["active_automation_import_job_id"], world.import_job_id,
                )

    def test_a_survivor_another_request_holds_fails_closed(self) -> None:
        """M4 — never merge or delete a curated request row."""
        self.world.db.seed_request(make_request_row(
            id=999,
            mb_release_id=SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing)",
        ))
        bv_result = mbid_not_found_result(candidate(SURVIVOR))
        before = copy.deepcopy(bv_result)
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="retagged album 7",
        ))

        outcome = self._follow(
            bv_result,
            canonical=RecordingCanonical(SURVIVOR),
            retag=retag,
        )

        self.assertEqual(outcome.status, MERGE_REKEY_REFUSED)
        self.assertEqual(bv_result.to_json(), before.to_json())
        self.assertEqual(self.world.stored_release_id(), MERGED)
        self.assertEqual(self.world.album_data.mb_release_id, MERGED)
        other = self.world.db.request(999)
        assert other is not None
        self.assertEqual(other["mb_release_id"], SURVIVOR)

    def test_a_request_without_its_exact_owner_never_asks_the_mirror(self) -> None:
        """M2 — an unowned world cannot act on the answer, so it isn't sought."""
        world = _MergeWorld(self.stack, status_owned=False)
        canonical = RecordingCanonical(SURVIVOR)
        retag = RecordingRetag(world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ))
        bv_result = mbid_not_found_result(candidate(SURVIVOR))

        with _silence_logs():
            outcome = _follow_merged_release(
                world.album_data,
                bv_result,
                world.ctx,
                import_job_id=world.import_job_id,
                canonical_release_fn=canonical,
                retag_fn=retag,
            )

        self.assertEqual(outcome.status, MERGE_NOT_OWNED)
        self.assertEqual(canonical.calls, [])
        self.assertEqual(retag.observations, [])
        self.assertEqual(world.stored_release_id(), MERGED)

    def test_a_discogs_request_is_never_a_merge_candidate(self) -> None:
        """Discogs release ids have no redirect concept — and no adapter."""
        world = _MergeWorld(self.stack, stored_release_id="1870")
        canonical = RecordingCanonical(SURVIVOR)
        bv_result = mbid_not_found_result(candidate(SURVIVOR), target="1870")

        with _silence_logs():
            outcome = _follow_merged_release(
                world.album_data,
                bv_result,
                world.ctx,
                import_job_id=world.import_job_id,
                canonical_release_fn=canonical,
                retag_fn=RecordingRetag(world.db, BeetsRetagResult(
                    outcome="retagged", detail="should never run",
                )),
            )

        self.assertEqual(outcome.status, MERGE_NOT_APPLICABLE)
        self.assertEqual(canonical.calls, [])
        self.assertEqual(world.stored_release_id(), "1870")

    def test_a_redownload_without_a_request_row_is_never_rekeyed(self) -> None:
        world = _MergeWorld(self.stack)
        world.album_data.db_request_id = None
        canonical = RecordingCanonical(SURVIVOR)

        with _silence_logs():
            outcome = _follow_merged_release(
                world.album_data,
                mbid_not_found_result(candidate(SURVIVOR)),
                world.ctx,
                import_job_id=world.import_job_id,
                canonical_release_fn=canonical,
                retag_fn=RecordingRetag(world.db, BeetsRetagResult(
                    outcome="retagged", detail="should never run",
                )),
            )

        self.assertEqual(outcome.status, MERGE_NOT_APPLICABLE)
        self.assertEqual(canonical.calls, [])

    def test_an_extra_tracks_survivor_is_re_derived_not_forced_valid(self) -> None:
        """M5 — the shared derivation decides; the rekey does not assume valid."""
        bv_result = mbid_not_found_result(
            candidate(SURVIVOR, distance=0.02, extra_tracks=3),
        )
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="already_current", detail="library already holds survivor",
        ))

        outcome = self._follow(
            bv_result,
            canonical=RecordingCanonical(SURVIVOR),
            retag=retag,
        )

        self.assertEqual(outcome.status, MERGE_REKEYED)
        self.assertEqual(self.world.stored_release_id(), SURVIVOR)
        self.assertFalse(bv_result.valid)
        self.assertEqual(bv_result.scenario, "extra_tracks")
        self.assertEqual(bv_result.detail, "MB has 3 more tracks than local files")

    def test_a_resolver_that_hands_back_the_stored_id_changes_nothing(
        self,
    ) -> None:
        """M3 — fail closed on a non-answer, never delegate the check.

        ``canonical_release_id``'s contract already forbids returning the
        stored id, but this seam authorizes a retag of installed files and a
        rekey of the request; it re-checks rather than trusting. Found by the
        generated property.
        """
        bv_result = mbid_not_found_result(candidate(MERGED))
        before = copy.deepcopy(bv_result)
        canonical = RecordingCanonical(MERGED)
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ))

        outcome = self._follow(bv_result, canonical=canonical, retag=retag)

        self.assertEqual(outcome.status, MERGE_NO_REDIRECT)
        self.assertEqual(retag.observations, [])
        self.assertEqual(bv_result.to_json(), before.to_json())
        self.assertEqual(self.world.stored_release_id(), MERGED)
        self.assertEqual(self.world.db.update_request_release_for_merge_calls, [])

    def test_a_non_musicbrainz_survivor_changes_nothing(self) -> None:
        """M3 — no adapter between MusicBrainz and Discogs, in either direction."""
        bv_result = mbid_not_found_result(candidate("1870"))
        canonical = RecordingCanonical("1870")
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ))

        outcome = self._follow(bv_result, canonical=canonical, retag=retag)

        self.assertEqual(outcome.status, MERGE_NO_REDIRECT)
        self.assertEqual(retag.observations, [])
        self.assertEqual(self.world.stored_release_id(), MERGED)

    def test_a_distant_survivor_stays_rejected_after_the_rekey(self) -> None:
        """M5 — the threshold is the shared function's, not the seam's."""
        bv_result = mbid_not_found_result(candidate(SURVIVOR, distance=0.9))
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="retagged album 7",
        ))

        outcome = self._follow(
            bv_result,
            canonical=RecordingCanonical(SURVIVOR),
            retag=retag,
        )

        self.assertEqual(outcome.status, MERGE_REKEYED)
        self.assertFalse(bv_result.valid)
        self.assertEqual(bv_result.scenario, "high_distance")
        self.assertEqual(bv_result.distance, 0.9)


class TestMergeRedirectAtTheValidationSeam(unittest.TestCase):
    """The seam itself: which validations reach the mirror, and what changes."""

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.world = _MergeWorld(self.stack)

    def _validate(
        self,
        bv_result: ValidationResult,
        *,
        canonical: RecordingCanonical,
        retag: Callable[..., BeetsRetagResult],
    ) -> None:
        with (
            _silence_logs(),
            patch("lib.beets.beets_validate", return_value=bv_result),
        ):
            _process_beets_validation(
                self.world.album_data,
                self.world.staged_album,
                self.world.ctx,
                import_job_id=self.world.import_job_id,
                canonical_release_fn=canonical,
                retag_fn=retag,
            )

    def test_a_healthy_validation_never_touches_the_mirror(self) -> None:
        """M2 — the performance contract, asserted on the mirror itself."""
        canonical = RecordingCanonical(SURVIVOR)
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ))

        for scenario, valid in (
            ("strong_match", True),
            ("high_distance", False),
            ("extra_tracks", False),
            ("no_choose_match", False),
            ("validation_error", False),
        ):
            with self.subTest(scenario=scenario):
                self._validate(
                    ValidationResult(
                        valid=valid,
                        distance=0.04,
                        scenario=scenario,
                        target_mbid=MERGED,
                        candidates=[candidate(SURVIVOR)],
                    ),
                    canonical=canonical,
                    retag=retag,
                )

        self.assertEqual(canonical.calls, [])
        self.assertEqual(retag.observations, [])
        self.assertEqual(self.world.stored_release_id(), MERGED)

    def test_mbid_not_found_asks_the_mirror_exactly_once(self) -> None:
        """M2 — one lookup per failing validation, never more."""
        canonical = RecordingCanonical(None)

        self._validate(
            mbid_not_found_result(candidate(SURVIVOR)),
            canonical=canonical,
            retag=RecordingRetag(self.world.db, BeetsRetagResult(
                outcome="retagged", detail="should never run",
            )),
        )

        self.assertEqual(canonical.calls, [MERGED])

    def test_the_dice_shape_composes_the_real_retag_with_the_real_seam(
        self,
    ) -> None:
        """M1 — the real retag guard and the real seam over one fake library.

        The upgrade world: the album is ALREADY installed under the merged-away
        id, which is exactly why the ordering matters.
        """
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [7])
        beets.set_album_ids_for_release(SURVIVOR, [])
        bv_result = mbid_not_found_result(candidate(SURVIVOR, distance=0.03))

        self._validate(
            bv_result,
            canonical=RecordingCanonical(SURVIVOR),
            retag=real_retag_over(beets),
        )

        self.assertEqual(self.world.stored_release_id(), SURVIVOR)
        self.assertEqual(self.world.album_data.mb_release_id, SURVIVOR)
        self.assertTrue(bv_result.valid)
        self.assertEqual(bv_result.scenario, "strong_match")
        self.assertEqual(bv_result.distance, 0.03)
        # The library really moved: the real retag re-read it and agreed.
        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [])
        self.assertEqual(beets.get_all_album_ids_for_release(SURVIVOR), [7])

    def test_a_library_that_does_not_move_leaves_the_row_at_the_old_id(
        self,
    ) -> None:
        """M1/M3 — mbsync exits 0 and changes nothing: no rekey, no parking."""
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [7])
        beets.set_album_ids_for_release(SURVIVOR, [])
        bv_result = mbid_not_found_result(candidate(SURVIVOR))

        self._validate(
            bv_result,
            canonical=RecordingCanonical(SURVIVOR),
            retag=real_retag_over(beets, moves=False),
        )

        self.assertEqual(self.world.stored_release_id(), MERGED)
        self.assertEqual(self.world.album_data.mb_release_id, MERGED)
        self.assertFalse(bv_result.valid)
        self.assertEqual(bv_result.scenario, "mbid_not_found")
        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [7])


class TestRekeyedRequestKeepsItsEvidence(unittest.TestCase):
    """M6 — the identity change carries the request's evidence lineage.

    Composed rather than unit-scoped on purpose: the writer is the rekey and
    the consumer is the HAVE rebuild, and the defect lives between them. The
    REAL seam rekeys, and then the REAL
    ``backfill_current_evidence_from_album_info`` rebuilds the current row —
    the same call the importer's post-import refresh makes.
    """

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.world = _MergeWorld(self.stack)

    def _album_info(self) -> AlbumInfo:
        """The beets facts the post-import HAVE rebuild reads."""
        return AlbumInfo(
            album_id=7,
            track_count=1,
            min_bitrate_kbps=900,
            avg_bitrate_kbps=950,
            median_bitrate_kbps=940,
            is_cbr=False,
            album_path=self.world.tmpdir,
            format="FLAC",
        )

    def _rebuild_current_evidence(self, mb_release_id: str):
        return backfill_current_evidence_from_album_info(
            self.world.db,
            request_id=REQUEST_ID,
            mb_release_id=mb_release_id,
            album_info=self._album_info(),
        )

    def test_the_have_rebuild_after_a_rekey_keeps_the_proof(self) -> None:
        """The verified-lossless proof lock survives the merge."""
        seeded = backfill_current_evidence_from_album_info(
            self.world.db,
            request_id=REQUEST_ID,
            mb_release_id=MERGED,
            album_info=self._album_info(),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier="spectral_verified_lossless",
                detail="genuine",
            ),
        )
        self.assertEqual(seeded.status, "ready")

        bv_result = mbid_not_found_result(candidate(SURVIVOR, distance=0.02))
        with _silence_logs():
            outcome = _follow_merged_release(
                self.world.album_data,
                bv_result,
                self.world.ctx,
                import_job_id=self.world.import_job_id,
                canonical_release_fn=RecordingCanonical(SURVIVOR),
                retag_fn=RecordingRetag(self.world.db, BeetsRetagResult(
                    outcome="retagged", detail="retagged album 7",
                )),
            )
        self.assertEqual(outcome.status, MERGE_REKEYED)

        # The linked row followed the identity, so it is still attributable.
        current_id = self.world.db.get_request_current_evidence_id(REQUEST_ID)
        assert current_id is not None
        linked = self.world.db.load_album_quality_evidence_by_id(current_id)
        assert linked is not None
        self.assertEqual(linked.mb_release_id, SURVIVOR)

        rebuilt = self._rebuild_current_evidence(SURVIVOR)

        self.assertEqual(rebuilt.status, "ready")
        assert rebuilt.evidence is not None
        proof = rebuilt.evidence.verified_lossless_proof
        assert proof is not None, (
            "the rebuilt HAVE row lost its verified-lossless proof: the "
            "evidence was stranded at the merged-away release id"
        )
        self.assertEqual(proof.source, "flac")
        self.assertEqual(proof.classifier, "spectral_verified_lossless")
        self.assertEqual(proof.provenance, "carried")

    def test_a_refused_rekey_leaves_the_evidence_exactly_where_it_was(
        self,
    ) -> None:
        """M3/M6 — a non-ready world moves neither the row nor the evidence."""
        seeded = backfill_current_evidence_from_album_info(
            self.world.db,
            request_id=REQUEST_ID,
            mb_release_id=MERGED,
            album_info=self._album_info(),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier="spectral_verified_lossless",
                detail="genuine",
            ),
        )
        self.assertEqual(seeded.status, "ready")

        with _silence_logs():
            outcome = _follow_merged_release(
                self.world.album_data,
                mbid_not_found_result(candidate(SURVIVOR)),
                self.world.ctx,
                import_job_id=self.world.import_job_id,
                canonical_release_fn=RecordingCanonical(SURVIVOR),
                retag_fn=RecordingRetag(self.world.db, BeetsRetagResult(
                    outcome=RETAG_FAILED, detail="the library did not move",
                )),
            )

        self.assertEqual(outcome.status, MERGE_RETAG_NOT_READY)
        current_id = self.world.db.get_request_current_evidence_id(REQUEST_ID)
        assert current_id is not None
        linked = self.world.db.load_album_quality_evidence_by_id(current_id)
        assert linked is not None
        self.assertEqual(linked.mb_release_id, MERGED)
        self.assertEqual(self.world.stored_release_id(), MERGED)
        rebuilt = self._rebuild_current_evidence(MERGED)
        assert rebuilt.evidence is not None
        self.assertIsNotNone(rebuilt.evidence.verified_lossless_proof)


class TestMergeRetagHoldsBothReleaseLocks(unittest.TestCase):
    """M7 — the two-identity Beets mutation is fenced from other processes."""

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.world = _MergeWorld(self.stack)

    def _follow(self, bv_result: ValidationResult, retag: RecordingRetag):
        with _silence_logs():
            return _follow_merged_release(
                self.world.album_data,
                bv_result,
                self.world.ctx,
                import_job_id=self.world.import_job_id,
                canonical_release_fn=RecordingCanonical(SURVIVOR),
                retag_fn=retag,
            )

    def test_both_release_locks_are_held_while_the_library_is_retagged(
        self,
    ) -> None:
        retag = RecordingRetag(self.world.db, BeetsRetagResult(
            outcome="retagged", detail="retagged album 7",
        ))
        # The fixture's handoff already took IMPORT(request); the seam runs
        # inside it, so the two RELEASE acquires must be exactly what this
        # seam adds — IMPORT outer, RELEASE inner (docs/advisory-locks.md).
        locks_before = tuple(self.world.db.advisory_lock_calls)

        outcome = self._follow(
            mbid_not_found_result(candidate(SURVIVOR, distance=0.02)), retag,
        )

        self.assertEqual(outcome.status, MERGE_REKEYED)
        self.assertEqual(len(retag.observations), 1)
        self.assertEqual(
            retag.observations[0].advisory_locks_held,
            locks_before + tuple(
                (ADVISORY_LOCK_NAMESPACE_RELEASE, key)
                for key in sorted({
                    release_id_to_lock_key(MERGED),
                    release_id_to_lock_key(SURVIVOR),
                })
            ),
            "the retag mutates both release identities and must hold both "
            "RELEASE locks, in a deterministic order, while it does",
        )

    def test_contention_on_either_identity_keeps_todays_rejection(self) -> None:
        """A held lock is a typed non-ready outcome, not a wait, not a retag."""
        for contended in (MERGED, SURVIVOR):
            with self.subTest(contended=contended):
                world = _MergeWorld(self.stack)
                blocked_key = release_id_to_lock_key(contended)
                world.db.set_advisory_lock_result(
                    lambda namespace, key, blocked_key=blocked_key: not (
                        namespace == ADVISORY_LOCK_NAMESPACE_RELEASE
                        and key == blocked_key
                    )
                )
                bv_result = mbid_not_found_result(candidate(SURVIVOR))
                before = copy.deepcopy(bv_result)
                retag = RecordingRetag(world.db, BeetsRetagResult(
                    outcome="retagged", detail="should never run",
                ))

                with _silence_logs():
                    outcome = _follow_merged_release(
                        world.album_data,
                        bv_result,
                        world.ctx,
                        import_job_id=world.import_job_id,
                        canonical_release_fn=RecordingCanonical(SURVIVOR),
                        retag_fn=retag,
                    )

                self.assertEqual(outcome.status, MERGE_RELEASE_LOCKED)
                self.assertEqual(outcome.survivor, SURVIVOR)
                self.assertEqual(retag.observations, [])
                self.assertEqual(
                    world.db.update_request_release_for_merge_calls, [],
                )
                self.assertEqual(bv_result.to_json(), before.to_json())
                self.assertEqual(world.stored_release_id(), MERGED)
                self.assertEqual(world.album_data.mb_release_id, MERGED)
                # Still runnable: nothing parked, next cycle re-derives.
                row = world.db.request(REQUEST_ID)
                assert row is not None
                self.assertEqual(row["status"], "processing")
                self.assertEqual(
                    row["active_automation_import_job_id"],
                    world.import_job_id,
                )


class TestCanonicalLookupStartupWiring(unittest.TestCase):
    """``lib.mb_canonical`` starts inert and an unwired process looks healthy.

    It would report "no redirect" forever with no error anywhere, so the
    wiring owes a test of both halves: that the helper really configures the
    operator's mirror, and that the importer's ``main`` really calls it. The
    importer is the ONE process that reaches the merge seam — the main loop
    enqueues automation jobs and this worker drains them
    (``lib.download.poll_active_downloads`` →
    ``scripts.importer.execute_automation_import_job`` →
    ``lib.download._run_completed_processing``).
    """

    def setUp(self) -> None:
        from lib.mb_canonical import (
            configure_canonical_base,
            configured_canonical_base,
        )

        previous = configured_canonical_base()
        self.addCleanup(configure_canonical_base, previous)

    def _cfg(self, api_base: str) -> CratediggerConfig:
        return CratediggerConfig(
            musicbrainz_api_base=api_base,
            pipeline_db_enabled=True,
        )

    def test_a_configured_mirror_becomes_the_ws2_base(self) -> None:
        from lib.mb_canonical import configured_canonical_base
        from scripts.importer import configure_canonical_release_lookup

        configure_canonical_release_lookup(self._cfg("http://192.168.1.43:5000"))

        self.assertEqual(
            configured_canonical_base(), "http://192.168.1.43:5000/ws/2",
        )

    def test_a_blank_mirror_leaves_resolution_inert_and_warns(self) -> None:
        from lib.mb_canonical import (
            configure_canonical_base,
            configured_canonical_base,
        )
        from scripts.importer import configure_canonical_release_lookup

        configure_canonical_base("http://stale/ws/2")

        with self.assertLogs("cratedigger-importer", level="WARNING") as logs:
            configure_canonical_release_lookup(self._cfg("   "))

        self.assertIsNone(configured_canonical_base())
        self.assertIn("merge survivors", "\n".join(logs.output))

    def test_the_importer_entrypoint_actually_wires_it(self) -> None:
        """Would fail if the one wiring call were dropped from ``main``."""
        self.assertEqual(
            _startup_wiring_calls(_importer_source()),
            ["configure_canonical_release_lookup"],
        )

    def test_a_main_without_the_wiring_call_is_detected(self) -> None:
        """Known-bad self-test for the wiring check itself."""
        planted = _importer_source().replace(
            "    configure_canonical_release_lookup(cfg)\n", "", 1,
        )

        self.assertEqual(_startup_wiring_calls(planted), [])


def _importer_source() -> str:
    import scripts.importer

    assert scripts.importer.__file__ is not None
    with open(scripts.importer.__file__, encoding="utf-8") as handle:
        return handle.read()


def _startup_wiring_calls(source: str) -> list[str]:
    """Names called directly from ``scripts/importer.py::main``'s own body.

    A deliberately bounded syntactic check over ONE named function, not a
    semantic scanner: it answers "does ``main`` call this" and nothing else.
    """
    import ast

    tree = ast.parse(source)
    main = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    )
    return [
        node.func.id
        for node in ast.walk(main)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "configure_canonical_release_lookup"
    ]


if __name__ == "__main__":
    unittest.main()
