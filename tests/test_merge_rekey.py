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
M8  **The library and the request never disagree about which release this
    is.** Both of the rekey write's ``UniqueViolation`` refusals are plain
    reads, so they are asked BEFORE the library is retagged. Retagging first
    and discovering the refusal afterwards leaves the installed album at the
    survivor and the request at the merged-away id — a divergence nothing
    re-derives, because the collision that refused the write is still there
    on the next attempt, which this same pre-check refuses before the library
    is read at all.
M9  **A merge outcome no retry can clear is audited, never silent.** Two
    qualify, and each writes one durable ``download_log`` row (invariant 11's
    Recents audit evidence, not a log line that is gone at the next journal
    rotation). A survivor that is already occupied stays occupied until an
    operator resolves it, and the force lane carries no rejection of its own
    to explain it — it imports despite the verdict and then reports a bare
    ``mbid_missing`` from ``import_one.py``, attempt after attempt. And no
    lock covers "another request acquires this release id", so when the
    pre-check loses that race the split state owes the same evidence, and the
    force lane must not go on to launch Beets at the id whose library album
    it just moved away. One row per execution that reaches the branch,
    deliberately not deduplicated: an execution is an operator force action
    or a completed download, each of which already writes its own row.
M10 **``library_moved`` means THIS execution moved the library.** The
    discriminator between "we created a split" and "we found the world as it
    was". ``already_current`` and ``not_held`` are READY outcomes that moved
    nothing, so a rekey refused after one of them must not claim a retag that
    never ran, and must not refuse a force launch over a divergence this
    execution did not create.

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
    RETAG_ALREADY_CURRENT,
    RETAG_AMBIGUOUS,
    RETAG_FAILED,
    RETAG_NOT_HELD,
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
    MERGE_REKEY_BLOCKED,
    MERGE_REKEY_REFUSED,
    MERGE_REKEYED,
    MERGE_RELEASE_LOCKED,
    MERGE_RETAG_NOT_READY,
    MERGE_SURVIVOR_NOT_OFFERED,
    MergeRekeyOutcome,
    _follow_merged_release,
    _process_beets_validation,
    merge_rekey_blocked_audit_message,
    merge_rekey_claim_holds,
    split_identity_audit_message,
)
from lib.grab_list import GrabListEntry
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
    automation_import_payload,
    force_import_payload,
    youtube_import_payload,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    MergeRekeyCollision,
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
    make_album_quality_evidence,
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


def follow_merge(
    world: _MergeWorld,
    bv_result: ValidationResult,
    *,
    canonical: Callable[[str], str | None],
    retag: Callable[..., BeetsRetagResult],
) -> MergeRekeyOutcome:
    """Drive the production merge seam over ``world``. Pure delegation.

    The helper builds the seam's arguments the way both production callers do
    and nothing else — in particular it does NOT apply the survivor to the
    in-flight entry, because since #1080 the seam reports the survivor and the
    CALLER applies it. That application is pinned through the real caller in
    ``TestMergeRedirectAtTheValidationSeam``.
    """
    with _silence_logs():
        return _follow_merged_release(
            bv_result,
            db=world.db,
            cfg=world.cfg,
            request_id=world.album_data.db_request_id,
            stored_release_id=world.album_data.mb_release_id,
            import_job_id=world.import_job_id,
            distance_threshold=world.cfg.beets_distance_threshold,
            canonical_release_fn=canonical,
            retag_fn=retag,
        )


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
            return follow_merge(
                self.world, bv_result, canonical=canonical, retag=retag,
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
        # And it moved afterwards. The seam moves the ROW and reports the
        # survivor; since #1080 it never reaches into a caller's in-flight
        # state, because it now has two callers with two different ones.
        # ``TestMergeRedirectAtTheValidationSeam`` pins the automation
        # caller applying it, and ``tests/test_force_import_merge_redirect.py``
        # pins the force caller applying it.
        self.assertEqual(self.world.stored_release_id(), SURVIVOR)
        self.assertEqual(self.world.album_data.mb_release_id, MERGED)
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
                    result = follow_merge(
                        world, bv_result,
                        canonical=RecordingCanonical(SURVIVOR),
                        retag=retag,
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

    def test_a_survivor_another_request_holds_never_touches_the_library(
        self,
    ) -> None:
        """M4/M8 — the refusal is known BEFORE the library is mutated.

        The reproduced #1080 world: 84 artist+album groups in the live DB hold
        more than one non-``replaced`` request (invariant 5 working as
        designed), and that is exactly the population an MB merge collides in.
        The REAL retag runs over a real fake library, so "the library was not
        touched" is asserted on the library, not on a stub's call count.
        """
        self.world.db.seed_request(make_request_row(
            id=999,
            mb_release_id=SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing)",
        ))
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [7])
        beets.set_album_ids_for_release(SURVIVOR, [])
        bv_result = mbid_not_found_result(candidate(SURVIVOR))
        before = copy.deepcopy(bv_result)

        outcome = self._follow(
            bv_result,
            canonical=RecordingCanonical(SURVIVOR),
            retag=real_retag_over(beets),
        )

        self.assertEqual(outcome.status, MERGE_REKEY_BLOCKED)
        self.assertFalse(outcome.library_moved)
        self.assertFalse(outcome.split_identity)
        self.assertIn("999", outcome.detail)
        # M8: the installed album is exactly where it was — ``mbsync`` never
        # ran, so there is no split identity to repair.
        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [7])
        self.assertEqual(beets.get_all_album_ids_for_release(SURVIVOR), [])
        # M9: but the operator still owes a decision no retry can make for
        # them, so it is recorded durably. Both the expected sentence and the
        # collision fragment inside it come from their PRODUCERS
        # (test-fidelity Rule C), never from a hand-typed literal.
        expected = merge_rekey_blocked_audit_message(
            old_release_id=MERGED,
            new_release_id=SURVIVOR,
            collision_detail=MergeRekeyCollision(rival_request_id=999).detail(),
        )
        self.assertEqual(len(self.world.db.download_logs), 1)
        audit = self.world.db.download_logs[0]
        self.assertEqual(audit.request_id, REQUEST_ID)
        self.assertEqual(audit.outcome, "failed")
        self.assertEqual(audit.error_message, expected)
        self.assertEqual(outcome.detail, expected)
        self.assertEqual(bv_result.to_json(), before.to_json())
        self.assertEqual(self.world.stored_release_id(), MERGED)
        self.assertEqual(self.world.album_data.mb_release_id, MERGED)
        self.assertEqual(self.world.db.update_request_release_for_merge_calls, [])
        other = self.world.db.request(999)
        assert other is not None
        self.assertEqual(other["mb_release_id"], SURVIVOR)
        # Still runnable: the operator decides, the pipeline keeps moving.
        row = self.world.db.request(REQUEST_ID)
        assert row is not None
        self.assertEqual(row["status"], "processing")

    def test_an_evidence_collision_at_the_survivor_blocks_before_the_retag(
        self,
    ) -> None:
        """M8 — the second documented refusal cause is a read too.

        ``UNIQUE (mb_release_id, snapshot_fingerprint)``: the same bytes
        already measured at the survivor. The write refuses, so the library
        must not have moved by the time it does.
        """
        for release_id in (MERGED, SURVIVOR):
            self.world.db.upsert_album_quality_evidence(
                make_album_quality_evidence(
                    mb_release_id=release_id,
                    source_path=f"/library/{release_id}",
                ),
            )
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [7])
        beets.set_album_ids_for_release(SURVIVOR, [])

        outcome = self._follow(
            mbid_not_found_result(candidate(SURVIVOR)),
            canonical=RecordingCanonical(SURVIVOR),
            retag=real_retag_over(beets),
        )

        self.assertEqual(outcome.status, MERGE_REKEY_BLOCKED)
        self.assertIn("evidence already exists", outcome.detail)
        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [7])
        self.assertEqual(beets.get_all_album_ids_for_release(SURVIVOR), [])
        self.assertEqual(self.world.stored_release_id(), MERGED)
        self.assertEqual(self.world.db.update_request_release_for_merge_calls, [])
        # M9 — the other blocked cause is audited exactly the same way, and
        # names the fingerprint rather than a rival request.
        self.assertEqual(len(self.world.db.download_logs), 1)
        audit = self.world.db.download_logs[0]
        self.assertEqual(audit.outcome, "failed")
        self.assertEqual(audit.error_message, outcome.detail)
        self.assertIn("evidence already exists", audit.error_message or "")

    def test_a_survivor_claimed_during_the_retag_records_the_split(self) -> None:
        """M9 — the residual race is audited, never silent.

        No lock covers "another request acquires this release id", so the
        pre-check narrows the window and cannot close it. When it loses, the
        library HAS moved and the request has not — the one merge outcome
        nothing re-derives — so it owes durable Recents evidence rather than a
        log line that is gone at the next journal rotation.
        """
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [7])
        beets.set_album_ids_for_release(SURVIVOR, [])
        real_retag = real_retag_over(beets)
        observed: list[BeetsRetagResult] = []

        def retag_and_lose_the_race(
            cfg: CratediggerConfig,
            *,
            old_identity: ReleaseIdentity,
            new_identity: ReleaseIdentity,
        ) -> BeetsRetagResult:
            # The rival appears while ``mbsync`` is running — the exact window
            # the pre-check cannot cover.
            self.world.db.seed_request(make_request_row(
                id=999,
                mb_release_id=SURVIVOR,
                artist_name="DICE",
                album_title="Midnight Zoo (other pressing)",
            ))
            result = real_retag(
                cfg, old_identity=old_identity, new_identity=new_identity,
            )
            observed.append(result)
            return result

        bv_result = mbid_not_found_result(candidate(SURVIVOR))
        before = copy.deepcopy(bv_result)

        outcome = self._follow(
            bv_result,
            canonical=RecordingCanonical(SURVIVOR),
            retag=retag_and_lose_the_race,
        )

        self.assertEqual(outcome.status, MERGE_REKEY_REFUSED)
        self.assertTrue(outcome.library_moved)
        self.assertTrue(outcome.split_identity)
        # The world really is split: the album moved, the request did not.
        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [])
        self.assertEqual(beets.get_all_album_ids_for_release(SURVIVOR), [7])
        self.assertEqual(self.world.stored_release_id(), MERGED)
        self.assertEqual(bv_result.to_json(), before.to_json())
        # And the operator can find it. The expected copy comes from the
        # PRODUCER, and its retag detail from the real retag that ran
        # (test-fidelity Rule C), never from a hand-typed literal.
        self.assertEqual(len(observed), 1)
        expected = split_identity_audit_message(
            old_release_id=MERGED,
            new_release_id=SURVIVOR,
            retag_detail=observed[0].detail,
        )
        self.assertEqual(len(self.world.db.download_logs), 1)
        audit = self.world.db.download_logs[0]
        self.assertEqual(audit.request_id, REQUEST_ID)
        self.assertEqual(audit.outcome, "failed")
        self.assertEqual(audit.error_message, expected)
        self.assertEqual(outcome.detail, expected)
        # Still runnable — the REQUEST is not parked by the split (the
        # library is what diverged, and that is what the audit is for).
        row = self.world.db.request(REQUEST_ID)
        assert row is not None
        self.assertEqual(row["status"], "processing")
        self.assertEqual(
            row["active_automation_import_job_id"], self.world.import_job_id,
        )

    def test_a_ready_but_unmoved_library_never_claims_a_retag(self) -> None:
        """M10 — a refusal only ever asserts the move THIS execution made.

        ``already_current`` and ``not_held`` are READY outcomes that moved
        nothing: the first found the album already at the survivor, the
        second found no album at all. Losing the same race after one of them
        is a refusal, not a split — the seam changed nothing about the
        library. Widening ``library_moved`` to "the retag was ready" would
        write the split sentence ("the installed album was retagged onto the
        survivor") about a retag that never ran, and would refuse a force
        launch over a divergence this execution did not create.
        """
        unmoved: tuple[tuple[RetagOutcome, str], ...] = (
            (RETAG_ALREADY_CURRENT, "library already holds the survivor"),
            (RETAG_NOT_HELD, "no album is filed under the merged-away id"),
        )
        for retag_outcome, retag_detail in unmoved:
            with self.subTest(retag_outcome=retag_outcome):
                world = _MergeWorld(self.stack)

                def retag_and_lose_the_race(
                    cfg: CratediggerConfig,
                    *,
                    old_identity: ReleaseIdentity,
                    new_identity: ReleaseIdentity,
                    _world: _MergeWorld = world,
                    _outcome: RetagOutcome = retag_outcome,
                    _detail: str = retag_detail,
                ) -> BeetsRetagResult:
                    del cfg, old_identity, new_identity
                    # The rival appears in the same window the split pin
                    # uses — but here the library was never moved.
                    _world.db.seed_request(make_request_row(
                        id=999,
                        mb_release_id=SURVIVOR,
                        artist_name="DICE",
                        album_title="Midnight Zoo (other pressing)",
                    ))
                    return BeetsRetagResult(
                        outcome=_outcome, detail=_detail,
                    )

                self.assertIn(retag_outcome, RETAG_READY_OUTCOMES)
                outcome = follow_merge(
                    world,
                    mbid_not_found_result(candidate(SURVIVOR)),
                    canonical=RecordingCanonical(SURVIVOR),
                    retag=retag_and_lose_the_race,
                )

                self.assertEqual(outcome.status, MERGE_REKEY_REFUSED)
                # The consequences first, because they are what a widened
                # discriminator actually costs the operator. No audit row at
                # all: nothing here is stuck — the library is exactly where
                # the seam found it, and the next attempt is refused at the
                # pre-check, which audits its own reason.
                self.assertEqual(world.db.download_logs, [])
                # And the refusal does not claim a move. The forbidden
                # sentence comes from the split PRODUCER (Rule C), so this
                # asserts the exact copy a widened discriminator would emit.
                self.assertNotEqual(
                    outcome.detail,
                    split_identity_audit_message(
                        old_release_id=MERGED,
                        new_release_id=SURVIVOR,
                        retag_detail=retag_detail,
                    ),
                )
                # The force lane's consequence: nothing to refuse a launch
                # over, because this execution created no divergence.
                self.assertFalse(outcome.split_identity)
                self.assertFalse(outcome.library_moved)
                self.assertEqual(world.stored_release_id(), MERGED)

    def test_every_blocked_attempt_records_its_own_audit_row(self) -> None:
        """M9 — the blocked audit is per execution, not deduplicated.

        A blocked world persists across every retry, so the choice is
        explicit: one row per execution that reaches the branch — one per
        operator force action, one per completed-download validation. Each of
        those already writes its own ``download_log`` row, so the audit trail
        stays proportional to the work attempted rather than to elapsed time,
        and a second force attempt is never silent. Deduplicating would need
        a read-before-write plus a staleness policy to answer "is this the
        same collision?", and would hide exactly the repetition the operator
        needs to see.
        """
        self.world.db.seed_request(make_request_row(
            id=999,
            mb_release_id=SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing)",
        ))
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [7])
        beets.set_album_ids_for_release(SURVIVOR, [])

        outcomes = [
            self._follow(
                mbid_not_found_result(candidate(SURVIVOR)),
                canonical=RecordingCanonical(SURVIVOR),
                retag=real_retag_over(beets),
            )
            for _ in range(2)
        ]

        self.assertEqual(
            [outcome.status for outcome in outcomes],
            [MERGE_REKEY_BLOCKED, MERGE_REKEY_BLOCKED],
        )
        expected = merge_rekey_blocked_audit_message(
            old_release_id=MERGED,
            new_release_id=SURVIVOR,
            collision_detail=MergeRekeyCollision(rival_request_id=999).detail(),
        )
        self.assertEqual(len(self.world.db.download_logs), 2)
        for audit in self.world.db.download_logs:
            self.assertEqual(audit.request_id, REQUEST_ID)
            self.assertEqual(audit.outcome, "failed")
            self.assertEqual(audit.error_message, expected)
        # And the library is still untouched after both attempts.
        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [7])

    def test_a_request_without_its_exact_owner_never_asks_the_mirror(self) -> None:
        """M2 — an unowned world cannot act on the answer, so it isn't sought."""
        world = _MergeWorld(self.stack, status_owned=False)
        canonical = RecordingCanonical(SURVIVOR)
        retag = RecordingRetag(world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ))
        bv_result = mbid_not_found_result(candidate(SURVIVOR))

        with _silence_logs():
            outcome = follow_merge(
                world, bv_result, canonical=canonical, retag=retag,
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
            outcome = follow_merge(
                world, bv_result, canonical=canonical,
                retag=RecordingRetag(world.db, BeetsRetagResult(
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
            outcome = follow_merge(
                world, mbid_not_found_result(candidate(SURVIVOR)),
                canonical=canonical,
                retag=RecordingRetag(world.db, BeetsRetagResult(
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


class TestMergeRekeyClaimHolds(unittest.TestCase):
    """The pure gate that stops an unclaimed world touching the library.

    ``merge_rekey_claim_holds`` mirrors production's two import claims term
    for term, and it is the reason a YouTube rescue or a stale claim never
    spends a mirror lookup or retags the shared Beets library. The seam tests
    above it reach a handful of its rows; this is the whole table, asserted on
    the decision itself.

    Job rows are built through ``ImportJob.from_row`` — the production decoder
    — so a payload or field-shape change fails here rather than passing
    against a hand-made object production could never produce.
    """

    CASES: tuple[tuple[str, str, str, str, int | None, bool], ...] = (
        # (description, job_type, job_status, row status, row owner, expected)
        # --- automation: the pointer IS ownership (invariant 10) ---
        ("automation owner attached", IMPORT_JOB_AUTOMATION, "running",
         "processing", 1, True),
        ("automation pointer names another job", IMPORT_JOB_AUTOMATION,
         "running", "processing", 99, False),
        ("automation with no pointer at all", IMPORT_JOB_AUTOMATION, "running",
         "processing", None, False),
        ("automation on a non-processing row", IMPORT_JOB_AUTOMATION,
         "running", "wanted", None, False),
        ("automation on a replaced row", IMPORT_JOB_AUTOMATION, "running",
         "replaced", None, False),
        # The automation arm has no job-status term in the SQL either: the
        # request's pointer is the claim, so this mirrors it exactly.
        ("automation pointer outlives the job status",
         IMPORT_JOB_AUTOMATION, "completed", "processing", 1, True),
        # --- force: a running job on an unowned, non-frozen row ---
        ("force claim on a wanted row", IMPORT_JOB_FORCE, "running",
         "wanted", None, True),
        ("force claim on an imported row", IMPORT_JOB_FORCE, "running",
         "imported", None, True),
        ("force claim on an unsearchable row", IMPORT_JOB_FORCE, "running",
         "unsearchable", None, True),
        ("force claim on a downloading row", IMPORT_JOB_FORCE, "running",
         "downloading", None, True),
        ("force job still queued", IMPORT_JOB_FORCE, "queued", "wanted",
         None, False),
        ("force job already completed", IMPORT_JOB_FORCE, "completed",
         "wanted", None, False),
        ("force job failed", IMPORT_JOB_FORCE, "failed", "wanted",
         None, False),
        ("force against a processing row", IMPORT_JOB_FORCE, "running",
         "processing", 1, False),
        ("force against a frozen replaced row", IMPORT_JOB_FORCE, "running",
         "replaced", None, False),
        # Migration 066's owner-equivalence CHECK makes an owner pointer on a
        # non-processing row impossible in the live DB. The term is still
        # asserted because this function mirrors the SQL conjunction, and a
        # dropped term must fail somewhere.
        ("force against a row with an owner attached", IMPORT_JOB_FORCE,
         "running", "wanted", 1, False),
        # --- everything else holds neither claim ---
        ("youtube rescue", IMPORT_JOB_YOUTUBE, "running", "wanted",
         None, False),
        ("youtube rescue on a processing row", IMPORT_JOB_YOUTUBE, "running",
         "processing", 1, False),
    )

    def _job(self, job_type: str, status: str) -> ImportJob:
        payloads = {
            IMPORT_JOB_AUTOMATION: automation_import_payload(),
            IMPORT_JOB_FORCE: force_import_payload(
                download_log_id=5, failed_path="/quarantine/dice",
            ),
            IMPORT_JOB_YOUTUBE: youtube_import_payload(
                staged_path="/Incoming/auto-import/dice",
                request_id=REQUEST_ID,
                browse_id="MPREb_dice",
                download_log_id=5,
            ),
        }
        return ImportJob.from_row({
            "id": 1,
            "job_type": job_type,
            "status": status,
            "request_id": REQUEST_ID,
            "dedupe_key": None,
            "payload": payloads[job_type],
            "result": None,
            "message": None,
            "error": None,
            "attempts": 0,
            "worker_id": None,
            "created_at": None,
            "updated_at": None,
            "started_at": None,
            "heartbeat_at": None,
            "completed_at": None,
        })

    def test_every_claim_world(self) -> None:
        for desc, job_type, job_status, status, owner, expected in self.CASES:
            with self.subTest(case=desc):
                self.assertEqual(
                    merge_rekey_claim_holds(
                        {
                            "status": status,
                            "active_automation_import_job_id": owner,
                        },
                        self._job(job_type, job_status),
                    ),
                    expected,
                )

    def test_the_two_arms_cover_disjoint_worlds(self) -> None:
        """No world satisfies both claims — they are mutually exclusive.

        The automation arm requires ``processing``; the force arm excludes it.
        That is what makes migration 066's reservation of the processing owner
        pointer for one active automation job safe to rely on here.
        """
        for status in ("processing", "wanted", "imported", "replaced"):
            for owner in (None, 1):
                with self.subTest(status=status, owner=owner):
                    row = {
                        "status": status,
                        "active_automation_import_job_id": owner,
                    }
                    self.assertFalse(
                        merge_rekey_claim_holds(
                            row, self._job(IMPORT_JOB_AUTOMATION, "running"),
                        )
                        and merge_rekey_claim_holds(
                            row, self._job(IMPORT_JOB_FORCE, "running"),
                        ),
                    )


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
            outcome = follow_merge(
                self.world, bv_result,
                canonical=RecordingCanonical(SURVIVOR),
                retag=RecordingRetag(self.world.db, BeetsRetagResult(
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
            outcome = follow_merge(
                self.world, mbid_not_found_result(candidate(SURVIVOR)),
                canonical=RecordingCanonical(SURVIVOR),
                retag=RecordingRetag(self.world.db, BeetsRetagResult(
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
            return follow_merge(
                self.world, bv_result,
                canonical=RecordingCanonical(SURVIVOR), retag=retag,
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
                    outcome = follow_merge(
                        world, bv_result,
                        canonical=RecordingCanonical(SURVIVOR), retag=retag,
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
