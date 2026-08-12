"""Force import follows a MusicBrainz merge, like every other import (#1080).

#1059 wired merge-redirect following at the validation seam. Force import
never reached that seam: it went straight to ``dispatch_import_core`` and met
the merged-away release at the OTHER comparison site,
``harness/import_one.py::_find_target_candidate``, which has no redirect
concept. Live proof, request 346 (DICE — "Midnight Zoo", MusicBrainz merged
``6b209cc5-…`` into ``9b59f78b-…``), same request, two lanes, two rejections:

    download_log 39802 | rejected | mbid_not_found   (automation lane)
    download_log 39846 | rejected | mbid_missing     (force lane, post-deploy)

The invariants pinned here — the generated sibling
``TestForceAndAutomationAgreeOnTheMerge`` in
``tests/test_merge_rekey_generated.py`` patrols the world space around them:

F1  **A force import of a merged-away release follows the redirect**: the
    library is retagged, the request is rekeyed, and the SURVIVOR is the
    release id handed to ``import_one.py``. This is #1080's whole point, and
    it asserts the decided consequence — the id the Beets child is launched
    with — not an intermediate field.
F2  **Force and automation reach the exact-release comparison through the SAME
    call, differing in one argument: the distance threshold.** This is the
    anti-divergence guard. If a future change re-forks the lanes — force
    stopping at ``dispatch_import_core``, or growing its own copy of the
    comparison — this fails.
F3  **The validation result never becomes a force verdict.** Force import
    exists to import DESPITE the validation verdict; nothing about routing it
    through validation may change what force permits. A validation the
    automation lane rejects (``extra_tracks``, ``no_choose_match``,
    ``validation_error``) still reaches the Beets launch under force, exactly
    as it did before #1080.
F4  **No claim, no rekey.** A force job that is not ``running``, or a request
    an automation job owns, retags nothing and moves nothing — the mirror is
    not even asked.
F5  **Force never launches Beets at a release its own execution just moved
    the library away from.** When the survivor is claimed inside the race the
    pre-check cannot cover, the installed album ends up at the survivor and
    the request does not follow. Continuing would hand the operator the
    pre-#1080 ``mbid_missing`` while their library had silently moved, so the
    launch is refused and the split is recorded durably. Scoped to the split
    THIS execution created: one an earlier execution left behind is refused
    at the occupancy pre-check before the library is read, so force proceeds
    and F7 is what the operator gets.
F6  **The comparison seam runs before candidate evidence is loaded.** A rekey
    moves the request's evidence rows onto the survivor in the same
    transaction, so evidence loaded first pins the pre-rekey identity. Pinned
    at the action file the Beets child is actually handed.
F7  **A merge that only an operator can resolve leaves durable evidence on
    every force attempt.** An occupied survivor refuses the rekey before the
    library is touched, and no retry clears it. Force imports DESPITE the
    verdict, so without an audit the operator gets a bare ``mbid_missing``
    from ``import_one.py`` — the exact pre-#1080 symptom — attempt after
    attempt, with nothing naming the merge, the survivor or the collision.

``lib.beets.beets_validate`` is the one patched name: it is the allowlisted
harness-subprocess wrapper, not our logic. ``canonical_release_fn`` and
``retag_fn`` are INJECTED through ``dispatch_import_from_db``'s kwargs, never
patched, because patching does not replace a captured default.
"""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
import unittest
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from unittest.mock import patch

from lib.beets import FORCE_IMPORT_DISTANCE_THRESHOLD
from lib.beets_retag import BeetsRetagResult
from lib.config import CratediggerConfig
from lib.dispatch import dispatch_import_from_db
from lib.dispatch.types import ImportOneRun, ImportOneRunner
from lib.download_validation import (
    merge_rekey_blocked_audit_message,
    split_identity_audit_message,
)
from lib.import_execution import CancellationToken
from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload
from lib.pipeline_db import MergeRekeyCollision
from lib.quality import (
    AudioQualityMeasurement,
    V0ProbeEvidence,
    ValidationResult,
)
from lib.quality_evidence import snapshot_audio_files
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    patch_dispatch_externals,
)
from tests.test_merge_rekey import (
    MERGED,
    SURVIVOR,
    UNRELATED,
    RecordingCanonical,
    RecordingRetag,
    candidate,
    mbid_not_found_result,
    real_retag_over,
)

REQUEST_ID = 346
HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


@contextlib.contextmanager
def _silence_logs() -> Iterator[None]:
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous)


def _beets_db_holding_nothing():
    """A Beets authority that answers "not installed" for every identity.

    The installed-library read is an external edge; every world here is about
    identity resolution, so the library holds nothing and the evidence
    pipeline routes through ``import_no_exist``.
    """
    from unittest.mock import MagicMock

    from lib.beets_db import CurrentBeetsMissing

    instance = MagicMock()
    instance.resolve_current_release.side_effect = (
        lambda identity: CurrentBeetsMissing(identity=identity)
    )
    cls = MagicMock()
    cls.return_value.__enter__ = MagicMock(return_value=instance)
    cls.return_value.__exit__ = MagicMock(return_value=False)
    return cls


def _action_file_release_id(path: str | None) -> str | None:
    """The candidate evidence's release id, decoded from the real action file.

    ``import_one.py`` reads this file for the action-time evidence, so the
    identity it carries is the one the Beets child measures against. Decoded
    with the production Struct rather than raw JSON, so a payload shape change
    fails here.
    """
    if path is None:
        return None
    import msgspec

    from lib.quality import QualityEvidenceActionPayload

    with open(path, "rb") as handle:
        payload = msgspec.json.decode(
            handle.read(), type=QualityEvidenceActionPayload,
        )
    return payload.candidate.mb_release_id


def _expected_blocked_audit(*, rival_request_id: int) -> str:
    """The blocked-audit sentence, composed exactly as production composes it.

    Both halves come from their producers — the collision fragment from
    ``MergeRekeyCollision.detail`` and the sentence from
    ``merge_rekey_blocked_audit_message`` — so this pin can never assert copy
    nothing can emit (test-fidelity Rule C).
    """
    return merge_rekey_blocked_audit_message(
        old_release_id=MERGED,
        new_release_id=SURVIVOR,
        collision_detail=MergeRekeyCollision(
            rival_request_id=rival_request_id,
        ).detail(),
    )


@dataclass(frozen=True)
class LaunchRecord:
    """The force-specific argv the Beets child was launched with."""

    mb_release_id: str
    force: bool
    preserve_source: bool


class _RecordingRunImport:
    """Stands in for the ``import_one.py`` child, recording its launch argv.

    The release id this receives is the decided consequence of the merge
    seam: it is the ``mb_release_id`` the Beets child is launched with, and
    therefore the id ``_find_target_candidate`` will look for. The other
    recorded flags are today's force semantics, which routing force through
    validation must not change. The signature is the real
    ``ImportOneRunner`` contract, spelled out rather than ``**kwargs``, so a
    production signature change fails here instead of silently passing.
    """

    def __init__(self) -> None:
        self.release_ids: list[str] = []
        self.calls: list[LaunchRecord] = []
        #: The release identity of the candidate evidence in the action file
        #: the child is handed. Read from the real tempfile the real writer
        #: produced, because that file IS the evidence boundary — nothing
        #: else tells the child which pressing the measurement describes.
        self.evidence_release_ids: list[str | None] = []

    def __call__(
        self,
        *,
        path: str,
        mb_release_id: str,
        request_id: int,
        force: bool,
        preserve_source: bool,
        override_min_bitrate: int | None,
        target_format: str | None,
        verified_lossless_target: str,
        beets_harness_path: str,
        quality_rank_config_json: str | None,
        existing_v0_probe: V0ProbeEvidence | None,
        quality_evidence_action_file: str | None,
        beets_config_dir: str | None,
        beets_python: str | None,
        beets_library_db_path: str | None,
        beets_library_root: str | None,
        cancellation_token: CancellationToken | None = None,
        on_spawn: Callable[[int], None] | None = None,
        owner_session_probe: Callable[[], bool] | None = None,
    ) -> ImportOneRun:
        del (
            path, request_id, override_min_bitrate, target_format,
            verified_lossless_target, beets_harness_path,
            quality_rank_config_json, existing_v0_probe,
            beets_config_dir, beets_python,
            beets_library_db_path, beets_library_root, cancellation_token,
            on_spawn, owner_session_probe,
        )
        self.evidence_release_ids.append(
            _action_file_release_id(quality_evidence_action_file),
        )
        self.release_ids.append(mb_release_id)
        self.calls.append(LaunchRecord(
            mb_release_id=mb_release_id,
            force=force,
            preserve_source=preserve_source,
        ))
        return ImportOneRun(
            command=("import_one.py", mb_release_id),
            returncode=0,
            stdout="",
            stderr="",
            import_result=make_import_result(
                decision="import", new_min_bitrate=320,
            ),
        )


#: Executable proof the recorder really implements the production seam.
_runner_conformance: ImportOneRunner = _RecordingRunImport()


class _ForceWorld:
    """One quarantined album with a claimed, running force-import job."""

    def __init__(
        self,
        stack: contextlib.ExitStack,
        *,
        stored_release_id: str = MERGED,
        claim: bool = True,
        automation_owned: bool = False,
        path: str | None = None,
    ) -> None:
        self.path = (
            path if path is not None
            else stack.enter_context(tempfile.TemporaryDirectory())
        )
        # Named exactly as ``make_grab_list_entry``'s default download file, so
        # the automation lane's staged-manifest check and the force lane's
        # origin-manifest guard both see the same one-track album. F2 compares
        # the two lanes over THIS directory, and the generated parity property
        # hands both lanes one shared ``path``.
        track = os.path.join(self.path, "01 - Track.mp3")
        if not os.path.exists(track):
            with open(track, "wb") as handle:
                handle.write(b"audio")
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=REQUEST_ID,
            mb_release_id=stored_release_id,
            status="wanted",
            artist_name="DICE",
            album_title="Midnight Zoo",
        ))
        self.db.set_tracks(REQUEST_ID, [{"track_number": 1, "title": "Track"}])
        self.download_log_id = self.db.log_download(
            REQUEST_ID,
            outcome="rejected",
            beets_scenario="mbid_not_found",
        )
        if automation_owned:
            from tests.helpers import handoff_automation_owner

            handoff_automation_owner(
                self.db,
                REQUEST_ID,
                state={
                    "filetype": "mp3",
                    "enqueued_at": "2026-08-11T00:00:00+00:00",
                    "current_path": self.path,
                    "files": [],
                },
                canonical_path=self.path,
            )
        job = self.db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=REQUEST_ID,
            payload=force_import_payload(
                download_log_id=self.download_log_id,
                failed_path=self.path,
            ),
        )
        self.db.mark_import_job_preview_importable(
            job.id, preview_result={}, message="force preview ready",
        )
        self.job_id = job.id
        if claim:
            claimed = self.db.claim_force_import_job_under_lock(
                job.id, request_id=REQUEST_ID, worker_id="force-merge-test",
            )
            if claimed is not None:
                assert claimed.status == "running"
        self._seed_candidate_evidence(stored_release_id)
        self.cfg = CratediggerConfig(
            beets_harness_path=HARNESS,
            beets_distance_threshold=0.15,
            beets_staging_dir=os.path.join(self.path, "staging"),
            slskd_download_dir=self.path,
            pipeline_db_enabled=True,
        )

    def _seed_candidate_evidence(self, release_id: str) -> None:
        evidence = make_album_quality_evidence(
            mb_release_id=release_id,
            source_path=self.path,
            files=snapshot_audio_files(self.path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_import_job_candidate_evidence(self.job_id, persisted.id)

    def stored_release_id(self) -> str | None:
        row = self.db.request(REQUEST_ID)
        return None if row is None else row.get("mb_release_id")


def force_dispatch(
    world: _ForceWorld,
    bv_result: ValidationResult,
    *,
    canonical: Callable[[str], str | None],
    retag: Callable[..., BeetsRetagResult],
    run_import: _RecordingRunImport | None = None,
):
    """Drive the REAL force entry point over ``world``.

    Only the two documented external edges stand in: the beets validation
    subprocess wrapper and the ``import_one.py`` child. Everything between —
    the claim fence, the merge seam, the manifest guard, the evidence gate,
    the launch authorization — is production code.
    """
    runner = run_import if run_import is not None else _RecordingRunImport()
    with (
        _silence_logs(),
        patch_dispatch_externals(),
        patch("lib.beets.beets_validate", return_value=bv_result) as validate,
        patch("lib.beets_db.BeetsDB", _beets_db_holding_nothing()),
        patch("lib.config.read_runtime_config", return_value=world.cfg),
    ):
        outcome = dispatch_import_from_db(
            world.db,  # pyright: ignore[reportArgumentType]
            request_id=REQUEST_ID,
            failed_path=world.path,
            import_job_id=world.job_id,
            download_log_id=world.download_log_id,
            cfg=world.cfg,
            quality_gate_fn=noop_quality_gate,
            run_import_fn=runner,
            canonical_release_fn=canonical,
            retag_fn=retag,
        )
    return outcome, runner, validate


class TestForceImportFollowsTheMerge(unittest.TestCase):
    """F1/F4 — the live request-346 world, through the real force lane."""

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)

    def test_dice_shape_retags_rekeys_and_launches_at_the_survivor(self) -> None:
        """F1 — the decided consequence: Beets is launched at the survivor."""
        world = _ForceWorld(self.stack)
        canonical = RecordingCanonical(SURVIVOR)
        retag = RecordingRetag(world.db, BeetsRetagResult(
            outcome="retagged", detail="retagged album 7",
        ), request_id=REQUEST_ID)

        _, runner, _ = force_dispatch(
            world,
            mbid_not_found_result(candidate(SURVIVOR, distance=0.02)),
            canonical=canonical,
            retag=retag,
        )

        self.assertEqual(canonical.calls, [MERGED])
        # M1's ordering, watched from inside the retag: the row had NOT moved
        # while the library was being retagged.
        self.assertEqual(len(retag.observations), 1)
        self.assertEqual(retag.observations[0].stored_release_id, MERGED)
        self.assertEqual(world.stored_release_id(), SURVIVOR)
        # F1: the id the Beets child was actually launched with. Before #1080
        # this was MERGED and the child rejected rc=4 / ``mbid_missing``.
        self.assertEqual(runner.release_ids, [SURVIVOR])
        # F3: today's force semantics reach the child unchanged. ``force``
        # raises the child's apply-time ``max_distance``; ``preserve_source``
        # keeps the operator's only lossless copy alive until the quality
        # decision (#111). Neither is a merge-seam concern, and neither moved.
        self.assertTrue(runner.calls[0].force)
        self.assertTrue(runner.calls[0].preserve_source)

    def test_a_high_distance_survivor_is_still_launched_under_force(self) -> None:
        """The threshold override is what makes the rekeyed result acceptable.

        The survivor's candidate sits far outside ``beets_distance_threshold``.
        The automation lane names that ``high_distance`` and rejects; the force
        lane runs the same seam with the override, so the same world is named
        ``strong_match`` and the import proceeds.
        """
        world = _ForceWorld(self.stack)
        bv_result = mbid_not_found_result(candidate(SURVIVOR, distance=0.62))

        _, runner, _ = force_dispatch(
            world,
            bv_result,
            canonical=RecordingCanonical(SURVIVOR),
            retag=RecordingRetag(world.db, BeetsRetagResult(
                outcome="retagged", detail="retagged album 7",
            ), request_id=REQUEST_ID),
        )

        self.assertEqual(bv_result.scenario, "strong_match")
        self.assertTrue(bv_result.valid)
        self.assertEqual(world.stored_release_id(), SURVIVOR)
        self.assertEqual(runner.release_ids, [SURVIVOR])

    def test_no_redirect_launches_at_the_stored_release_exactly_as_before(
        self,
    ) -> None:
        """F4/M3 — the common answer changes nothing about the force import."""
        world = _ForceWorld(self.stack)
        canonical = RecordingCanonical(None)
        retag = RecordingRetag(world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ), request_id=REQUEST_ID)

        _, runner, _ = force_dispatch(
            world,
            mbid_not_found_result(candidate(UNRELATED)),
            canonical=canonical,
            retag=retag,
        )

        self.assertEqual(canonical.calls, [MERGED])
        self.assertEqual(retag.observations, [])
        self.assertEqual(world.stored_release_id(), MERGED)
        self.assertEqual(runner.release_ids, [MERGED])

    def test_an_unclaimed_force_job_never_asks_the_mirror(self) -> None:
        """F4 — no claim is no authority, and the lookup is not spent."""
        world = _ForceWorld(self.stack, claim=False)
        canonical = RecordingCanonical(SURVIVOR)
        retag = RecordingRetag(world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ), request_id=REQUEST_ID)

        force_dispatch(
            world,
            mbid_not_found_result(candidate(SURVIVOR)),
            canonical=canonical,
            retag=retag,
        )

        self.assertEqual(canonical.calls, [])
        self.assertEqual(retag.observations, [])
        self.assertEqual(world.stored_release_id(), MERGED)

    def test_an_automation_owned_request_is_refused_before_the_mirror(
        self,
    ) -> None:
        """F4 — the processing owner wins; force never touches an owned row."""
        world = _ForceWorld(self.stack, claim=False, automation_owned=True)
        canonical = RecordingCanonical(SURVIVOR)
        retag = RecordingRetag(world.db, BeetsRetagResult(
            outcome="retagged", detail="should never run",
        ), request_id=REQUEST_ID)

        outcome, runner, _ = force_dispatch(
            world,
            mbid_not_found_result(candidate(SURVIVOR)),
            canonical=canonical,
            retag=retag,
        )

        self.assertFalse(outcome.success)
        self.assertEqual(canonical.calls, [])
        self.assertEqual(retag.observations, [])
        self.assertEqual(runner.release_ids, [])
        self.assertEqual(world.stored_release_id(), MERGED)


class TestForceEvidenceFollowsTheRekey(unittest.TestCase):
    """F6 — the merge seam runs BEFORE candidate evidence is loaded.

    A rekey moves the request's ``album_quality_evidence`` rows onto the
    survivor in the SAME transaction as the row, because evidence is
    content-addressed by ``(mb_release_id, snapshot_fingerprint)``. Load the
    evidence first and the action the child runs is pinned to the pre-rekey
    identity, describing a pressing the request no longer names.

    Asserted at the outermost real adapter rather than on the ordering: the
    action file ``import_one.py`` is actually launched with, written by the
    real ``lib.evidence_action_file`` writer and decoded with the production
    Struct.
    """

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)

    def test_the_evidence_the_child_receives_names_the_survivor(self) -> None:
        world = _ForceWorld(self.stack)

        _, runner, _ = force_dispatch(
            world,
            mbid_not_found_result(candidate(SURVIVOR, distance=0.02)),
            canonical=RecordingCanonical(SURVIVOR),
            retag=RecordingRetag(world.db, BeetsRetagResult(
                outcome="retagged", detail="retagged album 7",
            ), request_id=REQUEST_ID),
        )

        self.assertEqual(world.stored_release_id(), SURVIVOR)
        self.assertEqual(runner.release_ids, [SURVIVOR])
        # The evidence rows moved with the identity, and the action the child
        # runs describes THAT pressing — not the id the request held when the
        # execution started.
        self.assertEqual(runner.evidence_release_ids, [SURVIVOR])

    def test_an_unrekeyed_force_import_still_names_the_stored_release(
        self,
    ) -> None:
        """Must-still-work: no merge, no movement, same evidence as always."""
        world = _ForceWorld(self.stack)

        _, runner, _ = force_dispatch(
            world,
            mbid_not_found_result(candidate(UNRELATED)),
            canonical=RecordingCanonical(None),
            retag=RecordingRetag(world.db, BeetsRetagResult(
                outcome="retagged", detail="should never run",
            ), request_id=REQUEST_ID),
        )

        self.assertEqual(runner.release_ids, [MERGED])
        self.assertEqual(runner.evidence_release_ids, [MERGED])


class TestForceRefusesToLaunchAtASplitIdentity(unittest.TestCase):
    """F5 — a moved library and an unmoved request never reach Beets.

    The residual the pre-check cannot cover: the survivor is claimed while
    the retag runs, so the installed album ends up at the survivor and the
    request still names the merged-away id. Launching Beets at that id would
    hand the operator the pre-#1080 ``mbid_missing`` while their library had
    silently moved — and Beets would flag no duplicate, because duplicate
    detection keys on ``mb_albumid``.
    """

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)

    def _library_holding_the_merged_album(self) -> FakeBeetsDB:
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [7])
        beets.set_album_ids_for_release(SURVIVOR, [])
        return beets

    def test_a_split_identity_refuses_the_launch_and_leaves_an_audit_row(
        self,
    ) -> None:
        world = _ForceWorld(self.stack)
        beets = self._library_holding_the_merged_album()
        real_retag = real_retag_over(beets)
        observed: list[BeetsRetagResult] = []

        def retag_and_lose_the_race(
            cfg: CratediggerConfig,
            *,
            old_identity: ReleaseIdentity,
            new_identity: ReleaseIdentity,
        ) -> BeetsRetagResult:
            world.db.seed_request(make_request_row(
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

        outcome, runner, _ = force_dispatch(
            world,
            mbid_not_found_result(candidate(SURVIVOR, distance=0.02)),
            canonical=RecordingCanonical(SURVIVOR),
            retag=retag_and_lose_the_race,
        )

        # The library really moved and the request really did not.
        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [])
        self.assertEqual(beets.get_all_album_ids_for_release(SURVIVOR), [7])
        self.assertEqual(world.stored_release_id(), MERGED)
        # The decided consequence: Beets was never launched at all.
        self.assertEqual(runner.release_ids, [])
        self.assertFalse(outcome.success)
        # One producer for the sentence, and its retag detail comes from the
        # retag that really ran (test-fidelity Rule C).
        self.assertEqual(len(observed), 1)
        expected = split_identity_audit_message(
            old_release_id=MERGED,
            new_release_id=SURVIVOR,
            retag_detail=observed[0].detail,
        )
        self.assertEqual(outcome.message, expected)
        audits = [
            row for row in world.db.download_logs if row.outcome == "failed"
        ]
        self.assertEqual(len(audits), 1)
        self.assertEqual(audits[0].request_id, REQUEST_ID)
        self.assertEqual(audits[0].error_message, expected)
        # Nothing is parked: the request keeps its runnable status.
        row = world.db.request(REQUEST_ID)
        assert row is not None
        self.assertEqual(row["status"], "wanted")

    def test_a_blocked_rekey_leaves_the_force_import_exactly_as_it_was(
        self,
    ) -> None:
        """F5/F7 — the deliberate scope line: blocked is not split.

        A rival already holding the survivor is refused BEFORE the retag, so
        the library is untouched and force proceeds against the id the request
        names — exactly what it did before #1080. Only a divergence THIS
        execution created stops the launch. The operator is not left guessing
        either: the seam records why.
        """
        world = _ForceWorld(self.stack)
        world.db.seed_request(make_request_row(
            id=999,
            mb_release_id=SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing)",
        ))
        beets = self._library_holding_the_merged_album()

        outcome, runner, _ = force_dispatch(
            world,
            mbid_not_found_result(candidate(SURVIVOR, distance=0.02)),
            canonical=RecordingCanonical(SURVIVOR),
            retag=real_retag_over(beets),
        )

        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [7])
        self.assertEqual(world.stored_release_id(), MERGED)
        self.assertEqual(runner.release_ids, [MERGED])
        self.assertTrue(outcome.success)
        self.assertEqual(
            [row.error_message for row in world.db.download_logs
             if row.outcome == "failed"],
            [_expected_blocked_audit(rival_request_id=999)],
        )

    def test_a_pre_existing_split_is_not_detected_but_is_explained(
        self,
    ) -> None:
        """F7 — the world an EARLIER execution's lost race left behind.

        The library is already at the survivor, the request still names the
        merged-away id, and the rival that won that race still holds the
        survivor. The occupancy pre-check refuses before the library is read,
        so this execution — which moved nothing — has no split of its own to
        refuse, and the launch proceeds exactly as it did before #1080:
        ``import_one.py`` matches by exact ``album_id`` and rejects
        ``mbid_missing`` rather than landing a second album beside the
        survivor. Without the blocked audit that repeats forever with no new
        evidence, which is the reported defect; with it, every attempt says
        which release this was merged into and what is holding it.
        """
        world = _ForceWorld(self.stack)
        world.db.seed_request(make_request_row(
            id=999,
            mb_release_id=SURVIVOR,
            artist_name="DICE",
            album_title="Midnight Zoo (other pressing)",
        ))
        # The split itself: the installed album is filed at the SURVIVOR
        # while the request still names the merged-away id.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MERGED, [])
        beets.set_album_ids_for_release(SURVIVOR, [7])

        outcome, runner, _ = force_dispatch(
            world,
            mbid_not_found_result(candidate(SURVIVOR, distance=0.02)),
            canonical=RecordingCanonical(SURVIVOR),
            retag=real_retag_over(beets),
        )

        # The seam never read the library, and never touched it.
        self.assertEqual(beets.get_all_album_ids_for_release(SURVIVOR), [7])
        self.assertEqual(beets.get_all_album_ids_for_release(MERGED), [])
        self.assertEqual(world.stored_release_id(), MERGED)
        self.assertEqual(runner.release_ids, [MERGED])
        self.assertTrue(outcome.success)
        # The operator's evidence, from the producer (test-fidelity Rule C).
        audits = [
            row for row in world.db.download_logs if row.outcome == "failed"
        ]
        self.assertEqual(len(audits), 1)
        self.assertEqual(audits[0].request_id, REQUEST_ID)
        self.assertEqual(
            audits[0].error_message, _expected_blocked_audit(rival_request_id=999),
        )
        # Still runnable: surfaced, never parked.
        row = world.db.request(REQUEST_ID)
        assert row is not None
        self.assertEqual(row["status"], "wanted")


class TestForceStillImportsDespiteTheVerdict(unittest.TestCase):
    """F3 — routing force through validation must not narrow what force does."""

    RESULTS = (
        (
            "extra_tracks",
            ValidationResult(
                valid=False, distance=0.04, scenario="extra_tracks",
                detail="MB has 3 more tracks than local files",
                mbid_found=True, target_mbid=MERGED,
                candidates=[candidate(MERGED, extra_tracks=3)],
            ),
        ),
        (
            "no_choose_match",
            ValidationResult(
                valid=False, scenario="no_choose_match",
                detail="beets harness ended without offering a match to review",
                target_mbid=MERGED,
            ),
        ),
        (
            "validation_error",
            ValidationResult(
                valid=False, scenario="validation_error",
                detail="beets validation did not complete",
                error="Failed to start harness", target_mbid=MERGED,
            ),
        ),
    )

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)

    def test_every_invalid_validation_still_reaches_the_beets_launch(
        self,
    ) -> None:
        for scenario, bv_result in self.RESULTS:
            with self.subTest(scenario=scenario):
                world = _ForceWorld(self.stack)
                canonical = RecordingCanonical(SURVIVOR)

                _, runner, _ = force_dispatch(
                    world,
                    bv_result,
                    canonical=canonical,
                    retag=RecordingRetag(world.db, BeetsRetagResult(
                        outcome="retagged", detail="should never run",
                    ), request_id=REQUEST_ID),
                )

                self.assertEqual(runner.release_ids, [MERGED])
                self.assertEqual(world.stored_release_id(), MERGED)
                # M2: only ``mbid_not_found`` may reach the mirror.
                self.assertEqual(canonical.calls, [])


class TestOneComparisonSeamTwoThresholds(unittest.TestCase):
    """F2 — the anti-divergence guard.

    Both lanes are driven over the same album, the same release id and the
    same harness path, and the arguments they hand the exact-release
    comparison are compared position by position. Everything must be equal
    except the distance threshold — that is the operator's stated contract
    ("exactly the same as anything else just with beets distance
    over-ridden"), asserted rather than described.
    """

    def setUp(self) -> None:
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)

    def _automation_validate_args(self, cfg: CratediggerConfig, path: str):
        """Drive the automation lane's real validation over the same album."""
        import contextlib as _contextlib

        from lib.download_validation import _process_beets_validation
        from lib.staged_album import StagedAlbum
        from tests.helpers import handoff_automation_owner, make_ctx_with_fake_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=REQUEST_ID, mb_release_id=MERGED,
            artist_name="DICE", album_title="Midnight Zoo",
        ))
        job = handoff_automation_owner(
            db,
            REQUEST_ID,
            state={
                "filetype": "mp3",
                "enqueued_at": "2026-08-11T00:00:00+00:00",
                "current_path": path,
                "files": [],
            },
            canonical_path=path,
        )
        ctx = make_ctx_with_fake_db(db, cfg=cfg)
        album_data = _automation_entry()
        with (
            _silence_logs(),
            _contextlib.suppress(Exception),
            patch(
                "lib.beets.beets_validate",
                return_value=mbid_not_found_result(candidate(UNRELATED)),
            ) as validate,
        ):
            _process_beets_validation(
                album_data,
                StagedAlbum(current_path=path, request_id=REQUEST_ID),
                ctx,
                import_job_id=job.id,
                canonical_release_fn=RecordingCanonical(None),
                retag_fn=RecordingRetag(db, BeetsRetagResult(
                    outcome="retagged", detail="should never run",
                ), request_id=REQUEST_ID),
            )
        return validate.call_args

    def test_both_lanes_call_one_seam_differing_only_in_the_threshold(
        self,
    ) -> None:
        world = _ForceWorld(self.stack)
        _, _, force_validate = force_dispatch(
            world,
            mbid_not_found_result(candidate(UNRELATED)),
            canonical=RecordingCanonical(None),
            retag=RecordingRetag(world.db, BeetsRetagResult(
                outcome="retagged", detail="should never run",
            ), request_id=REQUEST_ID),
        )
        automation_args = self._automation_validate_args(world.cfg, world.path)

        self.assertIsNotNone(automation_args)
        assert automation_args is not None
        force_args = force_validate.call_args
        self.assertIsNotNone(force_args)
        assert force_args is not None

        # harness path, album path and release id: identical inputs.
        self.assertEqual(force_args.args[:3], automation_args.args[:3])
        # The one documented difference.
        self.assertEqual(force_args.args[3], FORCE_IMPORT_DISTANCE_THRESHOLD)
        self.assertEqual(automation_args.args[3], world.cfg.beets_distance_threshold)
        self.assertNotEqual(force_args.args[3], automation_args.args[3])
        # And it is genuinely unbounded: no Beets distance can exceed it.
        self.assertGreater(FORCE_IMPORT_DISTANCE_THRESHOLD, 1.0)


def _automation_entry():
    from tests.helpers import make_grab_list_entry

    return make_grab_list_entry(
        album_id=REQUEST_ID,
        artist="DICE",
        title="Midnight Zoo",
        mb_release_id=MERGED,
        db_source="request",
        db_request_id=REQUEST_ID,
    )


if __name__ == "__main__":
    unittest.main()
