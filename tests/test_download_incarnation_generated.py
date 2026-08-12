"""Generated whole-state download-incarnation contracts for issue #898 PR1.

Two paired deterministic/generated invariants cover the composition boundary:

1. Once attempt B owns a request, delayed whole-state payloads derived by the
   four PR1 A seams (enqueue, event, harvest, poll) cannot change B, while
   payloads carrying B's exact witness remain writable.
2. A post-event poll refresh admits only the exact ``(request_id,
   enqueued_at)`` incarnations captured before the transfer snapshot, in
   refreshed-row order and with the original witness text preserved.

Event occurrence-time, candidate-selection, cursor-hold, and replay generation
remain in ``tests/test_slskd_events_generated.py`` where the complete event
window is observable. Downstream processing and side-effect ownership remain
the explicit PR2 boundary.
"""

from __future__ import annotations

import copy
import re
import unittest
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Literal

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.download import (
    _admit_download_incarnations,
    _decode_valid_download_incarnations,
)
from lib.import_queue import AutomationHandoffResult
from lib.pipeline_db.rows import AlbumRequestRow
from lib.quality import ActiveDownloadFileState, ActiveDownloadState
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

PayloadFamily = Literal["enqueue", "event", "harvest", "poll"]
HandoffRejectionKind = Literal[
    "missing_state",
    "non_downloading",
    "active_conflict",
    "lock_unavailable",
]
AdmissionKind = Literal[
    "unchanged",
    "unchanged_alt",
    "same_id_b",
    "left_or_new",
    "missing",
    "empty",
    "malformed",
    "invalid",
]

_PAYLOAD_FAMILIES: tuple[PayloadFamily, ...] = (
    "enqueue",
    "event",
    "harvest",
    "poll",
)
_ADMISSION_KINDS: tuple[AdmissionKind, ...] = (
    "unchanged",
    "unchanged_alt",
    "same_id_b",
    "left_or_new",
    "missing",
    "empty",
    "malformed",
    "invalid",
)
_DETERMINISTIC_PATH = "/processing/albums/Artist - Album [same-attempt-path]"
# The four rows whose stored state is undecodable or witness-less are the SAME
# row observed twice: they are already degenerate in the pre-snapshot, so a
# dropped decode guard admits a matching pair instead of excluding nothing.
_DEGENERATE_PRE_KINDS: dict[int, AdmissionKind] = {
    4: "missing",
    5: "empty",
    6: "malformed",
    7: "invalid",
}


def _anchored(message: str) -> str:
    """Anchor a full clause message so no sibling clause can satisfy it."""
    return "^" + re.escape(message) + "$"


def _anchored_prefix(prefix: str) -> str:
    """Anchor a clause message whose tail carries generated values."""
    return "^" + re.escape(prefix)


@dataclass(frozen=True)
class TranscriptWorld:
    """One same-request, same-path A/B whole-state transcript."""

    witness_a: str
    witness_b: str
    operation_order: tuple[PayloadFamily, ...]
    username: str
    filename: str
    size: int
    progress_bytes: int


@dataclass(frozen=True)
class AdmissionWorld:
    """One post-event refreshed cohort with every admission category."""

    witness_a: str
    witness_b: str
    refreshed_order: tuple[AdmissionKind, ...]
    replacement_is_new_id: bool


@dataclass(frozen=True)
class HandoffWorld:
    """One exact or stale downloader-to-processor handoff attempt."""

    current_witness: str
    attempted_witness: str
    canonical_path: str


@dataclass(frozen=True)
class HandoffRejectionWorld:
    """One non-admissible downloader-to-processor transcript."""

    kind: HandoffRejectionKind
    witness: str
    canonical_path: str


def _render_witness(
    instant: datetime,
    *,
    style: str,
    offset_minutes: int,
) -> str:
    """Render one valid ISO witness without sharing production parsing."""
    if style == "naive":
        return instant.astimezone(UTC).replace(tzinfo=None).isoformat(
            timespec="microseconds",
        )
    if style == "z":
        return instant.astimezone(UTC).isoformat(
            timespec="microseconds",
        ).replace("+00:00", "Z")
    return instant.astimezone(
        timezone(timedelta(minutes=offset_minutes)),
    ).isoformat(timespec="microseconds")


@st.composite
def _distinct_witnesses(draw) -> tuple[str, str]:
    base_seconds = draw(st.integers(min_value=0, max_value=180 * 24 * 3600))
    delta_seconds = draw(st.integers(min_value=1, max_value=14 * 24 * 3600))
    base = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=base_seconds)
    style_a = draw(st.sampled_from(("offset", "z", "naive")))
    style_b = draw(st.sampled_from(("offset", "z", "naive")))
    offset_a = draw(st.sampled_from((-330, 0, 345, 480)))
    offset_b = draw(st.sampled_from((-330, 0, 345, 480)))
    return (
        _render_witness(base, style=style_a, offset_minutes=offset_a),
        _render_witness(
            base + timedelta(seconds=delta_seconds),
            style=style_b,
            offset_minutes=offset_b,
        ),
    )


@st.composite
def transcript_worlds(draw) -> TranscriptWorld:
    witness_a, witness_b = draw(_distinct_witnesses())
    size = draw(st.integers(min_value=1, max_value=2**31))
    return TranscriptWorld(
        witness_a=witness_a,
        witness_b=witness_b,
        operation_order=tuple(draw(st.permutations(_PAYLOAD_FAMILIES))),
        username=draw(st.sampled_from(("peer", "PEER", "péer♪"))),
        filename=draw(st.sampled_from((
            "Music\\Artist\\Album\\01 track.flac",
            "@@direct\\Album\\same path.opus",
            "Music\\Ártîst 音\\Album\\01.mp3",
        ))),
        size=size,
        progress_bytes=draw(st.integers(min_value=0, max_value=size)),
    )


@st.composite
def admission_worlds(draw) -> AdmissionWorld:
    witness_a, witness_b = draw(_distinct_witnesses())
    return AdmissionWorld(
        witness_a=witness_a,
        witness_b=witness_b,
        refreshed_order=tuple(draw(st.permutations(_ADMISSION_KINDS))),
        replacement_is_new_id=draw(st.booleans()),
    )


@st.composite
def handoff_worlds(draw) -> HandoffWorld:
    witness_a, witness_b = draw(_distinct_witnesses())
    exact = draw(st.booleans())
    return HandoffWorld(
        current_witness=witness_b,
        attempted_witness=witness_b if exact else witness_a,
        canonical_path=draw(st.sampled_from((
            "/processing/albums/exact",
            "/processing/albums/Ártist - 音 [pressing]",
        ))),
    )


@st.composite
def handoff_rejection_worlds(draw) -> HandoffRejectionWorld:
    witness_a, _witness_b = draw(_distinct_witnesses())
    return HandoffRejectionWorld(
        kind=draw(st.sampled_from((
            "missing_state",
            "non_downloading",
            "active_conflict",
            "lock_unavailable",
        ))),
        witness=witness_a,
        canonical_path=draw(st.sampled_from((
            "/processing/albums/rejected",
            "/processing/albums/Ártist - 音 [rejected]",
        ))),
    )


def _state_dict(state: ActiveDownloadState) -> dict[str, object]:
    """Encode through the production wire Struct, retaining its exact text."""
    return msgspec.json.decode(
        state.to_json(),
        type=dict[str, object],
    )


def _payload_state(
    world: TranscriptWorld,
    *,
    witness: str,
    family: PayloadFamily,
) -> ActiveDownloadState:
    """Build one of the four PR1 whole-state payload families."""
    local_path: str | None = None
    last_state: str | None = None
    bytes_transferred = 0
    retry_count = 0
    if family == "event":
        local_path = "/downloads/complete/01 track.flac"
    elif family == "harvest":
        bytes_transferred = world.size
        last_state = "Completed, Succeeded"
    elif family == "poll":
        bytes_transferred = world.progress_bytes
        last_state = "InProgress"
        retry_count = 1
    return ActiveDownloadState(
        filetype="flac",
        enqueued_at=witness,
        last_progress_at=witness,
        current_path=_DETERMINISTIC_PATH,
        files=[
            ActiveDownloadFileState(
                username=world.username,
                filename=world.filename,
                file_dir=(
                    world.filename.rsplit("\\", 1)[0]
                    if "\\" in world.filename
                    else "Music"
                ),
                size=world.size,
                retry_count=retry_count,
                bytes_transferred=bytes_transferred,
                last_state=last_state,
                local_path=local_path,
            ),
        ],
    )


def _stored_witness(row: Mapping[str, object]) -> str | None:
    raw_state = row.get("active_download_state")
    if raw_state is None:
        return None
    try:
        return ActiveDownloadState.from_raw(raw_state).enqueued_at
    except (TypeError, ValueError, msgspec.DecodeError, msgspec.ValidationError):
        return None


def assert_witnessed_write_contract(
    *,
    before_row: Mapping[str, object],
    outgoing_state: ActiveDownloadState,
    expected_witness: str,
    applied: bool,
    after_row: Mapping[str, object],
) -> None:
    """Independent oracle for the three-predicate whole-state CAS."""
    expected_applied = (
        before_row.get("status") == "downloading"
        and _stored_witness(before_row) == expected_witness
        and outgoing_state.enqueued_at == expected_witness
    )
    if applied != expected_applied:
        raise AssertionError(
            "whole-state CAS result diverged from status/stored/outgoing "
            f"witness predicates: expected={expected_applied} actual={applied}"
        )

    before = dict(before_row)
    after = dict(after_row)
    if not expected_applied:
        if after != before:
            raise AssertionError(
                "rejected whole-state CAS changed row state or metadata"
            )
        return

    expected_after = copy.deepcopy(before)
    expected_after["active_download_state"] = _state_dict(outgoing_state)
    expected_after["updated_at"] = after.get("updated_at")
    if after != expected_after:
        raise AssertionError(
            "accepted whole-state CAS changed fields beyond state/updated_at"
        )


def assert_exact_admission(
    expected_pairs: Sequence[tuple[int, str]],
    actual_pairs: Sequence[tuple[int, str]],
) -> None:
    """Independent exact-pair, refreshed-order poll-admission checker."""
    if tuple(actual_pairs) != tuple(expected_pairs):
        raise AssertionError(
            "post-event poll admission diverged from exact incarnation "
            f"oracle: expected={tuple(expected_pairs)!r} "
            f"actual={tuple(actual_pairs)!r}"
        )


def _seed_current_b(
    db: FakePipelineDB,
    world: TranscriptWorld,
    *,
    status: str = "downloading",
) -> None:
    state_b = _payload_state(
        world,
        witness=world.witness_b,
        family="enqueue",
    )
    db.seed_request(make_request_row(
        id=1,
        mb_release_id="incarnation-contract-request",
        status=status,
        active_download_state=_state_dict(state_b),
    ))


def _execute_witnessed_write_and_check(
    db: FakePipelineDB,
    outgoing_state: ActiveDownloadState,
    *,
    expected_witness: str,
) -> None:
    """Capture one row, execute its witnessed write, and check the result."""
    before = copy.deepcopy(db.request(1))
    applied = db.update_download_state_if_downloading(
        1,
        outgoing_state.to_json(),
        expected_enqueued_at=expected_witness,
    )
    after = copy.deepcopy(db.request(1))
    assert_witnessed_write_contract(
        before_row=before,
        outgoing_state=outgoing_state,
        expected_witness=expected_witness,
        applied=applied,
        after_row=after,
    )


def _exercise_transcript(world: TranscriptWorld) -> None:
    if (
        len(world.operation_order) != len(_PAYLOAD_FAMILIES)
        or set(world.operation_order) != set(_PAYLOAD_FAMILIES)
    ):
        raise AssertionError(
            f"operation alphabet drifted outside PR1: {world.operation_order!r}"
        )

    db = FakePipelineDB()
    _seed_current_b(db, world)

    for family in world.operation_order:
        stale_a = _payload_state(
            world,
            witness=world.witness_a,
            family=family,
        )
        _execute_witnessed_write_and_check(
            db,
            outgoing_state=stale_a,
            expected_witness=world.witness_a,
        )

        current_b = _payload_state(
            world,
            witness=world.witness_b,
            family=family,
        )
        _execute_witnessed_write_and_check(
            db,
            outgoing_state=current_b,
            expected_witness=world.witness_b,
        )

    # Make the other two predicates independently decisive against the real
    # fake implementation. The stale-A transcript above isolates the stored
    # witness predicate; these worlds hold the other two predicates true.
    status_db = FakePipelineDB()
    _seed_current_b(status_db, world, status="wanted")
    for family in world.operation_order:
        matching_b = _payload_state(
            world,
            witness=world.witness_b,
            family=family,
        )
        _execute_witnessed_write_and_check(
            status_db,
            outgoing_state=matching_b,
            expected_witness=world.witness_b,
        )

    outgoing_db = FakePipelineDB()
    _seed_current_b(outgoing_db, world)
    for family in world.operation_order:
        mismatched_outgoing_a = _payload_state(
            world,
            witness=world.witness_a,
            family=family,
        )
        _execute_witnessed_write_and_check(
            outgoing_db,
            outgoing_state=mismatched_outgoing_a,
            expected_witness=world.witness_b,
        )


def _admission_state(witness: str) -> ActiveDownloadState:
    return ActiveDownloadState(
        filetype="flac",
        enqueued_at=witness,
        current_path=_DETERMINISTIC_PATH,
        files=[
            ActiveDownloadFileState(
                username="peer",
                filename="Music\\Artist\\Album\\01 track.flac",
                file_dir="Music\\Artist\\Album",
                size=1000,
            ),
        ],
    )


def _admission_row(
    request_id: int,
    *,
    witness: str,
    status: str = "downloading",
    raw_state: object = ...,
) -> dict[str, object]:
    state = (
        _state_dict(_admission_state(witness))
        if raw_state is ...
        else raw_state
    )
    return make_request_row(
        id=request_id,
        mb_release_id=f"incarnation-admission-{request_id}",
        status=status,
        active_download_state=state,
    )


def _build_admission_inputs(
    world: AdmissionWorld,
) -> tuple[
    list[tuple[AlbumRequestRow, ActiveDownloadState]],
    list[AlbumRequestRow],
    tuple[tuple[int, str], ...],
]:
    refreshed_by_kind: dict[AdmissionKind, dict[str, object]] = {
        "unchanged": _admission_row(1, witness=world.witness_a),
        "unchanged_alt": _admission_row(8, witness=world.witness_b),
        "same_id_b": _admission_row(2, witness=world.witness_b),
        "left_or_new": _admission_row(
            99 if world.replacement_is_new_id else 3,
            witness=world.witness_a,
            status=(
                "downloading" if world.replacement_is_new_id else "wanted"
            ),
        ),
        "missing": _admission_row(
            4,
            witness=world.witness_a,
            raw_state=None,
        ),
        "empty": _admission_row(5, witness=""),
        "malformed": _admission_row(
            6,
            witness=world.witness_a,
            raw_state={
                "filetype": "flac",
                "enqueued_at": world.witness_a,
                "files": "not-a-file-list",
            },
        ),
        "invalid": _admission_row(7, witness="not-an-iso-witness"),
    }

    pre_db = FakePipelineDB()
    for request_id in range(1, 9):
        degenerate_kind = _DEGENERATE_PRE_KINDS.get(request_id)
        if degenerate_kind is not None:
            pre_db.seed_request(copy.deepcopy(refreshed_by_kind[degenerate_kind]))
            continue
        witness = world.witness_b if request_id == 8 else world.witness_a
        pre_db.seed_request(_admission_row(request_id, witness=witness))
    pre_snapshot = _decode_valid_download_incarnations(
        pre_db.get_downloading(),
        phase="generated pre-snapshot",
    )

    refreshed_db = FakePipelineDB()
    for kind in world.refreshed_order:
        refreshed_db.seed_request(refreshed_by_kind[kind])
    refreshed_rows = refreshed_db.get_downloading()

    exact_by_kind: dict[AdmissionKind, tuple[int, str]] = {
        "unchanged": (1, world.witness_a),
        "unchanged_alt": (8, world.witness_b),
    }
    expected = tuple(
        exact_by_kind[kind]
        for kind in world.refreshed_order
        if kind in exact_by_kind
    )
    return pre_snapshot, refreshed_rows, expected


def assert_handoff_contract(
    *,
    exact: bool,
    before: Mapping[str, object],
    after: Mapping[str, object],
    job_count: int,
) -> None:
    """Independent oracle for the exact-witness ownership publication."""
    if not exact:
        if dict(after) != dict(before) or job_count != 0:
            raise AssertionError("stale handoff produced an observable change")
        return
    owner = after.get("active_automation_import_job_id")
    state = after.get("active_download_state")
    if (
        after.get("status") != "processing"
        or not isinstance(owner, int)
        or owner <= 0
        or not isinstance(state, dict)
        or state.get("processing_started_at") is None
        or job_count != 1
    ):
        raise AssertionError("exact handoff did not publish one processor owner")


def _exercise_handoff(
    world: HandoffWorld,
    *,
    db_factory: Callable[[], FakePipelineDB] = FakePipelineDB,
) -> None:
    db = db_factory()
    request_id = db.add_request(
        "Artist",
        "Album",
        "request",
        mb_release_id="handoff-generated",
    )
    state = ActiveDownloadState(
        filetype="flac",
        enqueued_at=world.current_witness,
        last_progress_at=world.current_witness,
        files=[ActiveDownloadFileState(
            username="peer",
            filename="Artist/Album/01.flac",
            file_dir="Artist/Album",
            size=10,
            last_state="Completed, Succeeded",
            bytes_transferred=10,
        )],
    )
    if not db.set_downloading(
        request_id,
        state.to_json(),
        expected_status="wanted",
    ):
        raise AssertionError("generated fixture failed to enter downloading")
    before = copy.deepcopy(db.get_request(request_id))
    assert before is not None

    result = db.handoff_automation_import(
        request_id=request_id,
        expected_enqueued_at=world.attempted_witness,
        canonical_path=world.canonical_path,
        message="generated handoff",
    )

    after = db.get_request(request_id)
    assert after is not None
    exact = world.attempted_witness == world.current_witness
    if result.committed != exact:
        raise AssertionError(
            f"handoff outcome {result.outcome!r} disagreed with exact={exact}"
        )
    jobs = db.list_import_jobs(request_id=request_id)
    assert_handoff_contract(
        exact=exact,
        before=before,
        after=after,
        job_count=len(jobs),
    )
    if exact:
        if result.job is None:
            raise AssertionError("committed handoff returned no job record")
        active_state = after["active_download_state"]
        if (
            after["active_automation_import_job_id"] != result.job.id
            or result.job.expected_request_status != "processing"
            or not isinstance(active_state, dict)
            or active_state.get("current_path") != world.canonical_path
        ):
            raise AssertionError("handoff owner, job, and canonical path diverged")
        before_rejected_writes = copy.deepcopy(after)
        if db.update_download_state_if_downloading(
            request_id,
            state.to_json(),
            expected_enqueued_at=world.current_witness,
        ):
            raise AssertionError("poll/event writer crossed the handoff")
        if db.reset_downloading_to_wanted(
            request_id,
            expected_status="downloading",
        ):
            raise AssertionError("reset writer crossed the handoff")
        if db.get_request(request_id) != before_rejected_writes:
            raise AssertionError("rejected post-handoff writer changed metadata")


def _exercise_rejected_handoff(
    world: HandoffRejectionWorld,
    *,
    db_factory: Callable[[], FakePipelineDB] = FakePipelineDB,
) -> None:
    db = db_factory()
    request_id = db.add_request(
        "Artist",
        "Album",
        "request",
        mb_release_id=f"handoff-rejected-{world.kind}",
    )
    state = _admission_state(world.witness)
    if (
        world.kind != "non_downloading"
        and not db.set_downloading(
            request_id,
            state.to_json(),
            expected_status="wanted",
        )
    ):
        raise AssertionError("rejection fixture failed to enter downloading")
    if world.kind == "missing_state":
        db.request(request_id)["active_download_state"] = None
    elif world.kind == "active_conflict":
        owner = db.handoff_automation_import(
            request_id=request_id,
            expected_enqueued_at=world.witness,
            canonical_path=world.canonical_path,
            message="seed conflict",
        )
        if not owner.committed:
            raise AssertionError("conflict fixture failed to create active job")
        row = db.request(request_id)
        row["status"] = "downloading"
        row["active_automation_import_job_id"] = None
    elif world.kind == "lock_unavailable":
        db.set_advisory_lock_result(False)

    expected = {
        "missing_state": "missing_state",
        "non_downloading": "not_downloading",
        "active_conflict": "owner_conflict",
        "lock_unavailable": "lock_unavailable",
    }[world.kind]
    before = copy.deepcopy(db.get_request(request_id))
    before_jobs = db.list_import_jobs(request_id=request_id)
    result = db.handoff_automation_import(
        request_id=request_id,
        expected_enqueued_at=world.witness,
        canonical_path=world.canonical_path,
        message="must reject",
    )
    after = db.get_request(request_id)
    after_jobs = db.list_import_jobs(request_id=request_id)
    if result.outcome != expected:
        raise AssertionError(
            f"{world.kind} returned {result.outcome}, expected {expected}"
        )
    if after != before:
        raise AssertionError(f"{world.kind} changed request metadata")
    if after_jobs != before_jobs:
        raise AssertionError(f"{world.kind} created or changed a job")


class TestDeterministicDownloadIncarnationContract(unittest.TestCase):
    """Named pin paired with the whole-state generated transcript."""

    def test_same_path_transcript_and_all_predicates_are_decisive(self) -> None:
        _exercise_transcript(TranscriptWorld(
            witness_a="2026-07-28T01:00:00.000000Z",
            witness_b="2026-07-28T09:00:01.000000+08:00",
            operation_order=("enqueue", "event", "harvest", "poll"),
            username="peer",
            filename="Music\\Artist\\Album\\01 track.flac",
            size=1000,
            progress_bytes=400,
        ))

    def test_exact_admission_preserves_refreshed_order_and_text(self) -> None:
        world = AdmissionWorld(
            witness_a="2026-07-28T01:00:00.000000Z",
            witness_b="2026-07-28T09:00:01.000000+08:00",
            refreshed_order=(
                "same_id_b",
                "unchanged_alt",
                "missing",
                "unchanged",
                "empty",
                "malformed",
                "invalid",
                "left_or_new",
            ),
            replacement_is_new_id=False,
        )
        with self.assertLogs("cratedigger", level="WARNING"):
            pre_snapshot, refreshed_rows, expected = _build_admission_inputs(world)
            admitted = _admit_download_incarnations(
                pre_snapshot,
                refreshed_rows,
                refreshed_phase="deterministic post-ingest refresh",
            )
        actual = tuple(
            (row["id"], state.enqueued_at)
            for row, state in admitted
        )
        assert_exact_admission(expected, actual)

    def test_exact_handoff_commits_and_stale_handoff_is_inert(self) -> None:
        _exercise_handoff(HandoffWorld(
            current_witness="2026-07-29T08:00:00+08:00",
            attempted_witness="2026-07-29T08:00:00+08:00",
            canonical_path=_DETERMINISTIC_PATH,
        ))
        _exercise_handoff(HandoffWorld(
            current_witness="2026-07-29T08:00:01+08:00",
            attempted_witness="2026-07-29T00:00:00Z",
            canonical_path=_DETERMINISTIC_PATH,
        ))

    def test_all_non_admissible_handoffs_are_inert(self) -> None:
        for kind in (
            "missing_state",
            "non_downloading",
            "active_conflict",
            "lock_unavailable",
        ):
            with self.subTest(kind=kind):
                _exercise_rejected_handoff(HandoffRejectionWorld(
                    kind=kind,
                    witness="2026-07-29T00:00:00Z",
                    canonical_path=_DETERMINISTIC_PATH,
                ))

    def test_fake_handoff_fault_boundaries_roll_back_and_burn_job_ids(
        self,
    ) -> None:
        db = FakePipelineDB()
        request_id = db.add_request("Artist", "Album", "request")
        state = _admission_state("2026-07-29T00:00:00Z")
        self.assertTrue(db.set_downloading(
            request_id,
            state.to_json(),
            expected_status="wanted",
        ))
        before = copy.deepcopy(db.get_request(request_id))

        for boundary in (1, 2):
            def fail_at(
                index: int,
                label: str,
                expected_boundary: int = boundary,
            ) -> None:
                del label
                if index == expected_boundary:
                    raise RuntimeError("fault")

            db._automation_handoff_write_boundary = fail_at
            with self.assertRaisesRegex(RuntimeError, "fault"):
                db.handoff_automation_import(
                    request_id=request_id,
                    expected_enqueued_at=state.enqueued_at,
                    canonical_path=_DETERMINISTIC_PATH,
                    message="faulted",
                )
            self.assertEqual(db.get_request(request_id), before)
            self.assertEqual(db.list_import_jobs(request_id=request_id), [])

        def no_fault(index: int, label: str) -> None:
            del index, label

        db._automation_handoff_write_boundary = no_fault
        committed = db.handoff_automation_import(
            request_id=request_id,
            expected_enqueued_at=state.enqueued_at,
            canonical_path=_DETERMINISTIC_PATH,
            message="retry",
        )
        assert committed.job is not None
        self.assertEqual(committed.job.id, 3)


class TestGeneratedDownloadIncarnationContract(unittest.TestCase):
    """Generated same-path transcripts across all four PR1 payload families."""

    @given(world=transcript_worlds())
    @example(world=TranscriptWorld(
        witness_a="2026-07-28T01:00:00",
        witness_b="2026-07-28T09:00:00+08:00",
        operation_order=("poll", "event", "enqueue", "harvest"),
        username="péer♪",
        filename="@@direct\\Album\\same path.opus",
        size=1,
        progress_bytes=1,
    ))
    def test_delayed_a_and_negative_worlds_preserve_current_rows(
        self,
        world: TranscriptWorld,
    ) -> None:
        _exercise_transcript(world)

    @given(world=admission_worlds())
    @example(world=AdmissionWorld(
        witness_a="2026-07-28T01:00:00.000000Z",
        witness_b="2026-07-28T09:00:01.000000+08:00",
        refreshed_order=_ADMISSION_KINDS,
        replacement_is_new_id=True,
    ))
    # Issue #1094: the same-ID replacement arm is the one that separates exact
    # pair admission from request-ID admission, so it is pinned rather than
    # left to the derandomized sweep.
    @example(world=AdmissionWorld(
        witness_a="2026-07-28T01:00:00.000000Z",
        witness_b="2026-07-28T09:00:01.000000+08:00",
        refreshed_order=tuple(reversed(_ADMISSION_KINDS)),
        replacement_is_new_id=False,
    ))
    def test_post_event_cohort_matches_exact_pair_oracle(
        self,
        world: AdmissionWorld,
    ) -> None:
        with self.assertLogs("cratedigger", level="WARNING"):
            pre_snapshot, refreshed_rows, expected = _build_admission_inputs(world)
            admitted = _admit_download_incarnations(
                pre_snapshot,
                refreshed_rows,
                refreshed_phase="generated post-ingest refresh",
            )
            decoded_refreshed = _decode_valid_download_incarnations(
                refreshed_rows,
                phase="request-id-only mutant",
            )
        actual = tuple(
            (row["id"], state.enqueued_at)
            for row, state in admitted
        )
        assert_exact_admission(expected, actual)

        pre_ids = {row["id"] for row, _state in pre_snapshot}
        request_id_only_mutant = tuple(
            (row["id"], state.enqueued_at)
            for row, state in decoded_refreshed
            if row["id"] in pre_ids
        )
        with self.assertRaisesRegex(
            AssertionError,
            _anchored_prefix(
                "post-event poll admission diverged from exact incarnation "
                "oracle: expected="
            ),
        ):
            assert_exact_admission(expected, request_id_only_mutant)

    @given(world=handoff_worlds())
    @example(world=HandoffWorld(
        current_witness="2026-07-29T08:00:00+08:00",
        attempted_witness="2026-07-29T00:00:00Z",
        canonical_path=_DETERMINISTIC_PATH,
    ))
    # Issue #1094: the exact-commit arm owns every post-handoff clause
    # (publication, canonical path, and both refused post-handoff writers).
    # Pinned so a future edit to this body cannot reshuffle it out of the
    # derandomized gating tier.
    @example(world=HandoffWorld(
        current_witness="2026-07-29T08:00:00+08:00",
        attempted_witness="2026-07-29T08:00:00+08:00",
        canonical_path="/processing/albums/Ártist - 音 [pressing]",
    ))
    def test_handoff_requires_exact_textual_witness(
        self,
        world: HandoffWorld,
    ) -> None:
        _exercise_handoff(world)

    @given(world=handoff_rejection_worlds())
    @example(world=HandoffRejectionWorld(
        kind="lock_unavailable",
        witness="2026-07-29T00:00:00Z",
        canonical_path=_DETERMINISTIC_PATH,
    ))
    # Issue #1094: each rejection kind is the only world that attributes its
    # own outcome/metadata/job clauses, so all four are pinned into the
    # derandomized gating tier rather than left to the sweep.
    @example(world=HandoffRejectionWorld(
        kind="missing_state",
        witness="2026-07-29T00:00:00Z",
        canonical_path=_DETERMINISTIC_PATH,
    ))
    @example(world=HandoffRejectionWorld(
        kind="non_downloading",
        witness="2026-07-29T00:00:00Z",
        canonical_path="/processing/albums/Ártist - 音 [rejected]",
    ))
    @example(world=HandoffRejectionWorld(
        kind="active_conflict",
        witness="2026-07-29T00:00:00Z",
        canonical_path=_DETERMINISTIC_PATH,
    ))
    def test_non_admissible_handoffs_preserve_all_state(
        self,
        world: HandoffRejectionWorld,
    ) -> None:
        _exercise_rejected_handoff(world)


_PLANTED_UPDATED_AT = datetime(2030, 1, 1, tzinfo=UTC)
_EXACT_HANDOFF_WORLD = HandoffWorld(
    current_witness="2026-07-29T08:00:00+08:00",
    attempted_witness="2026-07-29T08:00:00+08:00",
    canonical_path=_DETERMINISTIC_PATH,
)
_STALE_HANDOFF_WORLD = HandoffWorld(
    current_witness="2026-07-29T08:00:01+08:00",
    attempted_witness="2026-07-29T00:00:00Z",
    canonical_path=_DETERMINISTIC_PATH,
)


def _rejection_world(kind: HandoffRejectionKind) -> HandoffRejectionWorld:
    return HandoffRejectionWorld(
        kind=kind,
        witness="2026-07-29T00:00:00Z",
        canonical_path=_DETERMINISTIC_PATH,
    )


class _PlantedHandoffFake(FakePipelineDB):
    """Run the real handoff transcript, then plant exactly one defect."""

    def _plant(
        self,
        request_id: int,
        result: AutomationHandoffResult,
    ) -> AutomationHandoffResult:
        del request_id
        return result

    def handoff_automation_import(
        self,
        *,
        request_id: int,
        expected_enqueued_at: str,
        canonical_path: str,
        message: str,
    ) -> AutomationHandoffResult:
        return self._plant(request_id, super().handoff_automation_import(
            request_id=request_id,
            expected_enqueued_at=expected_enqueued_at,
            canonical_path=canonical_path,
            message=message,
        ))


class _NoDownloadingEntry(FakePipelineDB):
    """The fixture can never enter ``downloading``."""

    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool:
        del request_id, state_json, expected_status
        return False


class _UnfencedHandoff(FakePipelineDB):
    """The exact-witness fence is disabled, so a stale attempt commits."""

    def _automation_handoff_enforce_witness(self) -> bool:
        return False


class _NeverCommittingHandoff(FakePipelineDB):
    """Every handoff attempt reports a witness mismatch."""

    def handoff_automation_import(
        self,
        *,
        request_id: int,
        expected_enqueued_at: str,
        canonical_path: str,
        message: str,
    ) -> AutomationHandoffResult:
        del request_id, expected_enqueued_at, canonical_path, message
        return AutomationHandoffResult("witness_mismatch")


class _CommitsWithoutJobRecord(_PlantedHandoffFake):
    """A committed handoff returns no job record."""

    def _plant(
        self,
        request_id: int,
        result: AutomationHandoffResult,
    ) -> AutomationHandoffResult:
        del request_id
        if result.committed:
            return AutomationHandoffResult(result.outcome)
        return result


class _PublishesWrongCanonicalPath(_PlantedHandoffFake):
    """Ownership publishes a canonical path the caller never asked for."""

    def _plant(
        self,
        request_id: int,
        result: AutomationHandoffResult,
    ) -> AutomationHandoffResult:
        if result.committed:
            state = self.request(request_id)["active_download_state"]
            if isinstance(state, dict):
                state["current_path"] = "/processing/albums/another-attempt"
        return result


class _RejectionReportsWrongOutcome(_PlantedHandoffFake):
    """A refused handoff reports the wrong rejection tag."""

    def _plant(
        self,
        request_id: int,
        result: AutomationHandoffResult,
    ) -> AutomationHandoffResult:
        del request_id
        if not result.committed:
            return AutomationHandoffResult("request_missing")
        return result


class _RejectionTouchesRequestRow(_PlantedHandoffFake):
    """A refused handoff still stamps the request row."""

    def _plant(
        self,
        request_id: int,
        result: AutomationHandoffResult,
    ) -> AutomationHandoffResult:
        if not result.committed:
            self.request(request_id)["updated_at"] = _PLANTED_UPDATED_AT
        return result


class _RejectionCreatesJob(_PlantedHandoffFake):
    """A refused handoff still mints an import job."""

    def _plant(
        self,
        request_id: int,
        result: AutomationHandoffResult,
    ) -> AutomationHandoffResult:
        if not result.committed:
            self._append_import_job(
                "automation_import",
                request_id=request_id,
                dedupe_key=None,
                payload={},
                message="planted rejection job",
            )
        return result


class _CasCrossesHandoff(FakePipelineDB):
    """The whole-state writer accepts a row the processor already owns."""

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool:
        del state_json, expected_enqueued_at
        return self.request(request_id)["status"] in ("downloading", "processing")


class _ResetCrossesHandoff(FakePipelineDB):
    """The reset writer accepts a row the processor already owns."""

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: object,
    ) -> bool:
        del request_id, expected_status, fields
        return True


class _RejectedWriterTouchesRow(FakePipelineDB):
    """A refused whole-state write still stamps the row it refused."""

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool:
        self.request(request_id)["updated_at"] = _PLANTED_UPDATED_AT
        return super().update_download_state_if_downloading(
            request_id,
            state_json,
            expected_enqueued_at=expected_enqueued_at,
        )


class TestIncarnationInvariantCheckersTripOnKnownBad(unittest.TestCase):
    """Committed known-bad inputs qualify every U5 invariant checker.

    Issue #1094 per-clause proof: each test below names the minimal world that
    makes exactly one clause fire while every earlier clause in the same
    function passes, and asserts that clause's own anchored message. A bare
    ``assertRaises`` or a loose substring would let a sibling clause satisfy
    the assertion.
    """

    @staticmethod
    def _planted_applied_row(
        before: Mapping[str, object],
        outgoing: ActiveDownloadState,
    ) -> dict[str, object]:
        after = copy.deepcopy(dict(before))
        after["active_download_state"] = _state_dict(outgoing)
        after["updated_at"] = datetime(2030, 1, 1, tzinfo=UTC)
        return after

    _PREDICATE_CLAUSE = (
        "whole-state CAS result diverged from status/stored/outgoing witness "
        "predicates: expected={expected} actual={actual}"
    )

    @staticmethod
    def _published_state() -> dict[str, object]:
        state = _state_dict(_admission_state("2026-07-29T00:00:00Z"))
        state["processing_started_at"] = "2026-07-29T00:00:01+00:00"
        state["current_path"] = _DETERMINISTIC_PATH
        return state

    @classmethod
    def _published_row(cls, **overrides: object) -> dict[str, object]:
        row = _admission_row(
            1,
            witness="2026-07-29T00:00:00Z",
            status="processing",
            raw_state=cls._published_state(),
        )
        row["active_automation_import_job_id"] = 7
        row.update(overrides)
        return row

    def test_checker_kills_missing_status_predicate(self) -> None:
        witness = "2026-07-28T01:00:00Z"
        outgoing = _admission_state(witness)
        before = _admission_row(1, witness=witness, status="wanted")
        with self.assertRaisesRegex(
            AssertionError,
            _anchored(self._PREDICATE_CLAUSE.format(
                expected=False, actual=True)),
        ):
            assert_witnessed_write_contract(
                before_row=before,
                outgoing_state=outgoing,
                expected_witness=witness,
                applied=True,
                after_row=self._planted_applied_row(before, outgoing),
            )

    def test_checker_kills_missing_stored_witness_predicate(self) -> None:
        witness_a = "2026-07-28T01:00:00Z"
        outgoing_a = _admission_state(witness_a)
        before_b = _admission_row(
            1,
            witness="2026-07-28T01:00:01Z",
        )
        with self.assertRaisesRegex(
            AssertionError,
            _anchored(self._PREDICATE_CLAUSE.format(
                expected=False, actual=True)),
        ):
            assert_witnessed_write_contract(
                before_row=before_b,
                outgoing_state=outgoing_a,
                expected_witness=witness_a,
                applied=True,
                after_row=self._planted_applied_row(before_b, outgoing_a),
            )

    def test_checker_kills_missing_outgoing_witness_predicate(self) -> None:
        witness_a = "2026-07-28T01:00:00Z"
        outgoing_b = _admission_state("2026-07-28T01:00:01Z")
        before_a = _admission_row(1, witness=witness_a)
        with self.assertRaisesRegex(
            AssertionError,
            _anchored(self._PREDICATE_CLAUSE.format(
                expected=False, actual=True)),
        ):
            assert_witnessed_write_contract(
                before_row=before_a,
                outgoing_state=outgoing_b,
                expected_witness=witness_a,
                applied=True,
                after_row=self._planted_applied_row(before_a, outgoing_b),
            )

    def test_checker_kills_fail_closed_witnessed_write(self) -> None:
        """All three predicates hold, so refusing the write is a violation."""
        witness = "2026-07-28T01:00:00Z"
        outgoing = _admission_state(witness)
        before = _admission_row(1, witness=witness)
        with self.assertRaisesRegex(
            AssertionError,
            _anchored(self._PREDICATE_CLAUSE.format(
                expected=True, actual=False)),
        ):
            assert_witnessed_write_contract(
                before_row=before,
                outgoing_state=outgoing,
                expected_witness=witness,
                applied=False,
                after_row=copy.deepcopy(before),
            )

    def test_checker_kills_rejected_write_that_changed_the_row(self) -> None:
        """The CAS result agrees; the refused write still moved the row."""
        witness = "2026-07-28T01:00:00Z"
        outgoing = _admission_state(witness)
        before = _admission_row(1, witness=witness, status="wanted")
        after = copy.deepcopy(before)
        after["updated_at"] = _PLANTED_UPDATED_AT
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("rejected whole-state CAS changed row state or metadata"),
        ):
            assert_witnessed_write_contract(
                before_row=before,
                outgoing_state=outgoing,
                expected_witness=witness,
                applied=False,
                after_row=after,
            )

    def test_checker_kills_accepted_write_beyond_state_and_stamp(self) -> None:
        """The accepted write carried a field the CAS may not touch."""
        witness = "2026-07-28T01:00:00Z"
        outgoing = _admission_state(witness)
        before = _admission_row(1, witness=witness)
        after = self._planted_applied_row(before, outgoing)
        after["validation_attempts"] = 3
        with self.assertRaisesRegex(
            AssertionError,
            _anchored(
                "accepted whole-state CAS changed fields beyond "
                "state/updated_at"
            ),
        ):
            assert_witnessed_write_contract(
                before_row=before,
                outgoing_state=outgoing,
                expected_witness=witness,
                applied=True,
                after_row=after,
            )

    def test_witnessed_write_checker_accepts_lawful_worlds(self) -> None:
        """Must-still-work control: the two lawful shapes raise nothing."""
        witness = "2026-07-28T01:00:00Z"
        outgoing = _admission_state(witness)
        accepted_before = _admission_row(1, witness=witness)
        assert_witnessed_write_contract(
            before_row=accepted_before,
            outgoing_state=outgoing,
            expected_witness=witness,
            applied=True,
            after_row=self._planted_applied_row(accepted_before, outgoing),
        )
        rejected_before = _admission_row(1, witness=witness, status="wanted")
        assert_witnessed_write_contract(
            before_row=rejected_before,
            outgoing_state=outgoing,
            expected_witness=witness,
            applied=False,
            after_row=copy.deepcopy(rejected_before),
        )

    def test_checker_kills_request_id_only_admission(self) -> None:
        expected = ((1, "2026-07-28T01:00:00Z"),)
        request_id_only = (
            (1, "2026-07-28T01:00:00Z"),
            (2, "2026-07-28T01:00:01Z"),
        )
        with self.assertRaisesRegex(
            AssertionError,
            _anchored(
                "post-event poll admission diverged from exact incarnation "
                f"oracle: expected={expected!r} actual={request_id_only!r}"
            ),
        ):
            assert_exact_admission(expected, request_id_only)

    def test_checker_kills_stale_handoff_mutant(self) -> None:
        """Both disjuncts of the stale-handoff clause, one world each."""
        before = make_request_row(
            status="downloading",
            active_download_state={"enqueued_at": "B"},
        )
        published = copy.deepcopy(before)
        published["status"] = "processing"
        published["active_automation_import_job_id"] = 1
        published_state = published["active_download_state"]
        if isinstance(published_state, dict):
            published_state["processing_started_at"] = "now"
        cases: tuple[tuple[str, dict[str, object], int], ...] = (
            ("row changed only", published, 0),
            ("job created only", copy.deepcopy(before), 1),
            ("row changed and job created", published, 1),
        )
        for description, after, job_count in cases:
            with self.subTest(world=description), self.assertRaisesRegex(
                AssertionError,
                _anchored("stale handoff produced an observable change"),
            ):
                assert_handoff_contract(
                    exact=False,
                    before=before,
                    after=after,
                    job_count=job_count,
                )
        assert_handoff_contract(
            exact=False,
            before=before,
            after=copy.deepcopy(before),
            job_count=0,
        )

    def test_checker_kills_incomplete_exact_handoff_publication(self) -> None:
        """Every disjunct of the exact-publication clause, one world each."""
        stampless_state = self._published_state()
        del stampless_state["processing_started_at"]
        cases: tuple[tuple[str, dict[str, object], int], ...] = (
            ("status is not processing", self._published_row(
                status="downloading"), 1),
            ("owner id is not an int", self._published_row(
                active_automation_import_job_id="7"), 1),
            ("owner id is not positive", self._published_row(
                active_automation_import_job_id=0), 1),
            ("state is not an object", self._published_row(
                active_download_state=None), 1),
            ("processing stamp missing", self._published_row(
                active_download_state=stampless_state), 1),
            ("no job published", self._published_row(), 0),
            ("two jobs published", self._published_row(), 2),
        )
        for description, after, job_count in cases:
            with self.subTest(world=description), self.assertRaisesRegex(
                AssertionError,
                _anchored("exact handoff did not publish one processor owner"),
            ):
                assert_handoff_contract(
                    exact=True,
                    before=self._published_row(),
                    after=after,
                    job_count=job_count,
                )
        assert_handoff_contract(
            exact=True,
            before=self._published_row(),
            after=self._published_row(),
            job_count=1,
        )

    def test_transcript_driver_rejects_alphabet_drift(self) -> None:
        """Fail-closed legislation: a future strategy edit cannot go quiet."""
        with self.assertRaisesRegex(
            AssertionError,
            _anchored_prefix("operation alphabet drifted outside PR1: "),
        ):
            _exercise_transcript(TranscriptWorld(
                witness_a="2026-07-28T01:00:00.000000Z",
                witness_b="2026-07-28T09:00:01.000000+08:00",
                operation_order=("enqueue",),
                username="peer",
                filename="Music\\Artist\\Album\\01 track.flac",
                size=1000,
                progress_bytes=400,
            ))

    def test_drivers_kill_fixtures_that_never_enter_downloading(self) -> None:
        with self.subTest(driver="handoff"), self.assertRaisesRegex(
            AssertionError,
            _anchored("generated fixture failed to enter downloading"),
        ):
            _exercise_handoff(
                _EXACT_HANDOFF_WORLD,
                db_factory=_NoDownloadingEntry,
            )
        with self.subTest(driver="rejection"), self.assertRaisesRegex(
            AssertionError,
            _anchored("rejection fixture failed to enter downloading"),
        ):
            _exercise_rejected_handoff(
                _rejection_world("missing_state"),
                db_factory=_NoDownloadingEntry,
            )

    def test_driver_kills_unfenced_stale_handoff_commit(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("handoff outcome 'committed' disagreed with exact=False"),
        ):
            _exercise_handoff(
                _STALE_HANDOFF_WORLD,
                db_factory=_UnfencedHandoff,
            )

    def test_driver_kills_committed_handoff_without_a_job_record(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("committed handoff returned no job record"),
        ):
            _exercise_handoff(
                _EXACT_HANDOFF_WORLD,
                db_factory=_CommitsWithoutJobRecord,
            )

    def test_driver_kills_wrong_published_canonical_path(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("handoff owner, job, and canonical path diverged"),
        ):
            _exercise_handoff(
                _EXACT_HANDOFF_WORLD,
                db_factory=_PublishesWrongCanonicalPath,
            )

    def test_driver_kills_post_handoff_whole_state_writer(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("poll/event writer crossed the handoff"),
        ):
            _exercise_handoff(
                _EXACT_HANDOFF_WORLD,
                db_factory=_CasCrossesHandoff,
            )

    def test_driver_kills_post_handoff_reset_writer(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("reset writer crossed the handoff"),
        ):
            _exercise_handoff(
                _EXACT_HANDOFF_WORLD,
                db_factory=_ResetCrossesHandoff,
            )

    def test_driver_kills_refused_writer_that_stamps_the_row(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("rejected post-handoff writer changed metadata"),
        ):
            _exercise_handoff(
                _EXACT_HANDOFF_WORLD,
                db_factory=_RejectedWriterTouchesRow,
            )

    def test_driver_kills_conflict_seed_that_never_commits(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("conflict fixture failed to create active job"),
        ):
            _exercise_rejected_handoff(
                _rejection_world("active_conflict"),
                db_factory=_NeverCommittingHandoff,
            )

    def test_driver_kills_wrong_rejection_outcome(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored(
                "missing_state returned request_missing, expected missing_state"
            ),
        ):
            _exercise_rejected_handoff(
                _rejection_world("missing_state"),
                db_factory=_RejectionReportsWrongOutcome,
            )

    def test_driver_kills_rejection_that_stamps_the_request(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("non_downloading changed request metadata"),
        ):
            _exercise_rejected_handoff(
                _rejection_world("non_downloading"),
                db_factory=_RejectionTouchesRequestRow,
            )

    def test_driver_kills_rejection_that_creates_a_job(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            _anchored("non_downloading created or changed a job"),
        ):
            _exercise_rejected_handoff(
                _rejection_world("non_downloading"),
                db_factory=_RejectionCreatesJob,
            )
