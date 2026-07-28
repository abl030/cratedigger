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
import unittest
from collections.abc import Mapping, Sequence
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
from lib.pipeline_db.rows import AlbumRequestRow
from lib.quality import ActiveDownloadFileState, ActiveDownloadState
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

PayloadFamily = Literal["enqueue", "event", "harvest", "poll"]
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


def _exercise_transcript(world: TranscriptWorld) -> None:
    db = FakePipelineDB()
    _seed_current_b(db, world)
    executed: list[PayloadFamily] = []

    for family in world.operation_order:
        stale_a = _payload_state(
            world,
            witness=world.witness_a,
            family=family,
        )
        before_a = copy.deepcopy(db.request(1))
        applied_a = db.update_download_state_if_downloading(
            1,
            stale_a.to_json(),
            expected_enqueued_at=world.witness_a,
        )
        after_a = copy.deepcopy(db.request(1))
        assert_witnessed_write_contract(
            before_row=before_a,
            outgoing_state=stale_a,
            expected_witness=world.witness_a,
            applied=applied_a,
            after_row=after_a,
        )

        current_b = _payload_state(
            world,
            witness=world.witness_b,
            family=family,
        )
        before_b = copy.deepcopy(db.request(1))
        applied_b = db.update_download_state_if_downloading(
            1,
            current_b.to_json(),
            expected_enqueued_at=world.witness_b,
        )
        after_b = copy.deepcopy(db.request(1))
        assert_witnessed_write_contract(
            before_row=before_b,
            outgoing_state=current_b,
            expected_witness=world.witness_b,
            applied=applied_b,
            after_row=after_b,
        )
        executed.append(family)

    if tuple(executed) != world.operation_order:
        raise AssertionError("not every generated operation was executed")
    if set(executed) != set(_PAYLOAD_FAMILIES):
        raise AssertionError(
            f"operation alphabet drifted outside PR1: {executed!r}"
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
        before = copy.deepcopy(status_db.request(1))
        applied = status_db.update_download_state_if_downloading(
            1,
            matching_b.to_json(),
            expected_enqueued_at=world.witness_b,
        )
        after = copy.deepcopy(status_db.request(1))
        assert_witnessed_write_contract(
            before_row=before,
            outgoing_state=matching_b,
            expected_witness=world.witness_b,
            applied=applied,
            after_row=after,
        )

    outgoing_db = FakePipelineDB()
    _seed_current_b(outgoing_db, world)
    for family in world.operation_order:
        mismatched_outgoing_a = _payload_state(
            world,
            witness=world.witness_a,
            family=family,
        )
        before = copy.deepcopy(outgoing_db.request(1))
        applied = outgoing_db.update_download_state_if_downloading(
            1,
            mismatched_outgoing_a.to_json(),
            expected_enqueued_at=world.witness_b,
        )
        after = copy.deepcopy(outgoing_db.request(1))
        assert_witnessed_write_contract(
            before_row=before,
            outgoing_state=mismatched_outgoing_a,
            expected_witness=world.witness_b,
            applied=applied,
            after_row=after,
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
    pre_db = FakePipelineDB()
    for request_id in range(1, 9):
        witness = world.witness_b if request_id == 8 else world.witness_a
        pre_db.seed_request(_admission_row(request_id, witness=witness))
    pre_snapshot = _decode_valid_download_incarnations(
        pre_db.get_downloading(),
        phase="generated pre-snapshot",
    )

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
        pre_snapshot, refreshed_rows, expected = _build_admission_inputs(world)
        with self.assertLogs("cratedigger", level="WARNING"):
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
    def test_post_event_cohort_matches_exact_pair_oracle(
        self,
        world: AdmissionWorld,
    ) -> None:
        pre_snapshot, refreshed_rows, expected = _build_admission_inputs(world)
        with self.assertLogs("cratedigger", level="WARNING"):
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
            "exact incarnation oracle",
        ):
            assert_exact_admission(expected, request_id_only_mutant)


class TestIncarnationInvariantCheckersTripOnKnownBad(unittest.TestCase):
    """Committed known-bad inputs qualify every U5 invariant checker."""

    @staticmethod
    def _planted_applied_row(
        before: Mapping[str, object],
        outgoing: ActiveDownloadState,
    ) -> dict[str, object]:
        after = copy.deepcopy(dict(before))
        after["active_download_state"] = _state_dict(outgoing)
        after["updated_at"] = datetime(2030, 1, 1, tzinfo=UTC)
        return after

    def test_checker_kills_missing_status_predicate(self) -> None:
        witness = "2026-07-28T01:00:00Z"
        outgoing = _admission_state(witness)
        before = _admission_row(1, witness=witness, status="wanted")
        with self.assertRaisesRegex(AssertionError, "predicates"):
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
        with self.assertRaisesRegex(AssertionError, "predicates"):
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
        with self.assertRaisesRegex(AssertionError, "predicates"):
            assert_witnessed_write_contract(
                before_row=before_a,
                outgoing_state=outgoing_b,
                expected_witness=witness_a,
                applied=True,
                after_row=self._planted_applied_row(before_a, outgoing_b),
            )

    def test_checker_kills_request_id_only_admission(self) -> None:
        expected = ((1, "2026-07-28T01:00:00Z"),)
        request_id_only = (
            (1, "2026-07-28T01:00:00Z"),
            (2, "2026-07-28T01:00:01Z"),
        )
        with self.assertRaisesRegex(
            AssertionError,
            "exact incarnation oracle",
        ):
            assert_exact_admission(expected, request_id_only)
