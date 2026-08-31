"""State transition validation and side-effect declarations.

Pure functions for transition validation. The imperative apply_transition()
delegates to pipeline_db methods and is the single entry point for all
state mutations.

Ordinary statuses: wanted, downloading, imported, unsearchable. ``processing``
is a processor-owned active state deliberately absent from ``RequestStatus``
and ``VALID_TRANSITIONS``; only owner-aware database bundles may cross that
private edge. ``initializing`` is a deliberately non-runnable creation state;
its publication CAS belongs only to ``RequestCreationService`` and is not an
ordinary lifecycle edge.
Terminal audit status: replaced (no outgoing lifecycle transitions).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import Enum
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Protocol,
    runtime_checkable,
)

from lib.json_narrow import is_str_object_dict

if TYPE_CHECKING:
    from lib.pipeline_db._shared import ProcessingOwnerProjection
    from lib.pipeline_db.rows import AlbumRequestRow


@runtime_checkable
class TransitionsDB(Protocol):
    """The PipelineDB surface the status-transition engine uses (#409).

    Parity tests live in ``tests/test_transitions.py``.
    """

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool: ...

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> bool: ...

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: Any,
    ) -> bool: ...

    def record_attempt(
        self,
        request_id: int,
        attempt_type: str,
        *,
        expected_status: str,
    ) -> bool: ...

    def mark_imported_with_rescue(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool: ...

    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool: ...

    def compare_request_status(
        self,
        request_id: int,
        *,
        expected_status: str,
    ) -> bool: ...


class _OmittedField:
    """Sentinel for distinguishing omitted fields from explicit NULL writes."""


_OMITTED = _OmittedField()


RequestStatus = Literal[
    "wanted", "downloading", "imported", "unsearchable"]


class TransitionConflictKind(str, Enum):
    """Machine-readable reason a request transition did not apply."""

    not_found = "not_found"
    invalid_edge = "invalid_edge"
    stale_source = "stale_source"
    processing_locked = "processing_locked"


@dataclass(frozen=True)
class TransitionApplied:
    request_id: int
    from_status: str
    target_status: str


@dataclass(frozen=True)
class TransitionConflict:
    request_id: int
    target_status: str
    kind: TransitionConflictKind
    expected_status: str | None
    actual_status: str | None
    processing_owner: ProcessingOwnerProjection | None = None


type TransitionResult = TransitionApplied | TransitionConflict


def processing_owner_payload(
    owner: ProcessingOwnerProjection | None,
) -> dict[str, object] | None:
    """Serialize the exact owner identically across API and CLI conflicts."""
    if owner is None:
        return None
    return {
        "job_id": owner.job_id,
        "status": owner.status,
        "preview_status": owner.preview_status,
    }


def transition_conflict_payload(
    conflict: TransitionConflict,
) -> dict[str, object]:
    """Return the shared wire payload for one lifecycle conflict."""
    payload: dict[str, object] = {
        "error": "transition_conflict",
        "reason": conflict.kind.value,
        "request_id": conflict.request_id,
        "expected_status": conflict.expected_status,
        "actual_status": conflict.actual_status,
        "target_status": conflict.target_status,
    }
    owner = processing_owner_payload(conflict.processing_owner)
    if owner is not None:
        payload["processing_owner"] = owner
    return payload


def processing_locked_conflict(
    row: Mapping[str, object] | None,
    request_id: int,
    target_status: str,
    *,
    expected_status: str | None = None,
) -> TransitionConflict | None:
    """Return the canonical exact-owner conflict for a processing row.

    Request readers validate and attach the exact joined owner projection.
    This helper converts that wire projection back to its one typed shape so
    every operator service can return the same conflict without inferring an
    owner from paths or a latest import job.
    """
    if row is None or row.get("status") != "processing":
        return None
    owner = row.get("processing_owner")
    if not is_str_object_dict(owner):
        raise TypeError(
            "processing request is missing its exact owner projection"
        )
    from lib.pipeline_db._shared import ProcessingOwnerProjection

    job_id = owner.get("job_id")
    owner_status = owner.get("status")
    preview_status = owner.get("preview_status")
    if (
        not isinstance(job_id, int)
        or isinstance(job_id, bool)
        or not isinstance(owner_status, str)
        or not isinstance(preview_status, str)
    ):
        raise TypeError(
            "processing request has a malformed exact owner projection"
        )
    return TransitionConflict(
        request_id=request_id,
        target_status=target_status,
        kind=TransitionConflictKind.processing_locked,
        expected_status=expected_status,
        actual_status="processing",
        processing_owner=ProcessingOwnerProjection(
            job_id=job_id,
            status=owner_status,
            preview_status=preview_status,
        ),
    )


def transition_conflict_http_status(conflict: TransitionConflict) -> int:
    """Map one lifecycle conflict onto its HTTP status.

    Shared by the HTTP conflict adapters and the CLI (whose exit code is
    derived from this status through the repository convention in
    ``lib/surface_outcomes.py``), so both surfaces classify a conflict
    identically: a vanished row is not-found, everything else wrong-state.
    """
    return 404 if conflict.kind is TransitionConflictKind.not_found else 409


def request_fields_cas_conflict(
    db: TransitionsDB,
    request_id: int,
    *,
    expected_status: str,
) -> TransitionConflict:
    """Explain one metadata compare-and-set miss through the transition contract.

    ``update_request_fields`` returns only ``applied``; when it misses, this
    re-read names why — the exact processing owner, a vanished row, or a
    concurrent lifecycle change — identically for every operator surface.
    """
    row = db.get_request(request_id)
    return processing_locked_conflict(
        row,
        request_id,
        expected_status,
        expected_status=expected_status,
    ) or TransitionConflict(
        request_id=request_id,
        target_status=expected_status,
        kind=(
            TransitionConflictKind.not_found
            if row is None
            else TransitionConflictKind.stale_source
        ),
        expected_status=expected_status,
        actual_status=None if row is None else str(row["status"]),
    )


def publish_initialized_request(
    db: TransitionsDB,
    request_id: int,
    *,
    fields: Mapping[str, object],
) -> TransitionResult:
    """Publish one fully initialized request through its dedicated CAS.

    This is intentionally separate from the ordinary lifecycle graph:
    generic operator adapters must never acquire authority to move a partial
    ``initializing`` row into runnable ``wanted`` state.
    """
    expected_status = "initializing"
    target_status = "wanted"
    row = db.get_request(request_id)
    if row is None:
        return TransitionConflict(
            request_id=request_id,
            target_status=target_status,
            kind=TransitionConflictKind.not_found,
            expected_status=expected_status,
            actual_status=None,
        )
    actual_status = str(row["status"])
    if actual_status != expected_status:
        return TransitionConflict(
            request_id=request_id,
            target_status=target_status,
            kind=TransitionConflictKind.stale_source,
            expected_status=expected_status,
            actual_status=actual_status,
        )
    if db.update_status(
        request_id,
        target_status,
        expected_status=expected_status,
        **dict(fields),
    ):
        return TransitionApplied(request_id, expected_status, target_status)
    refreshed = db.get_request(request_id)
    return TransitionConflict(
        request_id=request_id,
        target_status=target_status,
        kind=(
            TransitionConflictKind.not_found
            if refreshed is None
            else TransitionConflictKind.stale_source
        ),
        expected_status=expected_status,
        actual_status=(
            str(refreshed["status"]) if refreshed is not None else None
        ),
    )


class RequestTransitionConflict(RuntimeError):
    """Raised by imperative callers that cannot continue after a conflict."""

    def __init__(self, conflict: TransitionConflict) -> None:
        self.conflict = conflict
        super().__init__(
            f"request {conflict.request_id} transition to "
            f"{conflict.target_status!r} conflicted: {conflict.kind.value} "
            f"(expected={conflict.expected_status!r}, "
            f"actual={conflict.actual_status!r})"
        )


def require_transition_applied(result: TransitionResult) -> TransitionApplied:
    """Return the applied result or stop a worker before dependent effects."""
    if isinstance(result, TransitionConflict):
        raise RequestTransitionConflict(result)
    return result


def _explicit_fields(**values: object) -> dict[str, object]:
    return {
        key: value
        for key, value in values.items()
        if value is not _OMITTED
    }


_WANTED_FIELDS = frozenset({
    "min_bitrate",
    "prev_min_bitrate",
    "priority_started_at",
    "search_filetype_override",
})

_IMPORTED_FIELDS = frozenset({
    "beets_distance",
    "beets_scenario",
    "current_spectral_bitrate",
    "current_spectral_grade",
    "current_lossless_source_v0_probe_avg_bitrate",
    "current_lossless_source_v0_probe_median_bitrate",
    "current_lossless_source_v0_probe_min_bitrate",
    "final_format",
    "last_download_spectral_bitrate",
    "last_download_spectral_grade",
    # Issue #1241: an explicit None clears the operator's incomplete mark
    # atomically with the imported status when the accepted candidate was
    # proven whole by beets. Only ever written as None; setting the mark is
    # the operator surfaces' job (set_marked_incomplete).
    "marked_incomplete_at",
    "min_bitrate",
    "prev_min_bitrate",
    "search_filetype_override",
    "verified_lossless",
})

_DOWNLOADING_FIELDS = frozenset({"state_json"})
_UNSEARCHABLE_FIELDS = _WANTED_FIELDS
_RESERVED_FIELDS = frozenset({"from_status", "attempt_type"})


def _reject_unknown_fields(
    target_status: str,
    fields: Mapping[str, object],
    allowed: frozenset[str],
) -> None:
    unknown = set(fields) - allowed
    if unknown:
        names = ", ".join(sorted(unknown))
        raise ValueError(
            f"{target_status} transitions do not accept fields: {names}")


def _validate_transition_fields(
    target_status: str,
    fields: Mapping[str, object],
) -> None:
    reserved = set(fields) & _RESERVED_FIELDS
    if reserved:
        names = ", ".join(sorted(reserved))
        raise ValueError(
            "RequestTransition.fields must not include reserved keys: "
            f"{names}. Use the explicit RequestTransition fields instead."
        )

    if target_status == "wanted":
        _reject_unknown_fields(target_status, fields, _WANTED_FIELDS)
        return
    if target_status == "imported":
        _reject_unknown_fields(target_status, fields, _IMPORTED_FIELDS)
        return
    if target_status == "unsearchable":
        _reject_unknown_fields(target_status, fields, _UNSEARCHABLE_FIELDS)
        return
    if target_status == "downloading":
        _reject_unknown_fields(target_status, fields, _DOWNLOADING_FIELDS)
        if "state_json" not in fields or fields["state_json"] is None:
            raise ValueError("state_json is required for downloading transitions")
        if not isinstance(fields["state_json"], str):
            raise ValueError("state_json must be a string")
        return
    raise ValueError(f"Unknown request status: {target_status!r}")


@dataclass(frozen=True)
class RequestTransition:
    """A typed command for one album_requests state transition."""

    target_status: RequestStatus
    from_status: str | None = None
    attempt_type: str | None = None
    fields: Mapping[str, object] = field(
        default_factory=dict[str, object],
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))

    @classmethod
    def to_wanted(
        cls,
        *,
        from_status: str | None = None,
        attempt_type: str | None = None,
        search_filetype_override: object = _OMITTED,
        min_bitrate: object = _OMITTED,
        prev_min_bitrate: object = _OMITTED,
        priority_started_at: object = _OMITTED,
    ) -> RequestTransition:
        return cls.to_wanted_fields(
            from_status=from_status,
            attempt_type=attempt_type,
            fields=_explicit_fields(
                search_filetype_override=search_filetype_override,
                min_bitrate=min_bitrate,
                prev_min_bitrate=prev_min_bitrate,
                priority_started_at=priority_started_at,
            ),
        )

    @classmethod
    def to_wanted_fields(
        cls,
        *,
        from_status: str | None = None,
        attempt_type: str | None = None,
        fields: Mapping[str, object],
    ) -> RequestTransition:
        _reject_unknown_fields("wanted", fields, _WANTED_FIELDS)
        return cls(
            target_status="wanted",
            from_status=from_status,
            attempt_type=attempt_type,
            fields=fields,
        )

    @classmethod
    def to_downloading(
        cls,
        *,
        state_json: str,
        from_status: str | None = None,
    ) -> RequestTransition:
        return cls(
            target_status="downloading",
            from_status=from_status,
            fields={"state_json": state_json},
        )

    @classmethod
    def to_imported(
        cls,
        *,
        from_status: str | None = None,
        beets_distance: object = _OMITTED,
        beets_scenario: object = _OMITTED,
        current_spectral_bitrate: object = _OMITTED,
        current_spectral_grade: object = _OMITTED,
        current_lossless_source_v0_probe_avg_bitrate: object = _OMITTED,
        current_lossless_source_v0_probe_median_bitrate: object = _OMITTED,
        current_lossless_source_v0_probe_min_bitrate: object = _OMITTED,
        final_format: object = _OMITTED,
        last_download_spectral_bitrate: object = _OMITTED,
        last_download_spectral_grade: object = _OMITTED,
        marked_incomplete_at: object = _OMITTED,
        min_bitrate: object = _OMITTED,
        prev_min_bitrate: object = _OMITTED,
        search_filetype_override: object = _OMITTED,
        verified_lossless: object = _OMITTED,
    ) -> RequestTransition:
        return cls.to_imported_fields(
            from_status=from_status,
            fields=_explicit_fields(
                beets_distance=beets_distance,
                beets_scenario=beets_scenario,
                current_spectral_bitrate=current_spectral_bitrate,
                current_spectral_grade=current_spectral_grade,
                current_lossless_source_v0_probe_avg_bitrate=(
                    current_lossless_source_v0_probe_avg_bitrate
                ),
                current_lossless_source_v0_probe_median_bitrate=(
                    current_lossless_source_v0_probe_median_bitrate
                ),
                current_lossless_source_v0_probe_min_bitrate=(
                    current_lossless_source_v0_probe_min_bitrate
                ),
                final_format=final_format,
                last_download_spectral_bitrate=last_download_spectral_bitrate,
                last_download_spectral_grade=last_download_spectral_grade,
                marked_incomplete_at=marked_incomplete_at,
                min_bitrate=min_bitrate,
                prev_min_bitrate=prev_min_bitrate,
                search_filetype_override=search_filetype_override,
                verified_lossless=verified_lossless,
            ),
        )

    @classmethod
    def to_imported_fields(
        cls,
        *,
        from_status: str | None = None,
        fields: Mapping[str, object],
    ) -> RequestTransition:
        _reject_unknown_fields("imported", fields, _IMPORTED_FIELDS)
        return cls(
            target_status="imported",
            from_status=from_status,
            fields=fields,
        )

    @classmethod
    def to_unsearchable(
        cls,
        *,
        from_status: str | None = None,
    ) -> RequestTransition:
        return cls(target_status="unsearchable", from_status=from_status)

    @classmethod
    def to_unsearchable_fields(
        cls,
        *,
        from_status: str | None = None,
        fields: Mapping[str, object],
    ) -> RequestTransition:
        """Retain a search stop while applying Bad Rip search policy."""
        _reject_unknown_fields("unsearchable", fields, _UNSEARCHABLE_FIELDS)
        return cls(
            target_status="unsearchable",
            from_status=from_status,
            fields=dict(fields),
        )

    @classmethod
    def status_only(
        cls,
        target_status: str,
        *,
        from_status: str | None = None,
    ) -> RequestTransition:
        if target_status == "wanted":
            return cls.to_wanted(from_status=from_status)
        if target_status == "imported":
            return cls.to_imported(from_status=from_status)
        if target_status == "unsearchable":
            return cls.to_unsearchable(from_status=from_status)
        if target_status == "downloading":
            raise ValueError("state_json is required for downloading transitions")
        raise ValueError(f"Unknown request status: {target_status!r}")


def finalize_request(
    db: TransitionsDB,
    request_id: int,
    transition: RequestTransition,
) -> TransitionResult:
    """Apply one validated request-state transition command."""

    _validate_transition_fields(transition.target_status, transition.fields)

    transition_kwargs = dict(transition.fields)
    if transition.from_status is not None:
        transition_kwargs["from_status"] = transition.from_status
    if transition.attempt_type is not None:
        transition_kwargs["attempt_type"] = transition.attempt_type

    return apply_transition(
        db,
        request_id,
        transition.target_status,
        **transition_kwargs,
    )


def finalize_operator_request(
    db: TransitionsDB,
    request_id: int,
    transition: RequestTransition,
    *,
    stale_retries: int = 3,
) -> TransitionResult:
    """Linearize an operator lifecycle command behind terminal writers.

    Operator adapters necessarily read a row before constructing their
    command. If a terminal transaction already owns the row lock, that
    snapshot can become stale while the compare-and-set waits. Rebase only
    operator intent onto the committed status and retry; automation continues
    to use ``finalize_request`` and treats the same conflict as a hard stop.
    """
    current = transition
    preflight = db.get_request(request_id)
    locked = processing_locked_conflict(
        preflight,
        request_id,
        current.target_status,
        expected_status=current.from_status,
    )
    if locked is not None:
        return locked
    if current.from_status is None:
        row = preflight
        if row is None:
            return TransitionConflict(
                request_id=request_id,
                target_status=current.target_status,
                kind=TransitionConflictKind.not_found,
                expected_status=None,
                actual_status=None,
            )
        current = replace(current, from_status=str(row["status"]))
    for retry in range(stale_retries + 1):
        is_status_only_same_source = (
            current.from_status == current.target_status
            and not current.fields
            and current.attempt_type is None
        )
        if is_status_only_same_source:
            assert current.from_status is not None
            _validate_transition_fields(
                current.target_status,
                current.fields,
            )
            applied = db.compare_request_status(
                request_id,
                expected_status=current.from_status,
            )
            if applied:
                result: TransitionResult = TransitionApplied(
                    request_id=request_id,
                    from_status=current.from_status,
                    target_status=current.target_status,
                )
            else:
                refreshed = db.get_request(request_id)
                result = processing_locked_conflict(
                    refreshed,
                    request_id,
                    current.target_status,
                    expected_status=current.from_status,
                ) or TransitionConflict(
                    request_id=request_id,
                    target_status=current.target_status,
                    kind=(
                        TransitionConflictKind.not_found
                        if refreshed is None
                        else TransitionConflictKind.stale_source
                    ),
                    expected_status=current.from_status,
                    actual_status=(
                        str(refreshed["status"])
                        if refreshed is not None
                        else None
                    ),
                )
        else:
            result = finalize_request(db, request_id, current)
        if not (
            isinstance(result, TransitionConflict)
            and result.kind is TransitionConflictKind.stale_source
            and result.actual_status is not None
            and retry < stale_retries
        ):
            return result
        current = replace(current, from_status=result.actual_status)
    raise AssertionError("operator transition retry loop did not return")


@dataclass(frozen=True)
class TransitionSideEffects:
    """What side effects a state transition requires.

    These flags tell the imperative layer (apply_transition) what
    db operations to perform alongside the status change. Clearing
    ``active_download_state`` is NOT modelled here — the terminal
    PipelineDB writers (``update_status``, ``mark_imported_with_rescue``,
    the reset-to-wanted paths) NULL it inline, unconditionally.
    """
    clear_retry_counters: bool = False
    record_attempt: bool = False


# Table of valid transitions and their required side effects.
# Any (from, to) pair not in this table is an invalid transition.
VALID_TRANSITIONS: dict[tuple[str, str], TransitionSideEffects] = {
    # Normal flow
    ("wanted", "downloading"): TransitionSideEffects(),
    ("downloading", "imported"): TransitionSideEffects(),
    ("downloading", "wanted"): TransitionSideEffects(record_attempt=True),
    # Operator-owned reversible search stop. It cannot abandon an active
    # download or retroactively stop an already-imported request.
    ("wanted", "unsearchable"): TransitionSideEffects(),
    # Idempotent reset (re-queue from wanted, field-only update)
    ("wanted", "wanted"): TransitionSideEffects(clear_retry_counters=True),

    # Re-queue (upgrade, explicit operator resume)
    ("imported", "wanted"): TransitionSideEffects(clear_retry_counters=True),
    ("unsearchable", "wanted"): TransitionSideEffects(clear_retry_counters=True),

    # In-place update (quality gate accept, bitrate update)
    ("imported", "imported"): TransitionSideEffects(),
    ("unsearchable", "unsearchable"): TransitionSideEffects(),

    # Admin overrides (force-import, web accept)
    ("unsearchable", "imported"): TransitionSideEffects(),
    ("wanted", "imported"): TransitionSideEffects(),
}


def validate_transition(from_status: str, to_status: str) -> bool:
    """Check whether a status transition is valid."""
    return (from_status, to_status) in VALID_TRANSITIONS


def apply_transition(
    db: TransitionsDB,
    request_id: int,
    to_status: str,
    **extra: Any,
) -> TransitionResult:
    """Execute a validated state transition.

    This is the single entry point for ordinary album_requests status
    mutations. It verifies an explicit source snapshot against the current row,
    rejects invalid edges, then delegates to a SQL compare-and-set writer.
    ``supersede_request_mbid`` is the sole deliberate creator of the terminal
    ``replaced`` audit status.

    Special keys extracted from extra:
        from_status: Current status (fetched from DB if not provided)
        search_filetype_override: For reset_to_wanted paths
        min_bitrate: For reset_to_wanted paths
        state_json: For set_downloading (wanted → downloading)
        attempt_type: For record_attempt (e.g. "download", "search")
        Everything else: passed to update_status as extra fields
    """
    # Extract special keys that control routing. Even an explicit source is
    # checked against the row: caller snapshots are assertions, not authority.
    expected_status = extra.pop("from_status", None)
    if expected_status is not None:
        expected_status = str(expected_status)
    # Presence-based: only fields explicitly passed get written.
    # Omitted fields are preserved by reset_to_wanted / update_status.
    transition_fields: dict[str, Any] = {}
    for _key in (
        "search_filetype_override",
        "min_bitrate",
        "prev_min_bitrate",
        "priority_started_at",
    ):
        if _key in extra:
            transition_fields[_key] = extra.pop(_key)
    state_json = extra.pop("state_json", None)
    attempt_type = extra.pop("attempt_type", None)
    row = db.get_request(request_id)
    if row is None:
        return TransitionConflict(
            request_id=request_id,
            target_status=to_status,
            kind=TransitionConflictKind.not_found,
            expected_status=expected_status,
            actual_status=None,
        )
    locked = processing_locked_conflict(
        row,
        request_id,
        to_status,
        expected_status=expected_status,
    )
    if locked is not None:
        return locked
    current = row["status"]
    assert isinstance(current, str)
    if expected_status is not None and current != expected_status:
        return TransitionConflict(
            request_id=request_id,
            target_status=to_status,
            kind=TransitionConflictKind.stale_source,
            expected_status=expected_status,
            actual_status=current,
        )
    from_status = current

    if not validate_transition(from_status, to_status):
        return TransitionConflict(
            request_id=request_id,
            target_status=to_status,
            kind=TransitionConflictKind.invalid_edge,
            expected_status=expected_status,
            actual_status=from_status,
        )

    fx = VALID_TRANSITIONS[(from_status, to_status)]

    # Status-only operator repeats are idempotent success. Field-bearing
    # same-status commands still take their ordinary CAS writer path.
    if (
        from_status == to_status
        and not transition_fields
        and state_json is None
        and attempt_type is None
        and not extra
    ):
        return TransitionApplied(request_id, from_status, to_status)

    def _cas_result(applied: bool) -> TransitionResult:
        if applied:
            return TransitionApplied(request_id, from_status, to_status)
        refreshed = db.get_request(request_id)
        return processing_locked_conflict(
            refreshed,
            request_id,
            to_status,
            expected_status=from_status,
        ) or TransitionConflict(
            request_id=request_id,
            target_status=to_status,
            kind=(TransitionConflictKind.not_found
                  if refreshed is None else TransitionConflictKind.stale_source),
            expected_status=from_status,
            actual_status=(
                str(refreshed["status"]) if refreshed is not None else None
            ),
        )

    # wanted → downloading: use set_downloading with JSONB state
    if to_status == "downloading":
        if state_json is None:
            raise ValueError("state_json is required for downloading transitions")
        if not isinstance(state_json, str):
            raise ValueError("state_json must be a string")
        return _cas_result(db.set_downloading(
            request_id,
            state_json,
            expected_status=from_status,
        ))

    # → wanted with counter reset: use reset_to_wanted
    if to_status == "wanted" and fx.clear_retry_counters:
        applied = db.reset_to_wanted(
            request_id,
            expected_status=from_status,
            **transition_fields,
        )
        if applied and attempt_type and not db.record_attempt(
            request_id,
            attempt_type,
            expected_status="wanted",
        ):
            return _cas_result(False)
        return _cas_result(applied)

    # downloading → wanted: clear active download state, preserve retry counters,
    # then record the failed automatic attempt so backoff can continue growing.
    if from_status == "downloading" and to_status == "wanted":
        reset_ok = bool(
            db.reset_downloading_to_wanted(
                request_id,
                expected_status=from_status,
                **transition_fields,
            )
        )
        if reset_ok and attempt_type and not db.record_attempt(
            request_id,
            attempt_type,
            expected_status="wanted",
        ):
            return _cas_result(False)
        return _cas_result(reset_ok)

    # All other transitions: use update_status
    all_extra: dict[str, object] = dict(extra)
    all_extra.update(transition_fields)
    # → imported is the long-tail-rescue capture seam (U14 / R21).
    # Routing through ``mark_imported_with_rescue`` makes the status
    # flip atomic with rescue-audit writes when the row had been
    # categorised unfindable. When it hadn't, the method behaves
    # like update_status (status + extras), just inside an explicit
    # transaction. No "import without rescue check" parallel path.
    if to_status == "imported":
        return _cas_result(db.mark_imported_with_rescue(
            request_id,
            expected_status=from_status,
            **all_extra,
        ))
    return _cas_result(db.update_status(
        request_id,
        to_status,
        expected_status=from_status,
        **all_extra,
    ))
