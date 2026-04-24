"""State transition validation and side-effect declarations.

Pure functions for transition validation. The imperative apply_transition()
delegates to pipeline_db methods and is the single entry point for all
state mutations.

4 statuses: wanted, downloading, imported, manual
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, Mapping, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("cratedigger")


class _OmittedField:
    """Sentinel for distinguishing omitted fields from explicit NULL writes."""


_OMITTED = _OmittedField()


RequestStatus = Literal["wanted", "downloading", "imported", "manual"]


def _explicit_fields(**values: object) -> dict[str, object]:
    return {
        key: value
        for key, value in values.items()
        if value is not _OMITTED
    }


_WANTED_FIELDS = frozenset({
    "min_bitrate",
    "prev_min_bitrate",
    "search_filetype_override",
})

_IMPORTED_FIELDS = frozenset({
    "beets_distance",
    "beets_scenario",
    "current_spectral_bitrate",
    "current_spectral_grade",
    "final_format",
    "imported_path",
    "last_download_spectral_bitrate",
    "last_download_spectral_grade",
    "min_bitrate",
    "prev_min_bitrate",
    "search_filetype_override",
    "verified_lossless",
})

_DOWNLOADING_FIELDS = frozenset({"state_json"})
_MANUAL_FIELDS = frozenset()
_RESERVED_FIELDS = frozenset({"from_status", "attempt_type"})


def _field_or_omitted(fields: Mapping[str, object], key: str) -> object:
    if key in fields:
        return fields[key]
    return _OMITTED


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
    if target_status == "manual":
        _reject_unknown_fields(target_status, fields, _MANUAL_FIELDS)
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
    fields: Mapping[str, object] = field(default_factory=dict)

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
    ) -> "RequestTransition":
        return cls(
            target_status="wanted",
            from_status=from_status,
            attempt_type=attempt_type,
            fields=_explicit_fields(
                search_filetype_override=search_filetype_override,
                min_bitrate=min_bitrate,
                prev_min_bitrate=prev_min_bitrate,
            ),
        )

    @classmethod
    def to_wanted_fields(
        cls,
        *,
        from_status: str | None = None,
        attempt_type: str | None = None,
        fields: Mapping[str, object],
    ) -> "RequestTransition":
        _reject_unknown_fields("wanted", fields, _WANTED_FIELDS)
        return cls.to_wanted(
            from_status=from_status,
            attempt_type=attempt_type,
            search_filetype_override=_field_or_omitted(
                fields, "search_filetype_override"),
            min_bitrate=_field_or_omitted(fields, "min_bitrate"),
            prev_min_bitrate=_field_or_omitted(fields, "prev_min_bitrate"),
        )

    @classmethod
    def to_downloading(
        cls,
        *,
        state_json: str,
        from_status: str | None = None,
    ) -> "RequestTransition":
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
        final_format: object = _OMITTED,
        imported_path: object = _OMITTED,
        last_download_spectral_bitrate: object = _OMITTED,
        last_download_spectral_grade: object = _OMITTED,
        min_bitrate: object = _OMITTED,
        prev_min_bitrate: object = _OMITTED,
        search_filetype_override: object = _OMITTED,
        verified_lossless: object = _OMITTED,
    ) -> "RequestTransition":
        return cls(
            target_status="imported",
            from_status=from_status,
            fields=_explicit_fields(
                beets_distance=beets_distance,
                beets_scenario=beets_scenario,
                current_spectral_bitrate=current_spectral_bitrate,
                current_spectral_grade=current_spectral_grade,
                final_format=final_format,
                imported_path=imported_path,
                last_download_spectral_bitrate=last_download_spectral_bitrate,
                last_download_spectral_grade=last_download_spectral_grade,
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
    ) -> "RequestTransition":
        _reject_unknown_fields("imported", fields, _IMPORTED_FIELDS)
        return cls.to_imported(
            from_status=from_status,
            beets_distance=_field_or_omitted(fields, "beets_distance"),
            beets_scenario=_field_or_omitted(fields, "beets_scenario"),
            current_spectral_bitrate=_field_or_omitted(
                fields, "current_spectral_bitrate"),
            current_spectral_grade=_field_or_omitted(
                fields, "current_spectral_grade"),
            final_format=_field_or_omitted(fields, "final_format"),
            imported_path=_field_or_omitted(fields, "imported_path"),
            last_download_spectral_bitrate=_field_or_omitted(
                fields, "last_download_spectral_bitrate"),
            last_download_spectral_grade=_field_or_omitted(
                fields, "last_download_spectral_grade"),
            min_bitrate=_field_or_omitted(fields, "min_bitrate"),
            prev_min_bitrate=_field_or_omitted(fields, "prev_min_bitrate"),
            search_filetype_override=_field_or_omitted(
                fields, "search_filetype_override"),
            verified_lossless=_field_or_omitted(fields, "verified_lossless"),
        )

    @classmethod
    def to_manual(
        cls,
        *,
        from_status: str | None = None,
    ) -> "RequestTransition":
        return cls(target_status="manual", from_status=from_status)

    @classmethod
    def status_only(
        cls,
        target_status: str,
        *,
        from_status: str | None = None,
    ) -> "RequestTransition":
        if target_status == "wanted":
            return cls.to_wanted(from_status=from_status)
        if target_status == "imported":
            return cls.to_imported(from_status=from_status)
        if target_status == "manual":
            return cls.to_manual(from_status=from_status)
        if target_status == "downloading":
            raise ValueError("state_json is required for downloading transitions")
        raise ValueError(f"Unknown request status: {target_status!r}")


def finalize_request(
    db: "PipelineDB",
    request_id: int,
    transition: RequestTransition,
) -> None:
    """Apply one validated request-state transition command."""

    _validate_transition_fields(transition.target_status, transition.fields)

    transition_kwargs = dict(transition.fields)
    if transition.from_status is not None:
        transition_kwargs["from_status"] = transition.from_status
    if transition.attempt_type is not None:
        transition_kwargs["attempt_type"] = transition.attempt_type

    apply_transition(
        db,
        request_id,
        transition.target_status,
        **transition_kwargs,
    )


@dataclass(frozen=True)
class TransitionSideEffects:
    """What side effects a state transition requires.

    These flags tell the imperative layer (apply_transition) what
    db operations to perform alongside the status change.
    """
    clear_download_state: bool = False
    clear_retry_counters: bool = False
    record_attempt: bool = False


# Table of valid transitions and their required side effects.
# Any (from, to) pair not in this table is an invalid transition.
VALID_TRANSITIONS: dict[tuple[str, str], TransitionSideEffects] = {
    # Normal flow
    ("wanted", "downloading"): TransitionSideEffects(),
    ("downloading", "imported"): TransitionSideEffects(clear_download_state=True),
    ("downloading", "wanted"): TransitionSideEffects(
        clear_download_state=True, record_attempt=True),
    ("downloading", "manual"): TransitionSideEffects(clear_download_state=True),

    # Manual status changes
    ("wanted", "manual"): TransitionSideEffects(),
    # Idempotent reset (re-queue from wanted, field-only update)
    ("wanted", "wanted"): TransitionSideEffects(clear_retry_counters=True),

    # Re-queue (upgrade, retry from manual)
    ("imported", "wanted"): TransitionSideEffects(clear_retry_counters=True),
    ("manual", "wanted"): TransitionSideEffects(clear_retry_counters=True),

    # In-place update (quality gate accept, bitrate update)
    ("imported", "imported"): TransitionSideEffects(clear_download_state=True),

    # Admin overrides (force-import, web accept)
    ("manual", "imported"): TransitionSideEffects(clear_download_state=True),
    ("wanted", "imported"): TransitionSideEffects(clear_download_state=True),
}


def validate_transition(from_status: str, to_status: str) -> bool:
    """Check whether a status transition is valid."""
    return (from_status, to_status) in VALID_TRANSITIONS


def transition_side_effects(from_status: str, to_status: str) -> TransitionSideEffects:
    """Return the side-effect flags for a valid transition.

    Raises ValueError for invalid transitions.
    """
    fx = VALID_TRANSITIONS.get((from_status, to_status))
    if fx is None:
        raise ValueError(
            f"Invalid transition: {from_status!r} -> {to_status!r}")
    return fx


def apply_transition(
    db: "PipelineDB",
    request_id: int,
    to_status: str,
    **extra: Any,
) -> None:
    """Execute a validated state transition.

    This is the single entry point for all album_requests status mutations.
    It validates the transition, then delegates to the appropriate PipelineDB
    method with the correct side effects.

    Special keys extracted from extra:
        from_status: Current status (fetched from DB if not provided)
        search_filetype_override: For reset_to_wanted paths
        min_bitrate: For reset_to_wanted paths
        state_json: For set_downloading (wanted → downloading)
        attempt_type: For record_attempt (e.g. "download", "search")
        Everything else: passed to update_status as extra fields
    """
    # Extract special keys that control routing
    from_status = extra.pop("from_status", None)
    if from_status is not None:
        from_status = str(from_status)
    # Presence-based: only fields explicitly passed get written.
    # Omitted fields are preserved by reset_to_wanted / update_status.
    transition_fields: dict[str, object] = {}
    for _key in ("search_filetype_override", "min_bitrate", "prev_min_bitrate"):
        if _key in extra:
            transition_fields[_key] = extra.pop(_key)
    state_json = extra.pop("state_json", None)
    attempt_type = extra.pop("attempt_type", None)
    if from_status is None:
        row = db.get_request(request_id)
        if row is None:
            logger.warning(f"apply_transition: request {request_id} not found")
            return
        current = row["status"]
        assert isinstance(current, str)
        from_status = current

    if not validate_transition(from_status, to_status):
        logger.warning(
            f"apply_transition: invalid {from_status!r} -> {to_status!r} "
            f"for request {request_id}, proceeding anyway")

    fx = VALID_TRANSITIONS.get((from_status, to_status), TransitionSideEffects())

    # wanted → downloading: use set_downloading with JSONB state
    if to_status == "downloading":
        if state_json is None:
            raise ValueError("state_json is required for downloading transitions")
        if not isinstance(state_json, str):
            raise ValueError("state_json must be a string")
        if not db.set_downloading(request_id, state_json):
            logger.warning(
                f"apply_transition: status guard prevented {from_status!r} -> "
                f"'downloading' for request {request_id} (album no longer wanted)")
        return

    # → wanted with counter reset: use reset_to_wanted
    if to_status == "wanted" and fx.clear_retry_counters:
        db.reset_to_wanted(request_id, **transition_fields)
        if fx.record_attempt and attempt_type:
            db.record_attempt(request_id, attempt_type)
        return

    # downloading → wanted: reset + record attempt
    if from_status == "downloading" and to_status == "wanted":
        db.reset_to_wanted(request_id, **transition_fields)
        if attempt_type:
            db.record_attempt(request_id, attempt_type)
        return

    # All other transitions: use update_status
    all_extra: dict[str, object] = dict(extra)
    all_extra.update(transition_fields)
    db.update_status(request_id, to_status, **all_extra)
