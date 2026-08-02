"""Fail-closed member tracking for known live-world audit debt."""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import msgspec

from lib.world_audit_service import WorldAuditReport, build_world_audit_report
from lib.world_invariants import WorldViolation

WORLD_AUDIT_DEBT_SCHEMA_VERSION = 1
_DIGEST_LENGTH = 64
_IDENTITY_DOMAIN = b"cratedigger-world-audit-debt:identity:v1\0"
_VIOLATION_DOMAIN = b"cratedigger-world-audit-debt:violation:v1\0"


class WorldAuditDebtError(ValueError):
    """The report or persisted authority state is invalid."""


class WorldAuditDebtCodeCount(msgspec.Struct, frozen=True):
    code: str
    count: int


class WorldAuditDebtMember(msgspec.Struct, frozen=True):
    code: str
    identity_digest: str
    violation_digest: str


class WorldAuditDebtState(msgspec.Struct, frozen=True):
    schema_version: int
    approved_total: int
    approved_by_code: tuple[WorldAuditDebtCodeCount, ...]
    remaining: tuple[WorldAuditDebtMember, ...]


class WorldAuditDebtCodeReport(msgspec.Struct, frozen=True):
    code: str
    approved: int
    current: int
    known_remaining: int
    newly_converged: int
    new_members: int
    changed_members: int


class WorldAuditDebtReport(msgspec.Struct, frozen=True):
    status: str
    strict_status: str
    strict_violations: int
    approved_total: int
    known_remaining: int
    newly_converged: int
    converged_total: int
    new_members: int
    changed_members: int
    growth: int
    state_updated: bool
    by_code: tuple[WorldAuditDebtCodeReport, ...]


@dataclass(frozen=True)
class WorldAuditDebtEvaluation:
    passed: bool
    report: WorldAuditDebtReport
    next_state: WorldAuditDebtState | None


class _WorldViolationIdentity(msgspec.Struct, frozen=True):
    request_id: int | None
    release_id: str | None
    album_ids: tuple[int, ...]


def _digest(domain: bytes, value: object) -> str:
    return hashlib.sha256(domain + msgspec.json.encode(value)).hexdigest()


def _member(violation: WorldViolation) -> WorldAuditDebtMember:
    identity = _WorldViolationIdentity(
        request_id=violation.request_id,
        release_id=violation.release_id,
        album_ids=violation.album_ids,
    )
    return WorldAuditDebtMember(
        code=violation.code,
        identity_digest=_digest(_IDENTITY_DOMAIN, identity),
        violation_digest=_digest(_VIOLATION_DOMAIN, violation),
    )


def _sorted_members(
    violations: tuple[WorldViolation, ...],
) -> tuple[WorldAuditDebtMember, ...]:
    members = tuple(sorted(
        (_member(violation) for violation in violations),
        key=lambda item: (
            item.violation_digest,
            item.identity_digest,
            item.code,
        ),
    ))
    if len({item.violation_digest for item in members}) != len(members):
        raise WorldAuditDebtError(
            "audit report contains duplicate violation fingerprints"
        )
    return members


def _code_counts(codes: list[str]) -> tuple[WorldAuditDebtCodeCount, ...]:
    counts = Counter(codes)
    return tuple(
        WorldAuditDebtCodeCount(code=code, count=counts[code])
        for code in sorted(counts)
    )


def _validate_digest(value: str, *, field: str) -> None:
    if (
        len(value) != _DIGEST_LENGTH
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise WorldAuditDebtError(f"invalid {field} in debt state")


def validate_world_audit_report(report: WorldAuditReport) -> None:
    if not report.complete:
        raise WorldAuditDebtError(
            "incomplete audit report cannot update tracked debt"
        )
    canonical = build_world_audit_report(
        counts=report.counts,
        violations=report.violations,
        complete=True,
    )
    if report != canonical:
        raise WorldAuditDebtError(
            "audit report does not match canonical ownership grouping"
        )
    _sorted_members(report.violations)


def validate_world_audit_debt_state(state: WorldAuditDebtState) -> None:
    if state.schema_version != WORLD_AUDIT_DEBT_SCHEMA_VERSION:
        raise WorldAuditDebtError(
            "unsupported debt-state schema "
            f"{state.schema_version}; expected "
            f"{WORLD_AUDIT_DEBT_SCHEMA_VERSION}"
        )
    if state.approved_total < 0:
        raise WorldAuditDebtError("debt-state approved_total is negative")
    approved_codes = [item.code for item in state.approved_by_code]
    if approved_codes != sorted(set(approved_codes)):
        raise WorldAuditDebtError(
            "debt-state approved code counts are not unique and sorted"
        )
    if any(not item.code or item.count <= 0 for item in state.approved_by_code):
        raise WorldAuditDebtError("debt-state approved code count is invalid")
    if sum(item.count for item in state.approved_by_code) != state.approved_total:
        raise WorldAuditDebtError(
            "debt-state approved code counts do not match approved_total"
        )
    if len(state.remaining) > state.approved_total:
        raise WorldAuditDebtError(
            "debt-state remaining members exceed approved_total"
        )
    expected_order = tuple(sorted(
        state.remaining,
        key=lambda item: (
            item.violation_digest,
            item.identity_digest,
            item.code,
        ),
    ))
    if state.remaining != expected_order:
        raise WorldAuditDebtError("debt-state remaining members are not sorted")
    if (
        len({item.violation_digest for item in state.remaining})
        != len(state.remaining)
    ):
        raise WorldAuditDebtError(
            "debt-state contains duplicate violation fingerprints"
        )
    approved_by_code = {
        item.code: item.count for item in state.approved_by_code
    }
    remaining_by_code = Counter(item.code for item in state.remaining)
    for item in state.remaining:
        if not item.code or item.code not in approved_by_code:
            raise WorldAuditDebtError(
                "debt-state remaining member has an unapproved code"
            )
        _validate_digest(item.identity_digest, field="identity digest")
        _validate_digest(item.violation_digest, field="violation digest")
    for code, count in remaining_by_code.items():
        if count > approved_by_code[code]:
            raise WorldAuditDebtError(
                f"debt-state remaining count exceeds approval for {code}"
            )


def initialize_world_audit_debt_state(
    report: WorldAuditReport,
) -> WorldAuditDebtState:
    validate_world_audit_report(report)
    members = _sorted_members(report.violations)
    state = WorldAuditDebtState(
        schema_version=WORLD_AUDIT_DEBT_SCHEMA_VERSION,
        approved_total=len(members),
        approved_by_code=_code_counts([item.code for item in members]),
        remaining=members,
    )
    validate_world_audit_debt_state(state)
    return state


def _code_report(
    state: WorldAuditDebtState,
    current: tuple[WorldAuditDebtMember, ...],
    known: tuple[WorldAuditDebtMember, ...],
    converged: tuple[WorldAuditDebtMember, ...],
    new: tuple[WorldAuditDebtMember, ...],
    changed: tuple[WorldAuditDebtMember, ...],
) -> tuple[WorldAuditDebtCodeReport, ...]:
    approved = {
        item.code: item.count for item in state.approved_by_code
    }
    current_counts = Counter(item.code for item in current)
    known_counts = Counter(item.code for item in known)
    converged_counts = Counter(item.code for item in converged)
    new_counts = Counter(item.code for item in new)
    changed_counts = Counter(item.code for item in changed)
    codes = sorted(
        set(approved)
        | set(current_counts)
        | set(new_counts)
        | set(changed_counts)
    )
    return tuple(
        WorldAuditDebtCodeReport(
            code=code,
            approved=approved.get(code, 0),
            current=current_counts[code],
            known_remaining=known_counts[code],
            newly_converged=converged_counts[code],
            new_members=new_counts[code],
            changed_members=changed_counts[code],
        )
        for code in codes
    )


def assess_world_audit_debt(
    state: WorldAuditDebtState,
    report: WorldAuditReport,
) -> WorldAuditDebtEvaluation:
    validate_world_audit_debt_state(state)
    validate_world_audit_report(report)
    current = _sorted_members(report.violations)
    remaining_by_violation = {
        item.violation_digest: item for item in state.remaining
    }
    remaining_identities = {
        item.identity_digest for item in state.remaining
    }
    current_digests = {
        item.violation_digest for item in current
    }
    known = tuple(
        item for item in current
        if item.violation_digest in remaining_by_violation
    )
    unmatched = tuple(
        item for item in current
        if item.violation_digest not in remaining_by_violation
    )
    changed = tuple(
        item for item in unmatched
        if item.identity_digest in remaining_identities
    )
    new = tuple(
        item for item in unmatched
        if item.identity_digest not in remaining_identities
    )
    passed = not unmatched
    converged = (
        tuple(
            item for item in state.remaining
            if item.violation_digest not in current_digests
        )
        if passed
        else ()
    )
    next_state = (
        WorldAuditDebtState(
            schema_version=state.schema_version,
            approved_total=state.approved_total,
            approved_by_code=state.approved_by_code,
            remaining=current,
        )
        if passed
        else None
    )
    if next_state is not None:
        validate_world_audit_debt_state(next_state)
    status = (
        "unrecognized_violations"
        if not passed
        else "clean"
        if not current
        else "tracked_debt"
    )
    persisted_remaining = (
        len(next_state.remaining) if next_state is not None
        else len(state.remaining)
    )
    gate_report = WorldAuditDebtReport(
        status=status,
        strict_status=report.status,
        strict_violations=len(current),
        approved_total=state.approved_total,
        known_remaining=len(known),
        newly_converged=len(converged),
        converged_total=state.approved_total - persisted_remaining,
        new_members=len(new),
        changed_members=len(changed),
        growth=max(0, len(current) - len(state.remaining)),
        state_updated=next_state is not None and next_state != state,
        by_code=_code_report(
            state,
            current,
            known,
            converged,
            new,
            changed,
        ),
    )
    return WorldAuditDebtEvaluation(
        passed=passed,
        report=gate_report,
        next_state=next_state,
    )


def initialization_world_audit_debt_report(
    state: WorldAuditDebtState,
    report: WorldAuditReport,
) -> WorldAuditDebtReport:
    evaluation = assess_world_audit_debt(state, report)
    base = evaluation.report
    return WorldAuditDebtReport(
        status="initialized",
        strict_status=base.strict_status,
        strict_violations=base.strict_violations,
        approved_total=base.approved_total,
        known_remaining=base.known_remaining,
        newly_converged=0,
        converged_total=0,
        new_members=0,
        changed_members=0,
        growth=0,
        state_updated=True,
        by_code=base.by_code,
    )


def decode_world_audit_report(payload: bytes) -> WorldAuditReport:
    try:
        report = msgspec.json.decode(payload, type=WorldAuditReport)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise WorldAuditDebtError(f"invalid audit report: {exc}") from exc
    validate_world_audit_report(report)
    return report


def load_world_audit_debt_state(path: Path) -> WorldAuditDebtState:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise WorldAuditDebtError(
            f"debt state is unavailable: {exc}"
        ) from exc
    try:
        state = msgspec.json.decode(payload, type=WorldAuditDebtState)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise WorldAuditDebtError(f"invalid debt state: {exc}") from exc
    validate_world_audit_debt_state(state)
    return state


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def write_world_audit_debt_state(
    path: Path,
    state: WorldAuditDebtState,
    *,
    exclusive: bool = False,
) -> None:
    validate_world_audit_debt_state(state)
    parent = path.parent
    if not parent.is_dir():
        raise WorldAuditDebtError(
            f"debt-state parent directory is unavailable: {parent}"
        )
    payload = msgspec.json.encode(state)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            os.fchmod(handle.fileno(), 0o600)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if exclusive:
            try:
                os.link(temporary_path, path)
            except FileExistsError as exc:
                raise WorldAuditDebtError(
                    f"debt state already exists: {path}"
                ) from exc
            temporary_path.unlink()
            temporary_path = None
        else:
            os.replace(temporary_path, path)
            temporary_path = None
        os.chmod(path, 0o600)
        _fsync_directory(parent)
    except WorldAuditDebtError:
        raise
    except OSError as exc:
        raise WorldAuditDebtError(
            f"could not persist debt state: {exc}"
        ) from exc
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass


__all__ = [
    "WORLD_AUDIT_DEBT_SCHEMA_VERSION",
    "WorldAuditDebtCodeCount",
    "WorldAuditDebtCodeReport",
    "WorldAuditDebtError",
    "WorldAuditDebtEvaluation",
    "WorldAuditDebtMember",
    "WorldAuditDebtReport",
    "WorldAuditDebtState",
    "assess_world_audit_debt",
    "decode_world_audit_report",
    "initialization_world_audit_debt_report",
    "initialize_world_audit_debt_state",
    "load_world_audit_debt_state",
    "validate_world_audit_debt_state",
    "validate_world_audit_report",
    "write_world_audit_debt_state",
]
