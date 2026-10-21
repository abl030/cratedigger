"""Operator mark that a request's installed copy is complete.

The library-completeness census can report findings an operator has
reviewed and judged wrong -- a segued Discogs composite the silence-gap
instrument cannot see into is the motivating case. The mark
(``album_requests.marked_complete_at`` plus ``marked_complete_findings``)
records the exact findings the operator signed off. The dashboard honours it
only while the census still reports exactly those findings, so any new or
changed finding for the album resurfaces on its own. Marking complete
withdraws an incomplete mark, and vice versa.

Never a measured verdict and never read by the import decision path: it
only changes what the census card lists.

``POST /api/pipeline/mark-complete`` wraps :func:`set_complete_mark`;
``pipeline-cli mark-complete`` relays to that route (CLI ⇄ API surface
symmetry), so its exit code derives from the HTTP status below.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

import msgspec

from lib.library_completeness_snapshot import read_library_completeness_snapshot
from lib.release_identity import normalize_release_id

CompleteMarkOutcome = Literal[
    "marked",
    "cleared",
    "already_marked",
    "already_clear",
    "not_found",
    "replaced",
    "not_listed",
    "census_unavailable",
]

#: Outcome → HTTP status. Idempotent no-ops are successes.
COMPLETE_MARK_HTTP_STATUS: dict[str, int] = {
    "marked": 200,
    "cleared": 200,
    "already_marked": 200,
    "already_clear": 200,
    "not_found": 404,
    "replaced": 409,
    "not_listed": 409,
    "census_unavailable": 503,
}

_CANONICAL_OUTCOMES: tuple[CompleteMarkOutcome, ...] = (
    "marked",
    "cleared",
    "already_marked",
    "already_clear",
    "not_found",
    "replaced",
    "not_listed",
    "census_unavailable",
)


@dataclass(frozen=True)
class CompleteMarkResult:
    """One outcome string per branch of the mark mutation."""

    outcome: CompleteMarkOutcome
    request_id: int


class _CompleteMarkDB(Protocol):
    def get_request(self, request_id: int) -> Mapping[str, object] | None: ...

    def set_marked_complete(
        self, request_id: int, *, findings: list[dict[str, str]] | None,
    ) -> str: ...


def canonical_findings(findings: Iterable[Mapping[str, object]]) -> list[dict[str, str]]:
    """The order-free form a mark stores and the dashboard compares.

    A finding can quote a surrogate-escaped Beets path, which JSONB refuses;
    backslashreplace keeps it readable, and both sides of the comparison
    pass through here, so they still agree.
    """
    return sorted(
        ({"kind": _jsonb_safe(f.get("kind")), "detail": _jsonb_safe(f.get("detail"))}
         for f in findings),
        key=lambda f: (f["kind"], f["detail"]),
    )


def _jsonb_safe(value: object) -> str:
    return str(value or "").encode("utf-8", "backslashreplace").decode("utf-8")


def request_release_ids(request: Mapping[str, object]) -> set[str]:
    """Both release-id columns, normalised; legacy Discogs rows use either."""
    return {
        normalized
        for value in (request.get("mb_release_id"), request.get("discogs_release_id"))
        if (normalized := normalize_release_id(value))
    }


def _census_findings_for(
    snapshot_path: str, release_ids: set[str],
) -> list[dict[str, str]] | None | Literal["unavailable"]:
    try:
        snapshot = read_library_completeness_snapshot(snapshot_path)
    except (OSError, UnicodeDecodeError, ValueError, msgspec.ValidationError):
        return "unavailable"
    if snapshot is None:
        return "unavailable"
    for album in snapshot.report.albums:
        if normalize_release_id(album.release_id) in release_ids and album.findings:
            return canonical_findings(
                {"kind": f.kind, "detail": f.detail} for f in album.findings
            )
    return None


def set_complete_mark(
    db: _CompleteMarkDB,
    request_id: int,
    *,
    marked: bool,
    snapshot_path: str | None,
) -> CompleteMarkResult:
    """Set or clear the operator's complete mark on one request.

    Setting reads the published census snapshot and records the request's
    current findings verbatim; a request the census lists no findings for
    has nothing to acknowledge (``not_listed``).
    """
    findings: list[dict[str, str]] | None = None
    if marked:
        request = db.get_request(request_id)
        if request is None:
            return CompleteMarkResult("not_found", int(request_id))
        if snapshot_path is None:
            return CompleteMarkResult("census_unavailable", int(request_id))
        found = _census_findings_for(snapshot_path, request_release_ids(request))
        if found == "unavailable":
            return CompleteMarkResult("census_unavailable", int(request_id))
        if found is None:
            return CompleteMarkResult("not_listed", int(request_id))
        findings = found
    outcome = db.set_marked_complete(request_id, findings=findings)
    for candidate in _CANONICAL_OUTCOMES:
        if outcome == candidate:
            return CompleteMarkResult(outcome=candidate, request_id=int(request_id))
    raise ValueError(f"set_marked_complete returned unknown outcome {outcome!r}")
