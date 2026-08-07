"""The one writer of ``album_requests.canonical_release_id`` (#1059).

MusicBrainz editors merge two release entries; the loser's MBID becomes a
permanent ``301`` to the survivor and ``mbsync`` retags the local files.
This service asks MusicBrainz what it calls each release now and stores the
answer, so the join and the import-time match can resolve over the union of
both identities without any read path making a network call.

**Whole library, every day.** The sweep asks about every non-``replaced``
request rather than only the rows whose join already failed. Merge drift is
invisible from the read side until ``mbsync`` runs — four of the six live
merged requests still have Beets holding the loser, so their join looks
perfectly healthy — and it is invisible from the write side until a download
completes and fails to match. Waiting for either signal means learning about
a merge only after it has already cost something. At ~72ms per lookup
against the local mirror, ~8,500 rows is about ten minutes once a day.

Authority: "teh reconciler should do the whole library, every day. it ain't
no big deal for people who run local mirrors."
— https://github.com/abl030/cratedigger/issues/1059

**Fail-open, always.** ``lib/mb_canonical.py`` returns ``None`` for every
non-answer alike — mirror down, timeout, ``4xx``, unusable shape, or the
ordinary case of an id MusicBrainz still considers current. This service
never distinguishes them into a write, so a bad mirror day leaves the stored
survivors exactly as they were.

**There is no automatic clearing path.** ``canonical_release_id()`` collapses
"asked, got a definitive 200 with no redirect" and "could not ask" into the
same ``None``, so reconciliation can never retract a stored survivor. An
operator may explicitly retire a named survivor when they have independent
evidence it is stale; that action is a fresh-read compare-and-set and never
consults the mirror (#1059 F6).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from lib.mb_canonical import CanonicalReleaseFn, canonical_release_id
from lib.release_identity import ReleaseIdentity

logger = logging.getLogger("cratedigger")

#: One row's outcome. Exactly one per branch, mapped to CLI exit codes and
#: HTTP statuses by the two thin adapters.
OUTCOME_RESOLVED = "resolved"
OUTCOME_UNCHANGED = "unchanged"
OUTCOME_NO_REDIRECT = "no_redirect"
OUTCOME_NOT_MUSICBRAINZ = "not_musicbrainz"
OUTCOME_INVALID_IDENTITY = "invalid_identity"
OUTCOME_FROZEN = "frozen"
OUTCOME_NOT_FOUND = "not_found"
OUTCOME_RETIRED = "retired"
OUTCOME_NO_CANONICAL = "no_canonical"
OUTCOME_STALE = "stale"

#: Outcomes that mean "nothing is wrong, nothing changed". Used by the
#: sweep summary so an operator reading the journal sees one number for
#: "the library is already correct".
QUIET_OUTCOMES = frozenset({
    OUTCOME_UNCHANGED,
    OUTCOME_NO_REDIRECT,
    OUTCOME_NOT_MUSICBRAINZ,
})


class CanonicalReleaseDB(Protocol):
    """The PipelineDB surface this service uses."""

    def get_request(self, request_id: int) -> Mapping[str, object] | None: ...

    def list_non_replaced_requests(self) -> Sequence[Mapping[str, object]]: ...

    def record_canonical_release_id(
        self,
        request_id: int,
        *,
        canonical_release_id: str,
        resolved_at: datetime,
    ) -> bool: ...

    def retire_canonical_release_id(
        self,
        request_id: int,
        *,
        expected_canonical_release_id: str,
        expected_resolved_at: datetime,
    ) -> bool: ...


@dataclass(frozen=True)
class CanonicalReconcileResult:
    """One request's reconciliation outcome."""

    request_id: int
    outcome: str
    acquisition_release_id: str | None = None
    canonical_release_id: str | None = None
    previous_canonical_release_id: str | None = None

    @property
    def changed(self) -> bool:
        return self.outcome == OUTCOME_RESOLVED


@dataclass(frozen=True)
class CanonicalSweepResult:
    """The whole-library pass, summarised for the journal and the API."""

    scanned: int
    resolved: tuple[CanonicalReconcileResult, ...]
    outcome_counts: Mapping[str, int]

    @property
    def changed(self) -> int:
        return len(self.resolved)


@dataclass(frozen=True)
class CanonicalRetireResult:
    """One explicit operator retirement outcome."""

    request_id: int
    outcome: str
    previous_canonical_release_id: str | None = None

    @property
    def changed(self) -> bool:
        return self.outcome == OUTCOME_RETIRED


def configure_reconciliation_mirror(mb_api_base: str) -> str | None:
    """Point the resolver at the configured MusicBrainz base.

    **Every** surface that reconciles must call this — the daily oneshot,
    ``pipeline-cli canonical``, and the API route. ``lib/mb_canonical`` is
    inert until a process wires a base, so a surface that forgets does not
    fail loudly: it reports ``no_redirect`` for every row and exits 0, which
    the outcome vocabulary reads as "the library is already correct". Wiring
    it in one shared place is the whole point of this function.

    Where that base points is the operator's business. Running against
    public MusicBrainz is slow for the same reason every other mirror
    consumer is slow without one, and it is no more this function's concern
    than it is the long-tail dashboard's.
    """
    from lib.mb_canonical import configure_canonical_base

    configured = (mb_api_base or "").strip().rstrip("/")
    if not configured:
        logger.warning(
            "canonical reconciliation is inert: musicbrainz.api_base is blank",
        )
        configure_canonical_base(None)
        return None
    base = configured + "/ws/2"
    configure_canonical_base(base)
    return base


def _acquisition(row: Mapping[str, object]) -> ReleaseIdentity | None:
    return ReleaseIdentity.from_strict_fields(
        row.get("mb_release_id"),
        row.get("discogs_release_id"),
    )


class CanonicalReleaseService:
    """Reconcile stored acquisition ids against MusicBrainz merge state."""

    def __init__(
        self,
        db: CanonicalReleaseDB,
        *,
        canonical_fn: CanonicalReleaseFn | None = None,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._db = db
        # Injected, never patched: the production resolver is the default
        # and tests pass their own explicitly.
        self._canonical_fn = canonical_fn or canonical_release_id
        self._now_fn = now_fn or (lambda: datetime.now(UTC))

    def reconcile_request(self, request_id: int) -> CanonicalReconcileResult:
        """Reconcile one request. Never raises on a mirror failure."""
        row = self._db.get_request(request_id)
        if row is None:
            return CanonicalReconcileResult(request_id, OUTCOME_NOT_FOUND)
        return self.reconcile_row(row)

    def retire_request(self, request_id: int) -> CanonicalRetireResult:
        """Explicitly retire one stored survivor using a fresh-read CAS.

        This is deliberately separate from reconciliation: a definitive
        no-redirect answer is indistinguishable from a failed lookup, and
        neither may clear a survivor. Only this named operator action can.
        """
        row = self._db.get_request(request_id)
        if row is None:
            return CanonicalRetireResult(request_id, OUTCOME_NOT_FOUND)
        if row.get("status") == "replaced":
            return CanonicalRetireResult(request_id, OUTCOME_FROZEN)

        stored = row.get("canonical_release_id")
        resolved_at = row.get("canonical_resolved_at")
        if not isinstance(stored, str) or not stored or not isinstance(
            resolved_at, datetime,
        ):
            return CanonicalRetireResult(request_id, OUTCOME_NO_CANONICAL)

        retired = self._db.retire_canonical_release_id(
            request_id,
            expected_canonical_release_id=stored,
            expected_resolved_at=resolved_at,
        )
        if not retired:
            return CanonicalRetireResult(
                request_id, OUTCOME_STALE,
                previous_canonical_release_id=stored,
            )
        return CanonicalRetireResult(
            request_id, OUTCOME_RETIRED,
            previous_canonical_release_id=stored,
        )

    def reconcile_row(
        self, row: Mapping[str, object],
    ) -> CanonicalReconcileResult:
        """Reconcile one already-loaded request row."""
        raw_id = row["id"]
        request_id = raw_id if isinstance(raw_id, int) else int(str(raw_id))
        stored_canonical = row.get("canonical_release_id")
        previous = str(stored_canonical) if stored_canonical else None

        identity = _acquisition(row)
        if identity is None:
            return CanonicalReconcileResult(
                request_id,
                OUTCOME_INVALID_IDENTITY,
                previous_canonical_release_id=previous,
            )
        if identity.source != "musicbrainz":
            # Discogs release ids have no merge concept and nothing retags
            # them, so this is a positive skip rather than a lookup that
            # happens to return nothing. It is also what keeps this service
            # from becoming an adapter between the two sources.
            return CanonicalReconcileResult(
                request_id,
                OUTCOME_NOT_MUSICBRAINZ,
                acquisition_release_id=identity.release_id,
                previous_canonical_release_id=previous,
            )

        survivor = self._canonical_fn(identity.release_id)
        if survivor is None:
            # Fail-open: a mirror outage, a 4xx, and "no merge" are all the
            # same non-answer here, and none of them may disturb what is
            # already stored (#1059 invariant 7).
            return CanonicalReconcileResult(
                request_id,
                OUTCOME_NO_REDIRECT,
                acquisition_release_id=identity.release_id,
                canonical_release_id=previous,
                previous_canonical_release_id=previous,
            )
        if survivor == previous:
            return CanonicalReconcileResult(
                request_id,
                OUTCOME_UNCHANGED,
                acquisition_release_id=identity.release_id,
                canonical_release_id=survivor,
                previous_canonical_release_id=previous,
            )

        written = self._db.record_canonical_release_id(
            request_id,
            canonical_release_id=survivor,
            resolved_at=self._now_fn(),
        )
        if not written:
            # A Replace superseded the row mid-sweep, or its acquisition id
            # moved under us. Frozen rows are audit records; the next sweep
            # sees the new row.
            return CanonicalReconcileResult(
                request_id,
                OUTCOME_FROZEN,
                acquisition_release_id=identity.release_id,
                canonical_release_id=survivor,
                previous_canonical_release_id=previous,
            )
        return CanonicalReconcileResult(
            request_id,
            OUTCOME_RESOLVED,
            acquisition_release_id=identity.release_id,
            canonical_release_id=survivor,
            previous_canonical_release_id=previous,
        )

    def reconcile_all(
        self,
        *,
        on_result: Callable[[CanonicalReconcileResult], None] | None = None,
    ) -> CanonicalSweepResult:
        """Sweep every non-``replaced`` request once.

        ``on_result`` is called per row so the oneshot can stream progress
        to the journal without the service accumulating ten minutes of
        output before saying anything.
        """
        rows: Iterable[Mapping[str, object]] = self._db.list_non_replaced_requests()
        counts: dict[str, int] = {}
        resolved: list[CanonicalReconcileResult] = []
        scanned = 0
        for row in rows:
            scanned += 1
            result = self.reconcile_row(row)
            counts[result.outcome] = counts.get(result.outcome, 0) + 1
            if result.changed:
                resolved.append(result)
            if on_result is not None:
                on_result(result)
        return CanonicalSweepResult(
            scanned=scanned,
            resolved=tuple(resolved),
            outcome_counts=counts,
        )


__all__ = [
    "OUTCOME_FROZEN",
    "OUTCOME_INVALID_IDENTITY",
    "OUTCOME_NOT_FOUND",
    "OUTCOME_NOT_MUSICBRAINZ",
    "OUTCOME_NO_CANONICAL",
    "OUTCOME_NO_REDIRECT",
    "OUTCOME_RESOLVED",
    "OUTCOME_RETIRED",
    "OUTCOME_STALE",
    "OUTCOME_UNCHANGED",
    "QUIET_OUTCOMES",
    "CanonicalReconcileResult",
    "CanonicalReleaseDB",
    "CanonicalReleaseService",
    "CanonicalRetireResult",
    "CanonicalSweepResult",
    "configure_reconciliation_mirror",
]
