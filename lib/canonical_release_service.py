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

**There is deliberately no clearing path, and that is a known limitation
rather than a free one.** A stale survivor — from an upstream un-merge, or a
mirror that once redirected wrongly — is only harmless while nothing holds
that id. If some *other* album is filed under it, the union returns that
album as unique with the identity rewritten to the acquisition id, so no
consumer's identity check trips and another pressing is silently attributed
to this request. ``canonical_release_id()`` also collapses "asked, got a
definitive 200 with no redirect" and "could not ask" into the same ``None``,
so this service structurally cannot tell a retractable canonical from a
mirror outage; adding a clear would need that distinction first. Zero live
instances, and retiring one today means raw SQL. Tracked as #1059 F6.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from lib.mb_canonical import CanonicalReleaseFn, canonical_release_id
from lib.release_identity import ReleaseIdentity

#: One row's outcome. Exactly one per branch, mapped to CLI exit codes and
#: HTTP statuses by the two thin adapters.
OUTCOME_RESOLVED = "resolved"
OUTCOME_UNCHANGED = "unchanged"
OUTCOME_NO_REDIRECT = "no_redirect"
OUTCOME_NOT_MUSICBRAINZ = "not_musicbrainz"
OUTCOME_INVALID_IDENTITY = "invalid_identity"
OUTCOME_FROZEN = "frozen"
OUTCOME_NOT_FOUND = "not_found"

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


#: Hostnames we refuse to sweep. ``musicbrainz.apiBase`` DEFAULTS to public
#: MusicBrainz and the module asserts it is a scheme-prefixed URL, so an
#: "unset base" guard fires only for a config the module cannot produce.
#: A whole-library sweep is ~8,500 requests; against musicbrainz.org, issued
#: as fast as the socket allows, that is an IP ban that takes every other MB
#: consumer in the deployment down with it. Reconciliation is a local-mirror
#: feature: without one, it does nothing rather than something harmful.
#:
#: Compared as a parsed HOSTNAME, never as a substring. DNS is
#: case-insensitive and substring matching is not, so ``MusicBrainz.org``
#: sailed through an earlier substring test straight at public MB; and
#: ``musicbrainz.org.lan`` — the obvious name for a split-horizon local
#: mirror — was refused as though it were public. This mirrors
#: ``web/mb.py::_mirror_concurrency``, which already got it right.
_REFUSED_HOSTS = frozenset({"musicbrainz.org", "www.musicbrainz.org"})


def configure_reconciliation_mirror(mb_api_base: str) -> str | None:
    """Wire the resolver for a sweep, or return ``None`` to refuse.

    **Every** surface that reconciles must call this — the daily oneshot,
    ``pipeline-cli canonical``, and the API route. ``lib/mb_canonical`` is
    inert until a process wires a base, so a surface that forgets does not
    fail loudly: it reports ``no_redirect`` for every row and exits 0,
    which the outcome vocabulary reads as "the library is already correct".
    """
    import urllib.parse

    from lib.mb_canonical import configure_canonical_base

    # ``hostname`` is RFC-lowercased by urlsplit. An empty origin, a blank
    # one, and a bare host with no scheme all parse to hostname=None, so the
    # single check covers "unset" and "unusable" as well as "public" — an
    # explicit emptiness branch alongside it was redundant, and a mutant
    # deleting it was unkillable because it changed nothing.
    origin = (mb_api_base or "").strip()
    host = urllib.parse.urlsplit(origin).hostname
    if host is None or host in _REFUSED_HOSTS:
        return None
    base = origin.rstrip("/") + "/ws/2"
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
    "OUTCOME_NO_REDIRECT",
    "OUTCOME_RESOLVED",
    "OUTCOME_UNCHANGED",
    "QUIET_OUTCOMES",
    "CanonicalReconcileResult",
    "CanonicalReleaseDB",
    "CanonicalReleaseService",
    "CanonicalSweepResult",
    "configure_reconciliation_mirror",
]
