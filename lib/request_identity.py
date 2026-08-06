"""The one place a request's release identities become a Beets resolution.

A MusicBrainz merge splits one request's identity in two. The acquisition id
is frozen history — "I went and got release X" — and the survivor is what
MusicBrainz calls that release now. Which of the two Beets holds depends on
whether ``mbsync`` has retagged the local files yet, and on a real library
that answer differs row by row: of the six merged requests measured on
2026-08-06, two were installed under the survivor and four still under the
loser. A resolver keyed on either id alone fixes one group and breaks the
other, so the join resolves the **union**.

**This module is the only union site.** ``BeetsDB.resolve_current_releases``
keeps its exact release-keyed contract, because callers that legitimately
ask "do I hold *this release*" — the browse overlay, the add-path collision
check — must not acquire a request's opinion. Merge-following inside the
resolver is the rejected design: it put the decision inside a shared
namespace with a dozen readers and broke four of them across two review
rounds (branch ``feat/mb-canonical-redirects``, PR #1056).

**Both sides resolving to different albums is ambiguous, never a pick.**
That is the double-sided merge: two pressings deliberately held as separate
acquisitions, which MusicBrainz later declares are one release. It is the
point where MusicBrainz's identity model contradicts this repository's
load-bearing invariant that different pressings ARE different releases, and
no release-keyed join can settle it. Zero live instances; it fails closed as
``merged_identity_split`` and waits for the operator.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from typing import Protocol

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.release_identity import ReleaseIdentity


class CurrentBeetsBatchResolver(Protocol):
    """The exact-resolver surface this module composes over."""

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]: ...


def acquisition_identity(
    row: Mapping[str, object],
) -> ReleaseIdentity | None:
    """The frozen identity the request was created for.

    This is what the request *asked* for and it never moves, so it is the
    identity every resolution reports and every audit compares against.
    """
    return ReleaseIdentity.from_strict_fields(
        row.get("mb_release_id"),
        row.get("discogs_release_id"),
    )


def canonical_identity(
    row: Mapping[str, object],
) -> ReleaseIdentity | None:
    """The merge survivor MusicBrainz declared, when one is stored.

    Only ever populated by the reconciler from an observed ``301``, and the
    schema refuses a value equal to the acquisition id, so a non-None result
    here is always a genuinely different pressing identity.
    """
    return ReleaseIdentity.from_id(row.get("canonical_release_id"))


def acceptable_identities(
    row: Mapping[str, object],
) -> tuple[ReleaseIdentity, ...]:
    """Every release identity that legitimately answers for this request.

    Canonical first, then acquisition — a deterministic preference order, so
    a caller that must choose one (candidate matching) and a caller that
    must probe all (the join) agree about which is preferred without
    re-deriving the rule.

    **This is the single definition of "acceptable" for the whole change.**
    The join, beets validation, candidate matching, the harness duplicate
    query, the duplicate-remove guard and post-import verification all read
    it here. Two of those disagreeing is the defect class #1059 exists to
    close: today the match demands the acquisition id while beets offers the
    survivor, and the import fails rc=4 forever.

    Empty when the row has no usable exact identity at all; callers treat
    that as an authority failure, never as absence.
    """
    identities: list[ReleaseIdentity] = []
    for identity in (canonical_identity(row), acquisition_identity(row)):
        if identity is not None and identity not in identities:
            identities.append(identity)
    return tuple(identities)


def merge_union_resolutions(
    acquisition: ReleaseIdentity,
    resolutions: Sequence[CurrentBeetsResolution],
) -> CurrentBeetsResolution:
    """Fold the per-identity resolutions of one request into one answer.

    The rule, in the order the branches are decided:

    * any ambiguous side          → that ambiguity
    * two uniques, different rows → ``merged_identity_split``
    * one or more uniques, same   → that unique
    * nothing found               → missing

    **Every result names the acquisition identity, never the canonical
    one.** A dozen consumers compare a resolution's identity against the
    request's stored id and treat a mismatch as "the resolver substituted
    another release identity" — ``lib/quality_evidence.py`` fails the whole
    evidence build on it. Returning the survivor because that is the side
    Beets happens to hold would break every one of them, which is round 1
    of the aborted attempt exactly (branch ``feat/mb-canonical-redirects``).

    ``selectors`` is deliberately NOT rewritten. The identity answers "what
    did this request ask for"; the selectors answer "where is the album
    actually filed", which after a retag is the survivor. A destructive
    action needs the second, and silently rewriting it to an id Beets no
    longer stores would make the removal a no-op.
    """
    ambiguous = [r for r in resolutions if isinstance(r, CurrentBeetsAmbiguous)]
    if ambiguous:
        first = ambiguous[0]
        return replace(first, identity=acquisition)

    uniques = [r for r in resolutions if isinstance(r, CurrentBeetsUnique)]
    if not uniques:
        return CurrentBeetsMissing(identity=acquisition)

    album_ids = sorted({unique.album_id for unique in uniques})
    if len(album_ids) > 1:
        return CurrentBeetsAmbiguous(
            identity=acquisition,
            album_ids=tuple(album_ids),
            reason="merged_identity_split",
        )
    return replace(uniques[0], identity=acquisition)


def resolve_current_for_requests(
    beets: CurrentBeetsBatchResolver,
    rows: Iterable[Mapping[str, object]],
) -> dict[int, CurrentBeetsResolution]:
    """Resolve many requests over their unions in ONE batched query.

    The identity set is flattened across every row before a single
    ``resolve_current_releases`` call, so a merged cohort costs the same one
    round trip an unmerged one does. Rows without a usable exact identity
    are absent from the result; callers must not read that as absence.
    """
    by_request: dict[int, tuple[ReleaseIdentity, ...]] = {}
    for row in rows:
        raw_id = row.get("id")
        if not isinstance(raw_id, int):
            continue
        identities = acceptable_identities(row)
        if not identities:
            continue
        by_request[raw_id] = identities

    wanted: list[ReleaseIdentity] = []
    for identities in by_request.values():
        for identity in identities:
            if identity not in wanted:
                wanted.append(identity)

    observed = beets.resolve_current_releases(wanted) if wanted else {}

    resolved: dict[int, CurrentBeetsResolution] = {}
    for request_id, identities in by_request.items():
        # The acquisition identity is always last in the tuple.
        acquisition = identities[-1]
        sides = [
            observed[identity]
            for identity in identities
            if identity in observed
        ]
        if len(sides) != len(identities):
            # The resolver omitted a requested identity. That is an
            # authority failure, not an absence claim, so the request is
            # left out rather than reported as missing.
            continue
        resolved[request_id] = merge_union_resolutions(acquisition, sides)
    return resolved


def resolve_current_for_request(
    beets: CurrentBeetsBatchResolver,
    row: Mapping[str, object],
) -> CurrentBeetsResolution | None:
    """Resolve one request over its union. ``None`` means unresolvable."""
    identities = acceptable_identities(row)
    if not identities:
        return None
    observed = beets.resolve_current_releases(list(identities))
    sides = [observed[i] for i in identities if i in observed]
    if len(sides) != len(identities):
        return None
    return merge_union_resolutions(identities[-1], sides)


__all__ = [
    "CurrentBeetsBatchResolver",
    "acceptable_identities",
    "acquisition_identity",
    "canonical_identity",
    "merge_union_resolutions",
    "resolve_current_for_request",
    "resolve_current_for_requests",
]
