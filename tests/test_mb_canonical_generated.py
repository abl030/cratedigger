"""Generated properties for MusicBrainz merge-redirect resolution (#1049).

The pins in ``tests/test_mb_canonical.py`` prove the exact live shapes; these
properties patrol the world space around them, driving the REAL
``BeetsDB`` resolver over a real Beets library.

Invariants patrolled — each is a module-level checker so the known-bad
self-tests below can call it directly:

C1  A resolution never names a sibling pressing. Only the stored id or the
    single successor MusicBrainz declares may resolve.
C2  The stored acquisition identity is never mutated, and a miss always
    names the identity the caller asked for.
C3  Fail-open: an unresolvable lookup is byte-identical to the literal
    resolver, so a mirror that is down or serving a 4xx is never worse.
C4  Miss-triggered: a lookup is paid only for a MusicBrainz identity whose
    literal join already failed — never for a hit, never for Discogs.
C5  A remaining miss means exactly one thing: no declared successor, or a
    successor Beets does not hold either.
"""

from __future__ import annotations

import configparser
import unittest
from pathlib import Path

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_db import (
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    open_beets_db,
)
from lib.config import CratediggerConfig
from lib.release_identity import ReleaseIdentity
from tests.beets_world import BeetsWorld, BeetsWorldRelease

REPO = Path(__file__).resolve().parent.parent

STORED = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
SIBLING = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DISCOGS = "12856590"

# What Beets currently holds for the release under test.
HELD_SIDES = ("none", "stored", "survivor")


def _identity(release_id: str) -> ReleaseIdentity:
    identity = ReleaseIdentity.from_id(release_id)
    assert identity is not None
    return identity


# ---------------------------------------------------------------------------
# Invariant checkers — module level so the known-bad self-tests can call them
# ---------------------------------------------------------------------------


def check_never_a_sibling(
    resolution: CurrentBeetsResolution,
    sibling_album_path: str | None,
) -> None:
    """C1 — strict pressing identity survives redirect-following."""
    if not isinstance(resolution, CurrentBeetsUnique):
        return
    if sibling_album_path is not None and resolution.album_path == sibling_album_path:
        raise AssertionError(
            "resolver substituted a sibling pressing for the target release"
        )
    if resolution.effective_identity.release_id not in (STORED, SURVIVOR):
        raise AssertionError(
            "resolution holds an identity that is neither the stored id nor "
            f"its declared successor: {resolution.effective_identity.release_id}"
        )


def check_resolution_answers_with_the_requested_identity(
    requested: ReleaseIdentity,
    resolution: CurrentBeetsResolution,
) -> None:
    """C6 — a resolution NEVER answers with someone else's identity.

    ``lib/banding.py``, ``lib/quality_evidence.py`` and the destructive
    services all compare a resolution back to its request, and
    ``assert_current_resolution`` calls any other value "resolver
    substituted another release identity". What Beets actually holds
    travels in ``held_identity`` instead. An earlier revision of this PR
    returned the survivor as ``.identity`` and broke all three.
    """
    if resolution.identity != requested:
        raise AssertionError(
            "resolution answered with a substituted identity: "
            f"{resolution.identity!r} != {requested!r}"
        )


def check_identity_never_mutated(
    requested: ReleaseIdentity,
    resolution: CurrentBeetsResolution,
) -> None:
    """C2 — the frozen acquisition identity is never rewritten."""
    if requested.release_id != STORED and requested.release_id != DISCOGS:
        raise AssertionError("the requested identity itself was mutated")
    if isinstance(resolution, CurrentBeetsMissing) and resolution.identity != requested:
        raise AssertionError(
            "a miss named an identity other than the one asked for: "
            f"{resolution.identity!r} != {requested!r}"
        )


def check_fail_open_matches_literal(
    unresolvable: CurrentBeetsResolution,
    literal: CurrentBeetsResolution,
) -> None:
    """C3 — mirror down is never worse than the status quo."""
    if unresolvable != literal:
        raise AssertionError(
            "an unresolvable lookup diverged from the literal resolver: "
            f"{unresolvable!r} != {literal!r}"
        )


def check_lookup_is_miss_triggered(
    lookups: list[str],
    requested: ReleaseIdentity,
    literal: CurrentBeetsResolution,
) -> None:
    """C4 — the trigger is the miss, not a scan."""
    literal_missed = isinstance(literal, CurrentBeetsMissing)
    expected = (
        [requested.release_id]
        if literal_missed and requested.source == "musicbrainz"
        else []
    )
    if lookups != expected:
        raise AssertionError(
            f"canonical lookups were not miss-triggered: {lookups!r} != {expected!r}"
        )


def check_remaining_miss_has_one_meaning(
    resolution: CurrentBeetsResolution,
    *,
    merge_declared: bool,
    survivor_held: bool,
    resolvable: bool,
) -> None:
    """C5 — after resolve-on-miss, a miss cannot be a followed merge."""
    if not isinstance(resolution, CurrentBeetsMissing):
        return
    if merge_declared and survivor_held and resolvable:
        raise AssertionError(
            "a release held under its declared successor still reported "
            "missing after resolve-on-miss"
        )


def check_real_consumers_accept(
    requested: ReleaseIdentity,
    resolution: CurrentBeetsResolution,
) -> None:
    """C7 — the REAL downstream readers accept what the resolver writes.

    ``CurrentBeetsResolution`` is a shared namespace with a dozen readers,
    and a guard over a shared namespace legislates for every other reader
    of it. Stopping this property at the resolver is exactly what let an
    earlier revision ship a resolution that made ``lib/banding.py`` raise
    for the whole long-tail cohort while every resolver-scope test stayed
    green (adversarial review, 2026-08-06).

    So drive the real consumers, not a restatement of their rules.
    """
    from lib.banding import (
        CurrentBeetsBandingAmbiguityError,
        band_current_resolutions,
    )
    from lib.quality import QualityRankConfig

    # Banding is the strictest reader: it raises outright on a substituted
    # identity. An ambiguous resolution legitimately aborts the batch, so
    # only that expected failure is tolerated here.
    try:
        band_current_resolutions(
            {requested: resolution}, QualityRankConfig.defaults(),
        )
    except CurrentBeetsBandingAmbiguityError:
        pass

    # The evidence authority's identity gate, called for real.
    from lib.beets_db import release_identity_for_lookup
    expected = release_identity_for_lookup(requested.release_id)
    if expected is not None and resolution.identity != expected:
        raise AssertionError(
            "lib/quality_evidence.py's identity gate would report 'current "
            "Beets resolution identity does not match evidence request'"
        )


class _World:
    """One real Beets library plus the recorded canonical lookups."""

    def __init__(self, world: BeetsWorld) -> None:
        self.world = world
        self.lookups: list[str] = []

    def hold(self, release_id: str, *, suffix: str) -> str:
        return self.world.import_release(BeetsWorldRelease(
            release_id=release_id,
            artist="Archivist",
            album=f"Exact pressing {suffix}",
            year=2001,
            track_count=2,
        )).album_path

    def open(self, canonical_map: dict[str, str] | None):
        def canonical_fn(release_id: str) -> str | None:
            self.lookups.append(release_id)
            return (canonical_map or {}).get(release_id)

        ini = configparser.RawConfigParser()
        ini["Beets"] = {
            "directory": str(self.world.library_root),
            "library": str(self.world.library_db),
        }
        return open_beets_db(
            CratediggerConfig.from_ini(ini),
            canonical_release_fn=canonical_fn,
        )


class TestMergeFollowingProperties(unittest.TestCase):
    @settings(
        deadline=None,
        max_examples=40,
        suppress_health_check=[HealthCheck.too_slow],
    )
    @given(
        held=st.sampled_from(HELD_SIDES),
        merge_declared=st.booleans(),
        resolvable=st.booleans(),
        sibling_present=st.booleans(),
        use_discogs_identity=st.booleans(),
    )
    # The live 316 shape: held only under the survivor, merge declared.
    @example(
        held="survivor",
        merge_declared=True,
        resolvable=True,
        sibling_present=True,
        use_discogs_identity=False,
    )
    # A sibling retag with no upstream merge — must stay missing.
    @example(
        held="none",
        merge_declared=False,
        resolvable=True,
        sibling_present=True,
        use_discogs_identity=False,
    )
    # The latent shape: still held under the stored id, nothing to follow.
    @example(
        held="stored",
        merge_declared=True,
        resolvable=True,
        sibling_present=False,
        use_discogs_identity=False,
    )
    def test_merge_following_upholds_every_invariant(
        self,
        held: str,
        merge_declared: bool,
        resolvable: bool,
        sibling_present: bool,
        use_discogs_identity: bool,
    ) -> None:
        with BeetsWorld(REPO) as raw:
            world = _World(raw)
            sibling_path = (
                world.hold(SIBLING, suffix="sibling") if sibling_present else None
            )
            if held == "stored":
                world.hold(STORED, suffix="stored")
            elif held == "survivor":
                world.hold(SURVIVOR, suffix="survivor")

            requested = _identity(DISCOGS if use_discogs_identity else STORED)
            canonical_map = (
                {STORED: SURVIVOR} if (merge_declared and resolvable) else {}
            )

            with world.open(canonical_map) as beets:
                resolution = beets.resolve_current_release(requested)
            lookups = list(world.lookups)

            # The literal baseline: the same world with nothing to follow.
            baseline = _World(raw)
            with baseline.open({}) as beets:
                literal = beets.resolve_current_release(requested)

            check_never_a_sibling(resolution, sibling_path)
            check_identity_never_mutated(requested, resolution)
            check_resolution_answers_with_the_requested_identity(
                requested, resolution,
            )
            check_real_consumers_accept(requested, resolution)
            check_lookup_is_miss_triggered(lookups, requested, literal)
            check_remaining_miss_has_one_meaning(
                resolution,
                merge_declared=merge_declared and not use_discogs_identity,
                survivor_held=(held == "survivor"),
                resolvable=resolvable,
            )
            if not canonical_map or use_discogs_identity:
                check_fail_open_matches_literal(resolution, literal)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every checker owes a planted violation proving it can fail."""

    def test_sibling_substitution_is_rejected(self) -> None:
        planted = CurrentBeetsUnique(
            identity=_identity(SIBLING),
            album_id=9,
            album_path="/library/sibling",
            items=(),
            selectors=(),
        )
        with self.assertRaises(AssertionError):
            check_never_a_sibling(planted, "/library/sibling")

    def test_foreign_identity_in_resolution_is_rejected(self) -> None:
        planted = CurrentBeetsUnique(
            identity=_identity(SIBLING),
            album_id=9,
            album_path="/library/elsewhere",
            items=(),
            selectors=(),
        )
        with self.assertRaises(AssertionError):
            check_never_a_sibling(planted, "/library/sibling")

    def test_miss_naming_another_identity_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_identity_never_mutated(
                _identity(STORED),
                CurrentBeetsMissing(identity=_identity(SURVIVOR)),
            )

    def test_fail_open_divergence_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_fail_open_matches_literal(
                CurrentBeetsUnique(
                    identity=_identity(STORED),
                    album_id=1,
                    album_path="/library/a",
                    items=(),
                    selectors=(),
                ),
                CurrentBeetsMissing(identity=_identity(STORED)),
            )

    def test_scanning_instead_of_miss_triggering_is_rejected(self) -> None:
        hit = CurrentBeetsUnique(
            identity=_identity(STORED),
            album_id=1,
            album_path="/library/a",
            items=(),
            selectors=(),
        )
        with self.assertRaises(AssertionError):
            check_lookup_is_miss_triggered([STORED], _identity(STORED), hit)

    def test_discogs_lookup_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_lookup_is_miss_triggered(
                [DISCOGS],
                _identity(DISCOGS),
                CurrentBeetsMissing(identity=_identity(DISCOGS)),
            )

    def test_substituted_identity_in_a_resolution_is_rejected(self) -> None:
        """C6's known-bad: the exact defect adversarial review found."""
        planted = CurrentBeetsUnique(
            identity=_identity(SURVIVOR),
            album_id=1,
            album_path="/library/a",
            items=(),
            selectors=(),
        )
        with self.assertRaises(AssertionError):
            check_resolution_answers_with_the_requested_identity(
                _identity(STORED), planted,
            )

    def test_real_consumers_reject_a_substituted_identity(self) -> None:
        """C7's known-bad: the REAL banding reader must trip on it."""
        planted = CurrentBeetsUnique(
            identity=_identity(SURVIVOR),
            album_id=1,
            album_path="/library/a",
            items=(),
            selectors=(),
        )
        with self.assertRaises(Exception) as caught:
            check_real_consumers_accept(_identity(STORED), planted)
        self.assertNotIsInstance(caught.exception, AssertionError)

    def test_unfollowed_declared_merge_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_remaining_miss_has_one_meaning(
                CurrentBeetsMissing(identity=_identity(STORED)),
                merge_declared=True,
                survivor_held=True,
                resolvable=True,
            )


if __name__ == "__main__":
    unittest.main()
