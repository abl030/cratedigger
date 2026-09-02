"""Generated properties for the one-album file-tag sync lane (#1260).

The pins in ``tests/test_beets_tag_sync.py`` prove the exact branches;
these properties patrol the world space around them, driving BOTH entry
points production actually calls over every combination of (entry × DB
identity × authorized identity × release resolution × authority failure ×
file-tag world × lock state × what the write subprocess actually does):

* ``sync_album_file_tags_from_borrowed_factory`` — the census card's
  button, through ``web/routes/retag_divergence_audit.py`` and the
  ``pipeline-cli sync-file-tags`` relay behind it.
* ``sync_release_file_tags_from_factory`` — the merge seam, through
  ``lib/download_validation.py``.

Until #1313 every clause below ran against a third entry point,
``sync_album_file_tags_from_factory``, which had no production caller at
all: a bystander by the house outermost-real-adapter rule, and the one
whose lifecycle (it closed the handle) neither survivor shares. It is
gone; the entry dimension is what replaced it. The boundary stops here
rather than at the HTTP route above it, because the route only maps an
outcome to a status code and ``tests/web/test_routes_retag_divergence_
audit.py`` owns that mapping.

Invariants patrolled — each a module-level accumulating checker (every
clause evaluates; ordering cannot mask one) with a message-asserting
known-bad self-test per clause:

W   **Write gating.** The ``beet write`` subprocess runs exactly once, and
    ONLY in the one world that authorizes a file mutation: the album
    exists, the caller's authorized identity is a MusicBrainz id equal to
    the album's own DB identity, at least one readable file disagrees (or
    is unreadable), and the RELEASE lock was granted. Its query tokens
    always pin BOTH the album id and that DB identity.
V   **Verdict from the re-read files.** ``synced`` is claimed only over a
    world whose re-read tags all agree; ``residual_divergence`` only over
    one that still disagrees; ``already_synced`` never follows a write;
    every typed refusal leaves the file-tag world byte-identical; and once
    the write RAN the verdict comes from the re-read alone — within THIS
    harness's world space, ``synced`` iff converged else
    ``residual_divergence``, whatever exit code the subprocess reported
    (the exit-code doctrine's consequence, V6). That totality is a
    harness-scoped claim, not a production one: production legitimately
    returns ``not_found`` (album vanished mid-sync) or, through the
    mediated wrapper, ``beets_unavailable`` (authority raised on the
    post-write re-read) after a landed write — worlds ``_FakeSyncBeets``
    cannot produce, which is exactly why those outcomes after a write ARE
    the returncode-mutant signature here. #1313 added authority failure to
    the world space and V6 survived unwidened, because all three injection
    sites fire strictly BEFORE the write: no world can reach the write and
    then lose the authority, so ``beets_unavailable`` after a write is
    still the mutant signature and not a legitimate outcome. Two worlds
    would still have to widen it: an album that vanishes mid-sync (that
    one widens V4 as well, since its ``not_found`` IS in the refusal set),
    and an authority that fails on the POST-write re-read (V6 only — V4
    deliberately excludes ``beets_unavailable``, which is the whole point
    of ``test_the_beets_unavailable_outcome_is_not_a_refusal_clause``).
R   **Release resolution gating.** The release entry's refusal names what
    Beets actually said: an ambiguous resolution refuses ``not_unique``, a
    missing one ``not_found``, and ``not_unique`` is claimed nowhere else.
A   **Authority failure is typed, never raised, and named.** Whenever the
    Beets authority actually went away — at the factory, at the identity
    read, or at the release resolution — the caller is told
    ``beets_unavailable`` (A1), reached through one mediator so the two
    entries cannot drift apart on it. The failure fires where the world
    put it (A2), the result names the album the dead site had actually
    reached (A3, the one operator-visible difference between a failed
    identity read and a failed resolution), and no other refusal wears
    the ``beets_unavailable`` name (A4).
L   **Handle lifecycle.** The borrowed entry NEVER closes the handle it
    was lent, on any path including every refusal — the request thread
    that lent it keeps using it. The release entry closes every handle it
    opened, exactly once, on every path past the open.
I   **Seam inertness.** ``lib.download_validation.
    _sync_file_tags_after_merge_rekey`` — the merge seam's best-effort
    caller — returns ``None`` and never raises, whatever the sync
    delivers: any outcome, or any raised exception type.
"""

from __future__ import annotations

import contextlib
import logging
import unittest
from collections.abc import Generator
from dataclasses import dataclass

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_child import BeetsChildRun
from lib.beets_db import CurrentBeetsAmbiguous, CurrentBeetsUnique
from lib.beets_tag_sync import (
    RESULT_ALREADY_SYNCED,
    RESULT_BEETS_UNAVAILABLE,
    RESULT_NOT_FOUND,
    RESULT_NOT_UNIQUE,
    RESULT_RESIDUAL_DIVERGENCE,
    RESULT_SYNCED,
    TAG_SYNC_HTTP_STATUS,
    TagSyncResult,
    sync_album_file_tags_from_borrowed_factory,
    sync_release_file_tags_from_factory,
)
from lib.config import CratediggerConfig
from lib.download_validation import _sync_file_tags_after_merge_rekey
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakePipelineDB
from tests.test_beets_tag_sync import (
    ALBUM_ID,
    DB_ID,
    OLD_TAG,
    _FakeSyncBeets,
    real_beets_authority_failure,
)

THIRD_ID = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"

#: The identity pool every strategy draws from — pre-normalized (lowercase
#: UUIDs) so the checkers' independent equality logic stays trivial.
IDENTITIES = (DB_ID, OLD_TAG, THIRD_ID)

WRITE_MODES = (
    "applies", "applies_nonzero", "noop", "raise", "raise_after_apply",
)

#: The two production entry points, by the lifecycle each takes.
ENTRIES = ("borrowed_album", "owned_release")

#: What Beets answers the release entry's ``resolve_current_release``.
#: Inert on the album entry, which is handed an album id directly.
RESOLUTIONS = ("unique", "missing", "ambiguous")

#: Where the Beets authority goes away, if it does. ``open`` is the
#: factory raising, ``read``/``resolve`` are the two reads a live handle
#: can lose the library under. Whether an injected failure actually FIRES
#: depends on how far the code got, so nothing here is derived from the
#: control flow — the checkers read the fake's own raise counter.
AUTHORITY_FAILURES = ("none", "open", "read", "resolve")

_REFUSAL_OUTCOMES = frozenset({
    "not_found", "identity_mismatch", "db_identity_absent",
    "release_locked", "not_unique",
})


@contextlib.contextmanager
def _silence_logs() -> Generator[None]:
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous)


@dataclass(frozen=True)
class SyncWorld:
    """One generated world, plus what running the real service produced."""

    entry: str
    album_present: bool
    db_identity: str
    #: The album entry's authorized identity, and the release entry's
    #: release id — the same value either way, because the release entry
    #: authorizes the resolved album with the release id it was given.
    expected: str
    resolution: str
    authority_failure: str
    #: (path, initial file tag, unreadable) per item.
    files: tuple[tuple[str, str, bool], ...]
    lock_granted: bool
    write_mode: str


@dataclass(frozen=True)
class SyncRun:
    world: SyncWorld
    result: TagSyncResult
    write_calls: tuple[tuple[str, str], ...]
    initial_tags: dict[str, str]
    final_tags: dict[str, str]
    open_calls: int
    close_calls: int
    #: One entry per firing of the injected authority failure, naming
    #: where it fired: "open" for the factory, "read" for the identity
    #: read, "resolve" for the release resolution.
    authority_raise_sites: tuple[str, ...]

    @property
    def authority_raises(self) -> int:
        return len(self.authority_raise_sites)


def _files_strategy() -> st.SearchStrategy[tuple[tuple[str, str, bool], ...]]:
    entry = st.tuples(
        st.sampled_from(IDENTITIES + ("",)),
        # Unreadable is a real but uncommon world, and at 50% it halved
        # how often any album had a readable divergent file to write.
        st.sampled_from((False, False, False, True)),
    )
    return st.lists(entry, min_size=0, max_size=3).map(
        lambda entries: tuple(
            (f"/library/a/{ordinal:02d}.opus", tag, unreadable)
            for ordinal, (tag, unreadable) in enumerate(entries, start=1)
        ),
    )


#: Weighted so the write path is reachable often enough to patrol. Every
#: world the uniform version could draw is still drawable — this steers
#: the budget, it does not filter (#1313). Measured: the uniform strategy
#: authorized a write 1 time in 400 and never once through the release
#: entry.
#:
#: Re-measured for #1313 batch E, because adding an ``@example`` reseeds
#: the whole derandomized sweep — Hypothesis digests the decorated source,
#: so every count below moves whenever a pin is added or removed, and any
#: figure here is a snapshot of one tree. Over the 165 examples the gating
#: tier now runs: 7 album writes, 1 release write, 4 refused release
#: locks, 12 DB-identity mismatches, 5 absent DB identities, 7 albums with
#: nothing readable diverging, and all four reachable (entry, authority
#: site) cells. Every arm a checker clause decides on is non-zero there,
#: which was not true before those pins: the release write is thin enough
#: to depend on its own ``@example``, so do not read these pins as
#: belt-and-braces. Re-run the arm census after touching a pin.
_MOSTLY_TRUE = st.sampled_from((True, True, True, False))


@st.composite
def sync_worlds(draw: st.DrawFn) -> SyncWorld:
    db_identity = draw(st.sampled_from(IDENTITIES + ("",)))
    return SyncWorld(
        entry=draw(st.sampled_from(ENTRIES)),
        album_present=draw(_MOSTLY_TRUE),
        db_identity=db_identity,
        # The authorizing case is one exact value out of six, so drawing
        # it independently almost never happens. Half the worlds now take
        # it, and the other half still draw the whole pool.
        # "123456" is the only value here that parses to a real DISCOGS
        # identity; "junk" and "[r123456]" both resolve to no identity at
        # all, so without it the guards' source != "musicbrainz" half is
        # unreachable by the whole harness (#1313 mutant runner).
        expected=draw(st.one_of(
            st.just(db_identity),
            st.sampled_from(IDENTITIES + ("", "junk", "[r123456]", "123456")),
        )),
        resolution=draw(st.sampled_from(("unique", *RESOLUTIONS))),
        # Weighted toward a live authority so the write path keeps its
        # share of the budget; every failure site is still drawable.
        authority_failure=draw(st.sampled_from(
            ("none", "none", "none", *AUTHORITY_FAILURES[1:]),
        )),
        files=draw(_files_strategy()),
        lock_granted=draw(_MOSTLY_TRUE),
        write_mode=draw(st.sampled_from(WRITE_MODES)),
    )


def _seed_resolution(beets: _FakeSyncBeets, world: SyncWorld) -> None:
    """Teach the fake what Beets answers for ``world.expected``.

    Only reached for a real MusicBrainz id: the release entry refuses
    anything else before it ever opens a handle, so a resolution keyed on
    ``"junk"`` would be unreachable and the checkers would be legislating
    over a world production cannot produce.
    """
    identity = ReleaseIdentity(source="musicbrainz", release_id=world.expected)
    if world.resolution == "unique":
        beets.resolutions[world.expected] = CurrentBeetsUnique(
            identity=identity, album_id=ALBUM_ID,
            album_path="/library/a", items=(), selectors=(),
        )
    elif world.resolution == "ambiguous":
        beets.resolutions[world.expected] = CurrentBeetsAmbiguous(
            identity=identity, album_ids=(7, 9), reason="multiple_matches",
        )
    # "missing" is the fake's own default — seed nothing.


def run_sync_world(world: SyncWorld) -> SyncRun:
    """Drive the REAL service over ``world`` with leaf-seam fakes only."""
    beets = _FakeSyncBeets(
        fail_authority_on=(
            world.authority_failure
            if world.authority_failure in ("read", "resolve") else ""
        ),
    )
    if world.album_present:
        beets.seed_album(
            ALBUM_ID, world.db_identity,
            tuple(path for path, _tag, _unreadable in world.files),
        )
        for path, tag, unreadable in world.files:
            beets.file_tags[path] = tag
            if unreadable:
                beets.unreadable.add(path)
    if world.entry == "owned_release" and world.expected in IDENTITIES:
        _seed_resolution(beets, world)
    initial_tags = dict(beets.file_tags)

    write_calls: list[tuple[str, str]] = []

    def run_write(query_tokens: tuple[str, str]) -> BeetsChildRun:
        write_calls.append(query_tokens)
        if world.write_mode == "raise":
            raise RuntimeError("write exploded before touching anything")
        if world.write_mode in (
            "applies", "applies_nonzero", "raise_after_apply",
        ):
            album_token, identity_token = query_tokens
            wanted = identity_token.split(":=", 1)[1]
            row = beets.rows.get(int(album_token.split(":=", 1)[1]))
            if row is not None and row.mb_albumid == wanted:
                for path in row.item_paths:
                    if path not in beets.unreadable:
                        beets.file_tags[path] = row.mb_albumid
        if world.write_mode == "raise_after_apply":
            raise RuntimeError("write exploded after the files moved")
        # "applies_nonzero" is the exit-code-is-not-evidence world in the
        # OTHER direction: the write landed AND the subprocess reported
        # failure (#1260 reader suspect 3).
        returncode = 2 if world.write_mode == "applies_nonzero" else 0
        return BeetsChildRun(returncode=returncode, stdout="", stderr="")

    lock_db = FakePipelineDB()
    lock_db.set_advisory_lock_result(world.lock_granted)

    opens = 0
    open_raises = 0

    def factory() -> _FakeSyncBeets:
        nonlocal opens, open_raises
        if world.authority_failure == "open":
            open_raises += 1
            raise real_beets_authority_failure()
        opens += 1
        return beets

    with _silence_logs():
        if world.entry == "borrowed_album":
            result = sync_album_file_tags_from_borrowed_factory(
                factory,
                lock_db,
                album_id=ALBUM_ID,
                expected_mb_albumid=world.expected,
                read_tag=beets.read_tag,
                run_write=run_write,
            )
        else:
            result = sync_release_file_tags_from_factory(
                factory,
                lock_db,
                release_id=world.expected,
                read_tag=beets.read_tag,
                run_write=run_write,
            )
    return SyncRun(
        world=world,
        result=result,
        write_calls=tuple(write_calls),
        initial_tags=initial_tags,
        final_tags=dict(beets.file_tags),
        open_calls=opens,
        close_calls=beets.close_calls,
        authority_raise_sites=(
            ("open",) * open_raises + tuple(beets.authority_raise_sites)
        ),
    )


def _write_authorized(run: SyncRun) -> bool:
    """The one world shape that authorizes a write, independently derived.

    Since #1260 review F6 the write additionally requires at least one
    READABLE file that actually disagrees — an album whose only
    non-agreeing items are unreadable refuses without launching the
    subprocess, because a write cannot heal what cannot be read back.

    The release entry adds one condition on top of the album entry's, and
    only one: Beets must name exactly one current album at that release.
    Everything after the resolution is the same authorization, which is
    why the entry dimension multiplies this world space instead of
    forking it (#1313).

    Whether an injected authority failure fired is read off the fake's own
    raise counter rather than re-derived from how far the code should have
    got — that derivation would be the production control flow written
    twice, and it would agree with production by construction.
    """
    if run.authority_raises:
        return False
    world = run.world
    if not world.album_present:
        return False
    if world.expected not in IDENTITIES:
        return False
    if world.entry == "owned_release" and world.resolution != "unique":
        return False
    if world.db_identity == "" or world.db_identity != world.expected:
        return False
    has_readable_divergence = any(
        not unreadable and tag != world.db_identity
        for _path, tag, unreadable in world.files
    )
    if not has_readable_divergence:
        return False
    return world.lock_granted


def _post_converged(run: SyncRun) -> bool:
    world = run.world
    if not world.files:
        return True
    return all(
        not unreadable and run.final_tags.get(path, "") == world.db_identity
        for path, _tag, unreadable in world.files
    )


def write_gating_violations(run: SyncRun) -> list[str]:
    """W — every clause evaluates; ordering cannot mask one."""
    violations: list[str] = []
    authorized = _write_authorized(run)
    if run.write_calls and not authorized:
        violations.append(
            "W1: the write ran in a world that never authorizes one",
        )
    if authorized and not run.write_calls:
        violations.append(
            "W2: an authorized divergent world never reached the write",
        )
    if len(run.write_calls) > 1:
        violations.append("W3: the write ran more than once")
    expected_tokens = (
        f"album_id:={ALBUM_ID}", f"mb_albumid:={run.world.db_identity}",
    )
    for tokens in run.write_calls:
        if tokens != expected_tokens:
            violations.append(
                "W4: the write query drifted off the authorized album/identity "
                f"pin ({tokens!r})",
            )
    return violations


def verdict_violations(run: SyncRun) -> list[str]:
    """V — the verdict comes from the re-read files, never the write."""
    violations: list[str] = []
    outcome = run.result.outcome
    if outcome == RESULT_SYNCED and not _post_converged(run):
        violations.append(
            "V1: synced claimed while a re-read file still disagrees",
        )
    if outcome == RESULT_RESIDUAL_DIVERGENCE and _post_converged(run):
        violations.append(
            "V2: residual_divergence claimed over a converged world",
        )
    if outcome == RESULT_ALREADY_SYNCED and run.write_calls:
        violations.append("V3: already_synced claimed after a write ran")
    if outcome in _REFUSAL_OUTCOMES and run.final_tags != run.initial_tags:
        violations.append(
            f"V4: refusal {outcome} mutated the file-tag world",
        )
    if outcome not in TAG_SYNC_HTTP_STATUS:
        violations.append(f"V5: unmapped outcome {outcome!r}")
    if run.write_calls:
        # V6 — the exit-code doctrine's consequence (#1278 item-4
        # reflection, RD1/SD1): once the write RAN, the verdict comes from
        # the re-read files alone. V1/V2 police the synced/residual claims
        # individually but let a returncode-driven mutant escape into any
        # OTHER mapped outcome (beets_unavailable was the proven survivor
        # shape); this clause closes that door. HARNESS-SCOPED, not a
        # production totality claim: production's post-write not_found
        # (album vanished mid-sync, lib/beets_tag_sync.py's post-is-None
        # arm) and the mediated wrapper's beets_unavailable (authority
        # raised on the re-read) are correct behavior in worlds
        # _FakeSyncBeets cannot produce — so seeing them here is evidence
        # of a returncode-driven verdict. #1313 added authority failure to
        # the strategy and this clause did NOT need widening: all three
        # injection sites fire before the write. A strategy that fails the
        # authority on the POST-write re-read, or deletes the album
        # mid-sync, still would.
        expected = (
            RESULT_SYNCED if _post_converged(run)
            else RESULT_RESIDUAL_DIVERGENCE
        )
        if outcome != expected:
            violations.append(
                f"V6: after a write the verdict must be {expected!r} (from "
                f"the re-read files alone), not {outcome!r} — the exit "
                "status decides nothing",
            )
    return violations


def release_resolution_violations(run: SyncRun) -> list[str]:
    """R — the release entry's refusal names what Beets actually said.

    R1 and R2 hold Beets to its own answer, so they only apply when Beets
    got to give one. An authority that went away has no answer to be held
    to, and ``beets_unavailable`` is then the honest outcome — the same
    ``run.authority_raises`` counter ``_write_authorized`` reads, for the
    same reason: whether the injected failure FIRED is an observation, not
    something to re-derive from how far the code should have got.
    """
    violations: list[str] = []
    world = run.world
    outcome = run.result.outcome
    resolved = (
        world.entry == "owned_release"
        and world.expected in IDENTITIES
        and not run.authority_raises
    )
    if resolved and world.resolution == "ambiguous" \
            and outcome != RESULT_NOT_UNIQUE:
        violations.append(
            "R1: an ambiguous release resolution must refuse not_unique, "
            f"not {outcome!r}",
        )
    if resolved and world.resolution == "missing" \
            and outcome != RESULT_NOT_FOUND:
        violations.append(
            "R2: a release no album holds must refuse not_found, not "
            f"{outcome!r}",
        )
    if outcome == RESULT_NOT_UNIQUE and not (
        resolved and world.resolution == "ambiguous"
    ):
        violations.append(
            "R3: not_unique claimed outside an ambiguous release resolution",
        )
    return violations


def _unavailable_album_id(entry: str, site: str) -> int | None:
    """What a failure at ``site`` leaves the run able to say about the album.

    The borrowed entry is handed an album id by its caller, so it names
    that album however early the authority dies. The release entry learns
    its album only from a unique resolution, so a failure at the open or
    at the resolution itself has no album to report. Every resolution the
    harness seeds names ``ALBUM_ID``, so that is the one answer past the
    resolve.
    """
    if entry == "borrowed_album" or site == "read":
        return ALBUM_ID
    return None


def authority_violations(run: SyncRun) -> list[str]:
    """A — a Beets authority that goes away is typed, never raised.

    A2 and A3 are what let this checker tell a failed identity read from
    a failed release resolution (#1313 residual 1332-5). While A1 read a
    bare counter, swapping the fake's two failure sites left this whole
    property green while three deterministic pins went red (measured on
    ``ce493ba8``). A3 is what kills that swap: the fake's label and the
    album production then reports disagree, and which album an
    unavailable result names is the one operator-visible difference
    between the two reads. A2 catches the neighbouring shape, a failure
    that fires where no world asked for one, which is what inverting the
    fake's site comparison does.
    """
    violations: list[str] = []
    outcome = run.result.outcome
    if run.authority_raise_sites and outcome != RESULT_BEETS_UNAVAILABLE:
        violations.append(
            "A1: the Beets authority failed and the caller was told "
            f"{outcome!r} instead of beets_unavailable",
        )
    for site in run.authority_raise_sites:
        if site != run.world.authority_failure:
            violations.append(
                f"A2: the authority was set to fail at "
                f"{run.world.authority_failure!r} and fired at {site!r}",
            )
    if outcome == RESULT_BEETS_UNAVAILABLE:
        for site in run.authority_raise_sites:
            expected = _unavailable_album_id(run.world.entry, site)
            if run.result.album_id != expected:
                violations.append(
                    f"A3: the authority died at {site!r} on the "
                    f"{run.world.entry} entry, which should report "
                    f"album_id={expected!r} and reported "
                    f"{run.result.album_id!r}",
                )
        if not run.authority_raise_sites:
            violations.append(
                "A4: beets_unavailable claimed while the authority never "
                "failed; some other refusal is wearing its name",
            )
    return violations


def lifecycle_violations(run: SyncRun) -> list[str]:
    """L — who closes the Beets handle is the difference between the two
    entry points, so it is the one thing neither can be sloppy about."""
    violations: list[str] = []
    if run.world.entry == "borrowed_album" and run.close_calls:
        violations.append(
            f"L1: the borrowed entry closed a lent handle {run.close_calls} "
            "time(s); the request thread that lent it is still using it",
        )
    if run.world.entry == "owned_release" \
            and run.close_calls != run.open_calls:
        violations.append(
            f"L2: the release entry opened {run.open_calls} handle(s) and "
            f"closed {run.close_calls}",
        )
    return violations


def _world(
    *,
    entry: str = "borrowed_album",
    album_present: bool = True,
    db_identity: str = DB_ID,
    expected: str = DB_ID,
    resolution: str = "unique",
    authority_failure: str = "none",
    files: tuple[tuple[str, str, bool], ...] = (
        ("/library/a/01.opus", OLD_TAG, False),
    ),
    lock_granted: bool = True,
    write_mode: str = "applies",
) -> SyncWorld:
    """The converging album world every self-test starts from."""
    return SyncWorld(
        entry=entry, album_present=album_present, db_identity=db_identity,
        expected=expected, resolution=resolution,
        authority_failure=authority_failure, files=files,
        lock_granted=lock_granted, write_mode=write_mode,
    )


class TestTagSyncProperties(unittest.TestCase):
    @settings(deadline=None)
    @given(world=sync_worlds())
    @example(world=SyncWorld(
        # The live RA.1000 world: one divergent readable file, lock free.
        entry="borrowed_album",
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        resolution="unique", authority_failure="none",
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="applies",
    ))
    @example(world=SyncWorld(
        # S2's exit-code world: green write that changes nothing.
        entry="borrowed_album",
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        resolution="unique", authority_failure="none",
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="noop",
    ))
    @example(world=SyncWorld(
        # A raise whose effect landed must still read as synced.
        entry="borrowed_album",
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        resolution="unique", authority_failure="none",
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="raise_after_apply",
    ))
    @example(world=SyncWorld(
        # V6's decisive world: the write landed AND exited nonzero — a
        # returncode-reading mutant flips this off ``synced`` (#1278
        # item-4 reflection, SD1). Pinned so the derandomized suite tier
        # kills that mutant, not just the fuzz tier.
        entry="borrowed_album",
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        resolution="unique", authority_failure="none",
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="applies_nonzero",
    ))
    @example(world=SyncWorld(
        # The merge seam's own happy path: the release resolves to one
        # album and the same write lands through the other entry.
        entry="owned_release",
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        resolution="unique", authority_failure="none",
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="applies",
    ))
    @example(world=SyncWorld(
        # L2's decisive world: an ambiguous resolution returns from
        # inside the ``closing`` block, which is where a handle leaks.
        entry="owned_release",
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        resolution="ambiguous", authority_failure="none",
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="applies",
    ))
    @example(world=_world(
        # A1's decisive world: the handle opens, then Beets goes away
        # under the identity read. The release entry must still close.
        entry="owned_release", authority_failure="read",
    ))
    @example(world=_world(
        # The same failure at the release resolution, one call earlier.
        entry="owned_release", authority_failure="resolve",
    ))
    @example(world=_world(authority_failure="open"))
    @example(world=_world(
        # R1/R2's own regression world, and the reason it is pinned: with
        # the authority-failure gate reverted the property stays GREEN at
        # the derandomized suite tier and only goes red at fuzz depth
        # (measured by the #1313 mutant runner). No other @example
        # combines a failed authority with a non-unique resolution — they
        # all default resolution="unique" — so without these two the
        # gating tier's only protection is the deterministic self-test.
        entry="owned_release", resolution="missing", authority_failure="open",
    ))
    @example(world=_world(
        entry="owned_release", resolution="ambiguous",
        authority_failure="resolve",
    ))
    @example(world=_world(
        # A real Discogs identity, the only value in the pool that reaches
        # the guards' source != "musicbrainz" half.
        entry="owned_release", expected="123456",
    ))
    @example(world=_world(
        # The DB-identity guard's own world: the album holds one real
        # MusicBrainz id and the caller authorized a different one, so the
        # lane must refuse instead of writing a stale identity into files.
        # Measured before this pin existed (#1313 residual 1332-5): the
        # derandomized suite tier reached that refusal 0 times in its 162
        # examples while fuzz depth reached it tens of times per 2,000 —
        # the 4 suite-tier worlds that could have were all short-circuited
        # by an earlier authority failure. Without this pin A4 and W1's
        # identity half are patrolled only at fuzz depth. The release
        # entry meets the same guard through the same mediator.
        expected=THIRD_ID,
    ))
    @example(world=_world(
        # A4's other producer: an album whose DB identity is empty. The
        # refusal is db_identity_absent, and a mutant relabelling it
        # beets_unavailable passes every W and V clause. Pinned because
        # this arm is a casualty of the pin above: adding an @example
        # reseeds the whole derandomized sweep (Hypothesis digests the
        # decorated source), and the arm went from 1 drawn example to 0
        # (#1313 batch E, mutant runner S2 and reader F1).
        db_identity="",
    ))
    @example(world=_world(
        # The refused RELEASE lock, the one refusal V4 polices that the
        # derandomized tier never drew: 0 examples on `ce493ba8` and 0
        # with the two pins above, because it needs an otherwise fully
        # authorized write world AND the lock denied. Pre-existing rather
        # than a casualty of those pins, and pinned here because the arm
        # census that found it is this PR's own (#1313 batch E).
        lock_granted=False,
    ))
    def test_write_gating_and_verdict(self, world: SyncWorld) -> None:
        run = run_sync_world(world)
        violations = (
            write_gating_violations(run)
            + verdict_violations(run)
            + release_resolution_violations(run)
            + authority_violations(run)
            + lifecycle_violations(run)
        )
        if violations:
            self.fail(
                f"world={world!r} outcome={run.result.outcome!r}: "
                + "; ".join(violations),
            )


class TestSeamInertness(unittest.TestCase):
    """I — the merge seam's best-effort caller can never change anything."""

    @settings(deadline=None)
    @given(
        behavior=st.sampled_from(("outcome", "raise")),
        outcome=st.sampled_from(sorted(TAG_SYNC_HTTP_STATUS)),
        exception=st.sampled_from((
            RuntimeError("boom"),
            OSError("EIO"),
            ValueError("bad identity"),
            KeyError("missing"),
        )),
    )
    def test_helper_returns_none_and_never_raises(
        self, behavior: str, outcome: str, exception: Exception,
    ) -> None:
        def sync_fn(db: object, cfg: object, release_id: str) -> TagSyncResult:
            del db, cfg
            if behavior == "raise":
                raise exception
            return TagSyncResult(outcome=outcome, error_message=release_id)

        with _silence_logs():
            returned = _sync_file_tags_after_merge_rekey(
                FakePipelineDB(),
                CratediggerConfig(pipeline_db_enabled=True),
                DB_ID,
                sync_fn=sync_fn,
            )
        self.assertIsNone(returned)


class TestCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests — one per clause, message-asserted."""

    def _base_run(
        self,
        *,
        world: SyncWorld | None = None,
        result: TagSyncResult | None = None,
        write_calls: tuple[tuple[str, str], ...] = (
            (f"album_id:={ALBUM_ID}", f"mb_albumid:={DB_ID}"),
        ),
        initial_tags: dict[str, str] | None = None,
        final_tags: dict[str, str] | None = None,
        open_calls: int = 1,
        close_calls: int = 0,
        authority_raise_sites: tuple[str, ...] = (),
    ) -> SyncRun:
        return SyncRun(
            world=world if world is not None else _world(),
            result=result if result is not None else TagSyncResult(
                outcome=RESULT_SYNCED, album_id=ALBUM_ID,
            ),
            write_calls=write_calls,
            initial_tags=(
                initial_tags if initial_tags is not None
                else {"/library/a/01.opus": OLD_TAG}
            ),
            final_tags=(
                final_tags if final_tags is not None
                else {"/library/a/01.opus": DB_ID}
            ),
            open_calls=open_calls,
            close_calls=close_calls,
            authority_raise_sites=authority_raise_sites,
        )

    def test_w1_trips_on_an_unauthorized_write(self) -> None:
        run = self._base_run(world=_world(expected=OLD_TAG))
        self.assertTrue(
            any(v.startswith("W1") for v in write_gating_violations(run)),
        )

    def test_w2_trips_on_a_skipped_authorized_write(self) -> None:
        run = self._base_run(
            write_calls=(),
            final_tags={"/library/a/01.opus": OLD_TAG},
        )
        self.assertTrue(
            any(v.startswith("W2") for v in write_gating_violations(run)),
        )

    def test_w3_trips_on_a_double_write(self) -> None:
        tokens = (f"album_id:={ALBUM_ID}", f"mb_albumid:={DB_ID}")
        run = self._base_run(write_calls=(tokens, tokens))
        self.assertTrue(
            any(v.startswith("W3") for v in write_gating_violations(run)),
        )

    def test_w4_trips_on_a_drifted_query(self) -> None:
        run = self._base_run(
            write_calls=((f"album_id:={ALBUM_ID}", f"mb_albumid:={THIRD_ID}"),),
        )
        self.assertTrue(
            any(v.startswith("W4") for v in write_gating_violations(run)),
        )

    def test_v1_trips_on_synced_over_divergence(self) -> None:
        run = self._base_run(final_tags={"/library/a/01.opus": OLD_TAG})
        self.assertTrue(
            any(v.startswith("V1") for v in verdict_violations(run)),
        )

    def test_v2_trips_on_residual_over_convergence(self) -> None:
        run = self._base_run(
            result=TagSyncResult(
                outcome=RESULT_RESIDUAL_DIVERGENCE, album_id=ALBUM_ID,
            ),
        )
        self.assertTrue(
            any(v.startswith("V2") for v in verdict_violations(run)),
        )

    def test_v3_trips_on_already_synced_after_a_write(self) -> None:
        run = self._base_run(
            result=TagSyncResult(
                outcome=RESULT_ALREADY_SYNCED, album_id=ALBUM_ID,
            ),
            final_tags={"/library/a/01.opus": DB_ID},
        )
        self.assertTrue(
            any(v.startswith("V3") for v in verdict_violations(run)),
        )

    def test_v4_trips_on_a_mutating_refusal(self) -> None:
        run = self._base_run(
            result=TagSyncResult(
                outcome="release_locked", album_id=ALBUM_ID,
            ),
        )
        self.assertTrue(
            any(v.startswith("V4") for v in verdict_violations(run)),
        )

    def test_v5_trips_on_an_unmapped_outcome(self) -> None:
        run = self._base_run(
            result=TagSyncResult(outcome="galaxy_brain", album_id=ALBUM_ID),
        )
        self.assertTrue(
            any(v.startswith("V5") for v in verdict_violations(run)),
        )

    def test_v6_trips_on_a_returncode_driven_verdict_after_a_write(
        self,
    ) -> None:
        """The proven survivor shape: the write ran, the files converged,
        and a mutant reading the exit code claims ``beets_unavailable`` —
        a mapped outcome V1–V5 all wave through."""
        run = self._base_run(
            result=TagSyncResult(
                outcome=RESULT_BEETS_UNAVAILABLE, album_id=ALBUM_ID,
            ),
        )
        violations = verdict_violations(run)
        self.assertTrue(
            any(
                v.startswith("V6") and "beets_unavailable" in v
                for v in violations
            ),
            violations,
        )
        # And every earlier clause stays quiet, proving V6 is the one
        # clause doing the work here (short-circuit masking cannot occur
        # in an accumulating checker, but the point deserves its pin).
        self.assertEqual(
            [v for v in violations if not v.startswith("V6")], [],
        )

    def test_the_beets_unavailable_outcome_is_not_a_refusal_clause(
        self,
    ) -> None:
        """V4's refusal set deliberately excludes ``beets_unavailable``:
        an authority failure can strike after the write mutated files, and
        claiming the world untouched there would be a false invariant."""
        self.assertNotIn(RESULT_BEETS_UNAVAILABLE, _REFUSAL_OUTCOMES)

    def test_r1_trips_when_an_ambiguous_release_refuses_something_else(
        self,
    ) -> None:
        run = self._base_run(
            world=_world(entry="owned_release", resolution="ambiguous"),
            result=TagSyncResult(outcome=RESULT_NOT_FOUND),
            close_calls=1,
        )
        violations = release_resolution_violations(run)
        self.assertTrue(any(v.startswith("R1") for v in violations), violations)

    def test_r2_trips_when_a_missing_release_refuses_something_else(
        self,
    ) -> None:
        run = self._base_run(
            world=_world(entry="owned_release", resolution="missing"),
            result=TagSyncResult(outcome=RESULT_NOT_UNIQUE),
            close_calls=1,
        )
        violations = release_resolution_violations(run)
        self.assertTrue(any(v.startswith("R2") for v in violations), violations)

    def test_r1_and_r2_stay_quiet_when_the_authority_went_away(self) -> None:
        """The false-positive direction, which is where these clauses were
        wrong when they shipped: an authority that failed before the
        resolution leaves Beets with no answer to be held to, and every
        such world is producible by the strategy."""
        for resolution in ("missing", "ambiguous"):
            for site in ("open", "resolve"):
                with self.subTest(resolution=resolution, site=site):
                    run = self._base_run(
                        world=_world(
                            entry="owned_release", resolution=resolution,
                            authority_failure=site,
                        ),
                        result=TagSyncResult(outcome=RESULT_BEETS_UNAVAILABLE),
                        write_calls=(), authority_raise_sites=(site,),
                        final_tags={"/library/a/01.opus": OLD_TAG},
                    )
                    self.assertEqual(release_resolution_violations(run), [])
                    self.assertEqual(authority_violations(run), [])

    def test_r3_trips_on_not_unique_from_the_album_entry(self) -> None:
        """The album entry has no resolution step, so it can never have a
        reason to say not_unique."""
        run = self._base_run(
            result=TagSyncResult(outcome=RESULT_NOT_UNIQUE, album_id=ALBUM_ID),
        )
        violations = release_resolution_violations(run)
        self.assertTrue(any(v.startswith("R3") for v in violations), violations)
        self.assertEqual([v for v in violations if not v.startswith("R3")], [])

    def test_a1_trips_when_a_dead_authority_answers_something_else(
        self,
    ) -> None:
        run = self._base_run(
            world=_world(authority_failure="read"),
            result=TagSyncResult(outcome=RESULT_NOT_FOUND),
            write_calls=(), authority_raise_sites=("read",),
            final_tags={"/library/a/01.opus": OLD_TAG},
        )
        violations = authority_violations(run)
        self.assertTrue(any(v.startswith("A1") for v in violations), violations)

    def test_a2_trips_when_the_failure_fires_somewhere_else(self) -> None:
        """The world asked for a dead identity read and the release
        resolution died instead. A1 cannot see it: the outcome is still
        beets_unavailable. In the running property this is the shape an
        inverted site comparison in the fake produces; the swapped-sites
        version is A3's kill."""
        run = self._base_run(
            world=_world(entry="owned_release", authority_failure="read"),
            result=TagSyncResult(outcome=RESULT_BEETS_UNAVAILABLE),
            write_calls=(), authority_raise_sites=("resolve",),
            final_tags={"/library/a/01.opus": OLD_TAG},
            close_calls=1,
        )
        # Asserted whole: the message names both sites, so a substring
        # check on either one is satisfied by a clause that transposed
        # them (measured by the #1313 batch E mutant runner, MC2).
        self.assertIn(
            "A2: the authority was set to fail at 'read' and fired at "
            "'resolve'",
            authority_violations(run),
        )

    def test_a2_stays_quiet_when_the_named_site_is_never_reached(self) -> None:
        """Q3: the album entry never resolves a release, so a world that
        asks for a dead resolution produces no failure at all. A2 must
        read what fired, not what the world asked for.

        Driven through the real service rather than a hand-built run: a
        constructed empty site tuple only proves an empty loop iterates
        zero times (#1313 batch E, reader F6).
        """
        run = run_sync_world(_world(authority_failure="resolve"))
        self.assertEqual(run.authority_raise_sites, ())
        self.assertEqual(authority_violations(run), [])

    def test_a3_trips_when_a_dead_site_names_the_wrong_album(self) -> None:
        """The release entry has no album until the resolution returns one,
        so an authority that died at the resolve cannot name one."""
        run = self._base_run(
            world=_world(entry="owned_release", authority_failure="resolve"),
            result=TagSyncResult(
                outcome=RESULT_BEETS_UNAVAILABLE, album_id=ALBUM_ID,
            ),
            write_calls=(), authority_raise_sites=("resolve",),
            final_tags={"/library/a/01.opus": OLD_TAG},
            close_calls=1,
        )
        violations = authority_violations(run)
        self.assertTrue(
            any(v.startswith("A3") for v in violations), violations,
        )
        self.assertEqual([v for v in violations if not v.startswith("A3")], [])

    def test_a3_trips_when_the_read_site_reports_no_album(self) -> None:
        """The read happens inside the album lane on both entries, so it
        always has an album to name. Without this the ``site == "read"``
        arm of the mapping has no deterministic pin at all — dropping it
        was killed only by the property (#1313 batch E, mutant runner
        MC3)."""
        run = self._base_run(
            world=_world(entry="owned_release", authority_failure="read"),
            result=TagSyncResult(outcome=RESULT_BEETS_UNAVAILABLE),
            write_calls=(), authority_raise_sites=("read",),
            final_tags={"/library/a/01.opus": OLD_TAG},
            close_calls=1,
        )
        violations = authority_violations(run)
        self.assertTrue(
            any(v.startswith("A3") for v in violations), violations,
        )
        self.assertEqual([v for v in violations if not v.startswith("A3")], [])

    def test_a3_stays_quiet_when_the_borrowed_entry_dies_at_the_open(
        self,
    ) -> None:
        """Q3: nothing was read, and the result still names an album —
        correctly, because the caller supplied it. A clause reading "died
        early, so it knows nothing" would accuse production here."""
        run = self._base_run(
            world=_world(authority_failure="open"),
            result=TagSyncResult(
                outcome=RESULT_BEETS_UNAVAILABLE, album_id=ALBUM_ID,
            ),
            write_calls=(), authority_raise_sites=("open",),
            open_calls=0,
            final_tags={"/library/a/01.opus": OLD_TAG},
        )
        self.assertEqual(authority_violations(run), [])

    def test_a4_trips_when_a_refusal_wears_the_unavailable_name(self) -> None:
        """A refusal relabelled beets_unavailable passes every V clause: no
        write ran, and the outcome is mapped."""
        run = self._base_run(
            world=_world(expected=THIRD_ID),
            result=TagSyncResult(
                outcome=RESULT_BEETS_UNAVAILABLE, album_id=ALBUM_ID,
            ),
            write_calls=(),
            final_tags={"/library/a/01.opus": OLD_TAG},
        )
        violations = authority_violations(run)
        self.assertTrue(any(v.startswith("A4") for v in violations), violations)
        # A4 is the only clause with anything to say about this run, in
        # its own family and in every other. Asserted rather than traced
        # (#1313 batch E, reader F7).
        self.assertEqual([v for v in violations if not v.startswith("A4")], [])
        self.assertEqual(verdict_violations(run), [])
        self.assertEqual(write_gating_violations(run), [])
        self.assertEqual(release_resolution_violations(run), [])
        self.assertEqual(lifecycle_violations(run), [])

    def test_l1_trips_when_the_borrowed_entry_closes_a_lent_handle(
        self,
    ) -> None:
        run = self._base_run(close_calls=1)
        violations = lifecycle_violations(run)
        self.assertTrue(any(v.startswith("L1") for v in violations), violations)

    def test_l2_trips_when_the_release_entry_leaks_its_own_handle(
        self,
    ) -> None:
        run = self._base_run(
            world=_world(entry="owned_release"),
            open_calls=1, close_calls=0,
        )
        violations = lifecycle_violations(run)
        self.assertTrue(any(v.startswith("L2") for v in violations), violations)

    def test_l2_trips_on_a_double_close_too(self) -> None:
        run = self._base_run(
            world=_world(entry="owned_release"),
            open_calls=1, close_calls=2,
        )
        violations = lifecycle_violations(run)
        self.assertTrue(any(v.startswith("L2") for v in violations), violations)


if __name__ == "__main__":
    unittest.main()
