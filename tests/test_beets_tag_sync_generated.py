"""Generated properties for the one-album file-tag sync lane (#1260).

The pins in ``tests/test_beets_tag_sync.py`` prove the exact branches;
these properties patrol the world space around them, driving the REAL
``sync_album_file_tags_from_factory`` over every combination of (DB
identity × authorized identity × file-tag world × lock state × what the
write subprocess actually does).

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
    and every typed refusal leaves the file-tag world byte-identical.
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
from lib.beets_tag_sync import (
    RESULT_ALREADY_SYNCED,
    RESULT_BEETS_UNAVAILABLE,
    RESULT_RESIDUAL_DIVERGENCE,
    RESULT_SYNCED,
    TAG_SYNC_HTTP_STATUS,
    TagSyncResult,
    TagSyncWriteRun,
    sync_album_file_tags_from_factory,
)
from lib.config import CratediggerConfig
from lib.download_validation import _sync_file_tags_after_merge_rekey
from tests.fakes import FakePipelineDB
from tests.test_beets_tag_sync import (
    ALBUM_ID,
    DB_ID,
    OLD_TAG,
    _FakeSyncBeets,
)

THIRD_ID = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"

#: The identity pool every strategy draws from — pre-normalized (lowercase
#: UUIDs) so the checkers' independent equality logic stays trivial.
IDENTITIES = (DB_ID, OLD_TAG, THIRD_ID)

WRITE_MODES = ("applies", "noop", "raise", "raise_after_apply")

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

    album_present: bool
    db_identity: str
    expected: str
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


def _files_strategy() -> st.SearchStrategy[tuple[tuple[str, str, bool], ...]]:
    entry = st.tuples(
        st.sampled_from(IDENTITIES + ("",)),
        st.booleans(),
    )
    return st.lists(entry, min_size=0, max_size=3).map(
        lambda entries: tuple(
            (f"/library/a/{ordinal:02d}.opus", tag, unreadable)
            for ordinal, (tag, unreadable) in enumerate(entries, start=1)
        ),
    )


def sync_worlds() -> st.SearchStrategy[SyncWorld]:
    return st.builds(
        SyncWorld,
        album_present=st.booleans(),
        db_identity=st.sampled_from(IDENTITIES + ("",)),
        expected=st.sampled_from(IDENTITIES + ("", "junk", "[r123456]")),
        files=_files_strategy(),
        lock_granted=st.booleans(),
        write_mode=st.sampled_from(WRITE_MODES),
    )


def run_sync_world(world: SyncWorld) -> SyncRun:
    """Drive the REAL service over ``world`` with leaf-seam fakes only."""
    beets = _FakeSyncBeets()
    if world.album_present:
        beets.seed_album(
            ALBUM_ID, world.db_identity,
            tuple(path for path, _tag, _unreadable in world.files),
        )
        for path, tag, unreadable in world.files:
            beets.file_tags[path] = tag
            if unreadable:
                beets.unreadable.add(path)
    initial_tags = dict(beets.file_tags)

    write_calls: list[tuple[str, str]] = []

    def run_write(query_tokens: tuple[str, str]) -> TagSyncWriteRun:
        write_calls.append(query_tokens)
        if world.write_mode == "raise":
            raise RuntimeError("write exploded before touching anything")
        if world.write_mode in ("applies", "raise_after_apply"):
            album_token, identity_token = query_tokens
            wanted = identity_token.split(":=", 1)[1]
            row = beets.rows.get(int(album_token.split(":=", 1)[1]))
            if row is not None and row.mb_albumid == wanted:
                for path in row.item_paths:
                    if path not in beets.unreadable:
                        beets.file_tags[path] = row.mb_albumid
        if world.write_mode == "raise_after_apply":
            raise RuntimeError("write exploded after the files moved")
        return TagSyncWriteRun(returncode=0, stdout="", stderr="")

    lock_db = FakePipelineDB()
    lock_db.set_advisory_lock_result(world.lock_granted)

    with _silence_logs():
        result = sync_album_file_tags_from_factory(
            lambda: beets,
            lock_db,
            album_id=ALBUM_ID,
            expected_mb_albumid=world.expected,
            read_tag=beets.read_tag,
            run_write=run_write,
        )
    return SyncRun(
        world=world,
        result=result,
        write_calls=tuple(write_calls),
        initial_tags=initial_tags,
        final_tags=dict(beets.file_tags),
    )


def _write_authorized(world: SyncWorld) -> bool:
    """The one world shape that authorizes a write, independently derived."""
    if not world.album_present:
        return False
    if world.expected not in IDENTITIES:
        return False
    if world.db_identity == "" or world.db_identity != world.expected:
        return False
    if not world.files:
        return False
    converged = all(
        not unreadable and tag == world.db_identity
        for _path, tag, unreadable in world.files
    )
    if converged:
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
    authorized = _write_authorized(run.world)
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
    return violations


class TestTagSyncProperties(unittest.TestCase):
    @settings(deadline=None)
    @given(world=sync_worlds())
    @example(world=SyncWorld(
        # The live RA.1000 world: one divergent readable file, lock free.
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="applies",
    ))
    @example(world=SyncWorld(
        # S2's exit-code world: green write that changes nothing.
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="noop",
    ))
    @example(world=SyncWorld(
        # A raise whose effect landed must still read as synced.
        album_present=True, db_identity=DB_ID, expected=DB_ID,
        files=(("/library/a/01.opus", OLD_TAG, False),),
        lock_granted=True, write_mode="raise_after_apply",
    ))
    def test_write_gating_and_verdict(self, world: SyncWorld) -> None:
        run = run_sync_world(world)
        violations = write_gating_violations(run) + verdict_violations(run)
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
    ) -> SyncRun:
        return SyncRun(
            world=world if world is not None else SyncWorld(
                album_present=True, db_identity=DB_ID, expected=DB_ID,
                files=(("/library/a/01.opus", OLD_TAG, False),),
                lock_granted=True, write_mode="applies",
            ),
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
        )

    def test_w1_trips_on_an_unauthorized_write(self) -> None:
        run = self._base_run(
            world=SyncWorld(
                album_present=True, db_identity=DB_ID, expected=OLD_TAG,
                files=(("/library/a/01.opus", OLD_TAG, False),),
                lock_granted=True, write_mode="applies",
            ),
        )
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

    def test_the_beets_unavailable_outcome_is_not_a_refusal_clause(
        self,
    ) -> None:
        """V4's refusal set deliberately excludes ``beets_unavailable``:
        an authority failure can strike after the write mutated files, and
        claiming the world untouched there would be a false invariant."""
        self.assertNotIn(RESULT_BEETS_UNAVAILABLE, _REFUSAL_OUTCOMES)


if __name__ == "__main__":
    unittest.main()
