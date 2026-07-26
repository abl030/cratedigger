#!/usr/bin/env python3
"""Generated companion for issue #868's materialize-failure evidence.

The deterministic pins live in ``tests/test_download.py``
(``TestEventPathMaterialization``, ``TestPollActiveDownloads``),
``tests/test_path_authority.py`` (``TestAuthorityFailureClassification``)
and ``tests/test_pipeline_db.py``. This module patrols the world space
around them.

Invariants under patrol
-----------------------

**I2 — the four distinguishable preflight outcomes never collapse.**
"slskd never stamped a location", "the stamp points at nothing",
"the name failed containment" and "the storage layer refused the open"
are four different operator problems. Before #868 they collapsed into
two strings, one of them produced by sniffing ``"No such file"`` out of
an exception message.

**I3 — containment and storage never cross.** A symlink, a path escape
or a special file is a SECURITY finding; ESTALE/EIO from virtiofs is a
sick mount. Neither may ever be reported as the other.

**I4 — the reason is derived from a structured field.** Never parsed out
of an exception's message text, so no reason can be truncated by a
message that happens to contain a colon.

**I5 — the lifecycle is unchanged by the split.** The status transition,
the cooldown, and the grace/retry decision depend on the failure TAG and
the grace window, never on which reason string the failure carries.

The properties drive REAL production functions
(``lib.fs_authority._raise_path_error``,
``lib.download_materialization._materialize_processing_dir`` /
``source_preflight_reason`` / ``materialize_authority_reason``,
``lib.download._enqueue_completed_processing``) against a real temporary
filesystem and a real ``FakePipelineDB``. Nothing this repo owns is
mocked: the writer, the guard and the persister all run for real.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

from __future__ import annotations

import errno
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given, settings
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.download import (
    PROCESSING_MATERIALIZE_GRACE_S,
    _enqueue_completed_processing,
    materialize_failure_action,
)
from lib.download_materialization import (
    REASON_EVENT_PATH_GONE_FROM_DISK,
    REASON_EVENT_PATH_NEVER_STAMPED,
    REASON_PROCESSING_AUTHORITY_UNSAFE,
    REASON_PROCESSING_OPEN_FAILED_PREFIX,
    REASON_PROCESSING_PATH_MISSING,
    REASON_SOURCE_OPEN_FAILED_PREFIX,
    REASON_UNSAFE_SOURCE_PATH,
    MaterializeFailed,
    MaterializeGuarded,
    Materialized,
    _materialize_processing_dir,
    materialize_authority_reason,
    source_preflight_reason,
)
from lib.fs_authority import _raise_path_error
from lib.grab_list import DownloadFile
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.quality import ActiveDownloadState
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_grab_list_entry, make_request_row


# ============================================================================
# Invariant checkers — module level so the known-bad self-tests can call them
# ============================================================================

_CONTAINMENT_REASONS = frozenset({
    REASON_UNSAFE_SOURCE_PATH,
    REASON_PROCESSING_AUTHORITY_UNSAFE,
})
_MISSING_REASONS = frozenset({
    REASON_EVENT_PATH_GONE_FROM_DISK,
    REASON_PROCESSING_PATH_MISSING,
})
_STORAGE_PREFIXES = (
    REASON_SOURCE_OPEN_FAILED_PREFIX,
    REASON_PROCESSING_OPEN_FAILED_PREFIX,
)
# Restated, not imported: a checker that groups by the same object
# production groups by would only echo the implementation back.
_CONTAINMENT_CODES = frozenset({"path_escape", "unsafe_symlink", "not_regular_file"})


def reason_family(reason: str) -> str:
    """Bucket one reason into its family without knowing the mapper."""
    if reason in _CONTAINMENT_REASONS:
        return "containment"
    if reason in _MISSING_REASONS:
        return "missing"
    if reason.startswith(_STORAGE_PREFIXES):
        return "storage"
    if reason == REASON_EVENT_PATH_NEVER_STAMPED:
        return "never_stamped"
    return "other"


def assert_reason_partition_invariant(
    *,
    reason: str,
    repeated_reason: str,
    expected_family: str,
    errno_symbol: str | None,
) -> None:
    """One authority refusal maps to exactly one stable, parseable reason.

    Deliberately independent of the mapper it checks: it re-derives the
    family from the reason string alone, so a mapper that answered
    ``unsafe_source_path`` for an ESTALE would be caught here rather than
    by echoing the implementation back at itself.
    """
    if reason != repeated_reason:
        raise AssertionError(
            f"reason was not stable across calls: {reason!r} vs {repeated_reason!r}",
        )
    if ":" in reason:
        raise AssertionError(
            f"reason {reason!r} contains a colon — the retired "
            "``str(exc).split(':', 1)[0]`` derivation would truncate it",
        )
    family = reason_family(reason)
    if family == "other":
        raise AssertionError(f"reason {reason!r} belongs to no known family")
    if family != expected_family:
        raise AssertionError(
            f"reason {reason!r} is family {family!r}, expected {expected_family!r}",
        )
    if family == "storage":
        if errno_symbol is None:
            raise AssertionError("a storage reason must carry an errno symbol")
        if not reason.endswith(errno_symbol):
            raise AssertionError(
                f"storage reason {reason!r} lost its errno {errno_symbol!r}",
            )
    elif errno_symbol is not None:
        raise AssertionError(
            f"non-storage reason {reason!r} was built from errno {errno_symbol!r}",
        )


def assert_lifecycle_unchanged_by_reason(
    *,
    reason: str,
    grace_expired: bool,
    status: str,
    cooldowns_applied: list[str],
    log_outcomes: list[str | None],
    persisted_details: list[str | None],
) -> None:
    """The reason decides the EVIDENCE, never the lifecycle (I1 + I5).

    Expired grace always resets to ``wanted`` with one cooldown and one
    ``failed`` row carrying the reason; an open grace window always
    leaves the row alone with no audit at all — whatever the reason is.
    """
    if not grace_expired:
        if status != "downloading":
            raise AssertionError(
                f"open grace window transitioned to {status!r}",
            )
        if cooldowns_applied or log_outcomes:
            raise AssertionError(
                "open grace window applied a cooldown or wrote an audit row",
            )
        return
    if status != "wanted":
        raise AssertionError(f"expired grace left status {status!r}, expected wanted")
    if cooldowns_applied != ["user1"]:
        raise AssertionError(
            f"expired grace applied cooldowns {cooldowns_applied!r}, expected ['user1']",
        )
    if log_outcomes != ["failed"]:
        raise AssertionError(
            f"expired grace wrote outcomes {log_outcomes!r}, expected ['failed']",
        )
    if persisted_details != [reason]:
        raise AssertionError(
            f"expired grace persisted {persisted_details!r}, expected [{reason!r}] "
            "— a reset without a recoverable cause",
        )


# ============================================================================
# Property 1 — the mapping is total, stable and non-crossing over every errno
# ============================================================================

_ALL_ERRNOS = sorted(errno.errorcode)
_CONTAINMENT_ERRNOS = (errno.ELOOP, errno.ENOTDIR)


class TestGeneratedAuthorityCodeMapping(unittest.TestCase):
    """I2/I3/I4 over the REAL ``_raise_path_error`` classifier."""

    @given(
        number=st.sampled_from(_ALL_ERRNOS),
        path=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-./: ",
            min_size=1,
            max_size=48,
        ),
        private_tree=st.booleans(),
    )
    # The live virtiofs shapes, plus the two that used to be conflated.
    @example(number=errno.ESTALE, path="a/b.mp3", private_tree=False)
    @example(number=errno.EIO, path="a/b.mp3", private_tree=False)
    @example(number=errno.ELOOP, path="a/b.mp3", private_tree=False)
    @example(number=errno.ENOENT, path="a/b.mp3", private_tree=False)
    # A path whose own text contains a colon: the retired colon-split
    # derivation truncated exactly here.
    @example(number=errno.ESTALE, path="weird: name.mp3", private_tree=False)
    def test_every_errno_maps_to_exactly_one_stable_reason(
        self, number: int, path: str, private_tree: bool,
    ) -> None:
        exc = _raise_path_error(path, OSError(number, os.strerror(number), path))
        mapper = materialize_authority_reason if private_tree else source_preflight_reason

        if number in _CONTAINMENT_ERRNOS:
            expected_family = "containment"
        elif number == errno.ENOENT:
            expected_family = "missing"
        else:
            expected_family = "storage"

        assert_reason_partition_invariant(
            reason=mapper(exc),
            repeated_reason=mapper(exc),
            expected_family=expected_family,
            errno_symbol=exc.errno_symbol,
        )
        self.assertEqual(
            exc.code in _CONTAINMENT_CODES,
            expected_family == "containment",
        )


# ============================================================================
# Property 2 — real materialize worlds, real filesystem, one reason each
# ============================================================================

_FAILING_WORLDS = (
    "never_stamped",
    "gone_from_disk",
    "symlink",
    "escape",
    "special_file",
    "unreadable",
)
_WORLD_REASONS = {
    "never_stamped": REASON_EVENT_PATH_NEVER_STAMPED,
    "gone_from_disk": REASON_EVENT_PATH_GONE_FROM_DISK,
    "symlink": REASON_UNSAFE_SOURCE_PATH,
    "escape": REASON_UNSAFE_SOURCE_PATH,
    "special_file": REASON_UNSAFE_SOURCE_PATH,
    "unreadable": f"{REASON_SOURCE_OPEN_FAILED_PREFIX}EACCES",
}
_WORLD_FAMILIES = {
    "never_stamped": "never_stamped",
    "gone_from_disk": "missing",
    "symlink": "containment",
    "escape": "containment",
    "special_file": "containment",
    "unreadable": "storage",
}


def _build_world(
    parent: str, world: str, leaf: str,
) -> tuple[CratediggerConfig, DownloadFile, list[str]]:
    """Materialize one generated failure world onto a real filesystem.

    Returns the config, the single tracked file, and the paths whose
    chmod must be restored before the tempdir is torn down.
    """
    source = os.path.join(parent, "downloads")
    processing = os.path.join(parent, "processing")
    incoming = os.path.join(parent, "Incoming")
    os.mkdir(source)
    os.mkdir(processing, 0o700)
    os.mkdir(os.path.join(processing, "albums"), 0o700)
    os.mkdir(os.path.join(processing, "preview"), 0o700)
    os.mkdir(incoming)
    cfg = CratediggerConfig(
        slskd_download_dir=source,
        processing_dir=processing,
        beets_staging_dir=incoming,
        audio_check_mode="off",
    )
    file = DownloadFile(
        filename=f"peer\\\\{leaf}.mp3", username="user1", id="1",
        file_dir="peer", size=5,
    )
    restore: list[str] = []

    if world == "never_stamped":
        file.local_path = None
        return cfg, file, restore

    stamped = os.path.join(source, f"{leaf}.mp3")
    if world == "gone_from_disk":
        file.local_path = stamped
    elif world == "symlink":
        outside = os.path.join(parent, "outside.mp3")
        with open(outside, "wb") as handle:
            handle.write(b"audio")
        os.symlink(outside, stamped)
        file.local_path = stamped
    elif world == "escape":
        escaped = os.path.join(parent, f"{leaf}-escaped.mp3")
        with open(escaped, "wb") as handle:
            handle.write(b"audio")
        file.local_path = escaped
    elif world == "special_file":
        os.mkfifo(stamped)
        file.local_path = stamped
    elif world == "unreadable":
        with open(stamped, "wb") as handle:
            handle.write(b"audio")
        os.chmod(stamped, 0o000)
        restore.append(stamped)
        file.local_path = stamped
    else:  # pragma: no cover - the strategy only produces the worlds above
        raise AssertionError(f"unknown world {world!r}")
    return cfg, file, restore


_LEAVES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-",
    min_size=1,
    max_size=24,
)


class TestGeneratedMaterializeFailureReasons(unittest.TestCase):
    """I2/I3 driven through the REAL private materialize publisher."""

    @given(world=st.sampled_from(_FAILING_WORLDS), leaf=_LEAVES)
    @example(world="never_stamped", leaf="track")
    @example(world="gone_from_disk", leaf="track")
    @example(world="unreadable", leaf="track")
    @settings(deadline=None)
    def test_each_failure_world_yields_exactly_its_own_reason(
        self, world: str, leaf: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as parent:
            cfg, file, restore = _build_world(parent, world, leaf)
            album = make_grab_list_entry(
                files=[file], artist="Artist", title="Album", year="2020")
            canonical = canonical_folder_for_row(
                album, processing_albums_dir(cfg.processing_dir))
            try:
                result = _materialize_processing_dir(
                    album,
                    StagedAlbum.from_entry(album, default_path=canonical),
                    make_ctx_with_fake_db(FakePipelineDB(), cfg=cfg),
                )
            finally:
                for path in restore:
                    os.chmod(path, 0o600)

            self.assertIsInstance(result, MaterializeFailed)
            assert isinstance(result, MaterializeFailed)
            assert_reason_partition_invariant(
                reason=result.reason,
                repeated_reason=result.reason,
                expected_family=_WORLD_FAMILIES[world],
                errno_symbol=(
                    "EACCES" if _WORLD_FAMILIES[world] == "storage" else None
                ),
            )
            self.assertEqual(result.reason, _WORLD_REASONS[world])
            # Nothing was published: a refused preflight copies no bytes.
            self.assertFalse(os.path.isdir(canonical))

    @given(
        world=st.sampled_from(_FAILING_WORLDS),
        other=st.sampled_from(_FAILING_WORLDS),
        leaf=_LEAVES,
    )
    @settings(deadline=None)
    def test_distinct_causes_never_share_a_reason(
        self, world: str, other: str, leaf: str,
    ) -> None:
        """I2 stated negatively: two worlds share a reason only when they
        are the same containment violation."""
        same_reason = _WORLD_REASONS[world] == _WORLD_REASONS[other]
        both_containment = (
            _WORLD_FAMILIES[world] == "containment"
            and _WORLD_FAMILIES[other] == "containment"
        )
        del leaf
        if world != other:
            self.assertEqual(same_reason, both_containment)
        else:
            self.assertTrue(same_reason)


# ============================================================================
# Property 3 — the lifecycle is decided by the tag, never by the reason (I5)
# ============================================================================


class TestGeneratedMaterializeLifecycle(unittest.TestCase):
    @given(world=st.sampled_from(_FAILING_WORLDS), grace_expired=st.booleans())
    @example(world="never_stamped", grace_expired=True)
    @example(world="gone_from_disk", grace_expired=True)
    @example(world="unreadable", grace_expired=False)
    @settings(deadline=None)
    def test_reason_changes_the_evidence_not_the_lifecycle(
        self, world: str, grace_expired: bool,
    ) -> None:
        """Drives the REAL poller enqueue path: real materialize, real
        grace arbitration, real transition, real audit write."""
        now = datetime.now(timezone.utc)
        started = (
            now - timedelta(seconds=PROCESSING_MATERIALIZE_GRACE_S + 600)
            if grace_expired
            else now
        )
        with tempfile.TemporaryDirectory() as parent:
            cfg, file, restore = _build_world(parent, world, "track")
            album = make_grab_list_entry(
                files=[file], artist="Artist", title="Album", year="2020",
                db_request_id=1, mb_release_id="")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=1, status="downloading"))
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            # The HAVE-evidence enrichment is a different subsystem; zero
            # budget short-circuits it without stubbing anything.
            ctx.evidence_enrichment_budget = 0
            state = ActiveDownloadState(
                filetype="mp3",
                enqueued_at=started.isoformat(),
                files=[],
                processing_started_at=started.isoformat(),
            )
            try:
                job = _enqueue_completed_processing(album, 1, state, db, ctx)
            finally:
                for path in restore:
                    os.chmod(path, 0o600)

            self.assertIsNone(job)
            assert_lifecycle_unchanged_by_reason(
                reason=_WORLD_REASONS[world],
                grace_expired=grace_expired,
                status=db.request(1)["status"],
                cooldowns_applied=list(db.cooldowns_applied),
                log_outcomes=[row.outcome for row in db.download_logs],
                persisted_details=[row.beets_detail for row in db.download_logs],
            )

    @given(reason=st.text(min_size=0, max_size=64), age_seconds=st.integers(
        min_value=0, max_value=PROCESSING_MATERIALIZE_GRACE_S * 3))
    def test_grace_arbitration_ignores_the_reason_string(
        self, reason: str, age_seconds: int,
    ) -> None:
        """I5 at the decision itself: only the tag and the clock matter."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(seconds=age_seconds)).isoformat()
        expected = "reset" if age_seconds > PROCESSING_MATERIALIZE_GRACE_S else "retry"
        self.assertEqual(
            materialize_failure_action(MaterializeFailed(reason=reason), started, now),
            expected,
        )
        # The same clock, a guarded tag: never auto-reset, whatever the age.
        self.assertEqual(
            materialize_failure_action(MaterializeGuarded(detail=reason), started, now),
            "leave",
        )
        self.assertEqual(
            materialize_failure_action(Materialized(), started, now),
            "leave",
        )


# ============================================================================
# Known-bad self-tests — a checker that never trips proves nothing
# ============================================================================


class TestMaterializeEvidenceCheckersTripOnViolations(unittest.TestCase):
    def test_partition_checker_rejects_a_storage_errno_called_containment(self) -> None:
        with self.assertRaises(AssertionError):
            assert_reason_partition_invariant(
                reason=REASON_UNSAFE_SOURCE_PATH,
                repeated_reason=REASON_UNSAFE_SOURCE_PATH,
                expected_family="storage",
                errno_symbol="ESTALE",
            )

    def test_partition_checker_rejects_a_containment_called_storage(self) -> None:
        with self.assertRaises(AssertionError):
            assert_reason_partition_invariant(
                reason=f"{REASON_SOURCE_OPEN_FAILED_PREFIX}ESTALE",
                repeated_reason=f"{REASON_SOURCE_OPEN_FAILED_PREFIX}ESTALE",
                expected_family="containment",
                errno_symbol="ESTALE",
            )

    def test_partition_checker_rejects_a_colon_bearing_reason(self) -> None:
        with self.assertRaises(AssertionError):
            assert_reason_partition_invariant(
                reason="unsafe_source_path: /peer/track.mp3",
                repeated_reason="unsafe_source_path: /peer/track.mp3",
                expected_family="containment",
                errno_symbol=None,
            )

    def test_partition_checker_rejects_an_unstable_reason(self) -> None:
        with self.assertRaises(AssertionError):
            assert_reason_partition_invariant(
                reason=REASON_UNSAFE_SOURCE_PATH,
                repeated_reason=REASON_EVENT_PATH_GONE_FROM_DISK,
                expected_family="containment",
                errno_symbol=None,
            )

    def test_partition_checker_rejects_a_storage_reason_without_its_errno(self) -> None:
        with self.assertRaises(AssertionError):
            assert_reason_partition_invariant(
                reason=f"{REASON_SOURCE_OPEN_FAILED_PREFIX}UNKNOWN",
                repeated_reason=f"{REASON_SOURCE_OPEN_FAILED_PREFIX}UNKNOWN",
                expected_family="storage",
                errno_symbol="ESTALE",
            )

    def test_lifecycle_checker_rejects_a_reset_with_no_recoverable_cause(self) -> None:
        with self.assertRaises(AssertionError):
            assert_lifecycle_unchanged_by_reason(
                reason=REASON_EVENT_PATH_NEVER_STAMPED,
                grace_expired=True,
                status="wanted",
                cooldowns_applied=["user1"],
                log_outcomes=["failed"],
                persisted_details=[None],
            )

    def test_lifecycle_checker_rejects_a_dropped_cooldown(self) -> None:
        with self.assertRaises(AssertionError):
            assert_lifecycle_unchanged_by_reason(
                reason=REASON_EVENT_PATH_NEVER_STAMPED,
                grace_expired=True,
                status="wanted",
                cooldowns_applied=[],
                log_outcomes=["failed"],
                persisted_details=[REASON_EVENT_PATH_NEVER_STAMPED],
            )

    def test_lifecycle_checker_rejects_a_reset_inside_the_grace_window(self) -> None:
        with self.assertRaises(AssertionError):
            assert_lifecycle_unchanged_by_reason(
                reason=REASON_EVENT_PATH_NEVER_STAMPED,
                grace_expired=False,
                status="wanted",
                cooldowns_applied=[],
                log_outcomes=[],
                persisted_details=[],
            )

    def test_family_bucketing_rejects_an_unknown_reason(self) -> None:
        self.assertEqual(reason_family("something_new"), "other")
        with self.assertRaises(AssertionError):
            assert_reason_partition_invariant(
                reason="something_new",
                repeated_reason="something_new",
                expected_family="containment",
                errno_symbol=None,
            )


if __name__ == "__main__":
    unittest.main()
