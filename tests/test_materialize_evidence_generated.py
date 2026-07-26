#!/usr/bin/env python3
"""Generated companion for issue #868's materialize-failure evidence.

The deterministic pins live in ``tests/test_download.py``
(``TestEventPathMaterialization``, ``TestPollActiveDownloads``),
``tests/test_path_authority.py`` (``TestAuthorityFailureClassification``)
and ``tests/test_pipeline_db.py``. This module patrols the world space
around them.

Invariants under patrol
-----------------------

**I1 — a materialize failure never resets a request uncaused.** The
reason reaches the journal at every failure site, and wherever the reset
also writes a ``download_log`` row, that row carries the reason. (The
pre-enqueue gate ``lib.download._processing_path_ready_for_importer``
deliberately writes no row at all — it fails closed before any import
attempt exists to audit — so the journal is its only record. Giving that
path an audit row is a lifecycle change, tracked separately.)

**I2 — distinguishable outcomes never collapse.** "slskd never stamped a
location", "the stamp points at nothing", "the name failed containment"
and "the storage layer refused the open" are four different operator
problems. Before #868 they collapsed into two strings, one of them
produced by sniffing ``"No such file"`` out of an exception message.

**I2b — the three subjects keep their own vocabulary.** One file inside
the share, the whole shared download root, and our own private
processing tree are three different subsystems. The same errno on two of
them must not produce the same reason: ``processing_open_failed_ESTALE``
for a sick slskd share sends the operator to the wrong filesystem.

**I3 — containment and storage never cross.** A symlink, a path escape,
a non-directory component or a special file is a SECURITY finding;
ESTALE/EIO from virtiofs is a sick mount. Neither may ever be reported
as the other — including the shapes that fail at ``open`` before an
``S_ISREG`` check can run (a unix socket answers ENXIO).

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
import socket
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
    REASON_MATERIALIZE_AUTHORITY_FAILED,
    REASON_PROCESSING_AUTHORITY_UNSAFE,
    REASON_PROCESSING_OPEN_FAILED_PREFIX,
    REASON_PROCESSING_PATH_MISSING,
    REASON_SLSKD_ROOT_MISSING,
    REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX,
    REASON_SLSKD_ROOT_REFUSED,
    REASON_SLSKD_ROOT_UNSAFE,
    REASON_SOURCE_OPEN_FAILED_PREFIX,
    REASON_SOURCE_PREFLIGHT_REFUSED,
    REASON_UNSAFE_SOURCE_PATH,
    MaterializeFailed,
    MaterializeGuarded,
    Materialized,
    _materialize_processing_dir,
    materialize_authority_reason,
    shared_download_root_reason,
    source_preflight_reason,
)
from lib.fs_authority import (
    FilesystemAuthorityError,
    SharedDownloadRootError,
    _raise_path_error,
    open_regular_under_held_root,
    open_shared_download_root,
)
from lib.grab_list import DownloadFile
from lib.processing_paths import canonical_folder_for_row, processing_albums_dir
from lib.quality import ActiveDownloadState
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db, make_grab_list_entry, make_request_row
from tests.test_path_authority import assert_publication_invariant


# ============================================================================
# Invariant checkers — module level so the known-bad self-tests can call them
# ============================================================================

_CONTAINMENT_REASONS = frozenset({
    REASON_UNSAFE_SOURCE_PATH,
    REASON_PROCESSING_AUTHORITY_UNSAFE,
    REASON_SLSKD_ROOT_UNSAFE,
})
_MISSING_REASONS = frozenset({
    REASON_EVENT_PATH_GONE_FROM_DISK,
    REASON_PROCESSING_PATH_MISSING,
    REASON_SLSKD_ROOT_MISSING,
})
_STORAGE_PREFIXES = (
    REASON_SOURCE_OPEN_FAILED_PREFIX,
    REASON_PROCESSING_OPEN_FAILED_PREFIX,
    REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX,
)
# "we could not classify this refusal". Its own family on purpose: a
# subject that answered a containment noun here would be manufacturing a
# security finding out of ignorance.
_UNCLASSIFIED_REASONS = frozenset({
    REASON_SOURCE_PREFLIGHT_REFUSED,
    REASON_MATERIALIZE_AUTHORITY_FAILED,
    REASON_SLSKD_ROOT_REFUSED,
})
# Restated, not imported: a checker that groups by the same object
# production groups by would only echo the implementation back.
_CONTAINMENT_CODES = frozenset({
    "path_escape", "unsafe_symlink", "not_a_directory", "not_regular_file",
})


def reason_family(reason: str) -> str:
    """Bucket one reason into its family without knowing the mapper."""
    if reason in _CONTAINMENT_REASONS:
        return "containment"
    if reason in _MISSING_REASONS:
        return "missing"
    if reason.startswith(_STORAGE_PREFIXES):
        return "storage"
    if reason in _UNCLASSIFIED_REASONS:
        return "unclassified"
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

    I1 is scoped to THIS path, which does write a row. The poller's own
    pre-enqueue gate resets without writing one at all and records its
    cause in the journal only; that is deliberate and out of scope here.
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
_CONTAINMENT_ERRNOS = (errno.ELOOP, errno.ENOTDIR, errno.ENXIO, errno.ENODEV)
_SUBJECT_MAPPERS = {
    "source_file": source_preflight_reason,
    "private_tree": materialize_authority_reason,
    "shared_root": shared_download_root_reason,
}


class TestGeneratedAuthorityCodeMapping(unittest.TestCase):
    """I2/I2b/I3/I4 over the REAL ``_raise_path_error`` classifier."""

    @given(
        number=st.sampled_from(_ALL_ERRNOS),
        path=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-./: ",
            min_size=1,
            max_size=48,
        ),
        subject=st.sampled_from(sorted(_SUBJECT_MAPPERS)),
    )
    # The live virtiofs shapes, plus the ones that used to be conflated.
    @example(number=errno.ESTALE, path="a/b.mp3", subject="source_file")
    @example(number=errno.EIO, path="a/b.mp3", subject="shared_root")
    @example(number=errno.ELOOP, path="a/b.mp3", subject="source_file")
    @example(number=errno.ENOENT, path="a/b.mp3", subject="source_file")
    @example(number=errno.ENXIO, path="a/b.mp3", subject="source_file")
    # A path whose own text contains a colon: the retired colon-split
    # derivation truncated exactly here.
    @example(number=errno.ESTALE, path="weird: name.mp3", subject="private_tree")
    def test_every_errno_maps_to_exactly_one_stable_reason(
        self, number: int, path: str, subject: str,
    ) -> None:
        exc = _raise_path_error(path, OSError(number, os.strerror(number), path))
        mapper = _SUBJECT_MAPPERS[subject]

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

    @given(
        number=st.sampled_from(_ALL_ERRNOS),
        path=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-",
            min_size=1,
            max_size=24,
        ),
        structured=st.booleans(),
    )
    @example(number=errno.ESTALE, path="share", structured=True)
    @example(number=errno.ENOENT, path="share", structured=True)
    # ``unspecified`` is unreachable from ``_raise_path_error`` but IS
    # reachable from the module's policy raises, so patrol it explicitly.
    @example(number=errno.ENOENT, path="share", structured=False)
    def test_the_three_subjects_never_share_a_reason(
        self, number: int, path: str, structured: bool,
    ) -> None:
        """I2b: the SAME refusal, read as three subjects, gives three
        distinct answers. Anything less means an operator cannot tell
        which filesystem to go look at."""
        exc = (
            _raise_path_error(path, OSError(number, os.strerror(number), path))
            if structured
            else FilesystemAuthorityError(f"policy refusal for {path}")
        )
        reasons = [mapper(exc) for mapper in _SUBJECT_MAPPERS.values()]
        self.assertEqual(
            len(set(reasons)), len(reasons),
            f"subjects collapsed onto a shared reason: {reasons}",
        )
        # ...and all three still land in the SAME family: the subject
        # decides the nouns, never the containment/storage partition.
        self.assertEqual(len({reason_family(r) for r in reasons}), 1)


# ============================================================================
# Property 2 — real materialize worlds, real filesystem, one reason each
# ============================================================================

_FAILING_WORLDS = (
    "never_stamped",
    "gone_from_disk",
    "symlink",
    "escape",
    "special_fifo",
    "special_socket",
    "unreadable",
    "shared_root_missing",
    "shared_root_unreadable",
)
_WORLD_REASONS = {
    "never_stamped": REASON_EVENT_PATH_NEVER_STAMPED,
    "gone_from_disk": REASON_EVENT_PATH_GONE_FROM_DISK,
    "symlink": REASON_UNSAFE_SOURCE_PATH,
    "escape": REASON_UNSAFE_SOURCE_PATH,
    "special_fifo": REASON_UNSAFE_SOURCE_PATH,
    "special_socket": REASON_UNSAFE_SOURCE_PATH,
    "unreadable": f"{REASON_SOURCE_OPEN_FAILED_PREFIX}EACCES",
    "shared_root_missing": REASON_SLSKD_ROOT_MISSING,
    "shared_root_unreadable": f"{REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX}EACCES",
}
_WORLD_FAMILIES = {
    "never_stamped": "never_stamped",
    "gone_from_disk": "missing",
    "symlink": "containment",
    "escape": "containment",
    "special_fifo": "containment",
    "special_socket": "containment",
    "unreadable": "storage",
    "shared_root_missing": "missing",
    "shared_root_unreadable": "storage",
}


class _World:
    """One generated failure world, materialized onto a real filesystem."""

    def __init__(self, cfg: CratediggerConfig, file: DownloadFile) -> None:
        self.cfg = cfg
        self.file = file
        self.restore_modes: list[str] = []
        self.sockets: list[socket.socket] = []
        # The bytes a refused materialize must not touch, and whether this
        # world has any in the first place. Measured, never assumed: a
        # hardcoded ``source_exists=True`` would make the shared
        # publication proof a tautology.
        self.source_path: str | None = None
        self.source_survives = False

    def source_present(self) -> bool:
        return self.source_path is not None and os.path.lexists(self.source_path)

    def close(self) -> None:
        for path in self.restore_modes:
            os.chmod(path, 0o700)
        for sock in self.sockets:
            sock.close()


def _build_world(parent: str, world: str, leaf: str) -> _World:
    """Build one failure world; the caller must ``close()`` it.

    ``close`` restores any mode-000 path so the tempdir can be torn down,
    and releases bound unix sockets.
    """
    source = os.path.join(parent, "downloads")
    processing = os.path.join(parent, "processing")
    incoming = os.path.join(parent, "Incoming")
    if world != "shared_root_missing":
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
    built = _World(cfg, file)
    stamped = os.path.join(source, f"{leaf}.mp3")

    if world == "never_stamped":
        file.local_path = None
    elif world == "gone_from_disk":
        file.local_path = stamped
        built.source_path = stamped
    elif world == "symlink":
        outside = os.path.join(parent, "outside.mp3")
        with open(outside, "wb") as handle:
            handle.write(b"audio")
        os.symlink(outside, stamped)
        file.local_path = stamped
        # The link is refused; the bytes it points at must be untouched.
        built.source_path = outside
        built.source_survives = True
    elif world == "escape":
        escaped = os.path.join(parent, f"{leaf}-escaped.mp3")
        with open(escaped, "wb") as handle:
            handle.write(b"audio")
        file.local_path = escaped
        built.source_path = escaped
        built.source_survives = True
    elif world == "special_fifo":
        os.mkfifo(stamped)
        file.local_path = stamped
        built.source_path = stamped
        built.source_survives = True
    elif world == "special_socket":
        # A socket fails at ``open`` with ENXIO, BEFORE any descriptor
        # exists for an ``S_ISREG`` check to inspect — the shape that
        # errno-only classification files under the wrong family.
        #
        # A FIXED SHORT NAME on purpose: AF_UNIX ``sun_path`` is ~107
        # bytes, and TMPDIR here comes from XDG_RUNTIME_DIR /
        # CRATEDIGGER_TEST_RAM_ROOT (scripts/test_tmpfs.sh). A generated
        # leaf under a longer root overruns it, and ``bind`` would raise
        # "AF_UNIX path too long" — a hard suite failure, since this repo
        # bans skips.
        stamped = os.path.join(source, "s")
        sock = socket.socket(socket.AF_UNIX)
        built.sockets.append(sock)
        sock.bind(stamped)
        file.local_path = stamped
        built.source_path = stamped
        built.source_survives = True
    elif world == "unreadable":
        with open(stamped, "wb") as handle:
            handle.write(b"audio")
        os.chmod(stamped, 0o000)
        built.restore_modes.append(stamped)
        file.local_path = stamped
        built.source_path = stamped
        built.source_survives = True
    elif world == "shared_root_missing":
        # The whole configured share is gone. Nothing about this is a
        # statement about our private tree OR about one file's stamp.
        file.local_path = stamped
        built.source_path = stamped
    elif world == "shared_root_unreadable":
        with open(stamped, "wb") as handle:
            handle.write(b"audio")
        os.chmod(source, 0o000)
        built.restore_modes.append(source)
        file.local_path = stamped
        built.source_path = stamped
        built.source_survives = True
    else:  # pragma: no cover - the strategy only produces the worlds above
        raise AssertionError(f"unknown world {world!r}")
    return built


_LEAVES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-",
    min_size=1,
    max_size=24,
)


def assert_leg_attribution_invariant(
    *,
    leg: str,
    raised: BaseException,
    reason: str,
) -> None:
    """WHICH LEG refused decides the subject, by exception type alone.

    The share's own open raises the typed refusal; anything beneath it
    stays ordinary. A caller that got this backwards would report "the
    whole mount is unreachable" for one absent track, or blame a single
    event stamp for a dead share.
    """
    typed = isinstance(raised, SharedDownloadRootError)
    if typed != (leg == "root"):
        raise AssertionError(
            f"{leg} leg raised {type(raised).__name__}: attribution is "
            "inverted or absent",
        )
    expected_share = leg == "root"
    is_share_reason = reason.startswith("slskd_root_")
    if is_share_reason != expected_share:
        raise AssertionError(
            f"{leg} leg produced reason {reason!r}, which names the "
            f"{'share' if is_share_reason else 'file'} — wrong subject",
        )


class TestGeneratedSharedRootLegAttribution(unittest.TestCase):
    """D1: a root refusal and a descendant refusal are told apart by TYPE."""

    @given(
        leaf=_LEAVES,
        breakage=st.sampled_from(("absent", "unreadable")),
        leg=st.sampled_from(("root", "descendant")),
    )
    @example(leaf="track", breakage="absent", leg="root")
    @example(leaf="track", breakage="absent", leg="descendant")
    @example(leaf="track", breakage="unreadable", leg="root")
    @settings(deadline=None)
    def test_the_failing_leg_decides_the_vocabulary(
        self, leaf: str, breakage: str, leg: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, "downloads")
            candidate = os.path.join(root, f"{leaf}.mp3")
            broken = root if leg == "root" else candidate
            os.mkdir(root)
            if leg == "descendant" or breakage == "unreadable":
                if leg == "descendant":
                    with open(candidate, "wb") as handle:
                        handle.write(b"audio")
                if breakage == "unreadable":
                    os.chmod(broken, 0o000)
                else:
                    os.unlink(broken)
            elif breakage == "absent":
                os.rmdir(root)

            raised: BaseException | None = None
            try:
                with open_shared_download_root(root) as root_fd:
                    opened = open_regular_under_held_root(root, root_fd, candidate)
                    opened.close()
            except FilesystemAuthorityError as exc:
                raised = exc
            finally:
                if breakage == "unreadable" and os.path.lexists(broken):
                    os.chmod(broken, 0o700)

            self.assertIsNotNone(raised)
            assert raised is not None
            assert isinstance(raised, FilesystemAuthorityError)
            reason = (
                shared_download_root_reason(raised)
                if isinstance(raised, SharedDownloadRootError)
                else source_preflight_reason(raised)
            )
            assert_leg_attribution_invariant(leg=leg, raised=raised, reason=reason)


class TestGeneratedMaterializeFailureReasons(unittest.TestCase):
    """I2/I3 driven through the REAL private materialize publisher."""

    @given(world=st.sampled_from(_FAILING_WORLDS), leaf=_LEAVES)
    @example(world="never_stamped", leaf="track")
    @example(world="gone_from_disk", leaf="track")
    @example(world="unreadable", leaf="track")
    @example(world="special_socket", leaf="track")
    @example(world="shared_root_missing", leaf="track")
    @example(world="shared_root_unreadable", leaf="track")
    @settings(deadline=None)
    def test_each_failure_world_yields_exactly_its_own_reason(
        self, world: str, leaf: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as parent:
            built = _build_world(parent, world, leaf)
            album = make_grab_list_entry(
                files=[built.file], artist="Artist", title="Album", year="2020")
            albums_root = processing_albums_dir(built.cfg.processing_dir)
            canonical = canonical_folder_for_row(album, albums_root)
            try:
                result = _materialize_processing_dir(
                    album,
                    StagedAlbum.from_entry(album, default_path=canonical),
                    make_ctx_with_fake_db(FakePipelineDB(), cfg=built.cfg),
                )
            finally:
                built.close()

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
            # Nothing was published, and no transaction directory leaked:
            # a refused materialize owes the same artifact contract as a
            # successful one, checked by the shared publication proof.
            self.assertFalse(os.path.isdir(canonical))
            assert_publication_invariant(
                result=result,
                source_exists=built.source_present(),
                expected_source_exists=built.source_survives,
                destination_names=(
                    set(os.listdir(canonical))
                    if os.path.isdir(canonical)
                    else set()
                ),
                expected_names=set(),
                artifact_names=os.listdir(albums_root),
                name_max=os.pathconf(albums_root, "PC_NAME_MAX"),
                allowed_result_types=(MaterializeFailed,),
            )

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
            built = _build_world(parent, world, "track")
            album = make_grab_list_entry(
                files=[built.file], artist="Artist", title="Album", year="2020",
                db_request_id=1, mb_release_id="")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=1, status="downloading"))
            ctx = make_ctx_with_fake_db(db, cfg=built.cfg)
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
                built.close()

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

    def test_leg_checker_rejects_a_root_refusal_blamed_on_one_file(self) -> None:
        """The exact D1 defect: the share refused, but the reason names a
        file's event stamp."""
        with self.assertRaises(AssertionError):
            assert_leg_attribution_invariant(
                leg="root",
                raised=SharedDownloadRootError.wrapping(
                    FilesystemAuthorityError("gone", code="missing"),
                ),
                reason=REASON_EVENT_PATH_GONE_FROM_DISK,
            )

    def test_leg_checker_rejects_an_untyped_root_refusal(self) -> None:
        with self.assertRaises(AssertionError):
            assert_leg_attribution_invariant(
                leg="root",
                raised=FilesystemAuthorityError("gone", code="missing"),
                reason=REASON_SLSKD_ROOT_MISSING,
            )

    def test_leg_checker_rejects_one_file_escalated_to_the_share(self) -> None:
        with self.assertRaises(AssertionError):
            assert_leg_attribution_invariant(
                leg="descendant",
                raised=FilesystemAuthorityError("gone", code="missing"),
                reason=REASON_SLSKD_ROOT_MISSING,
            )

    def test_partition_checker_rejects_ignorance_dressed_as_containment(self) -> None:
        """The generated I2b property found this one: a subject that answers
        its containment noun for an UNCLASSIFIED refusal manufactures a
        security finding out of not knowing."""
        for reason in _UNCLASSIFIED_REASONS:
            self.assertEqual(reason_family(reason), "unclassified")
            self.assertNotIn(reason, _CONTAINMENT_REASONS)
        with self.assertRaises(AssertionError):
            assert_reason_partition_invariant(
                reason=REASON_UNSAFE_SOURCE_PATH,
                repeated_reason=REASON_UNSAFE_SOURCE_PATH,
                expected_family="unclassified",
                errno_symbol=None,
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
