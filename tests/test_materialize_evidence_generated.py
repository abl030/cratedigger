"""Generated companion for issue #868's materialize-failure evidence.

The deterministic pins live in ``tests/test_download.py``
(``TestEventPathMaterialization``, ``TestPollActiveDownloads``),
``tests/test_path_authority.py`` (``TestAuthorityFailureClassification``)
and ``tests/test_pipeline_db.py``. This module patrols the world space
around them.

Invariants under patrol
-----------------------

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

The properties drive REAL production functions
(``lib.fs_authority._raise_path_error``,
``lib.download_materialization._materialize_processing_dir`` /
``source_preflight_reason`` / ``materialize_authority_reason``) against
a real temporary filesystem and a real ``FakePipelineDB``. Nothing this
repo owns is mocked: the writer, the guard and the persister all run for
real. Request lifecycle ownership is tested at the processor boundary;
the downloader no longer materializes or applies a grace-window reset.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

from __future__ import annotations

import errno
import os
import re
import socket
import tempfile
import unittest
import unittest.mock

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.config import CratediggerConfig
from lib.download_materialization import (
    REASON_EVENT_PATH_GONE_FROM_DISK,
    REASON_EVENT_PATH_NEVER_STAMPED,
    REASON_MATERIALIZE_AUTHORITY_FAILED,
    REASON_PRIVATE_MATERIALIZE_FAILED,
    REASON_PROCESSING_AUTHORITY_UNSAFE,
    REASON_PROCESSING_OPEN_FAILED_PREFIX,
    REASON_PROCESSING_PATH_MISSING,
    REASON_PROCESSING_WRITE_FAILED_PREFIX,
    REASON_SLSKD_ROOT_MISSING,
    REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX,
    REASON_SLSKD_ROOT_REFUSED,
    REASON_SLSKD_ROOT_UNSAFE,
    REASON_SOURCE_OPEN_FAILED_PREFIX,
    REASON_SOURCE_PREFLIGHT_REFUSED,
    REASON_SOURCE_READ_FAILED_PREFIX,
    REASON_UNSAFE_SOURCE_PATH,
    Materialized,
    MaterializeFailed,
    _fsync_private_directory,
    _materialize_processing_dir,
    materialize_authority_reason,
    shared_download_root_reason,
    source_preflight_reason,
)
from lib.fs_authority import (
    CopyDestinationWriteError,
    FilesystemAuthorityError,
    SharedDownloadRootError,
    _raise_path_error,
    open_regular_under_held_root,
    open_shared_download_root,
)
from lib.grab_list import DownloadFile
from lib.processing_paths import (
    canonical_folder_for_row,
    processing_albums_dir,
)
from lib.staged_album import StagedAlbum
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_ctx_with_fake_db,
    make_grab_list_entry,
)
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
    REASON_PRIVATE_MATERIALIZE_FAILED,
    REASON_PROCESSING_WRITE_FAILED_PREFIX,
    REASON_SOURCE_READ_FAILED_PREFIX,
    REASON_SLSKD_ROOT_REFUSED,
})
# Restated, not imported: a checker that groups by the same object
# production groups by would only echo the implementation back.
_CONTAINMENT_CODES = frozenset({
    "path_escape", "unsafe_symlink", "not_a_directory", "not_regular_file",
    # An ownership/permission downgrade of the tree we hold: the guarantee
    # the boundary rests on no longer holds (issue #868 review A8).
    "untrusted_ownership",
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

    if world == "healthy":
        # The only world whose copy phase actually runs — where 100% of the
        # share's bytes are read, and where a flaky mount is most likely to
        # fire (issue #868 review A1).
        with open(stamped, "wb") as handle:
            handle.write(b"audio")
        file.local_path = stamped
        built.source_path = stamped
    elif world == "never_stamped":
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
                with open_shared_download_root(root) as held_root:
                    opened = open_regular_under_held_root(held_root, candidate)
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

            # Nothing was published, and no transaction directory leaked:
            # a refused materialize owes the same artifact contract as a
            # successful one, checked by the shared publication proof.
            #
            # It runs FIRST on purpose. Clause ordering masks ACROSS
            # checkers, not just within one (issue #1094): a preceding
            # ``assertIsInstance`` answered for this checker's result-type
            # clause on every world, and a preceding
            # ``assertFalse(isdir(canonical))`` answered for its
            # destination-manifest clause, so neither could ever be
            # attributed to the checker that legislates it.
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
        are the same containment violation.

        Both reasons are PRODUCED by ``_materialize_processing_dir`` over a
        real filesystem. The earlier version of this test compared two
        test-local dictionaries, `del`'d its generated input and invoked no
        production symbol at all — it survived every mutant a reviewer
        planted, including deleting the containment check outright, while
        reading as coverage (issue #868 review).
        """
        produced = tuple(
            self._produced_reason(name, leaf) for name in (world, other)
        )
        same_reason = produced[0] == produced[1]
        both_containment = (
            _WORLD_FAMILIES[world] == "containment"
            and _WORLD_FAMILIES[other] == "containment"
        )
        if world != other:
            self.assertEqual(
                same_reason, both_containment,
                f"{world}->{produced[0]} vs {other}->{produced[1]}",
            )
        else:
            self.assertTrue(same_reason)

    def _produced_reason(self, world: str, leaf: str) -> str:
        """Materialize one world for real and return the reason it records."""
        with tempfile.TemporaryDirectory() as parent:
            built = _build_world(parent, world, leaf)
            album = make_grab_list_entry(
                files=[built.file], artist="Artist", title="Album", year="2020")
            canonical = canonical_folder_for_row(
                album, processing_albums_dir(built.cfg.processing_dir))
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
        return result.reason


# ============================================================================
# Property 3 — the lifecycle is decided by the tag, never by the reason (I5)
# ============================================================================


# Per-clause proof (issue #1094, docs/generated-testing.md § "Per-clause
# proof"). ``assert_reason_partition_invariant`` short-circuits through
# SEVEN clauses and ``assert_leg_attribution_invariant`` through two; a
# bare ``assertRaises(AssertionError)`` cannot tell which one answered, so
# a world that violates three of them advertises coverage for all three
# while only ever exercising the first. Each clause below gets the minimal
# world that makes ITS condition true while every EARLIER clause passes,
# and asserts that clause's own message anchored end to end.


def _exactly(message: str) -> str:
    """Anchor one clause's complete message for ``assertRaisesRegex``."""
    return f"^{re.escape(message)}$"


class TestMaterializeEvidenceCheckersTripOnViolations(unittest.TestCase):
    def test_every_partition_clause_fires_on_its_own_world(self) -> None:
        storage_reason = f"{REASON_SOURCE_OPEN_FAILED_PREFIX}ESTALE"
        cases = (
            (
                "1: the reason was not stable across two calls",
                {"reason": REASON_UNSAFE_SOURCE_PATH,
                 "repeated_reason": REASON_EVENT_PATH_GONE_FROM_DISK,
                 "expected_family": "containment", "errno_symbol": None},
                "reason was not stable across calls: 'unsafe_source_path' vs "
                "'event_path_gone_from_disk'",
            ),
            (
                "2: a colon the retired split derivation would truncate",
                {"reason": "unsafe_source_path: /peer/track.mp3",
                 "repeated_reason": "unsafe_source_path: /peer/track.mp3",
                 "expected_family": "containment", "errno_symbol": None},
                "reason 'unsafe_source_path: /peer/track.mp3' contains a "
                "colon — the retired ``str(exc).split(':', 1)[0]`` "
                "derivation would truncate it",
            ),
            (
                "3: a reason belonging to no known family",
                {"reason": "something_new", "repeated_reason": "something_new",
                 "expected_family": "containment", "errno_symbol": None},
                "reason 'something_new' belongs to no known family",
            ),
            (
                "4: a storage errno filed under the containment noun",
                {"reason": REASON_UNSAFE_SOURCE_PATH,
                 "repeated_reason": REASON_UNSAFE_SOURCE_PATH,
                 "expected_family": "storage", "errno_symbol": "ESTALE"},
                "reason 'unsafe_source_path' is family 'containment', "
                "expected 'storage'",
            ),
            (
                "4: the mirror image — a containment called storage",
                {"reason": storage_reason, "repeated_reason": storage_reason,
                 "expected_family": "containment", "errno_symbol": "ESTALE"},
                "reason 'source_open_failed_ESTALE' is family 'storage', "
                "expected 'containment'",
            ),
            (
                # The generated I2b property found this one: a subject that
                # answers its containment noun for an UNCLASSIFIED refusal
                # manufactures a security finding out of not knowing.
                "4: ignorance dressed as a containment finding",
                {"reason": REASON_UNSAFE_SOURCE_PATH,
                 "repeated_reason": REASON_UNSAFE_SOURCE_PATH,
                 "expected_family": "unclassified", "errno_symbol": None},
                "reason 'unsafe_source_path' is family 'containment', "
                "expected 'unclassified'",
            ),
            (
                # Clause 5 is reachable in production: an ``errno_symbol``
                # helper that answers None still yields a ``*_UNKNOWN``
                # storage reason, and the caller then has no errno to pass.
                "5: a storage reason carrying no errno symbol at all",
                {"reason": storage_reason, "repeated_reason": storage_reason,
                 "expected_family": "storage", "errno_symbol": None},
                "a storage reason must carry an errno symbol",
            ),
            (
                "6: a storage reason that lost its errno on the way out",
                {"reason": f"{REASON_SOURCE_OPEN_FAILED_PREFIX}UNKNOWN",
                 "repeated_reason":
                     f"{REASON_SOURCE_OPEN_FAILED_PREFIX}UNKNOWN",
                 "expected_family": "storage", "errno_symbol": "ESTALE"},
                "storage reason 'source_open_failed_UNKNOWN' lost its errno "
                "'ESTALE'",
            ),
            (
                # Clause 7 is the converse of 5: a containment verdict is
                # never built from an errno, so a classifier that started
                # attaching one to every refusal is caught here.
                "7: a non-storage reason built from an errno",
                {"reason": REASON_UNSAFE_SOURCE_PATH,
                 "repeated_reason": REASON_UNSAFE_SOURCE_PATH,
                 "expected_family": "containment", "errno_symbol": "ESTALE"},
                "non-storage reason 'unsafe_source_path' was built from "
                "errno 'ESTALE'",
            ),
        )
        for clause, kwargs, message in cases:
            with self.subTest(clause=clause):
                with self.assertRaisesRegex(AssertionError, _exactly(message)):
                    assert_reason_partition_invariant(**kwargs)

    def test_a_correctly_named_refusal_passes_every_partition_clause(self) -> None:
        """The must-still-work control for all seven clauses at once."""
        for reason, family, symbol in (
            (REASON_UNSAFE_SOURCE_PATH, "containment", None),
            (REASON_EVENT_PATH_GONE_FROM_DISK, "missing", None),
            (f"{REASON_SOURCE_OPEN_FAILED_PREFIX}ESTALE", "storage", "ESTALE"),
            (REASON_SLSKD_ROOT_REFUSED, "unclassified", None),
            (REASON_EVENT_PATH_NEVER_STAMPED, "never_stamped", None),
        ):
            with self.subTest(reason=reason):
                assert_reason_partition_invariant(
                    reason=reason, repeated_reason=reason,
                    expected_family=family, errno_symbol=symbol,
                )

    def test_unclassified_reasons_are_their_own_family(self) -> None:
        """No subject's "we could not classify this" noun may be read as a
        containment finding."""
        for reason in _UNCLASSIFIED_REASONS:
            self.assertEqual(reason_family(reason), "unclassified")
            self.assertNotIn(reason, _CONTAINMENT_REASONS)
        self.assertEqual(reason_family("something_new"), "other")

    def test_every_leg_attribution_clause_fires_on_its_own_world(self) -> None:
        typed = SharedDownloadRootError.wrapping(
            FilesystemAuthorityError("gone", code="missing"),
        )
        untyped = FilesystemAuthorityError("gone", code="missing")
        cases = (
            (
                "1: the share refused but arrived untyped",
                {"leg": "root", "raised": untyped,
                 "reason": REASON_SLSKD_ROOT_MISSING},
                "root leg raised FilesystemAuthorityError: attribution is "
                "inverted or absent",
            ),
            (
                "1: the inverse — one file's refusal typed as the share's",
                {"leg": "descendant", "raised": typed,
                 "reason": REASON_SLSKD_ROOT_MISSING},
                "descendant leg raised SharedDownloadRootError: attribution "
                "is inverted or absent",
            ),
            (
                # The exact D1 defect: the share refused, but the reason
                # names one file's event stamp.
                "2: a root refusal blamed on one file's event stamp",
                {"leg": "root", "raised": typed,
                 "reason": REASON_EVENT_PATH_GONE_FROM_DISK},
                "root leg produced reason 'event_path_gone_from_disk', which "
                "names the file — wrong subject",
            ),
            (
                "2: one file's refusal escalated to the whole share",
                {"leg": "descendant", "raised": untyped,
                 "reason": REASON_SLSKD_ROOT_MISSING},
                "descendant leg produced reason 'slskd_root_missing', which "
                "names the share — wrong subject",
            ),
        )
        for clause, kwargs, message in cases:
            with self.subTest(clause=clause):
                with self.assertRaisesRegex(AssertionError, _exactly(message)):
                    assert_leg_attribution_invariant(**kwargs)

    def test_a_correctly_attributed_leg_passes_every_clause(self) -> None:
        """The must-still-work control: both legs, named right."""
        assert_leg_attribution_invariant(
            leg="root",
            raised=SharedDownloadRootError.wrapping(
                FilesystemAuthorityError("gone", code="missing"),
            ),
            reason=REASON_SLSKD_ROOT_MISSING,
        )
        assert_leg_attribution_invariant(
            leg="descendant",
            raised=FilesystemAuthorityError("gone", code="missing"),
            reason=REASON_EVENT_PATH_GONE_FROM_DISK,
        )


# ============================================================================
# Property 4 — the copy phase names its own subject (issue #868 review A1)
# ============================================================================
#
# ``copy_opened_file`` reads the shared slskd share and writes our private
# tree. Both failures used to land in ``_materialize_processing_dir``'s
# generic ``except OSError`` arm as ``private_materialize_failed`` with the
# errno discarded — so an ESTALE on the SHARE (the convicted live nested-
# virtiofs shape) was recorded as a fault of OUR OWN tree, in the one phase
# that reads every byte the share has.

_COPY_ERRNOS = ("ESTALE", "EIO", "ENOSPC", "EACCES", "EBADF")


def check_copy_failure_names_its_subject(
    subject: str,
    errno_name: str,
    reason: str,
) -> str | None:
    """Return why a copy-phase reason is wrong, or None when it is right.

    Module-level so the known-bad self-test can hand it the exact opaque
    string that shipped.
    """
    # Read and write are different facts from open: a destination that ran
    # out of space opened perfectly well, and ``processing_open_failed_``
    # rendered "could not be opened" for every ENOSPC (issue #868 review
    # B2). The subject AND the verb have to be right.
    expected_prefix = (
        REASON_SOURCE_READ_FAILED_PREFIX if subject == "source"
        else REASON_PROCESSING_WRITE_FAILED_PREFIX
    )
    wrong_prefixes = (
        (REASON_PROCESSING_WRITE_FAILED_PREFIX,
         REASON_PROCESSING_OPEN_FAILED_PREFIX,
         REASON_SOURCE_OPEN_FAILED_PREFIX)
        if subject == "source"
        else (REASON_SOURCE_READ_FAILED_PREFIX,
              REASON_SOURCE_OPEN_FAILED_PREFIX,
              REASON_PROCESSING_OPEN_FAILED_PREFIX)
    )
    if reason == REASON_PRIVATE_MATERIALIZE_FAILED:
        return f"{subject} failure collapsed into {reason!r} with no errno"
    for wrong in wrong_prefixes:
        if reason.startswith(wrong):
            return (
                f"{subject} failure named the wrong subject or verb: {reason!r}"
            )
    if not reason.startswith(expected_prefix):
        return f"{subject} failure did not name its subject: {reason!r}"
    if not reason.endswith(errno_name):
        return f"{subject} failure lost its errno: {reason!r} (want {errno_name})"
    return None


class TestGeneratedCopyPhaseSubjects(unittest.TestCase):

    def _reason_for(self, subject: str, errno_name: str) -> str:
        """Materialize a healthy world with ONE syscall failing for real.

        ``os.read`` / ``os.write`` are the syscalls ``copy_opened_file``
        forwards to — the outermost external edge, and the only reader and
        writer inside this call. Patching them injects the exact failure a
        sick mount produces without faking any of our own logic.
        """
        code = getattr(errno, errno_name)

        def failing(*_args: object, **_kwargs: object) -> int:
            raise OSError(code, os.strerror(code))

        with tempfile.TemporaryDirectory() as parent:
            built = _build_world(parent, "healthy", "track")
            album = make_grab_list_entry(
                files=[built.file], artist="Artist", title="Album", year="2020")
            canonical = canonical_folder_for_row(
                album, processing_albums_dir(built.cfg.processing_dir))
            staged = StagedAlbum.from_entry(album, default_path=canonical)
            ctx = make_ctx_with_fake_db(FakePipelineDB(), cfg=built.cfg)
            target = "os.read" if subject == "source" else "os.write"
            try:
                with unittest.mock.patch(target, side_effect=failing):
                    result = _materialize_processing_dir(album, staged, ctx)
            finally:
                built.close()
            self.assertFalse(os.path.isdir(canonical))
        self.assertIsInstance(result, MaterializeFailed)
        assert isinstance(result, MaterializeFailed)
        return result.reason

    @given(
        subject=st.sampled_from(("source", "destination")),
        errno_name=st.sampled_from(_COPY_ERRNOS),
    )
    @example(subject="source", errno_name="ESTALE")
    @example(subject="source", errno_name="EIO")
    @example(subject="destination", errno_name="ENOSPC")
    @settings(deadline=None, max_examples=10)
    def test_a_mid_copy_failure_names_its_subject_and_errno(
        self, subject: str, errno_name: str,
    ) -> None:
        reason = self._reason_for(subject, errno_name)
        violation = check_copy_failure_names_its_subject(
            subject, errno_name, reason)
        self.assertIsNone(violation, violation)

    def test_the_live_share_shapes_are_pinned_exactly(self) -> None:
        """The three failures a reviewer injected at the real copy boundary."""
        self.assertEqual(
            self._reason_for("source", "ESTALE"),
            f"{REASON_SOURCE_READ_FAILED_PREFIX}ESTALE",
        )
        self.assertEqual(
            self._reason_for("source", "EIO"),
            f"{REASON_SOURCE_READ_FAILED_PREFIX}EIO",
        )
        self.assertEqual(
            self._reason_for("destination", "ENOSPC"),
            f"{REASON_PROCESSING_WRITE_FAILED_PREFIX}ENOSPC",
        )

    def test_a_failing_directory_flush_answers_the_same_vocabulary(self) -> None:
        """Review B2: the transaction directory's own flush is the same
        physical fault one line after the copy's, and used to answer
        ``private_materialize_failed`` — one mount failure, two
        vocabularies.

        Driven at the helper rather than through a whole materialize:
        patching ``os.fsync`` globally also breaks the transaction
        rollback's own flush, whose failure then replaces this one (the
        pre-existing leak noted as review #14 and deliberately not fixed
        here). The helper IS the production raise site, and the mapping it
        feeds is the production mapper.
        """
        read_fd, write_fd = os.pipe()
        try:
            with self.assertRaises(CopyDestinationWriteError) as caught:
                _fsync_private_directory(write_fd, "transaction directory")
        finally:
            os.close(read_fd)
            os.close(write_fd)
        self.assertEqual(caught.exception.code, "write_failed")
        self.assertEqual(
            materialize_authority_reason(caught.exception),
            f"{REASON_PROCESSING_WRITE_FAILED_PREFIX}"
            f"{caught.exception.errno_symbol}",
        )

    def test_a_healthy_world_still_publishes(self) -> None:
        """The must-still-work guard: the new handlers are not fail-closed."""
        with tempfile.TemporaryDirectory() as parent:
            built = _build_world(parent, "healthy", "track")
            album = make_grab_list_entry(
                files=[built.file], artist="Artist", title="Album", year="2020")
            canonical = canonical_folder_for_row(
                album, processing_albums_dir(built.cfg.processing_dir))
            try:
                result = _materialize_processing_dir(
                    album,
                    StagedAlbum.from_entry(album, default_path=canonical),
                    make_ctx_with_fake_db(FakePipelineDB(), cfg=built.cfg),
                )
                self.assertIsInstance(result, Materialized)
                self.assertEqual(os.listdir(canonical), ["track.mp3"])
            finally:
                built.close()

    def test_every_copy_subject_clause_returns_its_own_violation(self) -> None:
        """``check_copy_failure_names_its_subject`` accumulates nothing — it
        returns the FIRST violation it finds, so each of its four clauses
        owes a world where the earlier three are satisfied, and the exact
        string it returns is the whole diagnosis."""
        cases = (
            (
                "1: the defect that shipped — collapsed with no errno",
                "source", "ESTALE", REASON_PRIVATE_MATERIALIZE_FAILED,
                "source failure collapsed into 'private_materialize_failed' "
                "with no errno",
            ),
            (
                "1: the same collapse on the destination side",
                "destination", "ENOSPC", REASON_PRIVATE_MATERIALIZE_FAILED,
                "destination failure collapsed into "
                "'private_materialize_failed' with no errno",
            ),
            (
                "2: our own tree blamed for the share's read failure",
                "source", "ESTALE",
                f"{REASON_PROCESSING_WRITE_FAILED_PREFIX}ESTALE",
                "source failure named the wrong subject or verb: "
                "'processing_write_failed_ESTALE'",
            ),
            (
                # The verb matters as much as the subject: an open failure
                # is not what a mid-copy read did.
                "2: right subject, wrong verb (open, not read)",
                "source", "ESTALE",
                f"{REASON_SOURCE_OPEN_FAILED_PREFIX}ESTALE",
                "source failure named the wrong subject or verb: "
                "'source_open_failed_ESTALE'",
            ),
            (
                "2: right subject, wrong verb on the destination side",
                "destination", "ENOSPC",
                f"{REASON_PROCESSING_OPEN_FAILED_PREFIX}ENOSPC",
                "destination failure named the wrong subject or verb: "
                "'processing_open_failed_ENOSPC'",
            ),
            (
                # Clause 3 needs a reason that is neither the collapsed
                # string nor one of the three named-wrong prefixes: a THIRD
                # subject's vocabulary. Reached in production by a copy
                # handler that reaches for the shared-root mapper.
                "3: a third subject's noun entirely",
                "source", "ESTALE", REASON_SLSKD_ROOT_MISSING,
                "source failure did not name its subject: "
                "'slskd_root_missing'",
            ),
            (
                "3: the same third subject on the destination side",
                "destination", "ENOSPC", REASON_SLSKD_ROOT_MISSING,
                "destination failure did not name its subject: "
                "'slskd_root_missing'",
            ),
            (
                "4: right subject and verb, wrong errno",
                "source", "ESTALE", f"{REASON_SOURCE_READ_FAILED_PREFIX}EIO",
                "source failure lost its errno: 'source_read_failed_EIO' "
                "(want ESTALE)",
            ),
        )
        for clause, subject, errno_name, reason, message in cases:
            with self.subTest(clause=clause):
                self.assertEqual(
                    check_copy_failure_names_its_subject(
                        subject, errno_name, reason),
                    message,
                )

    def test_a_correctly_named_copy_failure_passes_every_clause(self) -> None:
        """The must-still-work control for all four clauses."""
        self.assertIsNone(check_copy_failure_names_its_subject(
            "source", "ESTALE",
            f"{REASON_SOURCE_READ_FAILED_PREFIX}ESTALE"))
        self.assertIsNone(check_copy_failure_names_its_subject(
            "destination", "ENOSPC",
            f"{REASON_PROCESSING_WRITE_FAILED_PREFIX}ENOSPC"))


if __name__ == "__main__":
    unittest.main()
