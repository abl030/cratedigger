"""Generated patrol for lib.startup_write_probe (issue #1085).

The invariant: for any single broken required path, and for each of three
break shapes (fully denied, missing, or write-only denied), exactly the
units that require it THAT WAY refuse at startup, and no others -- with
``cratedigger-unfindable`` never even asked, because it never calls this
module at all (its own systemd unit exists specifically so the
never-stop-searching invariant is enforceable at the systemd level; a
storage-availability gate there would violate it).

Drives the REAL per-unit ``*_required_paths`` builders and the REAL
``probe_startup_paths`` over a real filesystem -- never a stand-in for
either.

**Scope, stated plainly (issue #1085 review round 2, MUST FIX 5).** The
"expected affected units" ground truth below reads the SAME
``RequiredPaths`` structs the builders under test produce -- it proves
``probe_startup_paths`` is MECHANICALLY faithful to whatever list it is
handed (including, with the write-only-denial world added here, that it
tells read and write requirements apart correctly), not that any builder's
list is COMPLETE. A builder that silently dropped a real required path
would pass this property unchanged, because the property's oracle and the
builder under test are the same function call. That is a real, known,
unclosed gap -- deriving required paths from actual runtime behaviour
(tracing every write production code performs) is a genuinely hard,
separate problem and is explicitly not attempted here. What DOES catch a
copy-paste of the wrong unit's builder is
``tests.test_startup_write_probe.TestEntrypointCallsItsOwnBuilder``, which
patches each real builder function and drives the real entrypoint.
"""

from __future__ import annotations

import itertools
import logging
import os
import tempfile
import unittest
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz tiers
from lib.config import CratediggerConfig
from lib.processing_paths import processing_albums_dir, processing_preview_dir
from lib.startup_write_probe import (
    RequiredPaths,
    StartupProbeError,
    cratedigger_required_paths,
    importer_required_paths,
    preview_worker_required_paths,
    probe_startup_paths,
    web_required_paths,
    youtube_ingest_required_paths,
)
from tests._source_pins import pinned_source
from tests.finite_domain import finite_generated_domain

REPO_ROOT = Path(__file__).resolve().parent.parent
_QUIET = logging.getLogger("test-startup-write-probe-generated")
_QUIET.addHandler(logging.NullHandler())
_QUIET.propagate = False

# ---------------------------------------------------------------------------
# The domain: six independent required-path leaves (none an ancestor of
# another, so breaking exactly one never cascades into a sibling), each
# broken three ways -- "denied" (real EACCES on open/enumerate/create),
# "missing" (never provisioned), and "write_denied" (0500: read/enumerate
# still succeeds, create does not -- the one shape that can tell a
# read-only requirer from a write requirer apart). 6 x 3 = 18, the exact
# Cartesian product, independently reconstructed below.
# ---------------------------------------------------------------------------

LEAF_NAMES: tuple[str, ...] = (
    "var_dir",
    "slskd_download_dir",
    "beets_staging_dir",
    "processing_albums",
    "processing_preview",
    "yt_temp_dir",
)
BREAK_MODES: tuple[str, ...] = ("denied", "missing", "write_denied")

# Every leaf the private processing tree owns must stay exactly 0700 for
# the strict private-root contract; every other leaf is an ordinary
# directory this probe only needs to open/enumerate/write, so an
# umask-shaped default is fine.
_HEALTHY_MODE: dict[str, int] = {
    "var_dir": 0o755,
    "slskd_download_dir": 0o755,
    "beets_staging_dir": 0o755,
    "processing_albums": 0o700,
    "processing_preview": 0o700,
    "yt_temp_dir": 0o755,
}


@dataclass(frozen=True)
class BrokenPathWorld:
    leaf: str
    mode: str


WORLD_COUNT = 18
WORLDS: tuple[BrokenPathWorld, ...] = tuple(
    BrokenPathWorld(leaf=leaf, mode=mode)
    for leaf, mode in itertools.product(LEAF_NAMES, BREAK_MODES)
)


def verify_world_domain() -> None:
    """Independently prove the domain is the exact 6 x 3 product."""
    if len(WORLDS) != WORLD_COUNT:
        raise AssertionError(
            f"broken-path domain must hold {WORLD_COUNT} worlds, "
            f"found {len(WORLDS)}"
        )
    reconstructed = {(world.leaf, world.mode) for world in WORLDS}
    if reconstructed != set(itertools.product(LEAF_NAMES, BREAK_MODES)):
        raise AssertionError("broken-path domain is not the full product")


# ---------------------------------------------------------------------------
# Fixture: one real filesystem tree with all six leaves present, plus the
# real per-unit required-path lists computed the SAME way production does.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ProbeFixture:
    leaf_paths: Mapping[str, str]
    required_by_unit: Mapping[str, RequiredPaths]


def _build_fixture(root: str) -> _ProbeFixture:
    var_dir = os.path.join(root, "var")
    slskd_download_dir = os.path.join(root, "slskd")
    beets_staging_dir = os.path.join(root, "staging")
    processing_dir = os.path.join(root, "processing")
    yt_temp_dir = os.path.join(root, "yt-temp")
    albums = processing_albums_dir(processing_dir)
    preview = processing_preview_dir(processing_dir)
    leaf_paths = {
        "var_dir": var_dir,
        "slskd_download_dir": slskd_download_dir,
        "beets_staging_dir": beets_staging_dir,
        "processing_albums": albums,
        "processing_preview": preview,
        "yt_temp_dir": yt_temp_dir,
    }
    os.makedirs(processing_dir)
    os.chmod(processing_dir, 0o700)
    for leaf, path in leaf_paths.items():
        os.makedirs(path)
        os.chmod(path, _HEALTHY_MODE[leaf])
    cfg = CratediggerConfig(
        var_dir=var_dir,
        slskd_download_dir=slskd_download_dir,
        beets_staging_dir=beets_staging_dir,
        processing_dir=processing_dir,
    )
    required_by_unit = {
        "cratedigger": cratedigger_required_paths(cfg),
        "cratedigger-importer": importer_required_paths(cfg),
        "cratedigger-import-preview-worker": preview_worker_required_paths(cfg),
        "cratedigger-web": web_required_paths(cfg),
        "cratedigger-youtube-ingest": youtube_ingest_required_paths(
            temp_dir=yt_temp_dir, staging_dir=beets_staging_dir),
    }
    return _ProbeFixture(leaf_paths=leaf_paths, required_by_unit=required_by_unit)


@contextmanager
def _broken_leaf(path: str, mode: str, *, healthy_mode: int) -> Generator[None]:
    if mode == "denied":
        os.chmod(path, 0o000)
        try:
            yield
        finally:
            os.chmod(path, healthy_mode)
    elif mode == "write_denied":
        os.chmod(path, 0o500)
        try:
            yield
        finally:
            os.chmod(path, healthy_mode)
    elif mode == "missing":
        os.rmdir(path)
        try:
            yield
        finally:
            os.makedirs(path)
            os.chmod(path, healthy_mode)
    else:
        raise AssertionError(f"unknown break mode: {mode}")


def _run_unit_probe(unit: str, required: RequiredPaths) -> bool:
    """Run the REAL probe; True means the unit refused (StartupProbeError)."""
    try:
        probe_startup_paths(unit=unit, logger=_QUIET, required=required)
    except StartupProbeError:
        return True
    return False


# ---------------------------------------------------------------------------
# The invariant checker.
# ---------------------------------------------------------------------------


def _leaf_requirement(
    required: RequiredPaths, leaf: str, leaf_paths: Mapping[str, str],
) -> tuple[bool, bool]:
    """Return (needs_read, needs_write) for ONE leaf, across every
    mechanism a unit might require it through: the generic read/write
    lists, or -- for ``slskd_download_dir`` specifically -- the private
    tree's own internal open of the shared download root for its
    physical-overlap proof (``open_private_processing_root`` always opens
    and verifies it, independent of which private children are touched)."""
    path = leaf_paths[leaf]
    needs_read = path in required.read
    needs_write = path in required.write
    private_engaged = bool(
        required.private_write_root or required.private_write_children
    )
    if private_engaged and leaf == "slskd_download_dir":
        needs_read = True
    if leaf == "processing_albums" and "albums" in required.private_write_children:
        needs_write = True
    if leaf == "processing_preview" and "preview" in required.private_write_children:
        needs_write = True
    return needs_read, needs_write


def units_that_must_refuse(
    *,
    leaf: str,
    mode: str,
    required_by_unit: Mapping[str, RequiredPaths],
    leaf_paths: Mapping[str, str],
) -> frozenset[str]:
    result: set[str] = set()
    for unit, required in required_by_unit.items():
        needs_read, needs_write = _leaf_requirement(required, leaf, leaf_paths)
        if mode == "write_denied":
            if needs_write:
                result.add(unit)
        else:
            if needs_read or needs_write:
                result.add(unit)
    return frozenset(result)


def assert_exactly_the_requiring_units_refuse(
    *,
    leaf: str,
    mode: str,
    required_by_unit: Mapping[str, RequiredPaths],
    leaf_paths: Mapping[str, str],
    refused_units: frozenset[str],
) -> None:
    expected = units_that_must_refuse(
        leaf=leaf, mode=mode,
        required_by_unit=required_by_unit, leaf_paths=leaf_paths,
    )
    if refused_units != expected:
        raise AssertionError(
            f"broken leaf {leaf!r} (mode={mode}) expected exactly "
            f"{sorted(expected)} to refuse; got {sorted(refused_units)}"
        )
    if "cratedigger-unfindable" in refused_units:
        raise AssertionError(
            "cratedigger-unfindable must never refuse -- it must never even "
            "be asked"
        )


# ---------------------------------------------------------------------------
# The property.
# ---------------------------------------------------------------------------


class TestStartupWriteProbeBrokenPathGenerated(unittest.TestCase):
    """One real fixture, built once and fully restored after every world --
    not a function-scoped Hypothesis fixture, so no health check to
    suppress. Every world starts and ends at the identical clean baseline
    (``_broken_leaf`` always restores its own leaf before returning)."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.fixture = _build_fixture(self._tmp.name)

    @finite_generated_domain(
        cardinality=WORLD_COUNT, verify=verify_world_domain)
    @given(world=st.sampled_from(WORLDS))
    @example(BrokenPathWorld(leaf="var_dir", mode="denied"))
    @example(BrokenPathWorld(leaf="var_dir", mode="missing"))
    @example(BrokenPathWorld(leaf="var_dir", mode="write_denied"))
    @example(BrokenPathWorld(leaf="slskd_download_dir", mode="denied"))
    @example(BrokenPathWorld(leaf="slskd_download_dir", mode="missing"))
    @example(BrokenPathWorld(leaf="slskd_download_dir", mode="write_denied"))
    @example(BrokenPathWorld(leaf="beets_staging_dir", mode="denied"))
    @example(BrokenPathWorld(leaf="beets_staging_dir", mode="missing"))
    # The discriminating case: preview reads beets_staging_dir (quarantine
    # lookup) but never writes it -- write_denied must refuse cratedigger/
    # importer/web and NOT preview.
    @example(BrokenPathWorld(leaf="beets_staging_dir", mode="write_denied"))
    @example(BrokenPathWorld(leaf="processing_albums", mode="denied"))
    @example(BrokenPathWorld(leaf="processing_albums", mode="missing"))
    @example(BrokenPathWorld(leaf="processing_albums", mode="write_denied"))
    @example(BrokenPathWorld(leaf="processing_preview", mode="denied"))
    @example(BrokenPathWorld(leaf="processing_preview", mode="missing"))
    @example(BrokenPathWorld(leaf="processing_preview", mode="write_denied"))
    @example(BrokenPathWorld(leaf="yt_temp_dir", mode="denied"))
    @example(BrokenPathWorld(leaf="yt_temp_dir", mode="missing"))
    @example(BrokenPathWorld(leaf="yt_temp_dir", mode="write_denied"))
    def test_exactly_the_requiring_units_refuse(
        self, world: BrokenPathWorld,
    ) -> None:
        fixture = self.fixture
        broken_path = fixture.leaf_paths[world.leaf]
        with _broken_leaf(
            broken_path, world.mode, healthy_mode=_HEALTHY_MODE[world.leaf],
        ):
            refused = frozenset(
                unit for unit, required in fixture.required_by_unit.items()
                if _run_unit_probe(unit, required)
            )
        assert_exactly_the_requiring_units_refuse(
            leaf=world.leaf,
            mode=world.mode,
            required_by_unit=fixture.required_by_unit,
            leaf_paths=fixture.leaf_paths,
            refused_units=refused,
        )
        # And after restoring the leaf, every unit's probe passes clean.
        for unit, required in fixture.required_by_unit.items():
            self.assertFalse(
                _run_unit_probe(unit, required),
                f"{unit} still refuses after {world.leaf} was restored",
            )

    def test_known_bad_checker_trips_on_a_missed_refusal(self) -> None:
        """The exact live #1063-shaped defect: a unit that requires the
        broken path but was reported as having passed cleanly."""
        leaf_paths = {"var_dir": "/var/lib/cratedigger"}
        with self.assertRaises(AssertionError):
            assert_exactly_the_requiring_units_refuse(
                leaf="var_dir",
                mode="denied",
                required_by_unit={
                    "cratedigger": RequiredPaths(
                        write=("/var/lib/cratedigger",)),
                },
                leaf_paths=leaf_paths,
                refused_units=frozenset(),
            )

    def test_known_bad_checker_trips_on_an_unrelated_refusal(self) -> None:
        """The other direction: a unit refuses a path it never required."""
        leaf_paths = {"var_dir": "/var/lib/cratedigger"}
        with self.assertRaises(AssertionError):
            assert_exactly_the_requiring_units_refuse(
                leaf="var_dir",
                mode="denied",
                required_by_unit={
                    "cratedigger-web": RequiredPaths(read=("/elsewhere",)),
                },
                leaf_paths=leaf_paths,
                refused_units=frozenset({"cratedigger-web"}),
            )

    def test_known_bad_checker_trips_if_unfindable_ever_refuses(self) -> None:
        leaf_paths = {"var_dir": "/var/lib/cratedigger"}
        with self.assertRaises(AssertionError):
            assert_exactly_the_requiring_units_refuse(
                leaf="var_dir",
                mode="denied",
                required_by_unit={
                    "cratedigger-unfindable": RequiredPaths(
                        write=("/var/lib/cratedigger",)),
                },
                leaf_paths=leaf_paths,
                refused_units=frozenset({"cratedigger-unfindable"}),
            )

    def test_known_bad_checker_trips_on_a_read_only_unit_over_write_denial(
        self,
    ) -> None:
        """The read/write-split discriminator itself: a unit that only
        READS the leaf must not be expected to refuse a write-only
        denial."""
        leaf_paths = {"beets_staging_dir": "/staging"}
        with self.assertRaises(AssertionError):
            assert_exactly_the_requiring_units_refuse(
                leaf="beets_staging_dir",
                mode="write_denied",
                required_by_unit={
                    "cratedigger-import-preview-worker": RequiredPaths(
                        read=("/staging",)),
                },
                leaf_paths=leaf_paths,
                refused_units=frozenset({"cratedigger-import-preview-worker"}),
            )


# ---------------------------------------------------------------------------
# cratedigger-unfindable: a bounded, deterministic source check that no
# wiring exists at all -- not a generated property (nothing to vary; the
# claim is "this file never calls the probe module"), and not a general
# semantic scanner (one literal substring, one named file).
# ---------------------------------------------------------------------------


class TestUnfindableNeverWired(unittest.TestCase):
    def test_run_unfindable_detection_never_imports_the_probe(self) -> None:
        source = pinned_source(REPO_ROOT / "scripts" / "run_unfindable_detection.py")
        self.assertNotIn("startup_write_probe", source)
        self.assertNotIn("probe_startup_paths", source)


# ---------------------------------------------------------------------------
# Known-bad self-test: a probe built on os.access() must be killed by a
# world where a real descriptor operation and os.access() disagree.
# ---------------------------------------------------------------------------


class TestOsAccessWouldBeFooled(unittest.TestCase):
    def test_a_required_path_replaced_by_a_symlink_fools_os_access(
        self,
    ) -> None:
        """The #1063-shaped world, one layer up: a required path swapped
        for a symlink. ``os.access()`` follows it and says "writable";
        the real no-follow probe this module uses refuses it outright.
        """
        with tempfile.TemporaryDirectory() as root:
            real_target = os.path.join(root, "real")
            os.makedirs(real_target)
            required_path = os.path.join(root, "required")
            os.symlink(real_target, required_path)

            # A probe implemented with os.access() would say "go ahead".
            self.assertTrue(os.access(required_path, os.W_OK))

            # The real probe this module ships refuses it: every path
            # component, including the leaf, is opened O_NOFOLLOW. A
            # symlink-to-a-directory answers ENOTDIR rather than ELOOP
            # under O_DIRECTORY|O_NOFOLLOW (lib.fs_authority.classify_path_errno's
            # own documented kernel behaviour) -- "not_a_directory", not
            # "unsafe_symlink" -- but it is refused either way, which is
            # the whole point: os.access() cannot tell the difference and
            # says yes; the real descriptor open never gets fooled.
            with self.assertRaises(StartupProbeError) as caught:
                probe_startup_paths(
                    unit="x", logger=_QUIET,
                    required=RequiredPaths(write=(required_path,)))
            self.assertIn("not_a_directory", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
