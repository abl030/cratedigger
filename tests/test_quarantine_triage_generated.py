"""Generated lifecycle invariant for unreferenced quarantine discovery."""

from __future__ import annotations

import os
import tempfile
import unittest

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.quarantine_triage_service import (
    QuarantineFolder,
    QuarantineTriageResult,
    list_unreferenced_quarantine_folders,
)
from tests.fakes import FakePipelineDB

REFERENCE_KINDS = (
    "none",
    "relative",
    "absolute",
    "relative_descendant",
    "absolute_descendant",
    "outside",
)
QUARANTINE_KINDS = (
    "failed_imports",
    "wrong_matches",
    "processing_failed_imports",
    "processing_wrong_matches",
)
# Kinds that get the code-owned bad_files/untracked_audio exclusion
# (issue #1122 F3: a live processing-side failed_imports/bad_files/ entry
# proved the bucket applies to BOTH failed_imports roots, not just the
# download-dir-rooted one).
SPECIAL_BUCKET_KINDS = ("failed_imports", "processing_failed_imports")
REQUEST_STATUSES = (
    "wanted",
    "downloading",
    "unsearchable",
    "imported",
    "replaced",
)


def assert_quarantine_listing_invariant(
    result: QuarantineTriageResult,
    *,
    expected_paths: set[str],
) -> None:
    """Assert exact, deterministic, immediate-folder discovery.

    Each clause carries its own message (issue #1122 F7) so a self-test can
    prove which clause tripped with ``assertRaisesRegex`` rather than a bare
    ``assertRaises(AssertionError)`` that cannot distinguish them.
    """
    actual_paths = [folder.path for folder in result.folders]
    assert actual_paths == sorted(
        expected_paths, key=lambda path: os.path.basename(path),
    ), "listed folders do not match the expected orphan set"
    assert len(actual_paths) == len(set(actual_paths)), \
        "a folder path was listed more than once"
    quarantine_roots = {
        result.quarantine_root,
        result.wrong_matches_root,
        result.processing_failed_imports_root,
        result.processing_wrong_matches_root,
    }
    # Every root that gets the code-owned special-bucket exclusion — both
    # failed_imports roots, download-dir-rooted and processing-rooted alike
    # (issue #1122 F3). The wrong_matches roots carry no such buckets.
    special_bucket_roots = {
        result.quarantine_root,
        result.processing_failed_imports_root,
    }
    for folder in result.folders:
        assert os.path.dirname(folder.path) in quarantine_roots, \
            f"folder {folder.path!r} does not belong to any known quarantine root"
        assert folder.name == os.path.basename(folder.path), \
            f"folder name {folder.name!r} does not match its own path {folder.path!r}"
        if os.path.dirname(folder.path) in special_bucket_roots:
            assert folder.name not in result.special_buckets, \
                f"special bucket {folder.name!r} was listed as an orphan folder"


def _seed_reference(
    db: FakePipelineDB,
    failed_path: str,
    index: int,
    *,
    request_status: str,
) -> None:
    request_id = db.add_request(
        artist_name=f"Artist {index}",
        album_title=f"Album {index}",
        source="request",
        status=request_status,
    )
    db.log_download(
        request_id,
        outcome="rejected",
        validation_result={
            "failed_path": failed_path,
            "scenario": "high_distance",
        },
    )


def _selected_root_for_kind(kind: str, root: str, proc_root: str) -> str:
    """Physical root directory for one of the configured quarantine kinds.

    The two download-dir-rooted kinds are simple children of the
    configured slskd download dir; the two processing-side kinds are
    nested two levels under a wholly separate ``processing_dir`` tree
    (``processing_albums_dir(processing_dir)`` + the marker name).
    """
    if kind.startswith("processing_"):
        marker = kind.removeprefix("processing_")
        return os.path.join(proc_root, "albums", marker)
    return os.path.join(root, kind)


def _relative_reference_reaches_kind(kind: str, root: str, proc_root: str) -> bool:
    """Would a RELATIVE ``failed_path`` aimed at ``kind`` actually protect it?

    A relative reference resolves by joining the single global slskd
    download dir (the production ``_visible_wrong_match_roots`` contract)
    — so it only ever protects a kind whose real root actually sits at
    ``<download_dir>/<kind>``. Both processing-side kinds live under an
    entirely different tree and can never be reached this way: a relative
    reference "aimed" at one of them does not test some special
    processing-specific resolution rule — it is just another way to build a
    reference that resolves outside every known quarantine root, the exact
    same world the explicit ``outside`` reference kind builds via a
    separate unrelated temp directory. This function returns ``False`` for
    those kinds precisely so the caller adds the folder to ``expected``
    (still unreferenced), not because "relative" carries any evidentiary
    weight for the processing side.
    """
    return os.path.normpath(os.path.join(root, kind)) == os.path.normpath(
        _selected_root_for_kind(kind, root, proc_root),
    )


def _fixture_result(folders: list[QuarantineFolder]) -> QuarantineTriageResult:
    """A stable four-root ``QuarantineTriageResult`` for checker self-tests
    (issue #1122 F7) — the specific folder set is each test's own concern.
    """
    return QuarantineTriageResult(
        quarantine_root="/downloads/failed_imports",
        wrong_matches_root="/downloads/wrong_matches",
        processing_failed_imports_root="/processing/albums/failed_imports",
        processing_wrong_matches_root="/processing/albums/wrong_matches",
        folders=folders,
        special_buckets=["bad_files", "untracked_audio"],
    )


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Per-clause proof (code-quality.md "Per-clause proof"): a minimal
    world that trips EXACTLY one clause, with every earlier clause passing,
    asserted by that clause's own message via ``assertRaisesRegex`` — never
    a bare ``assertRaises(AssertionError)``, which cannot distinguish which
    clause actually fired.
    """

    def test_listing_checker_rejects_a_referenced_or_unexpected_folder(self) -> None:
        bad = _fixture_result([QuarantineFolder(
            name="Referenced",
            path="/downloads/failed_imports/Referenced",
            mtime_ns=1,
        )])
        with self.assertRaisesRegex(
            AssertionError, "do not match the expected orphan set",
        ):
            assert_quarantine_listing_invariant(bad, expected_paths=set())

    def test_listing_checker_rejects_a_folder_outside_every_known_root(self) -> None:
        bad = _fixture_result([QuarantineFolder(
            name="Rogue",
            path="/elsewhere/Rogue",
            mtime_ns=1,
        )])
        with self.assertRaisesRegex(
            AssertionError, "does not belong to any known quarantine root",
        ):
            assert_quarantine_listing_invariant(
                bad, expected_paths={"/elsewhere/Rogue"},
            )

    def test_listing_checker_rejects_a_folder_whose_name_does_not_match_its_path(
        self,
    ) -> None:
        bad = _fixture_result([QuarantineFolder(
            name="Impostor Name",
            path="/downloads/failed_imports/Real Name",
            mtime_ns=1,
        )])
        with self.assertRaisesRegex(
            AssertionError, "does not match its own path",
        ):
            assert_quarantine_listing_invariant(
                bad, expected_paths={"/downloads/failed_imports/Real Name"},
            )

    def test_listing_checker_rejects_a_special_bucket_under_the_download_dir_failed_imports_root(
        self,
    ) -> None:
        bad = _fixture_result([QuarantineFolder(
            name="bad_files",
            path="/downloads/failed_imports/bad_files",
            mtime_ns=1,
        )])
        with self.assertRaisesRegex(
            AssertionError, "was listed as an orphan folder",
        ):
            assert_quarantine_listing_invariant(
                bad, expected_paths={"/downloads/failed_imports/bad_files"},
            )

    def test_listing_checker_rejects_a_special_bucket_under_the_processing_failed_imports_root(
        self,
    ) -> None:
        """#1122 F3/F7: the SAME clause, now proven for the NEW processing-
        side failed_imports root too — not just the pre-existing
        download-dir-rooted one."""
        bad = _fixture_result([QuarantineFolder(
            name="untracked_audio",
            path="/processing/albums/failed_imports/untracked_audio",
            mtime_ns=1,
        )])
        with self.assertRaisesRegex(
            AssertionError, "was listed as an orphan folder",
        ):
            assert_quarantine_listing_invariant(
                bad,
                expected_paths={
                    "/processing/albums/failed_imports/untracked_audio",
                },
            )


class TestGeneratedQuarantineLifecycle(unittest.TestCase):
    @given(st.lists(
        st.tuples(
            st.sampled_from(QUARANTINE_KINDS),
            st.sampled_from(REFERENCE_KINDS),
            st.sampled_from(REQUEST_STATUSES),
        ),
        min_size=0,
        max_size=12,
    ))
    def test_only_unreferenced_immediate_album_roots_surface(
        self,
        row_states: list[tuple[str, str, str]],
    ) -> None:
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as other, \
                tempfile.TemporaryDirectory() as proc_root:
            quarantine = os.path.join(root, "failed_imports")
            wrong_matches = os.path.join(root, "wrong_matches")
            processing_failed_imports = _selected_root_for_kind(
                "processing_failed_imports", root, proc_root,
            )
            processing_wrong_matches = _selected_root_for_kind(
                "processing_wrong_matches", root, proc_root,
            )
            os.makedirs(quarantine)
            os.makedirs(wrong_matches)
            os.makedirs(processing_failed_imports)
            os.makedirs(processing_wrong_matches)
            # Both failed_imports roots get the same code-owned special-
            # bucket exclusion (issue #1122 F3) — manufacturing a bucket
            # child under EACH is what lets a mutant that drops
            # SPECIAL_QUARANTINE_BUCKETS from either tuple entry get caught
            # by this property, not just by a deterministic pin.
            for special_kind in SPECIAL_BUCKET_KINDS:
                special_root = _selected_root_for_kind(special_kind, root, proc_root)
                os.makedirs(os.path.join(special_root, "bad_files", "Bad Child"))
                os.makedirs(
                    os.path.join(special_root, "untracked_audio", "Leftover Child"),
                )
            db = FakePipelineDB()
            expected: set[str] = set()

            for index, (
                kind,
                reference_kind,
                request_status,
            ) in enumerate(row_states):
                name = f"Album {index:02d}"
                selected_root = _selected_root_for_kind(kind, root, proc_root)
                path = os.path.join(selected_root, name)
                descendant = os.path.join(path, "Disc 1")
                os.makedirs(descendant)

                if reference_kind == "none":
                    expected.add(path)
                    continue
                # Relative reference forms join the single global download
                # dir (production contract) — they only protect a kind
                # whose real root actually lives there. The processing-side
                # root never does, so a relative reference aimed at it is a
                # legitimate generated world that must stay unreferenced.
                relative_reaches = _relative_reference_reaches_kind(
                    kind, root, proc_root,
                )
                if reference_kind == "relative":
                    failed_path = os.path.join(kind, name)
                    if not relative_reaches:
                        expected.add(path)
                elif reference_kind == "absolute":
                    failed_path = path
                elif reference_kind == "relative_descendant":
                    failed_path = os.path.join(kind, name, "Disc 1")
                    if not relative_reaches:
                        expected.add(path)
                elif reference_kind == "absolute_descendant":
                    failed_path = descendant
                else:
                    failed_path = os.path.join(other, kind, name)
                    expected.add(path)
                if request_status == "replaced":
                    expected.add(path)
                _seed_reference(
                    db,
                    failed_path,
                    index,
                    request_status=request_status,
                )

            result = list_unreferenced_quarantine_folders(
                db, root, processing_dir=proc_root,
            )

            assert_quarantine_listing_invariant(
                result,
                expected_paths=expected,
            )


if __name__ == "__main__":
    unittest.main()
