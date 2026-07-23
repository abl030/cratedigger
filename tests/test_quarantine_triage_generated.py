"""Generated lifecycle invariant for unreferenced quarantine discovery."""

from __future__ import annotations

import os
import tempfile
import unittest

from hypothesis import given, strategies as st

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
QUARANTINE_DIRECTORIES = ("failed_imports", "wrong_matches")
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
    """Assert exact, deterministic, immediate-folder discovery."""
    actual_paths = [folder.path for folder in result.folders]
    assert actual_paths == sorted(expected_paths, key=lambda path: os.path.basename(path))
    assert len(actual_paths) == len(set(actual_paths))
    quarantine_roots = {
        result.quarantine_root,
        result.wrong_matches_root,
    }
    for folder in result.folders:
        assert os.path.dirname(folder.path) in quarantine_roots
        assert folder.name == os.path.basename(folder.path)
        if os.path.dirname(folder.path) == result.quarantine_root:
            assert folder.name not in result.special_buckets


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


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_listing_checker_rejects_a_referenced_or_unexpected_folder(self) -> None:
        bad = QuarantineTriageResult(
            quarantine_root="/downloads/failed_imports",
            wrong_matches_root="/downloads/wrong_matches",
            folders=[QuarantineFolder(
                name="Referenced",
                path="/downloads/failed_imports/Referenced",
                mtime_ns=1,
            )],
            special_buckets=["bad_files", "untracked_audio"],
        )
        with self.assertRaises(AssertionError):
            assert_quarantine_listing_invariant(bad, expected_paths=set())


class TestGeneratedQuarantineLifecycle(unittest.TestCase):
    @given(st.lists(
        st.tuples(
            st.sampled_from(QUARANTINE_DIRECTORIES),
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
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as other:
            quarantine = os.path.join(root, "failed_imports")
            wrong_matches = os.path.join(root, "wrong_matches")
            os.makedirs(quarantine)
            os.makedirs(wrong_matches)
            os.makedirs(os.path.join(quarantine, "bad_files", "Bad Child"))
            os.makedirs(os.path.join(quarantine, "untracked_audio", "Leftover Child"))
            db = FakePipelineDB()
            expected: set[str] = set()

            for index, (
                quarantine_directory,
                reference_kind,
                request_status,
            ) in enumerate(row_states):
                name = f"Album {index:02d}"
                selected_root = os.path.join(root, quarantine_directory)
                path = os.path.join(selected_root, name)
                descendant = os.path.join(path, "Disc 1")
                os.makedirs(descendant)

                if reference_kind == "none":
                    expected.add(path)
                    continue
                if reference_kind == "relative":
                    failed_path = os.path.join(quarantine_directory, name)
                elif reference_kind == "absolute":
                    failed_path = path
                elif reference_kind == "relative_descendant":
                    failed_path = os.path.join(
                        quarantine_directory,
                        name,
                        "Disc 1",
                    )
                elif reference_kind == "absolute_descendant":
                    failed_path = descendant
                else:
                    failed_path = os.path.join(
                        other, quarantine_directory, name,
                    )
                    expected.add(path)
                if request_status == "replaced":
                    expected.add(path)
                _seed_reference(
                    db,
                    failed_path,
                    index,
                    request_status=request_status,
                )

            result = list_unreferenced_quarantine_folders(db, root)

            assert_quarantine_listing_invariant(
                result,
                expected_paths=expected,
            )


if __name__ == "__main__":
    unittest.main()
