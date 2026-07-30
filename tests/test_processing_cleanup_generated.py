"""Generated patrol for exact journaled cleanup paths and manifests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Literal

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.processing_cleanup import (
    PROCESSING_CLEANUP_NO_OP,
    PROCESSING_CLEANUP_QUARANTINE_SOURCE,
    PROCESSING_CLEANUP_REMOVE_SOURCE,
    execute_processing_cleanup,
)
from tests.test_processing_cleanup import _journal_row, _JournalStore

GeneratedAction = Literal[
    "remove_source_tree",
    "quarantine_source_tree",
    "no_op",
]


def assert_exact_cleanup_postcondition(
    *,
    action: GeneratedAction,
    source_exists: bool,
    selected_destination: str | None,
    observed_destination: str | None,
    receipt_present: bool,
) -> None:
    if not receipt_present:
        raise AssertionError("cleanup completed without a typed receipt")
    if source_exists:
        raise AssertionError("completed cleanup retained its source")
    if action == PROCESSING_CLEANUP_QUARANTINE_SOURCE:
        if (
            selected_destination is None
            or observed_destination != selected_destination
        ):
            raise AssertionError(
                "quarantine did not use the one journal-selected destination"
            )
    elif observed_destination is not None:
        raise AssertionError("non-quarantine cleanup invented a destination")


class TestProcessingCleanupGenerated(unittest.TestCase):
    @example(
        action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
        files=[("a", b"one"), ("b", b"two")],
    )
    @given(
        action=st.sampled_from(
            (
                PROCESSING_CLEANUP_REMOVE_SOURCE,
                PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                PROCESSING_CLEANUP_NO_OP,
            )
        ),
        files=st.lists(
            st.tuples(
                st.text(
                    alphabet="abcdefghijklmnopqrstuvwxyz",
                    min_size=1,
                    max_size=8,
                ),
                st.binary(max_size=64),
            ),
            min_size=0,
            max_size=6,
            unique_by=lambda item: item[0],
        ),
    )
    def test_generated_exact_tree_cleanup(
        self,
        action: GeneratedAction,
        files: list[tuple[str, bytes]],
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            source = base / "source"
            destination: Path | None = None
            if action != PROCESSING_CLEANUP_NO_OP:
                source.mkdir()
                for name, content in files:
                    (source / name).write_bytes(content)
            if action == PROCESSING_CLEANUP_QUARANTINE_SOURCE:
                quarantine = base / "quarantine"
                quarantine.mkdir()
                destination = quarantine / "selected"
            store = _JournalStore(
                _journal_row(
                    action=action,
                    source_path=str(source),
                    destination_path=(
                        str(destination)
                        if destination is not None
                        else None
                    ),
                )
            )

            completed = execute_processing_cleanup(
                store,
                store.row,
                owner_checkpoint=lambda: None,
            )

            observed_destination = (
                str(destination)
                if destination is not None and destination.exists()
                else None
            )
            assert_exact_cleanup_postcondition(
                action=action,
                source_exists=source.exists(),
                selected_destination=(
                    str(destination) if destination is not None else None
                ),
                observed_destination=observed_destination,
                receipt_present=completed["completed_receipt"] is not None,
            )
            if destination is not None:
                self.assertEqual(
                    {
                        path.name: path.read_bytes()
                        for path in destination.iterdir()
                    },
                    dict(files),
                )

    def test_checker_rejects_recomputed_destination_known_bad(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "one journal-selected destination",
        ):
            assert_exact_cleanup_postcondition(
                action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                source_exists=False,
                selected_destination="/quarantine/album",
                observed_destination="/quarantine/album_1",
                receipt_present=True,
            )

    def test_checker_rejects_post_cancellation_rollback_known_bad(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "retained its source",
        ):
            assert_exact_cleanup_postcondition(
                action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                source_exists=True,
                selected_destination=None,
                observed_destination=None,
                receipt_present=True,
            )


if __name__ == "__main__":
    unittest.main()
