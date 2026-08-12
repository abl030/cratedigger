"""Generated patrol for exact journaled cleanup paths and manifests."""

from __future__ import annotations

import re
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

# Per-clause proof (#1094). Each clause of
# ``assert_exact_cleanup_postcondition`` is named so a self-test can anchor on
# that clause's own message instead of a substring a sibling could satisfy.
CLAUSE_NO_RECEIPT = "cleanup completed without a typed receipt"
CLAUSE_SOURCE_RETAINED = "completed cleanup retained its source"
CLAUSE_UNSELECTED_DESTINATION = (
    "quarantine did not use the one journal-selected destination"
)
CLAUSE_INVENTED_DESTINATION = "non-quarantine cleanup invented a destination"


def _exact_clause(message: str) -> str:
    """Anchor a clause message so no sibling clause can satisfy the regex."""
    return "^" + re.escape(message) + "$"


def observed_cleanup_destination(
    base: Path,
    *,
    source: Path,
    selected: Path | None,
) -> str | None:
    """Whatever tree the executor actually left behind outside the source.

    #1094 Q2: reading only ``selected`` made ``CLAUSE_INVENTED_DESTINATION``
    unfalsifiable — a non-quarantine journal carries no destination, so a
    production mutant that renamed the source aside instead of removing it
    (a "soft delete" regression) left the whole property green. The observation
    is therefore the base directory's real post-state: a surviving tree that
    is neither the source nor the journal-selected destination (nor one of its
    ancestors) is exactly the invented destination the clause legislates
    against, and it also names a recomputed quarantine target precisely.
    """
    excluded = {base, source}
    if selected is not None:
        excluded.add(selected)
        excluded.update(selected.parents)
    strays = sorted(
        str(path)
        for path in base.rglob("*")
        if path.is_dir() and path not in excluded
    )
    if strays:
        return strays[0]
    if selected is not None and selected.exists():
        return str(selected)
    return None


def assert_exact_cleanup_postcondition(
    *,
    action: GeneratedAction,
    source_exists: bool,
    selected_destination: str | None,
    observed_destination: str | None,
    receipt_present: bool,
) -> None:
    if not receipt_present:
        raise AssertionError(CLAUSE_NO_RECEIPT)
    if source_exists:
        raise AssertionError(CLAUSE_SOURCE_RETAINED)
    if action == PROCESSING_CLEANUP_QUARANTINE_SOURCE:
        if (
            selected_destination is None
            or observed_destination != selected_destination
        ):
            raise AssertionError(CLAUSE_UNSELECTED_DESTINATION)
    elif observed_destination is not None:
        raise AssertionError(CLAUSE_INVENTED_DESTINATION)


class TestProcessingCleanupGenerated(unittest.TestCase):
    @example(
        action=PROCESSING_CLEANUP_QUARANTINE_SOURCE,
        files=[("a", b"one"), ("b", b"two")],
    )
    # #1094 Q3/Q4: one pin per decisive arm. ``remove_source_tree`` is the
    # only arm that can fire ``CLAUSE_SOURCE_RETAINED`` or
    # ``CLAUSE_INVENTED_DESTINATION``. Editing this property body reshuffles
    # the whole generated sequence, so the arms are pinned rather than left to
    # the deterministic draw.
    #
    # The ``no_op`` pin is a coverage pin, not a clause pin: this property
    # never creates ``source`` for that action, so ``base`` holds no directory
    # and ``observed_cleanup_destination`` returns ``None`` whatever
    # ``execute_processing_cleanup`` does — no mutant can make that arm fire
    # the invented-destination clause. Giving a no-op journal a source tree it
    # must leave untouched is a real entropy gap in this property, pre-dating
    # #1094 and not closed here (round-3 review N2).
    @example(
        action=PROCESSING_CLEANUP_REMOVE_SOURCE,
        files=[("a", b"one"), ("b", b"two")],
    )
    @example(action=PROCESSING_CLEANUP_NO_OP, files=[])
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

            observed_destination = observed_cleanup_destination(
                base,
                source=source,
                selected=destination,
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

    def test_every_postcondition_clause_has_a_named_world(self) -> None:
        """One minimal world per clause, anchored on that clause's message.

        Each row makes exactly one clause's condition true while every EARLIER
        clause in the function passes, so a short-circuiting ``raise`` chain
        cannot attribute a world to the wrong clause (#1094 Q1). The two
        historical known-bad tests are rows 4 and 2; they asserted unanchored
        substrings and left the receipt and invented-destination clauses with
        no proof at all.
        """
        cases: tuple[
            tuple[str, GeneratedAction, bool, str | None, str | None, bool, str],
            ...,
        ] = (
            (
                "completed without a receipt",
                PROCESSING_CLEANUP_REMOVE_SOURCE,
                False,
                None,
                None,
                False,
                CLAUSE_NO_RECEIPT,
            ),
            (
                "post-cancellation rollback restored the source",
                PROCESSING_CLEANUP_REMOVE_SOURCE,
                True,
                None,
                None,
                True,
                CLAUSE_SOURCE_RETAINED,
            ),
            (
                "quarantine ran with no journal-selected destination",
                PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                False,
                None,
                "/quarantine/album",
                True,
                CLAUSE_UNSELECTED_DESTINATION,
            ),
            (
                "quarantine recomputed a collision suffix",
                PROCESSING_CLEANUP_QUARANTINE_SOURCE,
                False,
                "/quarantine/album",
                "/quarantine/album_1",
                True,
                CLAUSE_UNSELECTED_DESTINATION,
            ),
            (
                "remove-source relocated instead of removing",
                PROCESSING_CLEANUP_REMOVE_SOURCE,
                False,
                None,
                "/processing/albums/source.quarantined",
                True,
                CLAUSE_INVENTED_DESTINATION,
            ),
            (
                "no-op invented a destination",
                PROCESSING_CLEANUP_NO_OP,
                False,
                None,
                "/quarantine/album",
                True,
                CLAUSE_INVENTED_DESTINATION,
            ),
        )
        for (
            description,
            action,
            source_exists,
            selected,
            observed,
            receipt,
            message,
        ) in cases:
            with (
                self.subTest(description),
                self.assertRaisesRegex(
                    AssertionError,
                    _exact_clause(message),
                ),
            ):
                assert_exact_cleanup_postcondition(
                    action=action,
                    source_exists=source_exists,
                    selected_destination=selected,
                    observed_destination=observed,
                    receipt_present=receipt,
                )

    def test_checker_admits_every_correct_postcondition(self) -> None:
        """Must-still-work: no clause fires on a correctly executed cleanup."""
        cases: tuple[
            tuple[GeneratedAction, str | None, str | None],
            ...,
        ] = (
            ("remove_source_tree", None, None),
            ("no_op", None, None),
            (
                "quarantine_source_tree",
                "/quarantine/album",
                "/quarantine/album",
            ),
        )
        for action, selected, observed in cases:
            with self.subTest(action):
                assert_exact_cleanup_postcondition(
                    action=action,
                    source_exists=False,
                    selected_destination=selected,
                    observed_destination=observed,
                    receipt_present=True,
                )

    def test_observation_names_a_stray_tree_outside_the_journal(self) -> None:
        """The widened observation is what makes the invented-destination
        clause reachable, so it gets its own direct contract."""
        with tempfile.TemporaryDirectory() as raw:
            # A correct remove/no-op world: nothing survives outside the source.
            base = Path(raw) / "clean"
            base.mkdir()
            source = base / "source"
            source.mkdir()
            self.assertIsNone(
                observed_cleanup_destination(
                    base,
                    source=source,
                    selected=None,
                )
            )

            # A correct quarantine world: only the selected destination.
            base = Path(raw) / "quarantined"
            base.mkdir()
            source = base / "source"
            selected = base / "quarantine" / "selected"
            selected.mkdir(parents=True)
            self.assertEqual(
                observed_cleanup_destination(
                    base,
                    source=source,
                    selected=selected,
                ),
                str(selected),
            )

            # The mutant world: a relocated source with no journaled
            # destination is named, for both journal shapes.
            base = Path(raw) / "relocated"
            base.mkdir()
            source = base / "source"
            stray = base / "source.quarantined"
            stray.mkdir()
            self.assertEqual(
                observed_cleanup_destination(
                    base,
                    source=source,
                    selected=None,
                ),
                str(stray),
            )
            selected = base / "quarantine" / "selected"
            selected.mkdir(parents=True)
            self.assertEqual(
                observed_cleanup_destination(
                    base,
                    source=source,
                    selected=selected,
                ),
                str(stray),
            )


if __name__ == "__main__":
    unittest.main()
