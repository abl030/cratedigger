"""Tests for lib/context.py — CratediggerContext dataclass."""

import unittest
from collections.abc import Callable
from unittest.mock import MagicMock

from lib.context import (
    CratediggerContext,
    CycleCollaborators,
    WorkerCollaborators,
)
from tests.fakes import FakePipelineDBSource
from tests.helpers import make_cycle_collaborators


class TestCratediggerContext(unittest.TestCase):
    """Test CratediggerContext construction and cache isolation."""

    def test_context_construction(self):
        mock_cfg = MagicMock()
        mock_slskd = MagicMock()
        mock_db_source = MagicMock()

        ctx = CratediggerContext(
            collaborators=make_cycle_collaborators(
                cfg=mock_cfg,
                slskd=mock_slskd,
                pipeline_db_source=mock_db_source,
            ),
        )

        self.assertIs(ctx.cfg, mock_cfg)
        self.assertIs(ctx.slskd, mock_slskd)
        self.assertIs(ctx.pipeline_db_source, mock_db_source)
        self.assertIsInstance(ctx.search_cache, dict)
        self.assertIsInstance(ctx.folder_cache, dict)
        self.assertIsInstance(ctx.user_upload_speed, dict)
        self.assertIsInstance(ctx.broken_user, set)
        self.assertIsNotNone(ctx.browse_coordinator_lock)
        self.assertEqual(len(ctx.search_cache), 0)
        self.assertEqual(len(ctx.broken_user), 0)

    def test_context_cache_isolation(self):
        mock_cfg = MagicMock()
        mock_slskd = MagicMock()
        mock_db_source = MagicMock()

        ctx1 = CratediggerContext(
            collaborators=make_cycle_collaborators(
                cfg=mock_cfg,
                slskd=mock_slskd,
                pipeline_db_source=mock_db_source,
            ),
        )
        ctx2 = CratediggerContext(
            collaborators=make_cycle_collaborators(
                cfg=mock_cfg,
                slskd=mock_slskd,
                pipeline_db_source=mock_db_source,
            ),
        )

        # Mutating one context's caches should not affect the other
        ctx1.search_cache[42] = {"user1": {}}
        ctx1.broken_user.add("bad_user")
        ctx1.user_upload_speed["user1"] = 50000

        self.assertEqual(len(ctx2.search_cache), 0)
        self.assertEqual(len(ctx2.broken_user), 0)
        self.assertEqual(len(ctx2.user_upload_speed), 0)
        self.assertIsNot(ctx1.browse_coordinator_lock, ctx2.browse_coordinator_lock)


class TestCollaboratorsAreRequired(unittest.TestCase):
    """A collaborator cannot be forgotten at a construction site (#1313).

    PR #1280 shipped a second, inline ``CratediggerContext(...)`` that
    silently omitted ``download_ownership``, failing every download-timeout
    cleanup closed with the whole suite green. An 888-line hand-registered
    AST audit (``tests/test_context_construction_audit.py``) held the set
    of construction sites to catch a repeat; it checked exactly one thing —
    that the kwarg is PRESENT at each site — which the required frozen
    field now checks at every site, existing and future, for free.

    These are the runtime half of that guarantee; pyright's is the half
    that fires before the code runs.
    """

    _CYCLE_FIELDS = (
        "cfg", "slskd", "pipeline_db_source", "download_ownership",
        "claimed_queue_keys_registry", "peer_cache",
    )

    def _full_kwargs(self) -> dict[str, object]:
        return {name: MagicMock() for name in self._CYCLE_FIELDS}

    def _cycle_ctor(self) -> Callable[..., CycleCollaborators]:
        """The class, behind an unchecked-argument callable type.

        These tests construct deliberately WRONG argument lists, which is
        what makes them the runtime half of the guarantee — pyright's half
        is what fires at a real call site. ``Callable[..., X]`` says
        "arguments unchecked here" in the type system rather than with an
        escape hatch.
        """
        return CycleCollaborators

    def _worker_ctor(self) -> Callable[..., WorkerCollaborators]:
        """``_cycle_ctor``'s twin for the worker world."""
        return WorkerCollaborators

    def test_every_cycle_collaborator_is_required(self):
        construct = self._cycle_ctor()

        # Control: the complete set constructs.
        construct(**self._full_kwargs())

        for missing in self._CYCLE_FIELDS:
            with self.subTest(omitted=missing):
                kwargs = self._full_kwargs()
                del kwargs[missing]
                with self.assertRaises(TypeError) as caught:
                    construct(**kwargs)
                self.assertIn(missing, str(caught.exception))

    def test_cycle_collaborators_are_keyword_only(self):
        # Positional construction would let two same-typed collaborators be
        # transposed silently; kw_only makes that a TypeError.
        construct = self._cycle_ctor()
        with self.assertRaises(TypeError):
            construct(*self._full_kwargs().values())

    def test_collaborators_are_frozen(self):
        ctx = CratediggerContext(collaborators=make_cycle_collaborators())
        with self.assertRaises(Exception) as caught:
            # setattr, not an attribute assignment: pyright rejects the
            # latter on a frozen dataclass, which is the point — this pins
            # the runtime half.
            setattr(ctx.collaborators, "download_ownership", None)  # noqa: B010
        self.assertIn(
            "cannot assign to field 'download_ownership'",
            str(caught.exception))

    def test_worker_world_has_no_slskd_and_no_ownership_writer(self):
        cfg = MagicMock()
        db_source = FakePipelineDBSource()
        ctx = CratediggerContext(
            collaborators=WorkerCollaborators(
                cfg=cfg, pipeline_db_source=db_source),
        )

        self.assertIs(ctx.cfg, cfg)
        self.assertIs(ctx.pipeline_db_source, db_source)
        # The four the importer / preview worker structurally do not have.
        # No slskd client means no reachable destructive slskd call, which
        # is why the ownership writer that gates one is absent rather than
        # defaulted away.
        self.assertIsNone(ctx.slskd)
        self.assertIsNone(ctx.download_ownership)
        self.assertIsNone(ctx.claimed_queue_keys_registry)
        self.assertIsNone(ctx.peer_cache)

    def test_worker_collaborators_require_both_fields(self):
        construct = self._worker_ctor()

        for missing in ("cfg", "pipeline_db_source"):
            with self.subTest(omitted=missing):
                kwargs: dict[str, object] = {
                    "cfg": MagicMock(), "pipeline_db_source": MagicMock()}
                del kwargs[missing]
                with self.assertRaises(TypeError) as caught:
                    construct(**kwargs)
                self.assertIn(missing, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
