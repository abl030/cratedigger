"""Generated property over the recovery-debris world space (issue #1089).

Patrols the world the deterministic pins in
``tests/test_automation_startup_recovery.py`` and
``tests/test_automation_recovery_debris.py`` name individually: album
present/absent, item paths under the launch source / the library root /
mixed, a processing-cleanup journal present/absent, and the job's launch
fields null/populated. Drives the REAL production composition
(``FakePipelineDB.recover_automation_import_job`` — itself delegating to the
real ``complete_owner_processing_cleanup`` and, via injection, the real
``remove_recovery_debris`` confinement logic) rather than reimplementing any
of it. Only the two Beets DI seams (``beets_db_factory``/``beets_delete_fn``)
are fast in-memory stand-ins, exactly the seams
``lib.automation_recovery_debris.remove_recovery_debris`` already declares
for this purpose — no new bespoke harness.
"""

from __future__ import annotations

import functools
import os
import sys
import tempfile
import unittest
from typing import Self

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.automation_recovery_debris import remove_recovery_debris
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteOutcome,
    BeetsDeleteRequest,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_automation_startup_recovery import _StartupRecoveryBuilders

_LIBRARY_ROOT = "/library/root"


class _FastBeetsDB:
    """In-memory stand-in for ``SupportsRecoveryDebrisBeetsDB`` — no real I/O.

    The world's identity matching is deliberately trivial here (candidate id
    ``1`` whenever ``album_present``) — identity mismatch is a SEPARATE,
    already mutant-tested axis in ``tests/test_beets_delete.py`` and
    ``tests/test_automation_recovery_debris.py``. This property patrols path
    confinement and the no-denylist invariant, not identity.
    """

    library_db_path = "/fake/beets.db"
    library_root = _LIBRARY_ROOT

    def __init__(self, item_paths: tuple[str, ...] | None) -> None:
        self._item_paths = item_paths

    def get_all_album_ids_for_release(self, release_id: str) -> list[int]:
        del release_id
        return [1] if self._item_paths is not None else []

    def get_album_detail(self, album_id: int) -> dict[str, object] | None:
        if self._item_paths is None or album_id != 1:
            return None
        return {"tracks": [{"path": p} for p in self._item_paths]}

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _RecordingBeetsDelete:
    """Records every request the admitted delete lane would have received.

    Reaching this stub at all already proves ``remove_recovery_debris``'s
    own confinement check passed — this only needs to report success so the
    composition's terminal-write path completes.
    """

    def __init__(self) -> None:
        self.requests: list[BeetsDeleteRequest] = []

    def __call__(self, request: BeetsDeleteRequest) -> BeetsDeleteOutcome:
        self.requests.append(request)
        return BeetsDeleteCompleted(
            album_id=request.album_id,
            album_name="Frozen",
            artist_name="Idina Menzel",
            former_album_path=request.debris_confinement_root or "",
            deleted_tracks=0,
            deleted_artifacts=0,
            preserved_paths=(),
            metadata_only=True,
        )


_PATH_WORLD = st.sampled_from(("all_source", "all_library", "mixed"))


class TestRecoveryDebrisWorldGenerated(_StartupRecoveryBuilders, unittest.TestCase):
    """No ``setUp``-built state: Hypothesis's ``@given`` on a ``unittest``
    method runs ``setUp``/``tearDown`` ONCE per test method invocation, not
    once per generated example — a ``self.db`` built in ``setUp`` would
    accumulate mutations across examples (the request handed off to
    ``processing`` in example 1 is no longer ``wanted`` in example 2).
    Every example therefore builds its own fresh ``FakePipelineDB`` via
    ``_fresh_state()``, called first thing in the test method body — the
    same pattern ``_World`` uses in
    ``tests/test_automation_startup_recovery_generated.py``.
    """

    def _fresh_state(self) -> None:
        self.db = FakePipelineDB()
        self.request_id = 42
        self.mb_release_id = "cbb51c9f-9999-8888-7777-666666666666"
        self.db.seed_request(make_request_row(
            id=self.request_id,
            mb_release_id=self.mb_release_id,
            status="wanted",
        ))

    def _item_paths_for(
        self,
        path_world: str,
        *,
        source_path: str,
    ) -> tuple[str, ...]:
        source_item = os.path.join(source_path, "01 - Track.mp3")
        library_item = os.path.join(_LIBRARY_ROOT, "02 - Track.mp3")
        if path_world == "all_source":
            return (source_item,)
        if path_world == "all_library":
            return (library_item,)
        assert path_world == "mixed"
        return (source_item, library_item)

    @given(
        album_present=st.booleans(),
        path_world=_PATH_WORLD,
        journal_present=st.booleans(),
        launched=st.booleans(),
    )
    def test_recovery_never_denylists_and_never_removes_unconfined_albums(
        self,
        album_present: bool,
        path_world: str,
        journal_present: bool,
        launched: bool,
    ) -> None:
        self._fresh_state()
        case = self._case()
        # Scoped locally per example, not via self._album_dir() -- that
        # helper's addCleanup binds to the whole @given test METHOD, not
        # each Hypothesis example (issue #1214 defect class). Mirrors
        # _album_dir("world", "01 - Track.mp3")'s exact layout.
        with tempfile.TemporaryDirectory(prefix="startup-recovery-") as root:
            path = os.path.join(root, "albums", "world")
            os.makedirs(path)
            with open(os.path.join(path, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"audio")

            if launched:
                owner, lease = self._launched_owner(path)
            else:
                owner, lease = self._preview_owner(path)
            if journal_present:
                self._journal(owner.id, path)

            item_paths = (
                self._item_paths_for(path_world, source_path=path)
                if album_present
                else None
            )
            stub_delete = _RecordingBeetsDelete()
            debris_fn = functools.partial(
                remove_recovery_debris,
                beets_db_factory=lambda: _FastBeetsDB(item_paths),
                beets_delete_fn=stub_delete,
            )

            self._recover(owner.id, lease, debris_removal_fn=debris_fn)

            # CLAUSE no_denylist: this recovery path writes ZERO source_denylist
            # rows in EVERY world — proven at a finer grain (a planted
            # `add_denylist` mutant in both the real PipelineDB and the fake
            # mirror) by the #1089 review's own mutant kill matrix.
            case.assertEqual(
                self.db.list_denylist_rows(),
                [],
                f"world: album_present={album_present} path_world={path_world} "
                f"journal_present={journal_present} launched={launched}",
            )

            # CLAUSE never_remove_unconfined: the admitted delete lane is only
            # ever reached when this job actually authorized a Beets launch AND
            # every item path is confined to that exact launch source — never
            # for a library-root or mixed world, whatever else varies.
            should_reach_delete_lane = (
                launched and album_present and path_world == "all_source"
            )
            if should_reach_delete_lane:
                case.assertEqual(len(stub_delete.requests), 1)
            else:
                case.assertEqual(
                    stub_delete.requests,
                    [],
                    f"world: album_present={album_present} path_world={path_world} "
                    f"journal_present={journal_present} launched={launched}",
                )


if __name__ == "__main__":
    unittest.main()
