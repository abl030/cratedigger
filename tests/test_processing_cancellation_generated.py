"""Generated fail-stop patrol for staged filesystem mutations."""

import errno
import os
import shutil
import tempfile
import unittest
from collections.abc import Callable
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.fs_authority import remove_relative_tree
from lib.import_execution import CancellationToken, ExecutionCancelled
from lib.staged_album import StagedAlbum

MoveFn = Callable[[StagedAlbum, str, CancellationToken], None]


class _CancelAtCheckpoint(CancellationToken):
    def __init__(self, checkpoint: int) -> None:
        super().__init__()
        self._checkpoint = checkpoint
        self._observed = 0

    def raise_if_cancelled(self) -> None:
        self._observed += 1
        if self._observed == self._checkpoint:
            self.cancel(f"cancel_at_{self._checkpoint}")
        super().raise_if_cancelled()


def assert_staged_move_is_fail_stop(
    *,
    file_count: int,
    cancel_at: int,
    move_fn: MoveFn,
) -> None:
    """A cancellation checkpoint permits no later move or cleanup mutation."""
    with tempfile.TemporaryDirectory() as raw:
        source = os.path.join(raw, "source")
        destination = os.path.join(raw, "destination")
        os.mkdir(source)
        names = [f"{index:02d}.flac" for index in range(file_count)]
        for name in names:
            with open(os.path.join(source, name), "wb") as handle:
                handle.write(name.encode())
        token = _CancelAtCheckpoint(cancel_at)
        real_listdir = os.listdir

        try:
            with patch(
                "lib.staged_album.os.listdir",
                side_effect=lambda path: sorted(real_listdir(path)),
            ):
                move_fn(StagedAlbum(source), destination, token)
        except ExecutionCancelled:
            pass
        else:
            raise AssertionError("staged move did not observe cancellation")

        expected_moved = min(file_count, max(0, cancel_at - 2))
        destination_names = (
            sorted(real_listdir(destination))
            if os.path.isdir(destination)
            else []
        )
        source_names = sorted(real_listdir(source))
        if len(destination_names) != expected_moved:
            raise AssertionError(
                "a filesystem mutation ran after its cancellation checkpoint"
            )
        if len(source_names) != file_count - expected_moved:
            raise AssertionError("source evidence changed after cancellation")
        if not os.path.isdir(source):
            raise AssertionError("cancelled execution removed the source tree")


def _production_move(
    staged: StagedAlbum,
    destination: str,
    token: CancellationToken,
) -> None:
    staged.move_to(destination, cancellation_token=token)


def _known_bad_preflight_only_move(
    staged: StagedAlbum,
    destination: str,
    token: CancellationToken,
) -> None:
    """Mutant: checks once, then performs every mutation without a checkpoint."""
    token.raise_if_cancelled()
    os.makedirs(destination, exist_ok=True)
    for name in os.listdir(staged.current_path):
        shutil.move(
            os.path.join(staged.current_path, name),
            os.path.join(destination, name),
        )
    shutil.rmtree(staged.current_path)


def assert_staged_move_supports_production_layout(
    *,
    file_count: int,
    missing_parent_depth: int,
    cross_filesystem: bool,
    move_fn: MoveFn,
) -> None:
    """A monitored move keeps the ordinary staging filesystem contract."""
    with tempfile.TemporaryDirectory() as raw:
        source = os.path.join(raw, "source")
        destination = os.path.join(
            raw,
            *(f"missing-{index}" for index in range(missing_parent_depth)),
            "album",
        )
        os.mkdir(source)
        expected = {f"{index:02d}.flac" for index in range(file_count)}
        for name in expected:
            with open(os.path.join(source, name), "wb") as handle:
                handle.write(name.encode())

        token = CancellationToken()
        if cross_filesystem:
            with patch(
                "lib.staged_album.os.rename",
                side_effect=OSError(errno.EXDEV, "cross-device link"),
            ):
                move_fn(StagedAlbum(source), destination, token)
        else:
            move_fn(StagedAlbum(source), destination, token)

        if set(os.listdir(destination)) != expected:
            raise AssertionError("staged move did not preserve every source entry")
        if os.path.exists(source):
            raise AssertionError("staged move did not consume its source directory")


def _known_bad_rename_only_move(
    staged: StagedAlbum,
    destination: str,
    token: CancellationToken,
) -> None:
    """Mutant: requires existing parents and one filesystem."""
    token.raise_if_cancelled()
    os.mkdir(destination)
    for name in os.listdir(staged.current_path):
        token.raise_if_cancelled()
        os.rename(
            os.path.join(staged.current_path, name),
            os.path.join(destination, name),
        )
    token.raise_if_cancelled()
    os.rmdir(staged.current_path)


def assert_recursive_cleanup_is_fail_stop(
    remove_fn: Callable[[int, str, CancellationToken], None],
) -> None:
    with tempfile.TemporaryDirectory() as raw:
        tree = os.path.join(raw, "tree")
        os.mkdir(tree)
        for name in ("a.flac", "b.flac"):
            with open(os.path.join(tree, name), "wb") as handle:
                handle.write(name.encode())
        parent_fd = os.open(raw, os.O_RDONLY | os.O_DIRECTORY)
        token = _CancelAtCheckpoint(2)
        try:
            remove_fn(parent_fd, "tree", token)
        except ExecutionCancelled:
            pass
        else:
            raise AssertionError("recursive cleanup did not observe cancellation")
        finally:
            os.close(parent_fd)
        if os.path.exists(os.path.join(tree, "a.flac")):
            raise AssertionError("first authorized deletion did not run")
        if not os.path.exists(os.path.join(tree, "b.flac")):
            raise AssertionError("cleanup mutated a child after cancellation")


def _production_remove(parent_fd: int, name: str, token: CancellationToken) -> None:
    remove_relative_tree(
        parent_fd,
        name,
        before_mutation=token.raise_if_cancelled,
    )


def _known_bad_preflight_only_remove(
    parent_fd: int,
    name: str,
    token: CancellationToken,
) -> None:
    token.raise_if_cancelled()
    remove_relative_tree(parent_fd, name)


class TestStagedMoveCancellationGenerated(unittest.TestCase):
    @given(
        file_count=st.integers(min_value=1, max_value=6),
        cancel_slot=st.integers(min_value=0, max_value=100),
    )
    @example(file_count=2, cancel_slot=2)
    def test_every_mutation_boundary_is_fail_stop(
        self,
        *,
        file_count: int,
        cancel_slot: int,
    ) -> None:
        cancel_at = 1 + cancel_slot % (file_count + 2)
        assert_staged_move_is_fail_stop(
            file_count=file_count,
            cancel_at=cancel_at,
            move_fn=_production_move,
        )

    @given(
        file_count=st.integers(min_value=1, max_value=6),
        missing_parent_depth=st.integers(min_value=0, max_value=3),
        cross_filesystem=st.booleans(),
    )
    @example(file_count=1, missing_parent_depth=3, cross_filesystem=False)
    @example(file_count=1, missing_parent_depth=0, cross_filesystem=True)
    def test_every_production_staging_layout_is_supported(
        self,
        *,
        file_count: int,
        missing_parent_depth: int,
        cross_filesystem: bool,
    ) -> None:
        assert_staged_move_supports_production_layout(
            file_count=file_count,
            missing_parent_depth=missing_parent_depth,
            cross_filesystem=cross_filesystem,
            move_fn=_production_move,
        )

    def test_checker_rejects_preflight_only_known_bad_mutant(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "did not observe cancellation",
        ):
            assert_staged_move_is_fail_stop(
                file_count=3,
                cancel_at=2,
                move_fn=_known_bad_preflight_only_move,
            )

    def test_layout_checker_rejects_rename_only_known_bad_mutant(self) -> None:
        with self.assertRaises(OSError):
            assert_staged_move_supports_production_layout(
                file_count=1,
                missing_parent_depth=0,
                cross_filesystem=True,
                move_fn=_known_bad_rename_only_move,
            )

    def test_recursive_cleanup_checks_every_child_mutation(self) -> None:
        assert_recursive_cleanup_is_fail_stop(_production_remove)

    def test_cleanup_checker_rejects_preflight_only_known_bad_mutant(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "did not observe cancellation",
        ):
            assert_recursive_cleanup_is_fail_stop(
                _known_bad_preflight_only_remove,
            )


if __name__ == "__main__":
    unittest.main()
