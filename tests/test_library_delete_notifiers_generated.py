#!/usr/bin/env python3
"""Generated targeting and observation laws for library-delete notifiers."""

from __future__ import annotations

import configparser
import logging
import tempfile
import unittest
from pathlib import Path

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.config import CratediggerConfig
from lib.library_delete_notifiers import notify_library_delete
from lib.util import JellyfinAlbumRef, PlexAlbumRef


def _notifier_config(
    root: str,
    *,
    plex: bool,
    jellyfin: bool,
) -> CratediggerConfig:
    parser = configparser.RawConfigParser()
    parser.read_dict({
        "Beets": {"directory": root},
        "Plex": {
            "url": "http://plex" if plex else "",
            "token": "plex-token" if plex else "",
            "library_section_id": "3",
            "path_map": f"{root}:/plex-music",
        },
        "Jellyfin": {
            "url": "http://jellyfin" if jellyfin else "",
            "token": "jellyfin-token" if jellyfin else "",
            "library_id": "library-root",
            "path_map": f"{root}:/jellyfin-music",
        },
    })
    return CratediggerConfig.from_ini(parser)


def assert_plex_delete_target_law(
    *,
    root: Path,
    former_album_path: Path,
    submitted_target: Path | None,
) -> None:
    """A Plex target is an existing in-root ancestor, never a deleted path."""
    resolved_root = root.resolve(strict=False)
    resolved_former = former_album_path.resolve(strict=False)
    try:
        resolved_former.relative_to(resolved_root)
        former_is_in_root = True
    except ValueError:
        former_is_in_root = False

    if not former_is_in_root:
        if submitted_target is not None:
            raise AssertionError("out-of-root delete acquired a Plex target")
        return
    if submitted_target is None:
        raise AssertionError("in-root delete did not acquire a Plex target")
    resolved_target = submitted_target.resolve(strict=False)
    try:
        resolved_target.relative_to(resolved_root)
    except ValueError as exc:
        raise AssertionError("Plex target escaped the library root") from exc
    if not submitted_target.exists():
        raise AssertionError("Plex target is not observably present")
    if not former_album_path.exists() and resolved_target == resolved_former:
        raise AssertionError("Plex targeted the deleted album path")
    expected = former_album_path
    while expected != root and not expected.exists():
        expected = expected.parent
    if not expected.exists() or resolved_target != expected.resolve(strict=False):
        raise AssertionError("Plex target was not the nearest existing ancestor")


def assert_jellyfin_delete_observation_law(
    *,
    initial_exact: bool,
    observed_absent: bool,
    lookup_failed: bool,
    refresh_failed: bool,
    outcome_status: str,
    refresh_target: str | None,
    raised: bool,
) -> None:
    """Exact targeting and observed absence are required for completion proof."""
    if raised:
        raise AssertionError("notifier failure escaped the best-effort boundary")
    expected_target = "exact-album" if initial_exact else "library-root"
    if refresh_target != expected_target:
        raise AssertionError("Jellyfin refresh used the wrong target")
    should_submit = (
        initial_exact
        and observed_absent
        and not lookup_failed
        and not refresh_failed
    )
    if (outcome_status == "submitted") != should_submit:
        raise AssertionError("Jellyfin status did not match observed absence")
    if (lookup_failed or refresh_failed) and outcome_status != "warning":
        raise AssertionError("Jellyfin failure was not surfaced as a warning")


SAFE_COMPONENTS = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=1,
    max_size=12,
)


class TestGeneratedDeleteNotifierLaws(unittest.TestCase):
    @example(
        artist="Artist", disc="Disc 1", album="Deleted Album",
        existing_depth=2, outside_root=False,
    )
    @example(
        artist="Artist", disc="Disc 1", album="Deleted Album",
        existing_depth=0, outside_root=True,
    )
    @given(
        artist=SAFE_COMPONENTS,
        disc=SAFE_COMPONENTS,
        album=SAFE_COMPONENTS,
        existing_depth=st.integers(min_value=0, max_value=2),
        outside_root=st.booleans(),
    )
    def test_plex_targets_nearest_existing_in_root_ancestor(
        self,
        artist: str,
        disc: str,
        album: str,
        existing_depth: int,
        outside_root: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root = base / "library"
            root.mkdir()
            artist_path = root / artist
            disc_path = artist_path / disc
            if existing_depth >= 1:
                artist_path.mkdir()
            if existing_depth >= 2:
                disc_path.mkdir()
            former = (
                base / "outside" / album
                if outside_root else disc_path / album
            )
            submissions: list[Path] = []

            outcomes = notify_library_delete(
                _notifier_config(str(root), plex=True, jellyfin=False),
                str(former),
                plex_find_fn=lambda _cfg, _path: PlexAlbumRef("77", 1),
                plex_scan_fn=lambda _cfg, path: (
                    submissions.append(Path(path)) or (200, path)
                ),
            )

            submitted_target = submissions[0] if submissions else None
            assert_plex_delete_target_law(
                root=root,
                former_album_path=former,
                submitted_target=submitted_target,
            )
            plex = next(item for item in outcomes if item.provider == "plex")
            if outside_root:
                self.assertEqual(plex.status, "warning")
            else:
                expected = (root, artist_path, disc_path)[existing_depth]
                self.assertEqual(submitted_target, expected)
                self.assertEqual(plex.status, "submitted")

    @example(mode="exact_then_absent", http_status=204)
    @example(mode="exact_stale", http_status=200)
    @example(mode="initial_lookup_error", http_status=202)
    @example(mode="exact_refresh_error", http_status=204)
    @given(
        mode=st.sampled_from((
            "initial_absent",
            "exact_then_absent",
            "exact_stale",
            "initial_lookup_error",
            "post_lookup_error",
            "exact_refresh_error",
            "fallback_refresh_error",
        )),
        http_status=st.integers(min_value=200, max_value=299),
    )
    def test_jellyfin_requires_observed_absence_and_contains_failures(
        self,
        mode: str,
        http_status: int,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            exact = JellyfinAlbumRef("exact-album", "date")
            lookup_count = 0
            refreshes: list[str | None] = []

            def find(_cfg: CratediggerConfig, _path: str):
                nonlocal lookup_count
                lookup_count += 1
                if mode == "initial_lookup_error":
                    raise RuntimeError("generated initial lookup failure")
                if mode == "initial_absent" or mode == "fallback_refresh_error":
                    return None
                if lookup_count == 1:
                    return exact
                if mode == "exact_then_absent":
                    return None
                if mode == "post_lookup_error":
                    raise RuntimeError("generated post-refresh lookup failure")
                return exact

            def refresh(_cfg: CratediggerConfig, item_id: str | None):
                refreshes.append(item_id)
                if mode in {"exact_refresh_error", "fallback_refresh_error"}:
                    raise RuntimeError("generated refresh failure")
                return http_status, f"/Items/{item_id or 'library'}/Refresh"

            previous_disable = logging.root.manager.disable
            try:
                logging.disable(logging.CRITICAL)
                outcomes = notify_library_delete(
                    _notifier_config(raw, plex=False, jellyfin=True),
                    str(former),
                    jellyfin_find_fn=find,
                    jellyfin_refresh_fn=refresh,
                )
            except Exception:
                assert_jellyfin_delete_observation_law(
                    initial_exact=mode not in {
                        "initial_absent", "initial_lookup_error",
                        "fallback_refresh_error",
                    },
                    observed_absent=mode == "exact_then_absent",
                    lookup_failed=mode in {
                        "initial_lookup_error", "post_lookup_error",
                    },
                    refresh_failed=mode in {
                        "exact_refresh_error", "fallback_refresh_error",
                    },
                    outcome_status="raised",
                    refresh_target=refreshes[0] if refreshes else None,
                    raised=True,
                )
                raise AssertionError("unreachable")
            finally:
                logging.disable(previous_disable)

            jellyfin = next(
                item for item in outcomes if item.provider == "jellyfin"
            )
            initial_exact = mode not in {
                "initial_absent", "initial_lookup_error",
                "fallback_refresh_error",
            }
            lookup_failed = mode in {
                "initial_lookup_error", "post_lookup_error",
            }
            refresh_failed = mode in {
                "exact_refresh_error", "fallback_refresh_error",
            }
            assert_jellyfin_delete_observation_law(
                initial_exact=initial_exact,
                observed_absent=mode == "exact_then_absent",
                lookup_failed=lookup_failed,
                refresh_failed=refresh_failed,
                outcome_status=jellyfin.status,
                refresh_target=refreshes[0] if refreshes else None,
                raised=False,
            )
            if lookup_failed or refresh_failed:
                self.assertIn("RuntimeError", jellyfin.detail)


class TestDeleteNotifierCheckerKnownBad(unittest.TestCase):
    def test_plex_checker_rejects_deleted_and_out_of_root_targets(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "library"
            root.mkdir()
            deleted = root / "Artist" / "Deleted Album"
            artist = root / "Artist"
            artist.mkdir()
            outside = Path(raw) / "outside"
            outside.mkdir()
            for name, former, target in (
                ("deleted", deleted, deleted),
                ("outside", outside / "Album", outside),
                ("not_nearest", deleted, root),
            ):
                with self.subTest(mutant=name), self.assertRaises(AssertionError):
                    assert_plex_delete_target_law(
                        root=root,
                        former_album_path=former,
                        submitted_target=target,
                    )

    def test_jellyfin_checker_rejects_target_status_and_boundary_mutants(
        self,
    ) -> None:
        mutants = {
            "wrong_exact_target": dict(
                initial_exact=True, observed_absent=True,
                lookup_failed=False, refresh_failed=False,
                outcome_status="submitted", refresh_target="library-root",
                raised=False,
            ),
            "stale_2xx_submitted": dict(
                initial_exact=True, observed_absent=False,
                lookup_failed=False, refresh_failed=False,
                outcome_status="submitted", refresh_target="exact-album",
                raised=False,
            ),
            "lookup_failure_hidden": dict(
                initial_exact=False, observed_absent=False,
                lookup_failed=True, refresh_failed=False,
                outcome_status="submitted", refresh_target="library-root",
                raised=False,
            ),
            "refresh_failure_hidden": dict(
                initial_exact=True, observed_absent=False,
                lookup_failed=False, refresh_failed=True,
                outcome_status="submitted", refresh_target="exact-album",
                raised=False,
            ),
            "exception_escaped": dict(
                initial_exact=True, observed_absent=False,
                lookup_failed=False, refresh_failed=True,
                outcome_status="raised", refresh_target="exact-album",
                raised=True,
            ),
        }
        for name, world in mutants.items():
            with self.subTest(mutant=name), self.assertRaises(AssertionError):
                assert_jellyfin_delete_observation_law(
                    initial_exact=bool(world["initial_exact"]),
                    observed_absent=bool(world["observed_absent"]),
                    lookup_failed=bool(world["lookup_failed"]),
                    refresh_failed=bool(world["refresh_failed"]),
                    outcome_status=str(world["outcome_status"]),
                    refresh_target=str(world["refresh_target"]),
                    raised=bool(world["raised"]),
                )


if __name__ == "__main__":
    unittest.main()
