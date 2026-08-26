"""Generated targeting and report laws for library-delete notifiers."""

from __future__ import annotations

import configparser
import logging
import tempfile
import unittest
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.config import CratediggerConfig
from lib.library_delete_notifiers import DeleteNotification, notify_library_delete
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


def jellyfin_report_law_violations(
    *,
    initial_exact: bool,
    lookup_failed: bool,
    outcome_status: str,
    outcome_target: str,
    outcome_detail: str,
    raised: bool,
) -> list[str]:
    """Every way an observed Jellyfin report outcome breaks the law (issue
    #1221 item 1: the Jellyfin leg is detect-and-report for EVERY caller).
    Accumulating — every clause is evaluated regardless of earlier results,
    so ordering cannot mask one clause behind another (code-quality.md "New
    checkers prefer an accumulating list[str]"). The retired law's
    refresh-target and observed-absence clauses are gone because their
    worlds are impossible by construction: the refresh machinery no longer
    exists to record against."""
    violations: list[str] = []

    if raised:
        violations.append("notifier failure escaped the best-effort boundary")
    if outcome_status == "submitted":
        violations.append("the Jellyfin leg claimed a submission")
    if lookup_failed:
        if outcome_status != "warning":
            violations.append("a lookup failure was not surfaced as a warning")
        if "identity lookup failed" not in outcome_detail:
            violations.append("a lookup failure was not named in the detail")
        if "no Jellyfin item found" in outcome_detail:
            violations.append(
                "a lookup failure claimed a not-found it cannot know")
    elif initial_exact:
        if outcome_status != "warning":
            violations.append(
                "a found item was not reported as a not-refreshed warning")
        if outcome_target != "exact-album":
            violations.append("the found item's id was not the outcome target")
        if "NOT refreshed" not in outcome_detail:
            violations.append(
                "the found item's detail did not state it was not refreshed")
    else:
        if outcome_status != "skipped":
            violations.append("a clean not-found was not reported as skipped")

    return violations


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

    @example(mode="exact_found")
    @example(mode="initial_absent")
    @example(mode="initial_lookup_error")
    @given(
        mode=st.sampled_from((
            "initial_absent",
            "exact_found",
            "initial_lookup_error",
        )),
    )
    def test_jellyfin_report_law_holds_and_is_lane_independent(
        self,
        mode: str,
    ) -> None:
        """The report law holds for every world, and the Jellyfin outcome is
        IDENTICAL across the two calling lanes (``allow_escalation``
        True/False) — the flag governs only the Plex escalation (issue
        #1221 item 1)."""
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            exact = JellyfinAlbumRef("exact-album", "date")

            def find(_cfg: CratediggerConfig, _path: str):
                if mode == "initial_lookup_error":
                    raise RuntimeError("generated initial lookup failure")
                return exact if mode == "exact_found" else None

            per_lane: list[DeleteNotification] = []
            previous_disable = logging.root.manager.disable
            try:
                logging.disable(logging.CRITICAL)
                for allow_escalation in (True, False):
                    outcomes = notify_library_delete(
                        _notifier_config(raw, plex=False, jellyfin=True),
                        str(former),
                        allow_escalation=allow_escalation,
                        jellyfin_find_fn=find,
                    )
                    per_lane.append(next(
                        item for item in outcomes
                        if item.provider == "jellyfin"
                    ))
            except Exception as exc:
                raise AssertionError(
                    "notifier failure escaped the best-effort boundary: "
                    f"{type(exc).__name__}: {exc}") from exc
            finally:
                logging.disable(previous_disable)

            violations: list[str] = []
            for jellyfin in per_lane:
                violations.extend(jellyfin_report_law_violations(
                    initial_exact=mode == "exact_found",
                    lookup_failed=mode == "initial_lookup_error",
                    outcome_status=jellyfin.status,
                    outcome_target=jellyfin.target,
                    outcome_detail=jellyfin.detail,
                    raised=False,
                ))
            if per_lane[0] != per_lane[1]:
                violations.append(
                    "the Jellyfin outcome depended on allow_escalation: "
                    f"{per_lane[0]!r} != {per_lane[1]!r}")
            if violations:
                raise AssertionError(f"{'; '.join(violations)} (mode={mode})")
            if mode == "initial_lookup_error":
                self.assertIn("RuntimeError", per_lane[0].detail)


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

    def test_jellyfin_checker_trips_each_clause_on_its_minimal_world(
        self,
    ) -> None:
        """Per-clause proof Q1 (code-quality.md): each clause trips on the
        minimal world that makes ITS condition true while every other
        clause stays clean, and the asserted message is that clause's own."""
        clean_found = {
            "initial_exact": True, "lookup_failed": False,
            "outcome_status": "warning", "outcome_target": "exact-album",
            "outcome_detail": "exact album item exact-album ... NOT refreshed",
            "raised": False,
        }
        clause_worlds = {
            "notifier failure escaped the best-effort boundary": {
                **clean_found, "raised": True,
            },
            "the Jellyfin leg claimed a submission": {
                **clean_found, "outcome_status": "submitted",
            },
            "a lookup failure was not surfaced as a warning": {
                "initial_exact": False, "lookup_failed": True,
                "outcome_status": "skipped", "outcome_target": "",
                "outcome_detail": "identity lookup failed: RuntimeError: x",
                "raised": False,
            },
            "a lookup failure was not named in the detail": {
                "initial_exact": False, "lookup_failed": True,
                "outcome_status": "warning", "outcome_target": "",
                "outcome_detail": "something else entirely",
                "raised": False,
            },
            "a lookup failure claimed a not-found it cannot know": {
                "initial_exact": False, "lookup_failed": True,
                "outcome_status": "warning", "outcome_target": "",
                "outcome_detail": (
                    "no Jellyfin item found by former path; "
                    "identity lookup failed: RuntimeError: x"),
                "raised": False,
            },
            "a found item was not reported as a not-refreshed warning": {
                **clean_found, "outcome_status": "skipped",
            },
            "the found item's id was not the outcome target": {
                **clean_found, "outcome_target": "library-root",
            },
            "the found item's detail did not state it was not refreshed": {
                **clean_found,
                "outcome_detail": "exact album item exact-album refreshed",
            },
            "a clean not-found was not reported as skipped": {
                "initial_exact": False, "lookup_failed": False,
                "outcome_status": "warning", "outcome_target": "",
                "outcome_detail": "no Jellyfin item found by former path",
                "raised": False,
            },
        }
        for message, world in clause_worlds.items():
            with self.subTest(clause=message):
                violations = jellyfin_report_law_violations(
                    initial_exact=bool(world["initial_exact"]),
                    lookup_failed=bool(world["lookup_failed"]),
                    outcome_status=str(world["outcome_status"]),
                    outcome_target=str(world["outcome_target"]),
                    outcome_detail=str(world["outcome_detail"]),
                    raised=bool(world["raised"]),
                )
                self.assertTrue(
                    any(message in v for v in violations), violations)

    def test_jellyfin_checker_accepts_clean_worlds(self) -> None:
        for name, world in {
            "found": {
                "initial_exact": True, "lookup_failed": False,
                "outcome_status": "warning", "outcome_target": "exact-album",
                "outcome_detail": "item exact-album ... NOT refreshed",
                "raised": False,
            },
            "absent": {
                "initial_exact": False, "lookup_failed": False,
                "outcome_status": "skipped", "outcome_target": "",
                "outcome_detail": "no Jellyfin item found by former path",
                "raised": False,
            },
            "lookup_failed": {
                "initial_exact": False, "lookup_failed": True,
                "outcome_status": "warning", "outcome_target": "",
                "outcome_detail": "identity lookup failed: RuntimeError: x",
                "raised": False,
            },
        }.items():
            with self.subTest(world=name):
                self.assertEqual(
                    jellyfin_report_law_violations(
                        initial_exact=bool(world["initial_exact"]),
                        lookup_failed=bool(world["lookup_failed"]),
                        outcome_status=str(world["outcome_status"]),
                        outcome_target=str(world["outcome_target"]),
                        outcome_detail=str(world["outcome_detail"]),
                        raised=bool(world["raised"]),
                    ),
                    [])


if __name__ == "__main__":
    unittest.main()
