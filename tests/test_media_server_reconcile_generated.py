"""Generated law for issue #1203 item 2 — post-import vanished-path
reconciliation.

Invariant under test: after a path-changing import, every replaced album
path (``postflight.replaced_albums``) whose normalized form differs from the
imported path is reconciled with both media servers exactly once, and never
by escalating to a Plex library-root scan or a Jellyfin collection-wide
refresh.

This composes the REAL production gating function
(``lib.dispatch.core._paths_needing_media_server_reconciliation``, exercised
indirectly through ``_reconcile_vanished_replaced_album_paths``) with the
REAL ``notify_library_delete`` over a real temporary directory tree — only
the Plex/Jellyfin HTTP leaf functions are faked. The deterministic wiring pin
(exact reconciled set, ordering against the pin capture, escalation refusal
at the seam level) lives in
``tests.test_import_dispatch.TestVanishedPathReconciliation``; the
deterministic escalation-refusal mechanics live in
``tests.test_library_delete_notifiers``. This file patrols the domain: many
shapes of (imported_path, replaced paths, surviving-ancestor depth).
"""

from __future__ import annotations

import configparser
import os
import tempfile
import unittest
from pathlib import Path

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.config import CratediggerConfig
from lib.dispatch.core import _reconcile_vanished_replaced_album_paths
from lib.library_delete_notifiers import notify_library_delete
from lib.quality import DuplicateRemoveCandidate

_JELLYFIN_LIBRARY_ID = "COLLECTION-WIDE-SENTINEL"


def _norm(path: str) -> str:
    stripped = path.strip()
    return os.path.normpath(stripped) if stripped else ""


def _expected_reconciled_paths(
    imported_path: str, replaced_paths: list[str],
) -> list[str]:
    """Independent reference oracle — does NOT call the production gating
    function under test. Mirrors the stated contract in plain terms: skip
    blank, skip paths matching the (normalized) imported path, dedupe by
    normalized form, preserve first-occurrence order."""
    imported_norm = _norm(imported_path)
    out: list[str] = []
    seen: set[str] = set()
    for p in replaced_paths:
        norm = _norm(p)
        if not norm or norm == imported_norm or norm in seen:
            continue
        seen.add(norm)
        out.append(p)
    return out


def _reconciliation_law_violations(
    *,
    expected_paths: list[str],
    reconciled_paths: list[str],
    plex_root: str,
    plex_scan_targets: list[str],
    jellyfin_library_id: str | None,
    jellyfin_refresh_item_ids: list[str | None],
) -> list[str]:
    """Every way an observed reconciliation run breaks the law. Accumulating
    — every clause is evaluated regardless of earlier results, so ordering
    cannot mask one clause behind another (code-quality.md "New checkers
    prefer an accumulating list[str]")."""
    violations: list[str] = []

    expected_set = {_norm(p) for p in expected_paths if _norm(p)}
    reconciled_set = {_norm(p) for p in reconciled_paths if _norm(p)}

    missing = expected_set - reconciled_set
    if missing:
        violations.append(
            "paths that should have been reconciled were skipped: "
            f"{sorted(missing)}")

    extra = reconciled_set - expected_set
    if extra:
        violations.append(
            "paths that should NOT have been reconciled were sent: "
            f"{sorted(extra)}")

    normalized_root = os.path.normpath(plex_root)
    if any(os.path.normpath(t) == normalized_root for t in plex_scan_targets):
        violations.append(
            "reconciliation escalated to a Plex library-root scan")

    if jellyfin_library_id in jellyfin_refresh_item_ids:
        violations.append(
            "reconciliation escalated to a Jellyfin collection-wide refresh")

    return violations


def _cfg(root: str) -> CratediggerConfig:
    parser = configparser.RawConfigParser()
    parser.read_dict({
        "Beets": {"directory": root},
        "Plex": {
            "url": "http://plex",
            "token": "plex-token",
            "library_section_id": "3",
            "path_map": f"{root}:/plex-music",
        },
        "Jellyfin": {
            "url": "http://jellyfin",
            "token": "jellyfin-token",
            "library_id": _JELLYFIN_LIBRARY_ID,
            "path_map": f"{root}:/jellyfin-music",
        },
    })
    return CratediggerConfig.from_ini(parser)


SAFE_COMPONENT = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=1, max_size=10,
)

# Shapes for the extra replaced-album paths beyond the always-present
# "primary" old path. Each names a WORLD, not a raw string, so the
# generated tree stays a coherent filesystem the Plex ancestor walk can
# actually reason about.
REPLACED_PATH_SHAPES = st.sampled_from((
    "distinct",             # another genuinely different old path
    "same_as_imported",     # identical to the new path -- must be skipped
    "blank",                # "" -- must be skipped
    "whitespace_blank",     # "   " -- must be skipped after strip
    "trailing_slash_dup",   # primary path + "/" -- dedupes with primary
    "outside_root",         # outside the configured Beets root entirely
))


class TestVanishedPathReconciliationGeneratedLaw(unittest.TestCase):
    @given(
        artist=SAFE_COMPONENT,
        album_new=SAFE_COMPONENT,
        album_old=SAFE_COMPONENT,
        artist_dir_survives=st.booleans(),
        extra_shapes=st.lists(REPLACED_PATH_SHAPES, max_size=4),
    )
    def test_reconciliation_law_holds(
        self,
        artist: str,
        album_new: str,
        album_old: str,
        artist_dir_survives: bool,
        extra_shapes: list[str],
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "library"
            root.mkdir()
            artist_dir = root / artist
            if artist_dir_survives:
                artist_dir.mkdir()

            imported_path = str(root / artist / album_new)
            primary_old_path = str(root / artist / album_old)

            replaced_paths = [primary_old_path]
            for shape in extra_shapes:
                if shape == "distinct":
                    replaced_paths.append(str(root / artist / (album_old + "X")))
                elif shape == "same_as_imported":
                    replaced_paths.append(imported_path)
                elif shape == "blank":
                    replaced_paths.append("")
                elif shape == "whitespace_blank":
                    replaced_paths.append("   ")
                elif shape == "trailing_slash_dup":
                    replaced_paths.append(primary_old_path + "/")
                elif shape == "outside_root":
                    replaced_paths.append(str(Path(raw) / "outside" / "Album"))

            cfg = _cfg(str(root))

            reconciled_paths: list[str] = []
            plex_scan_targets: list[str] = []
            jellyfin_refresh_item_ids: list[str | None] = []

            def _notify(cfg, path, *, allow_escalation):
                reconciled_paths.append(path)
                return notify_library_delete(
                    cfg, path, allow_escalation=allow_escalation,
                    plex_find_fn=lambda _c, _p: None,
                    plex_scan_fn=lambda _c, p: (
                        plex_scan_targets.append(p) or (200, p)),
                    jellyfin_find_fn=lambda _c, _p: None,
                    jellyfin_refresh_fn=lambda _c, item_id=None: (
                        jellyfin_refresh_item_ids.append(item_id) or (
                            204, f"/Items/{item_id or 'library'}/Refresh")),
                )

            _reconcile_vanished_replaced_album_paths(
                cfg,
                imported_path=imported_path,
                replaced_albums=[
                    DuplicateRemoveCandidate(album_path=p)
                    for p in replaced_paths
                ],
                notify_fn=_notify,
            )

            expected = _expected_reconciled_paths(imported_path, replaced_paths)
            violations = _reconciliation_law_violations(
                expected_paths=expected,
                reconciled_paths=reconciled_paths,
                plex_root=str(root),
                plex_scan_targets=plex_scan_targets,
                jellyfin_library_id=cfg.jellyfin_library_id,
                jellyfin_refresh_item_ids=jellyfin_refresh_item_ids,
            )
            if violations:
                raise AssertionError(
                    f"{'; '.join(violations)} "
                    f"(imported_path={imported_path!r}, "
                    f"replaced_paths={replaced_paths!r}, "
                    f"artist_dir_survives={artist_dir_survives})")


class TestReconciliationLawCheckerKnownBad(unittest.TestCase):
    """Per-clause proof (code-quality.md): each clause must trip on its own
    minimal world, with every earlier clause held clean."""

    def test_missing_reconciliation_clause_trips(self) -> None:
        violations = _reconciliation_law_violations(
            expected_paths=["/root/Artist/Old"],
            reconciled_paths=[],
            plex_root="/root",
            plex_scan_targets=[],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=[],
        )
        self.assertTrue(
            any("should have been reconciled were skipped" in v
                for v in violations),
            violations,
        )

    def test_extra_reconciliation_clause_trips(self) -> None:
        violations = _reconciliation_law_violations(
            expected_paths=[],
            reconciled_paths=["/root/Artist/Old"],
            plex_root="/root",
            plex_scan_targets=[],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=[],
        )
        self.assertTrue(
            any("should NOT have been reconciled were sent" in v
                for v in violations),
            violations,
        )

    def test_plex_root_escalation_clause_trips(self) -> None:
        violations = _reconciliation_law_violations(
            expected_paths=["/root/Artist/Old"],
            reconciled_paths=["/root/Artist/Old"],
            plex_root="/root",
            plex_scan_targets=["/root"],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=[],
        )
        self.assertTrue(
            any("Plex library-root scan" in v for v in violations), violations,
        )

    def test_jellyfin_collection_escalation_clause_trips(self) -> None:
        violations = _reconciliation_law_violations(
            expected_paths=["/root/Artist/Old"],
            reconciled_paths=["/root/Artist/Old"],
            plex_root="/root",
            plex_scan_targets=[],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=["lib-id"],
        )
        self.assertTrue(
            any("Jellyfin collection-wide refresh" in v for v in violations),
            violations,
        )

    def test_clean_world_produces_no_violations(self) -> None:
        violations = _reconciliation_law_violations(
            expected_paths=["/root/Artist/Old"],
            reconciled_paths=["/root/Artist/Old"],
            plex_root="/root",
            plex_scan_targets=["/root/Artist"],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=["exact-album"],
        )
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
