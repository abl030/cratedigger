"""Generated law for issue #1203 item 2 — post-import vanished-path
reconciliation.

Invariant under test: after a successful import that triggers notifiers,
every album directory Beets previously held for that request's release
identity, and no longer holds (the ``vanished_snapshot_paths`` primary,
authoritative source — a Beets before/after directory-set diff), UNIONED
with every replaced album path (``postflight.replaced_albums``, a secondary
source) whose normalized form differs from the imported path, is reconciled
with both media servers EXACTLY ONCE, and never by escalating to a Plex
library-root scan or calling the Jellyfin refresh endpoint AT ALL (a
source-level finding against Jellyfin 10.11: a targeted refresh cannot reap
a vanished item and would instead delete its child rows — the reconciler
only ever finds and reports on Jellyfin, never refreshes).

This composes the REAL production gating function
(``lib.dispatch.core._paths_needing_media_server_reconciliation``, exercised
indirectly through ``_reconcile_vanished_replaced_album_paths``) with the
REAL ``notify_library_delete`` over a real temporary directory tree — only
the Plex/Jellyfin HTTP leaf functions are faked. The deterministic wiring pin
(exact reconciled set, ordering against the pin capture, escalation refusal
at the seam level, the regression pin for the measured live defect) lives in
``tests.test_import_dispatch.TestVanishedPathReconciliation``; the
deterministic mechanics — including the Jellyfin found-item detect-and-report
path (never a refresh call) this file's found-item strategy widening exists
to reach structurally — live in ``tests.test_library_delete_notifiers``.
This file patrols the domain: many shapes of (imported_path, snapshot-diff
paths, replaced paths, surviving-ancestor depth, Jellyfin item
found/not-found).
"""

from __future__ import annotations

import configparser
import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.config import CratediggerConfig
from lib.dispatch.core import _reconcile_vanished_replaced_album_paths
from lib.library_delete_notifiers import notify_library_delete
from lib.quality import DuplicateRemoveCandidate
from lib.util import JellyfinAlbumRef

_JELLYFIN_LIBRARY_ID = "COLLECTION-WIDE-SENTINEL"


def _norm(path: str) -> str:
    stripped = path.strip()
    return os.path.normpath(stripped) if stripped else ""


def _expected_reconciled_paths(
    imported_path: str, snapshot_paths: list[str], replaced_paths: list[str],
) -> list[str]:
    """Reference oracle for the union-then-gate contract. This is a
    line-for-line transliteration of
    ``lib.dispatch.core._paths_needing_media_server_reconciliation`` — NOT
    an independently-derived check — so it cannot catch a defect present in
    both. What DOES qualify it as a regression guard (proven by the mutant
    kill matrix): it is asserted against the OBSERVED reconciliation calls
    the REAL composed production code made, so a mutant that changes
    production's actual behavior without changing this transliteration
    still shows up as a mismatch between ``expected`` and ``reconciled``.
    Skip blank, skip paths matching the (normalized) imported path, dedupe
    by normalized form ACROSS BOTH sources, preserve first-occurrence order
    with snapshot paths first (mirroring production's source ordering)."""
    imported_norm = _norm(imported_path)
    out: list[str] = []
    seen: set[str] = set()
    for p in [*snapshot_paths, *replaced_paths]:
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

    # "Exactly once": a set comparison alone is blind to a path reconciled
    # TWICE (e.g. deleting the gating function's own dedupe clause) — a
    # duplicate call still lands in the same set. Count normalized
    # occurrences directly.
    reconciled_norm_counts = Counter(
        norm for p in reconciled_paths if (norm := _norm(p))
    )
    duplicated = {p: c for p, c in reconciled_norm_counts.items() if c > 1}
    if duplicated:
        violations.append(
            "a path was reconciled more than once (must be reconciled "
            f"exactly once): {duplicated}")

    normalized_root = os.path.normpath(plex_root)
    if any(os.path.normpath(t) == normalized_root for t in plex_scan_targets):
        violations.append(
            "reconciliation escalated to a Plex library-root scan")

    # allow_escalation=False means the Jellyfin refresh endpoint is never
    # called at all -- not a targeted refresh, not the collection-wide
    # cfg.jellyfin_library_id fallback. jellyfin_library_id is retained as a
    # parameter (and a distinguishing sentinel) so a regression that DOES
    # call it, in either shape, is still nameable in the failure message.
    if jellyfin_refresh_item_ids:
        escalated = jellyfin_library_id in jellyfin_refresh_item_ids
        violations.append(
            "reconciliation called the Jellyfin refresh endpoint at all "
            f"(escalated to the collection-wide fallback: {escalated}): "
            f"{jellyfin_refresh_item_ids}")

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

# Shapes for the PRIMARY source: paths the Beets before/after snapshot diff
# reports vanished. "same_as_replaced" specifically exercises cross-source
# dedupe (a path both the snapshot diff AND replaced_albums name must still
# be reconciled exactly once).
SNAPSHOT_PATH_SHAPES = st.sampled_from((
    "distinct",             # a genuinely different vanished snapshot path
    "same_as_replaced",     # overlaps the replaced_albums primary path
    "same_as_imported",     # identical to the new path -- must be skipped
    "blank",                # "" -- must be skipped
))


class TestVanishedPathReconciliationGeneratedLaw(unittest.TestCase):
    @given(
        artist=SAFE_COMPONENT,
        album_new=SAFE_COMPONENT,
        album_old=SAFE_COMPONENT,
        artist_dir_survives=st.booleans(),
        extra_shapes=st.lists(REPLACED_PATH_SHAPES, max_size=4),
        snapshot_shapes=st.lists(SNAPSHOT_PATH_SHAPES, max_size=3),
        jellyfin_item_found=st.booleans(),
    )
    def test_reconciliation_law_holds(
        self,
        artist: str,
        album_new: str,
        album_old: str,
        artist_dir_survives: bool,
        extra_shapes: list[str],
        snapshot_shapes: list[str],
        jellyfin_item_found: bool,
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

            snapshot_paths: list[str] = []
            for shape in snapshot_shapes:
                if shape == "distinct":
                    snapshot_paths.append(
                        str(root / artist / (album_old + "SNAP")))
                elif shape == "same_as_replaced":
                    snapshot_paths.append(primary_old_path)
                elif shape == "same_as_imported":
                    snapshot_paths.append(imported_path)
                elif shape == "blank":
                    snapshot_paths.append("")

            cfg = _cfg(str(root))

            reconciled_paths: list[str] = []
            plex_scan_targets: list[str] = []
            jellyfin_refresh_item_ids: list[str | None] = []

            def _jellyfin_find(_c, _p):
                # Widened per #1203 item 2 review: a generated world must
                # sometimes exercise the FOUND-item detect-and-report branch
                # (never a refresh call, either way -- see
                # notify_library_delete's own docstring), not only the "no
                # item found at all" branch.
                if jellyfin_item_found:
                    return JellyfinAlbumRef("found-item", "date")
                return None

            def _notify(cfg, path, *, allow_escalation):
                reconciled_paths.append(path)
                return notify_library_delete(
                    cfg, path, allow_escalation=allow_escalation,
                    plex_find_fn=lambda _c, _p: None,
                    plex_scan_fn=lambda _c, p: (
                        plex_scan_targets.append(p) or (200, p)),
                    jellyfin_find_fn=_jellyfin_find,
                    jellyfin_refresh_fn=(
                        lambda _c, item_id=None: (
                            jellyfin_refresh_item_ids.append(item_id) or (
                                204,
                                f"/Items/{item_id or 'library'}/Refresh"))),
                )

            _reconcile_vanished_replaced_album_paths(
                cfg,
                imported_path=imported_path,
                replaced_albums=[
                    DuplicateRemoveCandidate(album_path=p)
                    for p in replaced_paths
                ],
                vanished_snapshot_paths=snapshot_paths,
                notify_fn=_notify,
            )

            expected = _expected_reconciled_paths(
                imported_path, snapshot_paths, replaced_paths)
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
                    f"snapshot_paths={snapshot_paths!r}, "
                    f"replaced_paths={replaced_paths!r}, "
                    f"artist_dir_survives={artist_dir_survives}, "
                    f"jellyfin_item_found={jellyfin_item_found})")


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

    def test_duplicate_reconciliation_clause_trips(self) -> None:
        """"Exactly once": a path reconciled TWICE must trip even though
        the set of reconciled paths matches the set of expected paths
        exactly (issue #1203 item 2 review — deleting the gating
        function's own ``in seen`` dedupe clause is invisible to a
        set-only comparison)."""
        violations = _reconciliation_law_violations(
            expected_paths=["/root/Artist/Old"],
            reconciled_paths=["/root/Artist/Old", "/root/Artist/Old/"],
            plex_root="/root",
            plex_scan_targets=[],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=[],
        )
        self.assertTrue(
            any("more than once" in v for v in violations), violations,
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

    def test_jellyfin_any_refresh_call_clause_trips(self) -> None:
        """A targeted refresh call is JUST as much a violation as the
        collection-wide fallback (issue #1203 item 2 review: Jellyfin
        cannot reap a vanished item via ANY refresh call, targeted or not,
        and a targeted one would empty the item's own child rows)."""
        violations = _reconciliation_law_violations(
            expected_paths=["/root/Artist/Old"],
            reconciled_paths=["/root/Artist/Old"],
            plex_root="/root",
            plex_scan_targets=[],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=["exact-album"],
        )
        self.assertTrue(
            any("called the Jellyfin refresh endpoint" in v
                for v in violations),
            violations,
        )

    def test_jellyfin_collection_wide_refresh_call_clause_trips(self) -> None:
        violations = _reconciliation_law_violations(
            expected_paths=["/root/Artist/Old"],
            reconciled_paths=["/root/Artist/Old"],
            plex_root="/root",
            plex_scan_targets=[],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=["lib-id"],
        )
        self.assertTrue(
            any("called the Jellyfin refresh endpoint" in v
                and "escalated to the collection-wide fallback: True" in v
                for v in violations),
            violations,
        )

    def test_clean_world_produces_no_violations(self) -> None:
        violations = _reconciliation_law_violations(
            expected_paths=["/root/Artist/Old"],
            reconciled_paths=["/root/Artist/Old"],
            plex_root="/root",
            plex_scan_targets=["/root/Artist"],
            jellyfin_library_id="lib-id",
            jellyfin_refresh_item_ids=[],
        )
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
