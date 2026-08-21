"""Generated production-seam invariants for the #1149 classifier.

Each property drives ``classify_album``; the checker is deliberately outside
the classifier so its named violations can be independently self-tested.
"""
from __future__ import annotations

import unittest

from beetsplug.discogs import DiscogsPlugin
from beetsplug.discogs.types import Track
from hypothesis import assume, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.library_completeness import (
    AudioTagReadError,
    CatalogItem,
    LibraryAlbum,
    classify_album,
    discogs_manifest,
    musicbrainz_manifest,
)
from lib.release_identity import ReleaseIdentity


def completeness_invariant_violations(
    kinds: set[str], *, expect_drift: bool, expect_missing: bool,
    expect_video_ignored: bool, expect_unknown: bool,
) -> list[str]:
    """Independent checker clauses exercised by properties and self-tests."""
    violations: list[str] = []
    if ("catalog_drift" in kinds) != expect_drift:
        violations.append("catalog drift must be symmetric between catalog and disk")
    if ("missing_source_audio" in kinds) != expect_missing:
        violations.append("missing source audio must require exact readable evidence")
    if expect_video_ignored and kinds:
        violations.append("video omission must not create a completeness finding")
    if expect_unknown and ("unknown" not in kinds or "missing_source_audio" in kinds):
        violations.append("unreadable extra audio must fail closed as unknown")
    return violations


def _discogs_world(positions: tuple[str, ...], *, catalog_positions: tuple[str, ...], physical_positions: tuple[str, ...], unreadable_extra: bool = False):
    release = "1"
    manifest = discogs_manifest(release, {
        "id": release,
        "tracks": [{"position": p, "title": p} for p in positions],
    })
    catalog = tuple(CatalogItem(f"/album/{p}.flac", f"{release}-{p}", "") for p in catalog_positions)
    album = LibraryAlbum(1, "a", "b", ReleaseIdentity("discogs", release), "/album", catalog)
    physical = tuple(f"/album/{p}.flac" for p in physical_positions)
    def tags(path: str) -> tuple[str, str]:
        if unreadable_extra and path.endswith("extra.flac"):
            raise AudioTagReadError("unreadable")
        return (f"{release}-{path.rsplit('/', 1)[-1].removesuffix('.flac')}", "")
    return classify_album(album, manifest, enumerate_files=lambda _: physical, tag_reader=tags)


#: Letters only, deliberately -- Discogs' own ``get_track_index`` parser
#: only assigns a subtrack index (and so groups) a position that contains a
#: DIGIT (either "16.1"-style or "1A"-style). A digit-bearing alphabet can
#: generate two ADJACENT distinct strings ("1A","1B") that Beets' own
#: coalescing would still merge into one physical component, breaking these
#: properties' 1:1 position<->component assumption. Coalescing has its own
#: dedicated pin + property (``tests/test_library_completeness.py``); these
#: four exercise identity-matching, independent of grouping.
_UNGROUPABLE_POSITION_ALPHABET = "ABC"


@given(st.lists(st.text(alphabet=_UNGROUPABLE_POSITION_ALPHABET, min_size=1, max_size=4), min_size=2, max_size=8, unique=True))
def _property_catalog_disk_symmetry(positions: list[str]) -> None:
    all_positions = tuple(positions)
    complete = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions)
    assert not completeness_invariant_violations({f.kind for f in complete.findings}, expect_drift=False, expect_missing=False, expect_video_ignored=False, expect_unknown=False)
    disk_missing = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions[1:])
    assert not completeness_invariant_violations({f.kind for f in disk_missing.findings}, expect_drift=True, expect_missing=True, expect_video_ignored=False, expect_unknown=False)
    extra = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions + ("extra",))
    assert not completeness_invariant_violations({f.kind for f in extra.findings}, expect_drift=True, expect_missing=False, expect_video_ignored=False, expect_unknown=False)


@given(st.lists(st.text(alphabet=_UNGROUPABLE_POSITION_ALPHABET, min_size=1, max_size=4), min_size=2, max_size=7, unique=True))
def _property_nonexclusive_missing_and_drift(positions: list[str]) -> None:
    all_positions = tuple(positions)
    # First is physically present but deliberately untracked; second is absent.
    result = _discogs_world(all_positions, catalog_positions=all_positions[2:], physical_positions=(all_positions[0],) + all_positions[2:])
    kinds = {f.kind for f in result.findings}
    assert not completeness_invariant_violations(kinds, expect_drift=True, expect_missing=True, expect_video_ignored=False, expect_unknown=False)
    assert {"catalog_drift", "missing_source_audio"} <= kinds


@given(st.text(alphabet="ABC123", min_size=1, max_size=4))
def _property_video_never_means_missing_audio(token: str) -> None:
    raw = {"id": "release", "media": [{"tracks": [
        {"id": "audio", "title": "Audio", "recording": {"id": "audio-rec", "video": False}},
        {"id": f"video-{token}", "title": "Video", "recording": {"id": "video-rec", "video": True}},
    ]}]}
    album = LibraryAlbum(1, "a", "b", ReleaseIdentity("musicbrainz", "release"), "/album", (CatalogItem("/album/a.flac", "audio", "audio-rec"),))
    result = classify_album(album, musicbrainz_manifest("release", raw), enumerate_files=lambda _: ("/album/a.flac",), tag_reader=lambda _: ("", ""))
    assert not completeness_invariant_violations({f.kind for f in result.findings}, expect_drift=False, expect_missing=False, expect_video_ignored=True, expect_unknown=False)


@given(st.lists(st.text(alphabet=_UNGROUPABLE_POSITION_ALPHABET, min_size=1, max_size=4), min_size=1, max_size=6, unique=True))
def _property_unreadable_extra_is_unknown_not_missing(positions: list[str]) -> None:
    result = _discogs_world(tuple(positions), catalog_positions=(), physical_positions=("extra",), unreadable_extra=True)
    assert not completeness_invariant_violations({f.kind for f in result.findings}, expect_drift=True, expect_missing=False, expect_video_ignored=False, expect_unknown=True)


def _beets_oracle_groups(raw_positions: list[str]) -> list[tuple[str, str]]:
    """Ground truth for issue #1237's coalescing: run the REAL Beets
    Discogs plugin's own ``_coalesce_tracks`` (the same "modern" cohort
    ``harness/beets_compat.py`` targets) and read off ``(position, title)``
    for each resulting physical track. ``object.__new__`` bypasses
    ``DiscogsPlugin.__init__`` (network/config setup this call never
    needs) -- the same construction ``tests/test_discogs_subtracks_e2e.py``
    already uses as its own candidate-shim oracle.
    """
    plugin = object.__new__(DiscogsPlugin)
    raw: list[Track] = [
        {"type_": "track", "position": position, "title": position, "duration": "0:01"}
        for position in raw_positions
    ]
    coalesced = plugin._coalesce_tracks(raw)
    return [(track["position"], track["title"]) for track in coalesced]


@st.composite
def _discogs_position_sequence(draw: st.DrawFn) -> list[str]:
    """A realistic raw Discogs position sequence: a mix of plain (never-
    group) positions and dotted subtrack FAMILIES (2-4 consecutive
    sub-positions sharing one physical prefix), each family's own prefix
    kept distinct so cross-family adjacency never accidentally merges.
    """
    group_count = draw(st.integers(min_value=1, max_value=6))
    used_prefixes: set[str] = set()
    used_plain: set[str] = set()
    positions: list[str] = []
    for _ in range(group_count):
        if draw(st.booleans()):
            letter = draw(st.sampled_from("ABCDEFGH"))
            number = draw(st.integers(min_value=1, max_value=99))
            plain = f"{letter}{number}"
            assume(plain not in used_plain)
            used_plain.add(plain)
            positions.append(plain)
        else:
            prefix = draw(st.integers(min_value=1, max_value=99))
            assume(str(prefix) not in used_prefixes)
            used_prefixes.add(str(prefix))
            size = draw(st.integers(min_value=2, max_value=4))
            positions.extend(f"{prefix}.{index}" for index in range(1, size + 1))
    return positions


@given(_discogs_position_sequence())
def _property_discogs_manifest_agrees_with_beets_oracle(raw_positions: list[str]) -> None:
    """Issue #1237: ``discogs_manifest``'s component set must match what
    Beets itself would catalogue -- the real adapter (``beetsplug.discogs
    .DiscogsPlugin``) is the oracle, not a second hand-written
    reimplementation of the same regex.
    """
    release = "1"
    manifest = discogs_manifest(release, {
        "id": release,
        "tracks": [{"position": p, "title": p} for p in raw_positions],
    })
    oracle = _beets_oracle_groups(raw_positions)
    assert [component.key for component in manifest.components] == [
        f"{release}-{position}" for position, _ in oracle
    ]
    assert [component.title for component in manifest.components] == [
        title for _, title in oracle
    ]


class TestDiscogsGroupingOracleGenerated(unittest.TestCase):
    def test_discogs_manifest_agrees_with_beets_oracle(self) -> None:
        _property_discogs_manifest_agrees_with_beets_oracle()


class TestLibraryCompletenessGenerated(unittest.TestCase):
    def test_catalog_disk_symmetry(self) -> None:
        _property_catalog_disk_symmetry()

    def test_nonexclusive_missing_and_drift(self) -> None:
        _property_nonexclusive_missing_and_drift()

    def test_video_omission_never_means_audio_missing(self) -> None:
        _property_video_never_means_missing_audio()

    def test_unreadable_extra_fails_closed(self) -> None:
        _property_unreadable_extra_is_unknown_not_missing()


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Named known-bad self-tests, one per checker clause."""
    def test_catalog_drift_clause(self) -> None:
        self.assertIn("catalog drift must be symmetric between catalog and disk", completeness_invariant_violations(set(), expect_drift=True, expect_missing=False, expect_video_ignored=False, expect_unknown=False))

    def test_missing_exactness_clause(self) -> None:
        self.assertIn("missing source audio must require exact readable evidence", completeness_invariant_violations(set(), expect_drift=False, expect_missing=True, expect_video_ignored=False, expect_unknown=False))

    def test_video_clause(self) -> None:
        self.assertIn("video omission must not create a completeness finding", completeness_invariant_violations({"missing_source_audio"}, expect_drift=False, expect_missing=False, expect_video_ignored=True, expect_unknown=False))

    def test_unknown_clause(self) -> None:
        self.assertIn("unreadable extra audio must fail closed as unknown", completeness_invariant_violations({"missing_source_audio"}, expect_drift=False, expect_missing=False, expect_video_ignored=False, expect_unknown=True))


if __name__ == "__main__":
    unittest.main()
