"""Generated production-seam invariants for the #1149 classifier.

Each property drives ``classify_album``; the checker is deliberately outside
the classifier so its named violations can be independently self-tested.
"""
from __future__ import annotations

import unittest

from hypothesis import given
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
    expect_video_non_audio: bool, expect_unknown: bool,
) -> list[str]:
    """Independent checker clauses exercised by properties and self-tests."""
    violations: list[str] = []
    if ("catalog_drift" in kinds) != expect_drift:
        violations.append("catalog drift must be symmetric between catalog and disk")
    if ("missing_source_audio" in kinds) != expect_missing:
        violations.append("missing source audio must require exact readable evidence")
    if expect_video_non_audio and ("non_audio_omitted" not in kinds or "missing_source_audio" in kinds):
        violations.append("video omission must remain non-audio, never missing audio")
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


@given(st.lists(st.text(alphabet="ABC123", min_size=1, max_size=4), min_size=2, max_size=8, unique=True))
def _property_catalog_disk_symmetry(positions: list[str]) -> None:
    all_positions = tuple(positions)
    complete = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions)
    assert not completeness_invariant_violations({f.kind for f in complete.findings}, expect_drift=False, expect_missing=False, expect_video_non_audio=False, expect_unknown=False)
    disk_missing = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions[1:])
    assert not completeness_invariant_violations({f.kind for f in disk_missing.findings}, expect_drift=True, expect_missing=True, expect_video_non_audio=False, expect_unknown=False)
    extra = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions + ("extra",))
    assert not completeness_invariant_violations({f.kind for f in extra.findings}, expect_drift=True, expect_missing=False, expect_video_non_audio=False, expect_unknown=False)


@given(st.lists(st.text(alphabet="ABC123", min_size=1, max_size=4), min_size=2, max_size=7, unique=True))
def _property_nonexclusive_missing_and_drift(positions: list[str]) -> None:
    all_positions = tuple(positions)
    # First is physically present but deliberately untracked; second is absent.
    result = _discogs_world(all_positions, catalog_positions=all_positions[2:], physical_positions=(all_positions[0],) + all_positions[2:])
    kinds = {f.kind for f in result.findings}
    assert not completeness_invariant_violations(kinds, expect_drift=True, expect_missing=True, expect_video_non_audio=False, expect_unknown=False)
    assert {"catalog_drift", "missing_source_audio"} <= kinds


@given(st.text(alphabet="ABC123", min_size=1, max_size=4))
def _property_video_never_means_missing_audio(token: str) -> None:
    raw = {"id": "release", "media": [{"tracks": [
        {"id": "audio", "title": "Audio", "recording": {"id": "audio-rec", "video": False}},
        {"id": f"video-{token}", "title": "Video", "recording": {"id": "video-rec", "video": True}},
    ]}]}
    album = LibraryAlbum(1, "a", "b", ReleaseIdentity("musicbrainz", "release"), "/album", (CatalogItem("/album/a.flac", "audio", "audio-rec"),))
    result = classify_album(album, musicbrainz_manifest("release", raw), enumerate_files=lambda _: ("/album/a.flac",), tag_reader=lambda _: ("", ""))
    assert not completeness_invariant_violations({f.kind for f in result.findings}, expect_drift=False, expect_missing=False, expect_video_non_audio=True, expect_unknown=False)


@given(st.lists(st.text(alphabet="ABC123", min_size=1, max_size=4), min_size=1, max_size=6, unique=True))
def _property_unreadable_extra_is_unknown_not_missing(positions: list[str]) -> None:
    result = _discogs_world(tuple(positions), catalog_positions=(), physical_positions=("extra",), unreadable_extra=True)
    assert not completeness_invariant_violations({f.kind for f in result.findings}, expect_drift=True, expect_missing=False, expect_video_non_audio=False, expect_unknown=True)


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
        self.assertIn("catalog drift must be symmetric between catalog and disk", completeness_invariant_violations(set(), expect_drift=True, expect_missing=False, expect_video_non_audio=False, expect_unknown=False))

    def test_missing_exactness_clause(self) -> None:
        self.assertIn("missing source audio must require exact readable evidence", completeness_invariant_violations(set(), expect_drift=False, expect_missing=True, expect_video_non_audio=False, expect_unknown=False))

    def test_video_clause(self) -> None:
        self.assertIn("video omission must remain non-audio, never missing audio", completeness_invariant_violations({"missing_source_audio"}, expect_drift=False, expect_missing=False, expect_video_non_audio=True, expect_unknown=False))

    def test_unknown_clause(self) -> None:
        self.assertIn("unreadable extra audio must fail closed as unknown", completeness_invariant_violations({"missing_source_audio"}, expect_drift=False, expect_missing=False, expect_video_non_audio=False, expect_unknown=True))


if __name__ == "__main__":
    unittest.main()
