"""Generated production-seam invariants for the #1149 classifier.

Each property drives ``classify_album``; the checker is deliberately outside
the classifier so its named violations can be independently self-tested.
"""
from __future__ import annotations

import unittest

from beetsplug.discogs import DiscogsPlugin
from beetsplug.discogs.types import AudioTrack, Track
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


def _identity_only_gap_detector(_path: str) -> bool:
    """Fake ``detect_composite_gap`` for the identity-focused properties
    below (issue #1237 review C3).

    ``_discogs_world`` catalogues/physically-places ONE item per literal
    RAW position (matching what Beets' #1183 flat retry actually installs
    for a genuinely split composite -- issue #1237 review C1's subtraction
    then recognises a group as complete purely by identity, with NO audio
    decode, whenever every sub-position is separately present). The decode
    branch is reached ONLY when some sub-position of a coalesced group
    remains genuinely unaccounted for after that subtraction -- in every
    such world here, the correct answer is "no gap" (still missing),
    agreeing with what identity already knows. A real ffmpeg decode
    against these synthetic (nonexistent) paths would either spawn a real
    subprocess per Hypothesis example or fail closed to ``unknown``;
    returning ``False`` directly is what keeps these properties fast and
    correct without either.
    """
    return False


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
    return classify_album(
        album, manifest, enumerate_files=lambda _: physical, tag_reader=tags,
        detect_composite_gap=_identity_only_gap_detector,
    )


@given(st.lists(st.text(alphabet="ABC123", min_size=1, max_size=4), min_size=2, max_size=8, unique=True))
def _property_catalog_disk_symmetry(positions: list[str]) -> None:
    all_positions = tuple(positions)
    complete = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions)
    assert not completeness_invariant_violations({f.kind for f in complete.findings}, expect_drift=False, expect_missing=False, expect_video_ignored=False, expect_unknown=False)
    disk_missing = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions[1:])
    assert not completeness_invariant_violations({f.kind for f in disk_missing.findings}, expect_drift=True, expect_missing=True, expect_video_ignored=False, expect_unknown=False)
    extra = _discogs_world(all_positions, catalog_positions=all_positions, physical_positions=all_positions + ("extra",))
    assert not completeness_invariant_violations({f.kind for f in extra.findings}, expect_drift=True, expect_missing=False, expect_video_ignored=False, expect_unknown=False)


@given(st.lists(st.text(alphabet="ABC123", min_size=1, max_size=4), min_size=2, max_size=7, unique=True))
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


@given(st.lists(st.text(alphabet="ABC123", min_size=1, max_size=4), min_size=1, max_size=6, unique=True))
def _property_unreadable_extra_is_unknown_not_missing(positions: list[str]) -> None:
    result = _discogs_world(tuple(positions), catalog_positions=(), physical_positions=("extra",), unreadable_extra=True)
    assert not completeness_invariant_violations({f.kind for f in result.findings}, expect_drift=True, expect_missing=False, expect_video_ignored=False, expect_unknown=True)


def _to_beets_audio_track(item: object) -> AudioTrack:
    """Beets' leaf ``AudioTrack`` shape for one flat entry. ``item`` comes
    from our own untyped generated ``dict[str, object]`` entries (or the
    untyped children of a ``sub_tracks`` list), never itself nested.
    """
    assert isinstance(item, dict)
    return {
        "type_": "track",
        "position": str(item["position"]),
        "title": str(item.get("title", "")),
        "duration": "0:01",
    }


def _to_beets_shape(entries: list[dict[str, object]]) -> list[Track]:
    """Inject the ``type_`` key Beets' own ``Track`` TypedDict requires
    (absent from Cratedigger's own wire shape). ``IndexTrack.sub_tracks``
    is declared ``list[AudioTrack]`` (never a further-nested index) --
    matching both Beets' own real shape and this module's generator, which
    never nests a header inside a header (issue #1237 review C6/C7).
    """
    shaped: list[Track] = []
    for item in entries:
        sub_tracks = item.get("sub_tracks")
        if sub_tracks is not None:
            assert isinstance(sub_tracks, list)
            shaped.append({
                "type_": "index",
                "position": "",
                "title": str(item.get("title", "")),
                "duration": str(item.get("duration", "")),
                "sub_tracks": [_to_beets_audio_track(child) for child in sub_tracks],
            })
        else:
            shaped.append(_to_beets_audio_track(item))
    return shaped


def _beets_oracle_groups(entries: list[dict[str, object]]) -> list[tuple[str, str]]:
    """Ground truth for issue #1237's coalescing: run the REAL Beets
    Discogs plugin's own ``_coalesce_tracks`` (the same "modern" cohort
    ``harness/beets_compat.py`` targets) over Cratedigger's raw entries
    and read off ``(position, title)`` for each resulting physical track.
    ``object.__new__`` bypasses ``DiscogsPlugin.__init__`` (network/config
    setup this call never needs) -- the same construction
    ``tests/test_discogs_subtracks_e2e.py`` already uses as its own
    candidate-shim oracle. ``config["index_tracks"]`` is read by the real
    plugin's non-subindexed nested branch, so a minimal stand-in is
    supplied (default ``False``, matching the deployed plugin default).
    """
    plugin = object.__new__(DiscogsPlugin)
    setattr(plugin, "config", {"index_tracks": False})  # noqa: B010 - real config type is untyped confuse Subview
    coalesced = plugin._coalesce_tracks(_to_beets_shape(entries))
    return [(track["position"], track["title"]) for track in coalesced]


@st.composite
def _discogs_raw_entries(draw: st.DrawFn) -> list[dict[str, object]]:
    """A realistic raw Discogs track list in Cratedigger's OWN wire shape
    (no ``type_`` key -- ``discogs_manifest`` doesn't need it), covering:

    * plain positions that never carry a subtrack index (``A5``);
    * digit-then-letter positions that DO (``3B`` -- issue #1237 review C7,
      previously ungenerated);
    * dotted subtrack families (``16.1``/``16.2``/...);
    * nested ``sub_tracks`` headers, both subindexed (Beets' merge branch,
      issue #1237 review C6) and not (Beets' expand branch).

    Deliberately does NOT force every family's prefix to be globally
    unique (issue #1237 review C7): two atoms can legitimately compute the
    SAME Beets group key (e.g. a ``5.1``/``5.2`` family followed by a
    ``5A`` plain-looking-but-groupable position) without colliding on
    LITERAL position text, which is exactly the "adjacent families sharing
    a prefix" shape most likely to diverge from Beets if grouping were
    reimplemented incorrectly. Duplicate LITERAL positions are rejected
    inline (a real Discogs release never repeats one), not via a
    whole-draw ``assume`` that would only lower yield.
    """
    atom_count = draw(st.integers(min_value=1, max_value=5))
    entries: list[dict[str, object]] = []
    used_positions: set[str] = set()

    def reserve(position: str) -> bool:
        if position in used_positions:
            return False
        used_positions.add(position)
        return True

    for _ in range(atom_count):
        kind = draw(st.sampled_from(["plain", "digit_letter", "family", "nested"]))
        if kind == "plain":
            letter = draw(st.sampled_from("ABCDEFGH"))
            number = draw(st.integers(min_value=1, max_value=99))
            position = f"{letter}{number}"
            if not reserve(position):
                continue
            entries.append({"position": position, "title": position})
        elif kind == "digit_letter":
            number = draw(st.integers(min_value=1, max_value=99))
            letter = draw(st.sampled_from("ABCDEFGH"))
            position = f"{number}{letter}"
            if not reserve(position):
                continue
            entries.append({"position": position, "title": position})
        elif kind == "family":
            prefix = draw(st.integers(min_value=1, max_value=20))
            size = draw(st.integers(min_value=2, max_value=4))
            family_positions = [f"{prefix}.{index}" for index in range(1, size + 1)]
            if any(p in used_positions for p in family_positions):
                continue
            used_positions.update(family_positions)
            entries.extend({"position": p, "title": p} for p in family_positions)
        else:  # nested
            prefix = draw(st.integers(min_value=1, max_value=20))
            subindexed = draw(st.booleans())
            size = draw(st.integers(min_value=1, max_value=3))
            if subindexed:
                child_positions = [f"{prefix}.{index}" for index in range(1, size + 1)]
            else:
                child_positions = [f"{chr(65 + index)}{prefix}" for index in range(size)]
            if any(p in used_positions for p in child_positions):
                continue
            used_positions.update(child_positions)
            entries.append({
                "position": "", "title": f"Header{prefix}", "duration": "",
                "sub_tracks": [{"position": p, "title": f"C{p}"} for p in child_positions],
            })
    return entries


@given(_discogs_raw_entries())
def _property_discogs_manifest_agrees_with_beets_oracle(entries: list[dict[str, object]]) -> None:
    """Issue #1237: ``discogs_manifest``'s component set must match what
    Beets itself would catalogue -- the real adapter (``beetsplug.discogs
    .DiscogsPlugin``) is the oracle, not a second hand-written
    reimplementation of the same regex, over flat siblings AND nested
    ``sub_tracks`` containers (issue #1237 review C6/C7).
    """
    release = "1"
    manifest = discogs_manifest(release, {"id": release, "tracks": entries})
    oracle = _beets_oracle_groups(entries)
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
