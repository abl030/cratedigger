"""Generated production-seam invariants for the #1149 classifier.

Each property drives ``classify_album``; the checker is deliberately outside
the classifier so its named violations can be independently self-tested.
"""
from __future__ import annotations

import unittest

from beetsplug.discogs import DiscogsPlugin
from beetsplug.discogs.types import AudioTrack, Track
from hypothesis import example, given
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


def _discogs_world(
    positions: tuple[str, ...], *, catalog_positions: tuple[str, ...],
    physical_positions: tuple[str, ...], unreadable_extra: bool = False,
    untagged_extra: bool = False, conflicting_extra: bool = False,
):
    """Issue #1237 review E2: ``unreadable_extra``/``untagged_extra``/
    ``conflicting_extra`` select which of the THREE real ``unknown_extra``
    producers in ``classify_album``'s Discogs uncatalogued-extra loop the
    "extra" physical file exercises -- an ``AudioTagReadError``, blank
    tags, or a tag pair matching two DIFFERENT top-level components at
    once. Mutually exclusive; all default ``False`` reproduces the
    pre-existing shape every other caller in this module relies on: a
    non-blank tag ("1-extra") that matches nothing, which none of the
    three producer branches recognise (contributes only to catalog
    drift). ``conflicting_extra`` requires >= 2 top-level components in
    ``positions`` -- a single coalesced group has only one.
    """
    release = "1"
    manifest = discogs_manifest(release, {
        "id": release,
        "tracks": [{"position": p, "title": p} for p in positions],
    })
    catalog = tuple(CatalogItem(f"/album/{p}.flac", f"{release}-{p}", "") for p in catalog_positions)
    album = LibraryAlbum(1, "a", "b", ReleaseIdentity("discogs", release), "/album", catalog)
    physical = tuple(f"/album/{p}.flac" for p in physical_positions)
    manifest_keys = [component.key for component in manifest.components]
    def tags(path: str) -> tuple[str, str]:
        if path.endswith("extra.flac"):
            if unreadable_extra:
                raise AudioTagReadError("unreadable")
            if untagged_extra:
                return ("", "")
            if conflicting_extra:
                assert len(manifest_keys) >= 2, "conflicting_extra needs >= 2 top-level components"
                return (manifest_keys[0], manifest_keys[1])
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


@given(
    positions=st.lists(st.text(alphabet="ABC123", min_size=1, max_size=4), min_size=1, max_size=6, unique=True),
    grouped=st.booleans(),
    prefix=st.integers(min_value=1, max_value=20),
    group_size=st.integers(min_value=2, max_value=4),
    extra_producer=st.sampled_from(["unreadable", "untagged", "conflicting"]),
)
@example(positions=["A"], grouped=True, prefix=1, group_size=2, extra_producer="untagged")
@example(positions=["A"], grouped=True, prefix=1, group_size=2, extra_producer="conflicting")
def _property_unreadable_extra_is_unknown_not_missing(
    positions: list[str], grouped: bool, prefix: int, group_size: int,
    extra_producer: str,
) -> None:
    """Issue #1237 review D1/E1/E2: an uncatalogued extra file with
    unresolved identity must fail closed to ``unknown``, never
    ``missing_source_audio`` -- both for the identity-driven verdict's
    pre-existing ``not unknown_extra`` guard (``grouped=False``, the
    original scenario, always the ``unreadable`` producer) AND for the
    grouped-composite physical check's OWN matching guard (``grouped=
    True``, issue #1237 review D1's live regression: the original
    scenario's ``catalog_positions=()`` left every component unknown, so
    it could never reach the grouped branch at all -- a mutant deleting
    the grouped block's own guard survived every run before this branch
    was added). ``extra_producer`` draws all THREE real ``unknown_extra``
    producers for the grouped branch (issue #1237 review E2: only
    ``unreadable`` was patrolled there before -- ``untagged`` and
    ``conflicting`` were not).
    """
    if grouped:
        group_positions = tuple(f"{prefix}.{index}" for index in range(1, group_size + 1))
        if extra_producer == "conflicting":
            # A conflicting-identity extra needs a SECOND, unrelated
            # top-level component to conflict against -- a single
            # coalesced group has only one (issue #1237 review E2). The
            # letter prefix keeps it structurally distinct from the
            # group's own bare-numeric dotted family; it is never
            # catalogued or physically present, so it becomes the
            # identity-driven verdict's own ``missing`` -- exercising
            # that verdict's ``unknown_extra`` downgrade too, not only
            # the grouped-composite block's.
            all_positions = group_positions + (f"Z{prefix}",)
        else:
            all_positions = group_positions
        result = _discogs_world(
            all_positions, catalog_positions=(group_positions[0],),
            physical_positions=(group_positions[0], "extra"),
            unreadable_extra=extra_producer == "unreadable",
            untagged_extra=extra_producer == "untagged",
            conflicting_extra=extra_producer == "conflicting",
        )
        assert not completeness_invariant_violations({f.kind for f in result.findings}, expect_drift=True, expect_missing=False, expect_video_ignored=False, expect_unknown=True)
        # Issue #1237 review E1: ``completeness_invariant_violations`` only
        # checks KIND-SET membership, so it cannot distinguish the grouped
        # composite emitting its OWN "unknown" evidence from it silently
        # contributing nothing while merely inheriting "unknown" from the
        # unrelated extra file's own finding (D1's original defect --
        # "the finding vanishes"). Assert directly that the composite's own
        # finding exists: its first sub-position's title (== its own
        # literal position, per ``_discogs_world``'s manifest construction)
        # must appear in some ``unknown`` finding's detail.
        unknown_details = [f.detail for f in result.findings if f.kind == "unknown"]
        assert any(group_positions[0] in detail for detail in unknown_details), (
            f"grouped composite {group_positions[0]!r} produced no unknown "
            f"finding naming itself: {unknown_details!r}"
        )
    else:
        result = _discogs_world(
            tuple(positions), catalog_positions=(), physical_positions=("extra",),
            unreadable_extra=True,
        )
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
    * bare numeric positions with no subtrack index at all (``16`` --
      issue #1237 review D3, previously ungenerated: distinct from
      ``plain``/``digit_letter``, which always mix a letter in, so neither
      could stand in for the coordinator's ``["16", "16.1", "16.2"]``
      counter-example);
    * dotted subtrack families, optionally letter-prefixed (``16.1``/
      ``16.2``/... or ``A2.1``/``A2.2``/... -- issue #1237 review D3);
    * nested ``sub_tracks`` headers, both subindexed (Beets' merge branch,
      issue #1237 review C6) and not (Beets' expand branch).

    A family atom may also lead with a BARE entry sharing its exact
    prefix (``lead_with_bare``, issue #1237 review D3) -- deterministically
    reproducing the adjacency shape that exposes
    ``_discogs_subtrack_group_key``'s ``subindex`` guard: without a bare,
    non-subindexed position immediately preceding a same-prefix dotted
    family, mutating that guard away is invisible, because
    ``discogs_manifest``'s own ``flush_pending`` collapses a length-1
    pending group back into an ordinary singleton component identical to
    what a direct append would have produced. Independent random draws of
    ``bare_numeric``/``family`` atoms sharing a prefix by chance are not
    relied on for this coverage.

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
        kind = draw(st.sampled_from(
            ["plain", "digit_letter", "bare_numeric", "family", "nested"]
        ))
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
        elif kind == "bare_numeric":
            number = draw(st.integers(min_value=1, max_value=20))
            position = str(number)
            if not reserve(position):
                continue
            entries.append({"position": position, "title": position})
        elif kind == "family":
            letter = draw(st.sampled_from(["", *"ABCDEFGH"]))
            prefix = draw(st.integers(min_value=1, max_value=20))
            label = f"{letter}{prefix}"
            size = draw(st.integers(min_value=2, max_value=4))
            family_positions = [f"{label}.{index}" for index in range(1, size + 1)]
            lead_with_bare = draw(st.booleans())
            candidate_positions = ([label] if lead_with_bare else []) + family_positions
            if any(p in used_positions for p in candidate_positions):
                continue
            used_positions.update(candidate_positions)
            if lead_with_bare:
                entries.append({"position": label, "title": label})
            entries.extend({"position": p, "title": p} for p in family_positions)
        else:  # nested
            prefix = draw(st.integers(min_value=1, max_value=20))
            subindexed = draw(st.booleans())
            size = draw(st.integers(min_value=1, max_value=3))
            if subindexed:
                child_positions = [f"{prefix}.{index}" for index in range(1, size + 1)]
                # Issue #1237 review D3: a subindexed header collapses to
                # ONE physical track keyed by the stripped medium+index of
                # its FIRST child (``_try_coalesce_nested_index`` mirrors
                # ``DiscogsPlugin._coalesce_index_track`` exactly), not by
                # any of the children's own literal positions. That derived
                # key can coincide with an unrelated atom's bare literal
                # position (e.g. a "bare_numeric" ``"1"`` next to a nested
                # header whose first child is ``"1.1"``) -- reserve it too,
                # or ``discogs_manifest`` legitimately raises
                # ``SourceManifestError("duplicate identity")`` on a world
                # no real Discogs release could produce (real releases
                # never repeat a physical track identity).
                first_medium, first_index, _ = DiscogsPlugin.get_track_index(
                    child_positions[0]
                )
                derived_key = f"{first_medium or ''}{first_index or ''}"
                reservation_targets = [derived_key, *child_positions]
            else:
                child_positions = [f"{chr(65 + index)}{prefix}" for index in range(size)]
                reservation_targets = child_positions
            if any(p in used_positions for p in reservation_targets):
                continue
            used_positions.update(reservation_targets)
            entries.append({
                "position": "", "title": f"Header{prefix}", "duration": "",
                "sub_tracks": [{"position": p, "title": f"C{p}"} for p in child_positions],
            })
    return entries


@given(_discogs_raw_entries())
@example(
    # Independent review D3: the coordinator's own counter-example. A bare
    # numeric position with no subtrack index ("16"), immediately followed
    # by a dotted family sharing that exact prefix ("16.1"/"16.2"). Beets
    # keeps "16" a standalone track and merges only "16.1"+"16.2"; the
    # strategy's earlier "plain"/"digit_letter"/"family" atom kinds could
    # never generate a bare, letterless, dotless leading position, so this
    # exact adjacency was unreachable and a mutant removing
    # ``_discogs_subtrack_group_key``'s ``subindex`` guard survived all 61
    # tests then in both this file and ``tests/test_library_completeness.py``
    # (52 + 9, measured at review time). Pinned rather than left to chance,
    # per the "widen the strategy, not delete the clause" / "pin the
    # decisive world" rules.
    [
        {"position": "16", "title": "16"},
        {"position": "16.1", "title": "16.1"},
        {"position": "16.2", "title": "16.2"},
    ],
)
def _property_discogs_manifest_agrees_with_beets_oracle(entries: list[dict[str, object]]) -> None:
    """Issue #1237: ``discogs_manifest``'s component set must match what
    Beets itself would catalogue -- the real adapter (``beetsplug.discogs
    .DiscogsPlugin``) is the oracle, not a second hand-written
    reimplementation of the same regex, over flat siblings AND nested
    ``sub_tracks`` containers (issue #1237 review C6/C7), plus the bare-
    numeric-then-family adjacency (issue #1237 review D3).
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
