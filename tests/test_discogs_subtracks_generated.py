"""Generated agreement property for Discogs candidate construction.

The invariant that would have caught the whole #1261 validation-side
class for free: **for one mirror-shaped Discogs payload, the harness's
REAL candidate (``DiscogsPlugin.get_tracks`` through the compat layer)
counts exactly the tracks the acquisition manifest
(``lib.discogs_positions.normalize_release_tracks``) counts.** The two
are independent implementations on either side of the pipeline↔beets
boundary; a rip satisfies the matcher's count gate iff it satisfies
beets' mapping, or every download bounces at one side or the other.

Worlds are drawn structurally (mirror-retyped headings, positioned
tracks with and without durations, positionless-but-timed ambiguous
rows, ADJACENT flat subtrack runs, positionless-only releases). Known
limits (deliberate): subtrack runs are always adjacent — Beets'
``groupby`` coalescing is consecutive-only while the manifest groups by
base string, so a non-adjacent run is a real upstream-data pathology
with no live producer and no agreed answer yet; and the placeholder
grammar axes are owned by the deterministic pins in
``tests/test_discogs_subtracks.py`` and ``tests/test_discogs_api.py``.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from beets import config
from beetsplug.discogs import ArtistState, DiscogsPlugin
from beetsplug.discogs.types import Track
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from harness.beets_compat import configure_discogs_subtracks
from lib.discogs_positions import normalize_release_tracks


@dataclass(frozen=True)
class World:
    rows: tuple[tuple[str, str, str], ...]  # (position, title, duration)


_titles = st.text(
    alphabet="abcdefghij XYZ'", min_size=1, max_size=12,
).filter(lambda t: t.strip())

_durations = st.sampled_from(("", "2:30", "4:07", "0:39"))


@st.composite
def worlds(draw: st.DrawFn) -> World:
    positionless_only = draw(st.booleans())
    rows: list[tuple[str, str, str]] = []
    if positionless_only:
        for _ in range(draw(st.integers(min_value=1, max_value=4))):
            rows.append(("", draw(_titles), draw(_durations)))
        return World(rows=tuple(rows))
    count = draw(st.integers(min_value=1, max_value=5))
    for index in range(count):
        number = index + 1
        kind = draw(st.sampled_from(
            ("track", "untimed_track", "heading", "ambiguous", "subrun"),
        ))
        if kind == "track":
            rows.append((f"A{number}", draw(_titles), draw(_durations)))
        elif kind == "untimed_track":
            rows.append((f"A{number}", draw(_titles), ""))
        elif kind == "heading":
            rows.append(("", draw(_titles), ""))
        elif kind == "ambiguous":
            rows.append(("", draw(_titles), "2:30"))
        else:
            for sub in range(1, draw(st.integers(min_value=2, max_value=3)) + 1):
                rows.append((f"A{number}.{sub}", draw(_titles), "4:00"))
    # Guarantee the world is not accidentally positionless-only (the
    # heading/ambiguous kinds could be the only draws): ensure at least
    # one positioned row so both sides use their positioned semantics.
    if not any(position for position, _, _ in rows):
        rows.append(("A9", draw(_titles), "3:00"))
    return World(rows=tuple(rows))


_TINY_DOTS_PIN = World(rows=(
    ("", 'Selections XYZ', ""),
    ("A1", "a", "6:44"),
    ("A2", "b", "2:32"),
    ("", "Selections Live", ""),
    ("B1", "departure", "3:33"),
    ("B2", "mayor", "3:29"),
))

_POSITIONLESS_PIN = World(rows=(
    ("", "one", "3:00"),
    ("", "two", ""),
    ("", "three", "2:30"),
))


def _plugin() -> DiscogsPlugin:
    plugin = object.__new__(DiscogsPlugin)
    plugin.config = config["discogs"]
    plugin.config.add({
        "index_tracks": False,
        "strip_disambiguation": True,
        "featured_string": "Feat.",
        "anv": {
            "artist_credit": True,
            "artist": False,
            "album_artist": False,
        },
    })
    return plugin


def _artist_state(plugin: DiscogsPlugin) -> ArtistState:
    return ArtistState.from_config(plugin.config, [{
        "id": "1", "name": "Agreement Artist", "anv": "", "join": "",
        "role": "", "tracks": "", "resource_url": "",
    }])


class TestCandidateManifestAgreement(unittest.TestCase):
    def tearDown(self) -> None:
        configure_discogs_subtracks(preserve_flat=False)

    @given(worlds())
    @example(_TINY_DOTS_PIN)
    @example(_POSITIONLESS_PIN)
    def test_candidate_counts_agree_with_the_manifest(
        self, world: World,
    ) -> None:
        mirror_tracklist: list[Track] = [
            {"type_": "track", "position": position, "title": title,
             "duration": duration}
            for position, title, duration in world.rows
        ]
        manifest = normalize_release_tracks([
            {"position": position, "title": title, "duration": duration}
            for position, title, duration in world.rows
        ])

        configure_discogs_subtracks(preserve_flat=False)
        plugin = _plugin()
        candidate = plugin.get_tracks(mirror_tracklist, _artist_state(plugin))

        self.assertEqual(
            len(candidate), len(manifest),
            "the harness candidate and the acquisition manifest must "
            f"count the same tracks for one payload; rows={world.rows!r}",
        )


if __name__ == "__main__":
    unittest.main()
