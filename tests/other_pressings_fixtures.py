"""Typed access to the captured other-pressing fixtures (issue #1445).

``tests/fixtures/other_pressings/<download_log_id>.json`` holds, per row:
the selected candidate's mapping from the pipeline DB, and the REAL
MusicBrainz mirror responses for the target release and its release-group
pages, trimmed to the fields the check reads. The TypedDicts below are that
trimmed shape, so tests edit a capture without escape hatches.

``mirror_get`` serves the captures at the ``lib.mb_api._get`` leaf seam,
answering by URL exactly as the mirror would, so route and CLI tests run
the real client functions and the real service.
"""

from __future__ import annotations

import email.message
import io
import json
import urllib.error
import urllib.parse
from collections.abc import Callable
from pathlib import Path
from typing import NotRequired, TypedDict

FIXTURES = Path(__file__).parent / "fixtures" / "other_pressings"

RAZORLIGHT = 28366
KURT_VILE = 24179
FAIRWAYS = 42692
DEER_TICK = 12368
BOB = 22324

class RecordingJSON(TypedDict):
    length: int | None


TrackJSON = TypedDict("TrackJSON", {
    "position": int | None, "number": str | None, "title": str,
    "length": int | None, "recording": NotRequired[RecordingJSON],
})
DiscJSON = TypedDict("DiscJSON", {
    "id": str, "offset-count": int, "sectors": int, "offsets": list[int],
})
MediumJSON = TypedDict("MediumJSON", {
    "position": int, "format": str | None, "track-count": int,
    "discs": list[DiscJSON], "tracks": list[TrackJSON],
})
GroupRefJSON = TypedDict("GroupRefJSON", {"id": str, "title": str})
ReleaseJSON = TypedDict("ReleaseJSON", {
    "id": str, "title": str, "country": str | None, "date": str | None,
    "media": list[MediumJSON], "release-group": NotRequired[GroupRefJSON],
})
PageJSON = TypedDict("PageJSON", {
    "release-count": int, "release-offset": NotRequired[int],
    "releases": list[ReleaseJSON],
})


class ItemJSON(TypedDict):
    path: str | None
    title: str | None
    track: int | None
    disc: int | None
    length: float


class TrackInfoJSON(TypedDict):
    title: str
    medium: int
    medium_index: int
    medium_total: int | None
    length: float


class PairJSON(TypedDict):
    item: ItemJSON
    track: TrackInfoJSON


class CandidateJSON(TypedDict):
    album_id: str
    extra_items: int
    extra_tracks: int
    mapping: list[PairJSON]


class FixtureJSON(TypedDict):
    download_log_id: int
    candidate: CandidateJSON
    target: ReleaseJSON
    release_group_pages: list[PageJSON]


def load_fixture(log_id: int) -> FixtureJSON:
    loaded: FixtureJSON = json.loads(
        (FIXTURES / f"{log_id}.json").read_text(encoding="utf-8"))
    return loaded


def releases(fixture: FixtureJSON) -> list[ReleaseJSON]:
    return [
        release for page in fixture["release_group_pages"]
        for release in page["releases"]
    ]


def validation_result(
    fixture: FixtureJSON, *, album_id: str | None = None,
) -> dict[str, object]:
    """The selected candidate in the wire shape the DB stores, next to a
    non-selected sibling candidate that must never be read."""
    candidate = fixture["candidate"]
    return {
        "valid": True, "scenario": "strong_match",
        "candidates": [
            {"album_id": "sibling-not-selected", "is_target": False,
             "mapping": []},
            {
                "album_id": album_id or candidate["album_id"],
                "is_target": True,
                "mapping": candidate["mapping"],
                "extra_items": [
                    {"path": f"extra-{n}.flac"}
                    for n in range(candidate["extra_items"])],
                "extra_tracks": [
                    {"title": f"extra {n}"}
                    for n in range(candidate["extra_tracks"])],
            },
        ],
    }


def not_found(url: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url, 404, "Not Found", hdrs=email.message.Message(),
        fp=io.BytesIO())


def mirror_get(fixture: FixtureJSON) -> Callable[..., object]:
    """A stand-in for ``lib.mb_api._get`` that answers from the capture."""
    target = fixture["target"]

    def get(
        url: str, *, timeout: float = 15.0, slot_timeout: float | None = None,
    ) -> object:
        assert timeout > 0
        assert slot_timeout is None or slot_timeout > 0
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path.endswith("/release") and "release-group" in query:
            offset = int(query["offset"][0])
            limit = int(query["limit"][0])
            pages = fixture["release_group_pages"]
            index = offset // limit
            if index < len(pages):
                return pages[index]
            return {"release-count": pages[0]["release-count"], "releases": []}
        release_id = parsed.path.rsplit("/", 1)[-1]
        if release_id == target["id"]:
            return target
        raise not_found(url)

    return get
