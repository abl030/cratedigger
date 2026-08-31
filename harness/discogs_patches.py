"""Cratedigger's Discogs plugin behaviour patches.

Deliberate, process-local behaviour changes to ``beetsplug.discogs`` —
flat-subtrack physical-program handling, mirror-retyped heading-row
filtering, and the real-API cover-art fallback (#1200) — installed by the
harness child before plugin loading. The structural beets-core era
boundary lives in ``beets_compat``; this module owns only the Discogs
plugin's own seams, resolved by attribute presence, never a version
string.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import TypeGuard

import msgspec
import requests

try:
    from harness import beets_compat
except ModuleNotFoundError:  # direct wrapper execution puts harness/ first
    beets_compat = importlib.import_module("beets_compat")

BeetsCapabilityError = beets_compat.BeetsCapabilityError
_class = beets_compat._class
_required_module = beets_compat._required_module

# beets_harness.py's own module-level logging.basicConfig(stream=sys.stderr)
# runs at import time, before any harness call reaches this module -- a
# plain getLogger here inherits that stderr handler, matching the harness's
# stdout-is-JSON-protocol / stderr-is-human-diagnostics split
# (.claude/rules/harness.md). Outside the harness (e.g. under a bare test
# runner), the logging module's own last-resort handler still writes
# WARNING+ to stderr.
_logger = logging.getLogger(__name__)


_discogs_original_subtrack_position: Callable[..., object] | None = None
_discogs_original_merge_subtracks: Callable[..., object] | None = None
_discogs_original_coalesce_tracks: Callable[..., object] | None = None
_discogs_original_add_merged_subtracks: Callable[..., object] | None = None
_discogs_original_get_tracks: Callable[..., object] | None = None
_discogs_original_select_cover_art: Callable[..., object] | None = None
@dataclass(frozen=True)
class DiscogsIndexedProgram:
    component_count: int
    duration_complete: bool


_discogs_indexed_programs: dict[
    tuple[str, str, float],
    DiscogsIndexedProgram,
] = {}

DISCOGS_INDEXED_COMPONENT_COUNT_ATTR = (
    "_cratedigger_discogs_indexed_component_count"
)
DISCOGS_INDEXED_DURATION_COMPLETE_ATTR = (
    "_cratedigger_discogs_indexed_duration_complete"
)


def _discogs_track_key(track: object) -> tuple[str, str, float]:
    return (
        str(getattr(track, "track_alt", "") or ""),
        str(getattr(track, "title", "") or ""),
        round(float(getattr(track, "length", 0.0) or 0.0), 1),
    )


def register_discogs_indexed_program(
    track: object,
    component_count: int,
    *,
    duration_complete: bool = True,
) -> None:
    if component_count < 1:
        raise BeetsCapabilityError(
            "Beets Discogs indexed component count is invalid"
        )
    _discogs_indexed_programs[_discogs_track_key(track)] = (
        DiscogsIndexedProgram(
            component_count=component_count,
            duration_complete=duration_complete,
        )
    )


def discogs_indexed_component_count(track: object) -> int:
    return _discogs_indexed_programs.get(
        _discogs_track_key(track),
        DiscogsIndexedProgram(component_count=1, duration_complete=True),
    ).component_count


def discogs_indexed_duration_complete(track: object) -> bool:
    return _discogs_indexed_programs.get(
        _discogs_track_key(track),
        DiscogsIndexedProgram(component_count=1, duration_complete=True),
    ).duration_complete


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _discogs_duration_seconds(value: object) -> int | None:
    """Parse Discogs' bounded ``M:SS`` duration without version internals."""

    if not isinstance(value, str):
        return None
    pieces = value.split(":")
    if len(pieces) != 2:
        return None
    try:
        minutes, seconds = (int(piece) for piece in pieces)
    except ValueError:
        return None
    if minutes < 0 or not 0 <= seconds < 60:
        return None
    return minutes * 60 + seconds


def _mark_discogs_indexed_program(
    track: dict[str, object],
    subtracks: list[dict[str, object]],
) -> None:
    """Attach complete physical-program evidence to one normalized track."""

    if len(subtracks) < 2:
        return
    seconds = [
        _discogs_duration_seconds(subtrack.get("duration"))
        for subtrack in subtracks
    ]
    duration_complete = all(
        value is not None and value > 0
        for value in seconds
    )
    if duration_complete:
        total = sum(value for value in seconds if value is not None)
        track["duration"] = f"{total // 60}:{total % 60:02d}"
    track[DISCOGS_INDEXED_COMPONENT_COUNT_ATTR] = len(subtracks)
    track[DISCOGS_INDEXED_DURATION_COMPLETE_ATTR] = duration_complete


def _discogs_subtrack_methods(
    plugin_class: type[object],
) -> tuple[str, Callable[..., object], Callable[..., object]] | None:
    """Resolve one complete flat-subtrack seam without version checks."""

    current_subtrack_position = getattr(
        plugin_class,
        "_subtrack_position",
        None,
    )
    current_merge_subtracks = getattr(
        plugin_class,
        "_merge_subtracks",
        None,
    )
    modern_present = (
        current_subtrack_position is not None
        or current_merge_subtracks is not None
    )
    if modern_present:
        if not callable(current_subtrack_position):
            raise BeetsCapabilityError(
                "Beets Discogs plugin lacks callable _subtrack_position"
            )
        if not callable(current_merge_subtracks):
            raise BeetsCapabilityError(
                "Beets Discogs plugin lacks callable _merge_subtracks"
            )
        return (
            "modern",
            current_subtrack_position,
            current_merge_subtracks,
        )

    current_coalesce_tracks = getattr(
        plugin_class,
        "_coalesce_tracks",
        None,
    )
    current_add_merged_subtracks = getattr(
        plugin_class,
        "_add_merged_subtracks",
        None,
    )
    legacy_present = (
        current_coalesce_tracks is not None
        or current_add_merged_subtracks is not None
    )
    if not legacy_present:
        return None
    if not callable(current_coalesce_tracks):
        raise BeetsCapabilityError(
            "Beets Discogs plugin lacks callable _coalesce_tracks"
        )
    if not callable(current_add_merged_subtracks):
        raise BeetsCapabilityError(
            "Beets Discogs plugin lacks callable _add_merged_subtracks"
        )
    return (
        "legacy",
        current_coalesce_tracks,
        current_add_merged_subtracks,
    )


def _is_discogs_heading_row(
    track: dict[str, object], *, any_positioned: bool,
) -> bool:
    if track.get("type_") == "heading":
        return True
    return (
        any_positioned
        and track.get("position") == ""
        and track.get("duration") == ""
        and not track.get("sub_tracks")
    )


def filter_discogs_heading_rows(
    tracklist: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Drop Discogs section-label rows before candidate construction.

    Two measured producer shapes (issue #1261's validation-side half,
    request 6937 / Discogs 8439330): the real api.discogs.com marks
    section labels ``type_ == "heading"`` (dropped unconditionally —
    Beets' own data says it is not a musical work), but the deployment's
    beets talks to the DISCOGS MIRROR (``nix/beets.nix`` repoints
    ``discogs_client._base_url``), whose ``/releases/<id>`` endpoint
    serves those same rows RETYPED ``type_ == "track"`` with a literal
    empty position and empty duration — so Beets' own heading handling
    never fires, on any version, and the labels become zero-length
    candidate tracks (live: a 12-file rip faced a 14-track candidate —
    ``extra_tracks`` reject at distance 0.5669 for a copy whose true
    mapping is 12↔12, with each label starting its own phantom medium).

    The shape branch mirrors ``lib/discogs_positions.py``'s measured
    header rule exactly, because the two sides must count the same
    tracks for the same payload: a literal empty position AND empty
    duration, never a row carrying nested ``sub_tracks`` (an index
    parent's children are real audio), and only when the release
    positions at least one other row — a release that positions NOTHING
    has no heading signal and keeps every row, exactly as the
    acquisition manifest does. (``lib/library_completeness.py`` shares
    the empty/empty shape but scopes it differently for the census; the
    manifest normalizer is the agreement partner here.) A tracklist that
    is ENTIRELY heading-shaped is returned unchanged: never manufacture
    an empty candidate from non-empty input.
    """
    any_positioned = any(
        str(track.get("position") or "") for track in tracklist
    )
    kept = [
        track for track in tracklist
        if not _is_discogs_heading_row(track, any_positioned=any_positioned)
    ]
    if not kept:
        return tracklist
    return kept


def configure_discogs_subtracks(*, preserve_flat: bool) -> None:
    """Install Cratedigger's narrow Discogs physical-track compatibility.

    Upstream intentionally coalesces consecutive flat ``A2.1``/``A2.2``
    entries because they can describe one physical track. Its merge retains
    only the first component's duration, however, so a genuine one-file
    composite is matched against an incomplete program. Always sum complete
    component durations.

    When a default candidate would leave admitted audio unmapped, controllers
    rerun one safe observational pass with ``preserve_flat=True``. That mode
    keeps flat indexed entries separate while leaving nested ``IndexTrack``
    handling on Beets' original path. The patch is process-local and is
    installed before plugin loading in the harness child.
    """

    module = _required_module("beetsplug.discogs")
    plugin_class = _class(module, "DiscogsPlugin")
    if plugin_class is None:
        raise BeetsCapabilityError("Beets Discogs plugin lacks DiscogsPlugin")

    methods = _discogs_subtrack_methods(plugin_class)
    if methods is None:
        # Releases without either complete normalization seam do not collapse
        # flat indexed subtracks, so there is nothing to adapt.
        return
    cohort, first_method, second_method = methods

    global _discogs_original_subtrack_position
    global _discogs_original_merge_subtracks
    global _discogs_original_coalesce_tracks
    global _discogs_original_add_merged_subtracks
    global _discogs_original_get_tracks
    if _discogs_original_get_tracks is None:
        original = getattr(plugin_class, "get_tracks", None)
        if not callable(original):
            raise BeetsCapabilityError(
                "Beets Discogs plugin lacks callable get_tracks"
            )
        _discogs_original_get_tracks = original

    original_get_tracks = _discogs_original_get_tracks
    _discogs_indexed_programs.clear()

    if cohort == "modern":
        if _discogs_original_subtrack_position is None:
            _discogs_original_subtrack_position = first_method
        if _discogs_original_merge_subtracks is None:
            _discogs_original_merge_subtracks = second_method
        original_merge = _discogs_original_merge_subtracks

        def merge_complete_program(
            subtracks: list[dict[str, object]],
        ) -> dict[str, object]:
            merged_value = original_merge(subtracks)
            if not isinstance(merged_value, dict):
                raise BeetsCapabilityError(
                    "Beets Discogs _merge_subtracks returned a non-dict"
                )
            merged = msgspec.convert(merged_value, type=dict[str, object])
            _mark_discogs_indexed_program(merged, subtracks)
            return merged

        type.__setattr__(
            plugin_class,
            "_merge_subtracks",
            staticmethod(merge_complete_program),
        )
        if preserve_flat:
            def keep_flat_entries_separate(
                _self: object,
                _track: dict[str, object],
            ) -> None:
                return None

            type.__setattr__(
                plugin_class,
                "_subtrack_position",
                keep_flat_entries_separate,
            )
        else:
            type.__setattr__(
                plugin_class,
                "_subtrack_position",
                _discogs_original_subtrack_position,
            )
    elif cohort == "legacy":
        if _discogs_original_coalesce_tracks is None:
            _discogs_original_coalesce_tracks = first_method
        if _discogs_original_add_merged_subtracks is None:
            _discogs_original_add_merged_subtracks = second_method
        original_coalesce = _discogs_original_coalesce_tracks
        original_add_merged = _discogs_original_add_merged_subtracks

        def add_complete_or_split_program(
            self: object,
            tracklist: list[dict[str, object]],
            subtracks: list[dict[str, object]],
        ) -> None:
            nested_index = bool(
                tracklist and not tracklist[-1].get("position")
            )
            if preserve_flat and not nested_index:
                tracklist.extend(subtracks)
                return
            original_add_merged(self, tracklist, subtracks)
            if nested_index or len(subtracks) < 2:
                return
            if not tracklist:
                raise BeetsCapabilityError(
                    "Beets Discogs _add_merged_subtracks produced no track"
                )
            _mark_discogs_indexed_program(tracklist[-1], subtracks)

        type.__setattr__(
            plugin_class,
            "_coalesce_tracks",
            original_coalesce,
        )
        type.__setattr__(
            plugin_class,
            "_add_merged_subtracks",
            add_complete_or_split_program,
        )
    else:
        raise BeetsCapabilityError(
            f"unsupported Beets Discogs subtrack cohort: {cohort}"
        )

    def retain_component_counts(
        self: object,
        tracklist: list[dict[str, object]],
        albumartistinfo: object,
    ) -> object:
        tracklist = filter_discogs_heading_rows(tracklist)
        programs_by_position: dict[str, DiscogsIndexedProgram] = {}
        if not preserve_flat:
            coalesce = getattr(self, "_coalesce_tracks", None)
            if not callable(coalesce):
                raise BeetsCapabilityError(
                    "Beets Discogs plugin lacks callable _coalesce_tracks"
                )
            normalized_value = coalesce(deepcopy(tracklist))
            if not _is_object_list(normalized_value):
                raise BeetsCapabilityError(
                    "Beets Discogs _coalesce_tracks returned a non-list"
                )
            for raw_track in normalized_value:
                if not isinstance(raw_track, dict):
                    raise BeetsCapabilityError(
                        "Beets Discogs _coalesce_tracks returned a non-dict track"
                    )
                track = msgspec.convert(
                    raw_track,
                    type=dict[str, object],
                )
                component_count = track.get(
                    DISCOGS_INDEXED_COMPONENT_COUNT_ATTR,
                    1,
                )
                duration_complete = track.get(
                    DISCOGS_INDEXED_DURATION_COMPLETE_ATTR,
                    True,
                )
                if (
                    not isinstance(component_count, int)
                    or isinstance(component_count, bool)
                    or component_count < 1
                    or not isinstance(duration_complete, bool)
                ):
                    raise BeetsCapabilityError(
                        "Beets Discogs indexed program marker is invalid"
                    )
                if component_count > 1:
                    programs_by_position[str(track.get("position", ""))] = (
                        DiscogsIndexedProgram(
                            component_count=component_count,
                            duration_complete=duration_complete,
                        )
                    )

        result = original_get_tracks(self, tracklist, albumartistinfo)
        if not _is_object_list(result):
            raise BeetsCapabilityError(
                "Beets Discogs get_tracks returned an unsupported shape"
            )
        for track_info in result:
            program = programs_by_position.get(
                str(getattr(track_info, "track_alt", "") or ""),
                DiscogsIndexedProgram(
                    component_count=1,
                    duration_complete=True,
                ),
            )
            register_discogs_indexed_program(
                track_info,
                program.component_count,
                duration_complete=program.duration_complete,
            )
        return result

    type.__setattr__(plugin_class, "get_tracks", retain_component_counts)


# The real Discogs API base (issue #1200). Deliberately a plain module-level
# constant, not a nix option or config knob: it names a fixed upstream
# service, not a deployment choice — see .claude/rules/scope.md
# "single-operator, no backwards-compat".
DISCOGS_REAL_API_BASE = "https://api.discogs.com"

_DISCOGS_COVER_ART_USER_AGENT = (
    "cratedigger-cover-art/1.0 +https://github.com/abl030/cratedigger"
)
_DISCOGS_COVER_ART_TIMEOUT_SECONDS = 10


class _DiscogsApiCoverArtImage(msgspec.Struct):
    """One entry of the real Discogs API's ``images`` array.

    The live payload carries many other keys (``type``, ``resource_url``,
    ``width``, ``height``, ``uri150``, ...); msgspec ignores unknown keys by
    default, so only the one field this fallback reads needs declaring.
    """

    uri: str


class _DiscogsApiCoverArtResponse(msgspec.Struct):
    """The subset of a real ``GET /releases/<id>`` response this reads.

    Defaults ``images`` to empty so a payload that omits the key entirely
    degrades to "no art" rather than a validation error.
    """

    images: list[_DiscogsApiCoverArtImage] = []


def _release_data_dict(result: object) -> dict[str, object]:
    """Narrow ``result.data`` to a string-keyed dict, gracefully.

    Reimplemented locally with ``msgspec`` (already a hard dependency of
    this module) rather than imported from ``lib`` -- harness/ is not
    guaranteed lib/ on sys.path (cratedigger.service's own wrapper does
    not export PYTHONPATH; issue #1200 review F1).

    Matches ``lib.json_narrow.json_dict``'s degrade-to-``{}`` behaviour
    for non-dict input, but is deliberately MORE graceful for the
    non-string-keyed case (issue #1200 review N3): ``json_dict`` calls
    ``msgspec.convert`` uncaught there and RAISES ``ValidationError``,
    while this helper catches that same error and also degrades to
    ``{}``. The two are not interchangeable -- the wider degrade is
    correct here because this is a fail-soft path (a malformed Discogs
    API payload must never raise into the caller).
    """
    data = getattr(result, "data", None)
    if not isinstance(data, dict):
        return {}
    try:
        return msgspec.convert(data, type=dict[str, object])
    except msgspec.ValidationError:
        return {}


def _real_discogs_cover_art_url(release_id: object, user_token: str) -> str | None:
    """Look up one release's cover art on the real (non-mirror) Discogs API.

    Fails soft in every case: a missing token, a non-int/str release id, a
    network error, a timeout, a non-2xx response, or a payload that does not
    match ``_DiscogsApiCoverArtResponse`` all return ``None`` rather than
    raising. Selection mirrors ``DiscogsPlugin.select_cover_art``'s own
    "first image in the list is the best candidate" rule.
    """
    if not user_token or not isinstance(release_id, (int, str)):
        return None
    try:
        response = requests.get(
            f"{DISCOGS_REAL_API_BASE}/releases/{release_id}",
            headers={
                "User-Agent": _DISCOGS_COVER_ART_USER_AGENT,
                "Authorization": f"Discogs token={user_token}",
            },
            timeout=_DISCOGS_COVER_ART_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = msgspec.convert(
            response.json(), type=_DiscogsApiCoverArtResponse
        )
    except (requests.RequestException, ValueError, msgspec.ValidationError) as exc:
        # Named and logged (issue #1200 review residual): a rate-limited
        # (429) window otherwise fails soft to "no art" with nothing to
        # diagnose from -- this is the only signal an operator gets that
        # the fallback is silently degrading imports.
        #
        # NEVER interpolate the exception object itself (issue #1200
        # review N1, a confirmed secret-disclosure defect): requests'
        # header validator raises InvalidHeader -- a RequestException, so
        # it IS caught here -- with the offending header VALUE embedded in
        # its message, and this call's Authorization header carries the
        # Discogs token. lib/beets.py deliberately dumps the harness's
        # full stderr to journald (truncating loses the exception line),
        # so `%s` on `exc` would land the token in the system journal. Log
        # only structural, never-secret fields: the exception TYPE and,
        # where present, the HTTP status code.
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        _logger.warning(
            "Discogs cover-art fallback failed soft for release %r: "
            "%s (status=%r)",
            release_id, type(exc).__name__, status_code,
        )
        return None
    if not payload.images:
        _logger.debug(
            "Discogs cover-art fallback found no images for release %r",
            release_id,
        )
        return None
    return payload.images[0].uri


def _discogs_select_cover_art_method(
    plugin_class: type[object],
) -> Callable[..., object]:
    """Resolve the ``select_cover_art`` seam, failing closed if it is gone."""

    original = getattr(plugin_class, "select_cover_art", None)
    if not callable(original):
        raise BeetsCapabilityError(
            "Beets Discogs plugin lacks callable select_cover_art"
        )
    return original


def configure_discogs_cover_art_fallback() -> None:
    """Install Cratedigger's real-API cover-art fallback for ``select_cover_art``.

    The Discogs mirror (``nix/beets.nix``) is built from the CC0 XML dumps,
    which carry zero artwork — every mirror release returns
    ``"images": []``, so upstream ``select_cover_art`` is structurally unable
    to find anything and ``cover_art_url`` is never set (issue #1200).

    This wraps ``select_cover_art`` so it is a complete no-op for ANY
    release the configured client already resolves art for — mirror- or
    stock-backed alike: that original result wins outright, and the real
    API is never called. Only when the original yields nothing does this
    perform ONE authenticated lookup against the real Discogs API for that
    exact release id — including on a stock (non-mirror) install whose
    release genuinely has no images, where it costs one additional
    authenticated request to the same ``api.discogs.com`` release the
    stock client just queried (issue #1200 review F5: it is NOT a no-op
    for that case). Fails soft in every case (see
    ``_real_discogs_cover_art_url``); never blocks or fails an import.
    """
    module = _required_module("beetsplug.discogs")
    plugin_class = _class(module, "DiscogsPlugin")
    if plugin_class is None:
        raise BeetsCapabilityError("Beets Discogs plugin lacks DiscogsPlugin")

    original = _discogs_select_cover_art_method(plugin_class)

    global _discogs_original_select_cover_art
    if _discogs_original_select_cover_art is None:
        _discogs_original_select_cover_art = original
    original_select_cover_art = _discogs_original_select_cover_art

    def select_cover_art_with_real_api_fallback(
        self: object, result: object
    ) -> str | None:
        stock_url = original_select_cover_art(self, result)
        if isinstance(stock_url, str) and stock_url:
            return stock_url
        release_id = _release_data_dict(result).get("id")
        config = getattr(self, "config", None)
        if config is None:
            return None
        try:
            user_token = config["user_token"].as_str()
        except Exception:  # noqa: BLE001 — confuse access fails soft to no art
            return None
        return _real_discogs_cover_art_url(release_id, user_token)

    type.__setattr__(
        plugin_class,
        "select_cover_art",
        select_cover_art_with_real_api_fallback,
    )
