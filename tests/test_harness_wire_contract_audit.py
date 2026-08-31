"""Harness wire contract, declared once and audited (#1278 item 8).

``harness/beets_harness.py`` runs inside the deployment-owned Beets
interpreter and cannot import ``lib/``, so the JSON keys its
``_serialize_*`` helpers hand-type duplicate the wire names that
``lib/quality/wire_types.py`` declares as msgspec Structs. Nothing at
runtime forces the two spellings to agree: msgspec ignores unknown keys,
and a defaulted Struct field fills silently when its key is dropped or
renamed on the emitting side.

This module closes both gaps:

1. **Key-set equality audit.** It drives the REAL harness serializers and
   the REAL ``HarnessImportSession.choose_match`` (mocked beets modules,
   duck-typed inputs — the same machinery as
   ``tests/test_harness_serialization.py``), captures one real protocol
   message off the harness's own protocol stdout, and asserts every
   emitted key set equals the paired Struct's declared wire names in both
   directions. Each deliberate divergence is carved out by name, and a
   carve-out that stops diverging is itself a violation, so the allowlist
   cannot rot. Bounded by construction: two key sets are compared; no
   source is scanned.

2. **Required-field strictness.** Wire fields a production decision path
   consumes are declared without defaults, so a dropped key raises
   ``msgspec.ValidationError`` at the decode boundary (the PR #98 drift
   class) instead of silently filling a default. The drop-one-key tables
   below enumerate the ENTIRE finite domain — every wire key of every
   harness Struct, derived by introspection, with the worlds derived from
   the captured real message (Rule C: the trigger comes from the
   producer, never a hand-typed literal). Exhaustive enumeration replaces
   a sampled generated property here because the domain is finite and
   fully covered.
"""

from __future__ import annotations

import copy
import functools
import io
import json
import sys
import unittest
from types import SimpleNamespace
from typing import TYPE_CHECKING, ClassVar
from unittest.mock import MagicMock

import msgspec

from lib.quality import (
    CandidateSummary,
    ChooseMatchMessage,
    HarnessItem,
    HarnessTrackInfo,
    TrackMapping,
)
from tests.harness_test_support import isolated_beets_harness, legacy_import_task_stub

if TYPE_CHECKING:
    from types import ModuleType


class _Stub:
    """Duck-typed attribute bag; hashable so it can key a beets mapping."""

    def __init__(self, **attrs: object) -> None:
        self.__dict__.update(attrs)


class _AlbumMatch(_Stub):
    """Real class (not MagicMock) so ``choose_match``'s isinstance passes."""


class _Distance:
    """Duck-typed beets Distance: float() total plus per-key breakdown."""

    def __float__(self) -> float:
        return 0.0321

    def items(self) -> list[tuple[str, float]]:
        return [("album", 0.0123), ("tracks", 0.0198)]


# Mocked beets modules, mirroring tests/test_harness_serialization.py; the
# real-beets import + API contract is tests/test_harness_beets2_contract.py.
_beets_mocks = {
    "beets": MagicMock(),
    "beets.config": MagicMock(),
    "beets.library": MagicMock(),
    "beets.plugins": MagicMock(),
    "beets.ui": MagicMock(),
    "beets.importer": MagicMock(),
    "beets.importer.actions": MagicMock(),
    "beets.importer.session": MagicMock(),
    "beets.importer.tasks": MagicMock(),
    "beets.autotag": MagicMock(),
    "beets.dbcore": MagicMock(),
    "beets.util": MagicMock(),
}
_beets_mocks["beets.ui"].get_path_formats = None
_beets_mocks["beets.ui"].get_replacements = None
_beets_mocks["beets.importer.session"].ImportSession = type(
    "ImportSession", (object,), {"resolve_duplicate": lambda *_args: None},
)
_beets_mocks["beets.importer.tasks"].ImportTask = legacy_import_task_stub()
# choose_match asserts isinstance against this exact name at serialize time.
_beets_mocks["beets.autotag"].AlbumMatch = _AlbumMatch

beets_harness: ModuleType
with isolated_beets_harness(_beets_mocks) as beets_harness:
    pass


def _item_stub() -> _Stub:
    return _Stub(
        path="/downloads/x/01 - Opener.flac",
        title="Opener",
        artist="The Artist",
        album="The Album",
        track=1,
        disc=1,
        length=201.4,
        bitrate=1017000,
        format="FLAC",
        mb_trackid="11111111-2222-3333-4444-555555555555",
        data_source="MusicBrainz",
    )


def _track_stub() -> _Stub:
    return _Stub(
        title="Opener",
        artist="The Artist",
        index=1,
        medium=1,
        medium_index=1,
        medium_total=10,
        length=201.0,
        track_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        release_track_id="eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa",
        track_alt=None,
        disctitle=None,
        data_source="MusicBrainz",
    )


TARGET_ALBUM_ID = "99999999-8888-7777-6666-555555555555"


def _album_match() -> _AlbumMatch:
    info = _Stub(
        artist="The Artist",
        album="The Album",
        album_id=TARGET_ALBUM_ID,
        albumdisambig="",
        year=2020,
        original_year=2019,
        country="AU",
        label="Chapter Music",
        catalognum="CH100",
        media="CD",
        mediums=1,
        albumtype="album",
        albumtypes=["album"],
        albumstatus="Official",
        releasegroup_id="12121212-3434-5656-7878-909090909090",
        release_group_title="The Album",
        va=False,
        language="eng",
        script="Latn",
        data_source="MusicBrainz",
        barcode="0000000000000",
        asin="",
        tracks=[_track_stub()],
    )
    return _AlbumMatch(
        distance=_Distance(),
        info=info,
        mapping={_item_stub(): _track_stub()},
        extra_items=[_item_stub()],
        extra_tracks=[_track_stub()],
    )


@functools.cache
def captured_choose_match_message() -> dict[str, object]:
    """One real ``choose_match`` message, captured off the protocol stdout.

    The session runs until it blocks for a controller decision; an empty
    stdin makes ``_recv`` raise EOFError there, AFTER the message is sent,
    which keeps the mocked ``Action`` machinery out of the exercised path.
    Callers must deepcopy before mutating.
    """
    session = beets_harness.HarnessImportSession.__new__(
        beets_harness.HarnessImportSession,
    )
    session._task_counter = 0
    session._pretend = True
    task = SimpleNamespace(
        candidates=[_album_match()],
        paths=[b"/downloads/x"],
        items=[_item_stub()],
        rec=SimpleNamespace(name="strong"),
        cur_artist="The Artist",
        cur_album="The Album",
    )
    buffer = io.StringIO()
    module_globals = vars(beets_harness)
    prior_stdout = module_globals["_protocol_stdout"]
    prior_stdin = sys.stdin
    module_globals["_protocol_stdout"] = buffer
    sys.stdin = io.StringIO("")
    hit_eof = False
    try:
        try:
            session.choose_match(task)
        except EOFError:
            hit_eof = True
    finally:
        module_globals["_protocol_stdout"] = prior_stdout
        sys.stdin = prior_stdin
    if not hit_eof:
        raise AssertionError(
            "choose_match returned without blocking on a controller decision",
        )
    lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
    if len(lines) != 1:
        raise AssertionError(f"expected exactly one protocol message, got {lines!r}")
    message: dict[str, object] = json.loads(lines[0])
    if message.get("type") != "choose_match":
        raise AssertionError(f"expected a choose_match message, got {message!r}")
    return message


def _wire_names(struct_type: type) -> frozenset[str]:
    return frozenset(f.encode_name for f in msgspec.structs.fields(struct_type))


def _required_wire_names(struct_type: type) -> frozenset[str]:
    return frozenset(
        f.encode_name for f in msgspec.structs.fields(struct_type) if f.required
    )


def key_set_violations(
    surface: str,
    emitted: frozenset[str],
    declared: frozenset[str],
    *,
    emitted_only: frozenset[str] = frozenset(),
    declared_only: frozenset[str] = frozenset(),
) -> list[str]:
    """Compare one emitted key set against one Struct's declared wire names.

    Every divergence must be named in a carve-out, and every carve-out must
    still be a live divergence — a stale carve-out is itself a violation.
    Accumulates every violation rather than short-circuiting, so ordering
    cannot mask a clause.
    """
    violations: list[str] = []
    for key in sorted(emitted - declared - emitted_only):
        violations.append(
            f"{surface}: harness emits key {key!r} that the struct does not declare",
        )
    for key in sorted(declared - emitted - declared_only):
        violations.append(
            f"{surface}: struct declares wire key {key!r} that the harness never emits",
        )
    for key in sorted(emitted_only - (emitted - declared)):
        violations.append(
            f"{surface}: stale emitted-only carve-out {key!r} no longer diverges",
        )
    for key in sorted(declared_only - (declared - emitted)):
        violations.append(
            f"{surface}: stale declared-only carve-out {key!r} no longer diverges",
        )
    return violations


def _as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def _as_list(value: object) -> list[object]:
    assert isinstance(value, list)
    return value


def _nested(message: dict[str, object], *steps: str | int) -> dict[str, object]:
    """Walk the captured message to a nested dict by keys and list indices."""
    node: object = message
    for step in steps:
        if isinstance(step, int):
            node = _as_list(node)[step]
        else:
            node = _as_dict(node)[step]
    return _as_dict(node)


#: Every audited (surface, path-into-message, struct, carve-outs) row.
#:
#: Carve-out rationale — each is a named, deliberate divergence:
#: - ``type``: message-routing key, consumed by both controllers before the
#:   typed decode (``lib/beets.py``, ``harness/import_one.py``).
#: - ``index``: emitted per candidate for raw-transcript debugging; both
#:   controllers select candidates positionally after matching by
#:   ``album_id`` (critical rule 3), and the Struct deliberately omits it.
#: - ``is_target``: lib-side annotation stamped AFTER decode
#:   (``lib/beets.py::apply_candidate_scenario``); never on the wire.
AUDITED_SURFACES: list[
    tuple[str, tuple[str | int, ...], type, frozenset[str], frozenset[str]]
] = [
    (
        "choose_match message",
        (),
        ChooseMatchMessage,
        frozenset({"type"}),
        frozenset(),
    ),
    (
        "task item",
        ("items", 0),
        HarnessItem,
        frozenset(),
        frozenset(),
    ),
    (
        "album candidate",
        ("candidates", 0),
        CandidateSummary,
        frozenset({"index"}),
        frozenset({"is_target"}),
    ),
    (
        "candidate track",
        ("candidates", 0, "tracks", 0),
        HarnessTrackInfo,
        frozenset(),
        frozenset(),
    ),
    (
        "mapping entry",
        ("candidates", 0, "mapping", 0),
        TrackMapping,
        frozenset(),
        frozenset(),
    ),
    (
        "mapped item",
        ("candidates", 0, "mapping", 0, "item"),
        HarnessItem,
        frozenset(),
        frozenset(),
    ),
    (
        "mapped track",
        ("candidates", 0, "mapping", 0, "track"),
        HarnessTrackInfo,
        frozenset(),
        frozenset(),
    ),
    (
        "extra item",
        ("candidates", 0, "extra_items", 0),
        HarnessItem,
        frozenset(),
        frozenset(),
    ),
    (
        "extra track",
        ("candidates", 0, "extra_tracks", 0),
        HarnessTrackInfo,
        frozenset(),
        frozenset(),
    ),
]


class TestHarnessKeySetsMatchStructWireNames(unittest.TestCase):
    """The one audit: every emitted key set equals its Struct's wire names."""

    def test_every_emitted_key_set_matches_its_struct(self) -> None:
        message = captured_choose_match_message()
        for surface, path, struct_type, emitted_only, declared_only in AUDITED_SURFACES:
            with self.subTest(surface=surface):
                emitted = frozenset(_nested(message, *path))
                self.assertEqual(
                    key_set_violations(
                        surface,
                        emitted,
                        _wire_names(struct_type),
                        emitted_only=emitted_only,
                        declared_only=declared_only,
                    ),
                    [],
                )

    def test_the_captured_worlds_are_populated(self) -> None:
        """The fixture exercises every nested list the audit walks into."""
        message = captured_choose_match_message()
        for surface, path, _struct, _eo, _do in AUDITED_SURFACES:
            with self.subTest(surface=surface):
                self.assertTrue(_nested(message, *path))


class TestKeySetCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad worlds per clause: the checker can actually fail."""

    DECLARED = frozenset({"a", "b"})

    def test_undeclared_emitted_key_is_named(self) -> None:
        violations = key_set_violations(
            "s", frozenset({"a", "b", "zzz"}), self.DECLARED,
        )
        self.assertEqual(len(violations), 1)
        self.assertRegex(violations[0], r"emits key 'zzz' that the struct")

    def test_unemitted_declared_key_is_named(self) -> None:
        violations = key_set_violations("s", frozenset({"a"}), self.DECLARED)
        self.assertEqual(len(violations), 1)
        self.assertRegex(violations[0], r"declares wire key 'b' that the harness")

    def test_stale_emitted_only_carve_out_is_named(self) -> None:
        violations = key_set_violations(
            "s", self.DECLARED, self.DECLARED, emitted_only=frozenset({"a"}),
        )
        self.assertEqual(len(violations), 1)
        self.assertRegex(violations[0], r"stale emitted-only carve-out 'a'")

    def test_stale_declared_only_carve_out_is_named(self) -> None:
        violations = key_set_violations(
            "s", self.DECLARED, self.DECLARED, declared_only=frozenset({"b"}),
        )
        self.assertEqual(len(violations), 1)
        self.assertRegex(violations[0], r"stale declared-only carve-out 'b'")

    def test_ordering_cannot_mask_a_clause(self) -> None:
        """Two independent violations both surface in one call."""
        violations = key_set_violations(
            "s", frozenset({"a", "zzz"}), self.DECLARED,
        )
        self.assertEqual(len(violations), 2)

    def test_carved_out_divergences_are_clean(self) -> None:
        violations = key_set_violations(
            "s",
            frozenset({"a", "b", "type"}),
            frozenset({"a", "b", "is_target"}),
            emitted_only=frozenset({"type"}),
            declared_only=frozenset({"is_target"}),
        )
        self.assertEqual(violations, [])


class TestRequiredFieldDeclarations(unittest.TestCase):
    """The required/optional split on the wire Structs is the contract.

    Required = a production decision path consumes the field, so a dropped
    key must raise at the decode boundary rather than fill a default:

    - ``ChooseMatchMessage``: every field. The message is decode-only in
      production (``lib/beets.py:333``, ``harness/import_one.py``) and the
      harness emits every key unconditionally; a missing ``candidates`` or
      ``items`` silently reading as empty is the worst drift shape.
    - ``CandidateSummary``: ``album_id`` (release identity — the #98
      incident class), ``distance`` (the accept gate), ``data_source``
      (the Discogs second-pass decision in ``lib/beets.py``), ``tracks``/
      ``mapping``/``extra_items``/``extra_tracks`` (``extra_tracks``
      validity + ``candidate_audio_coverage`` inputs).
    - ``TrackMapping``: both halves; an empty default mapping entry is
      meaningless.
    - ``HarnessItem`` keeps every default: ``harness/import_one.py``'s
      filesystem fallback legitimately constructs path-only items.
    - ``HarnessTrackInfo`` keeps every default: its #1183 fields carry
      documented semantic defaults, and its key set is audited above.
    """

    EXPECTED_REQUIRED: ClassVar[list[tuple[type, frozenset[str]]]] = [
        (
            ChooseMatchMessage,
            frozenset({
                "task_id",
                "path",
                "cur_artist",
                "cur_album",
                "item_count",
                "items",
                "recommendation",
                "candidate_count",
                "candidates",
            }),
        ),
        (
            CandidateSummary,
            frozenset({
                "album_id",
                "distance",
                "data_source",
                "tracks",
                "mapping",
                "extra_items",
                "extra_tracks",
            }),
        ),
        (TrackMapping, frozenset({"item", "track"})),
        (HarnessItem, frozenset()),
        (HarnessTrackInfo, frozenset()),
    ]

    def test_required_wire_keys_match_the_contract(self) -> None:
        for struct_type, expected in self.EXPECTED_REQUIRED:
            with self.subTest(struct=struct_type.__name__):
                self.assertEqual(_required_wire_names(struct_type), expected)


def _drop_key_world(
    *steps: str | int, key: str,
) -> dict[str, object]:
    """The captured real message with exactly one nested key removed."""
    world = copy.deepcopy(captured_choose_match_message())
    del _nested(world, *steps)[key]
    return world


class TestDroppedKeysFailLoudAtTheBoundary(unittest.TestCase):
    """Exhaustive drop-one-key enumeration over the whole wire contract.

    Worlds derive from the captured real message; the key list derives
    from Struct introspection. A required key's absence raises
    ``msgspec.ValidationError``; an optional key's absence still decodes
    (pinning the deliberate leaf tolerance).
    """

    #: (surface, path to the container whose keys are dropped, struct)
    DROP_SITES: ClassVar[list[tuple[str, tuple[str | int, ...], type]]] = [
        ("choose_match message", (), ChooseMatchMessage),
        ("album candidate", ("candidates", 0), CandidateSummary),
        ("mapping entry", ("candidates", 0, "mapping", 0), TrackMapping),
        ("task item", ("items", 0), HarnessItem),
        ("candidate track", ("candidates", 0, "tracks", 0), HarnessTrackInfo),
    ]

    def test_every_wire_key_dropped_once(self) -> None:
        for surface, path, struct_type in self.DROP_SITES:
            for field in msgspec.structs.fields(struct_type):
                with self.subTest(surface=surface, key=field.encode_name):
                    if field.encode_name not in _nested(
                        captured_choose_match_message(), *path,
                    ):
                        # Struct-only names (``is_target``) never ride the
                        # wire; the key-set audit owns that divergence.
                        continue
                    world = _drop_key_world(*path, key=field.encode_name)
                    if field.required:
                        with self.assertRaises(msgspec.ValidationError):
                            msgspec.convert(world, type=ChooseMatchMessage)
                    else:
                        msgspec.convert(world, type=ChooseMatchMessage)

    def test_the_full_captured_message_decodes_strictly(self) -> None:
        """Composition guard: the real emitter satisfies the strict decoder."""
        decoded = msgspec.convert(
            captured_choose_match_message(), type=ChooseMatchMessage,
        )
        self.assertEqual(len(decoded.candidates), 1)
        target = decoded.candidates[0]
        self.assertEqual(target.mbid, TARGET_ALBUM_ID)
        self.assertFalse(target.is_target)
        self.assertEqual(decoded.recommendation, "strong")
        self.assertEqual(len(decoded.items), 1)
        self.assertEqual(len(target.mapping), 1)
        self.assertEqual(len(target.extra_tracks), 1)


if __name__ == "__main__":
    unittest.main()
