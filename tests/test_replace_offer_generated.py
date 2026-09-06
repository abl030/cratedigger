"""Generated patrol for the inverted Replace button's offer decision
(``web/js/replace_offer.js``, issue #1366 part 2), driven through one Node
worker so the REAL JavaScript answers every world.

Invariants, written down first:

* Enabled iff the row's own key holds an active request, or the paired
  key does. Never enabled from a lookup that failed with nothing else
  active; never enabled from a pair the compare did not produce — and a
  pair handed to a surface whose pairing is not ``checked`` is not one
  the compare produced, so it is ignored entirely.
* The reason code is one of the nine, and it is an enabling reason iff
  the offer is enabled. An own-key offer beats a paired one.
* Honesty: a disabled offer explains the FIRST thing that could not be
  answered. A failed lookup (with a key to look up) is reported as
  "could not check", never as absence, and its scope names the paired
  group iff there is one. A surface with no compare (``pairing ==
  'none'``) keeps the pre-#1366 own-group copy verbatim and never
  mentions a pairing or the other pathway. A pending pairing is reported
  as "could not be checked". Only when both were checked may the tooltip
  claim absence, and then it names what was checked: the pair when there
  is one, the other pathway's noun when there is none — for a masterless
  row too.
* A masterless row with no pair has nothing to look up, so a failed
  lookup is irrelevant to it, not "unavailable".
* Every disabled offer carries a tooltip; an enabled paired offer names
  the pair; an enabled own-key offer carries none.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from tests.node_jsonl_worker import NodeJsonlWorker

ROOT = Path(__file__).resolve().parent.parent

_OFFER_WORKER = """
import { replaceOfferState } from './web/js/replace_offer.js';
async function handle(operation, payload) {
  if (operation !== 'offer') throw new Error(`unknown operation: ${operation}`);
  return replaceOfferState(payload);
}
"""

Source = Literal["mb", "discogs"]
Kind = Literal["work", "release"]
Pairing = Literal["none", "pending", "checked"]
REASONS = frozenset({
    "own", "paired", "lookup_unavailable", "no_request", "masterless",
    "pairing_unchecked", "no_pair", "pair_inactive", "masterless_no_pair",
})
NO_REQUEST_TITLE = "No existing request in this release group"


@dataclass(frozen=True)
class Pair:
    id: str
    kind: Kind
    source: Source
    label: str


@dataclass(frozen=True)
class World:
    own_key: str | None
    own_active: bool
    lookup_failed: bool
    pairing: Pairing
    pair: Pair | None
    pair_active: bool
    row_source: Source

    def payload(self) -> dict[str, object]:
        pair = (
            None if self.pair is None else {
                "id": self.pair.id, "kind": self.pair.kind,
                "source": self.pair.source, "label": self.pair.label,
            }
        )
        return {
            "ownKey": self.own_key,
            "ownActive": self.own_active,
            "lookupFailed": self.lookup_failed,
            "pairing": self.pairing,
            "pair": pair,
            "pairActive": self.pair_active,
            "rowSource": self.row_source,
        }

    @property
    def effective_pair(self) -> Pair | None:
        """The pair the decision may believe: only a checked one."""
        return self.pair if self.pairing == "checked" else None


def _other(source: Source) -> Source:
    return "discogs" if source == "mb" else "mb"


def _pair_noun(pair: Pair) -> str:
    if pair.source == "mb":
        return "MusicBrainz release group"
    return "Discogs release" if pair.kind == "release" else "Discogs master"


def _other_noun(row_source: Source) -> str:
    return (
        "Discogs master or release" if row_source == "mb"
        else "MusicBrainz release group"
    )


@st.composite
def worlds(draw: st.DrawFn) -> World:
    row_source: Source = draw(st.sampled_from(("mb", "discogs")))
    own_key = draw(st.one_of(st.none(), st.text(min_size=1, max_size=40)))
    if draw(st.booleans()):
        pair_source = _other(row_source)
        kind: Kind = (
            draw(st.sampled_from(("work", "release")))
            if pair_source == "discogs" else "work"
        )
        pair: Pair | None = Pair(
            id=draw(st.text(min_size=1, max_size=40)),
            kind=kind,
            source=pair_source,
            label=draw(st.text(max_size=60)),
        )
    else:
        pair = None
    return World(
        own_key=own_key,
        own_active=draw(st.booleans()),
        lookup_failed=draw(st.booleans()),
        pairing=draw(st.sampled_from(("none", "pending", "checked"))),
        pair=pair,
        pair_active=draw(st.booleans()),
        row_source=row_source,
    )


# ---------------------------------------------------------------------------
# Checker — accumulating, every clause evaluates.
# ---------------------------------------------------------------------------

def offer_violations(world: World, offer: object) -> list[str]:
    out: list[str] = []
    if not isinstance(offer, dict):
        return [f"offer is not an object: {offer!r}"]
    enabled = offer.get("enabled")
    reason = offer.get("reason")
    title = offer.get("title")
    if not isinstance(enabled, bool) or not isinstance(reason, str) \
            or not isinstance(title, str):
        return [f"offer fields are not (bool, str, str): {offer!r}"]
    if reason not in REASONS:
        out.append(f"unknown reason {reason!r}")

    pair = world.effective_pair
    own_holds = world.own_key is not None and world.own_active
    pair_holds = pair is not None and world.pair_active
    expected_enabled = own_holds or pair_holds
    if enabled != expected_enabled:
        out.append(
            f"enabled={enabled} but own_holds={own_holds} pair_holds={pair_holds}"
        )
    if enabled != (reason in ("own", "paired")):
        out.append(f"enabled={enabled} disagrees with reason {reason!r}")
    if own_holds and reason != "own":
        out.append(f"own key holds a request but reason is {reason!r}")
    if not own_holds and pair_holds and reason != "paired":
        out.append(f"only the pair holds a request but reason is {reason!r}")

    has_any_key = world.own_key is not None or pair is not None
    if not enabled:
        if world.lookup_failed and has_any_key:
            if reason != "lookup_unavailable":
                out.append(
                    f"failed lookup with a key to check reported {reason!r}"
                )
            if "Could not check" not in title or "No existing request" in title:
                out.append(
                    f"failed lookup described as absence: {title!r}"
                )
            names_pair = "or its paired" in title
            if pair is not None and (
                not names_pair or _pair_noun(pair) not in title
            ):
                out.append(f"failed-lookup scope omits the pair: {title!r}")
            if pair is None and names_pair:
                out.append(f"failed-lookup scope names a pair there is none of: {title!r}")
        elif world.pairing == "none":
            expected = "no_request" if world.own_key is not None else "masterless"
            if reason != expected:
                out.append(f"no compare on this surface: expected {expected!r}, got {reason!r}")
            if title != NO_REQUEST_TITLE:
                out.append(f"no-compare surface copy changed: {title!r}")
            if "pair" in title.lower() or "other pathway" in title:
                out.append(f"no-compare surface mentions a pairing: {title!r}")
        elif world.pairing == "pending":
            if reason != "pairing_unchecked":
                out.append(f"unchecked pairing reported {reason!r}")
            if "could not be checked" not in title:
                out.append(f"unchecked pairing not said: {title!r}")
        elif pair is None:
            expected = "no_pair" if world.own_key is not None else "masterless_no_pair"
            if reason != expected:
                out.append(f"no pair: expected {expected!r}, got {reason!r}")
            if _other_noun(world.row_source) not in title:
                out.append(
                    f"no-pair tooltip does not name the other pathway: "
                    f"{title!r}"
                )
            if world.own_key is None and "no master" not in title:
                out.append(f"masterless no-pair tooltip does not say no master: {title!r}")
        else:
            if reason != "pair_inactive":
                out.append(f"inactive pair reported {reason!r}")
            if pair.label not in title:
                out.append(f"inactive-pair tooltip omits the pair: {title!r}")
            if "either pathway" not in title:
                out.append(f"inactive-pair tooltip does not say both sides: {title!r}")
        if not title:
            out.append("disabled offer carries no tooltip")
    else:
        if reason == "own" and title:
            out.append(f"own-key offer carries a tooltip: {title!r}")
        if reason == "paired":
            if pair is None:
                out.append("paired reason without a checked pair")
            elif pair.label not in title:
                out.append(f"paired offer tooltip omits the pair: {title!r}")
    return out


class _Offer:
    """One Node worker per Python target, shared by the property and pins."""

    worker: NodeJsonlWorker | None = None

    @classmethod
    def get(cls) -> NodeJsonlWorker:
        if cls.worker is None:
            cls.worker = NodeJsonlWorker(_OFFER_WORKER, cwd=ROOT)
        return cls.worker

    @classmethod
    def close(cls) -> None:
        if cls.worker is not None:
            cls.worker.close()
            cls.worker = None


def _drive(world: World) -> object:
    return _Offer.get().request("offer", world.payload())


_MASTER = Pair(id="11052", kind="work", source="discogs", label="Absolution")
_RG = Pair(
    id="6f151223-f3a3-3e57-810f-598f7897006c", kind="work", source="mb",
    label="Absolution",
)
_RELEASE = Pair(id="3938744", kind="release", source="discogs", label="Fraulein")
_BASE = World(
    own_key="rg-own", own_active=False, lookup_failed=False,
    pairing="checked", pair=None, pair_active=False, row_source="mb",
)


def _world(**overrides: object) -> World:
    return replace(_BASE, **overrides)


class TestReplaceOfferGenerated(unittest.TestCase):
    @classmethod
    def tearDownClass(cls) -> None:
        _Offer.close()

    @given(world=worlds())
    @example(world=_BASE)
    @example(world=_world(own_active=True, pair=_MASTER, pair_active=True))
    @example(world=_world(pair=_MASTER, pair_active=True))
    @example(world=_world(own_key=None, pair=_RG, pair_active=True, row_source="discogs"))
    @example(world=_world(own_key=None, own_active=True, lookup_failed=True,
                          pair_active=True, row_source="discogs"))
    @example(world=_world(lookup_failed=True, pairing="pending", pair=_RELEASE))
    @example(world=_world(pairing="pending"))
    @example(world=_world(pair=_MASTER))
    @example(world=_world(own_key=None, lookup_failed=True, row_source="discogs"))
    @example(world=_world(pairing="none"))
    @example(world=_world(pairing="none", own_key=None, row_source="discogs"))
    @example(world=_world(pairing="none", pair=_MASTER, pair_active=True))
    @example(world=_world(pairing="pending", pair=_MASTER, pair_active=True))
    @example(world=_world(lookup_failed=True, pair=_MASTER))
    @example(world=_world(own_key=None, row_source="discogs"))
    # An empty-label pair is still a pair: the runner's fuzz-only kill
    # (M4, 2026-09-06) pinned at the gating tier.
    @example(world=_world(pair=Pair(id="0", kind="work", source="discogs", label="")))
    def test_offer_decision_invariants(self, world: World) -> None:
        violations = offer_violations(world, _drive(world))
        self.assertEqual(violations, [], "\n".join(violations))


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Q1: each clause trips on a planted offer; Q3: quiet on the real one."""

    @classmethod
    def tearDownClass(cls) -> None:
        _Offer.close()

    def _real(self, world: World) -> dict[str, object]:
        offer = _drive(world)
        assert isinstance(offer, dict)
        return dict(offer)

    def test_quiet_on_real_offers(self) -> None:
        for world in (
            _BASE,
            _world(own_active=True),
            _world(own_key=None, pair=_RG, pair_active=True, row_source="discogs"),
            _world(lookup_failed=True, pair=_MASTER),
            _world(lookup_failed=True),
            _world(pairing="none"),
            _world(pairing="none", own_key=None, row_source="discogs"),
            _world(pairing="none", lookup_failed=True),
            _world(pairing="pending", pair=_MASTER, pair_active=True),
            _world(own_key=None, row_source="discogs"),
        ):
            with self.subTest(world=world):
                self.assertEqual(offer_violations(world, self._real(world)), [])

    def test_shape_clauses_trip(self) -> None:
        self.assertEqual(
            offer_violations(_BASE, "nope"), ["offer is not an object: 'nope'"],
        )
        self.assertRegex(
            "\n".join(offer_violations(_BASE, {"enabled": "yes", "reason": "own", "title": ""})),
            r"^offer fields are not",
        )
        self.assertRegex(
            "\n".join(offer_violations(_BASE, {"enabled": False, "reason": "bogus", "title": "x"})),
            r"unknown reason 'bogus'",
        )

    def test_enable_clauses_trip(self) -> None:
        base = self._real(_BASE)
        with self.subTest(clause="enabled disagrees with keys"):
            self.assertRegex(
                "\n".join(offer_violations(_BASE, {**base, "enabled": True, "reason": "own", "title": ""})),
                r"enabled=True but own_holds=False pair_holds=False",
            )
        own = _world(own_active=True, pair=_MASTER, pair_active=True)
        real_own = self._real(own)
        with self.subTest(clause="own beats paired"):
            self.assertRegex(
                "\n".join(offer_violations(own, {**real_own, "reason": "paired", "title": "x Absolution"})),
                r"own key holds a request but reason is 'paired'",
            )
        with self.subTest(clause="enabled/reason disagreement"):
            self.assertRegex(
                "\n".join(offer_violations(own, {**real_own, "reason": "no_pair"})),
                r"enabled=True disagrees with reason 'no_pair'",
            )
        paired = _world(pair=_MASTER, pair_active=True)
        real_paired = self._real(paired)
        with self.subTest(clause="paired reason"):
            self.assertRegex(
                "\n".join(offer_violations(paired, {**real_paired, "reason": "own", "title": ""})),
                r"only the pair holds a request but reason is 'own'",
            )
        with self.subTest(clause="paired tooltip names the pair"):
            self.assertRegex(
                "\n".join(offer_violations(paired, {**real_paired, "title": "somewhere else"})),
                r"paired offer tooltip omits the pair",
            )
        with self.subTest(clause="own-key offer has no tooltip"):
            own_only = _world(own_active=True)
            self.assertRegex(
                "\n".join(offer_violations(own_only, {**self._real(own_only), "title": "chatty"})),
                r"own-key offer carries a tooltip",
            )
        with self.subTest(clause="an unchecked pair cannot enable"):
            # The checker must demand disabled when the pair was handed to
            # a surface whose pairing is not checked, however active it is.
            ignored = _world(pairing="pending", pair=_MASTER, pair_active=True)
            planted = "\n".join(offer_violations(ignored, {"enabled": True, "reason": "paired", "title": "x Absolution"}))
            self.assertRegex(planted, r"enabled=True but own_holds=False pair_holds=False")
            self.assertRegex(planted, r"paired reason without a checked pair")

    def test_honesty_clauses_trip(self) -> None:
        failed = _world(lookup_failed=True, pair=_MASTER)
        real = self._real(failed)
        with self.subTest(clause="failed lookup reason"):
            self.assertRegex(
                "\n".join(offer_violations(failed, {**real, "reason": "no_pair"})),
                r"failed lookup with a key to check reported 'no_pair'",
            )
        with self.subTest(clause="failed lookup described as absence"):
            self.assertRegex(
                "\n".join(offer_violations(failed, {**real, "title": "No existing request in this release group"})),
                r"failed lookup described as absence",
            )
        with self.subTest(clause="failed-lookup scope omits the pair"):
            self.assertRegex(
                "\n".join(offer_violations(failed, {**real, "title": "Could not check for an existing request in this release group. Collapse and re-expand to retry."})),
                r"failed-lookup scope omits the pair",
            )
        failed_alone = _world(lookup_failed=True)
        with self.subTest(clause="failed-lookup scope names a pair there is none of"):
            self.assertRegex(
                "\n".join(offer_violations(failed_alone, {**self._real(failed_alone), "title": "Could not check for an existing request in this release group or its paired Discogs master. Collapse and re-expand to retry."})),
                r"failed-lookup scope names a pair there is none of",
            )
        none = _world(pairing="none")
        real_none = self._real(none)
        with self.subTest(clause="no-compare surface reason"):
            self.assertRegex(
                "\n".join(offer_violations(none, {**real_none, "reason": "no_pair"})),
                r"no compare on this surface: expected 'no_request', got 'no_pair'",
            )
        with self.subTest(clause="no-compare surface copy changed"):
            self.assertRegex(
                "\n".join(offer_violations(none, {**real_none, "title": "No existing request here"})),
                r"no-compare surface copy changed",
            )
        with self.subTest(clause="no-compare surface mentions a pairing"):
            self.assertRegex(
                "\n".join(offer_violations(none, {**real_none, "title": "No existing request in this release group; the other pathway's pairing could not be checked."})),
                r"no-compare surface mentions a pairing",
            )
        none_masterless = _world(pairing="none", own_key=None, row_source="discogs")
        with self.subTest(clause="no-compare surface masterless reason"):
            self.assertRegex(
                "\n".join(offer_violations(none_masterless, {**self._real(none_masterless), "reason": "no_request"})),
                r"no compare on this surface: expected 'masterless', got 'no_request'",
            )
        unchecked = _world(pairing="pending")
        real_u = self._real(unchecked)
        with self.subTest(clause="unchecked pairing reason"):
            self.assertRegex(
                "\n".join(offer_violations(unchecked, {**real_u, "reason": "no_pair"})),
                r"unchecked pairing reported 'no_pair'",
            )
        with self.subTest(clause="unchecked pairing not said"):
            self.assertRegex(
                "\n".join(offer_violations(unchecked, {**real_u, "title": "No existing request in this release group"})),
                r"unchecked pairing not said",
            )
        real_base = self._real(_BASE)
        with self.subTest(clause="no pair reason"):
            self.assertRegex(
                "\n".join(offer_violations(_BASE, {**real_base, "reason": "pair_inactive"})),
                r"no pair: expected 'no_pair', got 'pair_inactive'",
            )
        with self.subTest(clause="no-pair tooltip names the other pathway"):
            self.assertRegex(
                "\n".join(offer_violations(_BASE, {**real_base, "title": "No existing request in this release group."})),
                r"no-pair tooltip does not name the other pathway",
            )
        masterless = _world(own_key=None, row_source="discogs")
        real_m = self._real(masterless)
        with self.subTest(clause="masterless reason"):
            self.assertRegex(
                "\n".join(offer_violations(masterless, {**real_m, "reason": "no_pair"})),
                r"no pair: expected 'masterless_no_pair', got 'no_pair'",
            )
        with self.subTest(clause="masterless no-pair tooltip names the other pathway"):
            self.assertRegex(
                "\n".join(offer_violations(masterless, {**real_m, "title": "This release has no master and no pair."})),
                r"no-pair tooltip does not name the other pathway",
            )
        with self.subTest(clause="masterless no-pair tooltip says no master"):
            self.assertRegex(
                "\n".join(offer_violations(masterless, {**real_m, "title": "No paired MusicBrainz release group was found."})),
                r"masterless no-pair tooltip does not say no master",
            )
        inactive = _world(pair=_MASTER)
        real_i = self._real(inactive)
        with self.subTest(clause="inactive pair reason"):
            self.assertRegex(
                "\n".join(offer_violations(inactive, {**real_i, "reason": "no_pair"})),
                r"inactive pair reported 'no_pair'",
            )
        with self.subTest(clause="inactive pair tooltip omits the pair"):
            self.assertRegex(
                "\n".join(offer_violations(inactive, {**real_i, "title": "No existing request on either pathway."})),
                r"inactive-pair tooltip omits the pair",
            )
        with self.subTest(clause="inactive pair tooltip says both sides"):
            self.assertRegex(
                "\n".join(offer_violations(inactive, {**real_i, "title": 'paired with Discogs master "Absolution"'})),
                r"inactive-pair tooltip does not say both sides",
            )
        with self.subTest(clause="disabled offer without tooltip"):
            self.assertRegex(
                "\n".join(offer_violations(_BASE, {**real_base, "title": ""})),
                r"disabled offer carries no tooltip",
            )


if __name__ == "__main__":
    unittest.main()
