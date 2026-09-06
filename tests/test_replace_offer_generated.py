"""Generated patrol for the inverted Replace button's offer decision
(``web/js/replace_offer.js``, issue #1366 part 2), driven through one Node
worker so the REAL JavaScript answers every world.

Invariants, written down first:

* Enabled iff the row's own key holds an active request, or the paired
  key does. Never enabled from a lookup that failed with nothing else
  active; never enabled from a pair the compare did not produce.
* The reason code is one of the seven, and it is an enabling reason iff
  the offer is enabled. An own-key offer beats a paired one.
* Honesty: a disabled offer explains the FIRST thing that could not be
  answered. A failed lookup (with a key to look up) is reported as
  "could not check", never as absence. An unchecked pairing is reported as
  "could not be checked". Only when both were checked may the tooltip
  claim absence, and then it names what was checked: the pair when there
  is one, the other pathway's noun when there is none.
* A masterless row with no pair has nothing to look up, so a failed
  lookup is irrelevant to it, not "unavailable".
* Every disabled offer carries a tooltip; an enabled paired offer names
  the pair; an enabled own-key offer carries none.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass
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
REASONS = frozenset({
    "own", "paired", "lookup_unavailable", "pairing_unchecked", "no_pair",
    "pair_inactive", "masterless_no_pair",
})


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
    pairing_checked: bool
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
            "pairingChecked": self.pairing_checked,
            "pair": pair,
            "pairActive": self.pair_active,
            "rowSource": self.row_source,
        }


def _other(source: Source) -> Source:
    return "discogs" if source == "mb" else "mb"


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
        pairing_checked=draw(st.booleans()),
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

    own_holds = world.own_key is not None and world.own_active
    pair_holds = world.pair is not None and world.pair_active
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

    has_any_key = world.own_key is not None or world.pair is not None
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
        elif not world.pairing_checked:
            if reason != "pairing_unchecked":
                out.append(f"unchecked pairing reported {reason!r}")
            if "could not be checked" not in title:
                out.append(f"unchecked pairing not said: {title!r}")
        elif world.pair is None:
            expected = "no_pair" if world.own_key is not None else "masterless_no_pair"
            if reason != expected:
                out.append(f"no pair: expected {expected!r}, got {reason!r}")
            if world.own_key is not None:
                noun = (
                    "Discogs master or release" if world.row_source == "mb"
                    else "MusicBrainz release group"
                )
                if noun not in title:
                    out.append(
                        f"no-pair tooltip does not name the other pathway: "
                        f"{title!r}"
                    )
        else:
            if reason != "pair_inactive":
                out.append(f"inactive pair reported {reason!r}")
            if world.pair.label not in title:
                out.append(f"inactive-pair tooltip omits the pair: {title!r}")
            if "either pathway" not in title:
                out.append(f"inactive-pair tooltip does not say both sides: {title!r}")
        if not title:
            out.append("disabled offer carries no tooltip")
    else:
        if reason == "own" and title:
            out.append(f"own-key offer carries a tooltip: {title!r}")
        if reason == "paired":
            assert world.pair is not None
            if world.pair.label not in title:
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
    pairing_checked=True, pair=None, pair_active=False, row_source="mb",
)


class TestReplaceOfferGenerated(unittest.TestCase):
    @classmethod
    def tearDownClass(cls) -> None:
        _Offer.close()

    @given(world=worlds())
    @example(world=_BASE)
    @example(world=World(
        own_key="rg-own", own_active=True, lookup_failed=False,
        pairing_checked=True, pair=_MASTER, pair_active=True, row_source="mb"))
    @example(world=World(
        own_key="rg-own", own_active=False, lookup_failed=False,
        pairing_checked=True, pair=_MASTER, pair_active=True, row_source="mb"))
    @example(world=World(
        own_key=None, own_active=False, lookup_failed=False,
        pairing_checked=True, pair=_RG, pair_active=True, row_source="discogs"))
    @example(world=World(
        own_key=None, own_active=True, lookup_failed=True,
        pairing_checked=True, pair=None, pair_active=True, row_source="discogs"))
    @example(world=World(
        own_key="rg-own", own_active=False, lookup_failed=True,
        pairing_checked=False, pair=_RELEASE, pair_active=False, row_source="mb"))
    @example(world=World(
        own_key="rg-own", own_active=False, lookup_failed=False,
        pairing_checked=False, pair=None, pair_active=False, row_source="mb"))
    @example(world=World(
        own_key="rg-own", own_active=False, lookup_failed=False,
        pairing_checked=True, pair=_MASTER, pair_active=False, row_source="mb"))
    @example(world=World(
        own_key=None, own_active=False, lookup_failed=True,
        pairing_checked=True, pair=None, pair_active=False, row_source="discogs"))
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
            World(own_key="rg-own", own_active=True, lookup_failed=False,
                  pairing_checked=True, pair=None, pair_active=False,
                  row_source="mb"),
            World(own_key=None, own_active=False, lookup_failed=False,
                  pairing_checked=True, pair=_RG, pair_active=True,
                  row_source="discogs"),
            World(own_key="rg-own", own_active=False, lookup_failed=True,
                  pairing_checked=True, pair=_MASTER, pair_active=False,
                  row_source="mb"),
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
        own = World(own_key="rg-own", own_active=True, lookup_failed=False,
                    pairing_checked=True, pair=_MASTER, pair_active=True,
                    row_source="mb")
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
        paired = World(own_key="rg-own", own_active=False, lookup_failed=False,
                       pairing_checked=True, pair=_MASTER, pair_active=True,
                       row_source="mb")
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
            own_only = World(own_key="rg-own", own_active=True, lookup_failed=False,
                             pairing_checked=True, pair=None, pair_active=False,
                             row_source="mb")
            self.assertRegex(
                "\n".join(offer_violations(own_only, {**self._real(own_only), "title": "chatty"})),
                r"own-key offer carries a tooltip",
            )

    def test_honesty_clauses_trip(self) -> None:
        failed = World(own_key="rg-own", own_active=False, lookup_failed=True,
                       pairing_checked=True, pair=_MASTER, pair_active=False,
                       row_source="mb")
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
        unchecked = World(own_key="rg-own", own_active=False, lookup_failed=False,
                          pairing_checked=False, pair=None, pair_active=False,
                          row_source="mb")
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
        inactive = World(own_key="rg-own", own_active=False, lookup_failed=False,
                         pairing_checked=True, pair=_MASTER, pair_active=False,
                         row_source="mb")
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
        masterless = World(own_key=None, own_active=False, lookup_failed=False,
                           pairing_checked=True, pair=None, pair_active=False,
                           row_source="discogs")
        with self.subTest(clause="masterless reason"):
            self.assertRegex(
                "\n".join(offer_violations(masterless, {**self._real(masterless), "reason": "no_pair"})),
                r"no pair: expected 'masterless_no_pair', got 'no_pair'",
            )


if __name__ == "__main__":
    unittest.main()
