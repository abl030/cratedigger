"""Generated hostile-input contracts for the real browser reference parser."""

from __future__ import annotations

import string
import unittest
from dataclasses import dataclass
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.node_jsonl_worker import NodeJsonlWorker

ROOT = Path(__file__).resolve().parents[1]
MB_RELEASE_ID = "c1f6a2c9-bcba-4e69-96f5-233c85b2830a"


@dataclass(frozen=True)
class ReferenceWorld:
    pasted: str
    expected: dict[str, str] | None


_REFERENCE_WORKER = """
import { parsePastedId } from './web/js/util.js';
async function handle(operation, payload) {
  if (operation !== 'parse') throw new Error(`unknown operation: ${operation}`);
  return parsePastedId(payload);
}
"""


def _parse_with_real_javascript(
    worker: NodeJsonlWorker,
    pasted: str,
) -> object:
    return worker.request("parse", pasted)


@st.composite
def canonical_reference_worlds(draw: st.DrawFn) -> ReferenceWorld:
    family = draw(st.sampled_from(("mb", "discogs")))
    as_url = draw(st.booleans())
    if family == "mb":
        release_id = str(draw(st.uuids()))
        if not as_url:
            return ReferenceWorld(
                pasted=release_id.upper() if draw(st.booleans()) else release_id,
                expected={"family": "mb", "kind": "unknown", "id": release_id},
            )
        kind = draw(st.sampled_from(("release", "release-group")))
        host = "musicbrainz.org"
        path = f"/{kind}/{release_id}"
    else:
        release_id = str(draw(st.integers(min_value=1, max_value=999_999_999_999)))
        if not as_url:
            return ReferenceWorld(
                pasted=release_id,
                expected={
                    "family": "discogs", "kind": "unknown", "id": release_id,
                },
            )
        kind = draw(st.sampled_from(("release", "master")))
        host = draw(st.sampled_from(("discogs.com", "www.discogs.com")))
        slug = draw(
            st.text(
                alphabet=string.ascii_letters + string.digits + "-",
                min_size=0,
                max_size=30,
            )
        )
        path = f"/{kind}/{release_id}" + (f"-{slug}" if slug else "")

    scheme = draw(st.sampled_from(("https://", "http://", "")))
    suffix = draw(st.sampled_from(("", "/", "?utm_source=mobile", "#images")))
    return ReferenceWorld(
        pasted=f"{scheme}{host}{path}{suffix}",
        expected={"family": family, "kind": kind, "id": release_id},
    )


@st.composite
def hostile_reference_worlds(draw: st.DrawFn) -> ReferenceWorld:
    family = draw(st.sampled_from(("mb", "discogs")))
    if family == "mb":
        host = "musicbrainz.org"
        path = f"/release/{draw(st.uuids())}"
    else:
        host = draw(st.sampled_from(("discogs.com", "www.discogs.com")))
        release_id = draw(st.integers(min_value=1, max_value=999_999_999_999))
        path = f"/release/{release_id}"

    variant = draw(st.sampled_from((
        "attacker_path",
        "scheme",
        "host_suffix",
        "userinfo",
        "port",
        "extra_path",
        "control",
        "oversized",
    )))
    if variant == "attacker_path":
        pasted = f"https://evil.example/{host}{path}"
    elif variant == "scheme":
        pasted = f"{draw(st.sampled_from(('javascript', 'ftp', 'file')))}://{host}{path}"
    elif variant == "host_suffix":
        pasted = f"https://{host}.evil.example{path}"
    elif variant == "userinfo":
        pasted = f"https://:x@{host}{path}"
    elif variant == "port":
        pasted = f"https://{host}:444{path}"
    elif variant == "extra_path":
        pasted = f"https://{host}{path}/extra"
    elif variant == "control":
        pasted = f"https://{host}\n{path}"
    else:
        pasted = f"https://{host}{path}?{'x' * 2048}"
    return ReferenceWorld(pasted=pasted, expected=None)


class TestBrowseReferenceParserGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = NodeJsonlWorker(_REFERENCE_WORKER, cwd=ROOT)
        self.addCleanup(self.worker.close)

    @given(st.one_of(canonical_reference_worlds(), hostile_reference_worlds()))
    @example(ReferenceWorld(
        pasted=(
            "https://evil.example/musicbrainz.org/release/"
            + MB_RELEASE_ID
        ),
        expected=None,
    ))
    def test_only_exact_canonical_references_produce_normalized_ids(
        self, world: ReferenceWorld,
    ) -> None:
        self.assertEqual(
            _parse_with_real_javascript(self.worker, world.pasted),
            world.expected,
        )


if __name__ == "__main__":
    unittest.main()
