"""Generated corpus-boundary coverage for native decision pairing.

The replay is not an ordered sidecar map.  It is an exported JSONL evidence
graph: candidate request FKs select exact current evidence rows after all rows
have passed the strict corpus wire schema and production's evidence decoder.
These properties drive that whole shipped path in both decision arms.
"""

from __future__ import annotations

import json
import unittest
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from scripts.decision_differential import (
    RenderDifferentialError,
    decide_corpus,
    decide_row,
    read_decision_corpus,
    without_persisted_proof,
)
from tests.test_decision_differential import _corpus_row

_RELEASE_ID = "mbid-generated-native-pairing"
_PROOF_COLUMNS: dict[str, object] = {
    "verified_lossless": True,
    "verified_lossless_provenance": "measured",
    "verified_lossless_source": "flac",
    "verified_lossless_classifier": "spectral_verified_lossless_v3",
    "verified_lossless_detail": "genuine",
}


@dataclass(frozen=True)
class NativePairingWorld:
    rows: tuple[dict[str, object], ...]
    permuted_rows: tuple[dict[str, object], ...]


@st.composite
def native_pairing_worlds(draw) -> NativePairingWorld:
    """Valid complete corpus rows with an arbitrary current-FK graph."""
    evidence_ids = draw(st.lists(
        st.integers(min_value=1, max_value=100),
        min_size=1,
        max_size=8,
        unique=True,
    ))
    candidate_ids = set(draw(st.lists(
        st.sampled_from(evidence_ids),
        min_size=1,
        max_size=len(evidence_ids),
        unique=True,
    )))
    current_by_candidate = {
        evidence_id: draw(st.sampled_from([None, *evidence_ids]))
        for evidence_id in candidate_ids
    }
    rows = tuple(
        _corpus_row(
            id=evidence_id,
            mb_release_id=_RELEASE_ID,
            is_candidate=evidence_id in candidate_ids,
            current_evidence_id=current_by_candidate.get(evidence_id),
            request_mb_release_id=(
                _RELEASE_ID if evidence_id in candidate_ids else None
            ),
            **_PROOF_COLUMNS,
        )
        for evidence_id in evidence_ids
    )
    return NativePairingWorld(
        rows=rows,
        permuted_rows=tuple(draw(st.permutations(rows))),
    )


def _write_corpus(path: Path, rows: tuple[dict[str, object], ...]) -> None:
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8",
    )


def _read_decided(path: Path) -> dict[int, dict[str, object]]:
    decided: dict[int, dict[str, object]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        evidence_id = row["id"]
        fields = row["fields"]
        if not isinstance(evidence_id, int) or not isinstance(fields, dict):
            raise TypeError("decide output no longer has its JSON contract")
        decided[evidence_id] = fields
    return decided


def _expected_replay(
    rows: tuple[dict[str, object], ...],
    *,
    counterfactual: bool,
) -> dict[int, dict[str, object]]:
    """Expected outputs from raw FKs, independently of the corpus resolver."""
    by_id: dict[int, dict[str, object]] = {}
    for row in rows:
        evidence_id = row["id"]
        assert isinstance(evidence_id, int)
        by_id[evidence_id] = row
    expected: dict[int, dict[str, object]] = {}
    for candidate in rows:
        if not candidate["is_candidate"]:
            continue
        current_id = candidate["current_evidence_id"]
        assert current_id is None or isinstance(current_id, int)
        candidate_id = candidate["id"]
        assert isinstance(candidate_id, int)
        current = by_id[current_id] if current_id is not None else None
        expected[candidate_id] = decide_row(
            candidate,
            current=current,
            counterfactual=counterfactual,
        ).fields
    return expected


def assert_native_replay_matches(
    rows: tuple[dict[str, object], ...],
    actual: dict[int, dict[str, object]],
    *,
    counterfactual: bool,
) -> None:
    """Check complete-corpus resolution and candidate-only counterfactuals."""
    expected = _expected_replay(rows, counterfactual=counterfactual)
    if actual.keys() != expected.keys():
        raise AssertionError(
            f"candidate output ids {sorted(actual)} != {sorted(expected)}")
    for evidence_id, fields in expected.items():
        if actual[evidence_id] != fields:
            raise AssertionError(
                f"candidate evidence {evidence_id} did not receive its exact "
                "current evidence or counterfactual input")


def _decide_world(
    root: Path,
    rows: tuple[dict[str, object], ...],
    *,
    counterfactual: bool,
    suffix: str,
) -> dict[int, dict[str, object]]:
    corpus = root / f"corpus-{suffix}.jsonl"
    output = root / f"decided-{suffix}.jsonl"
    _write_corpus(corpus, rows)
    decided = decide_corpus(
        str(corpus), str(output), counterfactual=counterfactual,
    )
    expected_count = sum(row["is_candidate"] is True for row in rows)
    if decided != expected_count:
        raise AssertionError(f"decided {decided} rows, expected {expected_count}")
    return _read_decided(output)


class TestNativeCurrentPairingGenerated(unittest.TestCase):
    @given(world=native_pairing_worlds())
    def test_complete_json_corpus_replays_exact_current_by_fk_in_both_arms(
        self,
        world: NativePairingWorld,
    ) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for counterfactual in (False, True):
                actual = _decide_world(
                    root,
                    world.rows,
                    counterfactual=counterfactual,
                    suffix=f"ordered-{counterfactual}",
                )
                permuted = _decide_world(
                    root,
                    world.permuted_rows,
                    counterfactual=counterfactual,
                    suffix=f"permuted-{counterfactual}",
                )
                assert_native_replay_matches(
                    world.rows, actual, counterfactual=counterfactual,
                )
                assert_native_replay_matches(
                    world.rows, permuted, counterfactual=counterfactual,
                )
                self.assertEqual(permuted, actual)

    @given(
        request_release=st.text(min_size=1, max_size=12),
        sibling_release=st.text(min_size=1, max_size=12),
        mismatch_current=st.booleans(),
    )
    def test_cross_release_pairings_are_rejected_at_the_json_boundary(
        self,
        request_release: str,
        sibling_release: str,
        mismatch_current: bool,
    ) -> None:
        from hypothesis import assume

        assume(request_release != sibling_release)
        candidate = _corpus_row(
            id=1,
            mb_release_id=(request_release if mismatch_current else sibling_release),
            current_evidence_id=2,
            request_mb_release_id=request_release,
        )
        current = _corpus_row(
            id=2,
            mb_release_id=(sibling_release if mismatch_current else request_release),
            is_candidate=False,
            current_evidence_id=None,
            request_mb_release_id=None,
        )
        with TemporaryDirectory() as tmp:
            corpus = Path(tmp) / "cross-release.jsonl"
            _write_corpus(corpus, (candidate, current))
            with self.assertRaises(RenderDifferentialError):
                read_decision_corpus(str(corpus))

    def test_replay_checker_rejects_fault_injected_resolver_decoder_and_wiring(self):
        """Qualification pin: the property checker rejects its own bad worlds."""
        candidate = _corpus_row(
            id=1,
            current_evidence_id=2,
            spectral_grade="suspect",
            ultrasonic_deficit_db=65.16,
            v0_min_bitrate_kbps=219,
            v0_avg_bitrate_kbps=241,
            v0_median_bitrate_kbps=241,
        )
        current = _corpus_row(
            id=2,
            source_path="/Beets/installed",
            min_bitrate_kbps=128,
            avg_bitrate_kbps=128,
            median_bitrate_kbps=128,
            format="MP3",
            is_cbr=True,
            spectral_grade=None,
            spectral_subject=None,
            spectral_provenance=None,
            codec_family=None,
            spectral_measurement_version=None,
            codec="mp3",
            container="mp3",
            storage_format="mp3",
            filetype_band="mp3",
            target_format=None,
            target_is_cbr=None,
            v0_min_bitrate_kbps=219,
            v0_avg_bitrate_kbps=240,
            v0_median_bitrate_kbps=240,
            v0_subject="source",
            is_candidate=False,
            request_mb_release_id=None,
            files=[{
                "relative_path": "01.mp3", "size_bytes": 1, "mtime_ns": 1,
                "extension": "mp3", "container": "mp3", "codec": "mp3",
                "decode_ok": True,
            }],
            **_PROOF_COLUMNS,
        )
        rows = (candidate, current)
        faults = {
            "resolver": decide_row(candidate, current=None, counterfactual=True),
            "decoder": decide_row(
                {**candidate, "source_path": ""},
                current=current,
                counterfactual=True,
            ),
            "current-proof wiring": decide_row(
                candidate,
                current=without_persisted_proof(current),
                counterfactual=True,
            ),
        }
        for name, rendered in faults.items():
            with self.subTest(name=name), self.assertRaises(AssertionError):
                assert_native_replay_matches(
                    rows,
                    {1: rendered.fields},
                    counterfactual=True,
                )


if __name__ == "__main__":
    unittest.main()
