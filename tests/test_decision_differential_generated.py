"""Generated corpus-boundary coverage for native decision pairing.

The replay is not an ordered sidecar map.  It is an exported JSONL evidence
graph: candidate request FKs select exact current evidence rows after all rows
have passed the strict corpus wire schema and production's evidence decoder.
These properties drive that whole shipped path in both decision arms.
"""

from __future__ import annotations

import json
import unittest
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.quality import AlbumQualityEvidenceFile
from lib.quality_evidence import snapshot_fingerprint
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
CurrentEvidenceProfile = Literal["low", "proof", "high"]
_CURRENT_PROFILES: tuple[CurrentEvidenceProfile, ...] = (
    "low", "proof", "high",
)
CorpusMutation = Literal[
    "duplicate_id",
    "dangling_current",
    "missing_required",
    "wrong_top_level",
    "wrong_file_primitive",
    "wrong_evidence_primitive",
    "null_db_not_null_evidence",
]
CorpusMutationRole = Literal["candidate", "referenced_current"]
_CORPUS_MUTATIONS: tuple[CorpusMutation, ...] = (
    "duplicate_id",
    "dangling_current",
    "missing_required",
    "wrong_top_level",
    "wrong_file_primitive",
    "wrong_evidence_primitive",
    "null_db_not_null_evidence",
)
_CORPUS_MUTATION_ROLES: tuple[CorpusMutationRole, ...] = (
    "candidate", "referenced_current",
)


@dataclass(frozen=True)
class NativePairingWorld:
    rows: tuple[dict[str, object], ...]
    permuted_rows: tuple[dict[str, object], ...]


def _profiled_evidence_row(
    evidence_id: int,
    *,
    profile: CurrentEvidenceProfile,
    is_candidate: bool,
    current_evidence_id: int | None,
) -> dict[str, object]:
    """One complete row whose current-side outcome differs by profile.

    A rejected candidate imports over ``low``, is locked by ``proof``, and is
    downgraded by ``high``.  Thus any resolver substitution among these valid
    current IDs is observable at the real JSONL-to-decision boundary.
    """
    bitrate = {"low": 96, "proof": 128, "high": 320}[profile]
    proof = _PROOF_COLUMNS if profile == "proof" else {}
    return _corpus_row(
        id=evidence_id,
        mb_release_id=_RELEASE_ID,
        source_path=f"/Beets/generated-{profile}-{evidence_id}",
        min_bitrate_kbps=bitrate,
        avg_bitrate_kbps=bitrate,
        median_bitrate_kbps=bitrate,
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
        is_candidate=is_candidate,
        current_evidence_id=current_evidence_id,
        request_mb_release_id=(
            _RELEASE_ID if is_candidate else None
        ),
        files=[{
            "relative_path": f"{evidence_id:02d}.mp3",
            "size_bytes": evidence_id,
            "mtime_ns": evidence_id,
            "extension": "mp3", "container": "mp3", "codec": "mp3",
            "decode_ok": True,
        }],
        **proof,
    )


def _snapshot_fingerprint_from_row(row: dict[str, object]) -> str:
    """Recompute the production content address from a corpus wire row."""
    files = msgspec.convert(
        row["files"],
        type=list[AlbumQualityEvidenceFile],
    )
    return snapshot_fingerprint(files)


def assert_generated_evidence_addresses(
    rows: tuple[dict[str, object], ...],
) -> None:
    """Generated rows must be valid distinct content-addressed evidence."""
    addresses: set[tuple[str, str]] = set()
    for row in rows:
        release_id = row["mb_release_id"]
        fingerprint = row["snapshot_fingerprint"]
        if not isinstance(release_id, str) or not isinstance(fingerprint, str):
            raise TypeError("generated evidence has no content address")
        if fingerprint != _snapshot_fingerprint_from_row(row):
            raise AssertionError(
                f"generated evidence {row['id']!r} fingerprint does not match "
                "its manifest")
        address = (release_id, fingerprint)
        if address in addresses:
            raise AssertionError(
                f"generated evidence has duplicate content address {address!r}")
        addresses.add(address)


@st.composite
def native_pairing_worlds(draw) -> NativePairingWorld:
    """Valid graph with a current-only row referenced by a candidate."""
    evidence_ids = draw(st.lists(
        st.integers(min_value=1, max_value=100),
        min_size=len(_CURRENT_PROFILES),
        max_size=len(_CURRENT_PROFILES),
        unique=True,
    ))
    candidate_ids = draw(st.lists(
        st.sampled_from(evidence_ids),
        min_size=1,
        max_size=len(evidence_ids) - 1,
        unique=True,
    ))
    candidate_id_set = set(candidate_ids)
    current_only_ids = [
        evidence_id for evidence_id in evidence_ids
        if evidence_id not in candidate_id_set
    ]
    paired_candidate_id = draw(st.sampled_from(candidate_ids))
    current_by_candidate = {
        evidence_id: (
            draw(st.sampled_from(current_only_ids))
            if evidence_id == paired_candidate_id
            else draw(st.sampled_from([None, *evidence_ids]))
        )
        for evidence_id in candidate_ids
    }
    profile_by_evidence_id: dict[int, CurrentEvidenceProfile] = {
        evidence_id: _CURRENT_PROFILES[offset % len(_CURRENT_PROFILES)]
        for offset, evidence_id in enumerate(evidence_ids)
    }
    rows = tuple(
        _profiled_evidence_row(
            evidence_id,
            profile=profile_by_evidence_id[evidence_id],
            is_candidate=evidence_id in candidate_id_set,
            current_evidence_id=current_by_candidate.get(evidence_id),
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


def _mutated_corpus_rows(
    rows: tuple[dict[str, object], ...],
    mutation: CorpusMutation,
    *,
    role: CorpusMutationRole,
) -> tuple[dict[str, object], ...]:
    """Plant one bounded JSONL failure without changing the valid source world."""
    mutated = [deepcopy(row) for row in rows]
    if role == "candidate":
        target = next(row for row in mutated if row["is_candidate"] is True)
    else:
        referenced_ids = {
            current_id
            for row in mutated
            if row["is_candidate"] is True
            for current_id in (row["current_evidence_id"],)
            if isinstance(current_id, int)
        }
        target = next(
            row for row in mutated
            if row["is_candidate"] is False and row["id"] in referenced_ids
        )
    if mutation == "duplicate_id":
        mutated.append(deepcopy(target))
    elif mutation == "dangling_current":
        ids: list[int] = []
        for row in mutated:
            evidence_id = row["id"]
            assert isinstance(evidence_id, int)
            ids.append(evidence_id)
        target["current_evidence_id"] = max(ids) + 1
    elif mutation == "missing_required":
        del target["request_mb_release_id"]
    elif mutation == "wrong_top_level":
        target["is_candidate"] = "true"
    elif mutation == "wrong_file_primitive":
        files = target["files"]
        assert isinstance(files, list)
        file = files[0]
        assert isinstance(file, dict)
        file["size_bytes"] = "1"
    elif mutation == "wrong_evidence_primitive":
        report = target["audio_validation"]
        assert isinstance(report, dict)
        report["files_checked"] = "0"
    elif mutation == "null_db_not_null_evidence":
        target["folder_layout"] = None
        target["audio_file_count"] = None
        target["filetype_band"] = None
    else:  # pragma: no cover - Literal and the complete tuple above guard this.
        raise AssertionError(f"unknown corpus mutation: {mutation}")
    return tuple(mutated)


def assert_corpus_rejected_by_outer_adapters(
    corpus: Path,
    output: Path,
) -> None:
    """Both public JSONL adapters must reject a malformed corpus."""
    accepted: list[str] = []
    for adapter, invoke in (
        ("read_decision_corpus", lambda: read_decision_corpus(str(corpus))),
        ("decide_corpus", lambda: decide_corpus(str(corpus), str(output))),
    ):
        try:
            invoke()
        except RenderDifferentialError:
            continue
        accepted.append(adapter)
    if accepted:
        raise AssertionError(
            f"malformed corpus was accepted by {', '.join(accepted)}")


class TestNativeCurrentPairingGenerated(unittest.TestCase):
    @given(world=native_pairing_worlds())
    def test_complete_json_corpus_replays_exact_current_by_fk_in_both_arms(
        self,
        world: NativePairingWorld,
    ) -> None:
        assert_generated_evidence_addresses(world.rows)
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

    @given(world=native_pairing_worlds())
    def test_jsonl_mutations_fail_closed_at_both_public_adapters(
        self,
        world: NativePairingWorld,
    ) -> None:
        """Every bounded malformed-wire class reaches both outer adapters."""
        assert_generated_evidence_addresses(world.rows)
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for role in _CORPUS_MUTATION_ROLES:
                for mutation in _CORPUS_MUTATIONS:
                    corpus = root / f"{role}-{mutation}.jsonl"
                    output = root / f"{role}-{mutation}-decided.jsonl"
                    _write_corpus(
                        corpus,
                        _mutated_corpus_rows(
                            world.rows,
                            mutation,
                            role=role,
                        ),
                    )
                    try:
                        assert_corpus_rejected_by_outer_adapters(corpus, output)
                    except AssertionError as exc:
                        raise AssertionError(f"{role} {mutation}: {exc}") from exc

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
        import scripts.decision_differential as differential

        candidate = _corpus_row(
            id=1,
            mb_release_id=_RELEASE_ID,
            current_evidence_id=2,
            request_mb_release_id=_RELEASE_ID,
            spectral_grade="suspect",
            ultrasonic_deficit_db=65.16,
            v0_min_bitrate_kbps=219,
            v0_avg_bitrate_kbps=241,
            v0_median_bitrate_kbps=241,
        )
        current = _profiled_evidence_row(
            2,
            profile="proof",
            is_candidate=False,
            current_evidence_id=None,
        )
        wrong_current = _profiled_evidence_row(
            3,
            profile="low",
            is_candidate=False,
            current_evidence_id=None,
        )
        rows = (candidate, current, wrong_current)
        original_resolver = differential.resolve_native_current_pairs

        def wrong_non_null_resolver(entries):
            wrong = next(entry for entry in entries if entry.evidence_id == 3)
            return [
                (entry, wrong)
                for entry in entries
                if entry.is_candidate
            ]

        differential.resolve_native_current_pairs = wrong_non_null_resolver
        try:
            with TemporaryDirectory() as tmp:
                actual = _decide_world(
                    Path(tmp),
                    rows,
                    counterfactual=True,
                    suffix="wrong-non-null-current",
                )
        finally:
            differential.resolve_native_current_pairs = original_resolver
        with self.assertRaises(AssertionError):
            assert_native_replay_matches(
                rows,
                actual,
                counterfactual=True,
            )
        faults = {
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

    def test_mutation_checker_trips_on_an_accepted_corpus(self):
        """Known-bad qualification for the two-adapter mutation checker."""
        candidate = _profiled_evidence_row(
            1,
            profile="low",
            is_candidate=True,
            current_evidence_id=2,
        )
        current = _profiled_evidence_row(
            2,
            profile="proof",
            is_candidate=False,
            current_evidence_id=None,
        )
        rows = (candidate, current)
        assert_generated_evidence_addresses(rows)
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus = root / "valid.jsonl"
            _write_corpus(corpus, rows)
            with self.assertRaises(AssertionError):
                assert_corpus_rejected_by_outer_adapters(
                    corpus,
                    root / "valid-decided.jsonl",
                )
            malformed = _mutated_corpus_rows(
                rows,
                "null_db_not_null_evidence",
                role="referenced_current",
            )
            malformed_candidate = next(
                row for row in malformed if row["is_candidate"] is True)
            self.assertEqual(
                malformed_candidate["folder_layout"],
                candidate["folder_layout"],
            )
            _write_corpus(corpus, malformed)
            assert_corpus_rejected_by_outer_adapters(
                corpus,
                root / "current-null-decided.jsonl",
            )

    def test_content_address_checker_trips_on_a_duplicate_manifest(self):
        """Known-bad qualification for generated content-address fidelity."""
        first = _profiled_evidence_row(
            1,
            profile="low",
            is_candidate=True,
            current_evidence_id=None,
        )
        duplicate = _profiled_evidence_row(
            2,
            profile="proof",
            is_candidate=False,
            current_evidence_id=None,
        )
        duplicate["files"] = deepcopy(first["files"])
        duplicate["snapshot_fingerprint"] = first["snapshot_fingerprint"]
        with self.assertRaises(AssertionError):
            assert_generated_evidence_addresses((first, duplicate))


if __name__ == "__main__":
    unittest.main()
