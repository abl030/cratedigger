"""Cross-runtime generated contracts for the browser CD-rip proof adapter."""

from __future__ import annotations

import copy
import unittest
from dataclasses import dataclass
from pathlib import Path

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.json_narrow import is_object_list, is_str_object_dict
from lib.quality import (
    AccurateRipBitMatch,
    CdRipBitVerification,
    CdTocIdentity,
    CtdbWholeDiscMatch,
)
from tests.node_jsonl_worker import NodeJsonlWorker

ROOT = Path(__file__).resolve().parents[1]

_PRESENTATION_WORKER = """
import { cdRipProofPresentation } from './web/js/cd_rip_proof.js';

async function handle(operation, payload) {
  if (operation !== 'present') throw new Error(`unknown operation: ${operation}`);
  return cdRipProofPresentation(payload);
}
"""


@dataclass(frozen=True)
class ValidProofWorld:
    proof: CdRipBitVerification
    expected: dict[str, object]


@dataclass(frozen=True)
class InvalidWireWorld:
    proof: CdRipBitVerification
    mutation: str


def _expected_presentation(
    proof: CdRipBitVerification,
) -> dict[str, object]:
    ctdb = proof.ctdb
    accuraterip = proof.accuraterip
    if ctdb is not None and accuraterip is not None:
        minimum = min(accuraterip.track_confidences)
        return {
            "provider": "ctdb+accuraterip",
            "confidence": None,
            "text": (
                f"CD bit-verified · CTDB confidence {ctdb.confidence} + "
                f"AccurateRip min confidence {minimum}"
            ),
        }
    if ctdb is not None:
        return {
            "provider": "ctdb",
            "confidence": ctdb.confidence,
            "text": f"CD bit-verified · CTDB confidence {ctdb.confidence}",
        }
    if accuraterip is None:
        raise AssertionError("valid proof world has no positive provider")
    minimum = min(accuraterip.track_confidences)
    return {
        "provider": "accuraterip",
        "confidence": minimum,
        "text": f"CD bit-verified · AccurateRip min confidence {minimum}",
    }


@st.composite
def valid_proof_worlds(draw: st.DrawFn) -> ValidProofWorld:
    track_count = draw(st.integers(min_value=1, max_value=20))
    gaps = draw(st.lists(
        st.integers(min_value=1, max_value=2_000),
        min_size=track_count,
        max_size=track_count,
    ))
    offsets = [0]
    for gap in gaps[:-1]:
        offsets.append(offsets[-1] + gap)
    leadout = offsets[-1] + gaps[-1]
    toc = CdTocIdentity(
        track_offsets_sectors=offsets,
        leadout_sector=leadout,
        accuraterip_id=f"generated-ar-id-{track_count}",
        musicbrainz_disc_id=f"generated-mb-disc-id-{leadout}",
    )

    provider_shape = draw(st.sampled_from(("ctdb", "accuraterip", "both")))
    accuraterip: AccurateRipBitMatch | None = None
    ctdb: CtdbWholeDiscMatch | None = None
    if provider_shape in ("accuraterip", "both"):
        confidences = draw(st.lists(
            st.integers(min_value=1, max_value=10_000),
            min_size=track_count,
            max_size=track_count,
        ))
        checksums = draw(st.lists(
            st.integers(min_value=0, max_value=0xFFFFFFFF),
            min_size=track_count,
            max_size=track_count,
        ))
        accuraterip = AccurateRipBitMatch(
            provider="accuraterip",
            url="https://www.accuraterip.com/generated.bin",
            checksum_version=draw(st.sampled_from(("arv1", "arv2"))),
            read_offset_samples=draw(st.integers(min_value=-5000, max_value=5000)),
            track_confidences=confidences,
            track_checksums=checksums,
            response_sha256=draw(st.sampled_from(("a" * 64, "f" * 64))),
        )
    if provider_shape in ("ctdb", "both"):
        shift = draw(st.integers(min_value=0, max_value=1_000))
        ctdb = CtdbWholeDiscMatch(
            provider="ctdb",
            url="https://db.cue.tools/generated",
            entry_id=f"generated-entry-{leadout}",
            confidence=draw(st.integers(min_value=1, max_value=10_000)),
            crc32=draw(st.integers(min_value=0, max_value=0xFFFFFFFF)),
            stride_samples=5880,
            response_toc_sectors=[
                *(offset + shift for offset in offsets),
                leadout + shift,
            ],
            response_toc_shift_sectors=shift,
            response_sha256=draw(st.sampled_from(("0" * 64, "e" * 64))),
        )

    proof = CdRipBitVerification(
        provenance=draw(st.sampled_from(("measured", "carried"))),
        source_format=draw(st.sampled_from(("flac", "alac"))),
        toc=toc,
        accuraterip=accuraterip,
        ctdb=ctdb,
    )
    if errors := proof.validation_errors():
        raise AssertionError(f"strategy produced invalid CD proof: {errors}")
    return ValidProofWorld(proof=proof, expected=_expected_presentation(proof))


_COMMON_MUTATIONS = (
    "algorithm",
    "provenance",
    "source_format",
    "toc_first_offset",
    "toc_leadout",
    "toc_identity",
    "no_providers",
    "adapter_source_format_camel_case",
    "adapter_toc_offsets_camel_case",
)
_CTDB_MUTATIONS = (
    "ctdb_provider",
    "ctdb_url",
    "ctdb_confidence",
    "ctdb_response_toc",
    "ctdb_response_shift",
    "ctdb_sha",
    "adapter_ctdb_response_sha_camel_case",
)
_ACCURATERIP_MUTATIONS = (
    "accuraterip_provider",
    "accuraterip_url",
    "accuraterip_checksum_version",
    "accuraterip_read_offset",
    "accuraterip_confidence",
    "accuraterip_confidence_cardinality",
    "accuraterip_checksum",
    "accuraterip_sha",
    "adapter_accuraterip_confidences_camel_case",
)


@st.composite
def invalid_wire_worlds(draw: st.DrawFn) -> InvalidWireWorld:
    world = draw(valid_proof_worlds())
    mutations: list[str] = list(_COMMON_MUTATIONS)
    if world.proof.ctdb is not None:
        mutations.extend(_CTDB_MUTATIONS)
    if world.proof.accuraterip is not None:
        mutations.extend(_ACCURATERIP_MUTATIONS)
    return InvalidWireWorld(
        proof=world.proof,
        mutation=draw(st.sampled_from(mutations)),
    )


def _wire_dict(proof: CdRipBitVerification) -> dict[str, object]:
    wire: object = msgspec.to_builtins(proof)
    if not is_str_object_dict(wire):
        raise TypeError("CdRipBitVerification did not serialize as an object")
    return wire


def _nested_dict(parent: dict[str, object], key: str) -> dict[str, object]:
    value = parent.get(key)
    if not is_str_object_dict(value):
        raise TypeError(f"expected object at {key}")
    return value


def _integer_list(parent: dict[str, object], key: str) -> list[object]:
    value = parent.get(key)
    if not is_object_list(value) or not all(
        isinstance(item, int) for item in value
    ):
        raise AssertionError(f"expected integer array at {key}")
    return value


def _mutate_wire(wire: dict[str, object], mutation: str) -> None:
    toc = _nested_dict(wire, "toc")
    if mutation == "algorithm":
        wire["algorithm"] = "cd-rip-bit-verifier-v0"
    elif mutation == "provenance":
        wire["provenance"] = "guessed"
    elif mutation == "source_format":
        wire["source_format"] = "mp3"
    elif mutation == "toc_first_offset":
        _integer_list(toc, "track_offsets_sectors")[0] = 1
    elif mutation == "toc_leadout":
        toc["leadout_sector"] = _integer_list(toc, "track_offsets_sectors")[-1]
    elif mutation == "toc_identity":
        toc["musicbrainz_disc_id"] = ""
    elif mutation == "no_providers":
        wire["ctdb"] = None
        wire["accuraterip"] = None
    elif mutation == "adapter_source_format_camel_case":
        wire["sourceFormat"] = wire.pop("source_format")
    elif mutation == "adapter_toc_offsets_camel_case":
        toc["trackOffsetsSectors"] = toc.pop("track_offsets_sectors")
    elif mutation.startswith(("ctdb_", "adapter_ctdb_")):
        ctdb = _nested_dict(wire, "ctdb")
        if mutation == "ctdb_provider":
            ctdb["provider"] = "accuraterip"
        elif mutation == "ctdb_url":
            ctdb["url"] = "http://db.cue.tools/generated"
        elif mutation == "ctdb_confidence":
            ctdb["confidence"] = 0
        elif mutation == "ctdb_response_toc":
            response_toc = _integer_list(ctdb, "response_toc_sectors")
            last_sector = response_toc[-1]
            if not isinstance(last_sector, int):
                raise TypeError("CTDB response TOC must contain integers")
            response_toc[-1] = last_sector + 1
        elif mutation == "ctdb_response_shift":
            ctdb["response_toc_shift_sectors"] = -1
        elif mutation == "ctdb_sha":
            ctdb["response_sha256"] = "A" * 64
        elif mutation == "adapter_ctdb_response_sha_camel_case":
            ctdb["responseSha256"] = ctdb.pop("response_sha256")
        else:
            raise AssertionError(f"unknown CTDB mutation: {mutation}")
    elif mutation.startswith(("accuraterip_", "adapter_accuraterip_")):
        accuraterip = _nested_dict(wire, "accuraterip")
        if mutation == "accuraterip_provider":
            accuraterip["provider"] = "ctdb"
        elif mutation == "accuraterip_url":
            accuraterip["url"] = "http://accuraterip.invalid/generated"
        elif mutation == "accuraterip_checksum_version":
            accuraterip["checksum_version"] = "arv3"
        elif mutation == "accuraterip_read_offset":
            accuraterip["read_offset_samples"] = 5001
        elif mutation == "accuraterip_confidence":
            _integer_list(accuraterip, "track_confidences")[0] = 0
        elif mutation == "accuraterip_confidence_cardinality":
            _integer_list(accuraterip, "track_confidences").pop()
        elif mutation == "accuraterip_checksum":
            _integer_list(accuraterip, "track_checksums")[0] = 0x100000000
        elif mutation == "accuraterip_sha":
            accuraterip["response_sha256"] = "B" * 64
        elif mutation == "adapter_accuraterip_confidences_camel_case":
            accuraterip["trackConfidences"] = accuraterip.pop(
                "track_confidences"
            )
        else:
            raise AssertionError(f"unknown AccurateRip mutation: {mutation}")
    else:
        raise AssertionError(f"unknown mutation: {mutation}")


def _assert_producer_rejects_wire(wire: dict[str, object]) -> None:
    try:
        decoded = msgspec.convert(wire, type=CdRipBitVerification)
    except (TypeError, msgspec.ValidationError):
        return
    errors = decoded.validation_errors()
    if not errors:
        raise AssertionError("known-bad wire remained valid to the Python producer")


def _assert_exact_presentation(
    expected: dict[str, object],
    actual: object,
) -> None:
    if actual != expected:
        raise AssertionError(f"presentation mismatch: {actual!r} != {expected!r}")


class TestCdRipProofCrossRuntimeGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = NodeJsonlWorker(_PRESENTATION_WORKER, cwd=ROOT)
        self.addCleanup(self.worker.close)

    @given(world=valid_proof_worlds())
    def test_python_producer_and_real_javascript_stay_in_exact_parity(
        self,
        world: ValidProofWorld,
    ) -> None:
        actual = self.worker.request("present", _wire_dict(world.proof))
        _assert_exact_presentation(world.expected, actual)

    @given(world=invalid_wire_worlds())
    def test_known_bad_wire_and_adapter_mutations_fail_closed(
        self,
        world: InvalidWireWorld,
    ) -> None:
        wire = copy.deepcopy(_wire_dict(world.proof))
        _mutate_wire(wire, world.mutation)
        if not world.mutation.startswith("adapter_"):
            _assert_producer_rejects_wire(wire)
        self.assertIsNone(
            self.worker.request("present", wire),
            f"JavaScript admitted {world.mutation}",
        )

    def test_checker_rejects_dual_provider_elision_adapter_mutant(self) -> None:
        expected = {
            "provider": "ctdb+accuraterip",
            "confidence": None,
            "text": (
                "CD bit-verified · CTDB confidence 11 + "
                "AccurateRip min confidence 3"
            ),
        }
        provider_elision_mutant = {
            "provider": "ctdb",
            "confidence": 11,
            "text": "CD bit-verified · CTDB confidence 11",
        }
        with self.assertRaisesRegex(AssertionError, "presentation mismatch"):
            _assert_exact_presentation(expected, provider_elision_mutant)

    def test_checker_rejects_nonconservative_confidence_adapter_mutant(
        self,
    ) -> None:
        expected = {
            "provider": "accuraterip",
            "confidence": 3,
            "text": "CD bit-verified · AccurateRip min confidence 3",
        }
        maximum_confidence_mutant = {
            "provider": "accuraterip",
            "confidence": 9,
            "text": "CD bit-verified · AccurateRip min confidence 9",
        }
        with self.assertRaisesRegex(AssertionError, "presentation mismatch"):
            _assert_exact_presentation(expected, maximum_confidence_mutant)


if __name__ == "__main__":
    unittest.main()
