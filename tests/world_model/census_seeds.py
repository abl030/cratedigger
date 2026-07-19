"""Anonymized categorical seeds sampled read-only from doc2 on 2026-07-19.

The corpus deliberately contains no request IDs, release IDs, artist/title
metadata, peer names, or paths. Counts are capture-time prevalence evidence,
not assertions about a live database that will continue changing.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re


@dataclass(frozen=True)
class WorldCensusSeed:
    """One exact request plus linked-evidence vocabulary/null row shape."""

    name: str
    observed_rows: int
    status: str
    identity_shape: str
    search_override: str | None
    has_imported_path: bool
    has_current_evidence: bool
    lineage_version: int
    final_format: str | None
    codec: str | None
    storage_format: str | None
    measured_format: str | None
    spectral_grade: str | None
    spectral_subject: str | None
    spectral_provenance: str | None
    v0_subject: str | None
    v0_provenance: str | None
    verified_lossless: bool

    @property
    def has_v0_metrics(self) -> bool:
        return self.v0_subject is not None


WORLD_CENSUS_SEEDS = (
    WorldCensusSeed(
        name="imported_mb_lineage1_verified_v0",
        observed_rows=4063,
        status="imported", identity_shape="musicbrainz", search_override=None,
        has_imported_path=True, has_current_evidence=True, lineage_version=1,
        final_format="opus 128", codec="opus", storage_format="Opus",
        measured_format="Opus", spectral_grade="genuine",
        spectral_subject="source", spectral_provenance="carried",
        v0_subject="source", v0_provenance="measured", verified_lossless=True,
    ),
    WorldCensusSeed(
        name="imported_mb_lineage4_verified_v0",
        observed_rows=1284,
        status="imported", identity_shape="musicbrainz", search_override=None,
        has_imported_path=True, has_current_evidence=True, lineage_version=4,
        final_format="opus 128", codec="opus", storage_format="Opus",
        measured_format="Opus", spectral_grade="genuine",
        spectral_subject="source", spectral_provenance="carried",
        v0_subject="source", v0_provenance="carried", verified_lossless=True,
    ),
    WorldCensusSeed(
        name="imported_mb_lineage4_verified_without_v0",
        observed_rows=708,
        status="imported", identity_shape="musicbrainz", search_override=None,
        has_imported_path=True, has_current_evidence=True, lineage_version=4,
        final_format="opus 128", codec="opus", storage_format="Opus",
        measured_format="Opus", spectral_grade="genuine",
        spectral_subject="source", spectral_provenance="carried",
        v0_subject=None, v0_provenance=None, verified_lossless=True,
    ),
    WorldCensusSeed(
        name="imported_dual_lineage1_verified_v0",
        observed_rows=188,
        status="imported", identity_shape="both", search_override=None,
        has_imported_path=True, has_current_evidence=True, lineage_version=1,
        final_format="opus 128", codec="opus", storage_format="Opus",
        measured_format="Opus", spectral_grade="genuine",
        spectral_subject="source", spectral_provenance="carried",
        v0_subject="source", v0_provenance="measured", verified_lossless=True,
    ),
    WorldCensusSeed(
        name="wanted_mb_pristine",
        observed_rows=210,
        status="wanted", identity_shape="musicbrainz", search_override=None,
        has_imported_path=False, has_current_evidence=False, lineage_version=0,
        final_format=None, codec=None, storage_format=None, measured_format=None,
        spectral_grade=None, spectral_subject=None, spectral_provenance=None,
        v0_subject=None, v0_provenance=None, verified_lossless=False,
    ),
    WorldCensusSeed(
        name="wanted_mb_lineage1_without_imported_path",
        observed_rows=249,
        status="wanted", identity_shape="musicbrainz", search_override=None,
        has_imported_path=False, has_current_evidence=True, lineage_version=1,
        final_format=None, codec="mp3", storage_format="MP3",
        measured_format="MP3", spectral_grade=None, spectral_subject=None,
        spectral_provenance=None, v0_subject=None, v0_provenance=None,
        verified_lossless=False,
    ),
    WorldCensusSeed(
        name="wanted_mb_lossless_lineage1_installed",
        observed_rows=97,
        status="wanted", identity_shape="musicbrainz",
        search_override="lossless", has_imported_path=True,
        has_current_evidence=True, lineage_version=1, final_format="MP3",
        codec="mp3", storage_format="MP3", measured_format="MP3",
        spectral_grade="genuine", spectral_subject="installed",
        spectral_provenance="measured", v0_subject="installed",
        v0_provenance="measured", verified_lossless=False,
    ),
    WorldCensusSeed(
        name="wanted_mb_three_tier_without_evidence",
        observed_rows=86,
        status="wanted", identity_shape="musicbrainz",
        search_override="lossless,mp3 v0,mp3 320", has_imported_path=True,
        has_current_evidence=False, lineage_version=0, final_format=None,
        codec=None, storage_format=None, measured_format=None,
        spectral_grade=None, spectral_subject=None, spectral_provenance=None,
        v0_subject=None, v0_provenance=None, verified_lossless=False,
    ),
    WorldCensusSeed(
        name="wanted_mb_full_legacy_ladder_lineage1",
        observed_rows=40,
        status="wanted", identity_shape="musicbrainz",
        search_override="lossless,mp3 v0,mp3 320,aac,opus,ogg",
        has_imported_path=True, has_current_evidence=True, lineage_version=1,
        final_format="MP3", codec="mp3", storage_format="MP3",
        measured_format="MP3", spectral_grade="likely_transcode",
        spectral_subject="installed", spectral_provenance="measured",
        v0_subject="installed", v0_provenance="measured",
        verified_lossless=False,
    ),
    WorldCensusSeed(
        name="wanted_mb_lineage3_without_imported_path",
        observed_rows=19,
        status="wanted", identity_shape="musicbrainz", search_override=None,
        has_imported_path=False, has_current_evidence=True, lineage_version=3,
        final_format=None, codec="mp3", storage_format="MP3",
        measured_format="MP3", spectral_grade=None, spectral_subject=None,
        spectral_provenance=None, v0_subject=None, v0_provenance=None,
        verified_lossless=False,
    ),
    WorldCensusSeed(
        name="downloading_mb_lineage3_likely_transcode",
        observed_rows=1,
        status="downloading", identity_shape="musicbrainz",
        search_override=None, has_imported_path=False,
        has_current_evidence=True, lineage_version=3, final_format=None,
        codec="mp3", storage_format="MP3", measured_format="MP3",
        spectral_grade="likely_transcode", spectral_subject="installed",
        spectral_provenance="measured", v0_subject="installed",
        v0_provenance="measured", verified_lossless=False,
    ),
    WorldCensusSeed(
        name="unsearchable_mb_without_evidence",
        observed_rows=2,
        status="unsearchable", identity_shape="musicbrainz",
        search_override=None, has_imported_path=False,
        has_current_evidence=False, lineage_version=0, final_format=None,
        codec=None, storage_format=None, measured_format=None,
        spectral_grade=None, spectral_subject=None, spectral_provenance=None,
        v0_subject=None, v0_provenance=None, verified_lossless=False,
    ),
    WorldCensusSeed(
        name="replaced_mb_lineage1_without_measurement",
        observed_rows=5,
        status="replaced", identity_shape="musicbrainz", search_override=None,
        has_imported_path=False, has_current_evidence=True, lineage_version=1,
        final_format=None, codec="mp3", storage_format="MP3",
        measured_format="MP3", spectral_grade=None, spectral_subject=None,
        spectral_provenance=None, v0_subject=None, v0_provenance=None,
        verified_lossless=False,
    ),
)


_STATEFUL_NAMES = frozenset({
    "imported_mb_lineage1_verified_v0",
    "imported_mb_lineage4_verified_v0",
    "imported_mb_lineage4_verified_without_v0",
    "imported_dual_lineage1_verified_v0",
    "wanted_mb_pristine",
    "wanted_mb_lineage1_without_imported_path",
    "wanted_mb_lossless_lineage1_installed",
    "wanted_mb_three_tier_without_evidence",
    "wanted_mb_full_legacy_ladder_lineage1",
    "wanted_mb_lineage3_without_imported_path",
})
STATEFUL_WORLD_CENSUS_SEEDS = tuple(
    seed for seed in WORLD_CENSUS_SEEDS if seed.name in _STATEFUL_NAMES
)


_UUID = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_SAFE_NAME_TOKENS = frozenset({
    "both", "downloading", "dual", "evidence", "full", "imported",
    "installed", "ladder", "legacy", "lineage1", "lineage3", "lineage4",
    "likely", "lossless", "mb", "measurement", "pristine", "replaced",
    "path", "three", "tier", "transcode", "unsearchable", "v0", "verified",
    "wanted", "without",
})


def assert_census_seed_anonymized(seed: WorldCensusSeed) -> None:
    """Reject accidental production identity/path material in the corpus."""

    rendered = repr(asdict(seed))
    name_tokens = frozenset(seed.name.split("_"))
    if (
        not name_tokens
        or not name_tokens.issubset(_SAFE_NAME_TOKENS)
        or "/" in rendered
        or "\\" in rendered
        or _UUID.search(rendered) is not None
    ):
        raise AssertionError(f"census seed is not anonymized: {seed.name!r}")
    if seed.observed_rows < 1:
        raise AssertionError("census seed must represent at least one live row")
    if seed.has_current_evidence != (seed.lineage_version > 0):
        raise AssertionError("census evidence presence and lineage disagree")


__all__ = [
    "STATEFUL_WORLD_CENSUS_SEEDS",
    "WORLD_CENSUS_SEEDS",
    "WorldCensusSeed",
    "assert_census_seed_anonymized",
]
