"""Generated outer-adapter contract for #1018 media identity."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.beets_db import AlbumInfo, BeetsDB
from lib.evidence_media_identity import (
    BEETS_CODEC_LABELS,
)
from lib.quality import AudioQualityMeasurement, QualityRankConfig
from lib.quality_evidence import (
    current_evidence_for_policy,
    propagate_candidate_evidence_to_current,
)
from tests.fakes.pipeline_db import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row

_PRESERVED_GRADE = "likely_transcode"

# Independently authored policy expectations.  This is intentionally not
# derived from the production pair table or Beets alias values.
_EXPECTED_SINGLE_FORMAT_POLICY: dict[str, tuple[str, str | None]] = {
    "mp3": ("mp3", _PRESERVED_GRADE),
    "aac": ("aac", _PRESERVED_GRADE),
    "alac": ("m4a", None),
    "flac": ("flac", None),
    "opus": ("opus", _PRESERVED_GRADE),
    "vorbis": ("ogg", _PRESERVED_GRADE),
    "wav": ("wav", None),
    "wma": ("wma", _PRESERVED_GRADE),
}

_MIXED_SAME_CONTAINER_WORLDS = (
    ("m4a", ("AAC", "ALAC"), frozenset({"aac", "alac"})),
    ("ogg", ("OGG", "FLAC"), frozenset({"vorbis", "flac"})),
    ("ogg", ("OGG", "Opus"), frozenset({"vorbis", "opus"})),
)


def assert_projected_policy_grade(
    *, actual: str | None, expected: str | None,
) -> None:
    """Assert the final policy projection, never an upstream predicate."""

    if actual != expected:
        raise AssertionError(f"projected grade was {actual!r}, expected {expected!r}")


def assert_producer_inventory_known(
    *, actual: frozenset[str], expected: frozenset[str],
) -> None:
    """Fail closed when the Beets producer emits an unowned codec label."""

    if actual != expected:
        raise AssertionError(
            f"producer outputs {sorted(actual)!r} != policy inventory "
            f"{sorted(expected)!r}"
        )


def _producer_raw_labels() -> tuple[str, ...]:
    """Derive every admitted input from the real Beets normalization surface."""

    return tuple(sorted(BEETS_CODEC_LABELS))


def _write_beets_db(
    path: Path,
    *,
    root: Path,
    container: str,
    format_labels: tuple[str, ...],
) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE albums (
            id INTEGER PRIMARY KEY,
            mb_albumid TEXT,
            discogs_albumid INTEGER
        );
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            album_id INTEGER,
            bitrate INTEGER,
            path BLOB,
            title TEXT,
            artist TEXT,
            track INTEGER,
            disc INTEGER,
            length REAL,
            format TEXT,
            samplerate INTEGER,
            bitdepth INTEGER
        );
    """)
    conn.execute("INSERT INTO albums (id, mb_albumid) VALUES (1, 'media-id')")
    for index, format_label in enumerate(format_labels, 1):
        conn.execute(
            "INSERT INTO items (id, album_id, bitrate, path, format) "
            "VALUES (?, 1, 128000, ?, ?)",
            (
                index,
                str(root / f"{index:02d}.{container}").encode(),
                format_label,
            ),
        )
    conn.commit()
    conn.close()


def _read_beets_info(
    *, container: str, format_labels: tuple[str, ...], root: Path, db_path: Path,
) -> AlbumInfo:
    _write_beets_db(
        db_path,
        root=root,
        container=container,
        format_labels=format_labels,
    )
    with BeetsDB(str(db_path), library_root=str(root.parent.parent)) as beets:
        info = beets.get_album_info("media-id", QualityRankConfig())
    assert info is not None
    return info


def _producer_output(raw_label: str) -> str:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "library" / "album"
        root.mkdir(parents=True)
        (root / "01.mp3").write_bytes(b"audio")
        return _read_beets_info(
            container="mp3",
            format_labels=(raw_label,),
            root=root,
            db_path=Path(temp) / "library.db",
        ).format.lower()


def _project_media_world(
    *, container: str, format_labels: tuple[str, ...],
) -> tuple[AlbumInfo, str | None]:
    """Drive SQLite Beets -> evidence propagation -> final policy projection."""

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "library" / "album"
        root.mkdir(parents=True)
        for index in range(1, len(format_labels) + 1):
            (root / f"{index:02d}.{container}").write_bytes(b"audio")
        info = _read_beets_info(
            container=container,
            format_labels=format_labels,
            root=root,
            db_path=Path(temp) / "library.db",
        )

        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(id=1, mb_release_id="media-id"))
        candidate = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            mb_release_id="media-id",
            measurement=AudioQualityMeasurement(
                format="FLAC",
                spectral_grade=_PRESERVED_GRADE,
                spectral_bitrate_kbps=128,
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=None,
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
        )
        result = propagate_candidate_evidence_to_current(
            pipeline,
            request_id=1,
            candidate_evidence=candidate,
            album_info=info,
        )
        assert result.evidence is not None, result.reason
        projected = current_evidence_for_policy(result.evidence)
        return info, projected.measurement.spectral_grade


class TestEvidenceMediaIdentityGenerated(unittest.TestCase):
    def test_mixed_m4a_aac_alac_fails_closed(self) -> None:
        info, grade = _project_media_world(
            container="m4a", format_labels=("AAC", "ALAC"),
        )
        self.assertEqual(info.formats_on_disk, frozenset({"aac", "alac"}))
        assert_projected_policy_grade(actual=grade, expected=None)

    @given(world=st.sampled_from(_MIXED_SAME_CONTAINER_WORLDS))
    @example(world=_MIXED_SAME_CONTAINER_WORLDS[2])
    def test_mixed_same_container_albums_fail_closed(
        self,
        world: tuple[str, tuple[str, ...], frozenset[str]],
    ) -> None:
        container, labels, expected_formats = world
        info, grade = _project_media_world(
            container=container, format_labels=labels,
        )
        self.assertEqual(info.formats_on_disk, expected_formats)
        assert_projected_policy_grade(actual=grade, expected=None)

    def test_m4a_alac_fails_closed_at_the_real_adapter(self) -> None:
        _info, grade = _project_media_world(
            container="m4a", format_labels=("ALAC",),
        )
        assert_projected_policy_grade(actual=grade, expected=None)

    def test_producer_inventory_exactly_matches_independent_policy(self) -> None:
        actual = frozenset(_producer_output(raw) for raw in _producer_raw_labels())
        assert_producer_inventory_known(
            actual=actual,
            expected=frozenset(_EXPECTED_SINGLE_FORMAT_POLICY),
        )

    @given(raw_labels=st.permutations(_producer_raw_labels()))
    @example(raw_labels=_producer_raw_labels())
    def test_every_beets_output_reaches_final_policy(
        self, raw_labels: list[str],
    ) -> None:
        for raw_label in raw_labels:
            output = _producer_output(raw_label)
            expected = _EXPECTED_SINGLE_FORMAT_POLICY.get(output)
            if expected is None:
                self.fail(f"Beets produced unknown canonical format {output!r}")
            container, expected_grade = expected
            _info, grade = _project_media_world(
                container=container,
                format_labels=(raw_label,),
            )
            assert_projected_policy_grade(actual=grade, expected=expected_grade)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_final_policy_bypass_mutant_is_rejected(self) -> None:
        with self.assertRaisesRegex(AssertionError, "expected None"):
            assert_projected_policy_grade(actual=_PRESERVED_GRADE, expected=None)

    def test_unknown_beets_alias_mutant_is_rejected(self) -> None:
        expected = frozenset(_EXPECTED_SINGLE_FORMAT_POLICY)
        with self.assertRaisesRegex(AssertionError, "future-codec"):
            assert_producer_inventory_known(
                actual=expected | {"future-codec"},
                expected=expected,
            )
