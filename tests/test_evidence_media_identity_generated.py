"""Generated outer-adapter contract for #1018 media identity."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

from lib.beets_db import BeetsDB
from lib.evidence_media_identity import (
    authoritative_lossy_media_pair,
    canonical_beets_format,
)
from lib.quality import AudioQualityMeasurement, QualityRankConfig
from lib.quality_evidence import (
    current_evidence_for_policy,
    current_evidence_preserves_source_spectral,
    propagate_candidate_evidence_to_current,
)
from tests.fakes.pipeline_db import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


def assert_media_policy_projection(*, actual: bool, expected: bool) -> None:
    """Fail closed when the final evidence-policy projection drifts."""

    if actual != expected:
        raise AssertionError(
            f"preserved-source projection was {actual}, expected {expected}"
        )


_BEETS_MEDIA_WORLDS = (
    ("ogg", "OGG", True),
    ("ogg", "Opus", True),
    ("m4a", "AAC", True),
    ("m4a", "ALAC", False),
    ("flac", "FLAC", False),
    ("wma", "Windows Media", True),
)


def _write_beets_db(path: Path, *, root: Path, format_label: str) -> None:
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
    for index in (1, 2):
        conn.execute(
            "INSERT INTO items (id, album_id, bitrate, path, format) "
            "VALUES (?, 1, 128000, ?, ?)",
            (index, str(root / f"{index:02d}").encode(), format_label),
        )
    conn.commit()
    conn.close()


def _project_media_world(
    *, container: str, beets_label: str,
) -> bool:
    """Drive SQLite Beets -> evidence propagation -> policy projection."""

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "library" / "album"
        root.mkdir(parents=True)
        for index in (1, 2):
            (root / f"{index:02d}.{container}").write_bytes(b"audio")
        db_path = Path(temp) / "library.db"
        _write_beets_db(db_path, root=root, format_label=beets_label)
        with BeetsDB(str(db_path), library_root=str(root.parent.parent)) as beets:
            info = beets.get_album_info("media-id", QualityRankConfig())
        assert info is not None
        assert info.format == canonical_beets_format(beets_label)

        pipeline = FakePipelineDB()
        pipeline.seed_request(make_request_row(id=1, mb_release_id="media-id"))
        candidate = make_album_quality_evidence(
            mb_release_id="media-id",
            measurement=AudioQualityMeasurement(
                format="FLAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="source",
                spectral_provenance="measured",
            ),
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
        return (
            current_evidence_preserves_source_spectral(result.evidence)
            and projected.measurement.spectral_grade is not None
        )


class TestEvidenceMediaIdentityGenerated(unittest.TestCase):
    def test_media_pairs_keep_ambiguous_containers_codec_authoritative(self) -> None:
        self.assertTrue(authoritative_lossy_media_pair("ogg", "vorbis"))
        self.assertTrue(authoritative_lossy_media_pair("ogg", "opus"))
        self.assertTrue(authoritative_lossy_media_pair("m4a", "aac"))
        self.assertFalse(authoritative_lossy_media_pair("m4a", "alac"))
        self.assertFalse(authoritative_lossy_media_pair("flac", "flac"))

    def test_m4a_alac_fails_closed_at_the_real_adapter(self) -> None:
        assert_media_policy_projection(
            actual=_project_media_world(container="m4a", beets_label="ALAC"),
            expected=False,
        )

    @given(world=st.sampled_from(_BEETS_MEDIA_WORLDS))
    @example(world=("ogg", "OGG", True))
    @example(world=("m4a", "ALAC", False))
    def test_every_beets_media_output_has_one_policy_meaning(
        self, world: tuple[str, str, bool],
    ) -> None:
        container, beets_label, expected = world
        assert_media_policy_projection(
            actual=_project_media_world(
                container=container, beets_label=beets_label,
            ),
            expected=expected,
        )


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    def test_media_policy_projection_checker_rejects_a_known_bad_alac_result(self) -> None:
        with self.assertRaisesRegex(AssertionError, "expected False"):
            assert_media_policy_projection(actual=True, expected=False)
