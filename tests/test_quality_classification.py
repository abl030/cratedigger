#!/usr/bin/env python3
"""Quality classification tests — synthetic-input regression suite.

Each ``TestLiveBugReproductions`` test reproduces a real production bug
through ``full_pipeline_decision`` (the flat-kwargs simulator) and asserts
the same outcome through ``full_pipeline_decision_from_evidence`` (the
evidence-pipeline twin). Inputs are constructed in-test — no audio
fixtures, no external binaries.
"""

import os
import sys
import unittest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import (full_pipeline_decision, AlbumQualityV0Metric)

class TestLiveBugReproductions(unittest.TestCase):
    """Reproduce bugs found in live pipeline runs.

    These test the full_pipeline_decision() against exact conditions
    observed in production. Each test documents a real incident.
    """

    def test_tyler_lamberts_grave_cbr320_transcode_accepted(self):
        """BUG: CBR 320 transcode from 160k source was accepted.

        Request 249, 2026-03-28. dangshnizzle uploaded CBR 320 that was
        a transcode from ~160kbps source. Spectral detected likely_transcode
        but the reject gate in process_completed_album only checked for
        grade=="suspect", missing "likely_transcode". Also, spectral said
        new=160 <= existing=160, so it should have been rejected.

        Root cause: cratedigger.py line 1426 checked `== "suspect"` not
        `in ("suspect", "likely_transcode")`.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=320,
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate=160,
            existing_min_bitrate=320,
            existing_spectral_bitrate=160,
        )
        # Should reject — spectral says transcode and not better than existing
        self.assertEqual(r["stage1_spectral"], "reject",
                         f"Should reject: new spectral 160 <= existing 160")
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_tyler_lamberts_grave_no_spectral_bitrate(self):
        """Same bug but when spectral_bitrate is None (HF deficit only, no cliff).

        When cliff detection doesn't fire, spectral_bitrate=None.
        The quality gate has nothing to override with, so CBR 320
        passes through as "requeue_lossless" at best.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=320,
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate=None,  # no cliff detected
            existing_min_bitrate=320,
            existing_spectral_bitrate=160,
        )
        # Without spectral_bitrate, stage1 can't compare numerically.
        # But grade is likely_transcode — should still reject or at minimum
        # not mark as final "imported".
        self.assertTrue(r["keep_searching"],
                        "likely_transcode should trigger keep_searching")

    def test_taboo_vi_fake_flac_192_accepted(self):
        """Fake FLAC (192k source) converted to V0 at 224kbps is provisional.

        Request 257, 2026-03-28. amyslskduser uploaded FLAC that was actually
        a 192k transcode. Spectral said likely_transcode but estimated_bitrate
        was None (HF deficit, not cliff). V0 conversion produced 224kbps which
        is above the 210 threshold, so import_one.py didn't flag as transcode.
        The provisional lossless-source lane now records the V0 source probe,
        imports it as unverified evidence, and keeps searching.

        Original root causes:
        1. import_one.py transcode threshold (210) too low for 192k fakes
        2. spectral_bitrate=None when cliff not detected → no quality gate override
        3. verified_lossless correctly NOT set (spectral=likely_transcode)
           but quality gate still accepts VBR above 210 without verification
        """
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=None,  # no cliff detected
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            post_conversion_min_bitrate=224,
            converted_count=10,
        )
        self.assertFalse(r["verified_lossless"],
                         "Fake FLAC should never get verified_lossless")
        self.assertEqual(r["stage2_import"], "provisional_lossless_upgrade")
        self.assertTrue(r["keep_searching"])
        self.assertTrue(r["denylisted"])

    def test_taboo_vi_with_spectral_bitrate(self):
        """Same scenario but if spectral_bitrate had been captured."""
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=192,
            existing_min_bitrate=128,
            existing_spectral_bitrate=96,
            post_conversion_min_bitrate=224,
            converted_count=10,
        )
        # The provisional source-probe decision runs before the generic quality
        # gate, even when spectral_bitrate is available.
        self.assertEqual(r["stage2_import"], "provisional_lossless_upgrade")
        self.assertIsNone(r["stage3_quality_gate"])
        self.assertTrue(r["keep_searching"])
        self.assertTrue(r["denylisted"])

    def test_live_mountain_goats_flux_flac_source_vs_lossy_no_spectral(self):
        """Mountain Goats - The Life of the World in Flux / AnderMachines.

        Request 4514, 2026-05-16 14:47 AWST. FLAC source with a suspect
        spectral cliff at 160 kbps (one-track cliff, 69% suspect grade).
        Lossless-source V0 probe: avg=211, min=198, median=214 — well
        above the V0 floor, so a strong provisional-lossless signal.

        Existing in beets: MP3 320 CBR, no spectral measurement.

        Pre-fix bug: the importer's ``preimport_decide`` ran a parallel
        spectral comparison that fell back to the existing container
        bitrate (320 kbps) when no existing spectral was measured. The
        candidate's 160 kbps cliff was compared against 320 kbps and
        rejected as ``spectral_reject`` — bypassing the full pipeline's
        FLAC provisional-lossless pathway entirely.

        Correct behavior: provisional_lossless_upgrade. The full pipeline
        owns spectral, codec rank, and the provisional lossless path —
        ``preimport_decide`` only owns folder/audio-integrity facts.
        """
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="suspect",
            spectral_bitrate=160,
            converted_count=13,
            post_conversion_min_bitrate=198,
            candidate_v0_probe_avg=211,
            candidate_v0_probe_min=198,
            existing_min_bitrate=320,
            existing_avg_bitrate=320,
            existing_format="MP3",
            existing_is_cbr=True,
        )

        self.assertEqual(r["stage0_spectral_gate"], "skipped_flac",
                         "FLAC skips the preimport spectral gate")
        # Stage 1 is informational; existing has no spectral → import_no_exist
        # (NOT 'reject' — spectral compares to spectral, not container).
        self.assertEqual(r["stage1_spectral"], "import_no_exist")
        # Stage 2 owns the FLAC provisional-lossless pathway. The V0 probe
        # (lossless-source min=198) outranks the suspect spectral cliff.
        self.assertEqual(r["stage2_import"], "provisional_lossless_upgrade")
        self.assertTrue(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])
        self.assertEqual(r["final_status"], "wanted")

    def test_lil_wayne_da_drought_3_transcoded_flac_rejects_duplicate_via_simulator(self):
        """Lil Wayne - Da Drought 3 / mymedia.

        Request 3779, MBID ``244322cc-51ba-4f35-b072-f7c5888fb5ce``, 2026-05-17.
        Live download_log rows: 16564 (force-imported predecessor at 08:06 UTC,
        transcoded FLAC → Opus V2) and 16682 (rejected duplicate at 18:32 UTC).

        Live bug: wrong-match cleanup triage classified the second candidate
        as ``kept_would_import`` because the on-disk library evidence row had
        NULL spectral / V0 fields. The library row exists (the first import
        succeeded and produced an Opus copy), but ``propagate_candidate_evidence_to_current``
        used to strip source-side evidence on transcoded imports — so triage
        had comparable evidence on the candidate side and nothing on the
        library side, and fell through to ``provisional_lossless_upgrade``.

        Correct behaviour (post-U5 propagation policy): triage sees that the
        library row was produced from a comparable lossless source (likely_transcode
        FLAC, spectral=128, V0 probe avg=215 min=184) and rejects the new
        candidate as a same-source duplicate via the provisional-lossless
        gate (``lossless_source_not_better``). ``import_mode="force"``
        mirrors what ``cleanup_wrong_match`` actually calls
        (lib/wrong_match_cleanup_service.py:330-343).

        This is the simulator side of the parity contract — the sibling
        ``test_lil_wayne_da_drought_3_transcoded_flac_rejects_duplicate_via_evidence``
        in ``TestLiveBugReproductionsThroughEvidencePipeline`` must reach
        the same outcome through the evidence pipeline.
        """
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            converted_count=13,
            post_conversion_min_bitrate=184,
            candidate_v0_probe_avg=215,
            candidate_v0_probe_min=184,
            # Existing-side facts mirror what the library row will look like
            # post-U5: the previous transcoded FLAC → Opus import propagated
            # source spectral + V0 onto the library evidence row, so triage
            # now sees comparable evidence on both sides.
            existing_min_bitrate=100,
            existing_avg_bitrate=119,
            existing_format="Opus",
            existing_is_cbr=False,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=128,
            existing_v0_probe_avg=215,
            import_mode="force",
        )

        # Provisional-lossless gate: same-source comparable evidence on both
        # sides — the candidate's likely_transcode spectral grade + lossless-
        # source V0 probe matches the library row's propagated provenance,
        # so the gate rejects the duplicate as ``suspect_lossless_downgrade``
        # rather than upgrading.
        self.assertEqual(r["stage2_import"], "suspect_lossless_downgrade")
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_heretic_pride_one_bad_track_infinite_requeue(self):
        """BUG: 13/14 tracks at 320kbps + 1 track at 192kbps → infinite requeue.

        Request 226, 2026-03-28. wallywubox. Album is CBR 320 except for
        one track at 192kbps. min_bitrate=192 < 210 → requeue_upgrade.
        But every source on Soulseek has the same bad track, so it keeps
        re-downloading the same thing. Downloaded 5 times.

        Root cause: quality gate uses MIN(bitrate) across all tracks.
        One outlier track drags the whole album below threshold.

        Possible fixes:
        - Use percentile instead of MIN (ignore bottom N%)
        - Accept when only 1 track is below and rest are well above
        - Track per-download bitrate comparison to detect "same source, same quality"
        """
        # First import: no existing, 192 < 210 → requeue
        r1 = full_pipeline_decision(
            is_flac=False,
            min_bitrate=192,
            is_cbr=False,
            spectral_grade="genuine",
            spectral_bitrate=None,
            existing_min_bitrate=None,  # first import
        )
        self.assertTrue(r1["imported"])
        # Quality gate: 192 < 210 → requeue_upgrade
        self.assertEqual(r1["stage3_quality_gate"], "requeue_upgrade")
        self.assertEqual(r1["final_status"], "wanted")

        # Second import attempt: same source, same quality
        r2 = full_pipeline_decision(
            is_flac=False,
            min_bitrate=192,
            is_cbr=False,
            spectral_grade="genuine",
            spectral_bitrate=None,
            existing_min_bitrate=192,  # same as what's on disk
        )
        # Stage2 rejects as downgrade (192 <= 192), but album stays wanted
        self.assertEqual(r2["stage2_import"], "downgrade")
        self.assertFalse(r2["imported"])
        # BUG: keep_searching=True means it will try AGAIN → infinite loop
        # When fixed, system should detect same-quality loop and accept
        self.assertTrue(r2["keep_searching"])

    def test_darcie_haven_native_opus_beats_mp3_transcode(self):
        """Darcie Haven - Angel of the Apocalypse / request 4679, 2026-05-31.

        A genuine native Opus ~124 kbps download (min 124, avg 129) was
        rejected as a downgrade against an existing MP3 CBR 128
        (likely_transcode). Root cause: the harness stamped EVERY native
        lossy download's measurement format as a hardcoded "MP3", so the
        Opus was scored on the MP3-VBR band table (acceptable floor 130) and
        129 landed POOR, losing to MP3-CBR-128 (ACCEPTABLE). With the real
        "opus" label it classifies TRANSPARENT (opus transparent threshold
        112) and wins. See the codec-label fix in tests/test_native_codec_label.py.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=124,
            avg_bitrate=129,
            is_cbr=False,
            new_format="opus",
            spectral_grade="genuine",
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
            existing_format="MP3",
            existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=128,
        )
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])

    def test_darcie_haven_opus_mislabelled_mp3_loses(self):
        """The bug itself: the SAME audio mislabelled "MP3" is (correctly,
        given that wrong label) a downgrade. This pins that the codec LABEL
        is the pivot — guards against a future regression that re-hardcodes
        the native format to MP3."""
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=124,
            avg_bitrate=129,
            is_cbr=False,
            new_format="MP3",
            spectral_grade="genuine",
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
            existing_format="MP3",
            existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=128,
        )
        self.assertNotEqual(r["stage2_import"], "import")
        self.assertFalse(r["imported"])


class TestLiveBugReproductionsThroughEvidencePipeline(unittest.TestCase):
    """Every TestLiveBugReproductions scenario must produce the same outcome
    when run through ``full_pipeline_decision_from_evidence`` — the function
    the importer actually calls in production.

    The simulator (``full_pipeline_decision``) and the evidence pipeline
    (``full_pipeline_decision_from_evidence``) are two entry points into
    the SAME decision logic. Quality decisions live in exactly one place;
    the simulator is a thin flat-kwargs adapter. This class proves the
    parity contract — if you can describe an album scenario with the
    simulator, you can describe it as evidence rows, and the outcome
    matches.

    See CLAUDE.md § "Quality decisions live in one place" for the rule.
    """

    def _build_candidate(
        self,
        *,
        is_flac: bool,
        min_bitrate: int,
        is_cbr: bool,
        avg_bitrate: int | None = None,
        spectral_grade: str | None = None,
        spectral_bitrate: int | None = None,
        post_conversion_min_bitrate: int | None = None,
        candidate_v0_probe_avg: int | None = None,
        candidate_v0_probe_min: int | None = None,
        native_codec: str = "mp3",
        native_format: str = "MP3",
        mb_release_id: str = "mbid-parity-candidate",
        audio_corrupt: bool = False,
        folder_layout: str = "flat",
        audio_file_count: int | None = None,
        matched_bad_audio_hash_id: int | None = None,
        matched_bad_audio_hash_path: str | None = None,
        snapshot_fingerprint: str = "sha256:candidate-fingerprint",
    ):
        """Build an ``AlbumQualityEvidence`` candidate row matching the
        simulator's flat-kwargs shape (post-U2/U3 schema)."""
        from datetime import datetime, timezone
        from lib.quality import (
            AlbumQualityEvidence,
            AlbumQualityEvidenceFile,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
        )

        # For a FLAC source post-conversion, the candidate measurement
        # reflects the V0 output the importer compares against.
        if is_flac and post_conversion_min_bitrate is not None:
            fmt = "MP3"
            container = "flac"
            codec = "flac"
            storage_format = "flac"
            measurement = AudioQualityMeasurement(
                min_bitrate_kbps=post_conversion_min_bitrate,
                avg_bitrate_kbps=candidate_v0_probe_avg or post_conversion_min_bitrate,
                median_bitrate_kbps=candidate_v0_probe_avg or post_conversion_min_bitrate,
                format=fmt,
                is_cbr=False,
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate,
                was_converted_from="flac",
            )
        elif is_flac:
            container = codec = "flac"
            storage_format = "flac"
            measurement = AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate or 900,
                avg_bitrate_kbps=min_bitrate or 900,
                median_bitrate_kbps=min_bitrate or 900,
                format="FLAC",
                is_cbr=False,
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate,
            )
        else:
            container = codec = native_codec
            if native_codec == "mp3":
                storage_format = "mp3 v0" if not is_cbr else "mp3 320"
            else:
                storage_format = native_format.lower()
            _avg = avg_bitrate if avg_bitrate is not None else min_bitrate
            measurement = AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate,
                avg_bitrate_kbps=_avg,
                median_bitrate_kbps=_avg,
                format=native_format,
                is_cbr=is_cbr,
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate,
            )

        v0_metric = None
        if candidate_v0_probe_avg is not None or candidate_v0_probe_min is not None:
            v0_metric = AlbumQualityV0Metric(
                min_bitrate_kbps=candidate_v0_probe_min,
                avg_bitrate_kbps=candidate_v0_probe_avg,
                median_bitrate_kbps=candidate_v0_probe_avg,
                source_lineage=V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
                source_provenance="neutral_album_quality_evidence",
            )

        files = [AlbumQualityEvidenceFile(
            relative_path=f"01.{container}",
            size_bytes=1, mtime_ns=1,
            extension=container, container=container, codec=codec,
        )]
        # ``audio_file_count`` defaults to len(files) for the standard
        # parity scenarios. Tests covering empty_fileset explicitly pass
        # ``audio_file_count=0`` and override ``files`` separately.
        return AlbumQualityEvidence(
            mb_release_id=mb_release_id,
            snapshot_fingerprint=snapshot_fingerprint,
            source_path="/Incoming/auto-import/candidate",
            measurement=measurement,
            measured_at=datetime(2026, 5, 16, tzinfo=timezone.utc),
            files=files,
            codec=codec,
            container=container,
            storage_format=storage_format,
            v0_metric=v0_metric,
            audio_corrupt=audio_corrupt,
            folder_layout=folder_layout,
            audio_file_count=(
                audio_file_count if audio_file_count is not None else len(files)
            ),
            filetype_band=storage_format,
            matched_bad_audio_hash_id=matched_bad_audio_hash_id,
            matched_bad_audio_hash_path=matched_bad_audio_hash_path,
        )

    def _build_current(
        self,
        *,
        min_bitrate: int | None,
        avg_bitrate: int | None = None,
        format: str = "MP3",
        is_cbr: bool = False,
        spectral_grade: str | None = None,
        spectral_bitrate: int | None = None,
        mb_release_id: str = "mbid-parity-candidate",
        v0_metric: AlbumQualityV0Metric | None = None,
        matched_bad_audio_hash_id: int | None = None,
        matched_bad_audio_hash_path: str | None = None,
    ):
        if min_bitrate is None:
            return None
        from datetime import datetime, timezone
        from lib.quality import (
            AlbumQualityEvidence,
            AlbumQualityEvidenceFile,
            AudioQualityMeasurement,
        )

        container = format.lower().split()[0]
        files = [AlbumQualityEvidenceFile(
            relative_path=f"01.{container}",
            size_bytes=1, mtime_ns=1,
            extension=container, container=container, codec=container,
        )]
        return AlbumQualityEvidence(
            mb_release_id=mb_release_id,
            snapshot_fingerprint="sha256:current-fingerprint",
            source_path="/Beets/current",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=min_bitrate,
                avg_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
                median_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
                format=format,
                is_cbr=is_cbr,
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate,
            ),
            measured_at=datetime(2026, 5, 16, tzinfo=timezone.utc),
            files=files,
            codec=container,
            container=container,
            storage_format=format.lower(),
            audio_file_count=len(files),
            filetype_band=format.lower(),
            v0_metric=v0_metric,
            matched_bad_audio_hash_id=matched_bad_audio_hash_id,
            matched_bad_audio_hash_path=matched_bad_audio_hash_path,
        )

    def test_mountain_goats_flux_provisional_lossless_via_evidence(self):
        """Request 4514 shape, but routed through the production decider."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="suspect",
            spectral_bitrate=160,
            post_conversion_min_bitrate=198,
            candidate_v0_probe_avg=211,
            candidate_v0_probe_min=198,
        )
        current = self._build_current(
            min_bitrate=320, avg_bitrate=320,
            format="MP3", is_cbr=True,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["stage2_import"], "provisional_lossless_upgrade",
                         "evidence pipeline must reach the same decision as "
                         "the simulator — FLAC source + V0 probe + existing "
                         "lossy = provisional_lossless_upgrade")
        self.assertTrue(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_mountain_goats_bride_provisional_via_evidence(self):
        """test_live_mountain_goats_bride_first_provisional_source_import
        — same scenario through the evidence pipeline."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="likely_transcode",
            post_conversion_min_bitrate=214,
            candidate_v0_probe_avg=214,
        )
        current = self._build_current(
            min_bitrate=320, avg_bitrate=320,
            format="MP3", is_cbr=True,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                import_mode="auto",
                verified_lossless_target="opus 128",
            ),
        )

        self.assertEqual(r["stage2_import"], "provisional_lossless_upgrade")
        self.assertTrue(r["imported"])

    def test_heretic_pride_downgrade_via_evidence(self):
        """test_heretic_pride second-pass downgrade case via the evidence
        pipeline — MP3 192 vs existing MP3 192."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=192, is_cbr=False,
            spectral_grade="genuine",
        )
        current = self._build_current(
            min_bitrate=192, avg_bitrate=192,
            format="MP3", is_cbr=False,
            spectral_grade="genuine",
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])

    def test_lil_wayne_da_drought_3_transcoded_flac_rejects_duplicate_via_evidence(self):
        """Parity sibling of
        ``TestLiveBugReproductions.test_lil_wayne_da_drought_3_transcoded_flac_rejects_duplicate_via_simulator``.

        Request 3779, MBID ``244322cc-51ba-4f35-b072-f7c5888fb5ce``, 2026-05-17.
        Encodes the post-U5 expectation: the library evidence row for the
        previously-transcoded FLAC → Opus import carries the propagated
        source-side spectral + V0 evidence, so triage rejects the second
        identical-source candidate as a same-source duplicate.

        Parity contract: the simulator and the evidence pipeline must
        reach the same ``stage2_import`` decision on the same album, and
        ``classify_full_pipeline_decision`` must mark the outcome
        ``confident_reject`` with ``cleanup_eligible=True`` so the
        wrong-match folder becomes eligible for cleanup.

        Today (pre-U5) the library row has NULL spectral / V0 because
        ``propagate_candidate_evidence_to_current`` strips source-side
        evidence on transcoded imports. The current evidence row is being
        synthesized here as the post-U5 shape, so this test will fail
        RED until U5 makes the production path produce that state.
        """
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityV0Metric,
            V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
            classify_full_pipeline_decision,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            post_conversion_min_bitrate=184,
            candidate_v0_probe_avg=215,
            candidate_v0_probe_min=184,
        )
        current = self._build_current(
            min_bitrate=100,
            avg_bitrate=119,
            format="Opus",
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=184,
                avg_bitrate_kbps=215,
                median_bitrate_kbps=215,
                source_lineage=V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
                source_provenance="neutral_album_quality_evidence",
            ),
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="force"),
        )

        # --- Parity contract -------------------------------------------------
        # Run the simulator with the same album facts and assert it reaches
        # the same outcome through the flat-kwargs decider. This is the
        # load-bearing parity assertion — it fails if the two entry points
        # ever diverge on this album, regardless of what the literal decision
        # name happens to be. The hardcoded check below pins the current
        # value (suspect_lossless_downgrade); the parity check guards
        # against future drift between the simulator and evidence pipeline.
        sim = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            converted_count=13,
            post_conversion_min_bitrate=184,
            candidate_v0_probe_avg=215,
            candidate_v0_probe_min=184,
            existing_min_bitrate=100,
            existing_avg_bitrate=119,
            existing_format="Opus",
            existing_is_cbr=False,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=128,
            existing_v0_probe_avg=215,
            import_mode="force",
        )
        self.assertEqual(
            r["stage2_import"], sim["stage2_import"],
            "Parity contract violated: simulator and evidence pipeline "
            "reached different stage2_import decisions on the same album "
            f"(simulator={sim['stage2_import']!r}, "
            f"evidence={r['stage2_import']!r})",
        )
        self.assertEqual(
            r["imported"], sim["imported"],
            "Parity contract violated: imported flag differs",
        )
        self.assertEqual(
            r["denylisted"], sim["denylisted"],
            "Parity contract violated: denylisted flag differs",
        )
        self.assertEqual(
            r["keep_searching"], sim["keep_searching"],
            "Parity contract violated: keep_searching flag differs",
        )

        # Literal value pin (sibling of the simulator test's hardcoded
        # assertion). Both deciders currently land on suspect_lossless_downgrade
        # for this album; if either side moves to a different reject branch,
        # update both tests together.
        self.assertEqual(r["stage2_import"], "suspect_lossless_downgrade")

        verdict, cleanup_eligible, _reason = classify_full_pipeline_decision(r)
        self.assertEqual(verdict, "confident_reject")
        self.assertTrue(cleanup_eligible)

    def test_darcie_haven_native_opus_beats_mp3_via_evidence(self):
        """Request 4679 shape through the production decider: a native Opus
        124/129 (genuine) candidate must beat an existing MP3 CBR 128
        (likely_transcode). Parity twin of
        test_darcie_haven_native_opus_beats_mp3_transcode."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False,
            min_bitrate=124,
            avg_bitrate=129,
            is_cbr=False,
            spectral_grade="genuine",
            native_codec="opus",
            native_format="opus",
        )
        current = self._build_current(
            min_bitrate=128, avg_bitrate=128,
            format="MP3", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=128,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["stage2_import"], "import",
                         "evidence pipeline must reach the same decision as "
                         "the simulator — native Opus TRANSPARENT beats MP3 "
                         "CBR 128 ACCEPTABLE")
        self.assertTrue(r["imported"])

    def test_darcie_haven_opus_mislabelled_mp3_loses_via_evidence(self):
        """Parity twin of test_darcie_haven_opus_mislabelled_mp3_loses: the
        SAME audio carried through the production decider with the buggy "MP3"
        label is (correctly, given that wrong label) a downgrade. Pins that the
        codec LABEL on the candidate measurement is the pivot at the evidence
        boundary too — a regression that re-hardcodes the native format to MP3
        in _new_format_hint_from_evidence would flip this back to a wrong
        rejection and be caught here."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False,
            min_bitrate=124,
            avg_bitrate=129,
            is_cbr=False,
            spectral_grade="genuine",
            native_codec="mp3",
            native_format="MP3",
        )
        current = self._build_current(
            min_bitrate=128, avg_bitrate=128,
            format="MP3", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=128,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertNotEqual(r["stage2_import"], "import")
        self.assertFalse(r["imported"])


class TestPreimportFactRejects(unittest.TestCase):
    """U11+: folder/audio-integrity facts that fire as early-exit rejects at
    the top of ``full_pipeline_decision_from_evidence`` before any quality
    stage runs. Each test covers one fact: asserts the decision dict carries
    the right ``preimport_*`` key AND that ``evidence_decision_name`` maps
    it to the expected decision string.

    Facts (in reject-priority order):
      * ``audio_corrupt``  → ``preimport_audio='reject_corrupt'``,
        ``evidence_decision_name='audio_corrupt'``,
        ``classify_full_pipeline_decision`` → confident_reject
      * ``bad_audio_hash`` → ``preimport_bad_hash='reject_bad_hash'``,
        ``evidence_decision_name='bad_audio_hash'``
      * ``nested_layout`` → ``preimport_nested='reject_nested'``,
        ``evidence_decision_name='nested_layout'``
      * ``empty_fileset`` → ``preimport_empty_fileset='reject_empty'``,
        ``evidence_decision_name='empty_fileset'``
      * ``mixed_source`` (lossless+lossy in one folder) →
        ``preimport_mixed_source='reject_mixed_source'``,
        ``evidence_decision_name='mixed_source'``. Lives here so a partial
        FLAC+MP3 source never stamps the parent album as verified-lossless
        — Cratedigger stays release-based, not song-based. See the Fast
        Times at Barrington High reproduction (request 4445, evidence 5888).
    """

    # Reuse the parity helpers so the new tests share the exact shape used
    # by the rest of TestLiveBugReproductionsThroughEvidencePipeline.
    _build_candidate = (
        TestLiveBugReproductionsThroughEvidencePipeline._build_candidate
    )
    _build_current = (
        TestLiveBugReproductionsThroughEvidencePipeline._build_current
    )

    def test_audio_corrupt_routes_through_full_pipeline(self):
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            classify_full_pipeline_decision,
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=245, is_cbr=False,
            audio_corrupt=True,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertFalse(r["imported"])
        # Audio-integrity rejects denylist the peer (source-quality problem).
        self.assertTrue(r["denylisted"])
        self.assertEqual(r["final_status"], "wanted")
        self.assertEqual(evidence_decision_name(r), "audio_corrupt")
        verdict, cleanup_eligible, reason = classify_full_pipeline_decision(r)
        self.assertEqual(verdict, "confident_reject")
        self.assertTrue(cleanup_eligible)
        self.assertEqual(reason, "audio_corrupt")

    def test_bad_audio_hash_routes_through_full_pipeline(self):
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            classify_full_pipeline_decision,
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=245, is_cbr=False,
            matched_bad_audio_hash_id=42,
            matched_bad_audio_hash_path="01 - track.mp3",
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["preimport_bad_hash"], "reject_bad_hash")
        # Curated bad-hash hit is a source-quality problem — denylist on auto.
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertEqual(r["final_status"], "wanted")
        self.assertEqual(evidence_decision_name(r), "bad_audio_hash")
        verdict, cleanup_eligible, reason = classify_full_pipeline_decision(r)
        self.assertEqual(verdict, "confident_reject")
        self.assertTrue(cleanup_eligible)
        self.assertEqual(reason, "bad_audio_hash")

    def test_nested_layout_routes_through_full_pipeline(self):
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            classify_full_pipeline_decision,
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=245, is_cbr=False,
            folder_layout="nested",
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["preimport_nested"], "reject_nested")
        self.assertFalse(r["imported"])
        # nested_layout is a folder-shape problem — peer is not at fault.
        self.assertFalse(r["denylisted"])
        # Auto path still self-heals (final_status='wanted', keep_searching).
        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["keep_searching"])
        self.assertEqual(evidence_decision_name(r), "nested_layout")
        verdict, cleanup_eligible, reason = classify_full_pipeline_decision(r)
        self.assertEqual(verdict, "confident_reject")
        self.assertTrue(cleanup_eligible)
        self.assertEqual(reason, "nested_layout")

    def test_empty_fileset_routes_through_full_pipeline(self):
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            classify_full_pipeline_decision,
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )

        # audio_file_count=0 AND no snapshot files — the explicit empty
        # signal (cannot collide with legacy SQL-default rows).
        candidate = self._build_candidate(
            is_flac=False, min_bitrate=245, is_cbr=False,
            audio_file_count=0,
        )
        # Override files to empty (the helper defaults to one snapshot file).
        from msgspec import structs
        candidate = structs.replace(candidate, files=[], audio_file_count=0)

        r = full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["preimport_empty_fileset"], "reject_empty")
        self.assertFalse(r["imported"])
        # Empty fileset is a folder-shape problem — peer not at fault.
        self.assertFalse(r["denylisted"])
        self.assertEqual(evidence_decision_name(r), "empty_fileset")
        verdict, cleanup_eligible, reason = classify_full_pipeline_decision(r)
        self.assertEqual(verdict, "confident_reject")
        self.assertTrue(cleanup_eligible)
        self.assertEqual(reason, "empty_fileset")

    def test_mixed_source_routes_through_full_pipeline(self):
        """Fast Times at Barrington High reproduction (request 4445).

        Source folder had 15 .flac + 2 .mp3 (bonus tracks). Previously
        ``determine_verified_lossless(converted_count=15, is_transcode=False)``
        returned True with no knowledge that 2 untouched lossy files would
        be copied into the library, producing a ``verified_lossless=true``
        stamp on a ``mixed_lossy`` album that then poisoned the wrong-match
        cleanup short-circuit (parent_album_verified_lossless → auto-delete
        future fully-FLAC candidates against the same MBID).

        The fix: detect lossless+lossy containers in the candidate snapshot
        files and reject before any conversion or import runs. Self-heals
        back to ``wanted`` like the other preimport-fact rejects.
        """
        from lib.quality import (
            AlbumQualityEvidence,
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityEvidenceFile,
            AudioQualityMeasurement,
            classify_full_pipeline_decision,
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )
        from datetime import datetime, timezone

        # 15 FLAC + 2 MP3, mirroring the live download 17772 source folder.
        files = [
            AlbumQualityEvidenceFile(
                relative_path=f"{i:02d}.flac",
                size_bytes=1, mtime_ns=1,
                extension="flac", container="flac", codec="flac",
            )
            for i in range(1, 16)
        ] + [
            AlbumQualityEvidenceFile(
                relative_path="16.mp3",
                size_bytes=1, mtime_ns=1,
                extension="mp3", container="mp3", codec="mp3",
            ),
            AlbumQualityEvidenceFile(
                relative_path="17.mp3",
                size_bytes=1, mtime_ns=1,
                extension="mp3", container="mp3", codec="mp3",
            ),
        ]
        candidate = AlbumQualityEvidence(
            mb_release_id="mbid-fast-times",
            snapshot_fingerprint="sha256:fast-times-mixed",
            source_path="/Incoming/auto-import/candidate",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=900,
                format="FLAC",
                is_cbr=False,
                spectral_grade="genuine",
            ),
            measured_at=datetime(2026, 5, 21, tzinfo=timezone.utc),
            files=files,
            codec="flac",
            container="flac",
            storage_format="flac",
            audio_file_count=len(files),
            # The slskd-string filetype_band that produced the live row 5887.
            filetype_band="flac, mp3",
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["preimport_mixed_source"], "reject_mixed_source")
        self.assertFalse(r["imported"])
        # Mixed source is a peer-quality problem (peer chose to bundle lossy
        # bonus tracks). Denylist them so the same person serving the same
        # mixed bag doesn't burn another cycle.
        self.assertTrue(r["denylisted"])
        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["keep_searching"])
        self.assertFalse(r["verified_lossless"])
        self.assertEqual(evidence_decision_name(r), "mixed_source")
        verdict, cleanup_eligible, reason = classify_full_pipeline_decision(r)
        self.assertEqual(verdict, "confident_reject")
        self.assertTrue(cleanup_eligible)
        self.assertEqual(reason, "mixed_source")

    def test_mixed_source_all_lossless_multi_codec_does_not_trip(self):
        """FLAC + WAV in the same folder is all-lossless — must NOT trip
        the mixed_source reject. The check is specifically "lossless +
        lossy in the same folder", not "multiple containers"."""
        from lib.quality import (
            AlbumQualityEvidence,
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityEvidenceFile,
            AudioQualityMeasurement,
            full_pipeline_decision_from_evidence,
        )
        from datetime import datetime, timezone

        files = [
            AlbumQualityEvidenceFile(
                relative_path="01.flac",
                size_bytes=1, mtime_ns=1,
                extension="flac", container="flac", codec="flac",
            ),
            AlbumQualityEvidenceFile(
                relative_path="02.wav",
                size_bytes=1, mtime_ns=1,
                extension="wav", container="wav", codec="wav",
            ),
        ]
        candidate = AlbumQualityEvidence(
            mb_release_id="mbid-multi-lossless",
            snapshot_fingerprint="sha256:multi-lossless",
            source_path="/Incoming/auto-import/candidate",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=900,
                format="FLAC",
                is_cbr=False,
            ),
            measured_at=datetime(2026, 5, 21, tzinfo=timezone.utc),
            files=files,
            codec="flac",
            container="flac",
            storage_format="flac",
            audio_file_count=len(files),
            filetype_band="flac, wav",
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertIsNone(r["preimport_mixed_source"])

    def test_decision_order_corrupt_takes_priority_over_other_facts(self):
        """When multiple facts are present, ``audio_corrupt`` wins
        (matches the deleted ``preimport_decide`` evaluation order)."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=245, is_cbr=False,
            audio_corrupt=True,
            matched_bad_audio_hash_id=99,
            matched_bad_audio_hash_path="01.mp3",
            folder_layout="nested",
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="auto"),
        )

        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertIsNone(r["preimport_bad_hash"])
        self.assertIsNone(r["preimport_nested"])
        self.assertEqual(evidence_decision_name(r), "audio_corrupt")

    def test_force_mode_does_not_set_keep_searching(self):
        """The preimport-fact reject dict reflects the auto-path requeue
        invariant; force/manual leaves the parent request alone
        (the unified reject helper independently forces requeue=True via
        ``_PREIMPORT_FACT_REJECT_DECISIONS`` — that's tested in
        tests/test_import_dispatch_evidence.py)."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=245, is_cbr=False,
            audio_corrupt=True,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(import_mode="force"),
        )

        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertIsNone(r["final_status"])
        self.assertFalse(r["denylisted"])
        self.assertFalse(r["keep_searching"])



if __name__ == "__main__":
    unittest.main()
