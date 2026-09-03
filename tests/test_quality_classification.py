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

from datetime import UTC

from lib.json_narrow import json_dict
from lib.quality import (
    STAGE2_COUNTERFACTUAL_UNAVAILABLE,
    AlbumQualityEvidenceDecisionFacts,
    CodecFamily,
    classify_full_pipeline_decision,
    full_pipeline_decision,
)
from tests.evidence_helpers import (
    PROVISIONAL_LANE_DECISIONS,
    build_parity_candidate_evidence,
    build_parity_current_evidence,
    make_aac_lattice_capture,
)


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

        Post tie-defer fix (Mark DeNardo, request 1308): an equal spectral
        floor (new 160 == existing 160) is a TIE, so Stage 1 no longer
        rejects — it defers to Stage 2, which rejects this equal-rank
        candidate as a ``downgrade``. The bug this guards is *acceptance* of a
        320 transcode from a 160k source; that is still prevented — the
        candidate is never imported and the search continues.

        Issue #813 Finding 2: the native-lossy ``downgrade`` return site used
        to leave the decision dict's ``denylisted`` field at its default
        ``False`` — a lie, since ``dispatch_action("downgrade").denylist``
        is ``True`` in production (the offering peer never gets a better
        candidate from re-grabbing the same source). Now single-sourced via
        ``resolve_pipeline_decision_denylist``, so the display matches the
        real write.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=320,
            is_cbr=True,
            # The codec has to be stated (issue #829 Phase 5 PR2b): production
            # only ever runs the preimport spectral gate on a lossless source
            # or an MP3, so a world that does not name its codec is a world
            # where the gate never fired. dangshnizzle's upload was an MP3.
            new_format="MP3",
            spectral_grade="likely_transcode",
            spectral_bitrate=160,
            existing_min_bitrate=320,
            existing_format="MP3",
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=160,
        )
        # Equal spectral floor ties → defers to Stage 2 → equal-rank downgrade.
        # Load-bearing guard: the transcode is NOT accepted.
        self.assertEqual(r["stage1_spectral"], "import")
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])
        self.assertTrue(r["keep_searching"])
        # Issue #813 Finding 2 pin: the display must match production exactly.
        from lib.quality import dispatch_action
        self.assertTrue(r["denylisted"])
        self.assertEqual(r["denylisted"], dispatch_action("downgrade").denylist)

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

    def test_mark_denardo_lion_tiger_bear_equal_spectral_higher_bitrate_imports(self):
        """BUG: a strictly-better transcode discarded as "not better".

        Mark DeNardo - Lion, Tiger, Bear (request 1308, download_log 37700,
        ruxxell2, 2026-07-21). Candidate: MP3 192 CBR, spectral grade
        ``suspect``, spectral estimate 128. On-disk: MP3 128 CBR, spectral
        grade ``likely_transcode``, spectral estimate 128. On every signal the
        candidate was better or equal — container 192 > 128, grade suspect
        (66% suspect tracks) vs likely_transcode (100%), V0 research 209 > 187
        — yet it was rejected as "Spectral quality not better than on-disk
        copy; searching continues".

        Root cause: Stage 1 ``spectral_import_decision`` compared ONLY the
        spectral estimate (128 <= 128 → reject) and short-circuited before
        Stage 2 ``compare_quality`` ever ran. An equal spectral floor is a
        TIE, not a downgrade; it now defers to Stage 2, whose codec-aware
        metric tiebreak picks the higher-container copy (192 vs 128, delta 64
        ≫ tolerance 5) as ``better`` → import. Archivist-correct outcome: the
        less-degraded transcode lands on disk and the search for a lossless
        copy continues.

        (V0 209/187 are native-lossy research probes — subject=installed,
        non-comparable — so they carry no policy weight here; the decision
        turns on the spectral tie + container tiebreak alone. The V0 numbers
        are recorded in the docstring as forensic context, not decision input.)
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=192,
            is_cbr=True,
            avg_bitrate=192,
            new_format="MP3",
            spectral_grade="suspect",
            spectral_bitrate=128,
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
            existing_format="MP3",
            existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=128,
        )
        # Stage 1 tie defers; Stage 2 codec-aware tiebreak imports the better copy.
        self.assertEqual(r["stage1_spectral"], "import")
        self.assertEqual(r["stage2_import"], "import")
        self.assertEqual(r["comparison_basis"]["verdict"], "better")
        self.assertEqual(r["comparison_basis"]["branch"], "metric_tiebreak")
        self.assertTrue(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_wavves_aac_natural_rolloff_is_not_a_transcode_accusation(self):
        """Download 37946 / request 6387, Wavves — *Wavves* (issue #829).

        A 256 kbps CBR AAC persisted as ``likely_transcode`` /
        ``spectral_bitrate_kbps=128``, because every codec is measured
        through LAME's MP3 encoder lowpass table. Fed against an MP3 HAVE
        whose own cliff says 192, the codec-blind decider rejected the AAC
        at Stage 1 (128 < 192). AAC's cliff is a one-sided content floor —
        never a bitrate, never an accusation — so it now contributes
        nothing and the AAC's real 256 kbps container decides.

        Full decision-consequence coverage, including the MP3
        counterfactual on identical numbers, is
        ``TestWavvesAacCodecBlindSpectral``.
        """
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=True, avg_bitrate=256,
            new_format="AAC",
            spectral_grade="likely_transcode", spectral_bitrate=128,
            existing_min_bitrate=192, existing_avg_bitrate=192,
            existing_format="MP3", existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=192,
        )
        self.assertEqual(r["stage0_spectral_gate"], "skipped_uncalibrated_codec")
        self.assertIsNone(r["stage1_spectral"])
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])

    def test_fall_2007_fake_320_does_not_displace_genuine_160(self):
        """Request 8902, Iron & Wine — *Fall 2007* (issue #911).

        An MP3 CBR 320 whose measured cliff (16.5 kHz) puts its real content
        at the 160 class, against a genuine MP3 CBR 160 with no spectral
        bitrate. The raw 320 container manufactured a ``transparent`` rank
        and displaced the genuine copy; the genuine copy then displaced it
        back, forever. The candidate is now bounded by its own class and the
        two are equivalent.

        Both directions and the legacy-bucket variant: ``TestFall2007AntiLoop``.
        """
        from lib.quality import SpectralCodecContext
        r = full_pipeline_decision(
            is_flac=False, min_bitrate=320, is_cbr=True, avg_bitrate=320,
            new_format="MP3",
            spectral_grade="likely_transcode", spectral_bitrate=128,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="mp3", cliff_hz=16500, filetype_band="mp3"),
            existing_min_bitrate=160, existing_avg_bitrate=160,
            existing_format="MP3", existing_is_cbr=True,
            existing_spectral_grade="genuine",
        )
        self.assertEqual(
            r["comparison_basis"]["branch"], "spectral_candidate_bound")
        self.assertEqual(r["comparison_basis"]["verdict"], "equivalent")
        self.assertEqual(r["comparison_basis"]["new_value_kbps"], 160)
        self.assertFalse(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_request_3182_spectral_class_stays_with_the_encode(self):
        """Request 3182 / evidence 6233 vs 342: role cannot change quality.

        The candidate's VBR container averages 275k but its decision-grade
        class is 192k.  The genuine installed MP3 averages 190k.  Those
        effective values are inside the configured tolerance, so neither
        direction authorizes a replacement.  Raw persisted evidence remains
        275k for diagnosis; only the comparison is normalized.
        """
        from lib.quality import SpectralCodecContext

        forward = full_pipeline_decision(
            is_flac=False, min_bitrate=275, avg_bitrate=275, is_cbr=False,
            new_format="MP3",
            spectral_grade="likely_transcode", spectral_bitrate=192,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="mp3", filetype_band="mp3"),
            existing_min_bitrate=190, existing_avg_bitrate=190,
            existing_format="MP3", existing_is_cbr=False,
            existing_spectral_grade="genuine",
        )
        reverse = full_pipeline_decision(
            is_flac=False, min_bitrate=190, avg_bitrate=190, is_cbr=False,
            new_format="MP3", spectral_grade="genuine",
            existing_min_bitrate=275, existing_avg_bitrate=275,
            existing_format="MP3", existing_is_cbr=False,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=192,
            existing_spectral_context=SpectralCodecContext(
                codec_family="mp3", filetype_band="mp3"),
        )

        self.assertEqual(forward["stage2_import"], "downgrade")
        self.assertFalse(forward["imported"])
        self.assertEqual(reverse["stage2_import"], "downgrade")
        self.assertFalse(reverse["imported"])
        self.assertEqual(
            forward["comparison_basis"]["branch"],
            "spectral_candidate_bound",
        )
        self.assertEqual(
            reverse["comparison_basis"]["branch"],
            "spectral_existing_bound",
        )
        self.assertEqual(
            (forward["comparison_basis"]["new_value_kbps"],
             forward["comparison_basis"]["existing_value_kbps"]),
            (192, 190),
        )
        self.assertEqual(
            (reverse["comparison_basis"]["new_value_kbps"],
             reverse["comparison_basis"]["existing_value_kbps"]),
            (190, 192),
        )
        self.assertEqual(forward["comparison_basis"]["tolerance_kbps"], 5)
        self.assertEqual(reverse["comparison_basis"]["tolerance_kbps"], 5)

    def test_deerhunter_rhapsody_original_identical_transcode_not_upgrade(self):
        """BUG: an identical transcode scored as an upgrade via a one-sided clamp.

        Deerhunter - Rhapsody Original (request 6795, download_log 37725,
        serkanovat, 2026-07-21). The candidate and the on-disk copy are
        quality-identical: MP3 256 CBR, spectral grade ``likely_transcode``,
        spectral estimate 192, native-lossy V0 research 241/232. The candidate
        was a wrong-pressing match (beets distance 0.199) so validation rejected
        it; the wrong-match cleanup then re-scored it on quality and stamped
        ``kept_would_import`` / ``requeue_upgrade`` — treating an identical
        transcode as an upgrade over what is already installed.

        Root cause (issue #813 Finding 1): the existing-side spectral-floor
        ``override_min_bitrate`` floored the installed copy to its spectral
        estimate (256 -> 192) while the candidate kept its raw container bitrate
        (256). The raw ``metric_tiebreak`` then compared candidate container 256
        against existing spectral 192 and called it ``better``. Both sides carry
        a spectral estimate, so ``_shared_spectral_bitrates`` already floors both
        symmetrically for rank; the one-sided override is now skipped when the
        shared clamp governs, so the tiebreak compares TRUE containers (256 vs
        256) -> ``equivalent`` -> not an upgrade. The request keeps searching for
        a genuinely-better copy (the installed one is still a transcode) — it
        just no longer re-grabs identical transcodes as phantom upgrades.

        Must-still-work guard:
        ``test_mark_denardo_lion_tiger_bear_equal_spectral_higher_bitrate_imports``
        (higher container 192 > 128, equal spectral) STILL imports as better.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=256,
            is_cbr=True,
            avg_bitrate=256,
            new_format="MP3",
            spectral_grade="likely_transcode",
            spectral_bitrate=192,
            existing_min_bitrate=256,
            existing_avg_bitrate=256,
            existing_format="MP3",
            existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=192,
            # The existing-side spectral floor the real pipeline derives from
            # the installed transcode (min(256, 192)); the fix must neutralise
            # its one-sided effect now that the candidate also carries spectral.
            override_min_bitrate=192,
        )
        self.assertEqual(r["stage1_spectral"], "import")
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertEqual(r["comparison_basis"]["verdict"], "equivalent")
        self.assertEqual(r["comparison_basis"]["branch"], "metric_tiebreak")
        # True containers compared symmetrically — existing NOT floored to 192.
        self.assertEqual(r["comparison_basis"]["new_value_kbps"], 256)
        self.assertEqual(r["comparison_basis"]["existing_value_kbps"], 256)
        self.assertFalse(r["imported"])
        # Never stop searching: the installed copy is still a transcode.
        self.assertTrue(r["keep_searching"])
        # Issue #813 Finding 2: downgrade always denylists in production.
        self.assertTrue(r["denylisted"])

    def test_stage_parity_review_f1_unbound_tied_spectral_stays_equivalent(self):
        """PR #827 review finding F1: neither side is spectral-bound here
        (both containers are LOWER than their own tied 256 spectral
        estimate), so the compared values are the raw avg metrics — a
        stealth ``metric_tiebreak`` with no tolerance, not a genuine
        ``spectral_tiebreak``. Before the fix (gating the tiebreak on
        ``new_bound and existing_bound``), the tied 256/256 spectral values
        made both sides classify identically (rank ties), and the
        UNGATED spectral_tiebreak branch then compared the RAW avg values
        (250 vs 247) with NO tolerance, flipping this into a phantom
        "better"/imported=True. With the ±5kbps tolerance restored via the
        raw ``metric_tiebreak`` fallback, delta=3 stays "equivalent" —
        not imported.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=250,
            is_cbr=False,
            avg_bitrate=250,
            new_format="MP3",
            spectral_grade="genuine",
            spectral_bitrate=256,
            existing_min_bitrate=247,
            existing_avg_bitrate=247,
            existing_format="MP3",
            existing_is_cbr=False,
            existing_spectral_grade="genuine",
            existing_spectral_bitrate=256,
        )
        self.assertEqual(r["comparison_basis"]["verdict"], "equivalent")
        self.assertEqual(r["comparison_basis"]["branch"], "metric_tiebreak")
        self.assertFalse(r["imported"])

    def test_stage_parity_review_f2_asymmetric_cbr_forcing_stays_worse(self):
        """PR #827 review finding F2: existing's spectral (256) IS bound
        (256 <= its own 260 container) but candidate's spectral (320)
        is NOT bound (320 > its own 246 container) — an asymmetric case.
        Before the fix (requiring BOTH sides bound before forcing CBR
        bands), existing alone got demoted from VBR "transparent" to CBR
        "excellent" while candidate kept VBR "transparent" unforced,
        letting a lower-container V0 candidate (246) outrank a
        higher-container V0 existing (260) purely from an asymmetric
        table swap driven by cliff-bucket noise at the V0 lowpass
        boundary. With CBR-forcing withheld unless both sides are bound,
        both classify under their own (matching) VBR table and the raw
        containers correctly decide: candidate 246 stays worse than
        existing 260 — not imported.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=246,
            is_cbr=False,
            avg_bitrate=246,
            new_format="MP3",
            spectral_grade="genuine",
            spectral_bitrate=320,
            existing_min_bitrate=260,
            existing_avg_bitrate=260,
            existing_format="MP3",
            existing_is_cbr=False,
            existing_spectral_grade="genuine",
            existing_spectral_bitrate=256,
        )
        self.assertEqual(r["comparison_basis"]["verdict"], "worse")
        self.assertFalse(r["imported"])

    def test_stage_parity_cross_codec_vorbis_bucket_never_rejects(self):
        """Issue #829 Phase 5 PR2c — the cross-codec half of the #813
        parity contract, as a decided outcome.

        The pre-#829 Stage-1 seam fed ``spectral_import_decision`` the two
        sides' RAW stored buckets, whatever codec produced them. Here that
        is an MP3 candidate's real 128 against a Vorbis HAVE's 192 — and a
        Vorbis 192 is the LAME table's documented one-directional over-read
        of q4's real 128 kbps, not a Vorbis class. Five live rows carry
        exactly this shape (evidence ids 33935/33941/33942/33943/33974, two
        of them holding the over-read 192).

        Stage 1 therefore rejected, short-circuited before Stage 2 ever
        ran, denylisted the source and left the request ``wanted``. The
        shipped seam refuses the comparison (``spectral_classes_comparable``
        → ``cross_codec_legacy_bucket``), Stage 1 withholds, and Stage 2
        decides on rank — which is an import.
        """
        # The counterfactual, driven through the REAL Stage-1 decider with
        # the raw stored numbers the pre-#829 seam handed it.
        from lib.quality import spectral_import_decision
        self.assertEqual(
            spectral_import_decision("likely_transcode", 128, 192), "reject")

        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=256,
            avg_bitrate=256,
            is_cbr=True,
            new_format="MP3",
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
            existing_format="Vorbis",
            existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=192,
        )
        # The decided outcome, not a proxy: the candidate is imported.
        self.assertEqual(r["stage1_spectral"], "import_no_exist")
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertEqual(r["comparison_basis"]["verdict"], "better")
        # Withholding a spectral opinion never stops the search.
        self.assertTrue(r["keep_searching"])

    def test_stage_parity_aac_have_bucket_never_rejects(self):
        """Issue #829 Phase 5 PR2c — download 37946's defect pointed at the
        library instead of the candidate.

        The installed AAC's 15.5 kHz cliff is native AAC behaviour at every
        encoder rate the four-arm calibration measured from 96 to 320 kbps,
        so its LAME-bucketed 192 is not a class in any codec's terms. The
        pre-#829 seam nonetheless weighed it against an MP3 candidate's
        real 128 and rejected a genuine upgrade over a 112 kbps AAC.
        """
        from lib.quality import SpectralCodecContext, spectral_import_decision
        self.assertEqual(
            spectral_import_decision("likely_transcode", 128, 192), "reject")

        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=256,
            avg_bitrate=256,
            is_cbr=True,
            new_format="MP3",
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
            existing_min_bitrate=112,
            existing_avg_bitrate=112,
            existing_format="AAC",
            existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=192,
            existing_spectral_context=SpectralCodecContext(cliff_hz=15500),
        )
        self.assertEqual(r["stage1_spectral"], "import_no_exist")
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertEqual(r["comparison_basis"]["existing_format"], "aac")
        self.assertTrue(r["keep_searching"])

    def test_taboo_vi_fake_flac_192_requires_an_explicit_v0_probe(self):
        """A fake FLAC cannot turn its target projection into source evidence.

        Request 257, 2026-03-28. amyslskduser uploaded FLAC that was actually
        a 192k transcode. Spectral said likely_transcode but estimated_bitrate
        was None (HF deficit, not cliff). V0 conversion produced 224kbps which
        is above the 210 threshold, so import_one.py didn't flag as transcode.
        The conversion's 224 kbps minimum is target-projection evidence, not
        a ``lossless_source_v0`` source probe. Without an explicit probe, the
        accused candidate fails closed as probe-missing and keeps searching.

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
        self.assertEqual(r["stage2_import"], "suspect_lossless_probe_missing")
        self.assertFalse(r["imported"])
        self.assertTrue(r["keep_searching"])
        self.assertTrue(r["denylisted"])

    def test_taboo_vi_with_spectral_bitrate_still_requires_an_explicit_v0_probe(self):
        """A spectral estimate cannot make a target minimum into a probe."""
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
        self.assertEqual(r["stage2_import"], "suspect_lossless_probe_missing")
        self.assertIsNone(r["stage3_quality_gate"])
        self.assertFalse(r["imported"])
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
            candidate_v0_probe_kind="lossless_source_v0",
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
        gate (``lossless_source_not_better``). This is the same reducer
        ``cleanup_wrong_match`` calls (lib/wrong_match_cleanup_service.py).

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
            candidate_v0_probe_kind="lossless_source_v0",
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
            existing_v0_probe_kind="lossless_source_v0",
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

    def test_olivia_rodrigo_wav_basis_uses_target_contract(self):
        """BUG: the persisted basis labeled a proxy value "avg" (dl 36660).

        Request 8781, 2026-07-11. WAV source converted to Opus (real files:
        min 216 / avg 255) vs on-disk AAC avg 256. The decision pipeline
        synthesized the compared measurement with avg fabricated = the
        post-conversion MIN, so the persisted basis read "avg 216k" while
        the V0-probe row on the same card honestly said "255kbps avg" —
        the display-lie class #608 exists to kill, injected one seam
        earlier at measurement synthesis. Because the rank is actually
        classified by the explicit Opus target, the basis must identify the
        128k contract rather than attach any measured label to the proxy.
        """
        r = full_pipeline_decision(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="genuine",
            existing_min_bitrate=256,
            existing_avg_bitrate=256,
            existing_format="AAC",
            post_conversion_min_bitrate=216,
            converted_count=14,
            verified_lossless_target="opus 128",
            candidate_v0_probe_avg=255,
            candidate_v0_probe_min=216,
            existing_v0_probe_avg=250,
            existing_v0_probe_kind="native_lossy_research_v0",
        )
        # Quality passed via the verified-lossless bypass (the rejection in
        # production was mbid_missing, downstream of this decision).
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        basis = r["comparison_basis"]
        assert basis is not None
        self.assertEqual(basis["branch"], "cross_family_same_rank")
        self.assertEqual(basis["verdict"], "equivalent")
        self.assertTrue(basis["verified_lossless_bypass"])
        # The honest labels: the candidate side was classified by the target
        # contract; the existing side was classified by its real average.
        self.assertEqual(basis["new_metric"], "contract")
        self.assertEqual(basis["new_value_kbps"], 128)
        self.assertEqual(basis["existing_metric"], "avg")
        self.assertEqual(basis["existing_value_kbps"], 256)

    def test_dirt_dress_theme_songs_marked_incomplete_imports_complete_candidate(
        self,
    ):
        """Request 1852, Dirt Dress — *Theme Songs*, Discogs 4738671 (#1241).

        The live incident (download_log 40355, peer iosononessuno). Measured
        world:

        * Installed: 4 files, 719.4 s, AAC ~128 kbps. Track 04 is 181.4 s
          where two declared components total 881 s — 700 s of declared
          program ("Peter and the Wolf", Discogs position 1B) is simply not
          on disk. 49% of the runtime is missing.
        * Candidate: 5 files, 1419.9 s, MP3 ~196 kbps, complete.

        The cleanup reducer asked the quality question, got an honest
        cross-family "equivalent" (aac good 128 vs mp3 good 196), decided
        ``downgrade``, and DELETED the only copy of the missing 700 s.

        Under #1241 the operator marks the request incomplete. With the mark
        set and beets' own proof that this candidate covers the declared
        program, the decider disregards the installed side entirely and the
        candidate is admitted exactly as it would be into an empty slot —
        "incomplete is incomplete and complete always always beats it."
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=196,
            avg_bitrate=196,
            is_cbr=False,
            new_format="MP3",
            existing_format="AAC",
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
            installed_marked_incomplete=True,
            candidate_covers_declared_program=True,
        )

        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertTrue(r["installed_incomplete_disregarded"])
        # No comparison ran — the installed side was disregarded, honestly:
        # the basis is absent rather than fabricated.
        self.assertIsNone(r["comparison_basis"])
        # A ~196k VBR import is below par, so the ordinary post-import gate
        # keeps the search open and denylists the peer against re-fetching
        # the same bytes — exactly the fresh-import policy, not a #1241
        # branch.
        self.assertEqual(r["stage3_quality_gate"], "requeue_upgrade")
        self.assertTrue(r["keep_searching"])
        verdict, cleanup_eligible, _reason = classify_full_pipeline_decision(r)
        self.assertEqual(verdict, "would_import")
        self.assertFalse(
            cleanup_eligible,
            "an import-class decision must never authorize folder deletion",
        )
        # Fresh-import equivalence — the whole invariant in one assertion:
        # the marked world's decision is byte-identical to the same candidate
        # arriving at an empty slot, except for the audit flag that records
        # the disregard.
        fresh = full_pipeline_decision(
            is_flac=False,
            min_bitrate=196,
            avg_bitrate=196,
            is_cbr=False,
            new_format="MP3",
        )
        fresh["installed_incomplete_disregarded"] = True
        self.assertEqual(r, fresh)

    def test_dirt_dress_world_without_a_covered_candidate_still_downgrades(
        self,
    ):
        """Negative twin: an UNPROVEN candidate never rescues itself.

        The same measured world, but the attempt carries no proof that the
        candidate covers the declared program — the shape the Wrong Matches
        reducer sees for an ``extra_tracks`` / ``mbid_not_found`` /
        ``no_choose_match`` row, none of which ever produced a checked
        candidate summary (``extra_tracks`` proves the OPPOSITE).

        One incomplete copy must never "upgrade" another, so the mark's
        candidate conjunct is load-bearing, not decorative: today's
        destructive ``downgrade`` is the correct outcome here, and the
        decision must be byte-identical to the unmarked world.
        """
        baseline = full_pipeline_decision(
            is_flac=False,
            min_bitrate=196,
            avg_bitrate=196,
            is_cbr=False,
            new_format="MP3",
            existing_format="AAC",
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
        )
        self.assertEqual(baseline["stage2_import"], "downgrade")
        for marked, covered in ((True, False), (False, True)):
            with self.subTest(marked=marked, covered=covered):
                r = full_pipeline_decision(
                    is_flac=False,
                    min_bitrate=196,
                    avg_bitrate=196,
                    is_cbr=False,
                    new_format="MP3",
                    existing_format="AAC",
                    existing_min_bitrate=128,
                    existing_avg_bitrate=128,
                    installed_marked_incomplete=marked,
                    candidate_covers_declared_program=covered,
                )
                self.assertEqual(r, baseline)
                self.assertEqual(r["stage2_import"], "downgrade")
                self.assertFalse(r["installed_incomplete_disregarded"])

    def test_a_worse_but_complete_candidate_imports_over_a_marked_incomplete(
        self,
    ):
        """Completeness outranks quality at EVERY level, ``worse`` included.

        MP3 96 CBR against an installed MP3 320 is an unambiguous ``worse``
        verdict — the widest quality gap the mark has to survive. It still
        imports, because the two sides are not the same program and the
        operator has said so: a 96 kbps copy that has the whole record beats
        a 320 kbps copy that does not. Authority: "incomplete is incomplete
        and complete always always beats it." — operator, issue #1241
        superseding comment (2026-08-25).

        This does not weaken issue #60's "worse is blocked regardless"
        acceptance criterion: that criterion governs a comparison between
        two copies of the SAME program, and the disregarded installed side
        never enters a comparison at all. Quality convergence resumes
        immediately — the below-par import keeps the search open, so the
        next complete candidate is judged by the NORMAL comparison against
        this now-complete copy.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=96,
            avg_bitrate=96,
            is_cbr=True,
            new_format="MP3",
            existing_format="MP3",
            existing_min_bitrate=320,
            existing_avg_bitrate=320,
            existing_is_cbr=True,
            installed_marked_incomplete=True,
            candidate_covers_declared_program=True,
        )

        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertTrue(r["installed_incomplete_disregarded"])
        # 96 CBR is far below par: the post-import gate keeps searching, so
        # the mark buys completeness without ever closing the quality search.
        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["keep_searching"])

    def test_a_worse_candidate_against_an_unmarked_install_still_rejects(
        self,
    ):
        """The no-regression twin: without the mark nothing changed.

        Same 96-vs-320 world, no operator mark. This is the pin that stops
        the disregard from becoming "any complete candidate imports itself
        over anything".
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=96,
            avg_bitrate=96,
            is_cbr=True,
            new_format="MP3",
            existing_format="MP3",
            existing_min_bitrate=320,
            existing_avg_bitrate=320,
            existing_is_cbr=True,
            candidate_covers_declared_program=True,
        )

        basis = json_dict(r["comparison_basis"])
        self.assertEqual(basis["verdict"], "worse")
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])
        self.assertFalse(r["installed_incomplete_disregarded"])

    def test_verified_lossless_locked_installed_yields_to_the_mark(self):
        """The mark disarms the proof lock — no lock-side machinery needed.

        Decision 21 makes an installed verified-lossless proof the absolute
        acquisition ceiling. A proof-LOCKED album that is nonetheless
        missing declared program (a pre-#1241 force-import of a partial
        rip, or a proof granted before the operator noticed) would
        otherwise be permanently closed. The operator's mark reopens it:
        with both conjuncts set, the locked installed side is disregarded
        like any other, and the complete candidate imports. The unmarked
        twin still locks — the ceiling itself is untouched.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=196,
            avg_bitrate=196,
            is_cbr=False,
            new_format="MP3",
            existing_format="AAC",
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
            current_verified_lossless_proof=True,
            installed_marked_incomplete=True,
            candidate_covers_declared_program=True,
        )
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertTrue(r["installed_incomplete_disregarded"])

        locked = full_pipeline_decision(
            is_flac=False,
            min_bitrate=196,
            avg_bitrate=196,
            is_cbr=False,
            new_format="MP3",
            existing_format="AAC",
            existing_min_bitrate=128,
            existing_avg_bitrate=128,
            current_verified_lossless_proof=True,
            candidate_covers_declared_program=True,
        )
        self.assertEqual(locked["stage2_import"], "verified_lossless_locked")
        self.assertFalse(locked["imported"])
        self.assertFalse(locked["installed_incomplete_disregarded"])

    def test_lossless_source_locked_installed_yields_to_the_mark(self):
        """The third existing-side lock, pinned deterministically (#1257
        review F5 — a mutant keeping the existing V0 probe under the
        disregard died only at the fuzz tier before this pin existed).

        An installed provisional-lossless copy's source V0 probe is the
        truth-of-source anchor that rejects every lossy candidate as
        ``lossless_source_locked``. Under the operator's mark plus beets'
        coverage proof, the anchor is disregarded with the rest of the
        installed side and the complete lossy candidate imports.
        """
        r = full_pipeline_decision(
            is_flac=False,
            min_bitrate=245,
            avg_bitrate=245,
            is_cbr=False,
            new_format="MP3",
            existing_format="Opus",
            existing_min_bitrate=110,
            existing_avg_bitrate=116,
            existing_v0_probe_avg=240,
            installed_marked_incomplete=True,
            candidate_covers_declared_program=True,
        )
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertTrue(r["installed_incomplete_disregarded"])

        locked = full_pipeline_decision(
            is_flac=False,
            min_bitrate=245,
            avg_bitrate=245,
            is_cbr=False,
            new_format="MP3",
            existing_format="Opus",
            existing_min_bitrate=110,
            existing_avg_bitrate=116,
            existing_v0_probe_avg=240,
            candidate_covers_declared_program=True,
        )
        self.assertEqual(locked["stage2_import"], "lossless_source_locked")
        self.assertFalse(locked["imported"])

    def test_issue_1355_corrupt_and_nested_denylists_the_peer(self):
        """Issue #1355 item 1: a candidate that is both corrupt AND nested
        must reject as audio_corrupt (not nested_layout) and denylist its
        peer. measure_preimport_state derives folder layout from a single
        path enumeration before it runs the audio-integrity decode, so a
        real download can carry both facts; a peer whose upload decodes as
        garbage must not escape the denylist just because its folder also
        happens to be nested (dispatch_actions.decision_denylists denylists
        audio_corrupt but not nested_layout)."""
        result = full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_corrupt=True, has_nested_audio=True,
        )
        self.assertEqual(result["preimport_audio"], "reject_corrupt")
        self.assertIsNone(result["preimport_nested"])
        self.assertFalse(result["imported"])
        self.assertTrue(result["denylisted"])
        self.assertEqual(result["final_status"], "wanted")
        self.assertTrue(result["keep_searching"])


class TestWavvesAacCodecBlindSpectral(unittest.TestCase):
    """Issue #829's opening defect, as a decision-consequence pin.

    Download 37946 / request 6387, Wavves — *Wavves*. An ordinary 256 kbps
    CBR AAC. ``lib/spectral_check.py``'s ``LAME_LOWPASS`` is a byte-exact
    transcription of LAME's MP3 encoder lowpass array, but ``analyze_album``
    measures every codec through it, so the AAC's natural 13-18 kHz rolloff
    was persisted as ``spectral_grade='likely_transcode'`` with
    ``spectral_bitrate_kbps=128`` (evidence 33591: ``format='AAC'``,
    ``cliff_hz`` NULL, ``codec_family`` NULL, min/avg 256/256, is_cbr=True).
    Its v2 sibling 33592 re-measured the same album as ``cliff_hz=15500``,
    ``codec_family='aac'``, ``spectral_measurement_version=2``.

    The four-arm calibration measured what an AAC cliff in that band means:
    a one-sided content floor produced by encoder rates from 96 to 320 kbps
    across ffmpeg-native, libfdk AND Apple CoreAudio. It is never a bitrate
    and never a transcode accusation.

    The pin is the DECIDED OUTCOME, and it turns on the codec label alone:
    the identical numbers labelled MP3 (where the LAME ladder IS calibrated)
    still reject, and labelled AAC now import on the album's real 256 kbps
    container. Before PR2b both answered "reject" — the AAC was scored on
    MP3's ladder, which is the whole defect.

    Scope of the claim: the CANDIDATE is evidence 33591 / 33592 exactly.
    The HAVE is a COUNTERFACTUAL — an MP3 whose own cliff says 192 — not
    the live installed copy, which was a 320 carrying a 128 estimate and
    which ``main`` already imports over. The counterfactual is chosen
    because it is the shape where the AAC's spurious class was DECISIVE,
    and a pin has to move something to be a pin.
    """

    @staticmethod
    def _have():
        """The MP3 HAVE whose own cliff really does say 192."""
        return build_parity_current_evidence(
            min_bitrate=192, avg_bitrate=192, format="MP3", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=192,
        )

    def _decide(self, *, native_codec: str, native_format: str,
                cliff_hz: int | None = None,
                codec_family: CodecFamily | None = None):
        from lib.quality import full_pipeline_decision_from_evidence
        candidate = build_parity_candidate_evidence(
            is_flac=False, min_bitrate=256, is_cbr=True, avg_bitrate=256,
            spectral_grade="likely_transcode", spectral_bitrate=128,
            native_codec=native_codec, native_format=native_format,
            cliff_hz=cliff_hz, codec_family=codec_family,
        )
        return full_pipeline_decision_from_evidence(candidate, self._have())

    def test_mp3_counterfactual_still_rejects(self):
        # The calibrated ladder: an MP3 whose cliff says 128 against a HAVE
        # whose cliff says 192 really is less content. Stage 1 rejects.
        r = self._decide(native_codec="mp3", native_format="MP3")
        self.assertEqual(r["stage1_spectral"], "reject")
        self.assertFalse(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_legacy_aac_row_imports_on_its_real_container(self):
        # Evidence 33591's shape. The AAC contributes no class, production's
        # gate would never have measured it, and its real 256 kbps container
        # outranks the MP3 192 HAVE.
        r = self._decide(native_codec="aac", native_format="AAC")
        self.assertEqual(r["stage0_spectral_gate"], "skipped_uncalibrated_codec")
        self.assertIsNone(r["stage1_spectral"])
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])

    def test_v2_aac_row_with_a_real_cliff_imports_too(self):
        # Evidence 33592's shape: a REAL measured cliff at 15.5 kHz plus the
        # PR1 codec capture. 15.5 kHz is squarely inside the band 94-96% of
        # all AAC cliffs land in, so it supports a >=96 content floor and
        # nothing else — never a class, never an accusation.
        r = self._decide(
            native_codec="aac", native_format="AAC",
            cliff_hz=15500, codec_family="aac",
        )
        self.assertIsNone(r["stage1_spectral"])
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])

    def test_codec_label_alone_flips_the_import_outcome(self):
        # The load-bearing pin: identical bitrates, identical grade,
        # identical HAVE — only the measured codec differs.
        self.assertFalse(
            self._decide(native_codec="mp3", native_format="MP3")["imported"])
        self.assertTrue(
            self._decide(native_codec="aac", native_format="AAC")["imported"])


class TestMixedBasisDisarmWindow(unittest.TestCase):
    """download_log 29525 — Clue to Kalo, *Lily Perdida* (PR2b review B1).

    Two mechanisms can represent an installed album by its real content
    instead of its container: the symmetric clamp inside
    ``_shared_spectral_bitrates``, and the one-sided
    ``override_min_bitrate``. ``full_pipeline_decision`` disarms the
    one-sided override precisely when the clamp governs instead — the two
    predicates must be the SAME condition, or a window opens where neither
    fires and a known-fake 320 keeps its inflated rank.

    This is the live world that found the window. The HAVE (evidence 17273)
    is an MP3 CBR 320 graded ``likely_transcode`` whose measured
    ``cliff_hz=15500`` re-derives to the 128 class. The candidate (evidence
    22689) is an MP3 VBR 217/234 graded ``likely_transcode`` carrying only a
    legacy stored bucket of 192 — no cliff.

    Their bases differ (``cliff_hz`` vs ``stored_bucket``), so the classes
    are not comparable and the clamp correctly withholds. The override must
    therefore still fire: the installed copy is represented by its own
    class, and the better candidate imports. 132 of 9,219 live pairs sit in
    this window, and it widens while ``cliff_hz`` capture rolls out.
    """

    def _decide(self):
        from lib.quality import full_pipeline_decision_from_evidence
        candidate = build_parity_candidate_evidence(
            is_flac=False, min_bitrate=217, avg_bitrate=234, is_cbr=False,
            spectral_grade="likely_transcode", spectral_bitrate=192,
        )
        current = build_parity_current_evidence(
            min_bitrate=320, avg_bitrate=320, format="MP3", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=128,
            cliff_hz=15500, codec_family="mp3",
        )
        return full_pipeline_decision_from_evidence(candidate, current)

    def test_the_better_candidate_still_imports(self):
        r = self._decide()
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])

    def test_the_fake_320_is_represented_by_its_own_class(self):
        # The load-bearing detail: the HAVE is compared at 128 (its own
        # cliff-derived class), not at its 320 container. If the override
        # were disarmed here this reads 320 and the verdict flips to
        # ``downgrade``.
        basis = self._decide()["comparison_basis"]
        self.assertEqual(basis["existing_value_kbps"], 128)
        self.assertEqual(basis["existing_rank"], "acceptable")
        self.assertEqual(basis["verdict"], "better")

    def test_the_clamp_itself_correctly_withholds(self):
        # ...and it is genuinely the one-sided override doing the work, not
        # the symmetric clamp: a mixed-derivation pair is never clamped.
        self.assertFalse(self._decide()["comparison_basis"]["spectral_clamped"])


class TestFall2007AntiLoop(unittest.TestCase):
    """Issue #911 folded into #829 Phase 5 PR2b — request 8902, Iron & Wine
    *Fall 2007*, live and looping.

    Authority: #829's accepted Fall 2007 case records that a fake
    320/spectral-128 versus genuine-160 is equivalent / not an import —
    https://github.com/abl030/cratedigger/issues/829#issuecomment-5098696861

    Issue #1157 keeps that fixed point while making the one-class comparison
    role-neutral and applying its same-family tolerance to effective values.

    The fake side is evidence id 34219: ``codec='mp3'``, ``format='MP3'``,
    ``spectral_grade='likely_transcode'``, ``spectral_bitrate_kbps=128``,
    ``cliff_hz=16500``, ``codec_family='mp3'``,
    ``spectral_measurement_version=2``, min/avg 320/320, ``is_cbr=True``,
    ``filetype_band='mp3'``. Note the two derivations disagree by one tier:
    16500 Hz re-derives to the 160 class through the detector-space ladder
    while the stored legacy value is 128. Both answers refuse the import
    here, which is why the pin is robust to which one a row carries.

    The loop: the fake's RAW 320 container manufactures a ``transparent``
    rank and displaces the genuine 160; later the genuine 160 displaces the
    fake back, because the fake's stored 128 floors it below 160. Forever.
    """

    def _fake_320(self):
        return build_parity_candidate_evidence(
            is_flac=False, min_bitrate=320, is_cbr=True, avg_bitrate=320,
            spectral_grade="likely_transcode", spectral_bitrate=128,
            cliff_hz=16500, codec_family="mp3",
        )

    def _fake_320_have(self):
        return build_parity_current_evidence(
            min_bitrate=320, avg_bitrate=320, format="MP3", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=128,
            cliff_hz=16500, codec_family="mp3",
        )

    def _genuine_160(self):
        return build_parity_candidate_evidence(
            is_flac=False, min_bitrate=160, is_cbr=True, avg_bitrate=160,
            spectral_grade="genuine",
        )

    def _genuine_160_have(self):
        return build_parity_current_evidence(
            min_bitrate=160, avg_bitrate=160, format="MP3", is_cbr=True,
            spectral_grade="genuine",
        )

    def _decide(self, candidate, current):
        from lib.quality import full_pipeline_decision_from_evidence
        return full_pipeline_decision_from_evidence(candidate, current)

    def test_fake_320_does_not_displace_the_genuine_160(self):
        r = self._decide(self._fake_320(), self._genuine_160_have())
        basis = r["comparison_basis"]
        self.assertEqual(basis["branch"], "spectral_candidate_bound")
        self.assertEqual(basis["verdict"], "equivalent")
        # The bound is the candidate's OWN class, re-derived from its cliff.
        self.assertEqual(basis["new_value_kbps"], 160)
        self.assertEqual(basis["existing_value_kbps"], 160)
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])
        # Never stop searching: neither copy is provably clean at 320.
        self.assertTrue(r["keep_searching"])

    def test_genuine_160_does_not_displace_the_fake_320_either(self):
        # The other half of the loop. The installed fake is represented by
        # its own class (160, from cliff 16500) instead of its inflated 320
        # container, so the genuine 160 is equivalent — not an upgrade.
        r = self._decide(self._genuine_160(), self._fake_320_have())
        self.assertEqual(r["comparison_basis"]["verdict"], "equivalent")
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])

    def test_the_loop_has_a_fixed_point(self):
        """Neither direction imports — the request stops thrashing.

        This is the decided-outcome pin. An oscillation is exactly the
        state where BOTH directions import; asserting each direction alone
        would not rule it out.
        """
        forward = self._decide(self._fake_320(), self._genuine_160_have())
        backward = self._decide(self._genuine_160(), self._fake_320_have())
        self.assertFalse(forward["imported"] and backward["imported"])
        self.assertFalse(forward["imported"])
        self.assertFalse(backward["imported"])

    def test_legacy_stored_bucket_refuses_the_import_too(self):
        """Robustness: the same album WITHOUT the PR1 cliff capture.

        A pre-PR1 row carries only the stored 128 bucket. The bound is then
        128 rather than 160, which is a lower rank than the genuine 160's
        — still not an import. The two derivations disagree by a tier and
        agree on the verdict.
        """
        import msgspec
        legacy = self._fake_320()
        legacy = msgspec.structs.replace(
            legacy,
            measurement=msgspec.structs.replace(
                legacy.measurement, cliff_hz=None,
                spectral_measurement_version=None,
            ),
        )
        r = self._decide(legacy, self._genuine_160_have())
        self.assertEqual(
            r["comparison_basis"]["branch"], "spectral_candidate_bound")
        self.assertEqual(r["comparison_basis"]["new_value_kbps"], 128)
        self.assertFalse(r["imported"])

    def test_an_unmeasured_have_is_not_a_known_clean_have(self):
        """The bound's precondition, pinned in the negative.

        A HAVE that was never spectrally measured carries no verdict at all.
        Bounding the candidate against it would be asserting the HAVE is
        clean on no evidence, so the container comparison stands and the
        320 imports — deliberately, and only until the HAVE is measured.
        """
        unmeasured = build_parity_current_evidence(
            min_bitrate=160, avg_bitrate=160, format="MP3", is_cbr=True,
        )
        r = self._decide(self._fake_320(), unmeasured)
        self.assertEqual(r["comparison_basis"]["branch"], "rank")
        self.assertTrue(r["imported"])


class TestSpectralLandmineDecisionConsequence(unittest.TestCase):
    """Issue #815 dl-37742 counterfactual: the persisted HAVE spectral grade
    flips the import decision through the REAL production decider.

    Shugo Tokumaru EXIT (request 4351, dl 37742). The installed genuine 192
    copy carried a STALE ``likely_transcode``/128 landmine (a rejected fake-320
    candidate's grade adopted in May-2026 and frozen into evidence). The fresh
    audit of the installed bytes says ``genuine``/160, and #815 fresh-audit-wins
    now re-persists it. Fed the exact fake-320 candidate
    (``likely_transcode``/128) against that installed 192 copy,
    ``full_pipeline_decision_from_evidence`` (the function the importer actually
    calls) reverses outcome on the HAVE grade alone:

    - fresh genuine/160  -> Stage 1 REJECTS the fake-320, imported=False
      (the genuine copy is protected).
    - stale lt/128 landmine -> Stage 1 imports, imported=True — the actual
      data-loss path that replaced the genuine 192 with the fake-320.

    (Note the missing-bitrate shape genuine/None routes through
    ``import_no_exist`` and imports — which is why the HAVE bitrate is part of
    what fresh-audit-wins re-persists, not just the grade.)
    """

    def _decide(self, have_grade: str | None, have_bitrate: int | None):
        from lib.quality import full_pipeline_decision_from_evidence
        candidate = build_parity_candidate_evidence(
            is_flac=False, min_bitrate=320, is_cbr=True, avg_bitrate=320,
            spectral_grade="likely_transcode", spectral_bitrate=128,
        )
        current = build_parity_current_evidence(
            min_bitrate=192, avg_bitrate=192, format="MP3", is_cbr=True,
            spectral_grade=have_grade, spectral_bitrate=have_bitrate,
        )
        return full_pipeline_decision_from_evidence(candidate, current)

    def test_fresh_genuine_have_rejects_the_fake_320(self):
        # Fresh-audit-wins value (genuine/160): the fake-320 candidate is
        # rejected and the genuine 192 copy is protected.
        #
        # Issue #829 Phase 5 PR2b moved WHERE that reject is made. A
        # ``genuine`` album verdict authorizes no spectral class at all, so
        # Stage 1 has nothing to compare and withholds; the protection is now
        # Stage 2's one-class comparison, which weighs the fake's OWN class
        # (128) against the genuine HAVE's real metric instead of weighing two
        # cliff estimates. Strictly more evidence, same outcome — and it is
        # the same mechanism that breaks the Fall 2007 loop (issue #911).
        r = self._decide("genuine", 160)
        self.assertEqual(r["stage1_spectral"], "import_no_exist")
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertEqual(
            r["comparison_basis"]["branch"], "spectral_candidate_bound")
        self.assertFalse(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_stale_transcode_landmine_imports_the_fake_320(self):
        # The pre-#815 landmine (frozen likely_transcode/128) is the actual
        # dl-37742 displacement: the fake-320 imports over the genuine copy.
        r = self._decide("likely_transcode", 128)
        self.assertEqual(r["stage1_spectral"], "import")
        self.assertTrue(r["imported"])
        # The landmine degrades the installed copy's rank to acceptable.
        self.assertEqual(r["comparison_basis"]["existing_rank"], "acceptable")

    def test_have_grade_flips_the_import_outcome(self):
        # The load-bearing pin: the persisted HAVE grade alone flips imported.
        self.assertFalse(self._decide("genuine", 160)["imported"])
        self.assertTrue(self._decide("likely_transcode", 128)["imported"])


class TestBoundaryHysteresisAlbums(unittest.TestCase):
    """Albums where a sub-tolerance difference must NOT authorise a replace.

    Every bug in the #1144/#1145 series is one bug: a small measurement
    difference crossing a discrete boundary produces a destructive action —
    a full replace, a ``beet move``, and media-server churn for a difference
    no listener can hear. These are the real worlds, kept as the contract.

    The two controls at the end are what stop the guard from becoming "never
    upgrade": a genuinely larger gap must still import, and a cross-family
    pair must still be decided on rank even when its two bitrates are one
    kbps apart, because those two numbers do not mean the same thing.
    """

    @staticmethod
    def _decide(
        *,
        candidate_kbps: int,
        candidate_format: str = "MP3",
        candidate_is_cbr: bool = False,
        installed_kbps: int,
        installed_format: str = "MP3",
        installed_is_cbr: bool = False,
    ) -> dict[str, object]:
        return full_pipeline_decision(
            is_flac=False,
            min_bitrate=candidate_kbps,
            avg_bitrate=candidate_kbps,
            is_cbr=candidate_is_cbr,
            new_format=candidate_format,
            existing_min_bitrate=installed_kbps,
            existing_avg_bitrate=installed_kbps,
            existing_format=installed_format,
            existing_is_cbr=installed_is_cbr,
        )

    def test_koppel_one_kbps_reimport_over_itself(self):
        """BUG: download_log 39947, request 4781 — the album that started it.

        Thomas Koppel, *Improvisationer for Klaver*. A re-download of an
        album already installed at CBR 256 measured 255: the exact frame
        arithmetic is 256 kbps
        (``8517888 * 8 == 266.184 s * 256 kbps * 1000``) but the float path
        yielded ``255.99999999999997`` and truncated. Two defects then
        stacked on that one kbps: the
        candidate's non-uniformity made it look VBR, so it ranked through the
        retired ``mp3_vbr`` table (transparent >= 245) while the installed
        CBR copy ranked through ``mp3_cbr`` (excellent at 256) — and the
        rank difference short-circuited before the +-5 kbps tiebreak could
        see that the two numbers were one kbps apart.

        Issue #1144 removed the float truncation that produced the 255, and
        issue #1145 removed the two-table amplifier; H2 then closed the
        no-tolerance rank cliff underneath both. The album must not import.
        """
        result = self._decide(
            candidate_kbps=255, candidate_is_cbr=False,
            installed_kbps=256, installed_is_cbr=True,
        )
        self.assertFalse(result["imported"])
        self.assertEqual(result["stage2_import"], "downgrade")
        self.assertEqual(
            json_dict(result["comparison_basis"])["branch"], "rank_within_tolerance")

    def test_the_collapsed_band_edges_do_not_authorise_a_replace(self):
        """Issue #1145 H2: the three live cliff worlds, rebuilt values.

        Collapsing the MP3 tables put the band edges on 320/256/192/128 —
        the nominal bitrates 817 of the library's 1,101 measured all-MP3
        albums sit exactly on — so a candidate on an edge began outranking an
        installed copy a few kbps below it. All three were ``equivalent`` on
        ``main`` and must stay that way.
        """
        for description, candidate, installed in (
            # Kerrie Biddell, *Only The Beginning* — installed avg 317.
            ("request 5629, transparent edge", 320, 317),
            # Dead Fawn, *Session III* — installed avg 190 (min 188).
            ("request 3182, good edge", 192, 190),
            # No live pair sits exactly here; this is the window's own edge.
            ("excellent edge at the full window", 256, 252),
        ):
            with self.subTest(album=description):
                result = self._decide(
                    candidate_kbps=candidate, installed_kbps=installed)
                self.assertFalse(result["imported"])
                self.assertEqual(result["stage2_import"], "downgrade")
                self.assertEqual(
                    json_dict(result["comparison_basis"])["branch"],
                    "rank_within_tolerance")

    def test_a_real_upgrade_still_imports(self):
        """Control: the guard must not become "never replace anything"."""
        result = self._decide(candidate_kbps=320, installed_kbps=200)
        self.assertTrue(result["imported"])
        self.assertEqual(result["stage2_import"], "import")
        self.assertEqual(json_dict(result["comparison_basis"])["branch"], "rank")

    def test_a_cross_family_pair_one_kbps_apart_still_imports(self):
        """Control: candidate 6963 / request 929 — Opus 127 over AAC 128.

        One kbps apart and two ranks apart, because the two numbers are not
        the same kind of number: 127 kbps of Opus is TRANSPARENT and 128 kbps
        of AAC is GOOD. Widening the within-tolerance window into cross-codec
        comparisons would silently stop this import; that is what this pins.
        """
        result = self._decide(
            candidate_kbps=127, candidate_format="Opus",
            installed_kbps=128, installed_format="AAC",
        )
        self.assertTrue(result["imported"])
        self.assertEqual(result["stage2_import"], "import")
        self.assertEqual(json_dict(result["comparison_basis"])["branch"], "rank")


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

    # Canonical simulator-world -> evidence-row mapping, shared with the
    # generated parity property in tests/test_quality_generated.py.
    _build_candidate = staticmethod(build_parity_candidate_evidence)
    _build_current = staticmethod(build_parity_current_evidence)

    def test_issue_1355_corrupt_and_nested_denylists_the_peer_via_evidence(self):
        """Parity twin of TestLiveBugReproductions' issue #1355 item 1 case,
        run through full_pipeline_decision_from_evidence — the function
        production dispatch actually calls."""
        from lib.quality import (
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_corrupt=True, folder_layout="nested",
        )
        result = full_pipeline_decision_from_evidence(candidate, None)

        self.assertEqual(result["preimport_audio"], "reject_corrupt")
        self.assertIsNone(result["preimport_nested"])
        self.assertFalse(result["imported"])
        self.assertTrue(result["denylisted"])
        self.assertEqual(result["final_status"], "wanted")
        self.assertTrue(result["keep_searching"])
        self.assertEqual(evidence_decision_name(result), "audio_corrupt")

    def test_issue_1355_item_2_unmeasured_early_reject_still_denylists_the_peer(
        self,
    ):
        """Issue #1355 item 2. Before this fix,
        ``lib.quality_evidence.evidence_from_measurement`` fabricated
        ``format="MP3"``/``min_bitrate_kbps=0`` on a candidate the harness
        never measured, purely to satisfy this same function's readiness
        gate. This evidence row is what it persists NOW for that candidate:
        genuinely unmeasured quality, on the exact corrupt-plus-nested world
        item 1's own scenario above uses. There is no flat-simulator twin
        for this scenario — ``full_pipeline_decision`` takes ``min_bitrate``
        as a required ``int`` and has no way to express "never measured"."""
        import msgspec

        from lib.quality import (
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_corrupt=True, folder_layout="nested",
        )
        unmeasured = msgspec.structs.replace(
            candidate,
            measurement=msgspec.structs.replace(
                candidate.measurement,
                format=None,
                min_bitrate_kbps=None,
                avg_bitrate_kbps=None,
                median_bitrate_kbps=None,
            ),
            storage_format=None,
        )

        result = full_pipeline_decision_from_evidence(unmeasured, None)

        self.assertEqual(result["preimport_audio"], "reject_corrupt")
        self.assertIsNone(result["preimport_nested"])
        self.assertFalse(result["imported"])
        self.assertTrue(result["denylisted"])
        self.assertEqual(result["final_status"], "wanted")
        self.assertTrue(result["keep_searching"])
        self.assertEqual(evidence_decision_name(result), "audio_corrupt")

    def test_request_3182_spectral_class_stays_with_the_encode_via_evidence(self):
        """Parity twin of the 6233/342 role-invariance reproduction.

        The importer receives these evidence rows, not simulator kwargs:
        a likely-transcode VBR MP3 whose raw average is 275k but whose
        decision-grade class is 192k, and a genuine 190k VBR MP3. Swapping
        their roles must preserve the no-replacement result while recording
        which encode supplied the effective spectral value.
        """
        from lib.quality import full_pipeline_decision_from_evidence

        classed_candidate = self._build_candidate(
            is_flac=False, min_bitrate=275, avg_bitrate=275, is_cbr=False,
            spectral_grade="likely_transcode", spectral_bitrate=192,
            codec_family="mp3", filetype_band="mp3",
        )
        genuine_current = self._build_current(
            min_bitrate=190, avg_bitrate=190, format="MP3", is_cbr=False,
            spectral_grade="genuine",
        )
        forward = full_pipeline_decision_from_evidence(
            classed_candidate, genuine_current)

        genuine_candidate = self._build_candidate(
            is_flac=False, min_bitrate=190, avg_bitrate=190, is_cbr=False,
            spectral_grade="genuine",
        )
        classed_current = self._build_current(
            min_bitrate=275, avg_bitrate=275, format="MP3", is_cbr=False,
            spectral_grade="likely_transcode", spectral_bitrate=192,
            codec_family="mp3", filetype_band="mp3",
        )
        reverse = full_pipeline_decision_from_evidence(
            genuine_candidate, classed_current)

        for decision in (forward, reverse):
            self.assertEqual(decision["stage2_import"], "downgrade")
            self.assertFalse(decision["imported"])
            self.assertEqual(decision["comparison_basis"]["tolerance_kbps"], 5)
        self.assertEqual(
            forward["comparison_basis"]["branch"],
            "spectral_candidate_bound",
        )
        self.assertEqual(
            reverse["comparison_basis"]["branch"],
            "spectral_existing_bound",
        )

    def test_error_grade_candidate_does_not_bound_a_classed_have(self):
        """An errored candidate is not affirmatively known-clean.

        A raw 224k MP3 whose spectral measurement errored must not use an
        installed VBR transcode's 160k class as its comparison value. That
        would turn the real 224k-vs-320k downgrade into an import; only
        ``genuine`` and ``marginal`` authorize the narrow one-class lane.
        """
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=224, avg_bitrate=224, is_cbr=False,
            spectral_grade="error",
        )
        current = self._build_current(
            min_bitrate=320, avg_bitrate=320, format="MP3", is_cbr=False,
            spectral_grade="likely_transcode", spectral_bitrate=160,
            codec_family="mp3", filetype_band="mp3",
        )

        decision = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(decision["stage2_import"], "downgrade")
        self.assertFalse(decision["imported"])
        self.assertEqual(decision["comparison_basis"]["branch"], "rank")
        self.assertFalse(decision["comparison_basis"]["spectral_clamped"])
        self.assertEqual(
            (decision["comparison_basis"]["new_value_kbps"],
             decision["comparison_basis"]["existing_value_kbps"]),
            (224, 320),
        )

    def test_mountain_goats_flux_provisional_lossless_via_evidence(self):
        """Request 4514 shape, but routed through the production decider."""
        from lib.quality import (
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="suspect",
            spectral_bitrate=160,
            candidate_v0_probe_avg=211,
            candidate_v0_probe_min=198,
        )
        current = self._build_current(
            min_bitrate=320, avg_bitrate=320,
            format="MP3", is_cbr=True,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
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
            candidate_v0_probe_avg=214,
        )
        current = self._build_current(
            min_bitrate=320, avg_bitrate=320,
            format="MP3", is_cbr=True,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target="opus 128",
            ),
        )

        self.assertEqual(r["stage2_import"], "provisional_lossless_upgrade")
        self.assertTrue(r["imported"])

    def test_heretic_pride_downgrade_via_evidence(self):
        """test_heretic_pride second-pass downgrade case via the evidence
        pipeline — MP3 192 vs existing MP3 192.

        Issue #813 Finding 2 pin: this is the production decider (the
        function the real importer calls) — proves the fix through the
        actual entry point, not just the flat-kwargs simulator twin.
        """
        from lib.quality import (
            dispatch_action,
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
        )

        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertEqual(r["denylisted"], dispatch_action("downgrade").denylist)

    def test_mark_denardo_equal_spectral_higher_bitrate_imports_via_evidence(self):
        """Mark DeNardo request 1308 through the production evidence decider.

        Parity twin of
        ``TestLiveBugReproductions.test_mark_denardo_lion_tiger_bear_equal_spectral_higher_bitrate_imports``:
        equal spectral floor (128 == 128) defers past Stage 1, and Stage 2's
        codec-aware tiebreak imports the higher-container copy (MP3 192 over
        MP3 128). The simulator and the evidence pipeline must agree.
        """
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False,
            min_bitrate=192,
            avg_bitrate=192,
            is_cbr=True,
            spectral_grade="suspect",
            spectral_bitrate=128,
        )
        current = self._build_current(
            min_bitrate=128,
            avg_bitrate=128,
            format="MP3",
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["stage1_spectral"], "import")
        self.assertEqual(r["stage2_import"], "import")
        self.assertEqual(r["comparison_basis"]["verdict"], "better")
        self.assertTrue(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_tyler_lamberts_grave_cbr320_transcode_via_evidence(self):
        """Parity twin of the CBR-320 Tyler Lamberts regression pin."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=320, is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=160,
        )
        current = self._build_current(
            min_bitrate=320, spectral_grade="likely_transcode",
            spectral_bitrate=160,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["stage1_spectral"], "import")
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_tyler_lamberts_grave_no_spectral_bitrate_via_evidence(self):
        """Parity twin of Tyler Lamberts' no-cliff reproduction."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=320, is_cbr=True,
            spectral_grade="likely_transcode",
        )
        current = self._build_current(
            min_bitrate=320, spectral_grade="likely_transcode",
            spectral_bitrate=160,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        # No cliff and no bucket on the candidate → no class to compare, so
        # Stage 1 withholds rather than claiming a tie (issue #829 Phase 5
        # PR2b). The load-bearing outcome — the 320 transcode is NOT
        # accepted — is unchanged and still owned by Stage 2.
        self.assertEqual(r["stage1_spectral"], "import_no_exist")
        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_stage_parity_review_f1_unbound_tied_spectral_via_evidence(self):
        """Parity twin of review finding F1's tolerance-boundary pin."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=250, avg_bitrate=250, is_cbr=False,
            spectral_grade="genuine", spectral_bitrate=256,
        )
        current = self._build_current(
            min_bitrate=247, avg_bitrate=247, format="MP3", is_cbr=False,
            spectral_grade="genuine", spectral_bitrate=256,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["comparison_basis"]["verdict"], "equivalent")
        self.assertEqual(r["comparison_basis"]["branch"], "metric_tiebreak")
        self.assertFalse(r["imported"])

    def test_stage_parity_review_f2_asymmetric_cbr_forcing_via_evidence(self):
        """Parity twin of review finding F2's asymmetric-bound pin."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=246, avg_bitrate=246, is_cbr=False,
            spectral_grade="genuine", spectral_bitrate=320,
        )
        current = self._build_current(
            min_bitrate=260, avg_bitrate=260, format="MP3", is_cbr=False,
            spectral_grade="genuine", spectral_bitrate=256,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["comparison_basis"]["verdict"], "worse")
        self.assertFalse(r["imported"])

    def test_wavves_aac_natural_rolloff_via_evidence(self):
        """Parity twin of the Wavves AAC reproduction (issue #829)."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=256, is_cbr=True, avg_bitrate=256,
            spectral_grade="likely_transcode", spectral_bitrate=128,
            native_codec="aac", native_format="AAC",
        )
        current = self._build_current(
            min_bitrate=192, avg_bitrate=192, format="MP3", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=192,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["stage0_spectral_gate"], "skipped_uncalibrated_codec")
        self.assertIsNone(r["stage1_spectral"])
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])

    def test_stage_parity_cross_codec_vorbis_bucket_via_evidence(self):
        """Parity twin of the cross-codec Stage-1 pin (issue #829 PR2c)."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=256, is_cbr=True, avg_bitrate=256,
            spectral_grade="likely_transcode", spectral_bitrate=128,
        )
        current = self._build_current(
            min_bitrate=128, avg_bitrate=128, format="Vorbis", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=192,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["stage1_spectral"], "import_no_exist")
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_stage_parity_aac_have_bucket_via_evidence(self):
        """Parity twin of the AAC-HAVE Stage-1 pin (issue #829 PR2c)."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=256, is_cbr=True, avg_bitrate=256,
            spectral_grade="likely_transcode", spectral_bitrate=128,
        )
        current = self._build_current(
            min_bitrate=112, avg_bitrate=112, format="AAC", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=192,
            cliff_hz=15500, codec_family="aac",
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["stage1_spectral"], "import_no_exist")
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_fall_2007_fake_320_via_evidence(self):
        """Parity twin of the Fall 2007 anti-loop pin (issue #911)."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=320, is_cbr=True, avg_bitrate=320,
            spectral_grade="likely_transcode", spectral_bitrate=128,
            cliff_hz=16500, codec_family="mp3",
        )
        current = self._build_current(
            min_bitrate=160, avg_bitrate=160, format="MP3", is_cbr=True,
            spectral_grade="genuine",
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(
            r["comparison_basis"]["branch"], "spectral_candidate_bound")
        self.assertEqual(r["comparison_basis"]["verdict"], "equivalent")
        self.assertEqual(r["comparison_basis"]["new_value_kbps"], 160)
        self.assertFalse(r["imported"])
        self.assertTrue(r["keep_searching"])

    def test_taboo_vi_fake_flac_192_requires_explicit_probe_via_evidence(self):
        """Parity twin: the production decider rejects the false probe."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="likely_transcode",
        )
        current = self._build_current(
            min_bitrate=128, spectral_grade="likely_transcode",
            spectral_bitrate=96,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                converted_count=10,
                post_conversion_min_bitrate=224,
            ),
        )

        self.assertFalse(r["verified_lossless"])
        self.assertEqual(r["stage2_import"], "suspect_lossless_probe_missing")
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_taboo_vi_with_spectral_bitrate_requires_explicit_probe_via_evidence(self):
        """A captured spectral estimate leaves the source-probe rule intact."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="likely_transcode", spectral_bitrate=192,
        )
        current = self._build_current(
            min_bitrate=128, spectral_grade="likely_transcode",
            spectral_bitrate=96,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                converted_count=10,
                post_conversion_min_bitrate=224,
            ),
        )

        self.assertEqual(r["stage2_import"], "suspect_lossless_probe_missing")
        self.assertIsNone(r["stage3_quality_gate"])
        self.assertFalse(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_deerhunter_identical_transcode_not_upgrade_via_evidence(self):
        """Deerhunter request 6795 through the production evidence decider.

        Parity twin of
        ``TestLiveBugReproductions.test_deerhunter_rhapsody_original_identical_transcode_not_upgrade``.
        The evidence pipeline derives the existing-side spectral-floor override
        itself (``override_bitrate_from_current_evidence``: min(256, 192) = 192),
        so this proves the real wrong-match cleanup path — not just the simulator
        — no longer mints a phantom upgrade for an identical transcode. The
        symmetric-representation gate skips the one-sided override because both
        sides carry a spectral estimate. Issue #813 Finding 1.
        """
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False,
            min_bitrate=256,
            avg_bitrate=256,
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate=192,
        )
        current = self._build_current(
            min_bitrate=256,
            avg_bitrate=256,
            format="MP3",
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate=192,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["stage2_import"], "downgrade")
        self.assertEqual(r["comparison_basis"]["verdict"], "equivalent")
        # True containers compared symmetrically — existing NOT floored to 192.
        self.assertEqual(r["comparison_basis"]["new_value_kbps"], 256)
        self.assertEqual(r["comparison_basis"]["existing_value_kbps"], 256)
        self.assertFalse(r["imported"])
        self.assertTrue(r["keep_searching"])
        # Issue #813 Finding 2: downgrade always denylists in production.
        self.assertTrue(r["denylisted"])

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
            EVIDENCE_SUBJECT_SOURCE,
            AlbumQualityV0Metric,
            classify_full_pipeline_decision,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=128,
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
                subject=EVIDENCE_SUBJECT_SOURCE,
                provenance="measured",
            ),
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
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
            candidate_v0_probe_kind="lossless_source_v0",
            existing_min_bitrate=100,
            existing_avg_bitrate=119,
            existing_format="Opus",
            existing_is_cbr=False,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=128,
            existing_v0_probe_avg=215,
            existing_v0_probe_kind="lossless_source_v0",
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
        )

        self.assertNotEqual(r["stage2_import"], "import")
        self.assertFalse(r["imported"])

    def test_olivia_rodrigo_wav_basis_contract_via_evidence(self):
        """dl 36660 through the production decider: the basis records the
        explicit Opus contract, never the V0 proxy's min or average."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityV0Metric,
            full_pipeline_decision_from_evidence,
        )
        from lib.quality.evidence_types import (
            EVIDENCE_SUBJECT_INSTALLED,
        )

        candidate = self._build_candidate(
            is_flac=True,
            min_bitrate=0,
            is_cbr=False,
            spectral_grade="genuine",
            candidate_v0_probe_avg=255,
            candidate_v0_probe_min=216,
        )
        current = self._build_current(
            min_bitrate=256, avg_bitrate=256,
            format="AAC", is_cbr=False,
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=208,
                avg_bitrate_kbps=250,
                median_bitrate_kbps=251,
                subject=EVIDENCE_SUBJECT_INSTALLED,
            ),
        )

        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target="opus 128",
            ),
        )

        self.assertEqual(r["stage2_import"], "import")
        basis = r["comparison_basis"]
        assert basis is not None
        self.assertEqual(basis["branch"], "cross_family_same_rank")
        self.assertTrue(basis["verified_lossless_bypass"])
        self.assertEqual(basis["new_metric"], "contract")
        self.assertEqual(basis["new_value_kbps"], 128)
        self.assertEqual(basis["existing_metric"], "avg")
        self.assertEqual(basis["existing_value_kbps"], 256)

    def test_dirt_dress_marked_incomplete_imports_complete_candidate_via_evidence(
        self,
    ):
        """Parity twin of the request-1852 reproduction (#1241).

        Same measured world (Dirt Dress — *Theme Songs*, Discogs 4738671,
        download_log 40355), expressed as the rows the production decider
        actually reads. The operator's mark and beets' coverage proof are
        ACTION-TIME facts — they ride
        ``AlbumQualityEvidenceDecisionFacts``, never the evidence rows —
        so the identical evidence decides differently only when the
        request carries the mark.
        """
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=196, avg_bitrate=196, is_cbr=False,
        )
        current = self._build_current(
            min_bitrate=128, avg_bitrate=128, format="AAC", is_cbr=False,
            mb_release_id="mbid-parity-candidate",
        )

        r = full_pipeline_decision_from_evidence(
            candidate,
            current,
            facts=AlbumQualityEvidenceDecisionFacts(
                installed_marked_incomplete=True,
                candidate_covers_declared_program=True,
            ),
        )

        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertTrue(r["installed_incomplete_disregarded"])
        self.assertIsNone(r["comparison_basis"])
        # Below par → the ordinary post-import gate keeps searching (and
        # applies its ordinary denylist policy); see the flat-twin pin.
        self.assertEqual(r["stage3_quality_gate"], "requeue_upgrade")
        self.assertTrue(r["keep_searching"])
        verdict, cleanup_eligible, _reason = classify_full_pipeline_decision(r)
        self.assertEqual(verdict, "would_import")
        self.assertFalse(
            cleanup_eligible,
            "an import-class decision must never authorize folder deletion",
        )
        # Fresh-import equivalence through the evidence twin: identical to
        # the same candidate with NO current row, modulo the audit flag.
        fresh = full_pipeline_decision_from_evidence(candidate, None)
        fresh["installed_incomplete_disregarded"] = True
        self.assertEqual(r, fresh)

    def test_dirt_dress_without_both_conjuncts_still_downgrades_via_evidence(
        self,
    ):
        """Parity twin of the negative pin — either conjunct alone changes
        nothing, byte-for-byte."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=196, avg_bitrate=196, is_cbr=False,
        )
        current = self._build_current(
            min_bitrate=128, avg_bitrate=128, format="AAC", is_cbr=False,
            mb_release_id="mbid-parity-candidate",
        )
        baseline = full_pipeline_decision_from_evidence(candidate, current)
        self.assertEqual(baseline["stage2_import"], "downgrade")
        for marked, covered in ((True, False), (False, True)):
            with self.subTest(marked=marked, covered=covered):
                r = full_pipeline_decision_from_evidence(
                    candidate,
                    current,
                    facts=AlbumQualityEvidenceDecisionFacts(
                        installed_marked_incomplete=marked,
                        candidate_covers_declared_program=covered,
                    ),
                )
                self.assertEqual(r, baseline)
                self.assertFalse(r["installed_incomplete_disregarded"])

    def test_verified_lossless_locked_yields_to_the_mark_via_evidence(self):
        """Parity twin of the proof-lock disarm pin (#1241).

        The evidence twin's own decision-21 early return — which fires
        before the flat twin is ever called — must honour the mark too,
        or the two twins would disagree on every locked world.
        """
        import msgspec

        from lib.quality import (
            VerifiedLosslessProof,
            full_pipeline_decision_from_evidence,
        )

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=196, avg_bitrate=196, is_cbr=False,
        )
        current = self._build_current(
            min_bitrate=128, avg_bitrate=128, format="AAC", is_cbr=False,
            mb_release_id="mbid-parity-candidate",
        )
        assert current is not None
        locked_current = msgspec.structs.replace(
            current,
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="pinned",
                classifier="pinned",
            ),
        )

        r = full_pipeline_decision_from_evidence(
            candidate,
            locked_current,
            facts=AlbumQualityEvidenceDecisionFacts(
                installed_marked_incomplete=True,
                candidate_covers_declared_program=True,
            ),
        )
        self.assertEqual(r["stage2_import"], "import")
        self.assertTrue(r["imported"])
        self.assertTrue(r["installed_incomplete_disregarded"])

        locked = full_pipeline_decision_from_evidence(
            candidate,
            locked_current,
            facts=AlbumQualityEvidenceDecisionFacts(
                candidate_covers_declared_program=True,
            ),
        )
        self.assertEqual(locked["stage2_import"], "verified_lossless_locked")
        self.assertFalse(locked["imported"])
        self.assertFalse(locked["installed_incomplete_disregarded"])

    def test_early_reject_still_records_the_disregard_flag_via_evidence(self):
        """#1257 review F7/M6: the absolute admission floors outrank the
        disregard — a corrupt candidate is rejected even under the mark —
        but the audit flag must still record that the predicate fired,
        matching the flat twin's dict for the same world."""
        from lib.quality import full_pipeline_decision_from_evidence

        corrupt = self._build_candidate(
            is_flac=False, min_bitrate=196, avg_bitrate=196, is_cbr=False,
            audio_corrupt=True,
        )
        current = self._build_current(
            min_bitrate=128, avg_bitrate=128, format="AAC", is_cbr=False,
            mb_release_id="mbid-parity-candidate",
        )
        r = full_pipeline_decision_from_evidence(
            corrupt,
            current,
            facts=AlbumQualityEvidenceDecisionFacts(
                installed_marked_incomplete=True,
                candidate_covers_declared_program=True,
            ),
        )
        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertFalse(r["imported"])
        self.assertTrue(r["installed_incomplete_disregarded"])

        unmarked = full_pipeline_decision_from_evidence(corrupt, current)
        self.assertEqual(unmarked["preimport_audio"], "reject_corrupt")
        self.assertFalse(unmarked["installed_incomplete_disregarded"])


class TestUltrasonicProofGateV3(unittest.TestCase):
    """Proof gate v3 — the ultrasonic deficit leg (issue #829 Phase 5 PR3).

    The album test set for the leg that replaced v2.1's relative
    affirmative-content test. Every scenario runs through BOTH twins: the
    flat-kwargs simulator and ``full_pipeline_decision_from_evidence``,
    the function the importer actually calls.

    Every pin asserts the DECIDED OUTCOME, not a proxy field. In this
    library's default configuration (``verified_lossless_target='opus
    128'``) the flip is terminal-versus-still-searching:

        proof granted   stage3='accept', final_status='imported',
                        keep_searching=False
        proof withheld  stage3='requeue_lossless', final_status='wanted',
                        keep_searching=True

    which is exactly the archivist semantics the plan requires: a denial
    is NOT a rejection. The album still imports; it simply carries no
    proof and stays on the search surface (Phase 5 plan §2, §1.7).

    ``target_final_format`` is deliberately NOT in that list: the stored
    format is config, not proof (issue #829, operator decision
    2026-08-01), so it reads ``opus 128`` on both sides. See
    ``TestLosslessStoredFormatIsProofBlind``.

    The measured worlds below use real launder deficits from the four
    committed calibration arms, not invented numbers.
    """

    #: ROUND-3 / William Basinski, measured ``U=65.16`` on both its
    #: ``t-mp3128-flac`` and ``t-vorbisq5-flac`` launders — a FLAC-container
    #: fraud the album grade alone does not catch. Above the frozen 59.5
    #: threshold; the ultrasonic leg is the only thing between it and a
    #: verified-lossless stamp.
    LAUNDER_DEFICIT_DB = 65.16

    #: A genuine control comfortably below the threshold.
    GENUINE_DEFICIT_DB = 45.0

    _FACTS_TARGET = "opus 128"

    def _evidence_decision(self, **candidate_kwargs):
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )
        candidate = build_parity_candidate_evidence(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            candidate_v0_probe_avg=245,
            candidate_v0_probe_min=245,
            codec_family="lossless",
            **candidate_kwargs,
        )
        return full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target=self._FACTS_TARGET,
            ),
        )

    def _simulator_decision(self, *, spectral_grade="genuine", **context_kwargs):
        from lib.quality import SpectralCodecContext
        return full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade=spectral_grade,
            converted_count=1,
            post_conversion_min_bitrate=245,
            post_conversion_is_cbr=False,
            candidate_v0_probe_avg=245,
            candidate_v0_probe_min=245,
            candidate_v0_probe_kind="lossless_source_v0",
            verified_lossless_target=self._FACTS_TARGET,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                **context_kwargs,
            ),
        )

    def _assert_proof_granted(self, r, where):
        self.assertTrue(r["verified_lossless"], where)
        self.assertEqual(r["stage3_quality_gate"], "accept", where)
        self.assertEqual(r["final_status"], "imported", where)
        self.assertFalse(r["keep_searching"], where)
        self.assertEqual(r["target_final_format"], self._FACTS_TARGET, where)

    def _assert_proof_withheld(self, r, where):
        self.assertFalse(r["verified_lossless"], where)
        # An unproven lossless source is owned by the provisional anchor
        # lane (issue #990, decision record on the issue): these worlds
        # have no anchor to clear, so the album imports provisionally and
        # the lane's own search action keeps it searchable. Stage 3 never
        # runs for a lane-decided import.
        self.assertEqual(
            r["stage2_import"], "provisional_lossless_upgrade", where)
        self.assertIsNone(r["stage3_quality_gate"], where)
        self.assertEqual(r["final_status"], "wanted", where)
        self.assertTrue(r["keep_searching"], where)
        # The stored format does NOT move with the proof (issue #829): the
        # same assertion as the granted case, on purpose.
        self.assertEqual(r["target_final_format"], self._FACTS_TARGET, where)
        # The load-bearing half of the archivist semantics: withholding a
        # proof is NOT rejecting the album. It imported.
        self.assertTrue(r["imported"], where)

    # -- the leg denies ----------------------------------------------------

    def test_a_launder_deficit_withholds_the_proof_on_both_twins(self):
        """The flip the leg exists for. Identical album, identical grade;
        only ``ultrasonic_deficit_db`` moves, and the pipeline goes from
        terminal-imported to imported-and-still-searching."""
        self._assert_proof_granted(
            self._evidence_decision(
                ultrasonic_deficit_db=self.GENUINE_DEFICIT_DB,
            ),
            "evidence twin, genuine deficit",
        )
        self._assert_proof_withheld(
            self._evidence_decision(
                ultrasonic_deficit_db=self.LAUNDER_DEFICIT_DB,
            ),
            "evidence twin, launder deficit",
        )
        self._assert_proof_granted(
            self._simulator_decision(
                ultrasonic_deficit_db=self.GENUINE_DEFICIT_DB,
                spectral_decode_path="sox_native",
            ),
            "simulator twin, genuine deficit",
        )
        self._assert_proof_withheld(
            self._simulator_decision(
                ultrasonic_deficit_db=self.LAUNDER_DEFICIT_DB,
                spectral_decode_path="sox_native",
            ),
            "simulator twin, launder deficit",
        )

    def test_the_frozen_threshold_is_the_boundary(self):
        """Both sides of 59.5, on the production decider. The constant is
        READ, not restated — a pin spelling 59.5 would pass a module that
        had drifted to any other value."""
        from lib.quality import ULTRASONIC_PROOF_DENY_DEFICIT_DB
        self._assert_proof_granted(
            self._evidence_decision(
                ultrasonic_deficit_db=ULTRASONIC_PROOF_DENY_DEFICIT_DB - 0.01,
            ),
            "one hundredth of a dB below the threshold",
        )
        self._assert_proof_withheld(
            self._evidence_decision(
                ultrasonic_deficit_db=ULTRASONIC_PROOF_DENY_DEFICIT_DB,
            ),
            "exactly at the threshold — inclusive",
        )

    def test_the_v0_override_cannot_outrank_a_denial(self):
        """The Bill Hicks shape (``suspect`` grade rescued by a
        lossless_source_v0 probe at avg 241 / min 219) does NOT rescue a
        launder deficit.

        Measured basis: the V0/Opus re-encode probe axis
        (``docs/research/calibration-data/probe_pair.tsv.gz``, 5,670 files)
        separates only mp3-128 — the one class the cliff leg already
        catches. Letting the override win here would reopen the exact hole
        the leg closes, to rescue albums whose only cost is staying
        searchable."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        def decide(deficit):
            candidate = build_parity_candidate_evidence(
                is_flac=True, min_bitrate=0, is_cbr=False,
                spectral_grade="suspect",
                candidate_v0_probe_avg=241,
                candidate_v0_probe_min=219,
                codec_family="lossless",
                ultrasonic_deficit_db=deficit,
            )
            return full_pipeline_decision_from_evidence(
                candidate, None,
                facts=AlbumQualityEvidenceDecisionFacts(
                    verified_lossless_target=self._FACTS_TARGET,
                ),
            )

        rescued = decide(self.GENUINE_DEFICIT_DB)
        self.assertTrue(
            rescued["verified_lossless"],
            "the V0-avg trust override must still rescue HF-poor lossless "
            "when the ultrasonic leg has no objection",
        )
        denied = decide(self.LAUNDER_DEFICIT_DB)
        self.assertFalse(
            denied["verified_lossless"],
            "a denied ultrasonic leg is a hard veto ahead of the V0 override",
        )

    # -- a denial withholds the proof and NOTHING else ---------------------

    #: The installed side of the worlds below: a provisional-cohort album.
    #: It was imported from a lossless source we ground down, so the linked
    #: ``lossless_source_v0`` probe (avg 240) is its only comparable
    #: anchor, and it carries no verified-lossless proof. A candidate probe
    #: at avg 241 does NOT clear that anchor by the rank tolerance, so the
    #: provisional lane answers ``suspect_lossless_downgrade`` — a
    #: confident reject that also denylists the offering peer.
    _HAVE_PROVISIONAL_V0_AVG = 240

    def _denial_pair_evidence(self, deficit, *, have_min, have_format,
                              have_is_cbr):
        from lib.quality import (
            EVIDENCE_SUBJECT_SOURCE,
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityV0Metric,
            full_pipeline_decision_from_evidence,
        )
        candidate = build_parity_candidate_evidence(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="suspect",
            candidate_v0_probe_avg=241,
            candidate_v0_probe_min=219,
            codec_family="lossless",
            ultrasonic_deficit_db=deficit,
        )
        current = build_parity_current_evidence(
            min_bitrate=have_min, avg_bitrate=have_min, format=have_format,
            is_cbr=have_is_cbr,
            v0_metric=AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_SOURCE,
                min_bitrate_kbps=219,
                avg_bitrate_kbps=self._HAVE_PROVISIONAL_V0_AVG,
            ),
        )
        return full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target=self._FACTS_TARGET,
            ),
        )

    def _denial_pair_simulator(self, deficit, *, have_min, have_format,
                               have_is_cbr):
        """The convert branch: the lossless source is ground to V0."""
        from lib.quality import SpectralCodecContext
        return full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="suspect",
            converted_count=1,
            post_conversion_min_bitrate=219,
            post_conversion_is_cbr=False,
            candidate_v0_probe_avg=241,
            candidate_v0_probe_min=219,
            candidate_v0_probe_kind="lossless_source_v0",
            existing_min_bitrate=have_min,
            existing_avg_bitrate=have_min,
            existing_format=have_format.lower(),
            existing_is_cbr=have_is_cbr,
            existing_v0_probe_avg=self._HAVE_PROVISIONAL_V0_AVG,
            existing_v0_probe_kind="lossless_source_v0",
            verified_lossless_target=self._FACTS_TARGET,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                spectral_decode_path="sox_native",
                ultrasonic_deficit_db=deficit,
            ),
        )

    def _denial_pair_flac_keep(self, deficit, *, have_min, have_format,
                               have_is_cbr):
        """The kept-on-disk branch of the same world (``target_format`` is
        flac, so nothing is converted)."""
        from lib.quality import SpectralCodecContext
        return full_pipeline_decision(
            is_flac=True, min_bitrate=900, is_cbr=False,
            spectral_grade="suspect",
            converted_count=0,
            target_format="flac",
            candidate_v0_probe_avg=241,
            candidate_v0_probe_min=219,
            candidate_v0_probe_kind="lossless_source_v0",
            existing_min_bitrate=have_min,
            existing_avg_bitrate=have_min,
            existing_format=have_format.lower(),
            existing_is_cbr=have_is_cbr,
            existing_v0_probe_avg=self._HAVE_PROVISIONAL_V0_AVG,
            existing_v0_probe_kind="lossless_source_v0",
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                spectral_decode_path="sox_native",
                ultrasonic_deficit_db=deficit,
            ),
        )

    _DENIAL_TWINS = (
        ("evidence twin", "_denial_pair_evidence"),
        ("simulator twin, convert branch", "_denial_pair_simulator"),
        ("simulator twin, flac-keep branch", "_denial_pair_flac_keep"),
    )

    def test_a_denial_never_costs_the_album_its_import(self):
        """A denial withholds the PROOF. It must not take the ALBUM.

        The world is the one the V0-avg trust override exists to rescue —
        HF-poor lossless graded ``suspect`` with a ``lossless_source_v0``
        probe at avg 241 / min 219 — against an INSTALLED side the earlier
        pins did not have: a provisional-cohort MP3 128 whose own
        comparable probe sits at avg 240. That probe is what arms the
        provisional lane: 241 does not clear 240 by the rank tolerance, so
        the lane answers ``suspect_lossless_downgrade``, a confident reject
        that denylists the peer as well.

        A denied album that reached that lane therefore lost its import
        outright — on exactly the HF-poor genuine-lossless cohort the leg
        promised never to touch (Phase 5 plan §2, §1.7: withholding a proof
        never rejects, denylists or accuses). Deciding the lane is the V0
        PROBE's job and the probe's answer does not change; the leg's whole
        effect is the proof and the search surface."""
        for label, method in self._DENIAL_TWINS:
            with self.subTest(twin=label):
                def decide(deficit, method=method):
                    return getattr(self, method)(
                        deficit, have_min=128, have_format="MP3",
                        have_is_cbr=True,
                    )
                rescued = decide(self.GENUINE_DEFICIT_DB)
                denied = decide(self.LAUNDER_DEFICIT_DB)
                self.assertTrue(
                    rescued["imported"],
                    f"{label}: the V0-rescued world imports",
                )
                self.assertTrue(
                    rescued["verified_lossless"],
                    f"{label}: the V0-avg trust override still rescues an "
                    "HF-poor lossless the leg has no objection to",
                )
                self.assertFalse(rescued["keep_searching"], label)
                self.assertTrue(
                    denied["imported"],
                    f"{label}: withholding a proof is NOT taking the album",
                )
                self.assertFalse(
                    denied["verified_lossless"],
                    f"{label}: the denial's first effect — no proof",
                )
                self.assertTrue(
                    denied["keep_searching"],
                    f"{label}: and its second — the album stays on the "
                    "search surface",
                )

    def test_a_denial_never_reroutes_the_album_into_the_provisional_lane(self):
        """The same world against a HAVE the unproved candidate cannot
        beat: an Opus 245 with the same provisional probe.

        Here the denial legitimately costs the import — an album with no
        proof is compared on what it measures, and a V0 grind does not beat
        an installed 245 — but it must lose that comparison in the MEASURED
        lane, with a comparison basis the operator can read, and never by
        being re-routed into the provisional lane's confident reject. The
        lane is the V0 probe's answer; the leg does not get a vote in it."""
        for label, method in self._DENIAL_TWINS:
            with self.subTest(twin=label):
                denied = getattr(self, method)(
                    self.LAUNDER_DEFICIT_DB, have_min=245,
                    have_format="Opus", have_is_cbr=False,
                )
                self.assertNotIn(
                    denied["stage2_import"],
                    PROVISIONAL_LANE_DECISIONS,
                    f"{label}: a denial re-routed the album into the "
                    "provisional lossless lane",
                )

    # -- the leg withholds: the decode-path scope --------------------------

    def test_an_ffmpeg_path_deficit_is_never_gated(self):
        """Phase 5 plan §1.5c. The SAME BITS measure 50.26 dB through
        ``_ffmpeg_to_wav`` at 48kHz and 47.17 dB sox-native at 44.1kHz — a
        +3.09 dB skew, larger than the gate's whole 2.05 dB margin
        (isolated on request 8923's ALAC control). A value on that scale
        is not comparable to a threshold frozen on the other one, so the
        leg refuses to gate it in EITHER direction."""
        r = self._evidence_decision(
            ultrasonic_deficit_db=self.LAUNDER_DEFICIT_DB,
            lossless_container="m4a", lossless_codec="alac",
        )
        self._assert_proof_granted(r, "ALAC source, ffmpeg decode path")
        self._assert_proof_granted(
            self._simulator_decision(
                ultrasonic_deficit_db=self.LAUNDER_DEFICIT_DB,
                spectral_decode_path="ffmpeg_resampled",
            ),
            "simulator twin, ffmpeg decode path",
        )

    def test_an_unknown_decode_path_is_never_gated(self):
        """No containers, no answer. Fail closed means the leg asserts
        nothing — not that it denies."""
        self._assert_proof_granted(
            self._simulator_decision(
                ultrasonic_deficit_db=self.LAUNDER_DEFICIT_DB,
                spectral_decode_path=None,
            ),
            "simulator twin, unresolved decode path",
        )

    # -- the leg withholds: the NULL tristate ------------------------------

    def test_the_three_null_states_never_demote_and_stay_distinct(self):
        """PR3 hard constraint 1. ``ultrasonic_deficit_db IS NULL`` is
        three different facts about the world, and every one of them must
        leave the pre-v3 outcome untouched — 6,273 proof rows can never be
        re-measured at any price, and demoting them would be exactly the
        retroactive demotion the plan forbids."""
        from lib.quality import ultrasonic_proof_leg

        # (a) R19 preserved source: a converted copy wearing its source's
        #     pre-capture spectral. The source was converted away.
        self._assert_proof_granted(
            self._evidence_decision(
                ultrasonic_deficit_db=None,
                spectral_measurement_version=None,
                was_converted_from="flac",
            ),
            "(a) preserved source spectral",
        )
        # (b) legacy row, measured before PR1's capture shipped.
        self._assert_proof_granted(
            self._evidence_decision(
                ultrasonic_deficit_db=None,
                spectral_measurement_version=None,
            ),
            "(b) legacy measurement",
        )
        # (c) the capture code ran and honestly reported no value — the
        #     20.5-22kHz bands were outside the file's Nyquist.
        self._assert_proof_granted(
            self._evidence_decision(
                ultrasonic_deficit_db=None,
                spectral_measurement_version=2,
                cliff_hz=19500,
            ),
            "(c) not measured",
        )
        # ...and the decision path tells them apart, which is what makes
        # them three states rather than one.
        reasons = {
            ultrasonic_proof_leg(
                deficit_db=None, spectral_measurement_version=None,
                decode_path="sox_native", preserved_source_spectral=True,
            ).reason,
            ultrasonic_proof_leg(
                deficit_db=None, spectral_measurement_version=None,
                decode_path="sox_native", preserved_source_spectral=False,
            ).reason,
            ultrasonic_proof_leg(
                deficit_db=None, spectral_measurement_version=2,
                decode_path="sox_native", preserved_source_spectral=False,
            ).reason,
        }
        self.assertEqual(
            reasons,
            {"preserved_source_spectral", "legacy_measurement",
             "not_measured"},
        )

    def test_a_carried_flac_source_deficit_does_adjudicate(self):
        """Carried values are the COMMON case — 15,399 of 15,547 live
        proofs carry ``spectral_provenance='carried'`` — and a carried
        deficit describes exactly the lossless SOURCE the proof is about.
        It must be gated, not skipped, whenever its decode path is
        comparable."""
        self._assert_proof_withheld(
            self._evidence_decision(
                ultrasonic_deficit_db=self.LAUNDER_DEFICIT_DB,
                spectral_measurement_version=2,
                was_converted_from="flac",
            ),
            "carried FLAC-source deficit above the threshold",
        )
        self._assert_proof_granted(
            self._evidence_decision(
                ultrasonic_deficit_db=self.LAUNDER_DEFICIT_DB,
                spectral_measurement_version=2,
                was_converted_from="alac",
            ),
            "carried ALAC-source deficit — different instrument, no gate",
        )


class TestProvisionalAnchorOwnsTheUnprovenCohort(unittest.TestCase):
    """Issue #990 — request 2066 (Sound Dimension, *Jamaica Soul Shake,
    Vol. 1*, download_log 39207): the provisional-lossless anchor lane owns
    EVERY unproven lossless source, not only the suspect-graded ones.

    The live loop: a genuine-graded FLAC whose verified-lossless proof the
    ultrasonic leg denies (deficit 71.29 dB vs the frozen 59.5) bypassed
    the anchor lane (entry was grade-keyed) and fell to the measured
    compare, where the incoming "opus 128" contract ranks transparent
    while the on-disk conversion measures 110k avg — excellent. An equal
    transcode-lineage source therefore "upgraded" the copy it was equal
    to, 95 downloads and counting. The anchor (existing
    ``lossless_source_v0`` avg 177 vs candidate 175, tolerance 5) is the
    evidence that kills it.

    Decision record for the lane-entry rekey and the V5/L5 scope
    amendment it required:
    https://github.com/abl030/cratedigger/issues/990#issuecomment-5158156922

    Every pin asserts the decided outcome through BOTH twins.
    """

    #: dl 39207's album-level ultrasonic deficit — above the frozen 59.5.
    DENIED_DEFICIT_DB = 71.29
    #: A genuine control comfortably below the threshold.
    PASSING_DEFICIT_DB = 45.0

    _FACTS_TARGET = "opus 128"

    def _evidence_decision(
        self,
        *,
        ultrasonic_deficit_db,
        candidate_v0_probe_avg=175,
        candidate_v0_probe_min=148,
        anchor_avg=177,
    ):
        from lib.quality import (
            EVIDENCE_SUBJECT_SOURCE,
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityV0Metric,
            full_pipeline_decision_from_evidence,
        )
        candidate = build_parity_candidate_evidence(
            is_flac=True, min_bitrate=354, is_cbr=False,
            spectral_grade="genuine",
            spectral_bitrate=128,
            cliff_hz=16500,
            codec_family="lossless",
            ultrasonic_deficit_db=ultrasonic_deficit_db,
            spectral_measurement_version=2,
            candidate_v0_probe_avg=candidate_v0_probe_avg,
            candidate_v0_probe_min=candidate_v0_probe_min,
        )
        current = build_parity_current_evidence(
            min_bitrate=95, avg_bitrate=110,
            format="OPUS", is_cbr=False,
            spectral_grade="genuine",
            v0_metric=AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_SOURCE,
                avg_bitrate_kbps=anchor_avg,
                min_bitrate_kbps=150,
            ) if anchor_avg is not None else None,
        )
        return full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target=self._FACTS_TARGET,
            ),
        )

    def _simulator_decision(
        self,
        *,
        ultrasonic_deficit_db,
        candidate_v0_probe_avg=175,
        candidate_v0_probe_min=148,
        anchor_avg=177,
    ):
        from lib.quality import SpectralCodecContext
        return full_pipeline_decision(
            is_flac=True, min_bitrate=354, is_cbr=False,
            spectral_grade="genuine", spectral_bitrate=128,
            existing_min_bitrate=95, existing_avg_bitrate=110,
            existing_format="OPUS", existing_spectral_grade="genuine",
            converted_count=16,
            post_conversion_min_bitrate=93, post_conversion_is_cbr=False,
            candidate_v0_probe_avg=candidate_v0_probe_avg,
            candidate_v0_probe_min=candidate_v0_probe_min,
            candidate_v0_probe_kind="lossless_source_v0",
            existing_v0_probe_avg=anchor_avg,
            existing_v0_probe_kind=(
                "lossless_source_v0" if anchor_avg is not None else None
            ),
            verified_lossless_target=self._FACTS_TARGET,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                cliff_hz=16500,
                ultrasonic_deficit_db=ultrasonic_deficit_db,
                spectral_decode_path="sox_native",
            ),
        )

    def _both_twins(self, **kwargs):
        return (
            (self._evidence_decision(**kwargs), "evidence twin"),
            (self._simulator_decision(**kwargs), "simulator twin"),
        )

    def test_request_2066_equal_copy_is_anchored_out_not_upgraded(self):
        """The churn-killer. Candidate probe 175 vs anchor 177 is within
        the 5 kbps tolerance: NOT better than the copy on disk, whatever
        the contract-vs-measured ranks say. Confident reject, still
        searching."""
        for r, where in self._both_twins(
            ultrasonic_deficit_db=self.DENIED_DEFICIT_DB,
        ):
            self.assertEqual(
                r["stage2_import"], "suspect_lossless_downgrade", where)
            self.assertFalse(r["imported"], where)
            self.assertEqual(r["final_status"], "wanted", where)
            self.assertTrue(r["keep_searching"], where)
            self.assertFalse(r["verified_lossless"], where)

    def test_a_passing_leg_still_mints_the_proof_and_terminates(self):
        """Identical album, deficit below the threshold: the proof is
        minted, the anchor lane never engages, and the request goes
        terminal. Proves the lane cannot block a PROVEN import."""
        for r, where in self._both_twins(
            ultrasonic_deficit_db=self.PASSING_DEFICIT_DB,
        ):
            self.assertTrue(r["verified_lossless"], where)
            self.assertTrue(r["imported"], where)
            self.assertEqual(r["final_status"], "imported", where)
            self.assertFalse(r["keep_searching"], where)

    def test_a_clearly_better_probe_still_imports_provisionally(self):
        """Denied proof but candidate probe 245 vs anchor 177 — well past
        the tolerance. A real upgrade lands, provisionally, and the
        search continues."""
        for r, where in self._both_twins(
            ultrasonic_deficit_db=self.DENIED_DEFICIT_DB,
            candidate_v0_probe_avg=245, candidate_v0_probe_min=213,
        ):
            self.assertEqual(
                r["stage2_import"], "provisional_lossless_upgrade", where)
            self.assertTrue(r["imported"], where)
            self.assertEqual(r["final_status"], "wanted", where)
            self.assertTrue(r["keep_searching"], where)
            self.assertFalse(r["verified_lossless"], where)

    def test_first_unproven_import_with_no_anchor_still_lands(self):
        """Denied proof, existing copy carries no lossless-source anchor:
        the provisional first-import behavior is unchanged — import,
        stay searching."""
        for r, where in self._both_twins(
            ultrasonic_deficit_db=self.DENIED_DEFICIT_DB,
            anchor_avg=None,
        ):
            self.assertEqual(
                r["stage2_import"], "provisional_lossless_upgrade", where)
            self.assertTrue(r["imported"], where)
            self.assertEqual(r["final_status"], "wanted", where)
            self.assertTrue(r["keep_searching"], where)

    def test_probe_less_converting_candidate_cannot_fabricate_anchor_evidence(self):
        """An explicit candidate V0 probe is the only comparable source fact.

        The configured target's post-conversion minimum describes the output
        projection, not a ``lossless_source_v0`` average. With the recorded
        current anchor present, an otherwise unproven candidate that carries
        no explicit probe must therefore reject as probe-missing, rather than
        compare a fabricated average against the anchor.
        """
        for r, where in self._both_twins(
            ultrasonic_deficit_db=self.DENIED_DEFICIT_DB,
            candidate_v0_probe_avg=None,
            candidate_v0_probe_min=None,
        ):
            self.assertEqual(
                r["stage2_import"], "suspect_lossless_probe_missing", where)
            self.assertFalse(r["imported"], where)
            self.assertTrue(r["denylisted"], where)

    def test_partial_source_probe_never_borrows_the_target_minimum(self):
        """A source avg without a source min stays provisional at every target.

        The 241kbps source average is comparable in the provisional lane, but
        it is not strong enough for the V0 trust override without an explicitly
        measured source minimum. A 199→200kbps target projection must not
        complete that missing source fact and route the album into measured
        comparison.
        """
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        candidate = build_parity_candidate_evidence(
            is_flac=True, min_bitrate=354, is_cbr=False,
            spectral_grade="suspect",
            candidate_v0_probe_avg=241,
            candidate_v0_probe_min=None,
        )
        for post_conversion_min in (199, 200):
            evidence = full_pipeline_decision_from_evidence(
                candidate,
                None,
                facts=AlbumQualityEvidenceDecisionFacts(
                    converted_count=12,
                    post_conversion_min_bitrate=post_conversion_min,
                    verified_lossless_target="opus 128",
                ),
            )
            simulator = full_pipeline_decision(
                is_flac=True, min_bitrate=354, is_cbr=False,
                spectral_grade="suspect", converted_count=12,
                post_conversion_min_bitrate=post_conversion_min,
                post_conversion_is_cbr=False,
                candidate_v0_probe_avg=241,
                candidate_v0_probe_min=None,
                candidate_v0_probe_kind="lossless_source_v0",
                verified_lossless_target="opus 128",
            )
            for result, where in (
                (evidence, "evidence twin"),
                (simulator, "simulator twin"),
            ):
                self.assertEqual(
                    result["stage2_import"],
                    "provisional_lossless_upgrade",
                    f"{where}, target min {post_conversion_min}",
                )
                self.assertTrue(result["imported"], where)
                self.assertFalse(result["verified_lossless"], where)

    def test_non_source_probe_kind_never_becomes_source_evidence(self):
        """Target bitrate cannot make research V0 metrics policy-comparable."""
        import msgspec

        from lib.quality import (
            EVIDENCE_SUBJECT_INSTALLED,
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityV0Metric,
            full_pipeline_decision_from_evidence,
        )

        candidate = msgspec.structs.replace(
            build_parity_candidate_evidence(
                is_flac=True, min_bitrate=354, is_cbr=False,
                spectral_grade="suspect",
            ),
            v0_metric=AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_INSTALLED,
                avg_bitrate_kbps=241,
                min_bitrate_kbps=None,
            ),
        )
        for post_conversion_min in (199, 200):
            evidence = full_pipeline_decision_from_evidence(
                candidate,
                None,
                facts=AlbumQualityEvidenceDecisionFacts(
                    converted_count=12,
                    post_conversion_min_bitrate=post_conversion_min,
                    verified_lossless_target="opus 128",
                ),
            )
            simulator = full_pipeline_decision(
                is_flac=True, min_bitrate=354, is_cbr=False,
                spectral_grade="suspect", converted_count=12,
                post_conversion_min_bitrate=post_conversion_min,
                post_conversion_is_cbr=False,
                candidate_v0_probe_avg=241,
                candidate_v0_probe_min=None,
                candidate_v0_probe_kind="native_lossy_research_v0",
                verified_lossless_target="opus 128",
            )
            for result, where in (
                (evidence, "evidence twin"),
                (simulator, "simulator twin"),
            ):
                self.assertEqual(
                    result["stage2_import"],
                    "suspect_lossless_probe_missing",
                    f"{where}, target min {post_conversion_min}",
                )
                self.assertFalse(result["imported"], where)

    def test_a_candidate_carrying_a_proof_is_never_owned_by_the_lane(self):
        """Existing stamps remain proofs under the old model (issue #829's
        forward-only rule): a candidate row that already CARRIES a
        verified-lossless proof is not unproven, however its legs would
        adjudicate today, and the anchor lane never sees it. Found by the
        as-persisted live-corpus differential — 40 pre-PR3-proof rows
        would otherwise have been re-routed."""
        import msgspec

        from lib.quality import (
            EVIDENCE_SUBJECT_SOURCE,
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityV0Metric,
            SpectralCodecContext,
            full_pipeline_decision_from_evidence,
            mint_verified_lossless_proof,
        )
        proof = mint_verified_lossless_proof(
            True,
            was_converted_from="flac",
            detected_source_format="flac",
            spectral_grade="genuine",
        )
        self.assertIsNotNone(proof)
        candidate = msgspec.structs.replace(
            build_parity_candidate_evidence(
                is_flac=True, min_bitrate=354, is_cbr=False,
                spectral_grade="genuine",
                spectral_bitrate=128,
                cliff_hz=16500,
                codec_family="lossless",
                ultrasonic_deficit_db=self.DENIED_DEFICIT_DB,
                spectral_measurement_version=2,
                candidate_v0_probe_avg=175,
                candidate_v0_probe_min=148,
            ),
            verified_lossless_proof=proof,
        )
        current = build_parity_current_evidence(
            min_bitrate=95, avg_bitrate=110,
            format="OPUS", is_cbr=False,
            spectral_grade="genuine",
            v0_metric=AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_SOURCE,
                avg_bitrate_kbps=177,
                min_bitrate_kbps=150,
            ),
        )
        r = full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target=self._FACTS_TARGET,
            ),
        )
        self.assertNotIn(
            r["stage2_import"], PROVISIONAL_LANE_DECISIONS,
            "a proof-bearing candidate was re-routed into the lane")
        self.assertTrue(r["imported"])
        self.assertTrue(r["verified_lossless"])

        sim = full_pipeline_decision(
            is_flac=True, min_bitrate=354, is_cbr=False,
            spectral_grade="genuine", spectral_bitrate=128,
            existing_min_bitrate=95, existing_avg_bitrate=110,
            existing_format="OPUS", existing_spectral_grade="genuine",
            converted_count=16,
            post_conversion_min_bitrate=93, post_conversion_is_cbr=False,
            candidate_v0_probe_avg=175, candidate_v0_probe_min=148,
            candidate_v0_probe_kind="lossless_source_v0",
            existing_v0_probe_avg=177,
            existing_v0_probe_kind="lossless_source_v0",
            candidate_verified_lossless_proof=True,
            verified_lossless_target=self._FACTS_TARGET,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                cliff_hz=16500,
                ultrasonic_deficit_db=self.DENIED_DEFICIT_DB,
                spectral_decode_path="sox_native",
            ),
        )
        self.assertNotIn(sim["stage2_import"], PROVISIONAL_LANE_DECISIONS)
        self.assertTrue(sim["imported"])
        self.assertTrue(sim["verified_lossless"])


#: Measured genuine per-track lattice contrasts — the 17 genuine
#: ALBUM-MAX z values from
#: ``docs/research/calibration-data/derrien-refinement/q3d_out.txt``. The
#: highest real genuine album ever measured tops out at 6.91, which is why
#: the leg's operating point sits at 12.
_GENUINE_Z_VALUES = (4.58, 4.80, 4.97, 5.17, 5.28, 5.54, 6.91)

#: Measured Apple CVBR-256 launder contrasts (``q2_out.txt``: LAU median
#: 28.598, max 31.134 at ``mode=high``).
_LAUNDER_Z_MEDIAN = 28.598

#: qaac/CoreAudio primes 2112 samples, ``2112 mod 1024 = 64``, so its
#: lattice lands at ``1024 - 64``. Used here only to BUILD a realistic
#: launder world — the leg itself never compares an offset to a constant.
_APPLE_MODAL_OFFSET = 960


class TestAacLatticeProofGate(unittest.TestCase):
    """Proof gate v4 — the AAC frame-lattice leg (issue #829 PR-B).

    The album test set for the leg that closes the Apple/CoreAudio blind
    spot v3 explicitly names. Every scenario runs through BOTH twins: the
    flat-kwargs simulator and ``full_pipeline_decision_from_evidence``,
    the function the importer actually calls.

    Every pin asserts the DECIDED OUTCOME, not a proxy field. In this
    library's default configuration (``verified_lossless_target='opus
    128'``) the flip is terminal-versus-still-searching:

        proof granted   stage3='accept', final_status='imported',
                        keep_searching=False
        proof withheld  stage3='requeue_lossless', final_status='wanted',
                        keep_searching=True

    A denial is NOT a rejection: the album still imports, carries no
    proof, and stays on the search surface (Phase 5 plan §2, §1.7). It
    does not change the stored format either — that is config, not proof
    (issue #829; see ``TestLosslessStoredFormatIsProofBlind``).

    Every capture below is built by ``AacLatticeCapture.from_tracks`` from
    per-track ``(offset, z)`` rows, using measured values from the
    committed calibration arms — never a hand-written album statistic.
    """

    _FACTS_TARGET = "opus 128"

    # -- the measured worlds ----------------------------------------------

    @staticmethod
    def _apple_launder_capture():
        """The Apple/CoreAudio shape: five of six tracks on one offset.

        ``qaac-cvbr256`` puts 97.5% of tracks on offset 960 and hits
        ``k >= 4`` on 17/17 albums (derrien-refinement README § coverage).
        """
        return make_aac_lattice_capture([
            (_APPLE_MODAL_OFFSET, 28.60),
            (_APPLE_MODAL_OFFSET, 29.11),
            (_APPLE_MODAL_OFFSET, 30.02),
            (_APPLE_MODAL_OFFSET, 28.35),
            (_APPLE_MODAL_OFFSET, 31.13),
            (512, 27.44),
        ])

    @staticmethod
    def _offset_concentration_only_capture():
        """Concentration WITHOUT an extreme sweep contrast.

        The ``aacffm-*`` shape, where the offsets still coincide but the
        contrast stays inside the genuine range. Isolates the
        parameter-free rule as the sole cause of the denial.
        """
        return make_aac_lattice_capture([
            (0, 5.11), (0, 4.92), (0, 6.30), (0, 5.77),
            (311, 4.61), (742, 5.02),
        ])

    @staticmethod
    def _z_exceeded_only_capture():
        """A scattered-offset album with one spiking sweep.

        ``k`` never reaches 4, so only the contrast rule can deny — and it
        must, on one track, which is why a denial is not gated on having
        four scored tracks.
        """
        return make_aac_lattice_capture([
            (12, 4.80), (455, 5.28), (901, _LAUNDER_Z_MEDIAN),
            (77, 4.97), (630, 5.54),
        ])

    @staticmethod
    def _genuine_capture():
        """A genuine album: uniform offsets, contrasts inside the measured
        genuine range (album maxima 4.58-6.91 over the 17-album arm)."""
        return make_aac_lattice_capture(list(zip(
            (13, 205, 418, 611, 803, 1001), _GENUINE_Z_VALUES, strict=False,
        )))

    @staticmethod
    def _thin_capture():
        """Three scored tracks and three detector errors — the 96 kHz /
        undecodable shape. The concentration rule could not have fired
        whatever the audio was, so a clean result means nothing."""
        return make_aac_lattice_capture([
            (13, 4.58), (205, 4.80), (418, 4.97),
            (None, None), (None, None), (None, None),
        ])

    # -- twins -------------------------------------------------------------

    def _evidence_decision(self, capture, **candidate_kwargs):
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )
        candidate = build_parity_candidate_evidence(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            candidate_v0_probe_avg=245,
            candidate_v0_probe_min=245,
            codec_family="lossless",
            aac_lattice=capture,
            **candidate_kwargs,
        )
        return full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target=self._FACTS_TARGET,
            ),
        )

    def _simulator_decision(self, capture, *, spectral_grade="genuine"):
        from lib.quality import SpectralCodecContext
        return full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade=spectral_grade,
            converted_count=1,
            post_conversion_min_bitrate=245,
            post_conversion_is_cbr=False,
            candidate_v0_probe_avg=245,
            candidate_v0_probe_min=245,
            candidate_v0_probe_kind="lossless_source_v0",
            verified_lossless_target=self._FACTS_TARGET,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                spectral_decode_path="sox_native",
            ),
            candidate_aac_lattice=capture,
        )

    def _assert_proof_granted(self, r, where):
        self.assertTrue(r["verified_lossless"], where)
        self.assertEqual(r["stage3_quality_gate"], "accept", where)
        self.assertEqual(r["final_status"], "imported", where)
        self.assertFalse(r["keep_searching"], where)
        self.assertEqual(r["target_final_format"], self._FACTS_TARGET, where)

    def _assert_proof_withheld(self, r, where):
        self.assertFalse(r["verified_lossless"], where)
        # Owned by the provisional anchor lane once unproven (issue #990):
        # no anchor exists in these worlds, so the album imports
        # provisionally with the lane's own search action; stage 3 never
        # runs for a lane-decided import.
        self.assertEqual(
            r["stage2_import"], "provisional_lossless_upgrade", where)
        self.assertIsNone(r["stage3_quality_gate"], where)
        self.assertEqual(r["final_status"], "wanted", where)
        self.assertTrue(r["keep_searching"], where)
        # Same as the granted case, on purpose: the stored format is
        # config, never proof (issue #829).
        self.assertEqual(r["target_final_format"], self._FACTS_TARGET, where)
        # Withholding a proof is NOT rejecting the album. It imported.
        self.assertTrue(r["imported"], where)

    def _both_twins(self, capture):
        return (
            ("evidence twin", self._evidence_decision(capture)),
            ("simulator twin", self._simulator_decision(capture)),
        )

    # -- the leg denies ----------------------------------------------------

    def test_an_apple_launder_withholds_the_proof_on_both_twins(self):
        """The flip the leg exists for. This album is spectrally
        indistinguishable from lossless — grade ``genuine``, V0 probe at
        245 — and every pre-v4 rule certifies it. Five of its six tracks
        recover the same MDCT frame offset, which no genuine album in the
        17-album control arm ever did (0/17 even at ``k >= 2``)."""
        for where, r in self._both_twins(self._apple_launder_capture()):
            self._assert_proof_withheld(r, where)
        for where, r in self._both_twins(self._genuine_capture()):
            self._assert_proof_granted(r, where)

    def test_offset_concentration_alone_denies(self):
        """No track exceeds the contrast threshold; the parameter-free
        coincidence count is the whole cause."""
        from lib.quality import (
            AAC_LATTICE_PROOF_DENY_MAX_Z,
            aac_lattice_proof_leg,
        )
        capture = self._offset_concentration_only_capture()
        leg = aac_lattice_proof_leg(capture)
        self.assertEqual(leg.reason, "offset_concentration")
        assert capture.max_z is not None
        self.assertLessEqual(capture.max_z, AAC_LATTICE_PROOF_DENY_MAX_Z)
        for where, r in self._both_twins(capture):
            self._assert_proof_withheld(r, where)

    def test_a_single_spiking_track_denies_without_four_scored_tracks(self):
        """A denial reads whatever evidence exists. ``k`` never reaches 4
        here, so refusing to act on one z=28.6 track — measured 0/197 on
        the genuine arm and 0/1136 on the wild arm above 12 — would fail
        OPEN, the wrong direction for a proof gate."""
        from lib.quality import aac_lattice_proof_leg
        capture = self._z_exceeded_only_capture()
        leg = aac_lattice_proof_leg(capture)
        self.assertEqual(leg.reason, "z_exceeded")
        assert leg.modal_count is not None
        self.assertLess(leg.modal_count, 4)
        for where, r in self._both_twins(capture):
            self._assert_proof_withheld(r, where)

    def test_the_thresholds_are_the_boundary(self):
        """Both sides of each operating point, on the production decider.
        The constants are READ, not restated — a pin spelling 4 or 12 would
        pass a module that had drifted to any other value."""
        from lib.quality import (
            AAC_LATTICE_PROOF_DENY_MAX_Z,
            AAC_LATTICE_PROOF_DENY_MODAL_COUNT,
        )
        k = AAC_LATTICE_PROOF_DENY_MODAL_COUNT
        shared = [(_APPLE_MODAL_OFFSET, 5.0)] * (k - 1)
        distinct = [(100 + i, 5.0) for i in range(3)]
        self._assert_proof_granted(
            self._evidence_decision(
                make_aac_lattice_capture([*shared, *distinct]),
            ),
            "one track short of the concentration count",
        )
        self._assert_proof_withheld(
            self._evidence_decision(
                make_aac_lattice_capture([
                    *shared, (_APPLE_MODAL_OFFSET, 5.0), *distinct[:2],
                ]),
            ),
            "exactly at the concentration count — inclusive",
        )
        z = AAC_LATTICE_PROOF_DENY_MAX_Z
        self._assert_proof_granted(
            self._evidence_decision(make_aac_lattice_capture([
                (13, z), (205, 4.8), (418, 5.0), (611, 5.2),
            ])),
            "exactly at the contrast threshold — exclusive",
        )
        self._assert_proof_withheld(
            self._evidence_decision(make_aac_lattice_capture([
                (13, z + 0.01), (205, 4.8), (418, 5.0), (611, 5.2),
            ])),
            "one hundredth above the contrast threshold",
        )

    def test_the_v0_override_cannot_outrank_a_denial(self):
        """The Bill Hicks shape (``suspect`` grade rescued by a
        lossless_source_v0 probe at avg 241 / min 219) does NOT rescue an
        album whose tracks share a frame lattice. The V0 probe measures
        re-encode difficulty; it cannot see an MDCT grid at all."""
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )

        def decide(capture):
            candidate = build_parity_candidate_evidence(
                is_flac=True, min_bitrate=0, is_cbr=False,
                spectral_grade="suspect",
                candidate_v0_probe_avg=241,
                candidate_v0_probe_min=219,
                codec_family="lossless",
                aac_lattice=capture,
            )
            return full_pipeline_decision_from_evidence(
                candidate, None,
                facts=AlbumQualityEvidenceDecisionFacts(
                    verified_lossless_target=self._FACTS_TARGET,
                ),
            )

        self.assertTrue(
            decide(self._genuine_capture())["verified_lossless"],
            "the V0-avg trust override must still rescue HF-poor lossless "
            "when the lattice leg has no objection",
        )
        self.assertFalse(
            decide(self._apple_launder_capture())["verified_lossless"],
            "a denied lattice leg is a hard veto ahead of the V0 override",
        )

    def test_neither_leg_can_overrule_the_other(self):
        """Two independent conditions on one proof. A clean lattice does
        not buy back an ultrasonic denial, and a clean ultrasonic does not
        buy back a lattice denial."""
        launder_deficit = TestUltrasonicProofGateV3.LAUNDER_DEFICIT_DB
        genuine_deficit = TestUltrasonicProofGateV3.GENUINE_DEFICIT_DB
        self._assert_proof_withheld(
            self._evidence_decision(
                self._genuine_capture(),
                ultrasonic_deficit_db=launder_deficit,
            ),
            "clean lattice, ultrasonic denial",
        )
        self._assert_proof_withheld(
            self._evidence_decision(
                self._apple_launder_capture(),
                ultrasonic_deficit_db=genuine_deficit,
            ),
            "clean ultrasonic, lattice denial",
        )

    # -- a denial withholds the proof and NOTHING else ---------------------

    #: The installed side of the worlds below, from PR3's V5 pins: a
    #: provisional-cohort album whose ``lossless_source_v0`` probe (avg
    #: 240) is its only comparable anchor. A candidate probe at avg 241
    #: does not clear it by the rank tolerance, so the provisional lane
    #: answers ``suspect_lossless_downgrade`` — a confident reject that
    #: also denylists the offering peer.
    _HAVE_PROVISIONAL_V0_AVG = 240

    def _denial_pair_evidence(self, capture, *, have_min, have_format,
                              have_is_cbr):
        from lib.quality import (
            EVIDENCE_SUBJECT_SOURCE,
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityV0Metric,
            full_pipeline_decision_from_evidence,
        )
        candidate = build_parity_candidate_evidence(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="suspect",
            candidate_v0_probe_avg=241,
            candidate_v0_probe_min=219,
            codec_family="lossless",
            aac_lattice=capture,
        )
        current = build_parity_current_evidence(
            min_bitrate=have_min, avg_bitrate=have_min, format=have_format,
            is_cbr=have_is_cbr,
            v0_metric=AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_SOURCE,
                min_bitrate_kbps=219,
                avg_bitrate_kbps=self._HAVE_PROVISIONAL_V0_AVG,
            ),
        )
        return full_pipeline_decision_from_evidence(
            candidate, current,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target=self._FACTS_TARGET,
            ),
        )

    def _denial_pair_simulator(self, capture, *, have_min, have_format,
                               have_is_cbr):
        """The convert branch: the lossless source is ground to V0."""
        from lib.quality import SpectralCodecContext
        return full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="suspect",
            converted_count=1,
            post_conversion_min_bitrate=219,
            post_conversion_is_cbr=False,
            candidate_v0_probe_avg=241,
            candidate_v0_probe_min=219,
            candidate_v0_probe_kind="lossless_source_v0",
            existing_min_bitrate=have_min,
            existing_avg_bitrate=have_min,
            existing_format=have_format.lower(),
            existing_is_cbr=have_is_cbr,
            existing_v0_probe_avg=self._HAVE_PROVISIONAL_V0_AVG,
            existing_v0_probe_kind="lossless_source_v0",
            verified_lossless_target=self._FACTS_TARGET,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                spectral_decode_path="sox_native",
            ),
            candidate_aac_lattice=capture,
        )

    def _denial_pair_flac_keep(self, capture, *, have_min, have_format,
                               have_is_cbr):
        """The kept-on-disk branch of the same world."""
        from lib.quality import SpectralCodecContext
        return full_pipeline_decision(
            is_flac=True, min_bitrate=900, is_cbr=False,
            spectral_grade="suspect",
            converted_count=0,
            target_format="flac",
            candidate_v0_probe_avg=241,
            candidate_v0_probe_min=219,
            candidate_v0_probe_kind="lossless_source_v0",
            existing_min_bitrate=have_min,
            existing_avg_bitrate=have_min,
            existing_format=have_format.lower(),
            existing_is_cbr=have_is_cbr,
            existing_v0_probe_avg=self._HAVE_PROVISIONAL_V0_AVG,
            existing_v0_probe_kind="lossless_source_v0",
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                spectral_decode_path="sox_native",
            ),
            candidate_aac_lattice=capture,
        )

    _DENIAL_TWINS = (
        ("evidence twin", "_denial_pair_evidence"),
        ("simulator twin, convert branch", "_denial_pair_simulator"),
        ("simulator twin, flac-keep branch", "_denial_pair_flac_keep"),
    )

    def test_a_denial_never_costs_the_album_its_import(self):
        """B-I1, against a HAVE-POPULATED world. A lattice denial withholds
        the PROOF; it must not take the ALBUM.

        The world is the one the V0-avg trust override exists to rescue —
        HF-poor lossless graded ``suspect`` with a ``lossless_source_v0``
        probe at avg 241 / min 219 — against a provisional-cohort MP3 128
        whose own comparable probe sits at avg 240. A denial that reached
        the lane choice would drop the album into
        ``suspect_lossless_downgrade``: a confident reject plus a peer
        denylist, on exactly the cohort the leg promised never to touch.
        This is the world PR3 shipped a blocking defect on; PR-B pins it
        for the lattice leg from birth."""
        for label, method in self._DENIAL_TWINS:
            with self.subTest(twin=label):
                def decide(capture, method=method):
                    return getattr(self, method)(
                        capture, have_min=128, have_format="MP3",
                        have_is_cbr=True,
                    )
                rescued = decide(self._genuine_capture())
                denied = decide(self._apple_launder_capture())
                self.assertTrue(
                    rescued["imported"],
                    f"{label}: the V0-rescued world imports",
                )
                self.assertTrue(
                    rescued["verified_lossless"],
                    f"{label}: the V0-avg trust override still rescues an "
                    "HF-poor lossless the leg has no objection to",
                )
                self.assertFalse(rescued["keep_searching"], label)
                self.assertTrue(
                    denied["imported"],
                    f"{label}: withholding a proof is NOT taking the album",
                )
                self.assertFalse(
                    denied["verified_lossless"],
                    f"{label}: the denial's first effect — no proof",
                )
                self.assertTrue(
                    denied["keep_searching"],
                    f"{label}: and its second — the album stays on the "
                    "search surface",
                )
                # The import lane itself does not move: Stage 2 decides
                # ``import`` either way, and the whole difference lands in
                # Stage 3, which is the ordinary "this V0 carries no proof,
                # keep looking" answer any unproved album gets.
                self.assertEqual(
                    denied["stage2_import"], rescued["stage2_import"],
                    f"{label}: a denial moved the Stage-2 import lane",
                )
                self.assertEqual(
                    denied["stage3_quality_gate"], "requeue_upgrade", label,
                )
                # ...and every denylist the denial causes is that ordinary
                # post-import policy, not a new class the leg introduced:
                # nothing Stage 2 decided denylists anything.
                from lib.quality import decision_denylists
                self.assertFalse(
                    decision_denylists(denied["stage2_import"]),
                    f"{label}: a denial minted a Stage-2 denylist",
                )
                self.assertEqual(
                    denied["denylisted"],
                    decision_denylists(denied["stage3_quality_gate"]),
                    f"{label}: the denylist is not the pre-existing "
                    "post-import policy's",
                )

    def test_a_denial_never_reroutes_the_album_into_the_provisional_lane(self):
        """The same world against a HAVE the unproved candidate cannot
        beat. The denial legitimately costs the import here — an album
        with no proof is compared on what it measures — but it must lose
        in the MEASURED lane, never by being re-routed into the
        provisional lane's confident reject."""
        for label, method in self._DENIAL_TWINS:
            with self.subTest(twin=label):
                denied = getattr(self, method)(
                    self._apple_launder_capture(), have_min=245,
                    have_format="Opus", have_is_cbr=False,
                )
                self.assertNotIn(
                    denied["stage2_import"],
                    PROVISIONAL_LANE_DECISIONS,
                    f"{label}: a denial re-routed the album into the "
                    "provisional lossless lane",
                )

    # -- the leg withholds -------------------------------------------------

    def test_an_unmeasured_album_is_untouched(self):
        """B-I2. NULL across all five columns means never measured, which
        is where essentially the whole library sits: the capture is gated
        to the promotion-plausible cohort and every pre-PR-A row has none.
        Withheld asserts nothing, and the pre-v4 outcome stands."""
        for where, r in self._both_twins(None):
            self._assert_proof_granted(r, f"{where}, no capture")

    def test_too_few_scored_tracks_withholds_rather_than_clearing(self):
        """Measured, and nothing usable found — three scored tracks and
        three detector errors. The concentration rule could not have fired
        whatever the audio was, so "it did not fire" is not a finding and
        must not be minted as one."""
        from lib.quality import aac_lattice_proof_leg
        capture = self._thin_capture()
        leg = aac_lattice_proof_leg(capture)
        self.assertEqual(leg.outcome, "withheld")
        self.assertEqual(leg.reason, "insufficient_scored_tracks")
        self.assertEqual(leg.scored_tracks, 3)
        for where, r in self._both_twins(capture):
            self._assert_proof_granted(r, f"{where}, thin capture")

    def test_a_measured_album_with_nothing_scored_withholds(self):
        """Every track errored — 96 kHz input has no scalefactor-band
        table at all. That is measured evidence of NOTHING, and it must
        never read as clean."""
        from lib.quality import aac_lattice_proof_leg
        capture = make_aac_lattice_capture([(None, None)] * 6)
        self.assertEqual(capture.scored_tracks, 0)
        leg = aac_lattice_proof_leg(capture)
        self.assertEqual(leg.outcome, "withheld")
        for where, r in self._both_twins(capture):
            self._assert_proof_granted(r, f"{where}, all tracks errored")

    def test_the_leg_never_reads_an_absolute_offset(self):
        """Absolute modal offsets are decode-path relative: a container
        whose decoder applies encoder-delay priming shifts the sample
        origin, so 960 and 0 are not portable facts. Concentration is —
        the SAME six tracks, moved off both constants, still deny."""
        shifted = make_aac_lattice_capture([
            (137, 5.11), (137, 4.92), (137, 6.30), (137, 5.77),
            (311, 4.61), (742, 5.02),
        ])
        self.assertNotIn(shifted.modal_offset, (0, _APPLE_MODAL_OFFSET))
        for where, r in self._both_twins(shifted):
            self._assert_proof_withheld(r, f"{where}, shifted lattice")


class TestLosslessStoredFormatIsProofBlind(unittest.TestCase):
    """The stored format of a lossless-sourced import is CONFIG, not proof.

    Issue #829, operator decision 2026-08-01, surfaced by the Badlands
    force import (Dirty Beaches, request 2147 / download 39087): a
    genuine-graded FLAC whose ultrasonic leg denied promotion landed on
    disk as MP3 V0 instead of the configured ``opus 128``, because both
    the harness's ``conversion_target`` and the decider's
    ``target_final_format`` keyed the target on the PROOF. Denial reaches
    ~34% of genuine-graded lossless, so that was live common behaviour.

    Authority: "no we always want it opus, the contract is not around
    verified or not, is the stored format for lossless absolutely.
    whatever people choose, v0,opus,aac it just has to be consistent" —
    https://github.com/abl030/cratedigger/issues/829

    The three-sentence model these pins encode: quality decides imports,
    proof decides names, config decides formats. So every scenario below
    asserts BOTH halves — the format does NOT move with the proof, and
    the proof itself still does.
    """

    _FACTS_TARGET = "opus 128"

    #: Download 39087's own measured deficit — the world that shipped the
    #: defect. Above the frozen 59.5 threshold, so the leg denies.
    BADLANDS_DENYING_DEFICIT_DB = 65.73

    #: The same album one measurement below the threshold.
    PASSING_DEFICIT_DB = 45.0

    def _evidence_decision(self, **candidate_kwargs):
        from lib.quality import (
            AlbumQualityEvidenceDecisionFacts,
            full_pipeline_decision_from_evidence,
        )
        candidate = build_parity_candidate_evidence(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            candidate_v0_probe_avg=245,
            candidate_v0_probe_min=245,
            codec_family="lossless",
            **candidate_kwargs,
        )
        return full_pipeline_decision_from_evidence(
            candidate, None,
            facts=AlbumQualityEvidenceDecisionFacts(
                verified_lossless_target=self._FACTS_TARGET,
            ),
        )

    def _simulator_decision(self, *, aac_lattice=None, **context_kwargs):
        from lib.quality import SpectralCodecContext
        return full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            converted_count=1,
            post_conversion_min_bitrate=245,
            post_conversion_is_cbr=False,
            candidate_v0_probe_avg=245,
            candidate_v0_probe_min=245,
            candidate_v0_probe_kind="lossless_source_v0",
            verified_lossless_target=self._FACTS_TARGET,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
                spectral_measurement_version=2,
                spectral_decode_path="sox_native",
                **context_kwargs,
            ),
            candidate_aac_lattice=aac_lattice,
        )

    def _assert_stored_as_configured(self, r, where):
        self.assertEqual(r["target_final_format"], self._FACTS_TARGET, where)
        # The album still lands: a withheld proof was never a rejection.
        self.assertTrue(r["imported"], where)

    def test_the_badlands_denial_still_stores_the_configured_target(self):
        """The incident, both twins. Only ``ultrasonic_deficit_db`` moves;
        the proof flips and the stored format does not."""
        denied_evidence = self._evidence_decision(
            ultrasonic_deficit_db=self.BADLANDS_DENYING_DEFICIT_DB)
        passed_evidence = self._evidence_decision(
            ultrasonic_deficit_db=self.PASSING_DEFICIT_DB)
        denied_sim = self._simulator_decision(
            ultrasonic_deficit_db=self.BADLANDS_DENYING_DEFICIT_DB)
        passed_sim = self._simulator_decision(
            ultrasonic_deficit_db=self.PASSING_DEFICIT_DB)

        # The proof leg still does its job — otherwise this pin would pass
        # on a tree where the leg had simply stopped denying.
        self.assertFalse(denied_evidence["verified_lossless"], "evidence twin")
        self.assertTrue(passed_evidence["verified_lossless"], "evidence twin")
        self.assertFalse(denied_sim["verified_lossless"], "simulator twin")
        self.assertTrue(passed_sim["verified_lossless"], "simulator twin")

        self._assert_stored_as_configured(denied_evidence, "evidence, denied")
        self._assert_stored_as_configured(passed_evidence, "evidence, passed")
        self._assert_stored_as_configured(denied_sim, "simulator, denied")
        self._assert_stored_as_configured(passed_sim, "simulator, passed")

    def test_a_lattice_denial_still_stores_the_configured_target(self):
        """The v4 leg's denial is the same shape of fact and costs the
        same nothing: an Apple/CoreAudio launder that imports is stored in
        the configured format like every other lossless-sourced import."""
        from lib.quality import aac_lattice_proof_leg
        capture = make_aac_lattice_capture([
            (960, 28.60), (960, 29.11), (960, 30.02),
            (960, 28.35), (960, 31.13), (512, 27.44),
        ])
        self.assertTrue(aac_lattice_proof_leg(capture).denies_promotion)

        denied_evidence = self._evidence_decision(aac_lattice=capture)
        denied_sim = self._simulator_decision(aac_lattice=capture)
        self.assertFalse(denied_evidence["verified_lossless"])
        self.assertFalse(denied_sim["verified_lossless"])
        self._assert_stored_as_configured(denied_evidence, "evidence, lattice")
        self._assert_stored_as_configured(denied_sim, "simulator, lattice")

    def test_an_ungraded_lossless_source_still_stores_the_configured_target(
        self,
    ):
        """No proof is possible at all here — the spectral analysis never
        produced a grade — and the stored format is still the config's.
        Absence of proof is not a different contract from denial."""
        from lib.quality import SpectralCodecContext
        ungraded_sim = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade=None,
            converted_count=1,
            post_conversion_min_bitrate=245,
            post_conversion_is_cbr=False,
            verified_lossless_target=self._FACTS_TARGET,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless",
            ),
        )
        self.assertFalse(ungraded_sim["verified_lossless"])
        self._assert_stored_as_configured(ungraded_sim, "simulator, ungraded")

    def test_no_configured_target_still_means_v0(self):
        """The must-still-work half. With nothing configured there is no
        target to store, and the decision must not invent one."""
        from lib.quality import SpectralCodecContext
        r = full_pipeline_decision(
            is_flac=True, min_bitrate=0, is_cbr=False,
            spectral_grade="genuine",
            converted_count=1,
            post_conversion_min_bitrate=245,
            post_conversion_is_cbr=False,
            candidate_v0_probe_avg=245,
            candidate_v0_probe_min=245,
            candidate_v0_probe_kind="lossless_source_v0",
            verified_lossless_target=None,
            candidate_spectral_context=SpectralCodecContext(
                codec_family="lossless", spectral_measurement_version=2,
                spectral_decode_path="sox_native",
                ultrasonic_deficit_db=self.PASSING_DEFICIT_DB,
            ),
        )
        self.assertTrue(r["verified_lossless"])
        self.assertIsNone(r["target_final_format"])


class TestVerifiedLosslessClassifierGeneration(unittest.TestCase):
    """``verified_lossless_classifier`` names WHICH MODEL proved a row.

    Issue #829 Phase 5 PR3 makes that load-bearing rather than
    decorative: the v3 name is minted only when the ultrasonic leg
    ADJUDICATED and passed, never merely because v3 code ran. A withheld
    leg proved nothing new, and stamping v3 on it would make the column
    mean two things again — the exact ambiguity it exists to remove.
    """

    @staticmethod
    def _mint(leg, lattice_leg=None):
        from lib.quality import mint_verified_lossless_proof
        return mint_verified_lossless_proof(
            True,
            was_converted_from="flac",
            detected_source_format="flac",
            spectral_grade="genuine",
            ultrasonic_leg=leg,
            aac_lattice_leg=lattice_leg,
        )

    def test_an_adjudicated_pass_mints_v3(self):
        from lib.quality import (
            VERIFIED_LOSSLESS_CLASSIFIER_V3,
            ultrasonic_proof_leg,
        )
        leg = ultrasonic_proof_leg(
            deficit_db=45.0, spectral_measurement_version=2,
            decode_path="sox_native", preserved_source_spectral=False,
        )
        self.assertEqual(leg.outcome, "passed")
        proof = self._mint(leg)
        assert proof is not None
        self.assertEqual(proof.classifier, VERIFIED_LOSSLESS_CLASSIFIER_V3)

    def test_a_withheld_leg_keeps_the_old_classifier(self):
        from lib.quality import (
            VERIFIED_LOSSLESS_CLASSIFIER,
            ultrasonic_proof_leg,
        )
        for reason_world in (
            {"deficit_db": None, "spectral_measurement_version": None,
             "decode_path": "sox_native", "preserved_source_spectral": True},
            {"deficit_db": None, "spectral_measurement_version": None,
             "decode_path": "sox_native", "preserved_source_spectral": False},
            {"deficit_db": None, "spectral_measurement_version": 2,
             "decode_path": "sox_native", "preserved_source_spectral": False},
            {"deficit_db": 45.0, "spectral_measurement_version": 2,
             "decode_path": "ffmpeg_resampled",
             "preserved_source_spectral": False},
            {"deficit_db": 45.0, "spectral_measurement_version": 2,
             "decode_path": None, "preserved_source_spectral": False},
        ):
            leg = ultrasonic_proof_leg(**reason_world)
            with self.subTest(reason=leg.reason):
                self.assertEqual(leg.outcome, "withheld")
                proof = self._mint(leg)
                assert proof is not None
                self.assertEqual(
                    proof.classifier, VERIFIED_LOSSLESS_CLASSIFIER,
                )

    def test_no_leg_at_all_keeps_the_old_classifier(self):
        """A caller with no ultrasonic evidence is the pre-v3 world and
        must mint the pre-v3 name."""
        from lib.quality import VERIFIED_LOSSLESS_CLASSIFIER
        proof = self._mint(None)
        assert proof is not None
        self.assertEqual(proof.classifier, VERIFIED_LOSSLESS_CLASSIFIER)

    # -- v4 composition (issue #829 AAC-lattice leg PR-B, B-I3) ------------

    def test_the_classifier_composes_over_both_legs(self):
        """Every cell of the composition table, on the real minter.

        The classifier names WHICH MODELS ran. v4 is claimed only when
        BOTH adjudicated and passed; a lattice pass with no ultrasonic
        adjudication is the BASE name, not v4 and not a v4-minus — the
        names are a ladder of what was tested, and skipping a rung must
        not buy the top one. A DENIED leg never reaches the minter at all,
        because ``determine_verified_lossless`` already vetoed the proof;
        the denied rows below prove the minter does not mistake a denial
        for an adjudication if it ever did.
        """
        from lib.quality import (
            VERIFIED_LOSSLESS_CLASSIFIER,
            VERIFIED_LOSSLESS_CLASSIFIER_V3,
            VERIFIED_LOSSLESS_CLASSIFIER_V4,
            aac_lattice_proof_leg,
            ultrasonic_proof_leg,
        )

        def ultrasonic(outcome):
            if outcome == "absent":
                return None
            leg = ultrasonic_proof_leg(
                deficit_db={"passed": 45.0, "denied": 65.16,
                            "withheld": None}[outcome],
                spectral_measurement_version=(
                    None if outcome == "withheld" else 2
                ),
                decode_path="sox_native", preserved_source_spectral=False,
            )
            self.assertEqual(leg.outcome, outcome)
            return leg

        def lattice(outcome):
            if outcome == "absent":
                return None
            captures = {
                "passed": TestAacLatticeProofGate._genuine_capture(),
                "denied": TestAacLatticeProofGate._apple_launder_capture(),
                "withheld": TestAacLatticeProofGate._thin_capture(),
            }
            leg = aac_lattice_proof_leg(captures[outcome])
            self.assertEqual(leg.outcome, outcome)
            return leg

        states = ("passed", "withheld", "denied", "absent")
        for ultra in states:
            for lat in states:
                expected = VERIFIED_LOSSLESS_CLASSIFIER
                if ultra == "passed":
                    expected = (
                        VERIFIED_LOSSLESS_CLASSIFIER_V4
                        if lat == "passed"
                        else VERIFIED_LOSSLESS_CLASSIFIER_V3
                    )
                with self.subTest(ultrasonic=ultra, lattice=lat):
                    proof = self._mint(ultrasonic(ultra), lattice(lat))
                    assert proof is not None
                    self.assertEqual(proof.classifier, expected)


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
    _build_candidate = staticmethod(build_parity_candidate_evidence)
    _build_current = staticmethod(build_parity_current_evidence)

    def test_audio_corrupt_routes_through_full_pipeline(self):
        from lib.quality import (
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
        from datetime import datetime

        from lib.quality import (
            AlbumQualityEvidence,
            AlbumQualityEvidenceFile,
            AudioQualityMeasurement,
            classify_full_pipeline_decision,
            evidence_decision_name,
            full_pipeline_decision_from_evidence,
        )

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
                spectral_subject="source",
                spectral_provenance="measured",
            ),
            measured_at=datetime(2026, 5, 21, tzinfo=UTC),
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
        from datetime import datetime

        from lib.quality import (
            AlbumQualityEvidence,
            AlbumQualityEvidenceFile,
            AudioQualityMeasurement,
            full_pipeline_decision_from_evidence,
        )

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
            measured_at=datetime(2026, 5, 21, tzinfo=UTC),
            files=files,
            codec="flac",
            container="flac",
            storage_format="flac",
            audio_file_count=len(files),
            filetype_band="flac, wav",
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
        )

        self.assertIsNone(r["preimport_mixed_source"])

    def test_decision_order_corrupt_takes_priority_over_other_facts(self):
        """When multiple facts are present, ``audio_corrupt`` wins
        (matches the deleted ``preimport_decide`` evaluation order)."""
        from lib.quality import (
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
        )

        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertIsNone(r["preimport_bad_hash"])
        self.assertIsNone(r["preimport_nested"])
        self.assertEqual(evidence_decision_name(r), "audio_corrupt")

    def test_classify_full_pipeline_decision_matches_evidence_priority(self):
        """Issue #1355 item 1, third site: ``classify_full_pipeline_decision``
        (preview/cleanup display) must name the same fact
        ``evidence_decision_name`` (dispatch) does, for every adjacent pair
        in the shared priority order (audio_corrupt > bad_audio_hash >
        nested_layout > empty_fileset > mixed_source). No real decision
        dict can carry two of these keys as reject values any more, so
        this drives the classifier's own pure contract directly on
        hand-built dicts rather than through either twin. A regression
        here would only resurface if some future writer ever populates two
        of these keys again, which is exactly the landmine this pins
        against: the audio/nested pair was the original bug, and review
        found the classifier's bad_hash/nested pair independently
        reordered too before this test existed.
        """
        from lib.quality import classify_full_pipeline_decision

        cases = (
            ("audio_corrupt over nested_layout",
             {"preimport_audio": "reject_corrupt",
              "preimport_nested": "reject_nested"},
             "audio_corrupt"),
            ("bad_audio_hash over nested_layout",
             {"preimport_bad_hash": "reject_bad_hash",
              "preimport_nested": "reject_nested"},
             "bad_audio_hash"),
        )
        for desc, keys, expected in cases:
            with self.subTest(desc=desc):
                decision = {**keys, "imported": False}
                verdict, cleanup_eligible, reason = (
                    classify_full_pipeline_decision(decision))
                self.assertEqual(verdict, "confident_reject")
                self.assertTrue(cleanup_eligible)
                self.assertEqual(reason, expected)

    def test_preimport_fact_reject_keeps_searching(self):
        """The mode-blind reducer reports the shared self-healing outcome."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = self._build_candidate(
            is_flac=False, min_bitrate=245, is_cbr=False,
            audio_corrupt=True,
        )

        r = full_pipeline_decision_from_evidence(
            candidate, None,
        )

        self.assertEqual(r["preimport_audio"], "reject_corrupt")
        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])


class TestStage2CounterfactualAudit(unittest.TestCase):
    """PAIR (deterministic half) — issue #829 Phase 5 PR2d.

    A Stage-1 spectral reject short-circuits before Stage 2 ever runs, so
    "Stage 1 rejected this, and Stage 2 would have said X" — the
    disagreement issue #813 is about, and the single most useful thing an
    operator can be told about a spectral reject — was computed nowhere.
    ``full_pipeline_decision`` now computes it and reports it under two
    audit-only keys.

    The generated twins live in ``tests/test_quality_generated.py``:
    ``test_the_counterfactual_is_absent_unless_stage1_short_circuits``,
    ``test_the_stage1_reject_decision_is_unchanged_by_its_audit`` and
    ``test_the_reported_counterfactual_is_what_stage_2_decides``.
    """

    @staticmethod
    def _decide(*, spectral_bitrate: int):
        """An MP3 CBR pair whose classes are comparable (same codec, both
        legacy stored buckets, both grades authorizing). The candidate's
        class is the only variable: strictly lower than the HAVE's 192 is
        the one shape ``spectral_import_decision`` rejects on. Shared
        verbatim with the generated half's
        ``_STAGE1_REJECT_COUNTERFACTUAL_WORLD``.
        """
        return full_pipeline_decision(
            is_flac=False, min_bitrate=256, avg_bitrate=256, is_cbr=True,
            is_vbr=False, new_format="MP3",
            spectral_grade="likely_transcode",
            spectral_bitrate=spectral_bitrate,
            existing_min_bitrate=256, existing_avg_bitrate=256,
            existing_format="MP3", existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=192,
        )

    @staticmethod
    def _decide_raising_tail(*, spectral_bitrate: int):
        """A FLAC-convert world whose Stage-2 tail RAISES.

        ``comparison_format_hint`` passes the bare ``MP3`` label straight
        through to ``TargetQualityContract.from_explicit_label``, which
        rejects it because a bare MP3 declares neither CBR nor VBR. That is
        the one Stage-2 path a Stage-1 reject never used to reach.
        """
        return full_pipeline_decision(
            is_flac=True, min_bitrate=200, is_cbr=True,
            new_format="MP3", target_format="mp3",
            existing_min_bitrate=256, existing_format="MP3",
            existing_is_cbr=True,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=192,
            converted_count=1, supported_lossless_source=False,
            spectral_grade="likely_transcode",
            spectral_bitrate=spectral_bitrate,
        )

    def test_stage1_reject_reports_what_stage2_would_have_decided(self):
        r = self._decide(spectral_bitrate=128)

        self.assertEqual(r["stage1_spectral"], "reject")
        # The counterfactual: Stage 2 would have called this a downgrade,
        # scoring the candidate "worse" on rank. Nothing in the pipeline
        # computed that before this audit existed.
        self.assertEqual(r["stage2_import_if_stage1_deferred"], "downgrade")
        basis = r["comparison_basis_if_stage1_deferred"]
        assert isinstance(basis, dict)
        self.assertEqual(basis["verdict"], "worse")
        self.assertEqual(basis["branch"], "rank")

    def test_the_audit_changes_no_decision_field(self):
        """Inertness, field by field: a Stage-1 reject decides exactly what
        it decided before the counterfactual was computed."""
        r = self._decide(spectral_bitrate=128)

        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["keep_searching"])
        self.assertTrue(r["denylisted"])
        self.assertFalse(r["imported"])
        self.assertFalse(r["verified_lossless"])
        # Stage 2 and Stage 3 never ran for the DECISION, whatever the
        # counterfactual computed.
        self.assertIsNone(r["stage2_import"])
        self.assertIsNone(r["stage3_quality_gate"])
        self.assertIsNone(r["comparison_basis"])
        self.assertIsNone(r["target_final_format"])

    def test_a_deferring_world_reports_no_counterfactual(self):
        """Both keys exist on every result and stay None when Stage 2 really
        ran — a counterfactual beside a real decision would be a second,
        contradictory answer."""
        r = self._decide(spectral_bitrate=192)

        self.assertNotEqual(r["stage1_spectral"], "reject")
        self.assertIsNotNone(r["stage2_import"])
        self.assertIsNone(r["stage2_import_if_stage1_deferred"])
        self.assertIsNone(r["comparison_basis_if_stage1_deferred"])

    def test_a_counterfactual_that_raises_cannot_break_the_decision(self):
        """The audit may not turn a clean Stage-1 reject into a crash.

        The trigger is PRODUCED, not invented
        (``.claude/rules/test-fidelity.md`` Rule C): the first half drives
        the real decider over the deferring twin of this world and shows
        that Stage 2 really does raise there. See
        ``_decide_raising_tail`` for which production call raises and why.
        """
        with self.assertRaises(ValueError) as raised:
            self._decide_raising_tail(spectral_bitrate=192)
        self.assertIn("bare MP3", str(raised.exception))

        r = self._decide_raising_tail(spectral_bitrate=128)

        self.assertEqual(r["stage1_spectral"], "reject")
        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["keep_searching"])
        self.assertTrue(r["denylisted"])
        self.assertFalse(r["imported"])
        self.assertIsNone(r["stage2_import"])
        # Reported, not silent: "the audit could not run" is a different
        # fact from "Stage 1 never short-circuited" (which is what a None
        # here means), and the operator is entitled to both.
        self.assertEqual(
            r["stage2_import_if_stage1_deferred"],
            STAGE2_COUNTERFACTUAL_UNAVAILABLE,
        )
        self.assertIsNone(r["comparison_basis_if_stage1_deferred"])

    def test_a_lossless_source_candidate_reports_its_own_counterfactual(self):
        """The counterfactual on a LOSSLESS-SOURCE Stage-2 branch, through
        the production decider (issue #829 Phase 5 PR2d review S1).

        Every other test here is native-lossy. This is the shape the
        evidence entrypoint really produces: a candidate whose measurement
        wears an MP3 label against a lossless target, so
        ``_lossless_source_from_evidence`` is True and the FLAC-keep branch
        runs. Stage 1's carve-out (``provisional_source_candidate and
        has_provisional_probe_input``) spares every lossless-source
        candidate that HAS probe evidence, so the only ones that
        short-circuit are the ones with none. Since issue #990 keyed lane
        entry on proof absence, which lane the counterfactual reaches
        follows the proof: an UNPROVEN candidate lands in the provisional
        lane (probe-missing, accused), while a PROOF-BEARING one — existing
        stamps remain proofs — is measured, with a comparison basis.
        """
        import msgspec

        from lib.quality import (
            EVIDENCE_SUBJECT_SOURCE,
            AlbumQualityEvidenceDecisionFacts,
            AlbumQualityV0Metric,
            VerifiedLosslessProof,
            full_pipeline_decision_from_evidence,
        )

        # The proof is load-bearing for branch selection on this synthetic
        # MP3-labeled shape (``_lossless_source_from_evidence`` reads it),
        # and only an MP3-labeled measurement is Stage-1-comparable against
        # the MP3 current. The unproven twin therefore derives its
        # lossless-source fact from a source-subject V0 metric instead —
        # min-only, so it is not a comparable probe and Stage 1's carve-out
        # does not spare it.
        unproven = msgspec.structs.replace(
            build_parity_candidate_evidence(
                is_flac=False, min_bitrate=250, avg_bitrate=250, is_cbr=False,
                spectral_grade="likely_transcode", spectral_bitrate=128,
            ),
            v0_metric=AlbumQualityV0Metric(
                subject=EVIDENCE_SUBJECT_SOURCE,
                min_bitrate_kbps=219,
            ),
        )
        proof_bearing = msgspec.structs.replace(
            build_parity_candidate_evidence(
                is_flac=False, min_bitrate=250, avg_bitrate=250, is_cbr=False,
                spectral_grade="likely_transcode", spectral_bitrate=128,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured", source="test", classifier="test"),
        )
        current = build_parity_current_evidence(
            min_bitrate=256, avg_bitrate=256, format="MP3", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=192,
        )

        r = full_pipeline_decision_from_evidence(
            unproven, current,
            facts=AlbumQualityEvidenceDecisionFacts(target_format="flac"),
        )
        self.assertEqual(r["stage0_spectral_gate"], "skipped_flac")
        self.assertEqual(r["stage1_spectral"], "reject")
        self.assertEqual(
            r["stage2_import_if_stage1_deferred"],
            "suspect_lossless_probe_missing",
        )
        # No comparison ran in the counterfactual, so no basis — a real
        # outcome, not a failure to evaluate.
        self.assertIsNone(r["comparison_basis_if_stage1_deferred"])
        # ... and the decision itself is the untouched Stage-1 reject.
        self.assertIsNone(r["stage2_import"])
        self.assertIsNone(r["stage3_quality_gate"])
        self.assertIsNone(r["comparison_basis"])
        self.assertEqual(r["final_status"], "wanted")

        proved = full_pipeline_decision_from_evidence(
            proof_bearing, current,
            facts=AlbumQualityEvidenceDecisionFacts(target_format="flac"),
        )
        self.assertEqual(proved["stage1_spectral"], "reject")
        self.assertNotIn(
            proved["stage2_import_if_stage1_deferred"],
            PROVISIONAL_LANE_DECISIONS,
            "a proof-bearing candidate's counterfactual must be measured, "
            "never lane-owned",
        )
        self.assertIsNotNone(proved["comparison_basis_if_stage1_deferred"])
        self.assertIsNone(proved["stage2_import"])
        self.assertTrue(r["keep_searching"])
        self.assertFalse(r["imported"])

    def test_the_evidence_twin_reports_the_same_counterfactual(self):
        """The production decider the importer calls carries the audit too —
        the same parity contract every other scenario in this module owes."""
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = build_parity_candidate_evidence(
            is_flac=False, min_bitrate=256, avg_bitrate=256, is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=128,
        )
        current = build_parity_current_evidence(
            min_bitrate=256, avg_bitrate=256, format="MP3", is_cbr=True,
            spectral_grade="likely_transcode", spectral_bitrate=192,
        )

        r = full_pipeline_decision_from_evidence(candidate, current)

        self.assertEqual(r["stage1_spectral"], "reject")
        self.assertEqual(r["stage2_import_if_stage1_deferred"], "downgrade")
        self.assertIsNone(r["stage2_import"])
        self.assertEqual(r["final_status"], "wanted")
        self.assertTrue(r["keep_searching"])

    def test_early_exit_paths_carry_the_audit_keys_as_none(self):
        """The two twins share ONE documented dict shape; the evidence
        decider's hand-written early-exit dicts must not drift from it."""
        import msgspec

        from lib.quality import (
            VerifiedLosslessProof,
            full_pipeline_decision_from_evidence,
        )

        corrupt = build_parity_candidate_evidence(
            is_flac=False, min_bitrate=245, is_cbr=False, audio_corrupt=True,
        )
        proof_bearing_current = build_parity_current_evidence(
            min_bitrate=320, avg_bitrate=320, format="MP3", is_cbr=True)
        assert proof_bearing_current is not None
        proof_bearing_current = msgspec.structs.replace(
            proof_bearing_current,
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured", source="test", classifier="test"),
        )
        simulator_keys = set(full_pipeline_decision(
            is_flac=False, min_bitrate=256, is_cbr=False).keys())

        for name, result in (
            ("preimport reject",
             full_pipeline_decision_from_evidence(corrupt, None)),
            ("verified-lossless lock", full_pipeline_decision_from_evidence(
                build_parity_candidate_evidence(
                    is_flac=False, min_bitrate=245, is_cbr=False),
                proof_bearing_current,
            )),
        ):
            with self.subTest(path=name):
                self.assertEqual(set(result.keys()), simulator_keys)
                self.assertIsNone(result["stage2_import_if_stage1_deferred"])
                self.assertIsNone(
                    result["comparison_basis_if_stage1_deferred"])


class TestBoundaryHysteresisAlbumsThroughEvidencePipeline(unittest.TestCase):
    """The parity half of ``TestBoundaryHysteresisAlbums``.

    Same four worlds, same expected outcomes, through the function the
    importer actually calls. A guard that only holds in the flat-kwargs
    simulator would not protect a single live album.
    """

    @staticmethod
    def _decide(
        *,
        candidate_kbps: int,
        candidate_format: str = "MP3",
        candidate_is_cbr: bool = False,
        installed_kbps: int,
        installed_format: str = "MP3",
        installed_is_cbr: bool = False,
    ) -> dict[str, object]:
        from lib.quality import full_pipeline_decision_from_evidence

        candidate = build_parity_candidate_evidence(
            is_flac=False,
            min_bitrate=candidate_kbps,
            avg_bitrate=candidate_kbps,
            is_cbr=candidate_is_cbr,
            native_codec=candidate_format.lower(),
            native_format=candidate_format,
        )
        current = build_parity_current_evidence(
            min_bitrate=installed_kbps,
            avg_bitrate=installed_kbps,
            format=installed_format,
            is_cbr=installed_is_cbr,
        )
        return full_pipeline_decision_from_evidence(candidate, current)

    def test_koppel_one_kbps_reimport_over_itself_via_evidence(self):
        """download_log 39947, request 4781 — through the real decider."""
        result = self._decide(
            candidate_kbps=255, candidate_is_cbr=False,
            installed_kbps=256, installed_is_cbr=True,
        )
        self.assertFalse(result["imported"])
        self.assertEqual(result["stage2_import"], "downgrade")
        self.assertEqual(
            json_dict(result["comparison_basis"])["branch"], "rank_within_tolerance")

    def test_the_collapsed_band_edges_do_not_authorise_a_replace_via_evidence(
        self,
    ):
        for description, candidate, installed in (
            # Kerrie Biddell, *Only The Beginning* — installed avg 317.
            ("request 5629, transparent edge", 320, 317),
            # Dead Fawn, *Session III* — installed avg 190 (min 188).
            ("request 3182, good edge", 192, 190),
            # No live pair sits exactly here; this is the window's own edge.
            ("excellent edge at the full window", 256, 252),
        ):
            with self.subTest(album=description):
                result = self._decide(
                    candidate_kbps=candidate, installed_kbps=installed)
                self.assertFalse(result["imported"])
                self.assertEqual(result["stage2_import"], "downgrade")
                self.assertEqual(
                    json_dict(result["comparison_basis"])["branch"],
                    "rank_within_tolerance")

    def test_a_real_upgrade_still_imports_via_evidence(self):
        result = self._decide(candidate_kbps=320, installed_kbps=200)
        self.assertTrue(result["imported"])
        self.assertEqual(result["stage2_import"], "import")
        self.assertEqual(json_dict(result["comparison_basis"])["branch"], "rank")

    def test_a_cross_family_pair_one_kbps_apart_still_imports_via_evidence(
        self,
    ):
        """Candidate 6963 / request 929 — Opus 127 over AAC 128."""
        result = self._decide(
            candidate_kbps=127, candidate_format="Opus",
            installed_kbps=128, installed_format="AAC",
        )
        self.assertTrue(result["imported"])
        self.assertEqual(result["stage2_import"], "import")
        self.assertEqual(json_dict(result["comparison_basis"])["branch"], "rank")


if __name__ == "__main__":
    unittest.main()
