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

from lib.quality import CodecFamily, full_pipeline_decision
from tests.helpers import (
    build_parity_candidate_evidence,
    build_parity_current_evidence,
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

    Authority: "when a candidate has a decision-grade transcode MP3 inferred
    class and a known non-transcode current copy has no spectral bitrate,
    compare using the candidate bound; import only when that bounded rank is
    strictly better than the current raw rank; 320/spectral-128 versus
    genuine-160 is equivalent / not an import" —
    https://github.com/abl030/cratedigger/issues/829#issuecomment-5089731485

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
        # Stage 2's candidate bound, which weighs the fake's OWN class (128)
        # against the genuine HAVE's real rank instead of weighing two
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

    def test_taboo_vi_fake_flac_192_via_evidence(self):
        """Parity twin of Taboo VI's no-cliff fake-FLAC reproduction."""
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
        self.assertEqual(r["stage2_import"], "provisional_lossless_upgrade")
        self.assertTrue(r["imported"])
        self.assertTrue(r["denylisted"])
        self.assertTrue(r["keep_searching"])

    def test_taboo_vi_with_spectral_bitrate_via_evidence(self):
        """Parity twin of Taboo VI's captured-spectral reproduction."""
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

        self.assertEqual(r["stage2_import"], "provisional_lossless_upgrade")
        self.assertIsNone(r["stage3_quality_gate"])
        self.assertTrue(r["imported"])
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
            existing_min_bitrate=100,
            existing_avg_bitrate=119,
            existing_format="Opus",
            existing_is_cbr=False,
            existing_spectral_grade="likely_transcode",
            existing_spectral_bitrate=128,
            existing_v0_probe_avg=215,
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
            post_conversion_min_bitrate=216,
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


if __name__ == "__main__":
    unittest.main()
