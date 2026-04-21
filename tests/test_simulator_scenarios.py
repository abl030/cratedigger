#!/usr/bin/env python3
"""Simulator scenario test suite for pipeline quality decisions.

Tests the COMPOSITION of decision functions — full_pipeline_decision() +
rejection_backfill_override() + search_tiers — against a matrix of album states
and download scenarios. Catches interaction bugs between stages that unit tests
on individual functions miss.

GitHub issue #32.
"""

import os
import sys
import unittest
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.quality import (
    compute_effective_override_bitrate,
    full_pipeline_decision,
    rejection_backfill_override,
    search_tiers,
)


# ============================================================================
# Fixtures
# ============================================================================

@dataclass(frozen=True)
class AlbumState:
    """On-disk state of an album in the pipeline."""
    name: str
    min_bitrate: int | None       # album_requests.min_bitrate
    is_cbr: bool                  # on-disk files are constant bitrate
    spectral_grade: str | None    # current_spectral_grade
    spectral_bitrate: int | None  # current_spectral_bitrate
    verified_lossless: bool
    search_filetype_override: str | None  # search_filetype_override (transient search filter)
    target_format: str | None = None  # persistent user intent
    existing_format: str | None = None  # beets items.format ("MP3"/"FLAC"/"Opus"/None)
    # Beets items.bitrate avg across the album. Defaults to None → simulator
    # falls back to min_bitrate (legacy scenarios that don't distinguish).
    # Supply an explicit value for genuinely VBR existing albums where avg
    # diverges from min; the CBR-conditional override in full_pipeline_decision
    # needs this to behave correctly (see test_unter_null_failure_epiphany_vbr_loop).
    avg_bitrate: int | None = None


def _derive_album_format(album: "AlbumState") -> str | None:
    """Derive a codec-aware format hint for an AlbumState.

    Legacy simulator fixtures don't carry a format field — derive one from
    ``verified_lossless`` (heuristic for FLAC-on-disk albums) / ``is_cbr`` /
    min bitrate so the rank model has what it needs without rewriting 14
    fixture rows. ``existing_format`` overrides everything.
    """
    if album.existing_format is not None:
        return album.existing_format
    if album.min_bitrate is None:
        return None
    # verified_lossless with raw FLAC-range bitrate (≥500) → FLAC on disk.
    if album.verified_lossless and album.min_bitrate >= 500:
        return "FLAC"
    # Otherwise assume MP3 — band table classifies via is_cbr.
    return "MP3"


@dataclass(frozen=True)
class DownloadScenario:
    """Properties of an incoming download."""
    name: str
    is_flac: bool
    min_bitrate: int
    is_cbr: bool
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    converted_count: int = 0
    post_conversion_min_bitrate: int | None = None
    new_format: str | None = None  # explicit format hint for the rank model
    # VBR + avg bitrate drive the preimport spectral gate (issue #93).
    # is_vbr defaults to None (simulator derives it from is_cbr); pass
    # explicitly only for scenarios where is_vbr != not is_cbr.
    is_vbr: bool | None = None
    avg_bitrate: int | None = None

    def dl_params(self) -> dict:
        """Download-side kwargs for full_pipeline_decision()."""
        return {
            "is_flac": self.is_flac,
            "min_bitrate": self.min_bitrate,
            "is_cbr": self.is_cbr,
            "is_vbr": self.is_vbr,
            "avg_bitrate": self.avg_bitrate,
            "spectral_grade": self.spectral_grade,
            "spectral_bitrate": self.spectral_bitrate,
            "converted_count": self.converted_count,
            "post_conversion_min_bitrate": self.post_conversion_min_bitrate,
            "new_format": self.new_format,
        }


@dataclass(frozen=True)
class SimResult:
    """Full outcome of a simulator run."""
    imported: bool
    keep_searching: bool
    denylisted: bool
    final_status: str | None
    stage0_spectral_gate: str | None
    stage1_spectral: str | None
    stage2_import: str | None
    stage3_quality_gate: str | None
    backfill_override: str | None
    search_filetype_override_after: str | None  # what search_filetype_override becomes after cycle
    target_format_after: str | None = None  # target_format is always preserved


# --- 13 album state fixtures ---

ALBUM_STATES = [
    AlbumState("fresh_request", None, False, None, None, False, None),
    AlbumState("cbr_320_no_spectral", 320, True, None, None, False, None),
    AlbumState("cbr_320_genuine", 320, True, "genuine", None, False, None),
    AlbumState("cbr_320_suspect", 320, True, "suspect", None, False, None),
    AlbumState("cbr_320_genuine_flac_override", 320, True, "genuine", None, False, "lossless"),
    AlbumState("cbr_320_genuine_spectral96", 320, True, "genuine", 96, False, "lossless"),
    AlbumState("vbr_v0_genuine", 240, False, "genuine", None, False, None),
    AlbumState("vbr_v0_no_spectral", 240, False, None, None, False, None),
    AlbumState("vbr_low_205", 205, False, None, None, False, None),
    AlbumState("verified_lossless_v0", 245, False, "genuine", None, True, None),
    AlbumState("verified_lossless_lofi", 207, False, "genuine", None, True, None),
    AlbumState("cbr_192_genuine", 192, True, "genuine", None, False, None),
    AlbumState("cbr_192_suspect", 192, True, "suspect", None, False, None),
    AlbumState("verified_lossless_opus", 123, False, "genuine", 123, True, None),
    # Unter Null - The Failure Epiphany (req 1749). 24-track beets album
    # with per-track bitrates 152-310 kbps (avg 225, median 224). Spectral
    # flags every track likely_transcode at ~96 kbps — very likely a false
    # positive on the genre's naturally sparse high frequencies. Quality gate
    # with override=lossless,mp3 v0,mp3 320 is already set (the 4th field
    # below). Existing test fixtures don't set avg, so the VBR→override
    # intersection (which drove the 2026-04-21 import→gate-deny loop) wasn't
    # covered until this entry.
    AlbumState("unter_null_failure_epiphany", 152, False,
               "likely_transcode", 96, False,
               "lossless,mp3 v0,mp3 320",
               avg_bitrate=225,
               existing_format="MP3"),
    # Brian Eno - Generative Music I (req 1486). After a successful MP3
    # 128kbps import (download_log 3284), beets held a uniform-128kbps MP3
    # album. The album's spectral grade rolled up to ``genuine`` (only 25%
    # of tracks were ``suspect``, below the 60% threshold) yet the album's
    # ``spectral_bitrate`` — ``min`` across the per-track estimates — still
    # reported a 96kbps cliff because several suspect tracks were detected.
    # Captures the "genuine grade + low spectral estimate on existing" shape
    # the pre-import comparison would then face on the next force-import.
    AlbumState("eno_genuine_spectral96_mp3_128", 128, False,
               "genuine", 96, False, None,
               avg_bitrate=128,
               existing_format="MP3"),
]

ALBUM_MAP = {a.name: a for a in ALBUM_STATES}

# --- 16 download scenario fixtures ---

DOWNLOAD_SCENARIOS = [
    # FLAC downloads
    DownloadScenario("flac_genuine_high", True, 245, False,
                     spectral_grade="genuine", converted_count=12,
                     post_conversion_min_bitrate=245),
    DownloadScenario("flac_genuine_lofi", True, 207, False,
                     spectral_grade="genuine", converted_count=12,
                     post_conversion_min_bitrate=207),
    DownloadScenario("flac_marginal", True, 240, False,
                     spectral_grade="marginal", converted_count=12,
                     post_conversion_min_bitrate=240),
    DownloadScenario("flac_suspect_190", True, 190, False,
                     spectral_grade="suspect", converted_count=12,
                     post_conversion_min_bitrate=190),
    DownloadScenario("flac_suspect_245", True, 245, False,
                     spectral_grade="suspect", converted_count=12,
                     post_conversion_min_bitrate=245),
    # FLAC kept on disk (no conversion) — raw FLAC bitrate
    DownloadScenario("flac_genuine_raw", True, 900, False,
                     spectral_grade="genuine",
                     converted_count=0,
                     post_conversion_min_bitrate=None),
    # MP3 VBR (avg_bitrate drives the preimport spectral gate — issue #93)
    DownloadScenario("mp3_v0_240", False, 240, False,
                     is_vbr=True, avg_bitrate=245),
    DownloadScenario("mp3_v0_low_205", False, 205, False,
                     is_vbr=True, avg_bitrate=205),
    DownloadScenario("mp3_v2_190", False, 190, False,
                     is_vbr=True, avg_bitrate=190),
    # VBR transcode masquerading as V0 (Go! Team shape from issue #93).
    # Low avg + likely_transcode spectral → stage 0 gates, stage 1 rejects.
    DownloadScenario("vbr_transcode_go_team_shape", False, 126, False,
                     is_vbr=True, avg_bitrate=182,
                     spectral_grade="likely_transcode",
                     spectral_bitrate=96),
    # CBR no spectral
    DownloadScenario("cbr_320_no_spectral", False, 320, True),
    DownloadScenario("cbr_256_no_spectral", False, 256, True),
    DownloadScenario("cbr_192_no_spectral", False, 192, True),
    DownloadScenario("cbr_152_no_spectral", False, 152, True),
    # CBR 152 with transcode spectral — the shape every Soulseek source for
    # Unter Null - The Failure Epiphany delivered on 2026-04-21 (drasslok23,
    # shutupman2, synthetic, DrinkMoxie). All 152 kbps CBR, all with a
    # cliff at ~96 kbps.
    DownloadScenario("cbr_152_likely_transcode_96", False, 152, True,
                     spectral_grade="likely_transcode", spectral_bitrate=96),
    # CBR with spectral
    DownloadScenario("cbr_320_genuine", False, 320, True,
                     spectral_grade="genuine"),
    DownloadScenario("cbr_320_suspect_128", False, 320, True,
                     spectral_grade="suspect", spectral_bitrate=128),
    DownloadScenario("cbr_320_suspect_192", False, 320, True,
                     spectral_grade="suspect", spectral_bitrate=192),
    DownloadScenario("cbr_256_genuine", False, 256, True,
                     spectral_grade="genuine"),
    DownloadScenario("cbr_192_genuine", False, 192, True,
                     spectral_grade="genuine"),
    # Brian Eno - Generative Music I (download_log 3291, 2026-04-12). Mixed
    # bitrate MP3 album: min=128 (one track), avg=290, median=320 — most
    # tracks are 320 CBR, one is a 128 outlier, so the set-of-bitrates
    # check treats the album as non-CBR. Per-track spectral detects a
    # cliff at 96kHz on several tracks; the album rolls up to ``genuine``
    # (fewer than 60% suspect) but the ``min``-across-estimates still
    # carries the 96 value. Paired with ``eno_genuine_spectral96_mp3_128``
    # this is the exact force-import the pipeline accepted.
    DownloadScenario("eno_mp3_mixed_avg_290_genuine_96", False, 128, False,
                     avg_bitrate=290,
                     is_vbr=True,
                     spectral_grade="genuine",
                     spectral_bitrate=96),
]

DL_MAP = {s.name: s for s in DOWNLOAD_SCENARIOS}


# ============================================================================
# Simulator helper (mirrors CLI logic from pipeline_cli.py cmd_quality)
# ============================================================================

def simulate(album: AlbumState, download: DownloadScenario,
             verified_lossless_target: str | None = None) -> SimResult:
    """Run full_pipeline_decision + rejection backfill."""
    # Derive existing state params (same logic as cmd_quality)
    existing_spectral_bitrate = album.spectral_bitrate
    effective_existing = compute_effective_override_bitrate(
        album.min_bitrate,
        album.spectral_bitrate,
        album.spectral_grade,
    )
    override = None
    if (effective_existing is not None and album.min_bitrate is not None
            and effective_existing != album.min_bitrate):
        override = effective_existing

    result = full_pipeline_decision(
        existing_min_bitrate=album.min_bitrate,
        existing_avg_bitrate=album.avg_bitrate,
        existing_spectral_bitrate=existing_spectral_bitrate,
        override_min_bitrate=override,
        existing_format=_derive_album_format(album),
        existing_is_cbr=album.is_cbr,
        verified_lossless=album.verified_lossless,
        verified_lossless_target=verified_lossless_target,
        target_format=album.target_format,
        **download.dl_params(),
    )

    # Simulate spectral propagation + backfill for rejections
    backfill = None
    if not result["imported"] and result["keep_searching"]:
        if not album.search_filetype_override:
            dl_spectral = download.spectral_grade
            propagated_grade = dl_spectral if dl_spectral else album.spectral_grade
            backfill = rejection_backfill_override(
                is_cbr=album.is_cbr,
                min_bitrate_kbps=album.min_bitrate,
                spectral_grade=propagated_grade,
                verified_lossless=album.verified_lossless,
            )

    # Model search_filetype_override after the full cycle.
    # This mirrors _check_quality_gate() in import_dispatch.py:
    # - accept → search_filetype_override=None (clears transient override)
    # - requeue_upgrade → search_filetype_override=upgrade tiers
    # - requeue_lossless → search_filetype_override="lossless"
    # - rejection (not imported) → preserve existing override
    # target_format (persistent user intent) is NEVER touched by the pipeline.
    gate = result["stage3_quality_gate"]
    if not result["imported"]:
        # Rejection: override stays (or backfill sets it)
        override_after = backfill if backfill else album.search_filetype_override
    elif gate == "accept":
        override_after = None  # production clears search_filetype_override on accept
    elif gate == "requeue_upgrade":
        override_after = "lossless,mp3 v0,mp3 320"
    elif gate == "requeue_lossless":
        override_after = "lossless"
    else:
        override_after = album.search_filetype_override

    return SimResult(
        imported=result["imported"],
        keep_searching=result["keep_searching"],
        denylisted=result["denylisted"],
        final_status=result["final_status"],
        stage0_spectral_gate=result["stage0_spectral_gate"],
        stage1_spectral=result["stage1_spectral"],
        stage2_import=result["stage2_import"],
        stage3_quality_gate=result["stage3_quality_gate"],
        backfill_override=backfill,
        search_filetype_override_after=override_after,
        target_format_after=album.target_format,  # always preserved
    )


# ============================================================================
# Invariant tests — properties that hold across the full matrix
# ============================================================================

class TestSimulatorInvariants(unittest.TestCase):
    """Properties that must hold for every (album, download) combination."""

    def test_fixture_counts(self):
        """Issue #32: at least 13 album states, all 16 download scenarios."""
        self.assertGreaterEqual(len(ALBUM_STATES), 13)
        self.assertGreaterEqual(len(DOWNLOAD_SCENARIOS), 16)
        # No duplicate names
        self.assertEqual(len(ALBUM_MAP), len(ALBUM_STATES))
        self.assertEqual(len(DL_MAP), len(DOWNLOAD_SCENARIOS))

    def test_stage0_gate_propagates(self):
        """Issue #93: every simulation must populate stage0_spectral_gate
        so the CLI + web UI can explain why spectral ran or didn't."""
        VALID = {"would_run", "skipped_vbr_high_avg", "skipped_flac"}
        album = ALBUM_MAP["fresh_request"]
        for dl in DOWNLOAD_SCENARIOS:
            with self.subTest(dl=dl.name):
                r = simulate(album, dl)
                self.assertIn(r.stage0_spectral_gate, VALID,
                              f"{dl.name}: stage0={r.stage0_spectral_gate}")

    def test_vbr_high_avg_skips_spectral_stage(self):
        """Genuine V0 download (avg 245) must have stage0=skipped_vbr_high_avg
        AND stage1 must not run. This locks the production semantic the CLI
        simulator must explain: 'we trust the VBR bitrate signal, no analysis'."""
        album = ALBUM_MAP["fresh_request"]
        r = simulate(album, DL_MAP["mp3_v0_240"])
        self.assertEqual(r.stage0_spectral_gate, "skipped_vbr_high_avg")
        self.assertIsNone(
            r.stage1_spectral,
            "high-avg VBR must skip spectral entirely, not model a "
            "decision on it")

    def test_vbr_low_avg_runs_spectral_stage(self):
        """Go! Team shape: VBR avg 182 + likely_transcode spectral → stage 0
        gates, stage 1 fires and rejects. The live issue #93 scenario."""
        album = ALBUM_MAP["vbr_v0_no_spectral"]
        r = simulate(album, DL_MAP["vbr_transcode_go_team_shape"])
        self.assertEqual(r.stage0_spectral_gate, "would_run")
        self.assertIn(r.stage1_spectral,
                      ("reject", "import_upgrade", "import_no_exist"),
                      "gate fired, stage 1 must have a real spectral decision")

    def test_final_status_always_set(self):
        """Every simulation must produce a definitive final_status."""
        for album in ALBUM_STATES:
            for dl in DOWNLOAD_SCENARIOS:
                with self.subTest(album=album.name, dl=dl.name):
                    r = simulate(album, dl)
                    self.assertIn(r.final_status, ("imported", "wanted"))

    def test_fresh_request_always_imports(self):
        """With nothing on disk, every download gets imported."""
        album = ALBUM_MAP["fresh_request"]
        for dl in DOWNLOAD_SCENARIOS:
            with self.subTest(dl=dl.name):
                r = simulate(album, dl)
                self.assertTrue(r.imported)

    def test_fresh_request_never_downgrades(self):
        """Nothing on disk means no downgrade possible."""
        album = ALBUM_MAP["fresh_request"]
        for dl in DOWNLOAD_SCENARIOS:
            with self.subTest(dl=dl.name):
                r = simulate(album, dl)
                self.assertNotIn(r.stage2_import,
                                 ("downgrade", "transcode_downgrade"))

    def test_verified_lossless_never_backfills(self):
        """Albums already verified lossless should never trigger backfill."""
        for album in ALBUM_STATES:
            if not album.verified_lossless:
                continue
            for dl in DOWNLOAD_SCENARIOS:
                with self.subTest(album=album.name, dl=dl.name):
                    r = simulate(album, dl)
                    self.assertIsNone(r.backfill_override)

    def test_denylist_requires_cause(self):
        """Denylisting only from: spectral reject, transcode, or requeue_upgrade."""
        for album in ALBUM_STATES:
            for dl in DOWNLOAD_SCENARIOS:
                with self.subTest(album=album.name, dl=dl.name):
                    r = simulate(album, dl)
                    if r.denylisted:
                        causes = (
                            r.stage1_spectral == "reject",
                            r.stage2_import in ("transcode_upgrade",
                                                "transcode_downgrade",
                                                "transcode_first"),
                            r.stage3_quality_gate == "requeue_upgrade",
                        )
                        self.assertTrue(any(causes),
                                        f"Denylisted without valid cause: {r}")

    def test_genuine_flac_on_fresh_is_verified_and_done(self):
        """Genuine/marginal FLAC on fresh request: imported, accepted, done."""
        album = ALBUM_MAP["fresh_request"]
        for name in ("flac_genuine_high", "flac_genuine_lofi", "flac_marginal"):
            with self.subTest(dl=name):
                r = simulate(album, DL_MAP[name])
                self.assertTrue(r.imported)
                self.assertEqual(r.final_status, "imported")
                self.assertEqual(r.stage3_quality_gate, "accept")
                self.assertFalse(r.keep_searching)
                self.assertFalse(r.denylisted)

    def test_backfill_only_on_rejection(self):
        """Backfill never computes when the download was imported."""
        for album in ALBUM_STATES:
            for dl in DOWNLOAD_SCENARIOS:
                with self.subTest(album=album.name, dl=dl.name):
                    r = simulate(album, dl)
                    if r.imported:
                        self.assertIsNone(r.backfill_override)

    def test_search_filetype_override_suppresses_backfill(self):
        """Albums with existing search_filetype_override skip backfill entirely."""
        for album in ALBUM_STATES:
            if album.search_filetype_override is None:
                continue
            for dl in DOWNLOAD_SCENARIOS:
                with self.subTest(album=album.name, dl=dl.name):
                    r = simulate(album, dl)
                    self.assertIsNone(r.backfill_override)

    def test_imported_and_searching_has_cause(self):
        """imported=True + keep_searching=True only from transcode or quality gate requeue."""
        for album in ALBUM_STATES:
            for dl in DOWNLOAD_SCENARIOS:
                with self.subTest(album=album.name, dl=dl.name):
                    r = simulate(album, dl)
                    if r.imported and r.keep_searching:
                        causes = (
                            r.stage2_import in ("transcode_upgrade",
                                                "transcode_first"),
                            r.stage3_quality_gate in ("requeue_upgrade",
                                                      "requeue_lossless"),
                        )
                        self.assertTrue(any(causes),
                                        f"Imported + searching without cause: {r}")


# ============================================================================
# Named regression tests — real bugs documented in issue #32
# ============================================================================

class TestNamedRegressions(unittest.TestCase):

    def test_stars_of_the_lid_loop(self):
        """CBR 320 genuine on disk + MP3 downgrade -> backfill fires -> flac only.

        Bug: CBR 320 genuine albums kept downloading MP3s rejected as
        downgrades. Without backfill, search_filetype_override stays NULL and the
        pipeline re-searches all tiers forever.
        """
        album = ALBUM_MAP["cbr_320_genuine"]
        r = simulate(album, DL_MAP["mp3_v0_240"])

        self.assertFalse(r.imported)
        self.assertEqual(r.stage2_import, "downgrade")
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.backfill_override, "lossless",
                         "Genuine CBR 320 must backfill to flac-only on rejection")

        # Verify the lossless override narrows search tiers
        tiers, allow_catch_all = search_tiers("lossless", [])
        self.assertEqual(tiers, ["lossless"])
        self.assertFalse(allow_catch_all)

    def test_springsteen_genuine_but_96kbps(self):
        """CBR 320 genuine + spectral_bitrate=96 ignores the stale low estimate.

        Contradictory state: genuine spectral grade but 96kbps spectral bitrate.
        Genuine grade means the spectral bitrate is not a transcode-quality
        estimate, so comparisons use the 320kbps container bitrate.
        """
        album = ALBUM_MAP["cbr_320_genuine_spectral96"]

        # Genuine FLAC imports (verified_lossless always wins)
        r1 = simulate(album, DL_MAP["flac_genuine_high"])
        self.assertTrue(r1.imported)
        self.assertEqual(r1.final_status, "imported")
        self.assertFalse(r1.keep_searching)

        # MP3 V0 240 is still lower than the 320kbps container bitrate.
        r2 = simulate(album, DL_MAP["mp3_v0_240"])
        self.assertFalse(r2.imported)
        self.assertEqual(r2.stage2_import, "downgrade")

        # Low V0 205 is also a downgrade; the stale 96kbps estimate is ignored.
        r3 = simulate(album, DL_MAP["mp3_v0_low_205"])
        self.assertFalse(r3.imported)
        self.assertEqual(r3.stage2_import, "downgrade")

    def test_scientists_no_spectral_loop(self):
        """CBR 320 no spectral + CBR 320 genuine download -> propagation -> backfill.

        Bug: CBR 320 albums with no spectral data looped forever:
        1. CBR 320 downloads rejected as downgrade (320 <= 320)
        2. No spectral on disk -> backfill can't fire (needs genuine grade)
        3. Download's spectral grade must propagate to break the loop
        """
        album = ALBUM_MAP["cbr_320_no_spectral"]
        r = simulate(album, DL_MAP["cbr_320_genuine"])

        self.assertFalse(r.imported)
        self.assertEqual(r.stage2_import, "downgrade")
        self.assertEqual(r.backfill_override, "lossless",
                         "Download's genuine spectral must propagate to break CBR loop")

    def test_scientists_suspect_download_no_backfill(self):
        """CBR 320 no spectral + suspect download -> no backfill, keep all tiers.

        Contrast to genuine: suspect spectral doesn't meet backfill requirements.
        The album needs better data before narrowing search.
        """
        album = ALBUM_MAP["cbr_320_no_spectral"]
        r = simulate(album, DL_MAP["cbr_320_suspect_128"])

        self.assertFalse(r.imported)
        self.assertTrue(r.keep_searching)
        self.assertIsNone(r.backfill_override,
                          "Suspect spectral must NOT trigger backfill")

    def test_unter_null_failure_epiphany_vbr_loop_break(self):
        """VBR existing + CBR 152 download (no spectral on new) -> stage2 downgrade.

        Live reproduction: Unter Null - The Failure Epiphany (req 1749).
        Beets album with per-track bitrates 152-310 kbps (avg 225), spectral
        flagged likely_transcode at ~96 kbps (false positive on genre).

        In production on 2026-04-21 every Soulseek source delivered a
        152 kbps MP3 transcode and each one was marked ``outcome=success``
        in download_log — meaning preimport's ``_needs_spectral_check``
        returned False (possibly a filetype/VBR-flag edge case in slskd
        metadata) so stage 1's spectral check never fired. The comparison
        that decided those imports was stage 2's ``import_quality_decision``
        under the AVG metric.

        Old behavior: override clobbered existing.avg from 225 to 96, so
        every 152-avg download "beat" the clobbered existing at stage 2,
        looped through every source, wasted bandwidth, wrongly denylisted.

        New behavior: VBR existing keeps its real 225 avg. The 152-avg
        download reads as ACCEPTABLE vs the existing's EXCELLENT → stage 2
        returns ``downgrade`` and nothing imports. The loop stops.

        Test uses ``cbr_152_no_spectral`` (download-side spectral omitted)
        so stage 1 does not short-circuit — we want stage 2 to make the
        call. The spectral-reject path on stage 1 is a separate safety
        net covered by its own tests; this one pins the stage-2 rank
        comparison under the AVG policy for a VBR existing.
        """
        album = ALBUM_MAP["unter_null_failure_epiphany"]
        r = simulate(album, DL_MAP["cbr_152_no_spectral"])

        self.assertFalse(
            r.imported,
            "CBR 152 download must not win stage-2 compare against a VBR "
            "225-avg existing — the clobber bug that drove 1749's loop")
        self.assertEqual(
            r.stage2_import, "downgrade",
            "Stage 2 must return 'downgrade' — pins the AVG-metric "
            "rank comparison with VBR existing preserving its real avg")

    def test_eno_generative_music_shared_spectral_floor(self):
        """Shared 96k spectral floor on both sides — not an upgrade.

        Live reproduction: Brian Eno - Generative Music I (req 1486),
        force-import ``download_log.id=3291`` on 2026-04-12.

        State at force-import time:
          existing on disk (from prior import 3284): MP3, min/avg/median=128,
            is_cbr=False, spectral_grade=genuine, spectral_bitrate=96.
          new download: MP3, min=128, avg=290, median≈320, is_cbr=False,
            spectral_grade=genuine, spectral_bitrate=96.

        Both sides carry the same 96k spectral floor with the same grade.
        ``compare_quality`` currently uses the configured bitrate metric
        (AVG by default) and classifies new (290 → TRANSPARENT rank) over
        existing (128 → ACCEPTABLE rank) → verdict ``better`` → stage 2
        returns ``import``. The force-import proceeded on an album whose
        underlying audio is the same 96k material, per the spectral signal
        both sides agree on.

        The spectral clamp that would catch this lives only in
        ``gate_rank`` (post-import) and is gated on
        ``SPECTRAL_TRANSCODE_GRADES`` (``suspect``/``likely_transcode``).
        Both sides here are ``genuine`` → clamp never fires → 290-vs-128
        comparison wins.

        Expected after fix: when new and existing both carry a
        ``spectral_bitrate`` at the same value, the comparison must treat
        them as equivalent (i.e. not ``import``) — no perceptual upgrade
        is possible when both sources agree on the audio floor.
        """
        album = ALBUM_MAP["eno_genuine_spectral96_mp3_128"]
        r = simulate(album, DL_MAP["eno_mp3_mixed_avg_290_genuine_96"])

        self.assertFalse(
            r.imported,
            "New download whose spectral floor matches existing's spectral "
            "floor (both 96kbps) must not import — container-avg comparison "
            "is misleading when spectral agrees on the underlying quality.")
        self.assertNotEqual(
            r.stage2_import, "import",
            "Stage 2 must not return 'import' when new.spectral_bitrate == "
            "existing.spectral_bitrate — no upgrade is possible at the same "
            "spectral floor.")

    def test_unter_null_failure_epiphany_vbr_real_upgrade(self):
        """VBR existing (1749 shape) + genuine V0 upgrade still imports.

        Companion to the loop-break test: the fix must not block real
        upgrades. A genuine V0 download with avg ~245 kbps (mp3_v0_240)
        is a legitimate upgrade for a VBR 225-avg existing, regardless of
        the spectral override on min. Confirms fake-CBR-320 protection
        is untouched and VBR-avg-preservation only affects the specific
        same-or-lower-quality downgrade case.
        """
        album = ALBUM_MAP["unter_null_failure_epiphany"]
        r = simulate(album, DL_MAP["mp3_v0_240"])

        self.assertTrue(
            r.imported,
            "Genuine V0 upgrade (245 avg) must still beat VBR 225-avg existing")
        self.assertEqual(r.stage2_import, "import")

    def test_lofi_verified_lossless_accepted(self):
        """207kbps verified lossless -> quality gate accepts despite < 210 threshold.

        Bug: Quality gate re-queued 207kbps albums for upgrade even though the
        source was verified genuine FLAC. Lo-fi recordings legitimately produce
        low V0 bitrates.
        """
        album = ALBUM_MAP["fresh_request"]
        r = simulate(album, DL_MAP["flac_genuine_lofi"])

        self.assertTrue(r.imported)
        self.assertEqual(r.stage3_quality_gate, "accept",
                         "207kbps from verified genuine FLAC must be accepted")
        self.assertEqual(r.final_status, "imported")
        self.assertFalse(r.keep_searching,
                         "Verified lossless lo-fi must not trigger further searching")

    def test_deloris_flac_override_cleared_on_accept(self):
        """target_format="flac" survives quality gate accept.

        Bug (Deloris - The Pointless Gift): User sets target_format="flac"
        (intent: download FLAC sources only). Pipeline downloads FLAC, converts
        to V0/Opus, quality gate accepts → search_filetype_override cleared to NULL.
        target_format is preserved because the pipeline never touches it.

        After the split: search_filetype_override is transient (cleared on accept),
        target_format is persistent (user intent, survives all pipeline actions).
        """
        # Album with user-set flac intent: "I want FLAC sources"
        album = AlbumState("cbr_320_genuine_user_flac", 320, True,
                           "genuine", None, False, "lossless", target_format="flac")
        dl = DL_MAP["flac_genuine_raw"]
        r = simulate(album, dl)

        # FLAC imported successfully, quality gate accepts
        self.assertTrue(r.imported)
        self.assertEqual(r.stage3_quality_gate, "accept")

        # search_filetype_override is correctly cleared on accept (transient)
        self.assertIsNone(r.search_filetype_override_after,
            "search_filetype_override should be cleared on accept")
        # target_format survives — user intent is preserved
        self.assertEqual(r.target_format_after, "flac",
            "target_format='flac' should survive quality gate accept — "
            "it represents user intent, not a system-set upgrade tier")

    def test_deloris_flac_kept_on_disk_not_converted(self):
        """target_format="flac" → FLAC stays on disk, no V0/Opus conversion.

        The full pipeline decision models the skip-conversion path: raw FLAC
        bitrate (~900kbps) is used for quality comparison, genuine FLAC is
        marked verified_lossless, quality gate accepts at high bitrate.

        Without target_format support in full_pipeline_decision, this would
        go through the conversion path (converted_count=12, post_conversion=245)
        and the user would get V0/Opus instead of FLAC.
        """
        # Album where user wants FLAC on disk
        album = AlbumState("fresh_wants_flac", None, False,
                           None, None, False, None, target_format="flac")
        dl = DL_MAP["flac_genuine_raw"]
        r = simulate(album, dl)

        self.assertTrue(r.imported)
        self.assertEqual(r.final_status, "imported")
        self.assertEqual(r.stage3_quality_gate, "accept")
        self.assertFalse(r.keep_searching)
        self.assertFalse(r.denylisted)

    def test_deloris_flac_on_disk_beats_existing_v0(self):
        """FLAC at ~900kbps on disk beats existing V0 at 245kbps."""
        album = AlbumState("v0_wants_flac", 245, False,
                           "genuine", None, True, None, target_format="flac")
        dl = DL_MAP["flac_genuine_raw"]
        r = simulate(album, dl)

        self.assertTrue(r.imported)
        self.assertEqual(r.stage2_import, "import")
        self.assertEqual(r.stage3_quality_gate, "accept")

    def test_deloris_flac_vs_flac_same_bitrate_downgrades(self):
        """FLAC on disk vs same FLAC → downgrade (not an upgrade)."""
        album = AlbumState("flac_on_disk", 900, False,
                           "genuine", None, True, None, target_format="flac")
        dl = DL_MAP["flac_genuine_raw"]
        r = simulate(album, dl)

        self.assertFalse(r.imported)
        self.assertEqual(r.stage2_import, "downgrade")

    def test_upgrade_button_unanalysed_album(self):
        """CBR 320 no spectral: first genuine download breaks the loop.

        User hits "Upgrade" on an unanalysed CBR 320 album. The first download
        with genuine spectral grade (even if rejected as downgrade) propagates
        its spectral -> backfill fires -> narrows to flac-only.
        """
        album = ALBUM_MAP["cbr_320_no_spectral"]
        for dl_name in ("cbr_320_genuine", "cbr_256_genuine", "cbr_192_genuine"):
            with self.subTest(dl=dl_name):
                r = simulate(album, DL_MAP[dl_name])
                self.assertFalse(r.imported)
                self.assertEqual(r.backfill_override, "lossless",
                                 f"{dl_name} must propagate genuine spectral -> backfill")

    # ------------------------------------------------------------------
    # Issue #60 — codec-aware cross-codec regressions
    # ------------------------------------------------------------------

    def test_opus_128_equivalent_to_mp3_v0(self):
        """Opus 128 reads as equivalent to MP3 V0 under the rank model.

        Simulates an existing MP3 V0 (245kbps) album, then dispatches a
        FLAC→Opus 128 verified lossless conversion. Under the old raw-
        bitrate model, Opus 128 (~120kbps) would lose to MP3 V0 (245) as a
        downgrade. Under the rank model, both classify TRANSPARENT and
        compare as equivalent, so the verified_lossless preference imports.
        """
        from lib.quality import (
            AudioQualityMeasurement, compare_quality, QualityRankConfig)
        cfg = QualityRankConfig.defaults()
        opus_128 = AudioQualityMeasurement(
            format="opus 128", avg_bitrate_kbps=130, verified_lossless=True)
        mp3_v0 = AudioQualityMeasurement(
            format="mp3 v0", avg_bitrate_kbps=245)
        # compare_quality uses label-based rank → both TRANSPARENT
        self.assertEqual(compare_quality(opus_128, mp3_v0, cfg), "equivalent")
        # import_quality_decision honors the verified_lossless preference
        from lib.quality import import_quality_decision
        self.assertEqual(
            import_quality_decision(opus_128, mp3_v0, cfg=cfg), "import")

    def test_opus_64_cannot_replace_mp3_v0(self):
        """Too-low verified_lossless target Opus 64 is blocked by rank floor.

        Issue #60 guardrail. Opus 64 from a verified FLAC lands at GOOD rank
        (below EXCELLENT), while existing MP3 V0 is TRANSPARENT. Verdict is
        "worse", and the verified_lossless bypass is tier-gated so the
        import is rejected.
        """
        from lib.quality import (
            AudioQualityMeasurement, import_quality_decision,
            QualityRankConfig)
        cfg = QualityRankConfig.defaults()
        opus_64 = AudioQualityMeasurement(
            format="opus 64", avg_bitrate_kbps=64, verified_lossless=True)
        mp3_v0 = AudioQualityMeasurement(
            format="mp3 v0", avg_bitrate_kbps=245)
        self.assertEqual(
            import_quality_decision(opus_64, mp3_v0, cfg=cfg), "downgrade")

    def test_flac_to_opus_128_replaces_cbr_320(self):
        """Verified Opus 128 replaces unverified CBR 320 (codec parity)."""
        from lib.quality import (
            AudioQualityMeasurement, import_quality_decision,
            QualityRankConfig)
        cfg = QualityRankConfig.defaults()
        opus_128 = AudioQualityMeasurement(
            format="opus 128", avg_bitrate_kbps=130, verified_lossless=True)
        cbr_320 = AudioQualityMeasurement(
            format="mp3 320", avg_bitrate_kbps=320, is_cbr=True)
        self.assertEqual(
            import_quality_decision(opus_128, cbr_320, cfg=cfg), "import")

    def test_lofi_v0_still_imports(self):
        """Lo-fi V0 (207kbps) imports under the rank model via the label.

        Issue #60: ``verified_lossless=True`` used to be a blanket bypass
        that let lo-fi V0 through the gate. Under the rank model the same
        case works because "mp3 v0" classifies as TRANSPARENT via
        cfg.mp3_vbr_levels[0] regardless of bitrate.
        """
        from lib.quality import (
            AudioQualityMeasurement, quality_gate_decision, QualityRankConfig)
        cfg = QualityRankConfig.defaults()
        current = AudioQualityMeasurement(
            format="mp3 v0", min_bitrate_kbps=207, avg_bitrate_kbps=207,
            verified_lossless=True)
        self.assertEqual(quality_gate_decision(current, cfg=cfg), "accept")

    def test_gate_min_rank_good_accepts_lower_cfg(self):
        """Custom cfg.gate_min_rank=GOOD lets 180kbps VBR pass the gate.

        Locks the runtime-config threading through the gate. Under default
        EXCELLENT the same measurement requeues; under GOOD it accepts.
        """
        from lib.quality import (
            AudioQualityMeasurement, quality_gate_decision,
            QualityRank, QualityRankConfig)
        default_cfg = QualityRankConfig.defaults()
        lenient_cfg = QualityRankConfig(gate_min_rank=QualityRank.GOOD)
        current = AudioQualityMeasurement(
            format="MP3", min_bitrate_kbps=180, avg_bitrate_kbps=180)
        self.assertEqual(
            quality_gate_decision(current, cfg=default_cfg), "requeue_upgrade")
        self.assertEqual(
            quality_gate_decision(current, cfg=lenient_cfg), "accept")


# ============================================================================
# Fresh request matrix — exact outcomes for all 16 scenarios
# ============================================================================

class TestFreshRequestOutcomes(unittest.TestCase):
    """Exact expected outcomes for every download on a fresh request."""

    def _sim(self, dl_name):
        return simulate(ALBUM_MAP["fresh_request"], DL_MAP[dl_name])

    # --- FLAC downloads ---

    def test_flac_genuine_high(self):
        r = self._sim("flac_genuine_high")
        self.assertTrue(r.imported)
        self.assertFalse(r.denylisted)
        self.assertFalse(r.keep_searching)
        self.assertEqual(r.final_status, "imported")
        self.assertEqual(r.stage3_quality_gate, "accept")

    def test_flac_genuine_lofi(self):
        r = self._sim("flac_genuine_lofi")
        self.assertTrue(r.imported)
        self.assertFalse(r.keep_searching)
        self.assertEqual(r.final_status, "imported")
        self.assertEqual(r.stage3_quality_gate, "accept")

    def test_flac_marginal(self):
        r = self._sim("flac_marginal")
        self.assertTrue(r.imported)
        self.assertFalse(r.keep_searching)
        self.assertEqual(r.final_status, "imported")
        self.assertEqual(r.stage3_quality_gate, "accept")

    def test_flac_suspect_190(self):
        r = self._sim("flac_suspect_190")
        self.assertTrue(r.imported)
        self.assertTrue(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage2_import, "transcode_first")
        self.assertEqual(r.stage3_quality_gate, "requeue_upgrade")

    def test_flac_suspect_245(self):
        r = self._sim("flac_suspect_245")
        self.assertTrue(r.imported)
        self.assertTrue(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "imported")
        self.assertEqual(r.stage2_import, "transcode_first")
        self.assertEqual(r.stage3_quality_gate, "accept")

    # --- MP3 VBR ---

    def test_mp3_v0_240(self):
        r = self._sim("mp3_v0_240")
        self.assertTrue(r.imported)
        self.assertFalse(r.denylisted)
        self.assertFalse(r.keep_searching)
        self.assertEqual(r.final_status, "imported")
        self.assertEqual(r.stage3_quality_gate, "accept")

    def test_mp3_v0_low_205(self):
        r = self._sim("mp3_v0_low_205")
        self.assertTrue(r.imported)
        self.assertTrue(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_upgrade")

    def test_mp3_v2_190(self):
        r = self._sim("mp3_v2_190")
        self.assertTrue(r.imported)
        self.assertTrue(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_upgrade")

    # --- CBR no spectral ---

    def test_cbr_320_no_spectral(self):
        r = self._sim("cbr_320_no_spectral")
        self.assertTrue(r.imported)
        self.assertFalse(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_lossless")

    def test_cbr_256_no_spectral(self):
        r = self._sim("cbr_256_no_spectral")
        self.assertTrue(r.imported)
        self.assertFalse(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_lossless")

    def test_cbr_192_no_spectral(self):
        r = self._sim("cbr_192_no_spectral")
        self.assertTrue(r.imported)
        self.assertTrue(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_upgrade")

    # --- CBR with spectral ---

    def test_cbr_320_genuine(self):
        r = self._sim("cbr_320_genuine")
        self.assertTrue(r.imported)
        self.assertFalse(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_lossless")

    def test_cbr_320_suspect_128(self):
        r = self._sim("cbr_320_suspect_128")
        self.assertTrue(r.imported)
        self.assertTrue(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_upgrade")

    def test_cbr_320_suspect_192(self):
        r = self._sim("cbr_320_suspect_192")
        self.assertTrue(r.imported)
        self.assertTrue(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_upgrade")

    def test_cbr_256_genuine(self):
        r = self._sim("cbr_256_genuine")
        self.assertTrue(r.imported)
        self.assertFalse(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_lossless")

    def test_cbr_192_genuine(self):
        r = self._sim("cbr_192_genuine")
        self.assertTrue(r.imported)
        self.assertTrue(r.denylisted)
        self.assertTrue(r.keep_searching)
        self.assertEqual(r.final_status, "wanted")
        self.assertEqual(r.stage3_quality_gate, "requeue_upgrade")


# ============================================================================
# CBR 320 no-spectral matrix — the state most prone to loops
# ============================================================================

class TestCBR320NoSpectralMatrix(unittest.TestCase):
    """CBR 320 with no spectral data: only genuine FLAC can escape."""

    def _sim(self, dl_name):
        return simulate(ALBUM_MAP["cbr_320_no_spectral"], DL_MAP[dl_name])

    def test_genuine_flac_imports_as_verified(self):
        """Genuine FLAC -> verified lossless import, done."""
        for name in ("flac_genuine_high", "flac_genuine_lofi", "flac_marginal"):
            with self.subTest(dl=name):
                r = self._sim(name)
                self.assertTrue(r.imported)
                self.assertEqual(r.final_status, "imported")
                self.assertEqual(r.stage3_quality_gate, "accept")
                self.assertFalse(r.keep_searching)

    def test_suspect_flac_rejected_as_transcode_downgrade(self):
        """Suspect FLAC: transcode detected, post-conversion bitrate < 320 -> downgrade."""
        for name in ("flac_suspect_190", "flac_suspect_245"):
            with self.subTest(dl=name):
                r = self._sim(name)
                self.assertFalse(r.imported)
                self.assertEqual(r.stage2_import, "transcode_downgrade")
                self.assertTrue(r.denylisted)
                self.assertTrue(r.keep_searching)

    def test_all_mp3_vbr_rejected_as_downgrade(self):
        """VBR MP3 (240, 205, 190) all < 320 -> downgrade."""
        for name in ("mp3_v0_240", "mp3_v0_low_205", "mp3_v2_190"):
            with self.subTest(dl=name):
                r = self._sim(name)
                self.assertFalse(r.imported)
                self.assertEqual(r.stage2_import, "downgrade")

    def test_all_cbr_rejected_as_downgrade(self):
        """All CBR downloads <= 320 -> downgrade (even equal bitrate)."""
        cbr_downloads = [
            "cbr_320_no_spectral", "cbr_320_genuine",
            "cbr_320_suspect_128", "cbr_320_suspect_192",
            "cbr_256_no_spectral", "cbr_256_genuine",
            "cbr_192_no_spectral", "cbr_192_genuine",
        ]
        for name in cbr_downloads:
            with self.subTest(dl=name):
                r = self._sim(name)
                self.assertFalse(r.imported)
                self.assertEqual(r.stage2_import, "downgrade")


# ============================================================================
# Verified lossless matrix — stable done state
# ============================================================================

class TestVerifiedLosslessMatrix(unittest.TestCase):
    """Verified lossless albums: quality gate accepted, most downloads downgrade."""

    def _sim(self, album_name, dl_name):
        return simulate(ALBUM_MAP[album_name], DL_MAP[dl_name])

    def test_genuine_flac_reimports_verified(self):
        """Genuine FLAC always imports over verified lossless (re-verified)."""
        for album_name in ("verified_lossless_v0", "verified_lossless_lofi"):
            with self.subTest(album=album_name):
                r = self._sim(album_name, "flac_genuine_high")
                self.assertTrue(r.imported)
                self.assertEqual(r.final_status, "imported")
                self.assertFalse(r.keep_searching)

    def test_mp3_lower_than_existing_downgrades(self):
        """MP3 V2 190 < 245 existing -> downgrade, no backfill."""
        r = self._sim("verified_lossless_v0", "mp3_v2_190")
        self.assertFalse(r.imported)
        self.assertEqual(r.stage2_import, "downgrade")
        self.assertIsNone(r.backfill_override)

    def test_mp3_higher_than_lofi_imports(self):
        """MP3 V0 240 > 207 existing -> import, accepted."""
        r = self._sim("verified_lossless_lofi", "mp3_v0_240")
        self.assertTrue(r.imported)
        self.assertEqual(r.stage3_quality_gate, "accept")


# ============================================================================
# Backfill propagation tests
# ============================================================================

class TestBackfillPropagation(unittest.TestCase):
    """Spectral propagation from downloads into backfill logic."""

    def test_genuine_download_propagates_to_no_spectral_album(self):
        """Download's genuine grade is used when album has no spectral data."""
        album = ALBUM_MAP["cbr_320_no_spectral"]
        for dl_name in ("cbr_320_genuine", "cbr_256_genuine", "cbr_192_genuine"):
            with self.subTest(dl=dl_name):
                r = simulate(album, DL_MAP[dl_name])
                self.assertEqual(r.backfill_override, "lossless")

    def test_suspect_download_does_not_propagate_backfill(self):
        """Suspect spectral grade does not trigger backfill."""
        album = ALBUM_MAP["cbr_320_no_spectral"]
        for dl_name in ("cbr_320_suspect_128", "cbr_320_suspect_192"):
            with self.subTest(dl=dl_name):
                r = simulate(album, DL_MAP[dl_name])
                self.assertIsNone(r.backfill_override)

    def test_no_spectral_download_uses_album_grade(self):
        """Downloads without spectral fall back to album's grade for backfill."""
        album = ALBUM_MAP["cbr_320_genuine"]
        r = simulate(album, DL_MAP["mp3_v0_240"])
        self.assertEqual(r.backfill_override, "lossless",
                         "Album's genuine grade used when download has no spectral")

    def test_no_spectral_either_side_no_backfill(self):
        """No spectral on album or download -> backfill can't fire."""
        album = ALBUM_MAP["cbr_320_no_spectral"]
        no_spectral_downloads = [
            "mp3_v0_240", "mp3_v0_low_205", "mp3_v2_190",
            "cbr_320_no_spectral", "cbr_256_no_spectral", "cbr_192_no_spectral",
        ]
        for dl_name in no_spectral_downloads:
            with self.subTest(dl=dl_name):
                r = simulate(album, DL_MAP[dl_name])
                # All rejected as downgrade on CBR 320
                self.assertFalse(r.imported)
                self.assertIsNone(r.backfill_override,
                                  "No spectral anywhere -> no backfill possible")

    def test_low_bitrate_album_no_backfill(self):
        """Album with genuine spectral but bitrate < 210 -> no backfill."""
        album = ALBUM_MAP["cbr_192_genuine"]
        # CBR 192 genuine rejected by downloads at or below 192
        for dl_name in ("mp3_v2_190", "cbr_192_no_spectral"):
            with self.subTest(dl=dl_name):
                r = simulate(album, DL_MAP[dl_name])
                if not r.imported and r.keep_searching:
                    self.assertIsNone(r.backfill_override,
                                      "192 < 210 -> backfill should not fire")

    def test_existing_override_suppresses_backfill(self):
        """Albums with search_filetype_override already set skip backfill entirely."""
        album = ALBUM_MAP["cbr_320_genuine_flac_override"]
        for dl in DOWNLOAD_SCENARIOS:
            with self.subTest(dl=dl.name):
                r = simulate(album, dl)
                self.assertIsNone(r.backfill_override)

    def test_spectral_propagation_essential_for_loop_breaking(self):
        """Prove that spectral propagation is required to break the CBR loop.

        Without propagation (using only the album's spectral grade), the
        cbr_320_no_spectral album would never backfill — its spectral grade
        is None, which doesn't meet backfill requirements.
        """
        album = ALBUM_MAP["cbr_320_no_spectral"]
        dl = DL_MAP["cbr_320_genuine"]

        # WITHOUT propagation: backfill uses album's spectral grade (None)
        backfill_without = rejection_backfill_override(
            is_cbr=album.is_cbr,
            min_bitrate_kbps=album.min_bitrate,
            spectral_grade=album.spectral_grade,  # None
            verified_lossless=album.verified_lossless,
        )
        self.assertIsNone(backfill_without,
                          "Without propagation, backfill can't fire (grade is None)")

        # WITH propagation (current behavior): uses download's genuine grade
        r = simulate(album, dl)
        self.assertEqual(r.backfill_override, "lossless",
                         "With propagation, download's genuine grade enables backfill")


if __name__ == "__main__":
    unittest.main()
