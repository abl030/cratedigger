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

    def dl_params(self) -> dict:
        """Download-side kwargs for full_pipeline_decision()."""
        return {
            "is_flac": self.is_flac,
            "min_bitrate": self.min_bitrate,
            "is_cbr": self.is_cbr,
            "spectral_grade": self.spectral_grade,
            "spectral_bitrate": self.spectral_bitrate,
            "converted_count": self.converted_count,
            "post_conversion_min_bitrate": self.post_conversion_min_bitrate,
        }


@dataclass(frozen=True)
class SimResult:
    """Full outcome of a simulator run."""
    imported: bool
    keep_searching: bool
    denylisted: bool
    final_status: str | None
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
    # MP3 VBR
    DownloadScenario("mp3_v0_240", False, 240, False),
    DownloadScenario("mp3_v0_low_205", False, 205, False),
    DownloadScenario("mp3_v2_190", False, 190, False),
    # CBR no spectral
    DownloadScenario("cbr_320_no_spectral", False, 320, True),
    DownloadScenario("cbr_256_no_spectral", False, 256, True),
    DownloadScenario("cbr_192_no_spectral", False, 192, True),
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
]

DL_MAP = {s.name: s for s in DOWNLOAD_SCENARIOS}


# ============================================================================
# Simulator helper (mirrors CLI logic from pipeline_cli.py cmd_quality)
# ============================================================================

def simulate(album: AlbumState, download: DownloadScenario,
             verified_lossless_target: str | None = None) -> SimResult:
    """Run full_pipeline_decision + rejection backfill."""
    # Derive existing state params (same logic as cmd_quality)
    existing_min_bitrate = album.min_bitrate
    existing_spectral_bitrate = album.spectral_bitrate
    override = None
    if (album.spectral_bitrate is not None and album.min_bitrate is not None
            and album.spectral_bitrate < album.min_bitrate):
        override = album.spectral_bitrate
        existing_min_bitrate = override

    result = full_pipeline_decision(
        existing_min_bitrate=existing_min_bitrate,
        existing_spectral_bitrate=existing_spectral_bitrate,
        override_min_bitrate=override,
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
        """CBR 320 genuine + spectral_bitrate=96 -> effective existing is 96kbps.

        Contradictory state: genuine spectral grade but 96kbps spectral bitrate.
        The spectral truth (96kbps) is used as effective existing, so almost any
        download is an upgrade.
        """
        album = ALBUM_MAP["cbr_320_genuine_spectral96"]

        # Genuine FLAC imports (verified_lossless always wins)
        r1 = simulate(album, DL_MAP["flac_genuine_high"])
        self.assertTrue(r1.imported)
        self.assertEqual(r1.final_status, "imported")
        self.assertFalse(r1.keep_searching)

        # MP3 V0 240 imports (240 > 96 effective)
        r2 = simulate(album, DL_MAP["mp3_v0_240"])
        self.assertTrue(r2.imported)
        self.assertEqual(r2.stage2_import, "import")

        # Even low V0 205 imports (205 > 96 effective)
        r3 = simulate(album, DL_MAP["mp3_v0_low_205"])
        self.assertTrue(r3.imported)

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
