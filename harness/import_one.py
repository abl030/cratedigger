#!/usr/bin/env python3
"""One-shot beets import for a single album with a known MBID.

Designed for the pipeline DB auto-import path (source='request').
Pre-flight checks beets DB, converts FLAC→V0, imports via harness,
post-flight verifies exact MBID in beets DB.

Usage:
    python3 import_one.py <album_path> <mb_release_id> [--request-id N] [--dry-run]

Exit codes:
    0 = imported (or already in beets)
    1 = FLAC conversion failed
    2 = beets import failed (harness error, post-flight verification failed)
    3 = album path not found
    4 = MBID not found in beets candidates
    5 = quality downgrade (new files worse than existing)
    6 = transcode detected — may or may not have imported:
        - If upgrade over existing: imported, but denylist user + keep searching
        - If not an upgrade: not imported, denylist user + keep searching
"""

import argparse
import json
import os
import select
import signal
import statistics
import subprocess
import sys
import shutil
import tempfile
from dataclasses import dataclass
from typing import NoReturn

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _bootstrap_import_paths() -> None:
    """Ensure standalone harness runs can import lib.* via the repo root."""
    if ROOT_DIR not in sys.path:
        sys.path.insert(0, ROOT_DIR)


_bootstrap_import_paths()

from lib.beets_db import AlbumInfo, BeetsDB
from lib.permissions import fix_library_modes, reset_umask
from lib.release_identity import ReleaseIdentity
from lib.util import beets_subprocess_env
from lib.quality import (AUDIO_EXTENSIONS_DOTTED as AUDIO_EXTENSIONS,
                         AudioQualityMeasurement, DuplicateRemoveCandidate,
                         DuplicateRemoveGuardInfo, ImportResult,
                         PostflightInfo, QualityRankConfig,
                         MeasuredImportDecisionInput,
                         ProvisionalLosslessDecisionInput,
                         V0_PROBE_LOSSLESS_SOURCE,
                         V0_PROBE_NATIVE_LOSSY_RESEARCH,
                         V0ProbeEvidence,
                         build_existing_quality_measurement,
                         comparison_format_hint, determine_verified_lossless,
                         measured_import_decision,
                         provisional_lossless_decision, transcode_detection,
                         v0_probe_overrides_spectral)
HARNESS = os.path.join(os.path.dirname(__file__), "..", "harness", "run_beets_harness.sh")
HARNESS_TIMEOUT = 300
IMPORT_TIMEOUT = 1800
MAX_DISTANCE = 0.5
DUPLICATE_REMOVE_GUARD_EXIT_CODE = 7
_current_result: ImportResult | None = None
_preview_temp_root: str | None = None

# Rank config for BeetsDB.get_album_info() mixed-format reduction + (commit 5)
# quality_rank() / compare_quality() / quality_gate_decision(). main() replaces
# this with the deserialized --quality-rank-config argv blob passed by
# lib.import_dispatch.dispatch_import_core. Missing or malformed argv falls
# back to the hardcoded defaults.
_rank_cfg: QualityRankConfig = QualityRankConfig.defaults()


def _find_target_candidate(candidates: list, target_mbid) -> int | None:
    """Return the index of the candidate whose `album_id` matches the
    target, or None. str() on both sides — beets' Discogs plugin emits
    int album_ids while target_mbid is the str DB column. Same int-vs-str
    trap as lib/beets.py::beets_validate (PR #98).
    """
    target = str(target_mbid)
    for i, c in enumerate(candidates):
        if str(c.get("album_id", "")) == target:
            return i
    return None


def _int_or_none(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value)
    return None


def _duplicate_candidates_from_message(msg: dict) -> list[DuplicateRemoveCandidate]:
    """Normalize a harness ``resolve_duplicate`` message into typed candidates."""
    raw_candidates = msg.get("duplicate_candidates")
    if isinstance(raw_candidates, list):
        candidates: list[DuplicateRemoveCandidate] = []
        for raw in raw_candidates:
            if not isinstance(raw, dict):
                continue
            candidates.append(DuplicateRemoveCandidate(
                beets_album_id=_int_or_none(raw.get("beets_album_id")),
                mb_albumid=str(raw.get("mb_albumid") or ""),
                discogs_albumid=str(raw.get("discogs_albumid") or ""),
                album_path=str(raw.get("album_path") or ""),
                item_count=_int_or_none(raw.get("item_count")) or 0,
                albumartist=str(raw.get("albumartist") or ""),
                album=str(raw.get("album") or ""),
            ))
        return candidates

    # Backward-compatible fallback for older harness messages and existing tests.
    dup_mbids = msg.get("duplicate_mbids", [])
    dup_album_ids = msg.get("duplicate_album_ids", [])
    count = msg.get("duplicate_count")
    if not isinstance(dup_mbids, list):
        dup_mbids = []
    if not isinstance(dup_album_ids, list):
        dup_album_ids = []
    max_len = max(
        len(dup_mbids),
        len(dup_album_ids),
        count if isinstance(count, int) else 0,
    )
    return [
        DuplicateRemoveCandidate(
            beets_album_id=(
                _int_or_none(dup_album_ids[idx])
                if idx < len(dup_album_ids) else None
            ),
            mb_albumid=str(dup_mbids[idx] or "") if idx < len(dup_mbids) else "",
        )
        for idx in range(max_len)
    ]


def _duplicate_remove_guard_failure(
    *,
    target_release_id: str,
    candidates: list[DuplicateRemoveCandidate],
) -> DuplicateRemoveGuardInfo | None:
    """Return guard failure details, or None when Beets may remove."""
    target_identity = ReleaseIdentity.from_id(target_release_id)
    target_source = target_identity.source if target_identity else ""
    normalized_target = target_identity.release_id if target_identity else str(target_release_id or "")

    def _info(reason: str, message: str) -> DuplicateRemoveGuardInfo:
        return DuplicateRemoveGuardInfo(
            reason=reason,
            target_source=target_source,
            target_release_id=normalized_target,
            duplicate_count=len(candidates),
            candidates=candidates,
            message=message,
        )

    if target_identity is None:
        return _info(
            "target_identity_unknown",
            f"target release id {target_release_id!r} is not an exact MB or Discogs id",
        )
    if len(candidates) != 1:
        return _info(
            "duplicate_count_not_one",
            f"beets reported {len(candidates)} duplicate albums; expected exactly 1",
        )

    candidate = candidates[0]
    candidate_identity = ReleaseIdentity.from_fields(
        candidate.mb_albumid,
        candidate.discogs_albumid,
    )
    if candidate_identity is None:
        return _info(
            "duplicate_identity_unknown",
            "beets duplicate album has no comparable release identity",
        )
    if candidate_identity.key != target_identity.key:
        return _info(
            "release_identity_mismatch",
            "beets duplicate album release identity does not match target",
        )
    return None


# ---------------------------------------------------------------------------
# Pure stage decision functions — extracted from main() for testability
# ---------------------------------------------------------------------------


@dataclass
class StageResult:
    """Result of a pipeline stage decision point."""
    decision: str = "continue"
    exit_code: int = 0
    error: str | None = None
    terminal: bool = False

    @property
    def is_terminal(self) -> bool:
        return self.terminal


@dataclass
class RunImportOutcome:
    """Result from the beets harness import subprocess."""

    exit_code: int
    beets_lines: list[str]
    duplicate_remove_guard: DuplicateRemoveGuardInfo | None = None
    beets_owned_replacement: bool = False


def preflight_decision(already_in_beets: bool, path_exists: bool) -> StageResult:
    """Decide whether to proceed based on pre-flight checks (pure)."""
    if not path_exists:
        if already_in_beets:
            return StageResult(decision="preflight_existing", exit_code=0, terminal=True)
        return StageResult(decision="path_missing", exit_code=3,
                           error="Path not found", terminal=True)
    return StageResult(decision="continue")


def conversion_decision(converted: int, failed: int) -> StageResult:
    """Decide whether to proceed after FLAC conversion (pure)."""
    if failed > 0:
        return StageResult(decision="conversion_failed", exit_code=1,
                           error=f"{failed} FLAC files failed to convert",
                           terminal=True)
    return StageResult(decision="continue")


def quality_decision_stage(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement | None,
    is_transcode: bool,
    cfg: QualityRankConfig | None = None,
) -> StageResult:
    """Run quality comparison and map to exit codes (pure wrapper).

    Delegates to import_quality_decision() and maps terminal decisions
    to exit codes: downgrade→5, transcode_downgrade→6.

    ``cfg`` flows through to import_quality_decision for codec-aware
    comparison. Falls back to QualityRankConfig.defaults() when omitted.
    """
    result = measured_import_decision(
        MeasuredImportDecisionInput(new, existing, is_transcode),
        cfg=cfg,
    )
    decision = result.decision

    if result.confident_reject:
        return StageResult(
            decision=decision,
            exit_code=result.exit_code,
            terminal=True,
        )
    # import, transcode_upgrade, transcode_first all proceed to import
    return StageResult(decision=decision, exit_code=0)


def build_existing_measurement(
    existing_info: AlbumInfo | None,
    *,
    override_min_bitrate: int | None,
    existing_spectral_grade: str | None,
    existing_spectral_bitrate: int | None,
) -> AudioQualityMeasurement | None:
    """Build the existing on-disk measurement for pre-import comparison.

    ``override_min_bitrate`` is already the pipeline's corrected view of the
    existing album after spectral downgrade logic. Under any non-MIN rank
    metric (AVG / MEDIAN) we must apply that override to every bitrate field
    so the harness compares against the same effective quality the caller
    intended — otherwise the median/avg would silently outvote the override.
    """
    if existing_info is None:
        return None
    return build_existing_quality_measurement(
        min_bitrate_kbps=existing_info.min_bitrate_kbps,
        avg_bitrate_kbps=existing_info.avg_bitrate_kbps,
        median_bitrate_kbps=existing_info.median_bitrate_kbps,
        format=existing_info.format,
        is_cbr=existing_info.is_cbr,
        override_min_bitrate=override_min_bitrate,
        spectral_grade=existing_spectral_grade,
        spectral_bitrate_kbps=existing_spectral_bitrate,
    )


def conversion_target(target_format: str | None,
                      will_be_verified_lossless: bool,
                      verified_lossless_target: str | None) -> str | None:
    """What should lossless files become on disk? (pure)

    Returns:
        "lossless" — keep lossless on disk (user intent via target_format)
        str        — verified_lossless_target spec (e.g. "opus 128", "mp3 v2")
        None       — keep V0 (default, or not verified lossless)
    """
    if target_format in ("flac", "lossless"):
        return "lossless"
    if not will_be_verified_lossless:
        return None
    if verified_lossless_target:
        return verified_lossless_target
    return None


def should_run_target_conversion(conv_target: str | None) -> bool:
    """Should we run the second conversion pass for a target format? (pure)

    The "lossless" sentinel means "keep lossless on disk" and must not be
    passed to parse_verified_lossless_target().
    """
    return conv_target not in (None, "lossless")


def target_cleanup_decision(target_achieved: bool,
                            target_was_configured: bool,
                            sources_kept: int,
                            preserve_source: bool = False) -> bool:
    """Should we clean up kept source files before beets import? (pure)

    V0 conversion may have preserved lossless originals for two reasons:

    1. A ``verified_lossless_target`` was configured — the second conversion
       pass planned to consume them. If that pass was skipped (transcode
       detected → not verified lossless), originals must be removed so beets
       only sees V0 MP3s. Gated on ``sources_kept > 0`` because without a
       kept source there is nothing to clean.
    2. ``--preserve-source`` was set (force/manual import, issue #111) — we
       held originals back in case the quality decision rejected the import,
       so the user's source FLACs in ``failed_imports/`` would not be
       destroyed on downgrade/transcode_downgrade. If we reach this call
       site, the quality decision was non-terminal and beets is about to
       run — originals must be removed so beets only sees V0 MP3s.

       Unlike case 1 we deliberately do NOT gate on ``sources_kept > 0``:
       on a retry of a previously-rejected force/manual attempt the V0
       MP3s already exist, so ``convert_lossless`` skips and reports
       ``converted == 0`` — but the lossless originals from the prior run
       are still on disk and still must be cleaned before beets runs
       (PR #112 Codex round 1 P2). ``_remove_lossless_files`` is
       idempotent, so a True verdict with nothing to remove is a safe
       no-op.

    Callers must additionally gate on "did the V0 pass run at all?" —
    passing ``preserve_source`` through when the harness is in
    keep-lossless-on-disk mode would delete the very files beets is
    supposed to receive (PR #112 Codex round 1 P1).
    """
    if preserve_source:
        return True
    if sources_kept <= 0:
        return False
    return target_was_configured and not target_achieved


def final_exit_decision(is_transcode: bool) -> int:
    """Determine the final exit code after a successful import."""
    return 6 if is_transcode else 0


# ---------------------------------------------------------------------------
# Conversion spec — parameterized ffmpeg conversion
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConversionSpec:
    """ffmpeg conversion parameters for lossless → lossy conversion.

    Carries everything needed to convert a lossless file to a specific
    lossy format via ffmpeg. Used by convert_lossless() for both V0
    verification and final target format conversion.
    """
    codec: str                              # ffmpeg codec name: "libmp3lame", "libopus", "aac"
    codec_args: tuple[str, ...] = ()        # quality/bitrate args: ("-q:a", "0") or ("-b:a", "128k")
    extension: str = "mp3"                  # output file extension (without dot)
    label: str = "mp3 v0"                   # human-readable label for logging/display
    metadata_args: tuple[str, ...] = ("-map_metadata", "0")  # metadata handling


# FLAC normalization spec — converts ALAC/WAV → FLAC when keeping lossless on disk
FLAC_SPEC = ConversionSpec(
    codec="flac",
    codec_args=(),
    extension="flac",
    label="flac",
)

# V0 verification spec — always used as the first conversion step for FLAC
V0_SPEC = ConversionSpec(
    codec="libmp3lame",
    codec_args=("-q:a", "0"),
    extension="mp3",
    label="mp3 v0",
    metadata_args=("-map_metadata", "0", "-id3v2_version", "3"),
)


def parse_verified_lossless_target(spec: str) -> ConversionSpec:
    """Parse a target format string into a ConversionSpec.

    Supported formats:
        "opus 128"  → libopus VBR 128kbps
        "opus 96"   → libopus VBR 96kbps
        "mp3 v0"    → LAME VBR quality 0
        "mp3 v2"    → LAME VBR quality 2
        "mp3 192"   → LAME CBR 192kbps
        "aac 128"   → AAC VBR 128kbps

    Raises ValueError for unrecognised formats.
    """
    spec = spec.strip().lower()
    if not spec:
        raise ValueError("empty target format spec")

    parts = spec.split(None, 1)
    if len(parts) != 2:
        raise ValueError(f"expected 'codec quality', got: {spec!r}")

    codec_name, quality = parts

    if codec_name == "opus":
        if not quality.isdigit():
            raise ValueError(f"opus requires numeric bitrate, got: {quality!r}")
        bitrate = int(quality)
        if bitrate < 6 or bitrate > 510:
            raise ValueError(f"opus bitrate must be 6-510, got: {bitrate}")
        return ConversionSpec(
            codec="libopus",
            codec_args=("-b:a", f"{quality}k"),
            extension="opus",
            label=spec,
        )
    elif codec_name == "mp3":
        if quality.startswith("v") and quality[1:].isdigit():
            # VBR quality: v0-v9
            q_num = int(quality[1:])
            if q_num > 9:
                raise ValueError(f"mp3 VBR quality must be v0-v9, got: v{q_num}")
            return ConversionSpec(
                codec="libmp3lame",
                codec_args=("-q:a", str(q_num)),
                extension="mp3",
                label=spec,
                metadata_args=("-map_metadata", "0", "-id3v2_version", "3"),
            )
        elif quality.isdigit():
            # CBR bitrate
            bitrate = int(quality)
            if bitrate < 32 or bitrate > 320:
                raise ValueError(f"mp3 CBR bitrate must be 32-320, got: {bitrate}")
            return ConversionSpec(
                codec="libmp3lame",
                codec_args=("-b:a", f"{quality}k"),
                extension="mp3",
                label=spec,
                metadata_args=("-map_metadata", "0", "-id3v2_version", "3"),
            )
        else:
            raise ValueError(f"mp3 quality must be 'vN' or numeric bitrate, got: {quality!r}")
    elif codec_name == "aac":
        if not quality.isdigit():
            raise ValueError(f"aac requires numeric bitrate, got: {quality!r}")
        bitrate = int(quality)
        if bitrate < 16 or bitrate > 512:
            raise ValueError(f"aac bitrate must be 16-512, got: {bitrate}")
        return ConversionSpec(
            codec="aac",
            codec_args=("-b:a", f"{quality}k"),
            extension="m4a",
            label=spec,
        )
    else:
        raise ValueError(f"unsupported codec: {codec_name!r} (supported: opus, mp3, aac)")


# ---------------------------------------------------------------------------
# Quality checking
# ---------------------------------------------------------------------------


def _get_folder_bitrates(folder_path,
                         ext_filter: set[str] | None = None) -> list[int]:
    """Probe per-track bitrates (kbps) for audio files in a folder via ffprobe.

    Uses audio stream bitrate (excludes cover art overhead). Falls back
    to format bitrate for VBR MP3s where stream bitrate is N/A.

    ext_filter: if provided, only measure files with these extensions
    (e.g. {".mp3"} to measure only V0 files when FLAC coexists).

    Returns a list of strictly-positive bitrates (kbps) in filesystem-listed
    order. Use _get_folder_min_bitrate() / _get_folder_avg_bitrate() for the
    aggregate helpers.
    """
    bitrates: list[int] = []
    for fname in os.listdir(folder_path):
        ext = os.path.splitext(fname)[1].lower()
        if ext not in AUDIO_EXTENSIONS:
            continue
        if ext_filter is not None and ext not in ext_filter:
            continue
        fpath = os.path.join(folder_path, fname)
        try:
            # Try audio stream bitrate first (accurate for CBR, excludes cover art)
            result = subprocess.run(
                ["ffprobe", "-v", "error",
                 "-select_streams", "a:0",
                 "-show_entries", "stream=bit_rate",
                 "-of", "csv=p=0", fpath],
                capture_output=True, text=True, timeout=30,
            )
            br_str = result.stdout.strip().rstrip(",")
            # VBR MP3s return N/A for stream bitrate — fall back to format
            if not br_str or not br_str.isdigit():
                result = subprocess.run(
                    ["ffprobe", "-v", "error",
                     "-show_entries", "format=bit_rate",
                     "-of", "csv=p=0", fpath],
                    capture_output=True, text=True, timeout=30,
                )
                br_str = result.stdout.strip().rstrip(",")
            if br_str and br_str.isdigit():
                br_kbps = int(br_str) // 1000
                if br_kbps > 0:
                    bitrates.append(br_kbps)
        except Exception:
            continue
    return bitrates


def _get_folder_min_bitrate(folder_path,
                            ext_filter: set[str] | None = None) -> int | None:
    """Legacy alias: minimum per-file bitrate (kbps), or None if none probed."""
    bitrates = _get_folder_bitrates(folder_path, ext_filter=ext_filter)
    return min(bitrates) if bitrates else None


def _get_folder_avg_bitrate(folder_path,
                            ext_filter: set[str] | None = None) -> int | None:
    """Mean per-file bitrate (kbps), or None if no probable files.

    Truncated to int. Used by the codec-aware rank model as the preferred
    metric for VBR codecs (issue #60) — album-mean is more robust to
    legitimate per-track VBR variance than the min.
    """
    bitrates = _get_folder_bitrates(folder_path, ext_filter=ext_filter)
    if not bitrates:
        return None
    return int(sum(bitrates) / len(bitrates))


# ---------------------------------------------------------------------------
# Lossless → MP3 VBR V0 conversion
# ---------------------------------------------------------------------------

# Extensions that are always lossless
_ALWAYS_LOSSLESS_EXTS = {".flac", ".wav"}


def _ffprobe_audio_codec_name(fpath: str) -> str | None:
    """Return the first audio stream codec name reported by ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name", "-of", "json", fpath],
            capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            return None
        payload: object = json.loads(result.stdout or "{}")
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None
    streams = payload.get("streams")
    if not isinstance(streams, list) or not streams:
        return None
    stream = streams[0]
    if not isinstance(stream, dict):
        return None
    codec = stream.get("codec_name")
    if not isinstance(codec, str):
        return None
    return codec.strip().lower() or None


def _is_m4a_alac(fpath: str) -> bool:
    """Check if an .m4a file contains ALAC (lossless) via ffprobe."""
    return _ffprobe_audio_codec_name(fpath) == "alac"


def _is_lossless_file(fname: str, folder: str = "") -> bool:
    """Check if a file is lossless. For .m4a, probes the codec with ffprobe."""
    ext = os.path.splitext(fname)[1].lower()
    if ext in _ALWAYS_LOSSLESS_EXTS:
        return True
    if ext == ".m4a":
        fpath = os.path.join(folder, fname) if folder else fname
        return _is_m4a_alac(fpath)
    return False


def _v0_probe_from_bitrates(
    bitrates: list[int],
    *,
    kind: str = V0_PROBE_LOSSLESS_SOURCE,
) -> V0ProbeEvidence | None:
    if not bitrates:
        return None
    return V0ProbeEvidence(
        kind=kind,
        min_bitrate_kbps=min(bitrates),
        avg_bitrate_kbps=int(sum(bitrates) / len(bitrates)),
        median_bitrate_kbps=int(statistics.median(bitrates)),
    )


def _existing_v0_probe_from_args(args: argparse.Namespace) -> V0ProbeEvidence | None:
    if args.existing_v0_probe_avg_bitrate is None:
        return None
    return V0ProbeEvidence(
        kind=V0_PROBE_LOSSLESS_SOURCE,
        min_bitrate_kbps=args.existing_v0_probe_min_bitrate,
        avg_bitrate_kbps=args.existing_v0_probe_avg_bitrate,
        median_bitrate_kbps=args.existing_v0_probe_median_bitrate,
    )


def _probe_lossless_source_as_v0(album_path: str) -> V0ProbeEvidence | None:
    """Non-destructively encode lossless sources to temp V0 files and measure them."""
    lossless_files = sorted(
        f for f in os.listdir(album_path) if _is_lossless_file(f, album_path))
    if not lossless_files:
        return None
    return _temp_v0_probe(
        album_path, lossless_files, kind=V0_PROBE_LOSSLESS_SOURCE)


def _probe_native_lossy_as_v0(album_path: str) -> V0ProbeEvidence | None:
    """Non-destructively encode native lossy sources to temp V0 files and
    measure them. Audit-only research evidence; never eligible for the
    lossless-source provisional comparison (kind != lossless_source_v0).
    """
    lossy_files = sorted(
        f for f in os.listdir(album_path)
        if os.path.splitext(f)[1].lower() in AUDIO_EXTENSIONS
        and not _is_lossless_file(f, album_path))
    if not lossy_files:
        return None
    return _temp_v0_probe(
        album_path, lossy_files, kind=V0_PROBE_NATIVE_LOSSY_RESEARCH)


def _temp_v0_probe(
    album_path: str, files: list[str], *, kind: str,
) -> V0ProbeEvidence | None:
    """Re-encode the given audio files to V0 in a temp dir and measure them."""
    with tempfile.TemporaryDirectory(prefix="cratedigger-v0-probe-") as temp_dir:
        failed = 0
        for index, fname in enumerate(files):
            src_path = os.path.join(album_path, fname)
            base = os.path.splitext(os.path.basename(fname))[0]
            out_path = os.path.join(temp_dir, f"{index:03d}-{base}.mp3")
            cmd = [
                "ffmpeg", "-i", src_path,
                "-c:a", V0_SPEC.codec, *V0_SPEC.codec_args,
                *V0_SPEC.metadata_args,
                "-y", out_path,
            ]
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=300)
            except subprocess.TimeoutExpired:
                failed += 1
                continue
            if (
                result.returncode != 0
                or not os.path.exists(out_path)
                or os.path.getsize(out_path) == 0
            ):
                failed += 1

        if failed:
            _log(f"  [V0 PROBE] {failed} temporary probe conversion(s) failed (kind={kind})")
            return None
        bitrates = _get_folder_bitrates(temp_dir, ext_filter={".mp3"})
        return _v0_probe_from_bitrates(bitrates, kind=kind)


def _remove_files_by_ext(folder: str, ext: str) -> None:
    """Remove all files with the given extension from a directory."""
    for fname in os.listdir(folder):
        if fname.lower().endswith(ext):
            os.remove(os.path.join(folder, fname))


def _remove_lossless_files(folder: str) -> None:
    """Remove all lossless files from a directory."""
    for fname in os.listdir(folder):
        if _is_lossless_file(fname, folder):
            os.remove(os.path.join(folder, fname))


def convert_lossless(album_path: str, spec: ConversionSpec,
                     dry_run: bool = False,
                     keep_source: bool = False) -> tuple[int, int, str | None]:
    """Convert all lossless files using the given ConversionSpec.

    Single conversion function — replaces both convert_lossless_to_v0()
    and convert_lossless_to_opus(). The spec carries ffmpeg args, output
    extension, and metadata handling.

    Returns (converted, failed, original_filetype) where original_filetype
    is the extension of the first source file (e.g. "flac", "m4a", "wav"),
    or None if no lossless files were found.

    When keep_source=True, original lossless files are preserved (used when
    a second conversion pass will run from the originals). If the target uses
    the same path as the source (ALAC .m4a → AAC .m4a), conversion runs through
    a temporary file first so the source is not silently skipped.
    """
    lossless_files = sorted(
        f for f in os.listdir(album_path) if _is_lossless_file(f, album_path))
    if not lossless_files:
        return 0, 0, None

    original_ext = os.path.splitext(lossless_files[0])[1].lstrip(".").lower()

    converted = 0
    failed = 0
    for fname in lossless_files:
        src_path = os.path.join(album_path, fname)
        out_path = os.path.splitext(src_path)[0] + "." + spec.extension
        same_path_output = os.path.normpath(src_path) == os.path.normpath(out_path)
        temp_out_path = (
            os.path.splitext(src_path)[0] + ".tmp." + spec.extension
            if same_path_output else out_path
        )

        if not same_path_output and os.path.exists(out_path):
            continue

        if dry_run:
            print(f"  [DRY] {fname} → {os.path.basename(out_path)}",
                  file=sys.stderr)
            converted += 1
            continue

        cmd = ["ffmpeg", "-i", src_path,
               "-c:a", spec.codec, *spec.codec_args,
               *spec.metadata_args,
               "-y", temp_out_path]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=300)
        except subprocess.TimeoutExpired:
            print(f"  [FAIL] {fname}: ffmpeg timed out after 300s",
                  file=sys.stderr)
            if os.path.exists(temp_out_path):
                os.remove(temp_out_path)
            failed += 1
            continue

        if (result.returncode != 0 or not os.path.exists(temp_out_path)
                or os.path.getsize(temp_out_path) == 0):
            print(f"  [FAIL] {fname}: {result.stderr[-200:]}",
                  file=sys.stderr)
            if os.path.exists(temp_out_path):
                os.remove(temp_out_path)
            failed += 1
        else:
            if same_path_output:
                backup_path = os.path.splitext(src_path)[0] + ".source" + os.path.splitext(src_path)[1]
                if keep_source:
                    os.replace(src_path, backup_path)
                else:
                    os.remove(src_path)
                os.replace(temp_out_path, out_path)
            elif not keep_source:
                os.remove(src_path)
            converted += 1

    return converted, failed, original_ext


# ---------------------------------------------------------------------------
# Beets harness controller (JSON protocol)
# ---------------------------------------------------------------------------

def run_import(path, mb_release_id):
    """Drive the beets harness to import one album.

    Returns ``RunImportOutcome``.

    Guarded Beets-owned replacement answers ``remove`` only when Beets
    reports exactly one duplicate whose release identity matches the target.
    """
    cmd = [HARNESS, "--noincremental", "--search-id", mb_release_id, path]
    print(f"  [HARNESS] {' '.join(cmd)}", file=sys.stderr)

    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, preexec_fn=os.setsid,
        env=beets_subprocess_env(),
    )
    assert proc.stdin is not None
    assert proc.stdout is not None
    assert proc.stderr is not None

    applied = False
    beets_owned_replacement = False
    timeout = HARNESS_TIMEOUT

    try:
        while True:
            ready, _, _ = select.select([proc.stdout.fileno()], [], [], timeout)
            if not ready:
                print(f"  [TIMEOUT] No output for {timeout}s", file=sys.stderr)
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait()
                return RunImportOutcome(2, [])

            line = proc.stdout.readline()
            if not line:
                break

            line = line.strip()
            if not line:
                continue

            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")

            if msg_type in ("session_start", "session_end", "error"):
                if msg_type == "error":
                    print(f"  [HARNESS ERROR] {msg.get('message', '')}", file=sys.stderr)
                continue

            elif msg_type == "should_resume":
                proc.stdin.write(json.dumps({"resume": False}) + "\n")
                proc.stdin.flush()

            elif msg_type == "resolve_duplicate":
                candidates = _duplicate_candidates_from_message(msg)
                failure = _duplicate_remove_guard_failure(
                    target_release_id=mb_release_id,
                    candidates=candidates,
                )
                if failure is not None:
                    proc.stdin.write(json.dumps({"action": "skip"}) + "\n")
                    proc.stdin.flush()
                    print(
                        f"  [DUP-GUARD] Refusing beets duplicate remove: "
                        f"{failure.reason}: {failure.message}",
                        file=sys.stderr,
                    )
                    if proc.poll() is None:
                        proc.wait()
                    return RunImportOutcome(
                        DUPLICATE_REMOVE_GUARD_EXIT_CODE,
                        [],
                        duplicate_remove_guard=failure,
                    )

                proc.stdin.write(json.dumps({"action": "remove"}) + "\n")
                proc.stdin.flush()
                beets_owned_replacement = True
                candidate = candidates[0]
                print(
                    f"  [DUP-GUARD] Allowing beets remove for "
                    f"beets album id:{candidate.beets_album_id} "
                    f"(mb={candidate.mb_albumid or '∅'}, "
                    f"discogs={candidate.discogs_albumid or '∅'}, "
                    f"path={candidate.album_path or '∅'})",
                    file=sys.stderr,
                )

            elif msg_type in ("choose_match", "choose_item"):
                candidates = msg.get("candidates", [])
                matched_idx = _find_target_candidate(candidates, mb_release_id)

                if matched_idx is None:
                    proc.stdin.write(json.dumps({"action": "skip"}) + "\n")
                    proc.stdin.flush()
                    avail = [str(c.get("album_id", "?")) for c in candidates]
                    print(f"  [SKIP] MBID {mb_release_id} not in {len(candidates)} candidates: {avail}",
                          file=sys.stderr)
                    if proc.poll() is None:
                        proc.wait()
                    return RunImportOutcome(4, [])

                cand = candidates[matched_idx]
                dist = cand.get("distance", 1.0)

                if dist > MAX_DISTANCE:
                    proc.stdin.write(json.dumps({"action": "skip"}) + "\n")
                    proc.stdin.flush()
                    print(f"  [REJECT] distance={dist:.4f} > {MAX_DISTANCE}", file=sys.stderr)
                    if proc.poll() is None:
                        proc.wait()
                    return RunImportOutcome(2, [])

                proc.stdin.write(json.dumps({"action": "apply", "candidate_index": matched_idx}) + "\n")
                proc.stdin.flush()
                applied = True
                timeout = IMPORT_TIMEOUT
                print(f"  [APPLY] {cand.get('artist')} - {cand.get('album')} (dist={dist:.4f})", file=sys.stderr)

    except BrokenPipeError:
        print("  [WARN] Harness pipe broken", file=sys.stderr)

    proc_rc = proc.wait() if proc.poll() is None else proc.poll()

    stderr_out = proc.stderr.read() if proc.stderr else ""
    beets_lines: list[str] = []
    if stderr_out.strip():
        for line in stderr_out.strip().split("\n"):
            if "Disabled fetchart" not in line:
                print(f"  [BEETS] {line}", file=sys.stderr)
                beets_lines.append(line.strip())

    if proc_rc not in (None, 0):
        return RunImportOutcome(
            2,
            beets_lines,
            beets_owned_replacement=beets_owned_replacement,
        )

    return RunImportOutcome(
        0 if applied else 2,
        beets_lines,
        beets_owned_replacement=beets_owned_replacement,
    )


# ---------------------------------------------------------------------------
# Pipeline DB updates
# ---------------------------------------------------------------------------

def update_pipeline_db(request_id, status, imported_path=None, distance=None, scenario=None):
    """Update pipeline DB via the shared finalization seam.

    Best-effort — failures are logged but do not block the import harness.
    """
    try:
        from lib import transitions
        from lib.pipeline_db import PipelineDB
        dsn = os.environ.get("PIPELINE_DB_DSN", "postgresql://cratedigger@localhost/cratedigger")
        db = PipelineDB(dsn)
        extra = {}
        if imported_path:
            extra["imported_path"] = imported_path
        if distance is not None:
            extra["beets_distance"] = distance
        if scenario:
            extra["beets_scenario"] = scenario
        try:
            if status == "imported":
                transition = transitions.RequestTransition.to_imported_fields(
                    fields=extra)
            elif status == "wanted":
                transition = transitions.RequestTransition.to_wanted_fields(
                    fields=extra)
            elif status == "manual":
                if extra:
                    names = ", ".join(sorted(extra))
                    raise ValueError(
                        f"manual transitions do not accept fields: {names}")
                transition = transitions.RequestTransition.to_manual()
            else:
                transition = transitions.RequestTransition.status_only(status)
            transitions.finalize_request(
                db,
                request_id,
                transition,
            )
        except (TypeError, ValueError) as e:
            print(f"  [WARN] Pipeline DB transition rejected: {e}", file=sys.stderr)
        finally:
            db.close()
    except Exception as e:
        print(f"  [WARN] Pipeline DB update failed: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _log(msg):
    """Human-readable log to stderr (visible in journalctl)."""
    print(msg, file=sys.stderr, flush=True)


def _emit_and_exit(r) -> NoReturn:
    """Emit ImportResult JSON on stdout and exit."""
    global _preview_temp_root  # noqa: PLW0603
    if _preview_temp_root is not None:
        shutil.rmtree(_preview_temp_root, ignore_errors=True)
        _preview_temp_root = None
    print(r.to_sentinel_line(), flush=True)
    sys.exit(r.exit_code)


def _record_bad_extension_warnings(
    beets: BeetsDB,
    mbid: str,
    r: ImportResult,
) -> list[str]:
    """Record postflight bad extensions without mutating imported files.

    This used to automatically rename ``.bak`` files and rewrite beets'
    SQLite paths. That repair was a workaround for ``mp3val -f`` creating
    backup files, fixed by running mp3val with ``-nb``. If a bad extension
    appears now, it is an upstream corruption signal, not a normal recovery
    path. The album is already imported at this point, so preserve the
    successful import result and make the anomaly loud + durable instead.
    """
    bad_ext_files: list[str] = []
    for _item_id, item_path in beets.get_item_paths(mbid):
        ext = os.path.splitext(item_path)[1].lower()
        if ext not in AUDIO_EXTENSIONS and os.path.isfile(item_path):
            bad_ext_files.append(os.path.basename(item_path))

    if bad_ext_files:
        r.postflight.bad_extensions = bad_ext_files
        joined = ", ".join(bad_ext_files)
        _log("[POSTFLIGHT BAD EXTENSIONS] CRITICAL: imported album contains "
             f"non-audio extension(s): {joined}")
        _log("[POSTFLIGHT BAD EXTENSIONS] Automatic rename repair is disabled; "
             "warning recorded in import_result.postflight.bad_extensions")

    return bad_ext_files


def main():
    # Belt-and-suspenders for systemd's UMask=0000 — see lib/permissions.py / GH #84.
    # Done in main() (not at module import) so importing this module for tests
    # doesn't leak a zero umask into the test process.
    reset_umask()

    parser = argparse.ArgumentParser(description="One-shot beets import for a single album")
    parser.add_argument("path", help="Path to staged album directory")
    parser.add_argument("mb_release_id", help="MusicBrainz release ID")
    parser.add_argument("--request-id", type=int, help="Pipeline DB request ID for status updates")
    parser.add_argument("--override-min-bitrate", type=int, default=None,
                        help="Override existing min bitrate for downgrade check (kbps)")
    parser.add_argument("--force", action="store_true",
                        help="Skip distance check (for force-importing rejected albums)")
    parser.add_argument("--verified-lossless-target", default=None,
                        help="Target format after verified lossless (e.g. 'opus 128', 'mp3 v2')")
    parser.add_argument("--target-format", default=None,
                        help="Desired format on disk (e.g. 'flac' to skip conversion)")
    parser.add_argument("--quality-rank-config", default=None,
                        help="Serialized QualityRankConfig (JSON). Provided by "
                             "lib.import_dispatch.dispatch_import_core so the "
                             "harness's rank classification matches the caller's "
                             "runtime config. Missing/empty falls back to defaults.")
    parser.add_argument("--existing-v0-probe-min-bitrate", type=int, default=None,
                        help="Current comparable lossless-source V0 probe min bitrate")
    parser.add_argument("--existing-v0-probe-avg-bitrate", type=int, default=None,
                        help="Current comparable lossless-source V0 probe avg bitrate")
    parser.add_argument("--existing-v0-probe-median-bitrate", type=int, default=None,
                        help="Current comparable lossless-source V0 probe median bitrate")
    parser.add_argument("--preserve-source", action="store_true",
                        help="Preserve lossless source files (FLAC/ALAC/WAV) "
                             "during V0 conversion until the quality decision "
                             "has approved the import. Used by force/manual "
                             "import so a downgrade verdict does not destroy "
                             "the user's only copy in failed_imports/ (#111).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mbid = args.mb_release_id
    request_id = args.request_id

    # Parse --quality-rank-config and replace the module-level _rank_cfg default.
    # Used by BeetsDB.get_album_info() mixed-format reduction + (commit 5)
    # quality_rank()/compare_quality()/quality_gate_decision().
    global _rank_cfg  # noqa: PLW0603
    if args.quality_rank_config:
        try:
            _rank_cfg = QualityRankConfig.from_json(args.quality_rank_config)
            _log(f"[CONFIG] quality_rank_config: "
                 f"metric={_rank_cfg.bitrate_metric.value}, "
                 f"gate_min_rank={_rank_cfg.gate_min_rank.name}")
        except (ValueError, KeyError) as exc:
            _log(f"[WARN] --quality-rank-config parse failed ({exc}); "
                 f"falling back to defaults")
            _rank_cfg = QualityRankConfig.defaults()

    # --force: raise distance threshold so high-distance candidates are accepted
    global MAX_DISTANCE
    if args.force:
        MAX_DISTANCE = 999
        _log("[FORCE] Distance check disabled (MAX_DISTANCE=999)")

    # Accumulate structured result (module-level so crash handler can preserve data)
    global _current_result  # noqa: PLW0603
    r = ImportResult()
    r.preview = args.dry_run
    r.existing_v0_probe = _existing_v0_probe_from_args(args)
    _current_result = r

    # --- Pre-flight: already imported? ---
    beets = BeetsDB()
    import atexit
    atexit.register(beets.close)
    already_in_beets = beets.album_exists(mbid)
    r.already_in_beets = already_in_beets
    if already_in_beets:
        _log(f"[PRE-FLIGHT] Already in beets: {mbid} — checking if new files are better")

    # --- Path check (pure decision) ---
    pf = preflight_decision(already_in_beets, os.path.isdir(args.path))
    if pf.is_terminal:
        r.decision = pf.decision
        r.exit_code = pf.exit_code
        r.error = pf.error
        if pf.decision == "preflight_existing":
            _log(f"[PRE-FLIGHT] No new files, keeping existing import")
            if request_id and not args.dry_run:
                info = beets.get_album_info(mbid, _rank_cfg)
                if info:
                    r.postflight = PostflightInfo(
                        beets_id=info.album_id,
                        track_count=info.track_count,
                        imported_path=info.album_path)
                    update_pipeline_db(request_id, "imported",
                                       imported_path=info.album_path)
        else:
            _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)

    work_path = args.path
    if args.dry_run:
        global _preview_temp_root  # noqa: PLW0603
        _preview_temp_root = tempfile.mkdtemp(prefix="cratedigger-import-preview-")
        basename = os.path.basename(os.path.abspath(args.path)) or "album"
        work_path = os.path.join(_preview_temp_root, basename)
        shutil.copytree(args.path, work_path)
        _log(f"[DRY-RUN] Previewing isolated copy: {work_path}")

    # --- Spectral analysis (pre-conversion) ---
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_spectral_grade: str | None = None
    existing_spectral_bitrate: int | None = None
    try:
        from lib.spectral_check import analyze_album as spectral_analyze
        spectral_result = spectral_analyze(work_path, trim_seconds=30)
        spectral_grade = spectral_result.grade
        spectral_bitrate = spectral_result.estimated_bitrate_kbps
        r.spectral.suspect_pct = spectral_result.suspect_pct
        r.spectral.per_track = [
            {"grade": t.grade, "hf_deficit_db": round(t.hf_deficit_db, 1),
             "cliff_detected": t.cliff_detected,
             "cliff_freq_hz": t.cliff_freq_hz,
             "estimated_bitrate_kbps": t.estimated_bitrate_kbps}
            for t in spectral_result.tracks
        ]
        _log(f"  spectral_grade={spectral_grade}")
        if spectral_bitrate is not None:
            _log(f"  spectral_bitrate={spectral_bitrate}")
        if spectral_grade in ("suspect", "likely_transcode"):
            cliff_tracks = [t for t in spectral_result.tracks if t.cliff_detected]
            if cliff_tracks:
                r.spectral.cliff_freq_hz = cliff_tracks[0].cliff_freq_hz
                _log(f"  spectral_cliff={cliff_tracks[0].cliff_freq_hz}Hz")
        # Spectral check on existing beets files
        if already_in_beets:
            existing_path = beets.get_album_path(mbid)
            if existing_path and os.path.isdir(existing_path):
                existing_spectral = spectral_analyze(existing_path, trim_seconds=30)
                existing_spectral_grade = existing_spectral.grade
                existing_spectral_bitrate = existing_spectral.estimated_bitrate_kbps
                r.spectral.existing_suspect_pct = existing_spectral.suspect_pct
                _log(f"  existing_spectral_grade={existing_spectral_grade}")
                if existing_spectral_bitrate is not None:
                    _log(f"  existing_spectral_bitrate={existing_spectral_bitrate}")
    except Exception as e:
        _log(f"  [SPECTRAL] error: {e}")

    # --- Convert lossless → V0 (unless keeping lossless on disk) ---
    keep_lossless = args.target_format in ("flac", "lossless")
    converted = 0
    failed = 0
    original_ext = None
    v0_ext_filter = None
    post_conv_br = None
    is_transcode = False
    supported_lossless_source = False

    has_target = bool(args.verified_lossless_target)
    # V0 must keep the lossless source when either a second conversion pass
    # is planned (``verified_lossless_target``) OR the caller asked us to
    # hold it until the quality decision (``--preserve-source``, issue #111).
    keep_v0_source = has_target or args.preserve_source
    if not keep_lossless:
        _log(f"[CONVERT] {work_path}")
        converted, failed, original_ext = convert_lossless(
            work_path, V0_SPEC,
            keep_source=keep_v0_source)
        r.conversion.converted = converted
        r.conversion.failed = failed
        if converted > 0:
            r.conversion.was_converted = True
            r.conversion.original_filetype = original_ext or "flac"
            r.conversion.target_filetype = "mp3"
            supported_lossless_source = (
                (original_ext or "").lower() in {"flac", "wav", "m4a", "alac"}
            )
        _log(f"  Converted {converted}, failed {failed}")
        cd = conversion_decision(converted, failed)
        if cd.is_terminal:
            r.exit_code = cd.exit_code
            r.decision = cd.decision
            r.error = cd.error
            _log(f"[ERROR] {r.error}")
            _emit_and_exit(r)

        # --- Transcode detection ---
        # When keep_v0_source=True, FLAC+MP3 coexist — measure only MP3 for V0 bitrate
        v0_ext_filter = {".mp3"} if keep_v0_source and converted > 0 else None
        post_conv_br = _get_folder_min_bitrate(work_path, ext_filter=v0_ext_filter) if converted > 0 else None
        r.conversion.post_conversion_min_bitrate = post_conv_br
        is_transcode = transcode_detection(
            converted, post_conv_br,
            spectral_grade=spectral_grade, cfg=_rank_cfg)
        r.conversion.is_transcode = is_transcode
        if is_transcode:
            # Threshold tracks the runtime cfg (issue #66) — log the value
            # the decision actually used so retuned deployments stay
            # auditable.
            _log(f"[TRANSCODE] converted FLAC min bitrate {post_conv_br}kbps "
                 f"< {_rank_cfg.mp3_vbr.excellent}kbps — source was not lossless")
        if post_conv_br is not None:
            _log(f"  post_conversion_min_bitrate={post_conv_br}")
    else:
        # Keeping lossless on disk — normalize ALAC/WAV → FLAC if needed
        lossless_files = sorted(
            f for f in os.listdir(work_path) if _is_lossless_file(f, work_path))
        supported_lossless_source = bool(lossless_files)
        has_non_flac = any(
            not f.lower().endswith(".flac") for f in lossless_files)
        if has_non_flac:
            _log(f"[NORMALIZE] Converting non-FLAC lossless → FLAC")
            converted, failed, original_ext = convert_lossless(
                work_path, FLAC_SPEC)
            r.conversion.converted = converted
            r.conversion.failed = failed
            if converted > 0:
                r.conversion.was_converted = True
                r.conversion.original_filetype = original_ext
                r.conversion.target_filetype = "flac"
            _log(f"  Normalized {converted} files, failed {failed}")
            cd = conversion_decision(converted, failed)
            if cd.is_terminal:
                r.exit_code = cd.exit_code
                r.decision = cd.decision
                r.error = cd.error
                _log(f"[ERROR] {r.error}")
                _emit_and_exit(r)
        else:
            _log(f"[CONVERT] Keeping lossless on disk (target_format={args.target_format})")
        r.final_format = "flac"
        r.v0_probe = _probe_lossless_source_as_v0(work_path)
        if r.v0_probe:
            _log(f"  source_v0_probe_avg={r.v0_probe.avg_bitrate_kbps}kbps")

    # --- Quality comparison ---
    new_bitrates = _get_folder_bitrates(work_path, ext_filter=v0_ext_filter)
    if not keep_lossless and supported_lossless_source and converted > 0:
        r.v0_probe = _v0_probe_from_bitrates(new_bitrates)
        if r.v0_probe:
            _log(f"  source_v0_probe_avg={r.v0_probe.avg_bitrate_kbps}kbps")
    elif not keep_lossless and not supported_lossless_source:
        # Native lossy candidate: emit a research probe (audit-only,
        # never eligible for the lossless-source provisional comparison).
        r.v0_probe = _probe_native_lossy_as_v0(work_path)
        if r.v0_probe:
            _log(f"  native_lossy_research_v0_avg={r.v0_probe.avg_bitrate_kbps}kbps")
    new_min_br = min(new_bitrates) if new_bitrates else None
    new_avg_br = int(sum(new_bitrates) / len(new_bitrates)) if new_bitrates else None
    new_median_br = (
        int(statistics.median(new_bitrates)) if new_bitrates else None
    )
    new_is_cbr = len(set(new_bitrates)) == 1 if new_bitrates else False
    existing_info = beets.get_album_info(mbid, _rank_cfg)
    existing_min_br = existing_info.min_bitrate_kbps if existing_info else None
    if args.override_min_bitrate is not None and existing_min_br is not None:
        if args.override_min_bitrate != existing_min_br:
            _log(f"  [OVERRIDE] pipeline says {args.override_min_bitrate}kbps, "
                 f"beets says {existing_min_br}kbps")
    effective_existing = args.override_min_bitrate if args.override_min_bitrate is not None else existing_min_br
    if effective_existing is not None:
        _log(f"  prev_min_bitrate={effective_existing}")
    if new_min_br is not None:
        _log(f"  new_min_bitrate={new_min_br}")
    if new_avg_br is not None:
        _log(f"  new_avg_bitrate={new_avg_br}")

    # Verified lossless: single source of truth in quality.py. r.v0_probe is
    # populated above (lossless source path) and lets the V0-avg trust
    # override flip a spectral-suspect spoken-word/sparse-HF lossless source
    # to verified when the V0 evidence corroborates a genuine master.
    will_be_verified_lossless = determine_verified_lossless(
        args.target_format, spectral_grade, converted, is_transcode,
        v0_probe=r.v0_probe)
    if (will_be_verified_lossless and is_transcode
            and v0_probe_overrides_spectral(r.v0_probe)):
        # Audit log: the V0 probe overrode the spectral-derived transcode
        # signal. Operators chasing a counter-intuitive verified_lossless
        # decision in download_log JSONB should see this in stderr too.
        _log(f"[V0_OVERRIDE] spectral={spectral_grade} but lossless_source_v0 "
             f"avg={r.v0_probe.avg_bitrate_kbps if r.v0_probe else '?'}kbps / "
             f"min={r.v0_probe.min_bitrate_kbps if r.v0_probe else '?'}kbps "
             "→ verified_lossless=True")

    # Final format label for the NEW measurement. Compute conv_target early
    # (originally it was computed after the quality decision — hoisted for
    # issue #60 so the rank model sees the correct target label).
    new_conv_target = conversion_target(
        args.target_format, will_be_verified_lossless,
        args.verified_lossless_target)
    new_format_label = comparison_format_hint(
        target_format=args.target_format,
        verified_lossless_target=new_conv_target,
        converted_count=converted,
        is_transcode=is_transcode,
        native_codec_family="MP3",
    )

    # --- Build measurements ---
    new_m = AudioQualityMeasurement(
        min_bitrate_kbps=new_min_br,
        avg_bitrate_kbps=new_avg_br,
        median_bitrate_kbps=new_median_br,
        format=new_format_label,
        is_cbr=new_is_cbr,
        spectral_grade=spectral_grade,
        spectral_bitrate_kbps=spectral_bitrate,
        verified_lossless=will_be_verified_lossless,
        was_converted_from=(original_ext or "flac") if converted > 0 else None,
    )
    existing_m = build_existing_measurement(
        existing_info,
        override_min_bitrate=args.override_min_bitrate,
        existing_spectral_grade=existing_spectral_grade,
        existing_spectral_bitrate=existing_spectral_bitrate,
    )
    r.new_measurement = new_m
    r.existing_measurement = existing_m

    # --- Quality comparison (pure decision) ---
    provisional = provisional_lossless_decision(
        ProvisionalLosslessDecisionInput(
            candidate_probe=r.v0_probe,
            existing_probe=r.existing_v0_probe,
            spectral_grade=spectral_grade,
            supported_lossless_source=supported_lossless_source,
        ),
        cfg=_rank_cfg,
    )
    qd: StageResult | None = None
    if provisional.decision is not None:
        decision = provisional.decision
        r.decision = decision
        r.error = provisional.reason if provisional.confident_reject else None
        if decision == "provisional_lossless_upgrade":
            _log(f"  [QUALITY] provisional lossless-source upgrade: "
                 f"source_v0_avg={r.v0_probe.avg_bitrate_kbps if r.v0_probe else None}kbps")
            if not keep_lossless and args.verified_lossless_target:
                new_conv_target = args.verified_lossless_target
                new_format_label = comparison_format_hint(
                    target_format=args.target_format,
                    verified_lossless_target=new_conv_target,
                    converted_count=converted,
                    is_transcode=is_transcode,
                    native_codec_family="MP3",
                )
                new_m = AudioQualityMeasurement(
                    min_bitrate_kbps=new_min_br,
                    avg_bitrate_kbps=new_avg_br,
                    median_bitrate_kbps=new_median_br,
                    format=new_format_label,
                    is_cbr=new_is_cbr,
                    spectral_grade=spectral_grade,
                    spectral_bitrate_kbps=spectral_bitrate,
                    verified_lossless=False,
                    was_converted_from=(
                        (original_ext or "flac") if converted > 0 else None
                    ),
                )
                r.new_measurement = new_m
    else:
        qd = quality_decision_stage(new_m, existing_m, is_transcode=is_transcode,
                                    cfg=_rank_cfg)
        decision = qd.decision
        r.decision = decision

    if args.dry_run:
        if provisional.decision is not None:
            r.exit_code = 5 if provisional.confident_reject else 0
        else:
            assert qd is not None
            r.exit_code = qd.exit_code if qd.is_terminal else 0
        _log(f"[DRY-RUN] Preview decision={decision}; stopping before beets import")
        beets.close()
        _emit_and_exit(r)

    if provisional.confident_reject:
        r.exit_code = 5
        log_prefix = (
            "[LOSSLESS SOURCE LOCKED]"
            if decision == "lossless_source_locked"
            else "[SUSPECT LOSSLESS REJECT]"
        )
        _log(f"{log_prefix} {provisional.reason}")
        if args.preserve_source and not keep_lossless and converted > 0:
            _remove_files_by_ext(work_path, "." + V0_SPEC.extension)
            _log(f"  [PRESERVE-SOURCE] Removed temporary V0 artifacts; "
                 f"lossless originals left intact for retry")
        _emit_and_exit(r)

    if qd is not None and qd.is_terminal:
        r.exit_code = qd.exit_code
        _log(f"[QUALITY DOWNGRADE] new format={new_m.format or 'unknown'} "
             f"min={new_min_br}kbps vs existing format="
             f"{existing_m.format if existing_m else 'none'} "
             f"min={effective_existing}kbps — skipping import"
             f"{' (transcode)' if decision == 'transcode_downgrade' else ''}")
        # PR #112 Codex round 2: --preserve-source terminal exit must leave
        # the user's original lossless files ALONE and remove our temporary
        # V0 MP3s. Otherwise a retry of the same folder sees mixed FLAC+MP3,
        # convert_lossless skips (output exists) and returns converted=0,
        # the quality stage then measures across mixed files and the
        # verified_lossless_target pass is wrongly skipped.
        if args.preserve_source and not keep_lossless and converted > 0:
            _remove_files_by_ext(work_path, "." + V0_SPEC.extension)
            _log(f"  [PRESERVE-SOURCE] Removed temporary V0 artifacts; "
                 f"lossless originals left intact for retry")
        _emit_and_exit(r)

    # Non-terminal quality decisions — log and proceed to import
    if decision == "import":
        if will_be_verified_lossless and effective_existing is not None:
            _log(f"  [QUALITY] verified-lossless target "
                 f"{new_m.format or V0_SPEC.label} accepted over existing "
                 f"{effective_existing}kbps")
        elif effective_existing is not None:
            _log(f"  [QUALITY] new {new_min_br}kbps > existing {effective_existing}kbps — upgrading")
    elif decision == "transcode_upgrade":
        _log(f"  [QUALITY] new {new_min_br}kbps > existing "
             f"{effective_existing}kbps — upgrading (transcode)")
    elif decision == "transcode_first":
        _log(f"  [QUALITY] no existing album in beets — importing transcode")

    if (not keep_lossless
            and (will_be_verified_lossless
                 or decision == "provisional_lossless_upgrade")
            and converted > 0
            and not should_run_target_conversion(new_conv_target)):
        # Persist the V0 label for the post-import quality gate and any
        # downstream UI/CLI consumers. Beets only stores the bare "MP3"
        # codec family, which is not enough to recover the V0 contract later.
        r.final_format = V0_SPEC.label

    # --- Target format conversion (after V0 verdict, before import) ---
    # conv_target was hoisted above for issue #60 (so new_m.format is
    # available at the quality decision). Re-use it here instead of
    # re-computing.
    conv_target = new_conv_target
    target_achieved = False
    if should_run_target_conversion(conv_target):
        assert conv_target is not None
        target_spec = parse_verified_lossless_target(conv_target)
        _log(f"[TARGET] Converting lossless source → {target_spec.label}")
        r.v0_verification_bitrate = post_conv_br
        # If target has same extension as V0 (.mp3), remove V0 files first
        # so convert_lossless doesn't skip due to existing output files.
        if target_spec.extension == V0_SPEC.extension:
            _remove_files_by_ext(work_path, "." + V0_SPEC.extension)
        target_converted, target_failed, _ = convert_lossless(
            work_path, target_spec, keep_source=True)
        if target_failed > 0:
            r.exit_code = 1
            r.decision = "target_conversion_failed"
            r.error = f"{target_failed} {target_spec.label} conversions failed"
            _log(f"[ERROR] {r.error}")
            _emit_and_exit(r)
        target_achieved = True
        # Remove V0 temp files (ephemeral verification artifacts) —
        # may already be gone if target had the same extension
        if target_spec.extension != V0_SPEC.extension:
            _remove_files_by_ext(work_path, "." + V0_SPEC.extension)
        # Remove original lossless files (consumed by target conversion)
        _remove_lossless_files(work_path)
        # Update measurements for the target format — include both the
        # measured min/avg bitrate and the declared format label so the
        # rank model classifies against the contract (e.g. "opus 128")
        # rather than the measured VBR number (which lands 95-150 kbps
        # depending on material).
        target_bitrates = _get_folder_bitrates(work_path)
        target_min_br = min(target_bitrates) if target_bitrates else None
        target_avg_br = (
            int(sum(target_bitrates) / len(target_bitrates))
            if target_bitrates else None
        )
        target_median_br = (
            int(statistics.median(target_bitrates)) if target_bitrates else None
        )
        target_is_cbr = len(set(target_bitrates)) == 1 if target_bitrates else False
        r.new_measurement = AudioQualityMeasurement(
            min_bitrate_kbps=target_min_br,
            avg_bitrate_kbps=target_avg_br,
            median_bitrate_kbps=target_median_br,
            format=target_spec.label,
            is_cbr=target_is_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            verified_lossless=will_be_verified_lossless,
            was_converted_from=(original_ext or "flac"),
        )
        r.conversion.target_filetype = target_spec.extension
        r.final_format = target_spec.label
        _log(f"  {target_spec.label} conversion complete: {target_converted} files, "
             f"min_bitrate={target_min_br}kbps, avg_bitrate={target_avg_br}kbps")
        _log(f"  V0 verification bitrate: {post_conv_br}kbps")

    # --- Clean up kept source files if target was skipped OR preserve-source
    # is active (force/manual import, issue #111). The quality decision has
    # already returned non-terminal at this point — beets is about to run,
    # so remaining lossless originals must be removed so only V0 MP3s are
    # cataloged. On terminal verdicts we exit at line 997 above and the
    # originals stay intact for the user. ---
    # Skip this cleanup entirely when target_format asks for lossless on
    # disk (keep_lossless=True): in that branch ``converted`` counts
    # ALAC/WAV→FLAC normalization, and the lossless files are exactly what
    # beets is meant to receive — removing them would destroy the only
    # copy (PR #112 Codex round 1 P1).
    if not keep_lossless and target_cleanup_decision(
            target_achieved, has_target, converted,
            preserve_source=args.preserve_source):
        _remove_lossless_files(work_path)
        _log(f"  [CLEANUP] Removed lossless originals "
             f"(target skipped or preserve-source approved)")

    # --- Import ---
    _log(f"[IMPORT] {work_path} → beets (mbid={mbid})")
    import_outcome = run_import(work_path, mbid)
    rc = import_outcome.exit_code
    beets_lines = import_outcome.beets_lines
    r.beets_log = beets_lines

    if rc != 0:
        r.exit_code = rc
        if import_outcome.duplicate_remove_guard is not None:
            r.decision = "duplicate_remove_guard_failed"
            r.postflight.duplicate_remove_guard = (
                import_outcome.duplicate_remove_guard)
            r.error = import_outcome.duplicate_remove_guard.message
        else:
            r.decision = "import_failed" if rc == 2 else "mbid_missing" if rc == 4 else "import_failed"
            r.error = next((line for line in reversed(beets_lines) if line.strip()),
                           f"Harness returned rc={rc}")
        _log(f"[ERROR] Import failed (rc={rc})")
        _emit_and_exit(r)

    # Beets owns duplicate replacement. Cratedigger only validates the
    # resulting DB shape and fails loudly if Beets did not leave exactly one
    # row for this release.
    post_import_ids = beets.get_all_album_ids_for_release(mbid)
    if not post_import_ids:
        r.exit_code = 2
        r.decision = "import_failed"
        r.error = (f"Post-import: MBID {mbid} NOT in beets DB after "
                   "import — the harness reported success but no row "
                   "survives.")
        _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)
    if len(post_import_ids) != 1:
        r.exit_code = 2
        r.decision = "import_failed"
        r.error = (f"Post-import: release {mbid} has multiple beets album "
                   f"rows {post_import_ids}. Beets duplicate replacement "
                   "must be atomic; Cratedigger no longer performs "
                   "post-import stale-row cleanup.")
        _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)
    imported_album_id = post_import_ids[0]

    # --- Post-flight verification ---
    pf_info = beets.get_album_info(mbid, _rank_cfg)
    if not pf_info:
        r.exit_code = 2
        r.decision = "import_failed"
        r.error = f"Post-flight: MBID {mbid} NOT in beets DB after import"
        _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)

    # Extra guard: pf_info must resolve to the single row validated above.
    if pf_info.album_id != imported_album_id:
        r.exit_code = 2
        r.decision = "import_failed"
        r.error = (f"Post-flight resolved to beets_id={pf_info.album_id} "
                   f"but the only release row was {imported_album_id}; "
                   "beets is in an inconsistent state.")
        _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)

    r.postflight = PostflightInfo(beets_id=pf_info.album_id,
                                   track_count=pf_info.track_count,
                                   imported_path=pf_info.album_path)
    album_path = pf_info.album_path
    _log(f"[POST-FLIGHT OK] mbid={mbid}, beets_id={pf_info.album_id}, "
         f"tracks={pf_info.track_count}, path={album_path}")

    # --- Post-import bad-extension detection ---
    # This is warning-only. The old automatic repair path rewrote filenames
    # and beets DB paths after import, which is too late and too risky.
    _record_bad_extension_warnings(beets, mbid, r)

    # --- Force library modes ---
    # Guards against any subprocess layer that dropped umask and created
    # 0o755 dirs despite the systemd unit's UMask=0000 — see GH #84.
    fix_library_modes(album_path)

    # --- Cleanup staged dir ---
    if os.path.isdir(work_path):
        for root, dirs, files in os.walk(work_path, topdown=False):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                try:
                    os.rmdir(os.path.join(root, d))
                except OSError:
                    pass
        try:
            os.rmdir(work_path)
        except OSError:
            pass
        parent = os.path.dirname(work_path)
        try:
            os.rmdir(parent)
        except OSError:
            pass

    # --- Pipeline DB: imported ---
    if request_id:
        update_pipeline_db(request_id, "imported", imported_path=album_path)

    # --- Final exit ---
    beets.close()
    r.exit_code = final_exit_decision(is_transcode)
    if r.decision == "provisional_lossless_upgrade":
        _log("[OK] Provisional lossless-source import — denylist user, keep searching")
    elif is_transcode:
        _log("[OK] Transcode imported (upgrade) — denylist user, keep searching")
    else:
        _log("[OK] Import complete")
    _emit_and_exit(r)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise  # _emit_and_exit uses sys.exit
    except Exception as exc:
        # Preserve intermediate data if main() had started building a result
        if _current_result is not None:
            r = _current_result
            r.exit_code = 99
            r.decision = "crash"
            r.error = f"{type(exc).__name__}: {exc}"
        else:
            r = ImportResult(exit_code=99, decision="crash",
                             error=f"{type(exc).__name__}: {exc}")
        _emit_and_exit(r)
