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
from lib.release_cleanup import (SelectorFailure,
                                 remove_album_by_beets_id)
from lib.util import beet_bin, beets_subprocess_env
from lib.quality import (AUDIO_EXTENSIONS_DOTTED as AUDIO_EXTENSIONS,
                         AudioQualityMeasurement, DisambiguationFailure,
                         ImportResult, PostflightInfo, QualityRankConfig,
                         comparison_format_hint,
                         determine_verified_lossless,
                         import_quality_decision, transcode_detection)
HARNESS = os.path.join(os.path.dirname(__file__), "..", "harness", "run_beets_harness.sh")
# Back-compat alias; new callsites should prefer ``beet_bin()`` from
# ``lib.util`` so subprocess resolution stays in one place.
BEET_BIN = beet_bin()
HARNESS_TIMEOUT = 300
IMPORT_TIMEOUT = 1800
MAX_DISTANCE = 0.5
_current_result: ImportResult | None = None

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
    decision = import_quality_decision(new, existing, is_transcode, cfg=cfg)

    if decision == "downgrade":
        return StageResult(decision="downgrade", exit_code=5, terminal=True)
    elif decision == "transcode_downgrade":
        return StageResult(decision="transcode_downgrade", exit_code=6, terminal=True)
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
    effective_existing = (
        override_min_bitrate
        if override_min_bitrate is not None
        else existing_info.min_bitrate_kbps
    )
    effective_avg = (
        override_min_bitrate
        if override_min_bitrate is not None
        else existing_info.avg_bitrate_kbps
    )
    effective_median = (
        override_min_bitrate
        if override_min_bitrate is not None
        else existing_info.median_bitrate_kbps
    )
    return AudioQualityMeasurement(
        min_bitrate_kbps=effective_existing,
        avg_bitrate_kbps=effective_avg,
        median_bitrate_kbps=effective_median,
        format=existing_info.format,
        is_cbr=existing_info.is_cbr,
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


def _is_m4a_alac(fpath: str) -> bool:
    """Check if an .m4a file contains ALAC (lossless) via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name", "-of", "csv=p=0", fpath],
            capture_output=True, text=True, timeout=10)
        return result.stdout.strip().lower() == "alac"
    except Exception:
        return False


def _is_lossless_file(fname: str, folder: str = "") -> bool:
    """Check if a file is lossless. For .m4a, probes the codec with ffprobe."""
    ext = os.path.splitext(fname)[1].lower()
    if ext in _ALWAYS_LOSSLESS_EXTS:
        return True
    if ext == ".m4a":
        fpath = os.path.join(folder, fname) if folder else fname
        return _is_m4a_alac(fpath)
    return False


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

    Returns (exit_code, beets_lines, kept_duplicate, sibling_mbids).

    - ``kept_duplicate`` is True whenever beets asked us to resolve a
      duplicate during this import — we ALWAYS answer "keep" now
      (never "remove", which has cross-MBID blast radius), so the
      flag is really "post-import beet move needed". Triggers the
      ``%aunique`` re-evaluation via ``_apply_disambiguation`` in
      main().
    - ``sibling_mbids`` is every non-target MBID we saw in
      ``resolve_duplicate`` callbacks. These are different-edition
      pressings beets flagged as duplicates; after the new album is
      in beets, main() re-runs ``beet move`` on each so ``%aunique``
      re-evaluates their paths too — otherwise one edition gets the
      ``[YEAR]`` suffix and the other stays unsuffixed, which looks
      asymmetric in Plex/Meelo.
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
    kept_duplicate = False
    sibling_mbids: set[str] = set()
    timeout = HARNESS_TIMEOUT

    try:
        while True:
            ready, _, _ = select.select([proc.stdout.fileno()], [], [], timeout)
            if not ready:
                print(f"  [TIMEOUT] No output for {timeout}s", file=sys.stderr)
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait()
                return 2, [], False, frozenset()

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
                # Always answer "keep". Never "remove".
                #
                # Live 2026-04-20 data-loss event (Shearwater "Palo Santo"):
                # beets' ``find_duplicates()`` returned a cross-MBID sibling
                # pressing even with ``duplicate_keys: [albumartist, album,
                # mb_albumid]`` in config. The old "same-MBID → remove" branch
                # then called beets' ``task.should_remove_duplicates = True``,
                # which iterates ``duplicate_items()`` and calls
                # ``util.remove(item.path)`` on every item in every found
                # duplicate — blast radius is whatever find_duplicates chose,
                # not what we asked for. The 11-track 2006 sibling (mb=
                # 157b51f8...) lost every mp3 on disk when the 19-track 2007
                # reissue (mb=168d7fea...) re-imported.
                #
                # Structural fix: the destructive branch is unrepresentable
                # from here. If a stale same-MBID album exists, main()
                # removes it AFTER the new album is successfully in beets,
                # using the numeric primary-key selector ``id:<stale>``
                # which cannot match any other row. At this callsite we
                # unconditionally answer "keep" — preserving every album
                # beets flagged, including legitimate different-MBID
                # siblings we must not touch.
                #
                # Sibling MBIDs (non-target hits) are collected so main()
                # can re-run ``beet move`` on each after the import —
                # otherwise ``%aunique`` disambiguates only the new album,
                # leaving the sibling at an un-suffixed path (ugly, and
                # music scanners see two differently-shaped folders for
                # the same album name).
                #
                # ``kept_duplicate = True`` unconditionally so post-import
                # ``beet move mb_albumid:<target>`` re-runs ``%aunique`` to
                # fix the new album's path.
                dup_mbids = msg.get("duplicate_mbids", [])
                proc.stdin.write(json.dumps({"action": "keep"}) + "\n")
                proc.stdin.flush()
                kept_duplicate = True
                for dm in dup_mbids:
                    if dm and dm != mb_release_id:
                        sibling_mbids.add(dm)
                if mb_release_id in dup_mbids:
                    # Rare: happens when an operator ran an upgrade
                    # without going through dispatch (or a race allowed
                    # two rows with the same MBID briefly). main() will
                    # remove the stale entry by its exact beets numeric
                    # id after the import finishes.
                    print(
                        f"  [DUP] Same MBID in library "
                        f"(existing: {dup_mbids}); keeping both — "
                        "main() will remove stale by beets id post-import",
                        file=sys.stderr)
                else:
                    print(
                        f"  [DUP] Different edition (existing: {dup_mbids}), "
                        "keeping both",
                        file=sys.stderr)

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
                    return 4, [], False, frozenset()

                cand = candidates[matched_idx]
                dist = cand.get("distance", 1.0)

                if dist > MAX_DISTANCE:
                    proc.stdin.write(json.dumps({"action": "skip"}) + "\n")
                    proc.stdin.flush()
                    print(f"  [REJECT] distance={dist:.4f} > {MAX_DISTANCE}", file=sys.stderr)
                    if proc.poll() is None:
                        proc.wait()
                    return 2, [], False, frozenset()

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
        return 2, beets_lines, kept_duplicate, frozenset(sibling_mbids)

    return ((0 if applied else 2), beets_lines, kept_duplicate,
            frozenset(sibling_mbids))


def _apply_disambiguation(
    mbid: str,
    beets: BeetsDB,
    album_path: str,
    r: ImportResult,
) -> str:
    """Run the post-import ``beet move`` and update ImportResult/path.

    Never raises. Returns the (possibly updated) album path. On clean
    exit, sets ``r.postflight.disambiguated = True`` and re-reads the
    path from beets DB. On any failure mode (timeout, OSError,
    non-zero rc), records a typed ``DisambiguationFailure`` on
    ``r.postflight.disambiguation_failure`` and returns the original
    ``album_path`` unchanged.

    Extracting this from ``main()`` makes the call-site contract
    testable in isolation: the album-on-disk state is decoupled from
    disambiguation success, ImportResult JSON always emits cleanly,
    and ``r.exit_code`` / ``r.decision`` are never touched by a
    disambiguation failure.
    """
    move_failure = _run_disambiguation_move(mbid)
    if move_failure is not None:
        # Album is already imported to beets — only the post-import
        # path-disambiguation move did not exit cleanly. Surface the
        # typed reason so the audit trail in download_log shows *why*
        # without lying that disambiguation succeeded.
        _log(f"  [DISAMBIGUATE] beet move failed "
             f"({move_failure.reason}): {move_failure.detail}")
        r.postflight.disambiguation_failure = move_failure
        return album_path

    pf_info_after = beets.get_album_info(mbid, _rank_cfg)
    if pf_info_after:
        new_path = pf_info_after.album_path
        if new_path != album_path:
            _log(f"  [DISAMBIGUATE] Path changed: {album_path} → {new_path}")
            album_path = new_path
            r.postflight.imported_path = new_path
        else:
            _log(f"  [DISAMBIGUATE] Path unchanged (already unique)")
    r.postflight.disambiguated = True
    return album_path


def _run_disambiguation_move(mbid: str) -> DisambiguationFailure | None:
    """Run ``beet move mb_albumid:<mbid>`` once, never raise (issue #127).

    Mirrors ``lib/release_cleanup.py::_run_remove_selector``: one place
    owns the subprocess invocation, catches every fragile failure mode
    (``TimeoutExpired``, ``OSError`` from a missing ``beet`` binary,
    non-zero rc), and returns a typed ``DisambiguationFailure`` (with a
    ``Literal["timeout","nonzero_rc","exception"]`` reason tag) so the
    caller and downstream consumers can classify failures without
    parsing the ``detail`` string. Returns ``None`` on a clean rc=0
    exit.

    Why this is its own function: the call site fires *after* beets
    has already imported the album to disk. An uncaught exception here
    would crash ``import_one.py`` before it could emit the
    ``__IMPORT_RESULT__`` sentinel — the caller would treat the import
    as failed even though the album is on disk, leaving a "semi-lie"
    that can trigger duplicate force-import attempts (the bug PR #126
    flagged as out-of-scope follow-up to #123).
    """
    try:
        proc = subprocess.run(
            [BEET_BIN, "move", f"mb_albumid:{mbid}"],
            capture_output=True, text=True, timeout=120,
            env=beets_subprocess_env(),
        )
    except subprocess.TimeoutExpired as exc:
        return DisambiguationFailure(
            reason="timeout", detail=f"timeout after {exc.timeout}s")
    except OSError as exc:
        return DisambiguationFailure(
            reason="exception", detail=f"{type(exc).__name__}: {exc}")

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip().splitlines()
        last = stderr[-1] if stderr else ""
        detail = (f"rc={proc.returncode}: {last}"
                  if last else f"rc={proc.returncode}")
        return DisambiguationFailure(reason="nonzero_rc", detail=detail)

    return None


# ---------------------------------------------------------------------------
# Same-MBID stale-entry handling — POST-import, by beets numeric id
# ---------------------------------------------------------------------------
#
# The 2026-04-20 Palo Santo event trained us off two things at once:
#
# 1. Never answer "remove" to beets' resolve_duplicate (cross-MBID blast
#    radius — see run_import).
# 2. Never destroy the existing copy BEFORE the replacement is in beets.
#    Codex (PR #131 round 1 P1) flagged this: a pre-flight remove that
#    is followed by a harness timeout leaves the request with no files
#    at all.
#
# The safe shape: capture the stale row's beets id BEFORE the import
# (no destructive action), let the import land the new album at a
# disambiguated path via ``%aunique``, THEN remove the stale row by its
# primary key. The ``id:<N>`` selector is a ``SELECT ... WHERE id = ?``
# — unique by SQLite auto-increment, so it is physically impossible to
# hit a sibling pressing.


def _capture_stale_beets_id(
    mbid: str, beets: BeetsDB) -> int | None:
    """Return the beets numeric id of the stale same-MBID album, or None.

    Called pre-import, before any destructive action. If the album is
    already present exactly once (true by construction — beets enforces
    unique insertion at import time, so there's at most one row with a
    given MBID before our import starts), we remember its id. After
    the new album imports, both briefly coexist; we then remove the
    captured id. If the album isn't in beets at all, returns None and
    the caller simply skips the post-import cleanup.

    Uses ``BeetsDB.locate()`` rather than ``get_album_info()`` because
    ``get_album_info()`` returns ``None`` for albums with no readable
    track/bitrate rows — and "partial stale import with broken items"
    is exactly the pathology this cleanup exists to handle (Codex PR
    #131 round 2 P2). ``locate()`` only queries ``albums`` so the
    presence check and id capture don't depend on item-level data.
    """
    if not mbid:
        return None
    location = beets.locate(mbid)
    if location.kind != "exact" or location.album_id is None:
        return None
    return location.album_id


def _remove_stale_by_id_logged(stale_id: int) -> SelectorFailure | None:
    """Post-import cleanup: delete the stale same-MBID album by beets id.

    Thin logger around ``remove_album_by_beets_id``. Always logs the
    outcome so the import audit trail (stderr → soularr journal)
    shows exactly which beets row was removed and why.
    """
    _log(f"[POST-IMPORT CLEANUP] Removing stale same-MBID entry "
         f"(beet remove -d id:{stale_id})")
    failure = remove_album_by_beets_id(stale_id)
    if failure is None:
        _log(f"  [POST-IMPORT CLEANUP] OK — id:{stale_id} removed")
    else:
        _log(f"  [POST-IMPORT CLEANUP] FAILED id:{stale_id} "
             f"({failure.reason}): {failure.detail}. "
             "Two albums with same MBID now in beets; "
             "operator should run ban-source cleanup.")
    return failure


def _canonicalize_siblings(sibling_mbids: frozenset[str]) -> None:
    """Re-run ``beet move`` on each sibling MBID after a kept-duplicate import.

    When beets' ``%aunique`` disambiguates the new album (adding a
    ``[YEAR]`` suffix because a different-edition sibling exists), the
    sibling album's path is NOT automatically re-evaluated — it stays
    at whatever path it was originally imported under. Left alone, you
    end up with an asymmetric library like
    ``/Shearwater/2006 - Palo Santo/`` (plain, old) vs
    ``/Shearwater/2007 - Palo Santo [2007]/`` (disambiguated, new).
    Running ``beet move mb_albumid:<sibling>`` on each sibling here
    re-evaluates ``%aunique`` for its path too, so both editions end
    up shaped consistently.

    Never raises — delegates to ``_run_disambiguation_move`` which
    returns a typed ``DisambiguationFailure`` on any subprocess
    error. Per-sibling failures are logged but do not abort the
    remaining moves; the import itself is already on disk and any
    failed sibling move just means that sibling stays at its old
    path until something re-runs ``beet move`` on it.
    """
    if not sibling_mbids:
        return
    _log(f"[CANONICALIZE] Re-running beet move for "
         f"{len(sibling_mbids)} sibling MBID(s) so %aunique stays symmetric")
    for sibling in sibling_mbids:
        _log(f"  [CANONICALIZE] beet move mb_albumid:{sibling}")
        failure = _run_disambiguation_move(sibling)
        if failure is not None:
            _log(f"  [CANONICALIZE] sibling {sibling} move failed "
                 f"({failure.reason}): {failure.detail} — sibling stays "
                 "at its current path, re-run `beet move` manually later")


# ---------------------------------------------------------------------------
# Pipeline DB updates
# ---------------------------------------------------------------------------

def update_pipeline_db(request_id, status, imported_path=None, distance=None, scenario=None):
    """Update pipeline DB status. Best-effort — failures logged but don't block."""
    try:
        from lib.pipeline_db import PipelineDB
        dsn = os.environ.get("PIPELINE_DB_DSN", "postgresql://soularr@localhost/soularr")
        db = PipelineDB(dsn)
        extra = {}
        if imported_path:
            extra["imported_path"] = imported_path
        if distance is not None:
            extra["beets_distance"] = distance
        if scenario:
            extra["beets_scenario"] = scenario
        db.update_status(request_id, status, **extra)
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
    print(r.to_sentinel_line(), flush=True)
    sys.exit(r.exit_code)


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
            if request_id:
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

    # --- Spectral analysis (pre-conversion) ---
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_spectral_grade: str | None = None
    existing_spectral_bitrate: int | None = None
    try:
        from lib.spectral_check import analyze_album as spectral_analyze
        spectral_result = spectral_analyze(args.path, trim_seconds=30)
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

    has_target = bool(args.verified_lossless_target)
    # V0 must keep the lossless source when either a second conversion pass
    # is planned (``verified_lossless_target``) OR the caller asked us to
    # hold it until the quality decision (``--preserve-source``, issue #111).
    keep_v0_source = has_target or args.preserve_source
    if not keep_lossless:
        _log(f"[CONVERT] {args.path}")
        converted, failed, original_ext = convert_lossless(
            args.path, V0_SPEC, dry_run=args.dry_run,
            keep_source=keep_v0_source)
        r.conversion.converted = converted
        r.conversion.failed = failed
        if converted > 0:
            r.conversion.was_converted = True
            r.conversion.original_filetype = original_ext or "flac"
            r.conversion.target_filetype = "mp3"
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
        post_conv_br = _get_folder_min_bitrate(args.path, ext_filter=v0_ext_filter) if converted > 0 else None
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
            f for f in os.listdir(args.path) if _is_lossless_file(f, args.path))
        has_non_flac = any(
            not f.lower().endswith(".flac") for f in lossless_files)
        if has_non_flac and not args.dry_run:
            _log(f"[NORMALIZE] Converting non-FLAC lossless → FLAC")
            converted, failed, original_ext = convert_lossless(
                args.path, FLAC_SPEC)
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

    if args.dry_run:
        r.decision = "dry_run"
        _emit_and_exit(r)

    # --- Quality comparison ---
    new_bitrates = _get_folder_bitrates(args.path, ext_filter=v0_ext_filter)
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

    # Verified lossless: single source of truth in quality.py
    will_be_verified_lossless = determine_verified_lossless(
        args.target_format, spectral_grade, converted, is_transcode)

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
    qd = quality_decision_stage(new_m, existing_m, is_transcode=is_transcode,
                                cfg=_rank_cfg)
    decision = qd.decision
    r.decision = decision

    if qd.is_terminal:
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
            _remove_files_by_ext(args.path, "." + V0_SPEC.extension)
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

    if (will_be_verified_lossless and converted > 0
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
        _log(f"[TARGET] Converting verified lossless → {target_spec.label}")
        r.v0_verification_bitrate = post_conv_br
        # If target has same extension as V0 (.mp3), remove V0 files first
        # so convert_lossless doesn't skip due to existing output files.
        if target_spec.extension == V0_SPEC.extension:
            _remove_files_by_ext(args.path, "." + V0_SPEC.extension)
        target_converted, target_failed, _ = convert_lossless(
            args.path, target_spec, dry_run=args.dry_run, keep_source=True)
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
            _remove_files_by_ext(args.path, "." + V0_SPEC.extension)
        # Remove original lossless files (consumed by target conversion)
        _remove_lossless_files(args.path)
        # Update measurements for the target format — include both the
        # measured min/avg bitrate and the declared format label so the
        # rank model classifies against the contract (e.g. "opus 128")
        # rather than the measured VBR number (which lands 95-150 kbps
        # depending on material).
        target_bitrates = _get_folder_bitrates(args.path)
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
            verified_lossless=True,
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
        _remove_lossless_files(args.path)
        _log(f"  [CLEANUP] Removed lossless originals "
             f"(target skipped or preserve-source approved)")

    # --- Capture stale beets id BEFORE the import ---
    #
    # The 2026-04-20 Palo Santo data-loss event trained us off the
    # mid-import "remove" path entirely (see run_import). Instead we
    # capture the stale row's beets numeric id here and remove it
    # AFTER the import succeeds, by its exact primary-key selector
    # ``id:<N>`` — the narrowest possible scope (cannot match siblings
    # by construction — SQLite autoincrement PKs are unique).
    #
    # Codex (PR #131 round 1 P1) flagged the alternative design where
    # pre-flight deleted the stale row before ``run_import``: if the
    # harness times out / crashes / rejects the candidate, we're left
    # with no album at all. Capture-then-import-then-remove preserves
    # the invariant that the existing copy survives until the
    # replacement is confirmed on disk.
    stale_beets_id = (_capture_stale_beets_id(mbid, beets)
                      if already_in_beets else None)
    if stale_beets_id is not None:
        _log(f"[PRE-FLIGHT] Captured stale beets id:{stale_beets_id} for "
             f"post-import cleanup (will remove after upgrade lands)")

    # --- Import ---
    _log(f"[IMPORT] {args.path} → beets (mbid={mbid})")
    rc, beets_lines, kept_duplicate, sibling_mbids = run_import(
        args.path, mbid)
    r.beets_log = beets_lines

    if rc != 0:
        r.exit_code = rc
        r.decision = "import_failed" if rc == 2 else "mbid_missing" if rc == 4 else "import_failed"
        r.error = next((line for line in reversed(beets_lines) if line.strip()),
                       f"Harness returned rc={rc}")
        _log(f"[ERROR] Import failed (rc={rc})")
        _emit_and_exit(r)

    # --- Post-import cleanup: remove stale same-MBID row by beets id ---
    #
    # If the album was already in beets when we started (stale_beets_id
    # captured above), there are now TWO rows with the target MBID —
    # the stale one and the fresh import (beets' ``%aunique`` placed
    # the new album at a disambiguated path, so both coexist on disk).
    # Remove the stale one by its exact album-mode numeric primary
    # key. ``beet remove -a -d id:<N>`` runs in album mode and matches
    # ``albums.id = N`` exactly — cannot match any other row.
    #
    # Codex (PR #131 round 2 P3): treat any cleanup failure as
    # ``import_failed``. The new album IS on disk at this point, but
    # leaving the stale row in beets silently leads to a split-brain
    # library where subsequent upgrades may capture the wrong id or
    # reason about the stale row's paths/bitrates. Failing explicitly
    # surfaces the problem so the operator's ban-source cleanup can
    # run before the request is considered complete.
    if stale_beets_id is not None:
        failure = _remove_stale_by_id_logged(stale_beets_id)
        if failure is not None:
            r.exit_code = 2
            r.decision = "import_failed"
            r.error = (f"Stale beets album id:{stale_beets_id} "
                       f"(mbid={mbid}) could not be removed: "
                       f"{failure.reason}: {failure.detail}. The new "
                       "album is on disk but two same-MBID rows now "
                       "exist in beets. Operator must clean up via "
                       "ban-source before the request can be marked "
                       "complete.")
            _log(f"[ERROR] {r.error}")
            beets.close()
            _emit_and_exit(r)

    # --- Post-flight verification ---
    pf_info = beets.get_album_info(mbid, _rank_cfg)
    if not pf_info:
        r.exit_code = 2
        r.decision = "import_failed"
        r.error = f"Post-flight: MBID {mbid} NOT in beets DB after import"
        _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)

    # Extra guard: if pf_info still resolves to the stale id, something
    # is seriously wrong (cleanup claimed success but beets still holds
    # the row). Fail loudly rather than quality-gate against the stale.
    if stale_beets_id is not None and pf_info.album_id == stale_beets_id:
        r.exit_code = 2
        r.decision = "import_failed"
        r.error = (f"Post-flight resolved to stale beets_id={stale_beets_id} "
                   f"for mbid={mbid} despite cleanup reporting success — "
                   "beets DB is in an inconsistent state. Operator must "
                   "clean up via ban-source.")
        _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)

    r.postflight = PostflightInfo(beets_id=pf_info.album_id,
                                   track_count=pf_info.track_count,
                                   imported_path=pf_info.album_path)
    album_path = pf_info.album_path
    _log(f"[POST-FLIGHT OK] mbid={mbid}, beets_id={pf_info.album_id}, "
         f"tracks={pf_info.track_count}, path={album_path}")

    # --- Post-import %aunique disambiguation ---
    # When beets kept a different edition during duplicate resolution,
    # %aunique doesn't fully disambiguate at import time (the new album
    # gets no disambiguator if its field value is empty). Running
    # `beet move` re-evaluates all editions and fixes the paths.
    if kept_duplicate:
        _log(f"[DISAMBIGUATE] Running beet move for album id:{pf_info.album_id}")
        album_path = _apply_disambiguation(mbid, beets, album_path, r)
        # Also canonicalize the sibling editions beets flagged as
        # duplicates — without this, the new album gets the ``[YEAR]``
        # suffix while the sibling stays un-suffixed (asymmetric
        # folder layout in Plex/Meelo).
        _canonicalize_siblings(sibling_mbids)

    # --- Post-import extension check ---
    # Detect .bak files (known bug: track 01 sometimes renamed to .bak during import)
    VALID_AUDIO_EXT = {".mp3", ".flac", ".ogg", ".opus", ".m4a", ".aac", ".wma", ".wav"}
    item_paths = beets.get_item_paths(mbid)
    bad_ext_files = []
    for item_id, item_path in item_paths:
        ext = os.path.splitext(item_path)[1].lower()
        if ext not in VALID_AUDIO_EXT and os.path.isfile(item_path):
            # Determine correct extension from actual audio format
            try:
                probe = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "format=format_name",
                     "-of", "csv=p=0", item_path],
                    capture_output=True, text=True, timeout=15)
                fmt = probe.stdout.strip().split(",")[0] if probe.stdout.strip() else ""
                ext_map = {"mp3": ".mp3", "flac": ".flac", "ogg": ".ogg",
                           "opus": ".opus", "wav": ".wav", "mp4": ".m4a"}
                correct_ext = ext_map.get(fmt, ".mp3")
            except Exception:
                correct_ext = ".mp3"
            new_path = os.path.splitext(item_path)[0] + correct_ext
            _log(f"[EXT-FIX] {os.path.basename(item_path)} → {os.path.basename(new_path)}")
            os.rename(item_path, new_path)
            # Update beets DB (writable connection for this fix)
            import sqlite3 as _sqlite3
            from lib.beets_db import DEFAULT_BEETS_DB
            with _sqlite3.connect(DEFAULT_BEETS_DB) as fix_conn:
                fix_conn.execute("UPDATE items SET path = ? WHERE id = ?",
                                 (new_path.encode(), item_id))
            bad_ext_files.append(os.path.basename(item_path))
    if bad_ext_files:
        r.postflight.bad_extensions = bad_ext_files
        _log(f"[EXT-FIX] Fixed {len(bad_ext_files)} file(s) with bad extensions")

    # --- Force library modes ---
    # Guards against any subprocess layer that dropped umask and created
    # 0o755 dirs despite the systemd unit's UMask=0000 — see GH #84.
    fix_library_modes(album_path)

    # --- Cleanup staged dir ---
    if os.path.isdir(args.path):
        for root, dirs, files in os.walk(args.path, topdown=False):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                try:
                    os.rmdir(os.path.join(root, d))
                except OSError:
                    pass
        try:
            os.rmdir(args.path)
        except OSError:
            pass
        parent = os.path.dirname(args.path)
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
    if is_transcode:
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
