"""Beets validation — dry-run import via the beets harness.

Takes a harness path, album path, and MBID, returns a typed
ValidationResult. No global state, no config dependency.
"""

import json
import logging
import subprocess as sp

import os
import sys

# Ensure lib/ is importable whether called from project root or lib/
_lib_dir = os.path.dirname(os.path.abspath(__file__))
if _lib_dir not in sys.path:
    sys.path.insert(0, _lib_dir)

from quality import ValidationResult, CandidateSummary

logger = logging.getLogger("soularr")


def _candidate_from_harness(cand: dict, target_mbid: str) -> CandidateSummary:
    """Build a CandidateSummary from a beets harness candidate dict.

    Defensive str-coercion on `album_id` for the `is_target` flag — the
    harness already coerces (post-fix) but Discogs candidates can carry
    int IDs from older logs or alternate code paths. Belt-and-braces so
    str/int drift can never re-introduce the "mbid_not_found" bug.
    """
    is_target = str(cand.get("album_id", "")) == str(target_mbid)
    d = {**cand, "is_target": is_target}
    return CandidateSummary.from_dict(d)


def beets_validate(harness_path, album_path, mb_release_id, distance_threshold=0.15):
    """Dry-run beets import with specific MBID. Returns ValidationResult.

    Args:
        harness_path: Path to the beets harness script (run_beets_harness.sh)
        album_path: Path to the album directory to validate
        mb_release_id: Target MusicBrainz release ID
        distance_threshold: Maximum acceptable distance (default 0.15)

    Returns: ValidationResult with candidates, distance, scenario, etc.
    """
    cmd = [harness_path, "--pretend", "--noincremental",
           "--search-id", mb_release_id, album_path]
    result = ValidationResult(target_mbid=mb_release_id)

    logger.info(f"BEETS_VALIDATE: path={album_path}, target_mbid={mb_release_id}, "
                f"threshold={distance_threshold}")
    logger.info(f"BEETS_VALIDATE: cmd={' '.join(cmd)}")

    try:
        proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
    except Exception as e:
        result.error = f"Failed to start harness: {e}"
        logger.error(f"BEETS_VALIDATE: {result.error}")
        return result
    assert proc.stdin is not None
    assert proc.stdout is not None
    assert proc.stderr is not None

    got_choose_match = False
    # Kill harness if it hangs — 120s total timeout
    import threading
    timed_out = False
    def _timeout_kill():
        nonlocal timed_out
        timed_out = True
        logger.error("BEETS_VALIDATE: harness timed out after 120s, killing")
        proc.kill()
    timer = threading.Timer(120.0, _timeout_kill)
    timer.start()
    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                logger.debug(f"BEETS_VALIDATE: non-JSON line: {line[:200]}")
                continue

            msg_type = msg.get("type", "")
            logger.info(f"BEETS_VALIDATE: msg type={msg_type}")

            if msg_type == "choose_match":
                got_choose_match = True
                raw_candidates = msg.get("candidates", [])
                result.candidate_count = len(raw_candidates)
                result.candidates = [
                    _candidate_from_harness(c, mb_release_id)
                    for c in raw_candidates
                ]
                # Store local file info and beets recommendation
                result.items = msg.get("items", [])
                result.local_track_count = msg.get("item_count")
                result.recommendation = msg.get("recommendation")
                result.path = msg.get("path")
                logger.info(f"BEETS_VALIDATE: {len(raw_candidates)} candidates, "
                            f"looking for mbid={mb_release_id}")
                for i, cand in enumerate(raw_candidates):
                    cand_mbid = cand.get("album_id", "")
                    cand_dist = cand.get("distance", "?")
                    cand_album = cand.get("album", "?")
                    logger.info(f"BEETS_VALIDATE:   candidate[{i}]: "
                                f"mbid={cand_mbid}, dist={cand_dist}, album={cand_album}")
                # Check if target MBID was found and distance is acceptable.
                # str() on both sides — Discogs candidates can carry int
                # album_ids and DB-stored mb_release_ids are always str.
                for cand in raw_candidates:
                    if str(cand.get("album_id", "")) == str(mb_release_id):
                        result.mbid_found = True
                        result.distance = cand["distance"]
                        extra_tracks_raw = cand.get("extra_tracks", [])
                        # Handle both old (int) and new (list) format
                        n_extra = len(extra_tracks_raw) if isinstance(extra_tracks_raw, list) else extra_tracks_raw
                        if n_extra > 0:
                            result.scenario = "extra_tracks"
                            result.detail = f"MB has {n_extra} more tracks than local files"
                        elif cand["distance"] <= distance_threshold:
                            result.valid = True
                            result.scenario = "strong_match"
                            result.detail = f"distance={cand['distance']}"
                        else:
                            result.scenario = "high_distance"
                            result.detail = f"distance={cand['distance']}"
                        break
                if not result.mbid_found:
                    result.scenario = "mbid_not_found"
                    result.detail = f"Target MBID {mb_release_id} not in candidates"
                logger.info(f"BEETS_VALIDATE: valid={result.valid}, "
                            f"scenario={result.scenario}, detail={result.detail}")
                # Always skip (dry-run)
                proc.stdin.write('{"action":"skip"}\n')
                proc.stdin.flush()

            elif msg_type in ("choose_item", "resolve_duplicate", "should_resume"):
                proc.stdin.write('{"action":"skip"}\n')
                proc.stdin.flush()

            elif msg_type == "session_end":
                break
    except Exception as e:
        result.error = str(e)
        logger.error(f"BEETS_VALIDATE: exception: {e}")
    finally:
        timer.cancel()
        if timed_out:
            result.error = "Harness timed out after 120s"
        stderr_out = ""
        try:
            stderr_out = proc.stderr.read()
        except Exception:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except sp.TimeoutExpired:
            proc.kill()

    if stderr_out:
        logger.warning(f"BEETS_VALIDATE: stderr: {stderr_out[:500]}")
    if not got_choose_match:
        logger.warning(f"BEETS_VALIDATE: harness never sent choose_match!")

    logger.info(f"BEETS_VALIDATE: result valid={result.valid}, scenario={result.scenario}")
    return result
