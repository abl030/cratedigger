"""Beets validation — dry-run import via the beets harness.

Pure function: takes a harness path, album path, and MBID, returns a
validation result dict. No global state, no config dependency.
"""

import json
import logging
import subprocess as sp

logger = logging.getLogger("soularr")


def beets_validate(harness_path, album_path, mb_release_id, distance_threshold=0.15):
    """Dry-run beets import with specific MBID. Returns validation result.

    Args:
        harness_path: Path to the beets harness script (run_beets_harness.sh)
        album_path: Path to the album directory to validate
        mb_release_id: Target MusicBrainz release ID
        distance_threshold: Maximum acceptable distance (default 0.15)

    Returns: {"valid": bool, "distance": float|None, "mbid_found": bool,
              "scenario": str, "detail": str, "error": str|None}
    """
    cmd = [harness_path, "--pretend", "--noincremental",
           "--search-id", mb_release_id, album_path]
    result = {"valid": False, "distance": None, "mbid_found": False, "error": None}

    logger.info(f"BEETS_VALIDATE: path={album_path}, target_mbid={mb_release_id}, "
                f"threshold={distance_threshold}")
    logger.info(f"BEETS_VALIDATE: cmd={' '.join(cmd)}")

    try:
        proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
    except Exception as e:
        result["error"] = f"Failed to start harness: {e}"
        logger.error(f"BEETS_VALIDATE: {result['error']}")
        return result

    got_choose_match = False
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
                candidates = msg.get("candidates", [])
                logger.info(f"BEETS_VALIDATE: {len(candidates)} candidates, "
                            f"looking for mbid={mb_release_id}")
                for i, cand in enumerate(candidates):
                    cand_mbid = cand.get("album_id", "")
                    cand_dist = cand.get("distance", "?")
                    cand_album = cand.get("album", "?")
                    logger.info(f"BEETS_VALIDATE:   candidate[{i}]: "
                                f"mbid={cand_mbid}, dist={cand_dist}, album={cand_album}")
                # Check if target MBID was found and distance is acceptable
                for cand in candidates:
                    if cand.get("album_id") == mb_release_id:
                        result["mbid_found"] = True
                        result["distance"] = cand["distance"]
                        extra_tracks = cand.get("extra_tracks", 0)
                        if extra_tracks > 0:
                            result["scenario"] = "extra_tracks"
                            result["detail"] = f"MB has {extra_tracks} more tracks than local files"
                        elif cand["distance"] <= distance_threshold:
                            result["valid"] = True
                            result["scenario"] = "strong_match"
                            result["detail"] = f"distance={cand['distance']}"
                        else:
                            result["scenario"] = "high_distance"
                            result["detail"] = f"distance={cand['distance']}"
                        break
                if not result["mbid_found"]:
                    result["scenario"] = "mbid_not_found"
                    result["detail"] = f"Target MBID {mb_release_id} not in candidates"
                logger.info(f"BEETS_VALIDATE: valid={result['valid']}, "
                            f"scenario={result['scenario']}, detail={result['detail']}")
                # Always skip (dry-run)
                proc.stdin.write('{"action":"skip"}\n')
                proc.stdin.flush()

            elif msg_type in ("choose_item", "resolve_duplicate", "should_resume"):
                proc.stdin.write('{"action":"skip"}\n')
                proc.stdin.flush()

            elif msg_type == "session_end":
                break
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"BEETS_VALIDATE: exception: {e}")
    finally:
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

    logger.info(f"BEETS_VALIDATE: result={result}")
    return result
