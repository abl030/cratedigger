"""Beets validation — dry-run import via the beets harness.

Takes a harness path, album path, and MBID, returns a typed
ValidationResult. No global state, no config dependency.
"""

import json
import logging
import subprocess as sp

import msgspec

from lib.quality import ValidationResult, ChooseMatchMessage
from lib.util import beets_subprocess_env

logger = logging.getLogger("soularr")


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
        proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, text=True,
                        env=beets_subprocess_env())
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
                # Strict-typed decode at the wire boundary. The harness
                # has already normalised IDs to str via `_id_str`; any
                # int/null/type-mismatch here means the harness regressed
                # and we surface it loud instead of silently mismatching
                # downstream (the PR #98 bug).
                try:
                    cm = msgspec.convert(msg, type=ChooseMatchMessage)
                except msgspec.ValidationError as e:
                    result.error = f"harness schema violation: {e}"
                    logger.error(f"BEETS_VALIDATE: {result.error}")
                    proc.stdin.write('{"action":"skip"}\n')
                    proc.stdin.flush()
                    continue

                result.candidate_count = cm.candidate_count or len(cm.candidates)
                result.candidates = list(cm.candidates)
                # items is stored as list[dict] on ValidationResult
                # (out-of-scope wire type for #99); round-trip through
                # msgspec to get plain dicts from the typed HarnessItem
                # structs.
                result.items = [msgspec.to_builtins(i) for i in cm.items]
                result.local_track_count = cm.item_count
                result.recommendation = cm.recommendation
                result.path = cm.path

                logger.info(f"BEETS_VALIDATE: {len(cm.candidates)} candidates, "
                            f"looking for mbid={mb_release_id}")
                for i, cand in enumerate(cm.candidates):
                    logger.info(f"BEETS_VALIDATE:   candidate[{i}]: "
                                f"mbid={cand.mbid}, dist={cand.distance}, "
                                f"album={cand.album}")

                # Find the target MBID. Both sides are str (msgspec has
                # validated `cand.mbid` as str; `mb_release_id` comes
                # from the DB TEXT column).
                for cand in cm.candidates:
                    if cand.mbid == mb_release_id:
                        cand.is_target = True
                        result.mbid_found = True
                        result.distance = cand.distance
                        n_extra = len(cand.extra_tracks)
                        if n_extra > 0:
                            result.scenario = "extra_tracks"
                            result.detail = f"MB has {n_extra} more tracks than local files"
                        elif cand.distance <= distance_threshold:
                            result.valid = True
                            result.scenario = "strong_match"
                            result.detail = f"distance={cand.distance}"
                        else:
                            result.scenario = "high_distance"
                            result.detail = f"distance={cand.distance}"
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
