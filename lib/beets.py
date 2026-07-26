"""Beets validation — dry-run import via the beets harness.

Takes a harness path, album path, and MBID, returns a typed
ValidationResult. No global state, no config dependency.
"""

import json
import logging
import subprocess as sp

import msgspec

from lib.quality import HarnessSessionEvidence, ValidationResult, ChooseMatchMessage
from lib.util import beets_subprocess_env

logger = logging.getLogger("cratedigger")

#: Scenario for a run that COMPLETED without error and never offered a match
#: to review. It names the OBSERVATION — nothing was put in front of us — and
#: deliberately not a cause: no importable audio, a session that ended early
#: and a harness that exited quietly are all consistent with it.
#: ``HarnessSessionEvidence`` carries what separates them (issue #888).
#:
#: The guard is ``result.error is None``, which is the same predicate the
#: issue-#888 investigation used to separate the 276 genuine rows from the
#: error branches. A run that RECORDED an error is never named this, because
#: at least one such run — the strict-decode refusal — is a case where beets
#: DID offer a match and our decoder declined it.
NO_CHOOSE_MATCH_SCENARIO = "no_choose_match"

#: Scenario for a run that recorded an error before any match could be
#: reviewed: the harness would not start, the strict wire decode refused a
#: ``choose_match``, the read loop raised, or the 120s timeout fired. It
#: claims only that validation did not complete — never that beets offered
#: nothing, which for the strict-decode case would be false.
VALIDATION_ERROR_SCENARIO = "validation_error"

#: The clause each scenario composes for itself, in front of everything
#: quoted from the harness. Kept as the FIRST ``;``-separated segment so the
#: sentence Cratedigger asserts is separable from wire-controlled text.
NO_CHOOSE_MATCH_CLAUSE = "beets harness ended without offering a match to review"
VALIDATION_ERROR_CLAUSE = "beets validation did not complete, so no match was reviewed"

#: How much of the harness's stderr to persist alongside the scenario. The
#: full text still goes to the journal; this is the bounded audit copy.
_STDERR_TAIL_CHARS = 4000

#: Bounds on the composed ``detail``, which reaches ``download_log
#: .beets_detail`` and the Recents card. A single 500 KB newline-free stderr
#: line is a real shape (measured), and neither the DB column nor the card is
#: the place for it — the bounded tail in ``harness_session`` is.
_STDERR_LINE_CHARS = 500
_DETAIL_MAX_CHARS = 2000


def _stderr_tail(stderr_out: str) -> str | None:
    """The bounded tail of harness stderr, or None when it said nothing."""
    trimmed = stderr_out.strip()
    if not trimmed:
        return None
    return trimmed[-_STDERR_TAIL_CHARS:]


def _last_stderr_line(stderr_out: str) -> str | None:
    """The final non-empty stderr line — a traceback's exception line.

    Bounded from the FRONT: on a traceback's last line the exception type
    and message lead, and this copy is the human hint, not the audit. The
    unabridged tail lives in ``HarnessSessionEvidence.stderr_tail``.
    """
    for line in reversed(stderr_out.splitlines()):
        stripped = line.strip()
        if stripped:
            return stripped[:_STDERR_LINE_CHARS]
    return None


def _record_unmatched_run(
    result: ValidationResult,
    *,
    message_types: list[str],
    session_end_seen: bool,
    stderr_out: str,
) -> None:
    """Name and evidence a run that reached no reviewed match.

    Before issue #888 every one of these paths persisted nothing beyond one
    WARNING: the result went out with ``scenario=None`` and every
    denormalized column NULL, so the rejection reached the operator as the
    bare word "Rejected". 276 live rows across 215 requests landed there,
    181 of which a later triage preview judged importable.

    Two names, split on exactly the discriminator the investigation used —
    whether an error was recorded:

    * no error → ``no_choose_match``: the run completed and offered nothing.
    * an error → ``validation_error``: validation did not complete. Merging
      this into the first name would assert beets offered nothing, which is
      FALSE for the strict-decode refusal (beets offered a match; we
      declined to decode it) and unprovable for the other three.

    Either way the stamp records observations only. It never claims WHY —
    the message types, the session-end flag, the recorded error and the
    stderr tail are what let the next person work that out.
    """
    if result.error is None:
        result.scenario = NO_CHOOSE_MATCH_SCENARIO
        clause = NO_CHOOSE_MATCH_CLAUSE
    else:
        result.scenario = VALIDATION_ERROR_SCENARIO
        clause = VALIDATION_ERROR_CLAUSE
    result.harness_session = HarnessSessionEvidence(
        message_types=message_types,
        session_end_seen=session_end_seen,
        stderr_tail=_stderr_tail(stderr_out),
    )
    observed = ", ".join(message_types) if message_types else "none"
    # The clause stands alone as segment 0; everything the harness chose —
    # its message type names, its error text, its stderr — follows the first
    # ``;`` so it can never be read as part of our assertion.
    parts = [clause, f"harness messages: {observed}"]
    if result.error:
        parts.append(result.error)
    last_line = _last_stderr_line(stderr_out)
    if last_line:
        parts.append(f"harness stderr ended: {last_line}")
    detail = "; ".join(parts)
    if len(detail) > _DETAIL_MAX_CHARS:
        detail = detail[:_DETAIL_MAX_CHARS - 1] + "…"
    result.detail = detail


def beets_validate(
    harness_path: str,
    album_path: str,
    mb_release_id: str,
    distance_threshold: float = 0.15,
) -> ValidationResult:
    """Dry-run beets import with specific MBID. Returns ValidationResult.

    Args:
        harness_path: Path to the beets harness script (run_beets_harness.sh)
        album_path: Path to the album directory to validate
        mb_release_id: Target MusicBrainz release ID
        distance_threshold: Maximum acceptable distance (default 0.15)

    Returns: ValidationResult with candidates, distance, scenario, etc.

    **Invariant: the returned result always names a scenario.** Exactly one
    of three (issue #888):

    * a ``choose_match`` was decoded and decided — ``strong_match`` /
      ``high_distance`` / ``extra_tracks`` / ``mbid_not_found``;
    * no error was recorded and none was ever offered — ``no_choose_match``;
    * an error was recorded first — ``validation_error``.

    The last two carry ``harness_session`` evidence. Callers rely on the
    invariant and must not invent a placeholder scenario of their own.
    """
    cmd = [harness_path, "--pretend", "--noincremental",
           "--search-id", mb_release_id, album_path]
    result = ValidationResult(target_mbid=mb_release_id)

    logger.info(f"BEETS_VALIDATE: path={album_path}, target_mbid={mb_release_id}, "
                f"threshold={distance_threshold}")
    logger.info(f"BEETS_VALIDATE: cmd={' '.join(cmd)}")

    # Ordered-unique harness message types plus the session-end flag: the
    # audit that turns "no match was offered" from unrecoverable into
    # diagnosable.
    message_types: list[str] = []
    session_end_seen = False

    try:
        proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE,
                        text=True, errors="replace",
                        env=beets_subprocess_env())
    except Exception as e:
        result.error = f"Failed to start harness: {e}"
        logger.error(f"BEETS_VALIDATE: {result.error}")
        _record_unmatched_run(
            result,
            message_types=message_types,
            session_end_seen=session_end_seen,
            stderr_out="",
        )
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
            if (
                isinstance(msg_type, str)
                and msg_type
                and msg_type not in message_types
            ):
                message_types.append(msg_type)

            if msg_type == "choose_match":
                # Strict-typed decode at the wire boundary. The harness
                # has already normalised IDs to str via `_id_str`; any
                # int/null/type-mismatch here means the harness regressed
                # and we surface it loud instead of silently mismatching
                # downstream (the PR #98 bug).
                try:
                    cm = msgspec.convert(msg, type=ChooseMatchMessage)
                except msgspec.ValidationError as e:
                    # An undecodable message is NOT a processed match: none
                    # of the decision code below ran, so the run still owes
                    # the no-match evidence stamp unless a later message
                    # decodes cleanly (issue #888).
                    result.error = f"harness schema violation: {e}"
                    logger.error(f"BEETS_VALIDATE: {result.error}")
                    proc.stdin.write('{"action":"skip"}\n')
                    proc.stdin.flush()
                    continue
                got_choose_match = True

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
                session_end_seen = True
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
        # Log the full stderr — truncating loses the actual exception line
        # at the bottom of a Python traceback. The 2026-05-04 Psilodump
        # ``library.Library()`` crash had its root cause hidden behind a
        # 500-char slice. journald handles multi-line records fine.
        logger.warning("BEETS_VALIDATE: stderr:\n%s", stderr_out)
    if not got_choose_match:
        _record_unmatched_run(
            result,
            message_types=message_types,
            session_end_seen=session_end_seen,
            stderr_out=stderr_out,
        )
        logger.warning("BEETS_VALIDATE: %s", result.detail)

    logger.info(f"BEETS_VALIDATE: result valid={result.valid}, scenario={result.scenario}")
    return result
