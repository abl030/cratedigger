"""Matching helpers extracted from cratedigger.py."""

from __future__ import annotations

import difflib
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence, TYPE_CHECKING

from lib.browse import (
    _browse_directories_for_ctx,
    _browse_directories_for_ctx_result,
    cache_browsed_directory,
    ensure_cache_user,
    rank_candidate_dirs,
)
from lib.quality import AUDIO_EXTENSIONS, CandidateScore, audio_file_matches
from lib.util import _track_titles_cross_check

_browse_directories = _browse_directories_for_ctx

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.context import CratediggerContext
    from cratedigger import SlskdDirectory, SlskdFile, TrackRecord


logger = logging.getLogger("cratedigger")


# Cap on flagged pre-filter-skip sample rows per search — bounds the
# JSONB blob for noisy peers. The aggregate count is always accurate
# (``MatchResult.pre_filter_skip_count``); only the sample is capped.
PRE_FILTER_SKIP_SAMPLE_CAP = 5


# ---------------------------------------------------------------------------
# Structured return types (U2 of search-escalation-and-forensics)
# ---------------------------------------------------------------------------
#
# `CandidateScore` is the wire-boundary type written into
# ``search_log.candidates`` JSONB. It lives in ``lib/quality/wire_types.py`` alongside
# other ``msgspec.Struct`` boundaries (``ImportResult``, ``ValidationResult``).
# Import it from ``lib.quality`` directly — no re-export here.

__all__ = [
    "AlbumMatchScore",
    "MatchResult",
    "album_match",
    "album_track_num",
    "check_for_match",
    "check_ratio",
    "classify_rejection_from_log_inputs",
    "classify_rejection_reason",
    "get_album_by_id",
    "matcher_score_top1_for",
]


@dataclass
class AlbumMatchScore:
    """Per-track filename-similarity score for one (user, dir, filetype) triple.

    Pure-function output of `album_match`. The strict-accept decision
    (every track above ratio AND _track_titles_cross_check) is computed by
    callers from these fields — this dataclass carries facts only.

    Internal — not a wire-boundary type. Forensic persistence uses
    `CandidateScore` (re-exported above from ``lib.quality``).
    """

    matched_tracks: int
    total_tracks: int
    avg_ratio: float
    missing_titles: list[str]
    best_per_track: list[float]


@dataclass
class MatchResult:
    """Return shape of `check_for_match`.

    `matched=True` means: a dir strictly accepted (every track above ratio AND
    cross-check passed). `directory` and `file_dir` describe that dir. The
    `candidates` list captures every dir the loop iterated, including the
    sub-count gate failures and cross-check rejections — used by U5 to persist
    a `search_log.candidates` JSONB blob for post-hoc forensics.

    Callers must use attribute access (`.matched`, `.directory`, `.file_dir`,
    `.candidates`). Tuple-unpacking is intentionally not supported — the old
    3-tuple shim silently dropped the `candidates` field.
    """

    matched: bool
    directory: Any
    file_dir: str
    candidates: list[CandidateScore] = field(default_factory=list)
    # Authoritative count of dirs rejected by the asymmetric pre-filter
    # before browse; sample rows in ``candidates`` are bounded by
    # ``PRE_FILTER_SKIP_SAMPLE_CAP``.
    pre_filter_skip_count: int = 0
    # U11 R22: dominant rejection reason for this matcher invocation.
    # ``None`` on a successful match or when the matcher produced no
    # classifiable candidates (broken_user / empty dirs_to_try / browse
    # crash). Computed by ``classify_rejection_reason`` from the final
    # candidate list + match outcome.
    rejection_reason: str | None = None
    # U11 R26: top-1 candidate score (``matched_tracks + avg_ratio``)
    # across the scored candidates produced by this matcher run.
    # ``None`` when no scored candidate was emitted (broken_user,
    # all-pre-filter-skipped, empty dirs_to_try, browse failure). Note:
    # avg_ratio is in ``[0.0, 1.0]`` so the composite remains in
    # ``[0, total_tracks + 1]`` and ties between candidates with the
    # same matched_tracks break on ratio.
    matcher_score_top1: float | None = None


def classify_rejection_reason(
    candidates: "list[CandidateScore] | tuple[CandidateScore, ...]",
    pre_filter_skip_count: int,
    matched: bool,
    *,
    strict_accept_then_failed_cross_check: bool = False,
) -> str | None:
    """Synthesise the dominant rejection reason for one matcher run.

    Pure function — no I/O. Inspects the per-(user, dir, filetype)
    forensic score list ``check_for_match`` produces and reports the
    dominant reason the matcher rejected every candidate. Used by
    ``MatchResult.rejection_reason`` (U11 R22) so the search-log row
    carries one scalar reason instead of forcing operators to JSONB-
    introspect every candidate.

    Returns:

      * ``None`` — the matcher accepted a candidate (``matched=True``)
        OR the matcher emitted nothing to classify (broken_user,
        empty ``dirs_to_try``, browse crash). These are upstream
        failures the rejection-reason axis cannot describe.
      * ``"all_skipped_pre_filter"`` — every emitted candidate was a
        pre-filter-skip sample AND ``pre_filter_skip_count > 0`` (i.e.
        the noisy-peer count gate rejected every dir before browse).
      * ``"cross_check_failed"`` — a strict-accept dir cleared the
        filename ratio AND the count gate but failed
        ``_track_titles_cross_check`` (likely a wrong pressing with
        the same track count). Caller passes
        ``strict_accept_then_failed_cross_check=True`` because the
        cross-check failure is structural rather than reflected in any
        candidate field. When True this dominates other reasons.
      * ``"avg_ratio_low"`` — the highest-scored candidate had the
        full file count for its filetype but ``matched_tracks <
        total_tracks`` (filename similarity below threshold).
      * ``"strict_count_mismatch"`` — the highest-scored candidate
        had ``file_count != total_tracks`` (sub-count gate failure;
        ``album_match`` was never called for the best dir).

    Composite score (``matched_tracks + avg_ratio``) breaks ties; the
    same ordering rule the U7 web/CLI surface uses to rank candidates.
    Defined here rather than in a generic "matcher policy" module so
    the classifier moves with the matcher's rejection logic.
    """
    if matched:
        return None
    if strict_accept_then_failed_cross_check:
        return "cross_check_failed"
    scored = [c for c in candidates if not c.pre_filter_skip]
    if not scored:
        if pre_filter_skip_count > 0:
            return "all_skipped_pre_filter"
        return None
    top = max(scored, key=lambda c: (c.matched_tracks, c.avg_ratio))
    # When the top scored dir has the full file count for the requested
    # filetype, the sub-count gate passed and ``album_match`` actually
    # scored the filenames. A non-strict accept here means filename
    # similarity / cross-check / ignored-users rejection — the
    # operator-meaningful signal is "we got close but the ratio was
    # below threshold".
    if top.file_count == top.total_tracks:
        return "avg_ratio_low"
    return "strict_count_mismatch"


def classify_rejection_from_log_inputs(
    candidates: "list[CandidateScore] | tuple[CandidateScore, ...]",
    pre_filter_skip_count: int,
    outcome: str,
) -> str | None:
    """Log-layer rejection-reason classifier (U11 R22).

    Wraps :func:`classify_rejection_reason` with the heuristics needed at
    the search-log write site, where the SearchResult aggregates
    candidates across every ``check_for_match`` call (multiple filetypes,
    multi-disc) and the per-matcher cross-check latch is no longer
    available.

    Heuristic for cross-check-failure: when ``outcome != "found"`` AND the
    top scored candidate has ``matched_tracks == total_tracks ==
    file_count`` (strict-accept clear), the rejection was either a
    cross-check failure or an ignored-user gate — both of which the
    operator wants surfaced as "got close but rejected". Logged as
    ``cross_check_failed``.

    Returns ``None`` when ``outcome == "found"`` (matched) or when the
    aggregated candidate list cannot be classified (no candidates, no
    pre-filter-skips). Outcomes other than ``"no_match"`` /
    ``"enqueue_failed"`` / ``"found"`` (e.g. ``"no_results"``,
    ``"error"``, ``"empty_query"``) also return ``None`` because the
    matcher never ran — the search never produced candidates to classify.
    """
    if outcome == "found":
        return None
    # Outcomes where the matcher never ran (no slskd results, error,
    # empty query) cannot have a rejection_reason — nothing was
    # classified. Surface NULL so the operator dashboard can distinguish
    # "the matcher tried and rejected" from "the matcher never ran".
    if outcome not in ("no_match", "enqueue_failed"):
        return None
    scored = [c for c in candidates if not c.pre_filter_skip]
    cross_check = False
    if scored:
        top = max(scored, key=lambda c: (c.matched_tracks, c.avg_ratio))
        if (
            top.file_count == top.total_tracks
            and top.matched_tracks == top.total_tracks
        ):
            cross_check = True
    return classify_rejection_reason(
        candidates, pre_filter_skip_count, matched=(outcome == "found"),
        strict_accept_then_failed_cross_check=cross_check,
    )


def matcher_score_top1_for(
    candidates: "list[CandidateScore] | tuple[CandidateScore, ...]",
) -> float | None:
    """Top-1 ``matched_tracks + avg_ratio`` composite over scored entries.

    Pure helper for ``MatchResult.matcher_score_top1`` (U11 R26).
    Excludes pre-filter-skip sample rows because they never reach
    ``album_match`` and so carry zeroed scores by construction. Returns
    ``None`` when no scored candidate was produced — the dashboard
    renders NULL as "n/a" rather than 0 so operators can distinguish
    "matcher tried and scored 0" from "matcher never scored anything".
    """
    scored = [c for c in candidates if not c.pre_filter_skip]
    if not scored:
        return None
    top = max(scored, key=lambda c: (c.matched_tracks, c.avg_ratio))
    return float(top.matched_tracks) + float(top.avg_ratio)


def get_album_by_id(album_id: int, ctx: CratediggerContext) -> Any:
    """Get album data by ID from the context cache."""
    if album_id in ctx.current_album_cache:
        return ctx.current_album_cache[album_id]
    raise KeyError(f"Album {album_id} not found in cache")


def album_match(
    expected_tracks: Sequence[TrackRecord],
    slskd_tracks: Sequence[SlskdFile],
    username: str,
    filetype: str,
    ctx: CratediggerContext,
) -> AlbumMatchScore:
    """Compute the per-track filename-similarity score for a candidate dir.

    Returns an `AlbumMatchScore` for every input. The strict-accept decision
    (matched_tracks == total_tracks AND ignored_users gate) is left to the
    caller — see `check_for_match`. This function is pure: no I/O, no
    side effects, no decision logic beyond the per-track ratio comparison.
    """
    match_cfg = ctx.cfg

    album_info = get_album_by_id(expected_tracks[0]["albumId"], ctx)
    album_name = album_info.title

    from lib.quality import parse_filetype_config

    spec = parse_filetype_config(filetype)

    matched_titles: list[str] = []
    missing_titles: list[str] = []
    best_per_track: list[float] = []
    total_match = 0.0

    for expected_track in expected_tracks:
        best_match = 0.0
        expected_filename = expected_track["title"]

        for slskd_track in slskd_tracks:
            slskd_ext = (
                slskd_track["filename"].rsplit(".", 1)[-1].lower()
                if "." in slskd_track["filename"]
                else ""
            )
            expected_ext = slskd_ext if slskd_ext else spec.extension
            expected_filename = (
                expected_track["title"] + "." + expected_ext
                if expected_ext and expected_ext != "*"
                else expected_track["title"]
            )
            slskd_filename = slskd_track["filename"]

            ratio = difflib.SequenceMatcher(
                None, expected_filename, slskd_filename
            ).ratio()
            ratio = check_ratio(
                " ", ratio, expected_filename, slskd_filename,
                match_cfg.minimum_match_ratio,
            )
            ratio = check_ratio(
                "_", ratio, expected_filename, slskd_filename,
                match_cfg.minimum_match_ratio,
            )
            ratio = check_ratio(
                "", ratio, album_name + " " + expected_filename,
                slskd_filename, match_cfg.minimum_match_ratio,
            )
            ratio = check_ratio(
                " ", ratio, album_name + " " + expected_filename,
                slskd_filename, match_cfg.minimum_match_ratio,
            )
            ratio = check_ratio(
                "_", ratio, album_name + " " + expected_filename,
                slskd_filename, match_cfg.minimum_match_ratio,
            )

            if ratio > best_match:
                best_match = ratio

        best_per_track.append(best_match)
        if best_match > match_cfg.minimum_match_ratio:
            matched_titles.append(expected_track["title"])
            total_match += best_match
        else:
            missing_titles.append(expected_track["title"])

    matched_tracks = len(matched_titles)
    total_tracks = len(expected_tracks)
    avg_ratio = (total_match / matched_tracks) if matched_tracks else 0.0

    return AlbumMatchScore(
        matched_tracks=matched_tracks,
        total_tracks=total_tracks,
        avg_ratio=avg_ratio,
        missing_titles=missing_titles,
        best_per_track=best_per_track,
    )


def check_ratio(
    separator: str,
    ratio: float,
    expected_filename: str,
    slskd_filename: str,
    minimum_match_ratio: float,
) -> float:
    """Retry a weak filename match with trimmed prefixes."""
    if ratio < minimum_match_ratio:
        if separator != "":
            expected_filename_word_count = len(expected_filename.split()) * -1
            truncated_slskd_filename = " ".join(
                slskd_filename.split(separator)[expected_filename_word_count:]
            )
            ratio = difflib.SequenceMatcher(
                None, expected_filename, truncated_slskd_filename
            ).ratio()
        else:
            ratio = difflib.SequenceMatcher(
                None, expected_filename, slskd_filename
            ).ratio()

    return ratio


def album_track_num(
    directory: SlskdDirectory,
    match_cfg: CratediggerConfig,
    *,
    allowed_filetype: str | None = None,
) -> dict[str, Any]:
    """Count matching audio tracks and infer a consistent filetype."""
    from lib.quality import parse_filetype_config

    files = directory["files"]
    specs = (
        (parse_filetype_config(allowed_filetype),)
        if allowed_filetype is not None
        else match_cfg.allowed_specs
    )
    global_catch_all = (
        allowed_filetype is None and any(s.extension == "*" for s in specs)
    )
    count = 0
    filetype = ""
    for file in files:
        matched_spec = next(
            (spec for spec in specs if audio_file_matches(file, spec)),
            None,
        )
        if matched_spec is None:
            continue

        ext = file["filename"].rsplit(".", 1)[-1].lower()
        if allowed_filetype is not None:
            current_filetype = matched_spec.config_string
        elif global_catch_all:
            current_filetype = ext
        else:
            current_filetype = matched_spec.extension

        if filetype == "":
            filetype = current_filetype
        elif filetype != current_filetype:
            filetype = ""
            break
        count += 1

    return {"count": count, "filetype": filetype}


def _audio_files_for_filetype(
    directory: SlskdDirectory,
    allowed_filetype: str,
) -> list[SlskdFile]:
    return [
        file for file in directory["files"]
        if audio_file_matches(file, allowed_filetype)
    ]


def _matched_directory_for_filetype(
    directory: SlskdDirectory,
    allowed_filetype: str,
) -> SlskdDirectory:
    files = []
    for file in directory["files"]:
        filename = file["filename"]
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if audio_file_matches(file, allowed_filetype) or ext not in AUDIO_EXTENSIONS:
            files.append(file)
    return {**directory, "files": files}


def _search_cache_concrete_codecs_for_dir(
    ctx: CratediggerContext,
    album_id: int,
    username: str,
    file_dir: str,
) -> set[str]:
    from lib.quality import parse_filetype_config

    album_cache = ctx.search_cache.get(album_id, {})
    user_cache = album_cache.get(username, {}) if isinstance(album_cache, dict) else {}
    if not isinstance(user_cache, dict):
        return set()
    codecs: set[str] = set()
    for filetype, dirs in user_cache.items():
        if file_dir not in dirs:
            continue
        codec = parse_filetype_config(filetype).codec
        if codec not in ("*", "lossless"):
            codecs.add(codec)
    return codecs


def check_for_match(
    tracks: Sequence[TrackRecord],
    allowed_filetype: str,
    file_dirs: list[str],
    username: str,
    ctx: CratediggerContext,
    *,
    album_match_fn: "Callable[..., Any] | None" = None,
    cross_check_fn: "Callable[..., bool] | None" = None,
) -> MatchResult:
    """Check candidate directories for an album match.

    Returns a `MatchResult` with the strict-accept boolean, the matched
    directory + name (if any), and a list of `CandidateScore` entries
    capturing every dir the loop touched — including dirs that failed the
    sub-count gate (cheap zero-score entry) and dirs that strict-accepted
    on filename ratio but failed `_track_titles_cross_check` (full score
    plus a negative_matches entry). U5 persists `candidates` to
    `search_log.candidates` for forensic introspection.

    ``album_match_fn`` and ``cross_check_fn`` are dependency-injection
    seams. Production callers leave them at their defaults (the
    module-level :func:`album_match` and :func:`_track_titles_cross_check`);
    tests pass stubs by value to exercise the try/finally credit
    accumulator on exception and the cross-check-failure candidate path
    without constructing inputs that happen to land in the right
    rejection bucket. Both default to ``None`` so the function resolves
    to the live module-level binding at call time — keeps the seam
    cheap when callers don't override.
    """
    _album_match: Callable[..., Any] = (
        album_match_fn if album_match_fn is not None else album_match
    )
    _cross_check: Callable[..., bool] = (
        cross_check_fn if cross_check_fn is not None else _track_titles_cross_check
    )
    candidates: list[CandidateScore] = []
    pre_filter_skip_count = 0
    pre_filter_skip_samples_emitted = 0
    # U11: cross-check-failure latch. Set when a strict-accept dir
    # cleared the filename ratio + ignored_users gate but failed
    # ``_track_titles_cross_check`` (wrong-pressing detection). Threaded
    # into ``classify_rejection_reason`` because the failure mode is
    # structural — no candidate field encodes "we strict-accepted then
    # the cross-check rejected" without it.
    cross_check_failure = False

    def _build_no_match() -> MatchResult:
        """Common no-match return: compute U11 rejection_reason + top1.

        Keeps every no-match exit consistent so the search-log row
        carries the synthesised forensics regardless of which branch
        the matcher took. The pure helpers are read-only on
        ``candidates`` — no mutation across branches.
        """
        return MatchResult(
            matched=False,
            directory={},
            file_dir="",
            candidates=candidates,
            pre_filter_skip_count=pre_filter_skip_count,
            rejection_reason=classify_rejection_reason(
                candidates, pre_filter_skip_count, matched=False,
                strict_accept_then_failed_cross_check=cross_check_failure,
            ),
            matcher_score_top1=matcher_score_top1_for(candidates),
        )

    logger.debug(f"Current broken users {ctx.broken_user}")
    if username in ctx.broken_user:
        return _build_no_match()
    track_num = len(tracks)
    album_id = tracks[0]["albumId"]
    album_info = get_album_by_id(album_id, ctx)
    ranked_dirs = rank_candidate_dirs(file_dirs, album_info.title, album_info.artist_name)

    dirs_to_try: list[str] = []
    for file_dir in ranked_dirs:
        neg_key = (username, file_dir, track_num, allowed_filetype)
        if neg_key in ctx.negative_matches:
            logger.debug(
                f"Negative cache hit: {username} {file_dir} "
                f"({track_num} tracks, {allowed_filetype})"
            )
            continue

        user_counts = ctx.search_dir_audio_count.get(username)
        if user_counts and file_dir in user_counts:
            search_count = user_counts[file_dir]
            cached_codecs = _search_cache_concrete_codecs_for_dir(
                ctx, album_id, username, file_dir,
            )
            if len(cached_codecs) <= 1 and search_count > 2 * track_num:
                logger.debug(
                    f"Pre-filter skip: {username} {file_dir} has {search_count} "
                    f"audio files, need {track_num} tracks"
                )
                ctx.negative_matches.add(neg_key)
                # U2: telemetry — always count, sample up to the cap.
                pre_filter_skip_count += 1
                if pre_filter_skip_samples_emitted < PRE_FILTER_SKIP_SAMPLE_CAP:
                    candidates.append(CandidateScore(
                        username=username,
                        dir=file_dir,
                        filetype=allowed_filetype,
                        matched_tracks=0,
                        total_tracks=track_num,
                        avg_ratio=0.0,
                        missing_titles=[],
                        file_count=search_count,
                        pre_filter_skip=True,
                    ))
                    pre_filter_skip_samples_emitted += 1
                continue

        dirs_to_try.append(file_dir)

    if not dirs_to_try:
        return _build_no_match()

    peer_cache_negative_skips = getattr(ctx, "peer_cache_negative_skips", set())
    if peer_cache_negative_skips:
        dirs_to_try = [
            file_dir for file_dir in dirs_to_try
            if (username, file_dir) not in peer_cache_negative_skips
        ]
        if not dirs_to_try:
            return _build_no_match()

    ensure_cache_user(ctx, username)

    uncached = [d for d in dirs_to_try if d not in ctx.folder_cache[username]]
    if uncached:
        logger.info(
            f"Browsing {len(uncached)} dirs from {username} "
            f"(parallelism={ctx.cfg.browse_parallelism})"
        )
        # U1 instrumentation (issue #198 R13): time the network-bound browse
        # phase so the cycle summary can split browse vs match wall-clock.
        # try/finally so an exception inside _browse_directories still
        # credits the accumulator — silent loss skews the baseline.
        browse_t0 = time.monotonic()
        browse_attempts = len(uncached)
        negative_skips: set[tuple[str, str]] = set()
        try:
            if getattr(ctx, "peer_cache", None) is not None:
                browse_result = _browse_directories_for_ctx_result(
                    uncached,
                    username,
                    ctx,
                    ctx.cfg.browse_global_max_workers,
                )
                browsed = {
                    file_dir: directory
                    for (_user, file_dir), directory in browse_result.directories.items()
                }
                browse_attempts = browse_result.browse_attempts
                negative_skips = browse_result.negative_skips
            else:
                browsed = _browse_directories(
                    uncached,
                    username,
                    ctx,
                    ctx.cfg.browse_global_max_workers,
                )
        finally:
            ctx.browse_time_s += time.monotonic() - browse_t0
            # Lazy-fallback path: fan-out either swallowed an exception via
            # _browse_one (no entry written, user not in broken_user) or
            # this caller bypassed the wave loop. Either way, count
            # separately from peers_browsed so the cycle summary can
            # distinguish primary fan-out load from residual lazy retries.
            ctx.peers_browsed_lazy += browse_attempts
        if getattr(ctx, "peer_cache", None) is None:
            for d, result in browsed.items():
                cache_browsed_directory(ctx, username, d, result)

        if (
            not browsed
            and not negative_skips
            and browse_attempts == len(uncached)
            and len(uncached) == len(dirs_to_try)
        ):
            ctx.broken_user.add(username)
            logger.debug(f"All browses failed for {username}, marked as broken")
            return _build_no_match()

    # U1 instrumentation: time the local matching/scoring loop separately
    # from the network-bound browse above. try/finally so an exception
    # inside album_match / cross-check / track_num still credits the
    # accumulator — silent loss of measured time would skew the baseline.
    match_t0 = time.monotonic()
    try:
        for file_dir in dirs_to_try:
            if file_dir not in ctx.folder_cache[username]:
                continue

            directory = ctx.folder_cache[username][file_dir]
            tracks_info = album_track_num(
                directory, ctx.cfg, allowed_filetype=allowed_filetype,
            )
            neg_key = (username, file_dir, track_num, allowed_filetype)

            # Sub-count gate: cheap CandidateScore, do NOT call album_match.
            if tracks_info["count"] != track_num or tracks_info["filetype"] == "":
                candidates.append(CandidateScore(
                    username=username,
                    dir=file_dir,
                    filetype=allowed_filetype,
                    matched_tracks=0,
                    total_tracks=track_num,
                    avg_ratio=0.0,
                    missing_titles=[],
                    file_count=tracks_info["count"],
                ))
                ctx.negative_matches.add(neg_key)
                continue

            # Count gate passed — score the dir.
            score_files = _audio_files_for_filetype(directory, allowed_filetype)
            score = _album_match(tracks, score_files, username, allowed_filetype, ctx)
            candidates.append(CandidateScore(
                username=username,
                dir=file_dir,
                filetype=allowed_filetype,
                matched_tracks=score.matched_tracks,
                total_tracks=score.total_tracks,
                avg_ratio=score.avg_ratio,
                missing_titles=list(score.missing_titles),
                file_count=tracks_info["count"],
            ))

            strict_accept = (
                score.matched_tracks == score.total_tracks
                and username not in ctx.cfg.ignored_users
            )
            if strict_accept:
                if _cross_check(tracks, score_files):
                    # Log SUCCESSFUL MATCH at the one place enqueue is going
                    # to happen — the previous log site in album_match fired
                    # before the cross-check / ignored_users gate at the
                    # caller, which was misleading for ignored users and for
                    # cross-check failures.
                    logger.info(
                        f"Found match from user: {username} for "
                        f"{score.matched_tracks} tracks! "
                        f"Track attributes: {allowed_filetype}"
                    )
                    logger.info(f"Average sequence match ratio: {score.avg_ratio}")
                    logger.info("SUCCESSFUL MATCH")
                    logger.info("-------------------")
                    return MatchResult(
                        matched=True,
                        directory=_matched_directory_for_filetype(
                            directory, allowed_filetype,
                        ),
                        file_dir=file_dir,
                        candidates=candidates,
                        pre_filter_skip_count=pre_filter_skip_count,
                        rejection_reason=None,
                        matcher_score_top1=matcher_score_top1_for(
                            candidates,
                        ),
                    )
                logger.warning(
                    f"Track title cross-check FAILED for user {username}, "
                    f"dir {file_dir} — skipping (wrong pressing?)"
                )
                # U11 R22: latch the cross-check failure so
                # ``_build_no_match`` reports it. The candidate carries
                # a full strict-accept score so the bare classification
                # would call it ``avg_ratio_low`` — but the matcher KNOWS
                # the rejection was structural, not score-driven.
                cross_check_failure = True
            ctx.negative_matches.add(neg_key)

        return _build_no_match()
    finally:
        ctx.match_time_s += time.monotonic() - match_t0
