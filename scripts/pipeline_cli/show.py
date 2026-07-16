"""pipeline-cli ``show`` command (#495 carve).

``pipeline-cli show <id>`` — full per-request detail dump: metadata,
active-download state, quality state, tracks, search history (+ U7
forensics summary), download history (+ import-result rendering), and
denylisted users.
"""

import argparse
import json

import msgspec

from lib.import_evidence import HaveAnalysisFailure
from lib.quality import ImportResult

from scripts.pipeline_cli._format import _fmt_br, _fmt_measurement


def _render_import_result(ir_raw):
    """Render an ImportResult JSONB blob as human-readable lines."""
    if not ir_raw:
        return []
    try:
        typed = (
            ImportResult.from_dict(ir_raw)
            if isinstance(ir_raw, dict)
            else ImportResult.from_json(ir_raw)
        )
        ir = msgspec.to_builtins(typed)
    except (json.JSONDecodeError, TypeError, ValueError, msgspec.ValidationError):
        return []

    lines = []
    decision = ir.get("decision", "?")
    lines.append(f"      decision:  {decision}")

    source_m = ir.get("source_measurement")
    if source_m:
        lines.append(f"      source:    {_fmt_measurement(source_m)}")
        target = ir.get("target_quality_contract") or {}
        if target.get("format"):
            lines.append(f"      target:    {target['format']} (contract)")
        existing_m = ir.get("current_measurement")
        if existing_m:
            lines.append(f"      current:   {_fmt_measurement(existing_m)}")

        conv = ir.get("conversion") or {}
        if conv.get("was_converted"):
            src = conv.get("original_filetype", "?")
            tgt = conv.get("target_filetype", "?")
            n = conv.get("converted", 0)
            extra = ""
            if conv.get("is_transcode"):
                extra = " (TRANSCODE)"
            lines.append(f"      converted: {src} -> {tgt} ({n} files){extra}")
    else:
        # A v1 row with no projected measurement at all.
        quality = ir.get("quality") or {}
        spectral = ir.get("spectral") or {}
        if quality.get("new_min_bitrate") is not None:
            lines.append(f"      new:       {_fmt_br(quality['new_min_bitrate'])}")
        if quality.get("prev_min_bitrate") is not None:
            lines.append(f"      existing:  {_fmt_br(quality['prev_min_bitrate'])}")
        if spectral.get("grade"):
            lines.append(f"      spectral:  {spectral['grade']}")

    if ir.get("error"):
        lines.append(f"      error:     {ir['error']}")

    # Issue #130: surface post-import `beet move` failures so operators
    # don't have to grep JSONB to see them. `disambiguated=True` means
    # the move ran cleanly; a `disambiguation_failure` object means it
    # did not exit cleanly and the album is in beets at a stale path.
    pf = ir.get("postflight") or {}
    dfail = pf.get("disambiguation_failure")
    if dfail:
        reason = dfail.get("reason", "unknown")
        detail = dfail.get("detail", "")
        lines.append(f"      disambig:  FAILED ({reason}): {detail}")
    elif pf.get("disambiguated") is True:
        lines.append(f"      disambig:  ok")

    return lines


def _render_have_analysis_failure(row: dict[str, object]) -> list[str]:
    """Render the typed installed-HAVE environment-failure payload."""

    if row.get("outcome") != "have_analysis_error":
        return []
    raw = row.get("validation_result")
    try:
        if isinstance(raw, dict):
            failure = msgspec.convert(raw, type=HaveAnalysisFailure)
        elif isinstance(raw, (str, bytes)):
            failure = msgspec.json.decode(raw, type=HaveAnalysisFailure)
        else:
            return []
    except (TypeError, ValueError, msgspec.ValidationError):
        return []

    lines = [
        f"      log_id:            {row.get('id', '?')}",
        f"      failure_category:  {failure.failure_category}",
        f"      analysis_error:    {failure.error}",
    ]
    if failure.installed_path:
        lines.append(f"      installed_path:     {failure.installed_path}")
    if failure.candidate_reference:
        lines.append(
            f"      candidate_reference: {failure.candidate_reference}"
        )
    return lines


def _render_search_forensics_summary(
    request_row: dict, latest_search: dict,
) -> list[str]:
    """Build the U7 forensic summary block printed above search history.

    Inputs:
    - ``request_row``: the row dict from ``PipelineDB.get_request``. Used
      for ``manual_reason`` (None when not exhausted / pre-U7).
    - ``latest_search``: the most recent ``search_log`` row dict (already
      ordered newest-first by ``get_search_history``). The candidates
      JSONB blob is decoded here via ``msgspec.convert(blob,
      type=list[CandidateScore])`` — single decode site for the CLI side
      per ``.claude/rules/code-quality.md`` § Wire-boundary types.

    Empty lists / NULL JSONB blobs render gracefully (no
    ``msgspec.ValidationError``); rows without forensic fields (e.g.
    pre-U1 search_log entries) print "no forensic data yet".
    """
    from lib.quality import CandidateScore, top_candidates

    lines: list[str] = ["", "  Search Forensics:"]
    variant = latest_search.get("variant")
    final_state = latest_search.get("final_state")
    manual_reason = request_row.get("manual_reason")

    if variant:
        lines.append(f"    variant:        {variant}")
    if final_state:
        lines.append(f"    final_state:    {final_state}")
    if manual_reason:
        lines.append(f"    manual_reason:  {manual_reason}")

    raw_candidates = latest_search.get("candidates")
    if raw_candidates is None:
        if not (variant or final_state or manual_reason):
            lines.append("    (no forensic data yet)")
        else:
            lines.append("    candidates:     (none captured)")
        return lines

    try:
        candidates = msgspec.convert(raw_candidates, type=list[CandidateScore])
    except msgspec.ValidationError as e:
        # Defensive — production writes via the same Struct so this should
        # never trip in practice, but a corrupted historical row should
        # not crash `pipeline-cli show`.
        lines.append(f"    candidates:     <decode error: {e}>")
        return lines

    if not candidates:
        lines.append("    candidates:     (empty list)")
        return lines

    # Top-3 by (matched_tracks DESC, avg_ratio DESC) for the compact CLI
    # glance; the web long-tail console's "peers seen" panel renders the full
    # stored slice (top-20). Same ranking, different surface depth. Shared
    # ranking lives in lib/quality/wire_types.py.
    top = top_candidates(candidates, limit=3)
    lines.append(f"    top candidates ({len(top)} of {len(candidates)}):")
    for c in top:
        lines.append(
            f"      {c.username} | {c.dir} | "
            f"{c.matched_tracks}/{c.total_tracks} | "
            f"avg={c.avg_ratio:.2f} | {c.filetype}"
        )
    return lines


def _render_download_history_header(row):
    source = row.get("source") or "slskd"
    outcome = row.get("outcome")
    created_at = row.get("created_at")
    if source == "youtube":
        meta = row.get("youtube_metadata")
        meta = meta if isinstance(meta, dict) else {}
        parts = ["via youtube"]
        browse_id = meta.get("browse_id")
        if browse_id:
            parts.append(f"browse_id={browse_id}")
        observed = meta.get("observed_track_count")
        expected = meta.get("expected_track_count")
        if observed is not None or expected is not None:
            parts.append(f"tracks={observed if observed is not None else '?'}/"
                         f"{expected if expected is not None else '?'}")
        reason = meta.get("reason")
        if reason:
            parts.append(f"reason={reason}")
        return f"    [{created_at}] {outcome} {' '.join(parts)}"
    username = row.get("soulseek_username") or "-"
    return (
        f"    [{created_at}] {outcome} via slskd from {username} "
        f"(dist={row.get('beets_distance')})"
    )


def _render_youtube_metadata(row):
    if (row.get("source") or "slskd") != "youtube":
        return []
    meta = row.get("youtube_metadata")
    if not isinstance(meta, dict):
        return []
    lines = []
    yt_url = meta.get("yt_url")
    if yt_url:
        lines.append(f"      yt_url:    {yt_url}")
    stderr = meta.get("stderr_excerpt")
    if stderr:
        lines.append(f"      stderr:    {str(stderr).splitlines()[-1]}")
    cleanup_error = meta.get("cleanup_error")
    if cleanup_error:
        lines.append(f"      cleanup_error: {cleanup_error}")
    return lines


def cmd_show(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return

    print(f"  ID:           {req['id']}")
    print(f"  Artist:       {req['artist_name']}")
    print(f"  Album:        {req['album_title']}")
    print(f"  Status:       {req['status']}")
    print(f"  Source:       {req['source']}")
    print(f"  MB Release:   {req['mb_release_id']}")
    print(f"  MB RG:        {req['mb_release_group_id']}")
    print(f"  MB Artist:    {req['mb_artist_id']}")
    print(f"  Discogs:      {req['discogs_release_id']}")
    print(f"  Year:         {req['year']}")
    print(f"  RG Year:      {req.get('release_group_year') or '-'}")
    print(f"  Country:      {req['country']}")
    print(f"  Format:       {req['format']}")
    print(f"  VA Comp:      {'yes' if req.get('is_va_compilation') else 'no'}")
    print(f"  Catalog #:    {req.get('catalog_number') or '-'}")
    print(f"  Source Path:  {req['source_path']}")
    if req.get("reasoning"):
        print(f"  Reasoning:    {req['reasoning'][:120]}...")
    print(f"  Distance:     {req['beets_distance']}")
    print(f"  Imported:     {req['imported_path']}")
    print(f"  Attempts:     search={req['search_attempts']} dl={req['download_attempts']} val={req['validation_attempts']}")
    print(f"  Created:      {req['created_at']}")
    print(f"  Updated:      {req['updated_at']}")

    # --- Active download state ---
    ads = req.get("active_download_state")
    if ads and isinstance(ads, dict):
        enq = ads.get("enqueued_at", "?")
        ftype = ads.get("filetype", "?")
        fcount = len(ads.get("files", []))
        print(f"\n  Active Download:")
        print(f"    filetype:     {ftype}")
        print(f"    enqueued_at:  {enq}")
        print(f"    files:        {fcount}")

    # --- Quality state ---
    min_br = req.get("min_bitrate")
    prev_br = req.get("prev_min_bitrate")
    verified = req.get("verified_lossless")
    s_grade = req.get("last_download_spectral_grade")
    s_br = req.get("last_download_spectral_bitrate")
    cur_grade = req.get("current_spectral_grade")
    cur_br = req.get("current_spectral_bitrate")
    q_override = req.get("search_filetype_override")
    has_quality = any(
        v is not None
        for v in [min_br, prev_br, verified, s_grade, s_br, cur_grade, cur_br, q_override]
    )
    if has_quality:
        print(f"\n  Quality:")
        print(f"    min_bitrate:        {_fmt_br(min_br)}")
        if prev_br is not None:
            print(f"    prev_min_bitrate:   {_fmt_br(prev_br)}")
        print(f"    verified_lossless:  {verified or False}")
        if s_grade:
            sg = s_grade
            if s_br:
                sg += f" ~{s_br}kbps"
            print(f"    last_download:     {sg}")
        if cur_grade:
            current = cur_grade
            if cur_br:
                current += f" ~{cur_br}kbps"
            print(f"    current_spectral:  {current}")
        if q_override:
            print(f"    search_filetype_override: {q_override}")

    tracks = db.get_tracks(req['id'])
    if tracks:
        print(f"\n  Tracks ({len(tracks)}):")
        for t in tracks:
            dur = f"{t['length_seconds']:.0f}s" if t['length_seconds'] else "?"
            print(f"    {t['disc_number']}.{t['track_number']:02d} {t['title']} ({dur})")

    searches = db.get_search_history(req['id'])
    if searches:
        # U7: print a forensic summary above the row table — the most
        # recent variant + final_state, the request's manual_reason if
        # populated, and the top-3 candidates from the latest search_log
        # row's JSONB blob. The blob is decoded once via msgspec.convert
        # per single-decode-site discipline (code-quality.md § Wire-
        # boundary types). Older rows / NULL blobs render gracefully.
        for line in _render_search_forensics_summary(req, searches[0]):
            print(line)
        print(f"\n  Search History ({len(searches)}):")
        for s in searches:
            q = s['query'] or "(no query)"
            rc = s['result_count']
            rc_str = f"{rc} results" if rc is not None else "n/a"
            el = s['elapsed_s']
            el_str = f"{el:.1f}s" if el is not None else ""
            variant = s.get('variant') or "-"
            print(f"    [{s['created_at']}] {s['outcome']:12s} {variant:14s} {rc_str:>12s} {el_str:>6s}  {q}")

    history = db.get_download_history(req['id'])
    if history:
        print(f"\n  Download History ({len(history)}):")
        for h in history:
            print(_render_download_history_header(h))
            for line in _render_youtube_metadata(h):
                print(line)
            for line in _render_have_analysis_failure(h):
                print(line)
            for line in _render_import_result(h.get("import_result")):
                print(line)

    denied = db.get_denylisted_users(req['id'])
    if denied:
        print(f"\n  Denylisted Users ({len(denied)}):")
        for d in denied:
            print(f"    {d['username']}: {d['reason']}")


def add_show_subparser(sub: argparse._SubParsersAction) -> None:
    """Add ``show`` (#521 carve out of ``routes_meta._build_parser``,
    verbatim argument definitions)."""
    p_show = sub.add_parser("show", help="Show full details of a request")
    p_show.add_argument("id", type=int, help="Request ID")
