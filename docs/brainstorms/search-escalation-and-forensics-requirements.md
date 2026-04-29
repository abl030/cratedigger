---
date: 2026-04-29
topic: search-escalation-and-forensics
---

# Search Escalation and Forensics for Stuck Releases

## Problem Frame

Releases like #1843 (The Wiggles, 1991 AU, 26 tracks) sit in `wanted` for weeks across dozens of search cycles, with `search_log` showing identical queries (`*he *iggles`), identical outcomes (`no_match`), and identical `result_count` (99–100). This is not a Soulseek availability problem — a manual search for `wiggles 1991` returns FLAC dirs with 26 tracks within seconds. It is a pipeline problem with three independent layers:

1. **`responseLimit` defaults to 100** in the slskd-api Python client (`SlskdClient.searches.search_text`). Cratedigger does not override it. Every search caps at 100 peer responses, in arrival order, regardless of how many peers actually have matching content. For obscure releases the right peer never makes it into the cap.
2. **The query never varies.** Cycle N issues exactly the same `build_query()` output as cycle 1. Re-running an identical search against a non-deterministic peer set is a closed loop: same query → same ~100-peer cap → same rejection → same 30-min sleep.
3. **`album_match()` returns `bool`.** Per-track sequence-match ratios are computed (`lib/matching.py:29`) but discarded. A peer with 24 of 26 expected tracks looks identical to a peer with 0 of 26 in `search_log`. Operators cannot diagnose why a release is stuck because no forensic record of considered candidates is retained.

This document defines the changes needed to (a) widen the search net, (b) escalate query strategy when a release fails to match, and (c) capture enough per-search forensics to make stuck releases self-explaining.

The existing strict-match rule (a peer must score above `minimum_match_ratio` on **every** expected track to be accepted) is **out of scope and must be preserved**. Strict matching is also a defence at import time — confirming that 26/26 named tracks are present is how Cratedigger knows the candidate is the right release.

---

## Actors

- A1. Pipeline search loop: builds queries, submits to slskd, collects responses, builds the grab list (`cratedigger.py:search_for_album`, `_submit_search`, `_collect_search_results`).
- A2. Album matcher: scores candidate directories against the expected track list (`lib/matching.py:album_match`).
- A3. Operator: inspects stuck releases via the web UI / `pipeline-cli` and decides whether to source manually, swap MBID, or abandon.
- A4. NixOS module: exposes the `responseLimit` knob so it can be tuned without a code change.

---

## Key Flows

- F1. Default search (cycles 1–4)
  - **Trigger:** Release in `wanted` is selected for a search cycle.
  - **Actors:** A1
  - **Steps:** Build artist+album wildcarded query via the existing `lib/search.py:build_query`. Submit with `responseLimit=cfg.search_response_limit` (default 1000). Collect responses, build grab list, attempt to match.
  - **Outcome:** Same as today for releases that match cycle 1; significantly more candidate peers per cycle for the rest.
  - **Covered by:** R1, R2, R8

- F2. Escalated search (cycles 5+)
  - **Trigger:** `album_requests.search_attempts >= cfg.search_escalation_threshold` (default 5) with no successful match.
  - **Actors:** A1
  - **Steps:** Select a query variant deterministically from the cycle index. V1 first, then V4 with rotating track-token slices. Each cycle uses one variant.
  - **Outcome:** Each escalated cycle samples a different ~1000-peer slice driven by a different query, increasing odds the correct peer enters the response cap.
  - **Covered by:** R3, R4, R5

- F3. Search exhaustion
  - **Trigger:** Escalated search has cycled through V1 plus every distinct V4 token slice without a match.
  - **Actors:** A1, A3
  - **Steps:** Flip the request to `manual` with reason `search_exhausted`. The web UI surfaces the request as needing operator intervention. Forensic blob from the last N searches is queryable to inform the operator's decision.
  - **Outcome:** The pipeline stops grinding on the release; the operator decides whether to source manually, swap MBID, abandon, or reset.
  - **Covered by:** R6, R12

- F4. Forensic capture per search
  - **Trigger:** Every search cycle, regardless of outcome.
  - **Actors:** A1, A2
  - **Steps:** `album_match()` returns a structured score per candidate directory instead of a `bool`. The pipeline persists the top-20 candidates by score as a JSONB blob on `search_log.candidates`. The strict accept/reject decision (every track above `minimum_match_ratio`) is unchanged — only the intermediate state is now retained.
  - **Outcome:** Operators can answer "did the right peer ever appear?" by reading `pipeline-cli show <id>` or querying the JSONB.
  - **Covered by:** R7, R9, R10

---

## Requirements

**Search Cap**

- R1. The slskd-api `responseLimit` parameter must be passed explicitly on every search submission in `cratedigger.py:search_for_album` and `cratedigger.py:_submit_search`. The value must come from a typed config field on `CratediggerConfig`.
- R2. The default value must be `1000`, with the ability to override via `cfg.search_response_limit`. The setting must be exposed as a NixOS module option in `nix/module.nix` so it can be tuned without a code change.

**Query Escalation**

- R3. A new typed config field `search_escalation_threshold` (default `5`) must control when escalation begins. While `search_attempts < threshold`, the search loop must use the existing `build_query()` output unchanged.
- R4. Variant V1 ("append year"): take the existing `build_query()` output and append the release year (e.g. `1991`) as an additional token. If the release year is unknown, V1 must be skipped and the loop must advance directly to V4.
- R5. Variant V4 ("distinctive track tokens"): build a candidate token pool from `album_tracks.title` for the request, normalize via the existing `strip_special_chars` / `strip_short_tokens` helpers, dedupe case-insensitively, and sort descending by length. Each V4 cycle takes the next 3 tokens from the pool. No artist tokens. No wildcarding.
- R6. When V1 has run and V4 has exhausted its token pool without a match, the request must be flipped to `status='manual'` with a structured reason of `search_exhausted` recorded in the existing manual-reason audit field (or equivalent). The request must not re-enter the wanted queue automatically.

**Forensic Capture**

- R7. `lib/matching.py:album_match` must be refactored to return a structured per-directory score: `{matched_tracks: int, total_tracks: int, avg_ratio: float, missing_titles: list[str], best_per_track: list[float]}`. The current accept/reject behaviour must be preserved by a thin wrapper that reads `matched_tracks == total_tracks` from the structured score.
- R8. A new column `search_log.candidates` (JSONB) must record up to the top 20 candidates per search, ordered by `matched_tracks DESC, avg_ratio DESC`. Each entry: `{username, dir, filetype, matched_tracks, total_tracks, avg_ratio, missing_titles, file_count}`.
- R9. A new column `search_log.variant` (TEXT) must record which variant was used for the search (`default`, `v1_year`, `v4_tracks_<slice_index>`).
- R10. A new column `search_log.final_state` (TEXT) must record the slskd terminal state (`Completed`, `ResponseLimitReached`, `TimedOut`, `Errored`) so operators can distinguish "we capped at 1000" from "slskd ran out of peers" without code spelunking.

**Wire-up & Diagnostics**

- R11. `pipeline-cli show <id>` must include the `variant` and top-3 candidate scores from the most recent search log row, so an operator triaging a release can see at a glance how the pipeline searched and what it found.
- R12. The web UI's request detail view must display the search exhaustion state and surface a copy of the last forensic blob, so an operator does not need shell access to triage.

---

## Scope Boundaries

**In scope**
- Raising the slskd `responseLimit` and exposing it via the Nix module.
- Variant-based query escalation after a configurable number of failed cycles.
- Forensic capture of per-candidate scores for every search.
- Search exhaustion as a terminal state.
- Refactoring `album_match` to return structured scores while preserving its current accept/reject behaviour.

**Deferred for later**
- Loosening the strict 26/26 rule (e.g. accepting 24/26 with a high avg ratio). Out of scope by design — strict matching is also an import-time defence and a personal hard rule.
- Variants V2 (country/format hints) and V3 (artist + tracks). User noted that release country and physical format almost never appear in Soulseek filenames; V2/V3 would burn cycles on signals peers don't index.
- Pagination within a single slskd search. slskd has no "next page" concept; we widen via a higher `responseLimit` and via query variation across cycles.
- Adaptive `responseLimit` (smaller for popular releases, larger for stuck ones). Premature optimisation; flat 1000 is the simple baseline. Revisit if slskd CPU pressure becomes an issue.
- Retiring the `search_type = incrementing_page` legacy config key. It is read nowhere; cleanup can ride a later commit but is not required for this work.

**Outside this brainstorm**
- Anything about `responseFileLimit`, `minimumResponseFileCount`, `minimumPeerUploadSpeed` tuning. The current values are working for the rest of the catalogue.
- Beets distance scoring at search time. The grab list scoring path lives in `lib/matching.py` and is the appropriate boundary; we are not introducing beets to the pre-import pipeline.

---

## Dependencies / Assumptions

- D1. `album_tracks` is reliably populated for every release. Verified for #1843 (26 rows). If a release has no track rows, V4 cannot run and the loop must fall back to default + V1 only, then exhaust.
- D2. The slskd default `responseLimit=100` is a soft, peer-side cap that slskd terminates gracefully at; raising to 1000 has no schema or protocol implication on slskd itself, only more CPU per search. (Verified empirically by inspecting `slskd-api` `search_text` defaults; needs operational confirmation under load.)
- D3. `search_attempts` is incremented exactly once per cycle and is the single source of truth for which variant to dispatch. (Verified — `_log_search_result` in `cratedigger.py` calls `record_attempt` on failure.)
- D4. Existing `search_log` rows do not need backfilling. New columns will be NULL for historical rows; tooling that reads them must accept NULL.

---

## Success Criteria

- S1. After deploy, a single search cycle for #1843 (and other releases stuck at `search_attempts >= 5`) yields a `search_log.candidates` JSONB blob with at least one peer scoring `matched_tracks >= 20/26`, OR the loop reaches search exhaustion within ~4 hours and surfaces the request to the UI.
- S2. The Nix module exposes a `services.cratedigger.searchResponseLimit` option (or equivalent) that flows into the rendered `config.ini` and is observable via `ssh doc2 'sudo cat /var/lib/cratedigger/config.ini'`.
- S3. `pipeline-cli show <id>` for a stuck release shows the most recent variant and top candidate scores without requiring raw SQL.
- S4. Existing tests in `tests/test_matching.py` and any harness/integration tests covering `album_match`'s accept/reject behaviour continue to pass without modification — strict match behaviour is preserved.
- S5. New tests cover: variant selection from cycle index (pure function), V4 token pool construction (pure function over `album_tracks` rows), `album_match` structured-return refactor (RED test first that asserts structured shape; GREEN behaviour-preservation test), forensic JSONB capture (orchestration test asserting `search_log.candidates` shape post-search).
- S6. Migration NNN_search_forensics.sql adds `candidates JSONB`, `variant TEXT`, `final_state TEXT` to `search_log` and runs cleanly via `tests.test_migrator`.

---

## Open Questions (defer to planning unless flagged)

- O1. Where does the `search_exhausted` reason live exactly? `album_requests` does not currently have a `manual_reason` column; do we add one, reuse `import_result` JSONB with a sentinel, or create a dedicated `request_exhaustion` row? Planning decision.
- O2. Should `search_attempts` be reset when an operator manually re-queues an exhausted release from the UI, so the variant ladder restarts at default? Probably yes; confirm in planning.
- O3. The existing `_log_search_result` path runs after grab list construction. The forensic candidates list must be populated before persistence — confirm the call ordering does not require restructuring the search loop. Planning to verify.
- O4. Does the Discogs-source path also flow through `album_match` and `search_log`, or is its grab list constructed differently? If the latter, decide whether forensic capture extends there or stays MB-only for now.
