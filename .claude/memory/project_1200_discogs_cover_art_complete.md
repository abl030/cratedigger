---
name: project_1200_discogs_cover_art_complete
description: "#1200 shipped 2026-08-19: Discogs albums got title-guessed art; two-part fix (mirror can't supply cover_art_url + source order) deployed, 101 albums remediated"
metadata:
  node_type: memory
  type: project
---

**#1200 COMPLETE 2026-08-19** — PR #1202 merged (`dd5fe796`), deployed (fleet anchor `0b6a06fc`), nixosconfig `c5b7387a`. Residuals: #1203.

**Symptom:** Bowie's 1969 Philips SBL 7912 imported carrying the 1967 Deram sleeve; byte-identical `cover.jpg` and embedded art.

**Root cause is TWO halves — either alone makes the fix inert:**
1. `nix/beets.nix` patches the discogs plugin to our mirror. The **CC0 dumps contain zero artwork** — 0 `<images>` elements across 99,892 releases in the live dump (vs 75,286 `<videos>`). `select_cover_art` always returned `None`. Not fixable in the mirror: `discogs-api` skips `images` in `src/xml.rs` and hardcodes `images: vec![]` in `src/db.rs`, but there is nothing to ingest anyway.
2. **`cover_art_url` was ranked below `itunes`** in fetchart sources. Beets' upstream default ranks it LAST, and `examples/cratedigger.nix` shipped no `sources` list — so the field would be populated and never consulted. iTunes matches `collectionName` by exact string equality, so same-titled pressings collide.

**Fix:** `harness/beets_compat.py::configure_discogs_cover_art_fallback` (real-API lookup only on a mirror miss, fail-soft) + identity-first source order + a `lib/beets_config_contract.py` startup WARNING that fires on absent `sources` (the real-world default).

## Lessons worth keeping

- **`harness/` cannot import `lib/`.** `cratediggerPkg` (the `cratedigger.service` wrapper) does NOT export `PYTHONPATH`; only `importerPkg`/`previewWorkerPkg` do. A `from lib.json_narrow import ...` in `beets_compat.py` passed the whole suite (the test runner inherits `PYTHONPATH`) and would have broken the main loop — and because `lib/beets.py` ignores the child's exit code, every completed download would have been stamped `no_choose_match`, which is delete-eligible. Guard with a real subprocess launch under `env -u PYTHONPATH`. See [[feedback_test_fidelity_meta_pattern]].
- **Never log an exception object on a path that builds an auth header.** `requests`' `InvalidHeader` embeds the header VALUE, so `%s` on the exception writes `Discogs token=...` to journald. Log `type(exc).__name__` + status code only.
- **Perceptual hashing cannot judge cover-art correctness.** dhash had ~64% false positives (116 flagged -> 40 genuinely wrong) because Discogs primaries are often amateur photos of physical sleeves. A *correct* album scored 14 and another 49; genuinely *wrong* ones scored 34 and 46. No threshold separates them — visual triage is required.
- **A reimport that gains a disambiguating field renames the folder** (`catalognum` beat `str(year)` in `path_disambig`), orphaning media-server items. Plex self-healed on a targeted scan; Jellyfin did not. Same class as [[project_plex_asciify_split]] but triggered by ordinary metadata arriving.
- **Remediation must follow the source-order deploy** — `beet fetchart` exposes only `--force`/`--quiet`, no source override, so an earlier run would re-fetch the same wrong art.
- Plex `addedAt` survives a content-only change (paths unchanged => same `Media.Part`); the pin machinery is for path/extension changes.

## Process

- Independent review found a blocker, a secret disclosure, 3 mutants that survived the authors' own kill matrices, and 6 false claims — all against a green suite across three rounds. Two consecutive correction rounds each minted new false claims; see [[feedback_orchestrator_briefs_become_defects]].
- An implementer correctly refused an uncited scope extension per the Authority rule. **Post the operator's verbatim decision as an issue comment and cite it** — a chat instruction relayed into a brief is not authority.
- I twice reported progress on processes that had already exited (a `pgrep` self-match, and a `nohup` whose exit code was the launching shell's). **Verify liveness by output growth, not by `pgrep` matching your own command line.**
