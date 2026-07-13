# Debugging quality decisions

```bash
pipeline-cli show <request_id>               # quality columns + download history with import decisions
pipeline-cli quality <request_id>            # simulate gate for genuine FLAC / V0 / CBR 320 / suspect FLAC
pipeline-cli debug-download <download_log_id>  # raw JSONB audit for one attempt
pipeline-cli search-plan show <request_id>   # active plan + cursor + per-slot usefulness stats (--json for machine output)
pipeline-cli triage quarantine --json       # unreferenced immediate failed_imports album folders (read-only)
pipeline-cli ban-source <request_id> --confirm BAN  # bad-rip removal; optional --release-id is confirmation-only
pipeline-cli library-delete <album_id> --confirm DELETE --purge-pipeline  # exact beets album delete
pipeline-cli search-plan regenerate <request_id>  # operator repair path; resets cursor on success, preserves old plan on failure
pipeline-cli query "SELECT ..."              # ad-hoc read-only SQL (add --json for machine output)
pipeline-cli query - <<'SQL'                 # multi-line SQL without shell quoting
SELECT id, artist_name, album_title, min_bitrate, current_spectral_bitrate
FROM album_requests
WHERE current_spectral_bitrate IS NOT NULL
ORDER BY updated_at DESC LIMIT 10
SQL
```

From doc1, run the CLI over SSH by sourcing doc2's sops-managed PG dotenv
inside a `sudo bash -c` (the secret is `-r-------- root root`, so a plain
`. /run/secrets/cratedigger-pgpass` in a user shell will get
`permission denied` and `pipeline-cli` will then fail with
`fe_sendauth: no password supplied`). `sudo` is NOPASSWD for `wheel` on
doc2, so this is non-interactive:

```bash
ssh doc2 'sudo bash -c "set -a; . /run/secrets/cratedigger-pgpass; set +a; export PGPASSWORD=\${PGPASSWORD:-\${PIPELINE_DB_PASSWORD:-\${POSTGRES_PASSWORD:-}}}; pipeline-cli query --json \"SELECT 1 AS ok\""'
```

Note the escaped `\$` and `\"` — they are evaluated inside the inner
`bash -c`, not by the outer ssh-side shell. For multi-line SQL, prefer
`pipeline-cli query - <<'SQL' ... SQL` *inside* the `sudo bash -c` body, or
write the query to a temp file and pass it as an argument.

`pipeline-cli query` sets `default_transaction_read_only = on` — safe for diagnostics. When debugging pipeline behavior, start with the simulator (`pipeline-cli quality`) and add scenarios that expose the bug FIRST — see `.claude/rules/code-quality.md` § "Pipeline Decision Debugging — Simulator-First TDD".

For search-plan iter2 triage signals, `album_requests.failure_class` (5-bucket cycle classification, written at plan-wrap) and `album_requests.unfindable_category` (4-bucket cohort taxonomy, written by the daily detection service) are queryable via `pipeline-cli query` — `GROUP BY failure_class` surfaces stuck-pattern distribution; `GROUP BY unfindable_category` surfaces unfindable-cohort distribution. `search_log.rejection_reason` (PR3 R22) is the per-search scalar that lets `GROUP BY` skip JSONB introspection into `candidates`. Full column inventory in `docs/pipeline-db-schema.md` § "Search-plan iteration 2".

## Full command reference

Every top-level `pipeline-cli` subcommand, one line each. Run `pipeline-cli routes` (or `pipeline-cli routes --json`) to regenerate this from the live argparse tree — it walks the same `_build_parser()` this table was derived from, so it can't drift from the actual CLI surface the way a hand-maintained list can.

| Subcommand | Purpose |
|---|---|
| `add` | Add a new request by MBID or Discogs ID |
| `beets-distance` | Real beets-distance between a download_log's audio and an MBID (refuses if MBID is outside the request's release group) |
| `ban-source` | Mark a request's server-resolved exact release as a bad rip, remove it from beets, and requeue it (requires `--confirm BAN`) |
| `cancel` | Cancel a request (set to skipped) |
| `disk-coverage` | Show which active pipeline rows are actually present in beets |
| `force-import` | Force-import a rejected download by download_log ID |
| `import-jobs` | List recent import queue jobs |
| `import-preview` | Preview whether an import would pass |
| `list` | List album requests |
| `long-tail` | Long-tail worklist — wanted cohort pre-banded by on-disk quality (missing / QualityRank / unknown) + in_flight_rescue |
| `library-delete` | Delete one server-resolved exact beets album; optional pipeline purge (requires `--confirm DELETE`) |
| `manual-import` | Import a local folder as a pipeline request |
| `quality` | Show quality state and simulate decisions |
| `query` | Run a read-only SQL query for debugging |
| `repair-spectral` | Fix albums stuck by stale `current_spectral_bitrate` (#18) |
| `replace` | Supersede a request with a new row at a different release id in the same release group/master (same pathway as the source) |
| `retry` | Reset a failed request to wanted |
| `routes` | Self-document the CLI surface — every subcommand, its args, and its description |
| `search-plan` | Inspect persisted search plans (read-only, U6) |
| `set` | Change the status of a request |
| `set-intent` | Toggle lossless-on-disk for a request |
| `show` | Show full details of a request |
| `status` | Show counts by status |
| `triage` | Read-only operator triage — request/search forensics, cohort listing, and `triage quarantine` for unreferenced immediate `failed_imports/` album folders |
| `wrong-match-delete` | Delete one visible Wrong Matches source folder |
| `wrong-match-delete-group` | Delete visible Wrong Matches source folders for one request |
| `wrong-match-triage` | Clean the full Wrong Matches queue using existing evidence |
| `youtube-album` | Resolve MBID/Discogs ID → YouTube Music album matrix (auto-widens to release group; N×M beets distances per YT sibling × MB sibling) |
| `youtube-rescue` | Submit a YouTube Music rescue ingest for one request (requires a resolver mapping; emits a `youtube_running` download_log row) |

`tests/test_docs_audit.py` enforces that every top-level subcommand from `_build_parser()` has a mention somewhere in this file — adding a subcommand without a row here fails the suite.
