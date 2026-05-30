# Debugging quality decisions

```bash
pipeline-cli show <request_id>               # quality columns + download history with import decisions
pipeline-cli quality <request_id>            # simulate gate for genuine FLAC / V0 / CBR 320 / suspect FLAC
pipeline-cli debug-download <download_log_id>  # raw JSONB audit for one attempt
pipeline-cli search-plan show <request_id>   # active plan + cursor + per-slot usefulness stats (--json for machine output)
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
