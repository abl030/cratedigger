# Pre-commit Quality Check

Run pyright + full test suite + type safety grep gate. Use this before committing.

## Steps

1. Run pyright on all key files:
```bash
pyright lib/quality.py lib/beets.py lib/beets_db.py lib/pipeline_db.py lib/import_dispatch.py lib/download.py harness/import_one.py harness/beets_harness.py album_source.py cratedigger.py scripts/pipeline_cli.py web/routes/pipeline.py web/routes/imports.py web/routes/library.py web/routes/browse.py tests/test_validation_result.py tests/test_import_result.py tests/test_quality_decisions.py tests/test_beets_db.py tests/test_pipeline_db.py tests/test_album_source.py tests/test_import_dispatch.py tests/test_web_server.py
```

Must be **0 errors**. Do not proceed if there are new errors (psycopg2/slskd_api "could not be resolved" warnings are OK — they're C extensions).

2. Dict access grep gate — catch missed dict→attribute conversions on typed objects:
```bash
grep -rn 'album\["\|album\['"'"'' --include='*.py' cratedigger.py lib/download.py album_source.py
```

Must return **0 matches**. `album` in cratedigger.py/download.py is a typed `AlbumRecord` dataclass. Note: `req["field"]` is fine — `get_request()` returns `dict[str, Any]`. `release["field"]` in web routes is fine — those are raw MusicBrainz API dicts.

3. Run full test suite:
```bash
nix-shell --run "bash scripts/run_tests.sh"
```

4. Check results:
```bash
grep -E "^Ran |^OK|^FAILED" /tmp/cratedigger-test-output.txt
grep "^FAIL:\|^ERROR:" /tmp/cratedigger-test-output.txt
```

Must show `OK`. slskd live test skips (Docker not running) are acceptable. The `test_calls_refresh_endpoint` error is a known pre-existing issue.

5. If all pass, safe to commit.
