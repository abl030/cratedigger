---
name: check
description: Run Cratedigger's pre-commit type, invariant, and full-suite quality gates.
---

# Pre-commit Quality Check

Run pyright + full test suite + type safety grep gate. Use this before committing.

## Steps

1. Run pyright on the full repository:
```bash
nix-shell --run "pyright"
```

Must be **0 errors**. Do not proceed if there are new errors (psycopg2/slskd_api "could not be resolved" warnings are OK — they're C extensions).

2. Dict access grep gate — catch missed dict→attribute conversions on typed objects:
```bash
rg 'album\["|album\['"'"'' cratedigger.py lib/download.py album_source.py
```

Must return **0 matches**. `album` in cratedigger.py/download.py is a typed `AlbumRecord` dataclass. Note: `req["field"]` is fine — `get_request()` returns `dict[str, Any]`. `release["field"]` in web routes is fine — those are raw MusicBrainz API dicts.

3. Run full test suite:
```bash
nix-shell --run "bash scripts/run_tests.sh"
```

4. Check results:
```bash
export ARTIFACT=/tmp/cratedigger-...  # copy the directory printed by that run
grep -E "^Ran |^OK|^FAILED" "$ARTIFACT/output.log"
grep "^FAIL:\|^ERROR:" "$ARTIFACT/output.log"
```

Must show `OK` with no skipped tests. Investigate every failure; do not carry a
chat-era "known issue" exemption forward without current repository evidence.
For a clean committed target, verify exact provenance with:

```bash
export ARTIFACT=/tmp/cratedigger-...  # if starting in a fresh shell
nix-shell --run 'python3 scripts/test_artifact.py verify --artifact \
  "$ARTIFACT" --expected-head "$(git rev-parse HEAD)"'
```

5. If all pass, safe to commit.
