---
name: Always use nix-shell for tests and Python
description: All Python commands in cratedigger must run inside nix-shell — provides psycopg2, sox, ffmpeg, etc.
type: feedback
---

Always run tests and Python commands via `nix-shell --run "..."` in the cratedigger repo.

**Why:** The dev shell (`shell.nix`) provides psycopg2, sox, ffmpeg, music-tag, slskd-api. Running `python3` directly causes import failures (e.g. psycopg2 missing → test_pipeline_cli errors) and skipped tests. User called this out after I ran tests outside nix-shell and missed 15 tests.

**How to apply:** Every `python3 -m unittest`, `python3 -c`, or any Python invocation in this repo should be wrapped in `nix-shell --run "..."`. No exceptions.

For the full test suite, use `nix-shell --run "bash scripts/run_tests.sh"` — it saves output to `/tmp/cratedigger-test-output.txt`. NEVER re-run the full 2-minute suite just to grep output differently — read the saved file instead.
