# UI work: the dev-server screenshot loop (verify with photos before pushing)

**Proven on #575 PR2/PR2b (2026-07-10).** UI changes are verified by
*looking at them* on real pipeline data BEFORE pushing — unit tests and
code review both passed while three visually-obvious defects were live.
The loop caught, per round:

1. **Round 1** — the tab-wide "everything centered" root cause: the
   `recents-content` / `pipeline-content` / `wrong-matches-content`
   containers carried `class="loading"` (`text-align: center`)
   *permanently* — JS replaces `innerHTML` but never touched the class.
   Invisible to tests; obvious in a screenshot.
2. **Round 3** — `/api/pipeline/log` never forwarded `downloaded_label`,
   so the evidence strips shipped without codec labels. The JS unit test
   passed (it seeded the field into its mock); only the photo of real
   API data showed it missing. Classic mock-hides-the-contract.
3. **Round 4** — operator-reported "256kbps (was 256kbps)" nonsense
   verified fixed on the exact motivating row (request 8640) before push.

The general lesson: **for UI work, "the test passes" and "the reviewer
approved" do not establish "it looks right on real data".** Screenshots
of the live-db dev server do.

## The recipe (doc1)

```bash
# 1. Tunnel to the pipeline DB (doc2-local subnet)
ssh -N -L 15432:10.20.0.11:5432 doc2 &

# 2. Dev server, live read-only DB + per-thread beets handles
export PIPELINE_DB_DSN="postgresql://cratedigger:$(ssh doc2 'sudo cat /run/secrets/cratedigger-pgpass' | grep '^PGPASSWORD=' | cut -d= -f2)@127.0.0.1:15432/cratedigger"
setsid nix-shell --run "python3 scripts/web_dev_server.py --data live-db \
  --host 127.0.0.1 --port 8096 \
  --beets-db /mnt/virtio/cratedigger/beets-db/beets-library.db \
  --mb-api http://192.168.1.35:5200/ws/2 \
  --discogs-api https://discogs.ablz.au" \
  > /tmp/devserver.log 2>&1 < /dev/null &

# 3. Metadata precondition — do not accept screenshots until the changed
#    cross-source route itself returns HTTP 200.
COMPARE_STATUS="$(curl --silent --show-error --output /tmp/artist-compare.json \
  --write-out '%{http_code}' --get \
  --data-urlencode 'name=Deloris' \
  http://127.0.0.1:8096/api/artist/compare)"
test "$COMPARE_STATUS" = 200 || {
  echo "artist compare precondition failed: HTTP $COMPARE_STATUS" >&2
  exit 1
}

# 4. Headless Chromium with CDP open (playwright-mcp managed-launch is
#    broken on doc1 — it tries to install a browser into the read-only
#    nix store; the wrapper auto-ATTACHES when 9222 answers)
/nix/store/<hash>-playwright-browsers/chromium-*/chrome-linux64/chrome \
  --headless=new --remote-debugging-port=9222 \
  --user-data-dir=/tmp/some-writable-profile --no-first-run about:blank &
```

5. Spawn the **playwright agent** with a read-only brief: navigate
   `http://127.0.0.1:8096`, exercise the changed surface, screenshot each
   state (viewport ~1000×900), and report what it sees. Explicitly forbid
   mutating buttons (delete / force import / converge / Replace / status
   toggles). Resume the SAME agent for later rounds (it keeps context).
6. **The main agent must Read the PNGs itself.** The sub-agent's text
   report is a summary — the centering root-cause and layout jank were
   found by looking at the pixels, not by reading the report.
7. Fix → restart the dev server if Python changed (static `web/` files
   are served live; JS/HTML edits only need a browser reload) → rerun the
   round. Target the exact motivating row/card when the work started from
   an operator complaint.

For artist-comparison work, run the HTTP precondition against the exact artist
used for the screenshots before every acceptance round. A plausible MB-only
page is not evidence that the Discogs half of the route ran.

## Gotchas (each cost a debugging detour once)

- **Screenshot save roots**: the playwright MCP only writes inside its
  allowed roots (the repo checkout + `.playwright-mcp/`). Screenshots land
  in the WORKTREE — `mv` them out before `git add -A`.
- **`pkill` self-match**: `pkill -f web_dev_server.py` inside a compound
  command kills your own shell (the pattern matches the command string).
  Use the bracket trick: `pkill -f 'web_dev_[s]erver.py'` — and never put
  a pkill and the thing it must not kill in one compound command.
- **Shared-SQLite crash**: fixed in-tree (the dev server sets
  `beets_db_path` only, per-thread handles) — if "SQLite objects created
  in a thread…" 500s reappear, something re-introduced a shared handle.
- **Stale dev servers shadow the port**: `nohup nix-shell --run …` spawns
  a python child that survives killing the wrapper. Check
  `ss -ltnp | grep 8096` / `pgrep -af web_dev_[s]erver` before trusting
  a 200 — you may be talking to the OLD code.
- **MCP servers that started in managed mode never attach to CDP
  retroactively** — the CDP check happens at wrapper startup. The
  playwright agent (fresh server per invocation) attaches; the main
  session's already-running server does not.

## Related

- `docs/web-dev-server.md` — the dev-server modes and remote-dev tunnels.
- `docs/playwright-mcp.md` — MCP setup per machine.
- Issue #575 — the UI consolidation arc this loop was built for.
