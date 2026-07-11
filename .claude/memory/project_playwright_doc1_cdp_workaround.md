---
name: playwright-doc1-cdp-workaround
description: doc1 playwright-mcp 0.0.76 managed mode is broken (read-only nix store); launch store chromium with CDP 9222 first
metadata: 
  node_type: memory
  type: project
  originSessionId: 2f201277-ea14-4a12-96d9-f567f6debb65
---

Since the nixpkgs bump to playwright-mcp 0.0.76 (observed 2026-07-10), managed-launch mode on doc1 fails: the server tries to `mkdir mcp-chrome-for-testing-*` inside the read-only `PLAYWRIGHT_BROWSERS_PATH` nix store path (it wants its own chrome-for-testing flavor, ignoring the bundled `chromium-1223`).

**Workaround that works:** launch the store chromium with CDP before any playwright use, then `scripts/mcp-playwright.sh` auto-attaches via `--cdp-endpoint`:

```
/nix/store/<hash>-playwright-browsers/chromium-1223/chrome-linux64/chrome \
  --headless=new --remote-debugging-port=9222 \
  --user-data-dir=<writable tmp> --no-first-run about:blank &
```

Caveats: an MCP server that already started in managed mode won't attach retroactively — spawn the `playwright` subagent (fresh server per invocation) or restart the server. Stale headless MCP server processes from earlier sessions can also shadow the CDP-attached one; kill the stale ones. Screenshot tool only writes inside allowed roots (`~/cratedigger`, `.playwright-mcp/`) — save there and `cp` out.

**Proper fix (not yet done):** make the wrapper pass `--executable-path` to the bundled chromium (or `--browser chromium`) in managed mode. Wrapper lives in this repo at `scripts/mcp-playwright.sh`; installed via nixosconfig home-manager.

**Instability (2026-07-10 evening):** the headless CDP chromium repeatedly died mid-session (~2-3 min lifetimes; four deaths in one verification round). Agents should check `ss -ltn | grep 9222` before each batch, relaunch on death, and drive flows in tight batches rather than one long session. Root cause unknown (possibly doc1 memory pressure); worth watching.

**Attach-order confirmed hard (2026-07-11, #608 session):** the session-level MCP server probes 9222 at ITS start — launching chromium after the server is already up does NOT make tools attach; they still fail with the ENOENT store mkdir. **Raw-CDP fallback that works with zero MCP:** the dev shell has the `websockets` Python lib — `PUT /json/new` for a tab, then over the tab's `webSocketDebuggerUrl` send `Emulation.setDeviceMetricsOverride` → `Page.navigate` → sleep → `Page.captureScreenshot` (base64 PNG). ~40-line script, used successfully for the #608 screenshot loop (incl. `Runtime.evaluate` `window.showTab('recents')` to switch SPA tabs — location.hash does not switch tabs on load). chrome one-shot `--headless=new --screenshot=` hangs forever on the SPA (never idle); don't bother.
