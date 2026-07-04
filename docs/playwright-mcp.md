# Playwright MCP (Web UI Testing)

The Playwright MCP server provides browser automation tools for testing the web UI at `https://music.ablz.au`. Use `browser_navigate`, `browser_snapshot`, `browser_click`, `browser_fill_form`, `browser_console_messages`, etc.

**The `playwright` agent needs no `.mcp.json`** — its frontmatter (`.claude/agents/playwright.md`) declares its own MCP server via `scripts/mcp-playwright.sh`, which spawns fresh on every agent invocation. `.mcp.json` (gitignored, per-machine) is only needed to expose the browser tools to the main session.

## Setup

### Linux / doc1 (primary dev host, headless)

Everything comes from Nix — no npm, no `playwright install`:

- `playwright-mcp` (nixpkgs) is installed via home-manager (`nixosconfig modules/home-manager/services/claude-code.nix`). Its wrapper exports `PLAYWRIGHT_BROWSERS_PATH` into the nix store, so browsers are bundled and patched for NixOS. Do not download browsers into `~/.cache/ms-playwright` — non-Nix binaries fail with missing shared libraries (`libglib-2.0.so.0`).
- `scripts/mcp-playwright.sh` resolves the server binary by either name (`playwright-mcp` from nixpkgs, `mcp-server-playwright` from npm) and auto-selects `--headless` when `DISPLAY`/`WAYLAND_DISPLAY` is unset — which is always, on doc1.
- Optional `.mcp.json` for main-session tools:

```json
{
  "mcpServers": {
    "playwright": {
      "command": "/home/abl030/cratedigger/scripts/mcp-playwright.sh",
      "args": []
    }
  }
}
```

Smoke check after a nixpkgs bump: `scripts/mcp-playwright.sh` should start and answer an MCP `initialize` over stdio; the bundled browser is at `$PLAYWRIGHT_BROWSERS_PATH/chromium-*/chrome-linux64/chrome`.

### Windows laptop (headed, legacy)

Node.js installed via scoop. `.mcp.json` must use absolute paths because scoop shims aren't in the Claude Code process PATH:

```json
{
  "mcpServers": {
    "playwright": {
      "command": "C:\\Users\\abl030\\scoop\\apps\\nodejs\\current\\node.exe",
      "args": ["C:\\Users\\abl030\\scoop\\apps\\nodejs\\current\\bin\\node_modules\\@playwright\\mcp\\cli.js"]
    }
  }
}
```

Requires:
1. `scoop install nodejs`
2. `npm install -g @playwright/mcp@latest` (with PATH set)
3. `npx playwright install chromium` to download the browser binary (~183MB, stored in `%LOCALAPPDATA%\ms-playwright\`)

## Usage notes

- Always use `https://music.ablz.au` (not http — connection will time out).
- `browser_snapshot` returns an accessibility tree (better than screenshots for automation).
- Use `browser_console_messages` with `level: "error"` to check for JS errors after interactions.
- Use `browser_wait_for` with `textGone` to wait for loading states to resolve.
- `.mcp.json` is gitignored (platform-specific paths) — each machine needs its own.
- When testing search flows, use **small/obscure artists** — the MusicBrainz mirror has a 15s timeout and a query like "Radiohead" will blow it.
