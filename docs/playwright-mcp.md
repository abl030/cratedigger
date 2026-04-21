# Playwright MCP (Web UI Testing)

The Playwright MCP server provides browser automation tools for testing the web UI at `https://music.ablz.au`. Configured in `.mcp.json` (not committed — platform-specific). Use `browser_navigate`, `browser_snapshot`, `browser_click`, `browser_fill_form`, `browser_console_messages`, etc.

## Setup

### Windows laptop

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

### Linux (doc1)

Use npx directly — Node.js is available system-wide:

```json
{
  "mcpServers": {
    "playwright": {
      "command": "npx",
      "args": ["@playwright/mcp@latest"]
    }
  }
}
```

First run will auto-install the package. You may still need `npx playwright install chromium` for the browser binary.

## Usage notes

- Always use `https://music.ablz.au` (not http — connection will time out).
- `browser_snapshot` returns an accessibility tree (better than screenshots for automation).
- Use `browser_console_messages` with `level: "error"` to check for JS errors after interactions.
- Use `browser_wait_for` with `textGone` to wait for loading states to resolve.
- `.mcp.json` is gitignored (platform-specific paths) — each machine needs its own.
- When testing search flows, use **small/obscure artists** — the MusicBrainz mirror has a 15s timeout and a query like "Radiohead" will blow it.
