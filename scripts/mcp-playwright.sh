#!/usr/bin/env bash
# Wrapper for Playwright MCP server (nixpkgs `playwright-mcp`).
# Installed on every Claude Code host via home-manager (see nixosconfig
# modules/home-manager/services/claude-code.nix).
#
# Two operating modes:
#
# 1. CDP attach mode (preferred on desktops)
#    If a Chromium is listening on 127.0.0.1:${PLAYWRIGHT_MCP_CDP_PORT:-9222}
#    (i.e. you've run the playwright-chromium launcher in nixosconfig), the
#    wrapper connects to it via --cdp-endpoint. The window survives MCP
#    server exits, so repeated agent invocations don't keep spawning fresh
#    windows.
#
# 2. Playwright-managed mode (default on headless hosts)
#    Playwright launches and tears down its own Chromium. Headless vs headed
#    is auto-detected from DISPLAY / WAYLAND_DISPLAY. Override via:
#      PLAYWRIGHT_MCP_FORCE_HEADLESS=1   always headless
#      PLAYWRIGHT_MCP_FORCE_HEADED=1     always headed (needs a display)
#
# Extra flags from the agent's mcpServers.args are appended via "$@".
set -euo pipefail

CDP_PORT="${PLAYWRIGHT_MCP_CDP_PORT:-9222}"
CDP_ENDPOINT="http://127.0.0.1:${CDP_PORT}"

if curl -sf --max-time 1 "${CDP_ENDPOINT}/json/version" >/dev/null 2>&1; then
  exec mcp-server-playwright --cdp-endpoint "${CDP_ENDPOINT}" "$@"
fi

mode=()
if [[ -n "${PLAYWRIGHT_MCP_FORCE_HEADLESS:-}" ]]; then
  mode=(--headless)
elif [[ -n "${PLAYWRIGHT_MCP_FORCE_HEADED:-}" ]]; then
  mode=()
elif [[ -z "${DISPLAY:-}" && -z "${WAYLAND_DISPLAY:-}" ]]; then
  mode=(--headless)
fi

exec mcp-server-playwright "${mode[@]}" "$@"
