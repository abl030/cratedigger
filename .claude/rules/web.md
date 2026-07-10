---
paths:
  - "web/**"
---

# Web UI Rules

- Single-page app: stdlib `http.server`, vanilla JS, no build step, no npm
- HTML + CSS in `web/index.html`, JS in `web/js/*.js` (ES6 modules, `<script type="module">`)
- Route handlers in `web/routes/*.py` — server.py is routing/cache/main only (~450 lines)
- Beets queries via `lib/beets_db.py` `BeetsDB` class — never raw `sqlite3.connect()` in handlers
- MusicBrainz queries through local mirror at `http://192.168.1.35:5200` via `web/mb.py`
- Redis cache: `meta:` namespace only — pure MB/Discogs mirror metadata, 24h TTL via `memoize_meta` in `web/mb.py`/`web/discogs.py`. The routing-level response cache was REMOVED by #101 (it baked pipeline overlay state into cached responses); do not reintroduce response caching or reference `cache_api.cached(...)` — it no longer exists
- Static JS served at `/js/*.js` — `node --check` validates syntax in CI
- Fetch-on-input UI must stamp requests with a module-scoped in-flight token and discard stale responses before rendering
- The web UI reads download_log JSONB columns (import_result, validation_result) — use the typed field names from the dataclasses, not arbitrary strings
- **Visible UI changes are verified with the dev-server screenshot loop BEFORE pushing** — live-db dev server + CDP chromium + playwright agent, and the main agent Reads the PNGs itself. Tests+review alone shipped three visually-obvious defects on #575. Full recipe + gotchas: `docs/solutions/ui-dev-server-screenshot-loop.md`
- After changes: `ssh doc2 'sudo systemctl restart cratedigger-web'`
