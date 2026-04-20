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
- Redis cache: MB data 24h TTL, beets/pipeline 5min TTL, explicit invalidation on POST mutations
- Static JS served at `/js/*.js` — `node --check` validates syntax in CI
- The web UI reads download_log JSONB columns (import_result, validation_result) — use the typed field names from the dataclasses, not arbitrary strings
- After changes: `ssh doc2 'sudo systemctl restart cratedigger-web'`
