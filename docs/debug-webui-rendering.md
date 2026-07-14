# Debug: Web UI Release Group Rendering Bug

## Problem

Some release groups with many releases (100+) intermittently fail to render when clicked in the web UI at music.ablz.au. Example: Bruce Springsteen's "Born to Run" (release group `39b22944-7503-3937-8bba-09b17281cc6a`, 105 releases).

## Symptoms

- Click a release group title → "Loading releases..." appears → nothing renders, stays on "Loading releases..." or goes blank
- The API endpoint works fine: `curl http://localhost:8085/api/release-group/39b22944-7503-3937-8bba-09b17281cc6a` returns valid JSON with 105 releases (~24KB)
- The issue is intermittent — sometimes works after hard refresh (Ctrl+Shift+R)
- Smaller release groups (< 25 releases) always work

## Backend (confirmed working)

The server-side pagination fix is deployed and working:
- `web/mb.py` `get_release_group_releases()` uses the browse endpoint (`/release?release-group=...`) with pagination, not the lookup endpoint (which caps at 25)
- Response time: ~1 second for Born to Run

## Frontend code to investigate

The rendering logic is in `web/js/discography.js`, function `loadReleaseGroup()` (around line 268):

```javascript
async function loadReleaseGroup(id, el) {
  const relEl = document.getElementById('rel-' + id);
  if (relEl.innerHTML) { relEl.innerHTML = ''; return; }  // toggle off
  relEl.innerHTML = '<div class="loading">Loading releases...</div>';
  try {
    const r = await fetch(`${API}/api/release-group/${id}`);
    const data = await r.json();
    const all = (data.releases || []).sort(...);
    const { visible, hidden } = splitPressings(all);  // owned/in-flight never hidden
    // ... renderRelease() for each, set relEl.innerHTML
  } catch (e) { relEl.innerHTML = '<div class="loading">Failed to load</div>'; }
}
```

## Likely causes to investigate

1. **JS error in template literal** — the `renderRelease()` function builds HTML via template literals. If any release data contains characters that break the template (backticks, `${`, unescaped HTML), the whole render fails silently in the catch block. But "Failed to load" should show in that case.

2. **Browser caching old JS** — the HTML is served without cache-control headers. The browser may cache the old `index.html` and the JS doesn't match the new API response format. Add `Cache-Control: no-cache` header.

3. **`event.stopPropagation()` / click handler conflict** — the release group `.rg` div has an onclick that calls `loadReleaseGroup()`. The parent `.type-body` div might be collapsing/toggling when the click bubbles up incorrectly.

4. **`relEl` not found** — expansion targets are namespaced as `rel-{source}-{identityKind}-{id}` and manual clicks prefer the clicked row's next sibling. A stale source/kind or a detached row therefore resolves no target. The `.releases` element lives inside a `.type-body`; verify the row was not replaced by a parent re-render while the fetch was in flight.

5. **Variable shadowing** — `const r` in `await fetch(...)` shadows the `r` parameter in `.filter(r => ...)`. This could cause issues in some JS engines.

## How to debug

1. Open browser dev tools (F12) → Console tab
2. Click "Born to Run" release group
3. Look for JS errors in console
4. Network tab: check if the API request goes out and what comes back
5. If no errors visible, add `console.log` statements in `loadReleaseGroup()` to trace execution

## Files

- `web/index.html` — frontend (the bug is here)
- `web/server.py` — backend (confirmed working)
- `web/mb.py` — MB API helpers (confirmed working)
- `docs/webui-primer.md` — full architecture docs
