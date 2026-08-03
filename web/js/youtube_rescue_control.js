/* Compact shared rescue control for release-detail surfaces (#1003). */
import { API } from './state.js';
import { esc } from './util.js';

export function renderYoutubeRescueControl(requestId, identifier) {
  if (!Number.isInteger(Number(requestId)) || !identifier) return '';
  const id = Number(requestId);
  return `<div class="yt-rescue-control" id="yt-rescue-${id}">
    <button class="p-btn" type="button" onclick="window.checkYoutubeRescue(${id}, ${JSON.stringify(String(identifier))})">Check YouTube</button>
    <input id="yt-watch-${id}" placeholder="https://music.youtube.com/watch?v=…" aria-label="YouTube Music watch URL">
  </div>`;
}

export async function checkYoutubeRescue(requestId, identifier) {
  const host = document.getElementById(`yt-rescue-${requestId}`);
  if (!host || host.dataset.busy === 'true') return;
  host.dataset.busy = 'true';
  const watchUrl = document.getElementById(`yt-watch-${requestId}`)?.value.trim();
  try {
    const r = await fetch(`${API}/api/youtube-album`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ identifier, ...(watchUrl ? { watch_url: watchUrl } : {}) }) });
    const result = await r.json();
    const choices = (result.youtube_releases || []).map((release) => `<button class="p-btn" type="button" data-browse-id="${esc(release.yt_browse_id)}">Rescue from ${esc(release.yt_browse_id)}</button>`).join('');
    host.insertAdjacentHTML('beforeend', choices || `<span>${esc(result.error_message || 'No YouTube album found.')}</span>`);
    host.querySelectorAll('[data-browse-id]').forEach((button) => button.addEventListener('click', async () => {
      if (!window.confirm('Queue this YouTube Music rescue?')) return;
      await fetch(`${API}/api/pipeline/${requestId}/youtube-rescue`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ browse_id: button.dataset.browseId }) });
    }, { once: true }));
  } finally { host.dataset.busy = 'false'; }
}
