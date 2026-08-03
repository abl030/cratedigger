/* The one bounded check / watch-url / choose / confirm rescue control (#1003). */
import { API, toast } from './state.js';
import { esc, jsArg, youtubeSectionState } from './util.js';

export function renderYoutubeRescueControl(key, requestId, identifier, result = null, handlers = {}) {
  if (!Number.isInteger(Number(requestId)) || !identifier) return '';
  const state = youtubeSectionState(result);
  const check = handlers.check || `window.checkYoutubeRescue(${jsArg(key)}, ${Number(requestId)}, ${jsArg(String(identifier))})`;
  const pick = handlers.pick || `window.pickYoutubeRescue`;
  const label = state.state === 'resolver_failed' ? 'Retry' : state.state === 'resolved_empty' ? 'Re-check' : 'Check YouTube';
  const choices = state.state === 'resolved_with_matrix'
    ? (result.youtube_releases || []).map((release) => `<button class="p-btn" type="button" onclick="${pick}(${Number(requestId)}, ${jsArg(release.yt_browse_id)})">Rescue from ${esc(release.yt_browse_id)}</button>`).join('')
    : state.state === 'resolver_failed' || state.state === 'resolved_empty'
      ? `<span>${esc(state.message)}</span>` : '';
  return `<div class="yt-rescue-control" id="yt-rescue-${esc(key)}">
    <button class="p-btn" type="button" onclick="${check}">${label}</button>
    <input id="yt-watch-${esc(key)}" placeholder="https://music.youtube.com/watch?v=…" aria-label="YouTube Music watch URL">
    <div class="yt-rescue-result">${choices}</div>
  </div>`;
}

export async function checkYoutubeRescue(key, requestId, identifier) {
  const host = document.getElementById(`yt-rescue-${key}`);
  if (!host || host.dataset.busy === 'true') return;
  const generation = String(Number(host.dataset.generation || '0') + 1);
  host.dataset.generation = generation;
  host.dataset.busy = 'true';
  const resultHost = host.querySelector('.yt-rescue-result');
  const watchUrl = host.querySelector('input')?.value.trim();
  try {
    const r = await fetch(`${API}/api/youtube-album`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ identifier, ...(watchUrl ? { watch_url: watchUrl } : {}) }) });
    const result = await r.json().catch(() => ({ error_message: `HTTP ${r.status}` }));
    if (host.dataset.generation !== generation || !resultHost) return;
    if (!r.ok || result.outcome !== 'ok') {
      resultHost.innerHTML = `<span>${esc(result.error_message || `Resolver failed (HTTP ${r.status}).`)}</span>`;
      return;
    }
    resultHost.innerHTML = (result.youtube_releases || []).map((release) => `<button class="p-btn" type="button" data-browse-id="${esc(release.yt_browse_id)}">Rescue from ${esc(release.yt_browse_id)}</button>`).join('') || '<span>No YouTube album found.</span>';
    resultHost.querySelectorAll('[data-browse-id]').forEach((button) => button.addEventListener('click', async () => {
      if (host.dataset.submitting === 'true' || !window.confirm('Queue this YouTube Music rescue?')) return;
      host.dataset.submitting = 'true';
      try {
        const rescue = await fetch(`${API}/api/pipeline/${requestId}/youtube-rescue`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ browse_id: button.dataset.browseId }) });
        const payload = await rescue.json().catch(() => ({ outcome: 'transient' }));
        toast(payload.outcome === 'accepted' ? 'YouTube rescue queued.' : (payload.detail || payload.error || 'YouTube rescue failed.'), payload.outcome !== 'accepted');
      } finally { host.dataset.submitting = 'false'; }
    }));
  } finally { if (host.dataset.generation === generation) host.dataset.busy = 'false'; }
}
