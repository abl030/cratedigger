/* The one bounded check / watch-url / choose / confirm rescue control (#1003). */
import { API, toast } from './state.js';
import { esc, jsArg, youtubeBrowseUrl, youtubeSectionState } from './util.js';

export function youtubeResolverPayload(identifier, watchUrl) {
  return { identifier, ...(watchUrl ? { watch_url: watchUrl } : {}) };
}

function renderCandidateChoices(result, identifier, requestId, pick) {
  return (result.youtube_releases || []).map((release) => {
    const distances = Array.isArray(release.distances) ? release.distances : [];
    const exact = distances.find((distance) => distance.mbid === identifier && distance.outcome === 'ok' && typeof distance.distance === 'number' && Number.isFinite(distance.distance) && Number.isInteger(distance.total_mb_tracks) && distance.total_mb_tracks > 0);
    const finite = distances.filter((distance) => Number.isFinite(distance.distance));
    const best = finite.length ? Math.min(...finite.map((distance) => distance.distance)) : null;
    const distanceLabel = exact ? `exact dist ${exact.distance.toFixed(3)}` : best != null ? `best sibling dist ${best.toFixed(3)}` : 'no distance';
    const evidence = [release.year != null ? String(release.year) : '', release.track_count != null ? `${release.track_count}t` : '', distanceLabel].filter(Boolean).join(' · ');
    const url = youtubeBrowseUrl(release.yt_browse_id);
    const action = exact ? (pick ? `onclick="event.stopPropagation(); ${pick}(${Number(requestId)}, ${jsArg(release.yt_browse_id)})"` : `data-browse-id="${esc(release.yt_browse_id)}"`) : 'disabled';
    return `<div class="yt-rescue-choice"><a href="${esc(url)}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${esc(release.yt_browse_id)}</a> <span>${esc(evidence)}</span> <button class="p-btn" type="button" ${action}>${exact ? 'Rescue from this' : 'Exact evidence required'}</button></div>`;
  }).join('');
}

export function renderYoutubeRescueControl(key, requestId, identifier, result = null, handlers = {}) {
  if (!Number.isInteger(Number(requestId)) || !identifier) return '';
  const state = youtubeSectionState(result);
  const check = handlers.check || `window.checkYoutubeRescue(${jsArg(key)}, ${Number(requestId)}, ${jsArg(String(identifier))})`;
  const pick = handlers.pick || `window.pickYoutubeRescue`;
  const label = state.state === 'resolver_failed' ? 'Retry' : state.state === 'resolved_empty' ? 'Re-check' : 'Check YouTube';
  const choices = state.state === 'resolved_with_matrix'
    ? renderCandidateChoices(result, identifier, requestId, pick)
    : state.state === 'resolver_failed' || state.state === 'resolved_empty'
      ? `<span>${esc(state.message)}</span>` : '';
  return `<div class="yt-rescue-control" id="yt-rescue-${esc(key)}">
    <button class="p-btn" type="button" onclick="event.stopPropagation(); ${check}">${label}</button>
    <input class="yt-rescue-watch" type="text" id="yt-watch-${esc(key)}" onclick="event.stopPropagation()" placeholder="https://music.youtube.com/watch?v=…" aria-label="YouTube Music watch URL">
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
    const r = await fetch(`${API}/api/youtube-album`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(youtubeResolverPayload(identifier, watchUrl)) });
    const result = await r.json().catch(() => ({ error_message: `HTTP ${r.status}` }));
    if (host.dataset.generation !== generation || !resultHost) return;
    if (!r.ok || result.outcome !== 'ok') {
      resultHost.innerHTML = `<span>${esc(result.error_message || `Resolver failed (HTTP ${r.status}).`)}</span>`;
      return;
    }
    resultHost.innerHTML = renderCandidateChoices(result, identifier, requestId, '') || '<span>No YouTube album found.</span>';
    resultHost.querySelectorAll('[data-browse-id]').forEach((button) => button.addEventListener('click', async (event) => {
      event.stopPropagation();
      if (host.dataset.submitting === 'true' || !window.confirm('Queue this YouTube Music rescue?')) return;
      host.dataset.submitting = 'true';
      try {
        try {
          const rescue = await fetch(`${API}/api/pipeline/${requestId}/youtube-rescue`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ browse_id: button.dataset.browseId }) });
          const payload = await rescue.json().catch(() => ({ outcome: 'transient' }));
          toast(payload.outcome === 'accepted' ? 'YouTube rescue queued.' : (payload.detail || payload.error || 'YouTube rescue failed.'), payload.outcome !== 'accepted');
        } catch (_error) {
          toast('YouTube rescue failed: network unavailable.', true);
        }
      } finally { host.dataset.submitting = 'false'; }
    }));
  } catch (_error) {
    if (host.dataset.generation === generation && resultHost) {
      resultHost.innerHTML = '<span>Could not reach the resolver. Retry.</span>';
    }
  } finally { if (host.dataset.generation === generation) host.dataset.busy = 'false'; }
}
