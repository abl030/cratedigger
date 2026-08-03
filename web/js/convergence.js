// @ts-check

/** @param {unknown} value */
function esc(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/** @param {Object|null|undefined} signal */
export function convergenceBadge(signal) {
  if (!signal) return '';
  return '<span class="badge badge-converged">search converged</span>';
}

/**
 * Operator prompt. Raw-cliff constancy is deliberately described as
 * provisional—not proof; only the operator may stop the forever cadence.
 * @param {Object|null|undefined} signal
 * @param {string|null|undefined} requestStatus
 */
export function renderConvergencePrompt(signal, requestStatus) {
  if (!signal) return '';
  const requestId = Number(signal.request_id);
  const latestLogId = Number(signal.latest_qualifying_log_id);
  const cliffHz = Number(signal.cliff_hz);
  const details = `${Number(signal.distinct_peer_count)} distinct peers · ${Number(signal.observation_count)} observations · ${Number(signal.distinct_candidate_snapshot_count)} snapshots · ${cliffHz / 1000} kHz`;
  const action = requestStatus === 'wanted'
    ? `<button class="p-btn convergence-stop" onclick="event.stopPropagation(); window.stopConvergedSearch({request_id:${requestId},latest_qualifying_log_id:${latestLogId},cliff_hz:${cliffHz}})">Stop searching</button>`
    : requestStatus === 'unsearchable'
      ? '<span class="convergence-stopped">Searching stopped; Resume reopens the request.</span>'
      : '';
  return `<div class="convergence-prompt" onclick="event.stopPropagation()">
    <div><strong>Search appears converged.</strong> ${esc(details)}</div>
    <div>This holding remains provisional—not proof. Stop only if you want to chill at the best repeatedly observed result.</div>
    ${action}
  </div>`;
}

/** @param {Object} signal */
export async function stopConvergedSearch(signal) {
  const requestId = Number(signal.request_id);
  const response = await fetch(`/api/triage/${requestId}/stop-converged-search`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      confirm: 'STOP',
      latest_qualifying_log_id: Number(signal.latest_qualifying_log_id),
      cliff_hz: Number(signal.cliff_hz),
    }),
  });
  const body = await response.json();
  if (!response.ok) {
    window.toast?.(`Stop searching failed: ${body.outcome || body.error || `HTTP ${response.status}`}`, true);
    return body;
  }
  window.toast?.('Searching stopped. The holding remains provisional and unproven.', false);
  window.loadRecents?.();
  window.reloadBrowseArtist?.();
  return body;
}
