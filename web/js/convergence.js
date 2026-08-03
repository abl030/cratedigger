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

/** @param {number} value */
function formatKhz(value) {
  return `${Number((value / 1000).toFixed(2))} kHz`;
}

/**
 * Operator prompt. The 500 Hz band is an aggregate observation; the raw
 * range stays visible so "same band" cannot be mistaken for exact equality.
 * @param {Object|null|undefined} signal
 * @param {string|null|undefined} requestStatus
 * @param {string} [origin]
 */
export function renderConvergencePrompt(signal, requestStatus, origin = 'unknown') {
  if (!signal) return '';
  const requestId = Number(signal.request_id);
  const cliffHz = Number(signal.cliff_hz);
  const rawMinHz = Number(signal.raw_cliff_min_hz);
  const rawMaxHz = Number(signal.raw_cliff_max_hz);
  const details = [
    `${Number(signal.distinct_peer_count)} peers`,
    `${Number(signal.observation_count)} observations`,
    `${Number(signal.distinct_candidate_snapshot_count)} snapshots`,
    `${Number(signal.distinct_codec_count)} codecs`,
    `raw cliffs ${formatKhz(rawMinHz)}-${formatKhz(rawMaxHz)} (${Number(signal.cliff_spread_hz)} Hz spread)`,
    `shared ${formatKhz(cliffHz)} band`,
  ].join(' · ');
  const actionSignal = JSON.stringify({
    request_id: requestId,
    signal_token: String(signal.signal_token),
  }).replace(/</g, '\\u003c');
  const action = requestStatus === 'wanted'
    ? `<button class="p-btn convergence-stop" onclick="event.stopPropagation(); window.stopConvergedSearch(${esc(actionSignal)}, this, ${esc(JSON.stringify(origin))})">Stop searching</button>`
    : requestStatus === 'unsearchable'
      ? '<span class="convergence-stopped">Searching stopped; Resume reopens the request.</span>'
      : '';
  return `<div class="convergence-prompt" onclick="event.stopPropagation()">
    <div><strong>Search appears converged.</strong> ${esc(details)}</div>
    <div>This holding remains provisional—not proof. Stop only if you want to chill at the best repeatedly observed result.</div>
    ${action}
  </div>`;
}

/** @param {Response|Object} response */
async function responseBody(response) {
  try {
    if (typeof response.text === 'function') {
      const text = await response.text();
      return text ? JSON.parse(text) : {};
    }
    if (typeof response.json === 'function') return await response.json();
  } catch (_error) {
    return {};
  }
  return {};
}

/** @param {Object|null|undefined} button @param {boolean} busy */
function setBusy(button, busy) {
  if (!button) return;
  if (button.dataset) {
    button.dataset.convergenceBusy = busy ? 'true' : 'false';
  }
  button.disabled = busy;
  if (typeof button.setAttribute === 'function') {
    button.setAttribute('aria-busy', busy ? 'true' : 'false');
  }
  button.textContent = busy ? 'Stopping…' : 'Stop searching';
}

/** @param {string} origin */
async function refreshOrigin(origin) {
  if (origin === 'recents') {
    await window.loadRecents?.();
    return;
  }
  if (origin === 'library-detail' || origin === 'browse') {
    await window.reloadBrowseArtist?.();
    return;
  }
  await window.loadRecents?.();
  await window.reloadBrowseArtist?.();
}

/**
 * Submit an opaque snapshot token once. A stale signal is removed and its
 * originating surface is refreshed; transient failures keep the action usable.
 * @param {Object} signal
 * @param {Object|null} [button]
 * @param {string} [origin]
 */
export async function stopConvergedSearch(signal, button = null, origin = 'unknown') {
  if (button?.dataset?.convergenceBusy === 'true') return null;
  setBusy(button, true);
  const requestId = Number(signal.request_id);
  try {
    const response = await fetch(`/api/triage/${requestId}/stop-converged-search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        confirm: 'STOP',
        signal_token: String(signal.signal_token),
      }),
    });
    const body = await responseBody(response);
    if (response.ok) {
      const prompt = button?.closest?.('.convergence-prompt');
      if (prompt) {
        prompt.innerHTML = '<span class="convergence-stopped">Searching stopped. The holding remains provisional and unproven.</span>';
      }
      window.toast?.('Searching stopped. The holding remains provisional and unproven.', false);
      await refreshOrigin(origin).catch(() => {});
      return body;
    }
    if (response.status === 409 || response.status === 422) {
      button?.closest?.('.convergence-prompt')?.remove?.();
      window.toast?.('That convergence observation is no longer current. Refreshing.', true);
      await refreshOrigin(origin).catch(() => {});
      return body;
    }
    window.toast?.(
      `Stop searching failed: ${body.outcome || body.error || `HTTP ${response.status}`}`,
      true,
    );
    setBusy(button, false);
    return body;
  } catch (_error) {
    window.toast?.('Stop searching failed: network unavailable', true);
    setBusy(button, false);
    return { outcome: 'unavailable' };
  }
}
