// @ts-check

/**
 * Replace operator action — shared picker modal.
 *
 * Handles two click modes:
 *
 *   Standard mode (source row IS the request being replaced):
 *     - Caller passes `{ sourceRequestId, releaseGroupId, sourceLabel }`.
 *     - Picker fetches release-group siblings from the MB mirror via
 *       `GET /api/release-group/<rg_id>`.
 *     - Operator picks a pressing → confirm dialog → POST to
 *       `/api/pipeline/<sourceRequestId>/replace`.
 *
 *   Inverted mode (Browse-search, row IS the new MBID):
 *     - Caller passes `{ targetMbid, releaseGroupId, targetLabel }`.
 *     - Picker fetches `GET /api/pipeline/requests-by-rg/<rg_id>` to
 *       discover which existing non-replaced request to supersede.
 *     - 0 results → error toast; the calling button should not have been
 *       enabled in this case (R7).
 *     - 1 result → skip request-picker, go straight to confirm.
 *     - 2+ results → "which request to replace?" list → confirm.
 *
 * The module is callable from cross-module onclick handlers via the
 * `window.openReplacePicker` binding installed in main.js.
 */

/**
 * @typedef {Object} ReplacePickerOptionsStandard
 * @property {number} sourceRequestId
 * @property {string} releaseGroupId
 * @property {string} [sourceLabel]  // "Pet Grief — Old Pressing"
 * @property {string} [source]       // calling-tab identifier for telemetry
 */

/**
 * @typedef {Object} ReplacePickerOptionsInverted
 * @property {string} targetMbid
 * @property {string} releaseGroupId
 * @property {string} [targetLabel]  // "Pet Grief — New Pressing (2025, JP)"
 * @property {string} [source]
 */

/**
 * @typedef {ReplacePickerOptionsStandard | ReplacePickerOptionsInverted} ReplacePickerOptions
 */

/**
 * @typedef {Object} ReleaseGroupSibling
 * @property {string} id
 * @property {string} title
 * @property {string} [date]
 * @property {string} [country]
 * @property {string} [status]
 * @property {number} [track_count]
 * @property {string} [format]
 */

/**
 * @typedef {Object} ExistingRequest
 * @property {number} id
 * @property {string} mb_release_id
 * @property {string} status
 * @property {string} artist_name
 * @property {string} album_title
 */

/** @typedef {{ outcome: 'cancelled' } | { outcome: 'confirmed', sourceRequestId: number, targetMbid: string, response: any }} ReplacePickerResult */

/* --------------------------------------------------------------------------
 * Pure renderers — kept pure so tests can call them in Node without a DOM.
 * ------------------------------------------------------------------------ */

/**
 * Escape a string for safe insertion into HTML text content.
 *
 * @param {string} s
 * @returns {string}
 */
export function esc(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Render the "pressings in this release group" list (standard mode).
 *
 * @param {ReleaseGroupSibling[]} releases
 * @param {string} sourceMbid       // current pressing — disabled in the list
 * @returns {string}
 */
export function renderPressingsList(releases, sourceMbid) {
  if (!releases.length) {
    return '<p style="color:#888;">No pressings found in this release group.</p>';
  }
  const rows = releases.map((r) => {
    const isCurrent = r.id === sourceMbid;
    const year = r.date ? r.date.slice(0, 4) : '';
    const meta = [year, r.country, r.format, r.track_count ? `${r.track_count}tk` : '']
      .filter((x) => !!x)
      .join(' · ');
    const disabledAttr = isCurrent ? ' disabled' : '';
    const labelSuffix = isCurrent ? ' (current pressing)' : '';
    return `<li>
      <button class="btn"${disabledAttr} data-mbid="${esc(r.id)}">
        <strong>${esc(r.title)}</strong>${labelSuffix}<br>
        <small style="color:#888;">${esc(meta)} · ${esc(r.id)}</small>
      </button>
    </li>`;
  });
  return `<ul style="list-style:none;padding:0;">${rows.join('')}</ul>`;
}

/**
 * Render the inverted-mode "which existing request to replace?" list.
 *
 * @param {ExistingRequest[]} requests
 * @returns {string}
 */
export function renderRequestsList(requests) {
  if (!requests.length) {
    return '<p style="color:#888;">No active requests exist in this release group.</p>';
  }
  const rows = requests.map((r) => `
    <li>
      <button class="btn" data-rid="${r.id}">
        <strong>#${r.id}</strong> · ${esc(r.artist_name)} — ${esc(r.album_title)}<br>
        <small style="color:#888;">${esc(r.mb_release_id)} (status=${esc(r.status)})</small>
      </button>
    </li>`);
  return `<ul style="list-style:none;padding:0;">${rows.join('')}</ul>`;
}

/**
 * Confirmation-dialog HTML. Reflects R23 — in-flight transfers orphan;
 * cleanup deferred to #278. Generic copy, not a service-computed
 * dry-run.
 *
 * @param {Object} args
 * @param {number} args.sourceRequestId
 * @param {string} args.targetMbid
 * @param {string} [args.targetLabel]
 * @returns {string}
 */
export function renderConfirmDialog(args) {
  const targetLabel = args.targetLabel || args.targetMbid;
  return `
    <div class="confirm-box" role="dialog" aria-modal="true">
      <h3>Replace request #${args.sourceRequestId}?</h3>
      <p>The current request will be marked <code>replaced</code> (frozen for audit).
      The library entry (if imported), wrong-matches folders, and staging folders
      for this request will be deleted.</p>
      <p>A new request will be created targeting:<br>
        <strong>${esc(targetLabel)}</strong><br>
        <code>${esc(args.targetMbid)}</code>
      </p>
      <p style="font-size:0.85em;color:#999;">In-flight Soulseek transfers for the old
      request are left running; their landed files become orphans cleaned up by
      future convergence work (issue #278).</p>
      <div class="actions">
        <button class="btn" id="replace-picker-cancel">Cancel</button>
        <button class="btn p-btn delete-beets" id="replace-picker-confirm">Replace</button>
      </div>
    </div>
  `;
}

/**
 * Header copy for standard mode.
 *
 * @param {string} sourceLabel
 * @returns {string}
 */
export function renderStandardHeader(sourceLabel) {
  const safe = esc(sourceLabel);
  return `
    <h2 style="margin-top:0;">Switch ${safe} to a different pressing</h2>
    <p style="color:#888;">Pick the pressing you want instead. The current request
    will be marked <code>replaced</code> and a new request will be created.</p>
  `;
}

/**
 * Header copy for inverted mode.
 *
 * @param {string} targetLabel
 * @returns {string}
 */
export function renderInvertedHeader(targetLabel) {
  const safe = esc(targetLabel);
  return `
    <h2 style="margin-top:0;">Use this pressing to replace an existing request</h2>
    <p style="color:#888;">Pick which request in this release group should be
    replaced with <strong>${safe}</strong>.</p>
  `;
}

/* --------------------------------------------------------------------------
 * DOM glue — only callable in a browser context.
 * ------------------------------------------------------------------------ */

/**
 * Open the picker. Returns a Promise that resolves when the operator
 * cancels or the POST completes.
 *
 * @param {ReplacePickerOptions} options
 * @returns {Promise<ReplacePickerResult>}
 */
export async function openReplacePicker(options) {
  const isInverted = 'targetMbid' in options;
  const modal = /** @type {HTMLElement} */ (document.getElementById('replace-picker-modal'));
  if (!modal) {
    throw new Error('replace-picker-modal element missing from index.html');
  }

  return new Promise(async (resolve) => {
    /** @param {ReplacePickerResult} result */
    function close(result) {
      modal.style.display = 'none';
      modal.innerHTML = '';
      resolve(result);
    }

    function showOverlay(/** @type {string} */ inner) {
      modal.innerHTML = `<div class="confirm-overlay"><div class="confirm-box" style="max-width:640px;text-align:left;">${inner}</div></div>`;
      modal.style.display = '';
      const overlay = modal.querySelector('.confirm-overlay');
      if (overlay) {
        overlay.addEventListener('click', (event) => {
          if (event.target === overlay) {
            close({ outcome: 'cancelled' });
          }
        });
      }
    }

    if (isInverted) {
      await runInverted(/** @type {ReplacePickerOptionsInverted} */ (options), showOverlay, close);
    } else {
      await runStandard(/** @type {ReplacePickerOptionsStandard} */ (options), showOverlay, close);
    }
  });
}

/**
 * @param {ReplacePickerOptionsStandard} options
 * @param {(html: string) => void} showOverlay
 * @param {(r: ReplacePickerResult) => void} close
 */
async function runStandard(options, showOverlay, close) {
  showOverlay(`${renderStandardHeader(options.sourceLabel || `request #${options.sourceRequestId}`)}
    <p>Loading pressings…</p>`);

  let releases;
  let sourceMbid = '';
  try {
    const res = await fetch(`/api/release-group/${encodeURIComponent(options.releaseGroupId)}`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const body = await res.json();
    releases = body.releases || [];
    // Identify the current pressing for the source request (so we can
    // disable that row in the list).
    const detail = await fetch(`/api/pipeline/${options.sourceRequestId}`);
    if (detail.ok) {
      const drow = await detail.json();
      sourceMbid = drow.mb_release_id || '';
    }
  } catch (err) {
    showOverlay(`${renderStandardHeader(options.sourceLabel || '')}
      <p style="color:#f66;">Failed to load release group: ${esc(String(err))}</p>
      <div class="actions"><button class="btn" id="replace-picker-cancel">Close</button></div>`);
    bindCancel(close);
    return;
  }

  showOverlay(`${renderStandardHeader(options.sourceLabel || `request #${options.sourceRequestId}`)}
    ${renderPressingsList(releases, sourceMbid)}
    <div class="actions" style="margin-top:12px;">
      <button class="btn" id="replace-picker-cancel">Cancel</button>
    </div>`);

  bindCancel(close);
  const modal = document.getElementById('replace-picker-modal');
  if (!modal) return;
  modal.querySelectorAll('button[data-mbid]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const targetMbid = btn.getAttribute('data-mbid') || '';
      await runConfirm({
        sourceRequestId: options.sourceRequestId,
        targetMbid,
        targetLabel: btn.textContent || targetMbid,
      }, showOverlay, close);
    });
  });
}

/**
 * @param {ReplacePickerOptionsInverted} options
 * @param {(html: string) => void} showOverlay
 * @param {(r: ReplacePickerResult) => void} close
 */
async function runInverted(options, showOverlay, close) {
  showOverlay(`${renderInvertedHeader(options.targetLabel || options.targetMbid)}
    <p>Loading active requests…</p>`);

  let requests = [];
  try {
    const res = await fetch(
      `/api/pipeline/requests-by-rg/${encodeURIComponent(options.releaseGroupId)}`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const body = await res.json();
    requests = body.requests || [];
  } catch (err) {
    showOverlay(`${renderInvertedHeader(options.targetLabel || '')}
      <p style="color:#f66;">Failed to load active requests: ${esc(String(err))}</p>
      <div class="actions"><button class="btn" id="replace-picker-cancel">Close</button></div>`);
    bindCancel(close);
    return;
  }

  if (requests.length === 0) {
    showOverlay(`${renderInvertedHeader(options.targetLabel || '')}
      <p style="color:#888;">No active requests in this release group to replace.</p>
      <div class="actions"><button class="btn" id="replace-picker-cancel">Close</button></div>`);
    bindCancel(close);
    return;
  }

  if (requests.length === 1) {
    await runConfirm({
      sourceRequestId: requests[0].id,
      targetMbid: options.targetMbid,
      targetLabel: options.targetLabel || options.targetMbid,
    }, showOverlay, close);
    return;
  }

  showOverlay(`${renderInvertedHeader(options.targetLabel || options.targetMbid)}
    ${renderRequestsList(requests)}
    <div class="actions" style="margin-top:12px;">
      <button class="btn" id="replace-picker-cancel">Cancel</button>
    </div>`);
  bindCancel(close);
  const modal = document.getElementById('replace-picker-modal');
  if (!modal) return;
  modal.querySelectorAll('button[data-rid]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const rid = Number(btn.getAttribute('data-rid'));
      await runConfirm({
        sourceRequestId: rid,
        targetMbid: options.targetMbid,
        targetLabel: options.targetLabel || options.targetMbid,
      }, showOverlay, close);
    });
  });
}

/**
 * @param {{ sourceRequestId: number, targetMbid: string, targetLabel?: string }} args
 * @param {(html: string) => void} showOverlay
 * @param {(r: ReplacePickerResult) => void} close
 */
async function runConfirm(args, showOverlay, close) {
  showOverlay(renderConfirmDialog(args));
  bindCancel(close);
  const modal = document.getElementById('replace-picker-modal');
  if (!modal) return;
  const confirm = modal.querySelector('#replace-picker-confirm');
  if (!confirm) return;
  confirm.addEventListener('click', async () => {
    showOverlay(renderConfirmDialog(args) + `<p style="color:#888;text-align:center;">Replacing…</p>`);
    try {
      const res = await fetch(
        `/api/pipeline/${args.sourceRequestId}/replace`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ target_mb_release_id: args.targetMbid }),
        });
      const body = await res.json();
      close({
        outcome: 'confirmed',
        sourceRequestId: args.sourceRequestId,
        targetMbid: args.targetMbid,
        response: { status: res.status, body },
      });
    } catch (err) {
      showOverlay(renderConfirmDialog(args) + `<p style="color:#f66;">Request failed: ${esc(String(err))}</p>`);
      bindCancel(close);
    }
  });
}

/**
 * @param {(r: ReplacePickerResult) => void} close
 */
function bindCancel(close) {
  const modal = document.getElementById('replace-picker-modal');
  if (!modal) return;
  const cancel = modal.querySelector('#replace-picker-cancel');
  if (cancel) {
    cancel.addEventListener('click', () => close({ outcome: 'cancelled' }));
  }
}
