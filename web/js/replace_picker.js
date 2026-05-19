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
 * @property {string|null} [releaseGroupId]  // null → lazy-resolve via resolve-rg
 * @property {string} [sourceLabel]  // "Pet Grief — Old Pressing"
 * @property {string} [source]       // calling-tab identifier for telemetry
 */

/**
 * @typedef {Object} ReplacePickerOptionsInverted
 * @property {string} targetMbid
 * @property {string|null} [releaseGroupId]  // null → lazy-resolve via /api/release/<mbid>
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
 * Build the meta line for a release row — matches the format used by
 * analysis.js pressing rows: `country · year · format · Nt`. Empty
 * fields drop out.
 *
 * @param {{ date?: string, country?: string, format?: string, track_count?: number }} r
 * @returns {string}
 */
export function pressingMeta(r) {
  const year = r.date ? r.date.slice(0, 4) : '';
  return [r.country, year, r.format, r.track_count ? `${r.track_count}t` : '']
    .filter((x) => !!x)
    .join(' · ');
}

/**
 * Render the "pressings in this release group" list (standard mode).
 *
 * Each row is click-to-expand. The MB tracklist is lazy-fetched on the
 * first expand; the "Use this pressing" button lives inside the expanded
 * detail panel so the row click never accidentally fires the destructive
 * confirm flow.
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
    const disabledAttr = isCurrent ? ' disabled' : '';
    const labelSuffix = isCurrent ? ' (current pressing)' : '';
    const meta = pressingMeta(r);
    const pickLabel = isCurrent
      ? ''
      : `<button class="replace-picker-confirm" data-mbid="${esc(r.id)}">Use this pressing</button>`;
    return `<li class="replace-picker-row" data-mbid-row="${esc(r.id)}">
      <button class="replace-picker-pick"${disabledAttr} data-expand-mbid="${esc(r.id)}" aria-expanded="false">
        <strong>${esc(r.title)}</strong>${labelSuffix}<br>
        <small>${esc(meta)}</small>
      </button>
      <div class="replace-picker-detail" data-tracks-for="${esc(r.id)}"></div>
      <div class="replace-picker-detail-actions-slot" data-actions-for="${esc(r.id)}" hidden>
        <div class="replace-picker-detail-actions">${pickLabel}</div>
      </div>
    </li>`;
  });
  return `<ul class="replace-picker-list">${rows.join('')}</ul>`;
}

/**
 * Format seconds → "m:ss". Returns empty string for null/undefined/NaN.
 *
 * @param {number|null|undefined} secs
 * @returns {string}
 */
export function formatLength(secs) {
  if (secs === null || secs === undefined || Number.isNaN(secs)) return '';
  const total = Math.round(Number(secs));
  if (!Number.isFinite(total) || total < 0) return '';
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}:${s.toString().padStart(2, '0')}`;
}

/**
 * @typedef {Object} TracklistTrack
 * @property {number} [disc_number]
 * @property {number} [track_number]
 * @property {string} [title]
 * @property {number|null} [length_seconds]
 */

/**
 * Render a compact tracklist. Disc headers only appear when there is
 * more than one disc — keeps the visual quiet for the common single-disc
 * case.
 *
 * @param {TracklistTrack[]} tracks
 * @returns {string}
 */
export function renderTracklist(tracks) {
  if (!tracks || !tracks.length) {
    return '<p style="color:#888;margin:8px 0;">No tracks listed.</p>';
  }
  const discs = new Set(tracks.map((t) => t.disc_number || 1));
  const multiDisc = discs.size > 1;
  const byDisc = new Map();
  for (const t of tracks) {
    const d = t.disc_number || 1;
    if (!byDisc.has(d)) byDisc.set(d, []);
    byDisc.get(d).push(t);
  }
  const parts = [];
  const sortedDiscs = [...byDisc.keys()].sort((a, b) => a - b);
  for (const d of sortedDiscs) {
    if (multiDisc) parts.push(`<div class="replace-picker-disc">Disc ${d}</div>`);
    const items = byDisc.get(d).map((t) => {
      const num = t.track_number || '';
      const dur = formatLength(t.length_seconds);
      const durHtml = dur ? `<span class="replace-picker-dur">${esc(dur)}</span>` : '';
      return `<li><span class="replace-picker-tnum">${esc(String(num))}.</span> ${esc(t.title || '')} ${durHtml}</li>`;
    });
    parts.push(`<ol class="replace-picker-tracks">${items.join('')}</ol>`);
  }
  return parts.join('');
}

/**
 * Render the pinned "current request" tracklist panel — the source
 * request in standard mode, the target MBID in inverted mode. Shown
 * once at the top of the picker so the operator always has the
 * reference visible while expanding candidate pressings below. The
 * summary line carries the same meta tags (country · year · format ·
 * Nt) as the rows below so visual comparison is direct.
 *
 * @param {{
 *   label: string,
 *   meta?: string,
 *   tracks: TracklistTrack[]|null,
 *   loading?: boolean,
 *   error?: string,
 * }} args
 * @returns {string}
 */
export function renderSourcePanel(args) {
  let body;
  if (args.error) {
    body = `<p style="color:#f66;margin:4px 0;">${esc(args.error)}</p>`;
  } else if (args.loading || args.tracks === null) {
    body = '<p style="color:#888;margin:4px 0;">Loading tracklist…</p>';
  } else {
    body = renderTracklist(args.tracks);
  }
  const metaHtml = args.meta
    ? `<br><small style="color:#888;">${esc(args.meta)}</small>`
    : '';
  return `<details class="replace-picker-source" open>
    <summary><strong>Current request:</strong> ${esc(args.label)}${metaHtml}</summary>
    <div class="replace-picker-source-body">${body}</div>
  </details>`;
}

/**
 * Render the inverted-mode "which existing request to replace?" list.
 *
 * Same click-to-expand pattern as the standard-mode pressings list: the
 * row is the disclosure, the "Use this request" button lives inside the
 * expanded detail panel.
 *
 * @param {ExistingRequest[]} requests
 * @returns {string}
 */
export function renderRequestsList(requests) {
  if (!requests.length) {
    return '<p style="color:#888;">No active requests exist in this release group.</p>';
  }
  const rows = requests.map((r) => `
    <li class="replace-picker-row" data-mbid-row="${esc(r.mb_release_id)}">
      <button class="replace-picker-pick" data-expand-mbid="${esc(r.mb_release_id)}" aria-expanded="false">
        <strong>#${r.id}</strong> · ${esc(r.artist_name)} — ${esc(r.album_title)}<br>
        <small>status: ${esc(r.status)}</small>
      </button>
      <div class="replace-picker-detail" data-tracks-for="${esc(r.mb_release_id)}"></div>
      <div class="replace-picker-detail-actions-slot" data-actions-for="${esc(r.mb_release_id)}" hidden>
        <div class="replace-picker-detail-actions">
          <button class="replace-picker-confirm" data-rid="${r.id}">Use this request</button>
        </div>
      </div>
    </li>`);
  return `<ul class="replace-picker-list">${rows.join('')}</ul>`;
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
      modal.innerHTML = `<div class="confirm-overlay"><div class="replace-picker-shell">${inner}</div></div>`;
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
  // Lazy-resolve release group id for legacy null-RG rows. The endpoint
  // both returns the RG and persists it back to the album_requests row
  // so the next click is fast.
  let releaseGroupId = options.releaseGroupId || null;
  if (!releaseGroupId) {
    showOverlay(`${renderStandardHeader(options.sourceLabel || `request #${options.sourceRequestId}`)}
      <p>Resolving release group…</p>`);
    try {
      const res = await fetch(
        `/api/pipeline/${options.sourceRequestId}/resolve-rg`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      const body = await res.json();
      if (!res.ok || !body.mb_release_group_id) {
        const msg = body.error || `HTTP ${res.status}`;
        showOverlay(`${renderStandardHeader(options.sourceLabel || '')}
          <p style="color:#f66;">Failed to resolve release group: ${esc(String(msg))}</p>
          <div class="actions"><button class="btn" id="replace-picker-cancel">Close</button></div>`);
        bindCancel(close);
        return;
      }
      releaseGroupId = body.mb_release_group_id;
    } catch (err) {
      showOverlay(`${renderStandardHeader(options.sourceLabel || '')}
        <p style="color:#f66;">Failed to resolve release group: ${esc(String(err))}</p>
        <div class="actions"><button class="btn" id="replace-picker-cancel">Close</button></div>`);
      bindCancel(close);
      return;
    }
  }

  showOverlay(`${renderStandardHeader(options.sourceLabel || `request #${options.sourceRequestId}`)}
    <p>Loading pressings…</p>`);

  let releases;
  let sourceMbid = '';
  try {
    const res = await fetch(`/api/release-group/${encodeURIComponent(releaseGroupId)}`);
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
      // The pipeline detail endpoint wraps the row under `request` —
      // a flat read returned `undefined`, leaving sourceMbid blank
      // (which the picker silently tolerated because it only drove the
      // "disable current pressing" visual). Fixed when we started
      // depending on it for the reference tracklist.
      sourceMbid = (drow.request && drow.request.mb_release_id) || drow.mb_release_id || '';
    }
  } catch (err) {
    showOverlay(`${renderStandardHeader(options.sourceLabel || '')}
      <p style="color:#f66;">Failed to load release group: ${esc(String(err))}</p>
      <div class="actions"><button class="btn" id="replace-picker-cancel">Close</button></div>`);
    bindCancel(close);
    return;
  }

  const sourceLabel = options.sourceLabel || `request #${options.sourceRequestId}`;
  const sourcePressing = releases.find((r) => r.id === sourceMbid) || null;
  const sourceMeta = sourcePressing ? pressingMeta(sourcePressing) : '';
  const sourcePanel = renderSourcePanel({
    label: sourceLabel,
    meta: sourceMeta,
    tracks: null,
    loading: true,
  });
  showOverlay(`${renderStandardHeader(sourceLabel)}
    ${sourcePanel}
    ${renderPressingsList(releases, sourceMbid)}
    <div class="replace-picker-cancel-bar">
      <button class="btn" id="replace-picker-cancel">Cancel</button>
    </div>`);

  bindCancel(close);
  const modal = document.getElementById('replace-picker-modal');
  if (!modal) return;
  wireRows(modal, async (targetMbid, rowLabel) => {
    await runConfirm({
      sourceRequestId: options.sourceRequestId,
      targetMbid,
      targetLabel: rowLabel,
    }, showOverlay, close);
  });
  // Lazy-fill the "current request" tracklist. Failure here is non-fatal —
  // the picker still works without the reference panel.
  loadSourceTracklist(modal, sourceMbid, sourceLabel).catch(() => {});
  // Fan out beets-distance lookups against the source request's
  // wrong-matches folders. Decorates each pressing row's meta line as
  // results arrive. Silent no-op when the request has no wrong-matches
  // entries — the picker looks identical to the non-distance UI.
  loadDistances(modal, options.sourceRequestId, releases).catch((err) => {
    console.warn('replace-picker distance overlay failed:', err);
  });
}

/**
 * Module-scoped cache for fetched MB releases — keyed by MBID. Drops on
 * picker close because the modal HTML is wiped, not because the cache
 * itself clears, so re-opening the picker against the same RG will
 * re-fetch. That's fine: /api/release/<mbid> is Redis-cached server-side
 * (24h TTL), so the second open is still effectively instant.
 *
 * @type {Map<string, Promise<{ tracks: TracklistTrack[] }>>}
 */
const tracklistCache = new Map();

/* --------------------------------------------------------------------------
 * Beets-distance overlay — pure helpers
 *
 * The picker decorates each pressing row with the best beets-distance against
 * the source request's wrong-matches folders. Pure helpers below are tested
 * via tests/test_js_util.mjs; DOM glue further down lives in `loadDistances`.
 * ------------------------------------------------------------------------ */

/**
 * @typedef {Object} BeetsDistanceResult
 * @property {string} outcome
 * @property {number|null} [distance]
 * @property {number|null} [matched_tracks]
 * @property {number|null} [total_local_tracks]
 * @property {number|null} [total_mb_tracks]
 * @property {string|null} [folder_path]
 * @property {number|null} [download_log_id]
 */

/**
 * Pick the lowest-distance "ok" result from a list. Returns null if none
 * of the inputs scored — keeps the caller branch-free (no distance UI
 * if every download errored, was wrong-RG-guarded, etc.).
 *
 * @param {BeetsDistanceResult[]} results
 * @returns {BeetsDistanceResult|null}
 */
export function pickBestDistance(results) {
  let best = null;
  for (const r of results) {
    if (!r || r.outcome !== 'ok' || typeof r.distance !== 'number') continue;
    if (best === null || r.distance < /** @type {number} */ (best.distance)) {
      best = r;
    }
  }
  return best;
}

/**
 * Format a distance badge for the pressing-row meta line: `best 0.07 (12/12)`.
 *
 * Distance only — no folder path, that's a different surface (the
 * expanded-row breakdown will carry the per-download details). Returns
 * an empty string for null so callers can concatenate unconditionally.
 *
 * @param {BeetsDistanceResult|null} best
 * @returns {string}
 */
export function formatDistanceBadge(best) {
  if (best === null) return '';
  if (typeof best.distance !== 'number') return '';
  const matched = best.matched_tracks;
  const total = best.total_mb_tracks;
  const ratio = (typeof matched === 'number' && typeof total === 'number')
    ? ` (${matched}/${total})`
    : '';
  return `best ${best.distance.toFixed(2)}${ratio}`;
}

/**
 * Run an async worker over a list of inputs with a concurrency cap.
 *
 * Used to fan out N×M `/api/beets-distance` calls without blowing past
 * the browser's per-origin connection limit. Resolves to the worker
 * results in input order (failed workers resolve to whatever the
 * worker returns on error — typically a struct with `outcome: 'error'`).
 *
 * @template T, R
 * @param {T[]} items
 * @param {number} limit
 * @param {(item: T, index: number) => Promise<R>} worker
 * @returns {Promise<R[]>}
 */
export async function runWithConcurrency(items, limit, worker) {
  /** @type {R[]} */
  const results = new Array(items.length);
  let cursor = 0;
  const lanes = Math.max(1, Math.min(limit, items.length));
  async function lane() {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      results[i] = await worker(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: lanes }, () => lane()));
  return results;
}

/**
 * @param {string} mbid
 * @returns {Promise<{ tracks: TracklistTrack[] }>}
 */
function fetchTracklist(mbid) {
  const cached = tracklistCache.get(mbid);
  if (cached) return cached;
  const p = fetch(`/api/release/${encodeURIComponent(mbid)}`)
    .then(async (res) => {
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = await res.json();
      return { tracks: body.tracks || [] };
    });
  tracklistCache.set(mbid, p);
  // If the fetch rejects, drop the cache entry so retries are possible.
  p.catch(() => tracklistCache.delete(mbid));
  return p;
}

/**
 * Wire row-click expansion + confirm-button click for every picker row
 * inside `root`.
 *
 * Row click toggles the tracklist + action panel. The first expansion
 * lazy-fetches `/api/release/<mbid>` and renders the tracklist; later
 * expansions just toggle visibility. The pick button (rendered inside
 * the expanded panel) calls `onPick` with the row's MBID, its label,
 * and the optional `data-rid` (inverted-mode existing-request id).
 *
 * @param {HTMLElement|Document} root
 * @param {(mbid: string, label: string, rid: string | null) => void | Promise<void>} onPick
 */
function wireRows(root, onPick) {
  root.querySelectorAll('.replace-picker-row').forEach((row) => {
    const pickBtn = /** @type {HTMLButtonElement|null} */ (
      row.querySelector('button.replace-picker-pick[data-expand-mbid]')
    );
    if (!pickBtn || pickBtn.disabled) return;
    const mbid = pickBtn.getAttribute('data-expand-mbid') || '';
    const panel = /** @type {HTMLElement|null} */ (
      row.querySelector(`.replace-picker-detail[data-tracks-for="${cssEscape(mbid)}"]`)
    );
    const actionsSlot = /** @type {HTMLElement|null} */ (
      row.querySelector(`.replace-picker-detail-actions-slot[data-actions-for="${cssEscape(mbid)}"]`)
    );
    if (!panel) return;

    pickBtn.addEventListener('click', async (event) => {
      event.stopPropagation();
      const expanded = pickBtn.getAttribute('aria-expanded') === 'true';
      if (expanded) {
        row.classList.remove('open');
        pickBtn.setAttribute('aria-expanded', 'false');
        if (actionsSlot) actionsSlot.hidden = true;
        return;
      }
      row.classList.add('open');
      pickBtn.setAttribute('aria-expanded', 'true');
      if (actionsSlot) actionsSlot.hidden = false;
      if (!panel.dataset.loaded) {
        panel.innerHTML = '<p style="color:#888;margin:4px 0;">Loading tracklist…</p>';
        try {
          const { tracks } = await fetchTracklist(mbid);
          panel.innerHTML = renderTracklist(tracks);
          panel.dataset.loaded = '1';
        } catch (err) {
          panel.innerHTML = `<p style="color:#f66;margin:4px 0;">Failed to load: ${esc(String(err))}</p>`;
        }
      }
    });

    const confirmBtn = /** @type {HTMLButtonElement|null} */ (
      row.querySelector('button.replace-picker-confirm')
    );
    if (!confirmBtn) return;
    confirmBtn.addEventListener('click', async (event) => {
      event.stopPropagation();
      const label = (pickBtn.textContent || '').trim().split('\n')[0] || mbid;
      const rid = confirmBtn.getAttribute('data-rid');
      await onPick(mbid, label, rid);
    });
  });
}

/**
 * Minimal CSS.escape replacement — picker MBIDs are UUIDs so the only
 * character classes we need to handle are `[0-9a-f-]`, but we guard
 * defensively anyway.
 *
 * @param {string} s
 * @returns {string}
 */
function cssEscape(s) {
  if (typeof CSS !== 'undefined' && CSS && typeof CSS.escape === 'function') {
    return CSS.escape(s);
  }
  return s.replace(/[^a-zA-Z0-9_-]/g, (c) => `\\${c}`);
}

/**
 * Maximum concurrent ``/api/beets-distance`` requests. Cap at six,
 * which matches the browser's typical per-origin keep-alive limit —
 * past that, requests just queue behind the limit anyway and we'd
 * waste server-side parallelism waiting for the next free socket.
 */
const DISTANCE_CONCURRENCY = 6;

/**
 * Fetch the source request's wrong-matches folders, compute beets-distance
 * against every candidate pressing, and decorate the picker rows with the
 * best result per pressing as each pressing's column resolves.
 *
 * Silent no-op when:
 *   - the request has zero wrong-matches entries on disk, or
 *   - every distance call fails (no signal to show).
 *
 * Failure mode for the operator: the distance badge just doesn't appear.
 * The picker remains fully usable; the badge is additive, not load-bearing.
 *
 * @param {HTMLElement} modal
 * @param {number} sourceRequestId
 * @param {ReleaseGroupSibling[]} releases
 */
async function loadDistances(modal, sourceRequestId, releases) {
  // 1. Find this request's wrong-matches entries → their download_log_ids.
  let downloadLogIds;
  try {
    const res = await fetch('/api/wrong-matches');
    if (!res.ok) return;
    const body = await res.json();
    const groups = body.groups || [];
    const ourGroup = groups.find((g) => g.request_id === sourceRequestId);
    if (!ourGroup) return;
    const entries = ourGroup.entries || [];
    downloadLogIds = entries
      .map((e) => e.download_log_id)
      .filter((id) => typeof id === 'number');
  } catch (err) {
    console.warn('replace-picker: wrong-matches fetch failed:', err);
    return;
  }
  if (downloadLogIds.length === 0) return;

  // 2. Build the (pressing, download_log_id) work list. Disabled rows
  //    (the current pressing) are skipped — there is no point telling
  //    the operator how well their existing downloads match the
  //    pressing they're trying to switch *away* from.
  /** @type {{mbid: string, logId: number}[]} */
  const work = [];
  for (const r of releases) {
    const row = modal.querySelector(`.replace-picker-row[data-mbid-row="${cssEscape(r.id)}"]`);
    const pickBtn = row && row.querySelector('button.replace-picker-pick');
    if (pickBtn && /** @type {HTMLButtonElement} */ (pickBtn).disabled) continue;
    for (const logId of downloadLogIds) {
      work.push({ mbid: r.id, logId });
    }
  }
  if (work.length === 0) return;

  // 3. Per-pressing accumulator: as results stream in we recompute the
  //    best score for that MBID and rewrite its row's badge. This keeps
  //    the UI feeling fast even when the last download is still
  //    crunching — early best-distances appear immediately.
  /** @type {Map<string, BeetsDistanceResult[]>} */
  const perPressing = new Map();
  for (const r of releases) perPressing.set(r.id, []);

  await runWithConcurrency(work, DISTANCE_CONCURRENCY, async ({ mbid, logId }) => {
    /** @type {BeetsDistanceResult} */
    let result;
    try {
      const res = await fetch(`/api/beets-distance/${logId}/${encodeURIComponent(mbid)}`);
      result = /** @type {BeetsDistanceResult} */ (await res.json());
    } catch (err) {
      result = { outcome: 'fetch_failed' };
    }
    const bucket = perPressing.get(mbid);
    if (bucket) {
      bucket.push(result);
      paintDistanceBadge(modal, mbid, pickBestDistance(bucket));
    }
    return result;
  });
}

/**
 * Rewrite the distance-badge span for one pressing row.
 *
 * The badge sits inside the row's `<small>` meta line as
 * ``<span class="replace-picker-distance">…</span>``. We create the
 * span on first paint and update its text on subsequent paints —
 * cheaper than rebuilding the row's innerHTML each time a result
 * lands.
 *
 * @param {HTMLElement} modal
 * @param {string} mbid
 * @param {BeetsDistanceResult|null} best
 */
function paintDistanceBadge(modal, mbid, best) {
  const row = modal.querySelector(
    `.replace-picker-row[data-mbid-row="${cssEscape(mbid)}"]`);
  if (!row) return;
  const meta = row.querySelector('button.replace-picker-pick small');
  if (!meta) return;
  let badge = /** @type {HTMLSpanElement|null} */ (
    meta.querySelector('.replace-picker-distance'));
  const text = formatDistanceBadge(best);
  if (!text) {
    if (badge) badge.remove();
    return;
  }
  if (!badge) {
    badge = document.createElement('span');
    badge.className = 'replace-picker-distance';
    meta.appendChild(document.createTextNode(' · '));
    meta.appendChild(badge);
  }
  badge.textContent = text;
}

/**
 * @param {HTMLElement} modal
 * @param {string} sourceMbid
 * @param {string} sourceLabel
 */
async function loadSourceTracklist(modal, sourceMbid, sourceLabel) {
  const body = modal.querySelector('.replace-picker-source-body');
  if (!body) return;
  if (!sourceMbid) {
    body.innerHTML = '<p style="color:#888;margin:4px 0;">No reference MBID for this request.</p>';
    return;
  }
  try {
    const { tracks } = await fetchTracklist(sourceMbid);
    body.innerHTML = renderTracklist(tracks);
  } catch (err) {
    body.innerHTML = `<p style="color:#f66;margin:4px 0;">Failed to load reference tracklist: ${esc(String(err))}</p>`;
  }
}

/**
 * @param {ReplacePickerOptionsInverted} options
 * @param {(html: string) => void} showOverlay
 * @param {(r: ReplacePickerResult) => void} close
 */
async function runInverted(options, showOverlay, close) {
  // Lazy-resolve release group id for legacy null-RG rows by hitting
  // the existing /api/release/<mbid> route (the response carries
  // ``release_group_id``). We don't persist the result anywhere — the
  // active-requests fetch below is the only consumer.
  let releaseGroupId = options.releaseGroupId || null;
  if (!releaseGroupId) {
    showOverlay(`${renderInvertedHeader(options.targetLabel || options.targetMbid)}
      <p>Resolving release group…</p>`);
    try {
      const res = await fetch(`/api/release/${encodeURIComponent(options.targetMbid)}`);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const body = await res.json();
      const rg = body.release_group_id;
      if (!rg) {
        showOverlay(`${renderInvertedHeader(options.targetLabel || '')}
          <p style="color:#f66;">Target MBID has no release group on the MB mirror.</p>
          <div class="actions"><button class="btn" id="replace-picker-cancel">Close</button></div>`);
        bindCancel(close);
        return;
      }
      releaseGroupId = rg;
    } catch (err) {
      showOverlay(`${renderInvertedHeader(options.targetLabel || '')}
        <p style="color:#f66;">Failed to resolve release group: ${esc(String(err))}</p>
        <div class="actions"><button class="btn" id="replace-picker-cancel">Close</button></div>`);
      bindCancel(close);
      return;
    }
  }

  showOverlay(`${renderInvertedHeader(options.targetLabel || options.targetMbid)}
    <p>Loading active requests…</p>`);

  let requests = [];
  try {
    const res = await fetch(
      `/api/pipeline/requests-by-rg/${encodeURIComponent(releaseGroupId)}`);
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

  const targetLabel = options.targetLabel || options.targetMbid;
  const sourcePanel = renderSourcePanel({
    label: targetLabel,
    meta: '',
    tracks: null,
    loading: true,
  });
  showOverlay(`${renderInvertedHeader(targetLabel)}
    ${sourcePanel}
    ${renderRequestsList(requests)}
    <div class="replace-picker-cancel-bar">
      <button class="btn" id="replace-picker-cancel">Cancel</button>
    </div>`);
  bindCancel(close);
  const modal = document.getElementById('replace-picker-modal');
  if (!modal) return;
  wireRows(modal, async (_rowMbid, _rowLabel, ridAttr) => {
    const rid = Number(ridAttr);
    if (!Number.isFinite(rid)) return;
    await runConfirm({
      sourceRequestId: rid,
      targetMbid: options.targetMbid,
      targetLabel: options.targetLabel || options.targetMbid,
    }, showOverlay, close);
  });
  loadSourceTracklist(modal, options.targetMbid, targetLabel).catch(() => {});
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
    // Re-bind cancel: showOverlay just rewrote the modal body, so the
    // prior bindCancel handler is gone with its target node. Without
    // this re-bind, the Cancel button is dead during the in-flight
    // request and the operator can't bail out of a stuck POST.
    bindCancel(close);
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
