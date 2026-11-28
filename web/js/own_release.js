// @ts-check
/**
 * "Import as its own release": take a Wrong Matches candidate that is really
 * a sister pressing, import it against that pressing's release id, and only
 * then remove the copy you have.
 *
 * This is Replace with an adopted candidate and `displace_after_import`
 * (lib/mbid_replace_service.py): the request moves to the chosen release,
 * the candidate is force-imported there, and the importer removes the old
 * copy only once that import has landed. A failed import leaves the old
 * copy in place. The choices come from the read-only other-pressings check
 * (lib/other_pressing_check.py); any release id can also be pasted.
 */

import { API, toast } from './state.js';
import { esc } from './util.js';

/** The other-pressings check bounds its own mirror work to 30s. */
const CHECK_TIMEOUT_MS = 60000;

const MB_RELEASE_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DISCOGS_RELEASE_ID = /^[1-9][0-9]*$/;

/**
 * @typedef {{release_id: string, description: string, url: string | null, why: string}} OwnReleaseChoice
 */

/**
 * The button plus an empty panel, for one Wrong Matches candidate.
 * @param {unknown} downloadLogId
 * @param {unknown} requestId
 * @returns {string}
 */
export function renderOwnReleaseControl(downloadLogId, requestId) {
  const logId = Number(downloadLogId);
  const reqId = Number(requestId);
  if (!Number.isInteger(logId) || logId <= 0) return '';
  if (!Number.isInteger(reqId) || reqId <= 0) return '';
  return `<div class="own-release">`
    + `<button class="p-btn own-release-btn" title="Import this copy as the pressing it really is. Your current copy is removed only after the import succeeds." onclick="event.stopPropagation(); window.openOwnRelease(${logId}, ${reqId}, this)">Import as its own release</button>`
    + `<div class="own-release-panel" aria-live="polite"></div>`
    + `</div>`;
}

/**
 * A pasted or offered id is used only when it is an exact release id.
 * @param {unknown} value
 * @returns {string | null}
 */
export function normalizeReleaseId(value) {
  if (typeof value !== 'string') return null;
  const id = value.trim().toLowerCase();
  return MB_RELEASE_ID.test(id) || DISCOGS_RELEASE_ID.test(id) ? id : null;
}

/**
 * Pressings worth offering: the ones the files fit where they do not fit
 * the request, then the ones the files fit just as well. One row per id.
 * @param {any} payload
 * @returns {OwnReleaseChoice[]}
 */
export function ownReleaseChoices(payload) {
  /** @type {OwnReleaseChoice[]} */
  const choices = [];
  const seen = new Set();
  const groups = [
    [payload?.matching_pressings, 'The files fit this pressing'],
    [payload?.same_listing_pressings, 'Same track lengths as the request'],
  ];
  for (const [list, why] of groups) {
    if (!Array.isArray(list)) continue;
    for (const pressing of list) {
      const id = normalizeReleaseId(pressing?.release_id);
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const url = typeof pressing?.url === 'string'
        && /^https:\/\/musicbrainz\.org\/release\/[0-9a-f-]{36}$/.test(pressing.url)
        ? pressing.url
        : null;
      choices.push({
        release_id: id,
        description: String(pressing?.description || id),
        url,
        why: String(why),
      });
    }
  }
  return choices;
}

/**
 * The choice panel: what will happen, the pressings, a paste box, and one
 * inline confirmation.
 * @param {number} logId
 * @param {number} requestId
 * @param {OwnReleaseChoice[]} choices
 * @param {string[]} [sentences] - the other-pressings answer, shown as-is
 * @returns {string}
 */
export function renderOwnReleasePanel(logId, requestId, choices, sentences = []) {
  const name = `own-release-${logId}`;
  const intro = '<div class="own-release-intro">'
    + 'Import this copy as the release you pick. This request will follow that release from now on. '
    + 'Your current copy stays until the import succeeds, and is removed after that; if the import fails, your current copy stays. '
    + 'Any other copies waiting here for this album are deleted now.'
    + '</div>';
  const said = sentences
    .map((s) => `<div class="other-pressings-sentence">${esc(String(s))}</div>`)
    .join('');
  const rows = choices.map((choice, index) => {
    const link = choice.url
      ? ` <a href="${esc(choice.url)}" target="_blank" rel="noopener" onclick="event.stopPropagation();">MusicBrainz</a>`
      : '';
    return `<label class="own-release-choice" onclick="event.stopPropagation();">`
      + `<input type="radio" name="${name}" value="${esc(choice.release_id)}"${index === 0 ? ' checked' : ''}> `
      + `${esc(choice.description)} <span class="own-release-why">(${esc(choice.why)})</span>${link}`
      + `</label>`;
  }).join('');
  const none = choices.length === 0
    ? '<div class="own-release-none">No other pressing matched these files. Paste the release id if you know it.</div>'
    : '';
  return intro + said + rows + none
    + `<input type="text" class="own-release-input" placeholder="Or paste a MusicBrainz or Discogs release id" onclick="event.stopPropagation();">`
    + '<div class="own-release-actions">'
    + `<button class="p-btn own-release-confirm" style="border-color:#6a9;color:#6a9;" onclick="event.stopPropagation(); window.confirmOwnRelease(${logId}, ${requestId}, this)">Yes, import as this release</button>`
    + `<button class="p-btn own-release-cancel" onclick="event.stopPropagation(); window.cancelOwnRelease(this)">Cancel</button>`
    + '</div>'
    + '<div class="own-release-error" style="color:#f88;"></div>';
}

/**
 * The release id the operator chose: a pasted id wins over the radio.
 * @param {any} panel
 * @param {number} logId
 * @returns {{target: string | null, problem: string | null}}
 */
export function chosenReleaseId(panel, logId) {
  const typed = panel?.querySelector?.('.own-release-input')?.value;
  if (typeof typed === 'string' && typed.trim() !== '') {
    const id = normalizeReleaseId(typed);
    return id
      ? { target: id, problem: null }
      : { target: null, problem: 'That is not a MusicBrainz or Discogs release id.' };
  }
  const picked = panel?.querySelector?.(`input[name="own-release-${logId}"]:checked`)?.value;
  const id = normalizeReleaseId(picked);
  return id
    ? { target: id, problem: null }
    : { target: null, problem: 'Pick a release, or paste its id.' };
}

/**
 * @param {any} button
 * @returns {any}
 */
function panelFor(button) {
  return button?.parentElement?.querySelector?.('.own-release-panel')
    ?? button?.closest?.('.own-release-panel')
    ?? null;
}

/**
 * Ask which pressings these files fit, then offer them.
 * @param {number} logId
 * @param {number} requestId
 * @param {any} button
 * @returns {Promise<void>}
 */
export async function openOwnRelease(logId, requestId, button) {
  const panel = panelFor(button);
  if (!panel) return;
  button.disabled = true;
  panel.innerHTML = '<div class="other-pressings-loading">Checking which pressings these files fit…</div>';
  const controller = typeof AbortController === 'function' ? new AbortController() : null;
  const timer = controller ? setTimeout(() => controller.abort(), CHECK_TIMEOUT_MS) : null;
  /** @type {any} */
  let payload = null;
  try {
    const response = await fetch(
      `${API}/api/other-pressings/${Number(logId)}`,
      controller ? { signal: controller.signal } : undefined,
    );
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
  } catch {
    payload = null;
  } finally {
    if (timer) clearTimeout(timer);
  }
  const sentences = Array.isArray(payload?.sentences) ? payload.sentences : [];
  panel.innerHTML = renderOwnReleasePanel(
    Number(logId), Number(requestId), ownReleaseChoices(payload), sentences,
  );
  button.disabled = false;
}

/**
 * Close the panel without doing anything.
 * @param {any} button
 */
export function cancelOwnRelease(button) {
  const panel = panelFor(button);
  if (panel) panel.innerHTML = '';
}

/**
 * The confirmed action: Replace onto the chosen release, adopting this
 * candidate, keeping the current copy until the import lands.
 * @param {number} logId
 * @param {number} requestId
 * @param {any} button
 * @returns {Promise<void>}
 */
export async function confirmOwnRelease(logId, requestId, button) {
  const panel = panelFor(button);
  if (!panel) return;
  const errorLine = panel.querySelector?.('.own-release-error');
  const { target, problem } = chosenReleaseId(panel, Number(logId));
  if (!target) {
    if (errorLine) errorLine.textContent = problem || '';
    return;
  }
  button.disabled = true;
  button.textContent = 'Working…';
  /** @type {any} */
  let data = null;
  let ok = false;
  try {
    const response = await fetch(`${API}/api/pipeline/${Number(requestId)}/replace`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        target_mb_release_id: target,
        adopt_download_log_id: Number(logId),
        displace_after_import: true,
      }),
    });
    ok = response.ok;
    try {
      data = await response.json();
    } catch {
      data = null;
    }
  } catch {
    data = null;
  }
  if (ok && data?.outcome === 'replaced') {
    const job = data.adopt_import_job_id;
    const kept = data.displace_album_id != null
      ? ' Your current copy stays until it imports, then it is removed.'
      : '';
    const warnings = Array.isArray(data.warnings) ? data.warnings : [];
    panel.innerHTML = `<div class="own-release-done">`
      + (job != null
        ? `Import job ${esc(String(job))} queued for this copy as its own release.${esc(kept)}`
        : 'The request moved, but the import was not queued. See the notes below.')
      + `</div>`
      + warnings.map((/** @type {unknown} */ w) => `<div class="own-release-warning" style="color:#d9a441;">${esc(String(w))}</div>`).join('');
    toast(job != null ? 'Queued import as its own release' : 'Request moved; import not queued', job == null);
    return;
  }
  button.disabled = false;
  button.textContent = 'Yes, import as this release';
  const message = data?.error_message || data?.error || 'That did not work. Nothing was changed.';
  if (errorLine) errorLine.textContent = String(message);
  toast(String(message), true);
}
