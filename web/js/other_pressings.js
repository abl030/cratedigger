// @ts-check
/**
 * "Check other pressings" (issue #1445): an on-demand, read-only check of
 * one download_log row against the other releases in its MusicBrainz
 * release group. Every sentence is server-owned
 * (lib/other_pressing_check.py); this module only fetches, shows a loading
 * line, and lays the sentences and links out.
 */

import { API } from './state.js';
import { esc } from './util.js';

/** The server bounds its own mirror work to 30s; this covers the reply. */
const CHECK_TIMEOUT_MS = 60000;

const FAILED_SENTENCE = 'The check could not be run right now. Try again later.';

/**
 * The button plus an empty result box, for one download_log row.
 * @param {unknown} downloadLogId
 * @returns {string}
 */
export function renderOtherPressingsControl(downloadLogId) {
  const id = Number(downloadLogId);
  if (!Number.isInteger(id) || id <= 0) return '';
  return `<div class="other-pressings">`
    + `<button class="p-btn other-pressings-btn" onclick="event.stopPropagation(); window.checkOtherPressings(${id}, this)">Check other pressings</button>`
    + `<div class="other-pressings-result" aria-live="polite"></div>`
    + `</div>`;
}

/**
 * A link is shown only for an https MusicBrainz release URL.
 * @param {unknown} url
 * @returns {string | null}
 */
function releaseUrl(url) {
  if (typeof url !== 'string') return null;
  return /^https:\/\/musicbrainz\.org\/release\/[0-9a-f-]{36}$/.test(url) ? url : null;
}

/**
 * The sentences, then one link per matching pressing and one to the
 * release itself.
 * @param {any} payload
 * @returns {string}
 */
export function renderOtherPressingsResult(payload) {
  const sentences = Array.isArray(payload?.sentences) ? payload.sentences : [];
  if (sentences.length === 0) {
    return `<div class="other-pressings-sentence">${esc(FAILED_SENTENCE)}</div>`;
  }
  const lines = sentences
    .map((/** @type {unknown} */ s) => `<div class="other-pressings-sentence">${esc(String(s))}</div>`)
    .join('');
  /** @type {string[]} */
  const links = [];
  const matches = Array.isArray(payload?.matching_pressings) ? payload.matching_pressings : [];
  for (const match of matches) {
    const url = releaseUrl(match?.url);
    if (url) {
      links.push(`<a href="${esc(url)}" target="_blank" rel="noopener">${esc(String(match.description || 'Matching pressing'))}</a>`);
    }
  }
  const target = releaseUrl(payload?.target_url);
  if (target) {
    links.push(`<a href="${esc(target)}" target="_blank" rel="noopener">This release</a>`);
  }
  const linkLine = links.length
    ? `<div class="other-pressings-links">On MusicBrainz: ${links.join(' · ')}</div>`
    : '';
  return lines + linkLine;
}

/**
 * Run the check for one row and show the answer under the button.
 * @param {number} downloadLogId
 * @param {any} button
 * @returns {Promise<void>}
 */
export async function checkOtherPressings(downloadLogId, button) {
  const box = button?.parentElement?.querySelector('.other-pressings-result');
  if (!box) return;
  button.disabled = true;
  box.innerHTML = '<div class="other-pressings-loading">Checking MusicBrainz…</div>';
  const controller = typeof AbortController === 'function' ? new AbortController() : null;
  const timer = controller ? setTimeout(() => controller.abort(), CHECK_TIMEOUT_MS) : null;
  try {
    const response = await fetch(
      `${API}/api/other-pressings/${Number(downloadLogId)}`,
      controller ? { signal: controller.signal } : undefined,
    );
    /** @type {any} */
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    box.innerHTML = renderOtherPressingsResult(payload);
  } catch {
    box.innerHTML = renderOtherPressingsResult(null);
  } finally {
    if (timer) clearTimeout(timer);
    button.disabled = false;
    button.textContent = 'Check again';
  }
}
