// @ts-check

/**
 * External-authorization session guard (issue #924).
 *
 * When an operator fronts Cratedigger with their own identity provider, an
 * expired browser session is answered by that component rather than by this
 * application. `fetch` follows the portal redirect transparently, so without
 * this guard every call site sees a successful response carrying HTML, fails
 * to parse it, and renders its own generic "failed to load" text.
 *
 * The guard is installed once over `window.fetch` rather than at each call
 * site: `web/js/` has ~40 direct `fetch()` calls and no shared client, and a
 * per-call-site fix would not hold for call sites added later.
 */

import { isExternalAuthInterruption } from './util.js';

const OVERLAY_ID = 'session-expired-overlay';

/**
 * Build the non-dismissible expired-session overlay.
 *
 * Reloading is the correct recovery: a document-level navigation lets the
 * external component run its own redirect flow, which an in-page fetch
 * cannot do.
 *
 * @param {Document} doc
 * @returns {HTMLElement}
 */
export function buildSessionExpiredOverlay(doc) {
  const overlay = doc.createElement('div');
  overlay.id = OVERLAY_ID;
  overlay.className = 'session-expired-overlay';
  overlay.setAttribute('role', 'alertdialog');
  overlay.setAttribute('aria-modal', 'true');
  overlay.setAttribute('aria-labelledby', 'session-expired-title');

  const panel = doc.createElement('div');
  panel.className = 'session-expired-panel';

  const heading = doc.createElement('h2');
  heading.id = 'session-expired-title';
  heading.textContent = 'Your session expired';

  const body = doc.createElement('p');
  body.textContent =
    'Sign-in is handled by a component in front of Cratedigger, and this '
    + 'session is no longer authorized. Reload to sign in again.';

  const reload = doc.createElement('button');
  reload.type = 'button';
  reload.textContent = 'Reload';
  reload.addEventListener('click', () => doc.location.reload());

  panel.append(heading, body, reload);
  overlay.append(panel);
  return overlay;
}

/**
 * Show the overlay exactly once per page life.
 *
 * @param {Document} doc
 * @returns {boolean} true when this call created the overlay
 */
function showSessionExpired(doc) {
  if (doc.getElementById(OVERLAY_ID)) return false;
  doc.body.appendChild(buildSessionExpiredOverlay(doc));
  return true;
}

/**
 * Wrap one fetch implementation so an external authorizer's answer surfaces
 * as an expired session instead of reaching a call site as data.
 *
 * @param {typeof fetch} original
 * @param {Document} doc
 * @returns {typeof fetch}
 */
export function wrapFetchWithSessionGuard(original, doc) {
  return async function guardedFetch(...args) {
    const response = await original(...args);
    if (isExternalAuthInterruption(response)) {
      showSessionExpired(doc);
      throw new Error('external authorization session expired');
    }
    return response;
  };
}

let installed = false;

/**
 * Install the guard over the page's own fetch.
 *
 * Idempotent, and a no-op outside a browser: `main.js` installs at module
 * scope, and the Node test runner imports that module without a `window`.
 *
 * @param {(Window & typeof globalThis)|undefined} target
 * @returns {boolean} true when this call installed the guard
 */
export function installSessionGuard(target) {
  if (installed) return false;
  if (!target || typeof target.fetch !== 'function' || !target.document) {
    return false;
  }
  target.fetch = wrapFetchWithSessionGuard(target.fetch.bind(target), target.document);
  installed = true;
  return true;
}
