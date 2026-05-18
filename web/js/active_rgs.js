// @ts-check

/**
 * Active release-group ID cache for the Browse-search inverted Replace
 * button.
 *
 * The Replace button on a Browse-search row is enabled only when an
 * existing non-replaced ``album_requests`` row already targets a
 * sibling MBID in the same release group — otherwise there's nothing
 * to replace. ``GET /api/pipeline/active-rgs`` returns the distinct
 * set of release-group IDs held by any non-replaced row; the frontend
 * caches that set and consults it per rendered pressing row.
 *
 * Cache lifecycle:
 *   - Lazy-loaded on first ``hasActiveRg`` call (the Browse view
 *     consumer) via an in-flight Promise so concurrent callers share
 *     one fetch.
 *   - Cleared after any successful add / replace / remove via
 *     ``invalidateActiveRgs``; the next access re-fetches.
 *   - The fetch is fire-and-forget if it fails (network error) —
 *     ``hasActiveRg`` returns ``false`` on a missing cache, which
 *     keeps the button disabled and surfaces a benign "nothing to
 *     replace" tooltip rather than a hard error.
 */

import { API } from './state.js';

/** @type {Set<string>|null} */
let activeRgSet = null;

/** @type {Promise<Set<string>>|null} */
let inflight = null;

/**
 * Fetch ``/api/pipeline/active-rgs`` and cache the result. Concurrent
 * callers share the same in-flight Promise.
 *
 * @returns {Promise<Set<string>>}
 */
export async function loadActiveRgs() {
  if (activeRgSet) return activeRgSet;
  if (inflight) return inflight;
  inflight = (async () => {
    try {
      const r = await fetch(`${API}/api/pipeline/active-rgs`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      const ids = Array.isArray(data.release_group_ids)
        ? data.release_group_ids
        : [];
      activeRgSet = new Set(ids.map(String));
      return activeRgSet;
    } catch (_e) {
      // Soft-fail: leave cache null so the next call retries. Consumers
      // observe an empty set this call, which keeps the button
      // disabled — a safer default than enabling it speculatively.
      activeRgSet = null;
      return new Set();
    } finally {
      inflight = null;
    }
  })();
  return inflight;
}

/**
 * Synchronous predicate the renderer calls per row. Returns ``false``
 * when the cache is empty / not yet loaded — the renderer must call
 * ``loadActiveRgs`` first if it wants enabled buttons on initial
 * render.
 *
 * @param {string|null|undefined} releaseGroupId
 * @returns {boolean}
 */
export function hasActiveRg(releaseGroupId) {
  if (!releaseGroupId) return false;
  if (!activeRgSet) return false;
  return activeRgSet.has(String(releaseGroupId));
}

/**
 * Clear the cache. Call after any mutation that may change the set:
 * successful add, replace, or remove. The next ``loadActiveRgs`` call
 * re-fetches.
 */
export function invalidateActiveRgs() {
  activeRgSet = null;
  inflight = null;
}
