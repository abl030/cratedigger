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
 *   - Loaded by an explicit ``loadActiveRgs()`` call — the Browse view
 *     consumer (``discography.js::loadReleaseGroup``) awaits it before
 *     rendering rows, so its ``hasActiveRg``/``activeRgsUnavailable``
 *     reads see a settled result. ``hasActiveRg`` itself is a
 *     synchronous cache read and never triggers a fetch. Concurrent
 *     ``loadActiveRgs`` callers share one in-flight Promise.
 *   - Cleared after any successful add / replace / remove via
 *     ``invalidateActiveRgs``; the next access re-fetches.
 *   - The fetch is fire-and-forget if it fails (HTTP error, network
 *     error, or a malformed response shape) — ``hasActiveRg`` returns
 *     ``false`` on a missing cache, which keeps the button disabled and
 *     is a safer default than enabling it speculatively. That failure
 *     is not silent, though: ``activeRgsUnavailable`` tells the
 *     renderer this attempt couldn't answer the question at all, so it
 *     doesn't have to present "couldn't check" as "confirmed no
 *     existing request" (issue #1355 item 6). Retrying is the same
 *     lazy re-fetch that already happens on the next
 *     ``loadActiveRgs`` call, since a failed attempt leaves the cache
 *     null.
 */

import { API } from './state.js';

/** @type {Set<string>|null} */
let activeRgSet = null;

/** @type {Promise<Set<string>>|null} */
let inflight = null;

/**
 * Whether the most recently completed fetch attempt failed to answer
 * the question, rather than confirming an empty collection. Cleared by
 * a subsequent successful load, or by ``invalidateActiveRgs``.
 * @type {boolean}
 */
let lastLoadUnavailable = false;

/**
 * Bumped by ``invalidateActiveRgs``. A ``loadActiveRgs`` attempt started
 * before a bump is stale by the time it settles — an in-flight request
 * that outlives an ``invalidateActiveRgs`` call (a fresh row triggers a
 * new load; the old cache/inflight pointer is gone; a second fetch
 * starts) can resolve or reject well after the bump. There is no
 * guarantee a successor attempt has actually finished (or even
 * started) by then, so a stale attempt does not write its own result
 * OR silently report "no answer" as a confirmed one: it re-enters
 * ``loadActiveRgs`` so its caller adopts whatever the current cache
 * already holds, joins whichever attempt is now in flight, or starts a
 * fresh one of its own — the caller asked a real question and gets a
 * real answer either way, never a stale write and never a false
 * "checked, found nothing".
 * @type {number}
 */
let generation = 0;

/**
 * Fetch ``/api/pipeline/active-rgs`` and cache the result. Concurrent
 * callers share the same in-flight Promise.
 *
 * @returns {Promise<Set<string>>}
 */
export async function loadActiveRgs() {
  if (activeRgSet) return activeRgSet;
  if (inflight) return inflight;
  const myGeneration = generation;
  inflight = (async () => {
    try {
      const r = await fetch(`${API}/api/pipeline/active-rgs`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      if (!Array.isArray(data.release_group_ids)) {
        // The API contract broke, not the collection. Route through the
        // same catch as a transport failure rather than caching an
        // empty set that would read as "confirmed no active requests".
        throw new Error('malformed release_group_ids');
      }
      if (myGeneration !== generation) {
        // Superseded by invalidateActiveRgs while this fetch was in
        // flight. Discarding this response is correct — whatever
        // changed may already make it wrong — but this caller still
        // asked a real question. Re-enter so it adopts the current
        // cache, joins whichever attempt is now in flight, or starts a
        // fresh one, rather than silently reporting "no answer" as a
        // confirmed empty collection.
        return loadActiveRgs();
      }
      activeRgSet = new Set(data.release_group_ids.map(String));
      lastLoadUnavailable = false;
      return activeRgSet;
    } catch (_e) {
      if (myGeneration !== generation) {
        return loadActiveRgs();
      }
      // Soft-fail: leave cache null so the next call retries. Consumers
      // observe an empty set this call, which keeps the button
      // disabled — a safer default than enabling it speculatively —
      // but ``activeRgsUnavailable`` now records that this call
      // couldn't answer the question.
      activeRgSet = null;
      lastLoadUnavailable = true;
      return new Set();
    } finally {
      // Only this attempt's own generation may release the in-flight
      // slot: a stale attempt finishing after a newer one has already
      // started must not clear the NEWER attempt's inflight pointer.
      if (myGeneration === generation) {
        inflight = null;
      }
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
 * Whether the cache's most recent completed load attempt failed to
 * answer the question (HTTP error, network error, or a malformed
 * response) rather than confirming an empty collection. The renderer
 * uses this to choose an honest explanation for a disabled
 * inverted-mode Replace button — "couldn't check" instead of "no
 * existing request" — while keeping the button itself disabled either
 * way.
 *
 * @returns {boolean}
 */
export function activeRgsUnavailable() {
  return lastLoadUnavailable;
}

/**
 * Clear the cache. Call after any mutation that may change the set:
 * successful add, replace, or remove. The next ``loadActiveRgs`` call
 * re-fetches.
 */
export function invalidateActiveRgs() {
  activeRgSet = null;
  inflight = null;
  lastLoadUnavailable = false;
  generation += 1;
}
