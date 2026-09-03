// @ts-check

/**
 * Tab identity — the one owner of internal name, visible label, display
 * order, and whether switching to a tab triggers an async follow-up
 * render (a `loadX()` call).
 *
 * Before this module existed, the same facts were spelled independently
 * in four places: `web/index.html` held label + order; `web/js/main.js`
 * held a parallel `tabOrder` array (used only to compute an `nth-child`
 * position) plus an `if (name === ...) loadX()` chain; `web/js/search_plan.js`
 * held a label-to-name reverse map plus its own hardcoded list of which
 * three tabs "consume" a stashed scroll restore; `web/js/library.js` held
 * two more comparisons against exact visible label text. None of the four
 * read from the others, so the WE3 residual bug shape (`closeSearchPlanDetail`'s
 * async-render list silently drifting from `main.js::showTab`'s real
 * `loadX()` dispatch) was reachable by editing only one site.
 *
 * `TAB_DEFS` is now the single source: `main.js::showTab` calls
 * {@link dispatchTabShown} to run a tab's follow-up render, and
 * `search_plan.js::closeSearchPlanDetail` calls {@link tabHasAsyncRender}
 * to decide whether to wait for that same render — both read the same
 * `onShow` field on the same entry, so they cannot drift apart again.
 * `web/index.html` still spells the label and order in markup (there is
 * no build step to generate HTML from this module), so
 * `tests/test_js_tabs.mjs` parses the tab bar and asserts it matches
 * {@link tabDefs} exactly; that test is what keeps the two in lockstep.
 *
 * Markup addresses a tab by its stable `data-tab-name` attribute (the
 * internal name) rather than by position, so `main.js` no longer needs
 * an `nth-child` index and `search_plan.js` no longer needs to reverse-map
 * a rendered label back to a name.
 */

import { loadPipeline } from './pipeline.js';
import { loadRecents } from './recents.js';
import { loadWrongMatches } from './wrong-matches.js';

/**
 * @typedef {Object} TabDef
 * @property {string} name    Internal name — used by `showTab`, `state`,
 *   the `<section id="${name}-section">` id, and the tab's
 *   `data-tab-name` attribute.
 * @property {string} label   Visible tab label.
 * @property {(() => void)|null} onShow  Called after `showTab(name)` makes
 *   this tab active. `null` for a tab with no follow-up render.
 */

/** @type {TabDef[]} */
const TAB_DEFS = [
  { name: 'browse', label: 'Browse', onShow: null },
  { name: 'recents', label: 'Recents', onShow: () => loadRecents() },
  { name: 'pipeline', label: 'Pipeline', onShow: () => loadPipeline() },
  { name: 'manual', label: 'Wrong Matches', onShow: () => loadWrongMatches() },
];

/**
 * The tab list in display order. `web/index.html`'s tab bar must render
 * this same name/label sequence — `tests/test_js_tabs.mjs` proves it.
 *
 * @returns {TabDef[]}
 */
export function tabDefs() {
  return TAB_DEFS;
}

/**
 * @param {string} name
 * @returns {TabDef|undefined}
 */
function findTab(name) {
  return TAB_DEFS.find((t) => t.name === name);
}

/**
 * @param {string} name
 * @returns {boolean} Whether `name` is a real tab's internal name.
 */
export function isTabName(name) {
  return findTab(name) !== undefined;
}

/**
 * Whether showing `name` triggers an async follow-up render. Read by
 * `closeSearchPlanDetail` to decide whether the destination's own render
 * will consume the stashed scroll restore, or whether to apply it
 * immediately because nothing else will.
 *
 * @param {string} name
 * @returns {boolean}
 */
export function tabHasAsyncRender(name) {
  const tab = findTab(name);
  return tab ? tab.onShow !== null : false;
}

/**
 * Run the follow-up render for `name`, if it has one. No-op for an
 * unknown name or a tab with no follow-up render.
 *
 * @param {string} name
 * @returns {void}
 */
export function dispatchTabShown(name) {
  const tab = findTab(name);
  if (tab && tab.onShow) tab.onShow();
}
