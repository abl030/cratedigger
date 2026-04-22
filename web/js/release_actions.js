// @ts-check

/**
 * Pure renderers for the shared browse-tab release action state.
 * Action handlers remain window-bound globals defined elsewhere.
 */

import { jsArg } from './util.js';

/** @typedef {import('./release_action_state.js').ReleaseActionState} ReleaseActionState */

/**
 * @typedef {Object} AcquireActionButtonOptions
 * @property {string} [className]
 * @property {string} [addClassName]
 * @property {string} [upgradeClassName]
 * @property {string} [removeClassName]
 * @property {string} [disabledClassName]
 * @property {string} [style]
 * @property {string} [addStyle]
 * @property {string} [upgradeStyle]
 * @property {string} [removeStyle]
 * @property {string} [disabledStyle]
 * @property {string} [addLabel]
 * @property {string} [upgradeLabel]
 * @property {string} [removeLabel]
 * @property {string} [disabledLabel]
 * @property {boolean} [stopPropagation]
 * @property {boolean} [hideDisabled]
 */

/**
 * @param {AcquireActionButtonOptions} opts
 * @param {'addClassName'|'upgradeClassName'|'removeClassName'|'disabledClassName'} key
 * @param {string} fallback
 * @returns {string}
 */
function buttonClass(opts, key, fallback) {
  return opts[key] || opts.className || fallback;
}

/**
 * @param {AcquireActionButtonOptions} opts
 * @param {'addStyle'|'upgradeStyle'|'removeStyle'|'disabledStyle'} key
 * @returns {string}
 */
function buttonStyle(opts, key) {
  const style = opts[key] || opts.style || '';
  return style ? ` style="${style}"` : '';
}

/**
 * Render the shared acquire action button from a release action state.
 *
 * @param {ReleaseActionState} state
 * @param {AcquireActionButtonOptions} [opts]
 * @returns {string}
 */
export function renderAcquireActionButton(state, opts = {}) {
  const stopPropagation = opts.stopPropagation ? 'event.stopPropagation(); ' : '';

  if (state.acquireKind === 'remove_request' && state.pipelineId) {
    const label = opts.removeLabel || 'Remove request';
    const className = buttonClass(opts, 'removeClassName', 'btn');
    const style = buttonStyle(opts, 'removeStyle');
    return `<button class="${className}"${style} onclick="${stopPropagation}window.disambRemove(${state.pipelineId}, this)">${label}</button>`;
  }

  if (state.acquireKind === 'upgrade' && state.releaseId) {
    const releaseArg = jsArg(state.releaseId);
    const label = opts.upgradeLabel || 'Upgrade';
    const className = buttonClass(opts, 'upgradeClassName', 'btn btn-add');
    const style = buttonStyle(opts, 'upgradeStyle');
    return `<button class="${className}"${style} onclick="${stopPropagation}window.upgradeAlbum(${releaseArg}, this)">${label}</button>`;
  }

  if (state.acquireKind === 'add' && state.releaseId) {
    const releaseArg = jsArg(state.releaseId);
    const label = opts.addLabel || 'Add request';
    const className = buttonClass(opts, 'addClassName', 'btn btn-add');
    const style = buttonStyle(opts, 'addStyle');
    return `<button class="${className}"${style} onclick="${stopPropagation}window.addRelease(${releaseArg}, this)">${label}</button>`;
  }

  if (opts.hideDisabled) {
    return '';
  }

  const label = opts.disabledLabel || 'Add request';
  const className = buttonClass(opts, 'disabledClassName', 'btn btn-add');
  const style = buttonStyle(opts, 'disabledStyle');
  return `<button class="${className}"${style} disabled>${label}</button>`;
}

/**
 * Render the shared delete-from-beets button for browse-tab surfaces.
 *
 * @param {ReleaseActionState} state
 * @param {Object} [opts]
 * @param {string} [opts.className]
 * @param {string} [opts.enabledStyle]
 * @param {string} [opts.disabledStyle]
 * @param {string} [opts.label]
 * @param {boolean} [opts.stopPropagation]
 * @param {boolean} [opts.hideDisabled]
 * @returns {string}
 */
export function renderRemoveFromBeetsButton(state, opts = {}) {
  const className = opts.className || 'btn';
  const label = opts.label || 'Remove from beets';
  const stopPropagation = opts.stopPropagation ? 'event.stopPropagation(); ' : '';

  const enabledStyle = opts.enabledStyle ? ` style="${opts.enabledStyle}"` : '';
  const disabledStyle = opts.disabledStyle ? ` style="${opts.disabledStyle}"` : '';

  if (!state.canRemoveBeets && opts.hideDisabled) {
    return '';
  }

  const artistArg = jsArg(state.artist);
  const albumArg = jsArg(state.album);
  const releaseArg = jsArg(state.releaseId);

  return state.canRemoveBeets
    ? `<button class="${className}"${enabledStyle} onclick="${stopPropagation}window.confirmDeleteBeets(${state.beetsAlbumId}, ${artistArg}, ${albumArg}, ${state.trackCount}, ${state.pipelineId ?? 'null'}, ${releaseArg})">${label}</button>`
    : `<button class="${className}"${disabledStyle} disabled>${label}</button>`;
}

/**
 * Render the toolbar HTML for one row.
 *
 * @param {ReleaseActionState} state
 * @param {Object} [opts]
 * @param {string} [opts.size] - 'normal' or 'small' for compact layouts
 * @returns {string}
 */
export function renderActionToolbar(state, opts = {}) {
  const sizeStyle = opts.size === 'small'
    ? 'padding:2px 8px;font-size:0.7em;'
    : 'padding:4px 10px;font-size:0.78em;';
  const baseStyle = `${sizeStyle}white-space:nowrap;`;
  const acquireBtn = renderAcquireActionButton(state, {
    addStyle: baseStyle,
    upgradeStyle: baseStyle,
    removeStyle: `${baseStyle}background:#5a2a2a;color:#f88;`,
    disabledStyle: baseStyle,
    stopPropagation: true,
  });
  const removeBeetsBtn = renderRemoveFromBeetsButton(state, {
    className: 'btn',
    enabledStyle: `${baseStyle}background:#3a2a2a;color:#f88;`,
    disabledStyle: baseStyle,
    stopPropagation: true,
  });

  return `<span class="action-toolbar" style="display:inline-flex;gap:4px;flex-wrap:wrap;">${acquireBtn}${removeBeetsBtn}</span>`;
}
