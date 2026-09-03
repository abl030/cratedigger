// @ts-check

/**
 * Pure renderers for the shared browse-tab release action state.
 * Action handlers remain window-bound globals defined elsewhere.
 */

import { esc, jsArg } from './util.js';
import {
  processingDescriptionId,
  suppressProcessingAction,
} from './release_action_state.js';

/** @typedef {import('./release_action_state.js').ReleaseActionState} ReleaseActionState */
/** @typedef {import('./release_action_state.js').ProcessingOwnerPresentation} ProcessingOwnerPresentation */

export { suppressProcessingAction };

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
 * Render one focusable, inert action and its visible owner explanation.
 *
 * @param {ProcessingOwnerPresentation} presentation
 * @param {number|null} requestId
 * @param {Object} [opts]
 * @param {string} [opts.className]
 * @param {string} [opts.style]
 * @param {string} [opts.label]
 * @param {string} [opts.descriptionSuffix]
 * @returns {string}
 */
export function renderProcessingLockedControl(
  presentation,
  requestId,
  opts = {},
) {
  const suffix = opts.descriptionSuffix || 'action';
  const descriptionId = processingDescriptionId(
    requestId,
    presentation.jobId,
    suffix,
  );
  const className = opts.className || 'btn';
  const style = opts.style ? ` style="${opts.style}"` : '';
  const label = opts.label || presentation.label;
  const requestAttr = requestId
    ? ` data-pipeline-request-id="${requestId}"`
    : '';
  const recovery = presentation.recoveryTarget
    ? ` <a href="${esc(presentation.recoveryTarget)}" data-processing-recovery-link="true" onclick="event.stopPropagation()">Recovery details</a>`
    : '';
  return `<span class="processing-lock">
    <button class="${className}"${style}${requestAttr} data-processing-locked="true" aria-disabled="true" aria-describedby="${descriptionId}" onclick="return window.suppressProcessingAction(event)" onkeydown="return window.suppressProcessingAction(event)">${esc(label)}</button>
    <span class="processing-lock-reason" id="${descriptionId}">${esc(presentation.lockReason)}${recovery}</span>
  </span>`;
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
  if (state.processingPresentation) {
    return renderProcessingLockedControl(
      state.processingPresentation,
      state.pipelineId,
      {
        className: buttonClass(opts, 'disabledClassName', 'btn btn-add'),
        style: opts.disabledStyle || opts.style || '',
        descriptionSuffix: 'acquire',
      },
    );
  }
  const requestAttr = state.pipelineId
    ? ` data-pipeline-request-id="${state.pipelineId}"`
    : '';

  if (state.acquireKind === 'remove_request' && state.pipelineId) {
    const label = opts.removeLabel || 'Remove request';
    const className = buttonClass(opts, 'removeClassName', 'btn');
    const style = buttonStyle(opts, 'removeStyle');
    return `<button class="${className}"${style}${requestAttr} onclick="${stopPropagation}window.disambRemove(${state.pipelineId}, this)">${label}</button>`;
  }

  if (state.acquireKind === 'upgrade' && state.releaseId) {
    const releaseArg = jsArg(state.releaseId);
    const label = opts.upgradeLabel || 'Upgrade';
    const className = buttonClass(opts, 'upgradeClassName', 'btn btn-add');
    const style = buttonStyle(opts, 'upgradeStyle');
    return `<button class="${className}"${style}${requestAttr} onclick="${stopPropagation}window.upgradeAlbum(${releaseArg}, this)">${label}</button>`;
  }

  if (state.acquireKind === 'add' && state.releaseId) {
    const releaseArg = jsArg(state.releaseId);
    const label = opts.addLabel || 'Add request';
    const className = buttonClass(opts, 'addClassName', 'btn btn-add');
    const style = buttonStyle(opts, 'addStyle');
    return `<button class="${className}"${style}${requestAttr} onclick="${stopPropagation}window.addRelease(${releaseArg}, this)">${label}</button>`;
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

  if (state.processingPresentation) {
    if (opts.hideDisabled) return '';
    return renderProcessingLockedControl(
      state.processingPresentation,
      state.pipelineId,
      {
        className,
        style: opts.disabledStyle || '',
        label,
        descriptionSuffix: 'remove-beets',
      },
    );
  }

  if (!state.canRemoveBeets && opts.hideDisabled) {
    return '';
  }

  const artistArg = jsArg(state.artist);
  const albumArg = jsArg(state.album);
  const releaseArg = jsArg(state.releaseId);
  const requestAttr = state.pipelineId
    ? ` data-pipeline-request-id="${state.pipelineId}"`
    : '';

  return state.canRemoveBeets
    ? `<button class="${className}"${enabledStyle}${requestAttr} onclick="${stopPropagation}window.confirmDeleteBeets(${state.beetsAlbumId}, ${artistArg}, ${albumArg}, ${state.trackCount}, ${state.pipelineId ?? 'null'}, ${releaseArg})">${label}</button>`
    : `<button class="${className}"${disabledStyle} disabled>${label}</button>`;
}

/**
 * Render the "Bad rip" button for library rows whose album was imported via
 * the pipeline. Click → window.banSource(requestId, mbid). The route resolves
 * the supplying user server-side from download_log, hashes the imported
 * tracks (tag-stripped), records them as known-bad, denylists the user (if
 * resolved), removes the album from beets, and requeues — see
 * docs/plans/2026-04-29-005-feat-bad-rip-button-and-content-hash-defense-plan.md.
 *
 * Only renders when the row has both a pipeline request and a release id;
 * library rows imported outside the pipeline have nothing to ban.
 *
 * @param {ReleaseActionState} state
 * @param {Object} [opts]
 * @param {string} [opts.className]
 * @param {string} [opts.label]
 * @param {boolean} [opts.stopPropagation]
 * @returns {string}
 */
export function renderBadRipButton(state, opts = {}) {
  if (!state.pipelineId || !state.releaseId) return '';
  const className = opts.className || 'btn';
  const label = opts.label || 'Bad rip';
  if (state.processingPresentation) {
    return renderProcessingLockedControl(
      state.processingPresentation,
      state.pipelineId,
      {
        className,
        label,
        descriptionSuffix: 'bad-rip',
      },
    );
  }
  const stopPropagation = opts.stopPropagation ? 'event.stopPropagation(); ' : '';
  const releaseArg = jsArg(state.releaseId);
  return `<button class="${className}" data-pipeline-request-id="${state.pipelineId}" onclick="${stopPropagation}window.banSource(${state.pipelineId}, ${releaseArg})">${label}</button>`;
}

/**
 * Render the Replace button for the operator action.
 *
 * Two modes:
 *
 *   Standard mode (`opts.mode === 'standard'`): the row IS the request
 *   being replaced. Used on Pipeline, Wrong Matches, and Browse-library
 *   surfaces. Click → ``window.openReplacePicker({sourceRequestId,
 *   releaseGroupId, sourceLabel})``.
 *
 *   Inverted mode (`opts.mode === 'inverted'`): the row IS the new
 *   MBID. Used on Browse-search rows. Enabled only when there is an
 *   existing non-replaced request in the same release group
 *   (``opts.enabled === true``); disabled otherwise so the affordance
 *   communicates "nothing to replace here" without requiring a click.
 *   A disabled button carries one of two different explanations
 *   (issue #1355 item 6). The caller passes ``opts.unavailable: true``
 *   when the active-RG lookup failed AND its answer would have been
 *   meaningful for this row — i.e. the row's release-group id is one
 *   the cache could actually contain. Omitting it (or passing
 *   ``false``) claims the confirmed-absence explanation instead, which
 *   is also the correct thing to pass when the lookup simply doesn't
 *   apply to this row (e.g. a Discogs id, never a member of the MB-only
 *   cache) even if the fetch itself failed — a failed check on a
 *   question that could never have been answered is not "unavailable",
 *   it's irrelevant. Either way the button stays disabled — this only
 *   changes what the operator is told about why.
 *   Click → ``window.openReplacePicker({targetMbid, releaseGroupId,
 *   targetLabel})``.
 *
 * @param {Object} args
 * @param {'standard'|'inverted'} args.mode
 * @param {number} [args.sourceRequestId]  // standard mode
 * @param {string} [args.targetMbid]       // inverted mode
 * @param {string|null} [args.releaseGroupId]  // null → picker lazy-resolves
 * @param {string} [args.sourceLabel]
 * @param {string} [args.targetLabel]
 * @param {ReleaseActionState|null} [args.processingState]
 * @param {Object} [opts]
 * @param {boolean} [opts.enabled]  // inverted-mode enable flag
 * @param {boolean} [opts.unavailable]  // inverted-mode: disabled because the lookup failed, not because it confirmed absence
 * @param {string} [opts.className]
 * @param {string} [opts.style]
 * @param {string} [opts.label]
 * @param {boolean} [opts.stopPropagation]
 * @returns {string}
 */
export function renderReplaceButton(args, opts = {}) {
  const className = opts.className || 'btn';
  const label = opts.label || 'Replace';
  const style = opts.style ? ` style="${opts.style}"` : '';
  const stopPropagation = opts.stopPropagation ? 'event.stopPropagation(); ' : '';
  if (args.processingState?.processingPresentation) {
    return renderProcessingLockedControl(
      args.processingState.processingPresentation,
      args.processingState.pipelineId,
      {
        className,
        style: opts.style || '',
        label,
        descriptionSuffix: 'replace',
      },
    );
  }

  if (args.mode === 'standard') {
    if (!args.sourceRequestId) return '';
    // ``releaseGroupId`` may be null on legacy rows; the picker
    // lazy-resolves via POST /api/pipeline/<id>/resolve-rg before
    // fetching siblings. Encode an explicit JS ``null`` literal so the
    // picker's ``in`` checks behave correctly.
    const sourceArg = jsArg(args.sourceLabel || '');
    const rgArg = args.releaseGroupId ? jsArg(args.releaseGroupId) : 'null';
    return `<button class="${className}"${style} data-pipeline-request-id="${args.sourceRequestId}" onclick="${stopPropagation}window.openReplacePicker({sourceRequestId: ${args.sourceRequestId}, releaseGroupId: ${rgArg}, sourceLabel: ${sourceArg}})">${label}</button>`;
  }

  // Inverted mode.
  if (!args.targetMbid) return '';
  const enabled = opts.enabled !== false;
  const mbidArg = jsArg(args.targetMbid);
  const rgArg = args.releaseGroupId ? jsArg(args.releaseGroupId) : 'null';
  const targetArg = jsArg(args.targetLabel || '');
  if (!enabled) {
    const title = opts.unavailable
      ? 'Could not check for an existing request in this release group. Collapse and re-expand to retry.'
      : 'No existing request in this release group';
    return `<button class="${className}"${style} disabled title="${title}">${label}</button>`;
  }
  return `<button class="${className}"${style} onclick="${stopPropagation}window.openReplacePicker({targetMbid: ${mbidArg}, releaseGroupId: ${rgArg}, targetLabel: ${targetArg}})">${label}</button>`;
}

/**
 * Render the toolbar HTML for one row.
 *
 * @param {ReleaseActionState} state
 * @param {Object} [opts]
 * @param {string} [opts.size] - 'normal' or 'small' for compact layouts
 * @param {boolean} [opts.hideDisabledRemove] - omit beets removal when unavailable
 * @returns {string}
 */
export function renderActionToolbar(state, opts = {}) {
  const sizeStyle = opts.size === 'small'
    ? 'padding:2px 8px;font-size:0.7em;'
    : 'padding:4px 10px;font-size:0.78em;';
  const baseStyle = `${sizeStyle}white-space:nowrap;`;
  if (state.processingPresentation) {
    const locked = renderProcessingLockedControl(
      state.processingPresentation,
      state.pipelineId,
      {
        className: 'btn',
        style: baseStyle,
        descriptionSuffix: 'toolbar',
      },
    );
    return `<span class="action-toolbar" style="display:inline-flex;gap:4px;flex-wrap:wrap;">${locked}</span>`;
  }
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
    hideDisabled: opts.hideDisabledRemove,
  });

  return `<span class="action-toolbar" style="display:inline-flex;gap:4px;flex-wrap:wrap;">${acquireBtn}${removeBeetsBtn}</span>`;
}
