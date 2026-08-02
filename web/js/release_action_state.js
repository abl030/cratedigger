// @ts-check

import {
  pipelineStoreKey,
  resolvePipelineLifecycle,
  updatePipelineStatus,
} from './state.js';

/**
 * @typedef {'add' | 'upgrade' | 'remove_request' | 'disabled'} AcquireActionKind
 */

/**
 * @typedef {Object} ReleaseActionInput
 * @property {string} id
 * @property {boolean} [in_library]
 * @property {number|null} [beets_album_id]
 * @property {string|null} [pipeline_status]
 * @property {number|null} [pipeline_id]
 * @property {ProcessingOwnerProjection|null} [processing_owner]
 * @property {string} [artist]
 * @property {string} [album]
 * @property {number} [track_count]
 */

/**
 * Shared action/view model for browse-tab release actions.
 *
 * @typedef {Object} ReleaseActionState
 * @property {string} releaseId
 * @property {boolean} inLibrary
 * @property {number|null} beetsAlbumId
 * @property {string|null} pipelineStatus
 * @property {number|null} pipelineId
 * @property {string} artist
 * @property {string} album
 * @property {number} trackCount
 * @property {AcquireActionKind} acquireKind
 * @property {boolean} canRemoveBeets
 * @property {ProcessingOwnerProjection|null} processingOwner
 * @property {ProcessingOwnerPresentation|null} processingPresentation
 * @property {boolean} processingLocked
 */

/**
 * Exact processor owner projected by the server from the request's recorded
 * `active_automation_import_job_id`. It is never selected by recency or path.
 *
 * @typedef {Object} ProcessingOwnerProjection
 * @property {number} job_id
 * @property {string} status
 * @property {string|null} preview_status
 */

/**
 * Shared presentation consumed by badges, rows, and mutation controls.
 *
 * @typedef {Object} ProcessingOwnerPresentation
 * @property {number|null} jobId
 * @property {string} label
 * @property {string} lockReason
 * @property {string} accessibleDescription
 * @property {string|null} recoveryTarget
 * @property {string} badgeClass
 */

/**
 * @typedef {Object} ProcessingConflict
 * @property {number} requestId
 * @property {ProcessingOwnerProjection|null} owner
 */

/** @type {Map<number, number>} */
const processingRefreshGenerations = new Map();

/**
 * Allocate one response-order fence for an affected request.
 *
 * @param {number} requestId
 * @returns {number}
 */
function beginProcessingRefresh(requestId) {
  const generation = (processingRefreshGenerations.get(requestId) || 0) + 1;
  processingRefreshGenerations.set(requestId, generation);
  return generation;
}

/**
 * @param {number} requestId
 * @param {number} generation
 * @returns {boolean}
 */
function processingRefreshIsCurrent(requestId, generation) {
  return processingRefreshGenerations.get(requestId) === generation;
}

/**
 * Authoritative request projection returned by the affected-row refresh.
 *
 * @typedef {Object} ProcessingRequestProjection
 * @property {number} requestId
 * @property {string} releaseId
 * @property {string} status
 * @property {ProcessingOwnerProjection|null} owner
 */

/**
 * @param {unknown} value
 * @returns {number|null}
 */
function toPositiveNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

/**
 * @param {unknown} value
 * @returns {number}
 */
function toCount(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

/**
 * Normalize only the typed owner projection. Invalid projections do not gain
 * a guessed job identity; a processing row still fails closed below.
 *
 * @param {unknown} raw
 * @returns {ProcessingOwnerProjection|null}
 */
function processingOwnerProjection(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const owner = /** @type {{job_id?: unknown, status?: unknown, preview_status?: unknown}} */ (raw);
  const jobId = toPositiveNumber(owner.job_id);
  if (!jobId || typeof owner.status !== 'string') return null;
  return {
    job_id: jobId,
    status: owner.status,
    preview_status: typeof owner.preview_status === 'string'
      ? owner.preview_status
      : null,
  };
}

/**
 * Present a processing request from its exact durable owner state.
 *
 * @param {string|null|undefined} pipelineStatus
 * @param {unknown} rawOwner
 * @returns {ProcessingOwnerPresentation|null}
 */
export function processingOwnerPresentation(pipelineStatus, rawOwner) {
  if (pipelineStatus !== 'processing') return null;
  const owner = processingOwnerProjection(rawOwner);
  if (!owner) {
    const reason = 'Actions are locked while processor ownership details are unavailable.';
    return {
      jobId: null,
      label: 'processing',
      lockReason: reason,
      accessibleDescription: reason,
      recoveryTarget: null,
      badgeClass: 'badge-processing',
    };
  }

  let label = 'processing';
  let badgeClass = 'badge-processing';
  if (owner.status === 'recovery_required') {
    label = 'needs recovery';
    badgeClass = 'badge-failed';
  } else if (owner.status === 'running') {
    label = 'importing';
  } else if (owner.status === 'queued' && owner.preview_status === 'waiting') {
    label = 'queued for preview';
  } else if (owner.status === 'queued' && owner.preview_status === 'running') {
    label = 'previewing';
  } else if (
    owner.status === 'queued'
    && owner.preview_status === 'evidence_ready'
  ) {
    label = 'waiting to import';
  }
  const lockReason = label === 'needs recovery'
    ? `Actions are locked while historical automation job #${owner.job_id} awaits automatic convergence.`
    : `Actions are locked while automation job #${owner.job_id} is ${label}.`;
  return {
    jobId: owner.job_id,
    label,
    lockReason,
    accessibleDescription: `${lockReason} Open the read-only exact owner recovery detail.`,
    recoveryTarget: `/api/import-jobs/${owner.job_id}/recovery`,
    badgeClass,
  };
}

/**
 * Stable description id used by every locked control.
 *
 * @param {number|null} requestId
 * @param {number|null} jobId
 * @param {string} [suffix]
 * @returns {string}
 */
export function processingDescriptionId(requestId, jobId, suffix = 'action') {
  const identity = jobId || requestId || 0;
  return `processing-owner-${identity}-${suffix.replace(/[^a-z0-9_-]/gi, '-')}`;
}

/**
 * Canonical typed 409 detector. The transition-conflict branch is temporary
 * while generic lifecycle adapters converge on the direct service shape.
 *
 * @param {number} httpStatus
 * @param {unknown} payload
 * @returns {ProcessingConflict|null}
 */
export function processingConflictFromResponse(httpStatus, payload) {
  if (httpStatus !== 409 || !payload || typeof payload !== 'object') return null;
  const body = /** @type {{
   * error?: unknown,
   * reason?: unknown,
   * request_id?: unknown,
   * processing_owner?: unknown,
   * }} */ (payload);
  const canonical = body.error === 'processing_locked';
  const transition = (
    body.error === 'transition_conflict'
    && body.reason === 'processing_locked'
  );
  if (!canonical && !transition) return null;
  const requestId = toPositiveNumber(body.request_id);
  if (!requestId) return null;
  return {
    requestId,
    owner: processingOwnerProjection(body.processing_owner),
  };
}

/**
 * Suppress a focusable aria-disabled processing action.
 *
 * @param {{type?: string, key?: string, preventDefault?: () => void, stopPropagation?: () => void}} event
 * @returns {boolean}
 */
export function suppressProcessingAction(event) {
  const blocks = event.type === 'click'
    || (event.type === 'keydown' && (event.key === 'Enter' || event.key === ' '));
  if (!blocks) return true;
  event.preventDefault?.();
  event.stopPropagation?.();
  return false;
}

/**
 * @param {ProcessingOwnerPresentation} presentation
 * @param {number} requestId
 * @returns {HTMLElement|null}
 */
function ensureVisibleProcessingDescription(presentation, requestId) {
  if (typeof document === 'undefined') return null;
  const id = processingDescriptionId(requestId, presentation.jobId, 'conflict');
  const existing = document.getElementById(id);
  const description = existing || document.createElement('span');
  if (!existing) {
    description.id = id;
    description.className = 'processing-lock-reason';
  }
  description.textContent = presentation.lockReason;
  if (presentation.recoveryTarget) {
    const link = document.createElement('a');
    link.href = presentation.recoveryTarget;
    link.textContent = ' Recovery details';
    link.setAttribute('data-processing-recovery-link', 'true');
    description.appendChild(link);
  }
  return description;
}

/**
 * @param {HTMLElement} control
 * @param {ProcessingOwnerPresentation} presentation
 * @param {number} requestId
 * @returns {HTMLElement}
 */
function lockProcessingControl(control, presentation, requestId) {
  let lockedControl = control;
  if (
    typeof HTMLSelectElement !== 'undefined'
    && control instanceof HTMLSelectElement
    && typeof document !== 'undefined'
  ) {
    const replacement = document.createElement('button');
    replacement.type = 'button';
    replacement.id = control.id;
    replacement.className = control.className;
    const style = control.getAttribute('style');
    if (style) replacement.setAttribute('style', style);
    control.replaceWith(replacement);
    lockedControl = replacement;
  }
  const description = ensureVisibleProcessingDescription(presentation, requestId);
  const descriptionId = processingDescriptionId(
    requestId,
    presentation.jobId,
    'conflict',
  );
  const oldDescriptionId = lockedControl.getAttribute('aria-describedby');
  if (oldDescriptionId && oldDescriptionId !== descriptionId) {
    document.getElementById(oldDescriptionId)?.remove();
  }
  lockedControl.setAttribute('aria-disabled', 'true');
  lockedControl.setAttribute('aria-describedby', descriptionId);
  lockedControl.setAttribute('data-pipeline-request-id', String(requestId));
  lockedControl.setAttribute('data-processing-locked', 'true');
  lockedControl.dataset.processingLocked = 'true';
  lockedControl.removeAttribute('disabled');
  lockedControl.removeAttribute('onclick');
  lockedControl.removeAttribute('onchange');
  lockedControl.onclick = suppressProcessingAction;
  lockedControl.onkeydown = suppressProcessingAction;
  if (
    (typeof HTMLButtonElement !== 'undefined'
      && lockedControl instanceof HTMLButtonElement)
  ) {
    lockedControl.disabled = false;
  }
  if (presentation.label) lockedControl.textContent = presentation.label;
  if (description && !description.isConnected) {
    lockedControl.insertAdjacentElement('afterend', description);
  }
  return lockedControl;
}

/**
 * Accept only an explicit, live mutation control that is not bound to another
 * request. Request-scoped controls discovered by querySelectorAll are already
 * authoritative and do not pass through this helper.
 *
 * @param {HTMLElement|null|undefined} control
 * @param {number} requestId
 * @returns {HTMLElement|null}
 */
function processingOriginControl(control, requestId) {
  if (!control || typeof control.setAttribute !== 'function') return null;
  if (
    typeof document !== 'undefined'
    && (
      control === document.body
      || (
        document.documentElement
        && control === document.documentElement
      )
    )
  ) {
    return null;
  }
  if (control.isConnected === false) return null;
  const boundRequestId = control.getAttribute('data-pipeline-request-id');
  if (boundRequestId && boundRequestId !== String(requestId)) return null;
  return control;
}

/**
 * @param {string} message
 */
function announceStatusMessage(message) {
  if (typeof document === 'undefined') return;
  let live = document.getElementById('processing-lock-live-region');
  if (!live) {
    live = document.createElement('div');
    live.id = 'processing-lock-live-region';
    live.setAttribute('role', 'status');
    live.setAttribute('aria-live', 'polite');
    live.setAttribute(
      'style',
      'position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);',
    );
    document.body.appendChild(live);
  }
  live.textContent = message;
}

/**
 * @param {ProcessingOwnerPresentation} presentation
 */
function announceProcessingLock(presentation) {
  announceStatusMessage(presentation.accessibleDescription);
}

/**
 * Refresh only the affected request projection.
 *
 * @param {number} requestId
 * @param {string} releaseId
 * @param {number|null} refreshGeneration
 * @returns {Promise<ProcessingRequestProjection|null>}
 */
export async function refetchProcessingRequest(
  requestId,
  releaseId = '',
  refreshGeneration = null,
) {
  const response = await fetch(`/api/pipeline/${requestId}`);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const data = await response.json();
  const request = data.request;
  if (!request || typeof request !== 'object') {
    throw new Error('request projection missing');
  }
  const row = /** @type {{
   * status?: unknown,
   * id?: unknown,
   * mb_release_id?: unknown,
   * discogs_release_id?: unknown,
   * processing_owner?: unknown,
   * }} */ (request);
  const status = typeof row.status === 'string' ? row.status : 'processing';
  const requestProjectionId = toPositiveNumber(row.id) || requestId;
  const projectedReleaseId = (
    typeof row.mb_release_id === 'string' && row.mb_release_id
  ) || (
    typeof row.discogs_release_id === 'string' && row.discogs_release_id
  ) || releaseId;
  const owner = processingOwnerProjection(row.processing_owner);
  const projection = {
    requestId: requestProjectionId,
    releaseId: projectedReleaseId,
    status,
    owner,
  };
  if (
    refreshGeneration !== null
    && !processingRefreshIsCurrent(requestId, refreshGeneration)
  ) {
    return null;
  }
  if (projectedReleaseId) {
    updatePipelineStatus(
      projectedReleaseId,
      status,
      requestProjectionId,
      owner,
    );
  }
  reconcileProcessingRequest(
    requestProjectionId,
    status,
    owner,
  );
  return projection;
}

/**
 * Repaint the controls already present for the affected row from the
 * authoritative request projection. A non-processing status stays inert
 * until its owning surface next renders its status-specific actions; this is
 * safer than restoring the stale mutation that lost the ownership race.
 *
 * @param {number} requestId
 * @param {string} status
 * @param {ProcessingOwnerProjection|null} owner
 */
function reconcileProcessingRequest(requestId, status, owner) {
  if (typeof document === 'undefined') return;
  const controls = document.querySelectorAll(
    `[data-pipeline-request-id="${requestId}"]`,
  );
  const presentation = processingOwnerPresentation(status, owner);
  for (const candidate of controls) {
    if (typeof candidate.setAttribute !== 'function') continue;
    const control = /** @type {HTMLElement} */ (candidate);
    if (presentation) {
      lockProcessingControl(control, presentation, requestId);
      continue;
    }
    const oldDescriptionId = control.getAttribute('aria-describedby');
    if (oldDescriptionId) document.getElementById(oldDescriptionId)?.remove();
    const descriptionId = processingDescriptionId(requestId, null, 'refreshed');
    let description = document.getElementById(descriptionId);
    if (!description) {
      description = document.createElement('span');
      description.id = descriptionId;
      description.className = 'processing-lock-reason';
    }
    description.textContent = `Request is now ${status}. Refresh this view for current actions.`;
    control.removeAttribute('data-processing-locked');
    delete control.dataset.processingLocked;
    control.setAttribute('data-request-refreshed-status', status);
    control.setAttribute('aria-disabled', 'true');
    control.setAttribute('aria-describedby', descriptionId);
    control.textContent = status;
    control.onclick = suppressProcessingAction;
    control.onkeydown = suppressProcessingAction;
    if (!description.isConnected) {
      control.insertAdjacentElement('afterend', description);
    }
  }
}

/**
 * @param {ProcessingRequestProjection|null|void} refreshed
 * @param {ProcessingOwnerPresentation} fallback
 */
function announceRefreshResult(refreshed, fallback) {
  if (refreshed === null) return;
  const freshPresentation = refreshed
    ? processingOwnerPresentation(refreshed.status, refreshed.owner)
    : null;
  if (freshPresentation) {
    announceProcessingLock(freshPresentation);
    return;
  }
  if (refreshed) {
    announceStatusMessage(
      `Request #${refreshed.requestId} is now ${refreshed.status}. `
      + 'Refresh this view for current actions.',
    );
    return;
  }
  announceProcessingLock(fallback);
}

/**
 * @param {HTMLElement} control
 * @param {() => Promise<ProcessingRequestProjection|null|void>} refetch
 * @param {ProcessingOwnerPresentation} presentation
 */
function exposeRefreshRetry(control, refetch, presentation) {
  if (typeof document === 'undefined') return;
  const retry = document.createElement('button');
  retry.type = 'button';
  retry.className = 'p-btn processing-refresh-retry';
  retry.textContent = 'Retry row refresh';
  retry.setAttribute('aria-label', `Retry row refresh. ${presentation.lockReason}`);
  retry.onclick = async () => {
    const ownsFocus = document.activeElement === retry;
    retry.setAttribute('aria-busy', 'true');
    try {
      const refreshed = await refetch();
      const stillOwnsFocus = ownsFocus && document.activeElement === retry;
      retry.remove();
      if (refreshed === null) return;
      announceRefreshResult(refreshed, presentation);
      if (stillOwnsFocus && control.isConnected) {
        control.focus({ preventScroll: true });
      }
    } catch (_error) {
      retry.removeAttribute('aria-busy');
      announceProcessingLock(presentation);
    }
  };
  control.insertAdjacentElement('afterend', retry);
}

/**
 * Convert the canonical typed conflict into the same locked state used by a
 * freshly rendered processing row, then refresh only that request projection.
 *
 * @param {Object} args
 * @param {number} args.httpStatus
 * @param {unknown} args.payload
 * @param {HTMLElement|null|undefined} [args.control]
 * @param {string} [args.releaseId]
 * @param {(requestId: number, refreshGeneration: number) => Promise<ProcessingRequestProjection|null|void>} [args.refetch]
 * @returns {Promise<boolean>}
 */
export async function handleProcessingLockedConflict({
  httpStatus,
  payload,
  control = null,
  releaseId = '',
  refetch,
}) {
  const conflict = processingConflictFromResponse(httpStatus, payload);
  if (!conflict) return false;
  const presentation = processingOwnerPresentation('processing', conflict.owner);
  if (!presentation) return false;
  const originatingControl = processingOriginControl(control, conflict.requestId);
  const focusTarget = typeof document !== 'undefined'
    ? document.activeElement
    : null;
  let originLocked = false;
  let retryAnchor = null;

  if (releaseId) {
    updatePipelineStatus(
      releaseId,
      'processing',
      conflict.requestId,
      conflict.owner,
    );
  }
  if (typeof document !== 'undefined') {
    const controls = document.querySelectorAll(
      `[data-pipeline-request-id="${conflict.requestId}"]`,
    );
    for (const candidate of controls) {
      if (typeof candidate.setAttribute === 'function') {
        const candidateControl = /** @type {HTMLElement} */ (candidate);
        const candidateWasFocused = candidateControl === focusTarget;
        const locked = lockProcessingControl(
          candidateControl,
          presentation,
          conflict.requestId,
        );
        if (!retryAnchor) retryAnchor = locked;
        if (candidateControl === originatingControl) {
          originLocked = true;
          retryAnchor = locked;
        }
        if (candidateWasFocused && locked !== candidateControl) {
          locked.focus({ preventScroll: true });
        }
      }
    }
  }
  if (originatingControl && !originLocked) {
    const originWasFocused = originatingControl === focusTarget;
    const locked = lockProcessingControl(
      originatingControl,
      presentation,
      conflict.requestId,
    );
    retryAnchor = locked;
    if (originWasFocused && locked !== originatingControl) {
      locked.focus({ preventScroll: true });
    }
  }
  announceProcessingLock(presentation);

  const refresh = async () => {
    const generation = beginProcessingRefresh(conflict.requestId);
    let refreshed;
    try {
      refreshed = refetch
        ? await refetch(conflict.requestId, generation)
        : await refetchProcessingRequest(
          conflict.requestId,
          releaseId,
          generation,
        );
    } catch (error) {
      if (!processingRefreshIsCurrent(conflict.requestId, generation)) {
        return null;
      }
      throw error;
    }
    return processingRefreshIsCurrent(conflict.requestId, generation)
      ? refreshed
      : null;
  };
  try {
    const refreshed = await refresh();
    announceRefreshResult(refreshed, presentation);
  } catch (_error) {
    if (retryAnchor) exposeRefreshRetry(retryAnchor, refresh, presentation);
  }
  return true;
}

/**
 * Build the shared browse action state from any row/detail payload.
 * The central pipeline store overlays recent mutations so all browse
 * surfaces render the same action semantics after local writes.
 *
 * @param {ReleaseActionInput} item
 * @returns {ReleaseActionState}
 */
export function buildReleaseActionState(item) {
  const releaseId = pipelineStoreKey(item.id);
  const lifecycle = resolvePipelineLifecycle(
    releaseId,
    item.pipeline_status,
    item.pipeline_id,
    item.processing_owner,
  );
  const pipelineStatus = lifecycle.status;
  const pipelineId = toPositiveNumber(lifecycle.id);
  const processingOwner = processingOwnerProjection(
    lifecycle.processing_owner,
  );
  const processingPresentation = processingOwnerPresentation(
    pipelineStatus,
    processingOwner,
  );
  const inLibrary = !!item.in_library;
  const beetsAlbumId = toPositiveNumber(item.beets_album_id);

  /** @type {AcquireActionKind} */
  let acquireKind = 'disabled';
  if (
    (pipelineStatus === 'wanted' || pipelineStatus === 'downloading')
    && pipelineId
  ) {
    acquireKind = 'remove_request';
  } else if (releaseId && (inLibrary || pipelineStatus === 'imported')) {
    acquireKind = 'upgrade';
  } else if (releaseId && !inLibrary && !pipelineStatus) {
    acquireKind = 'add';
  }

  return {
    releaseId,
    inLibrary,
    beetsAlbumId,
    pipelineStatus,
    pipelineId,
    artist: item.artist || '',
    album: item.album || '',
    trackCount: toCount(item.track_count),
    acquireKind,
    canRemoveBeets: !processingPresentation && inLibrary && !!beetsAlbumId,
    processingOwner,
    processingPresentation,
    processingLocked: processingPresentation !== null,
  };
}
