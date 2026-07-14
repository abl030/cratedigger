// @ts-check
import { API, state, toast, updatePipelineStatus } from './state.js';
import { esc, externalReleaseUrl, sourceLabel, detectSource, jsArg, normalizeReleaseId } from './util.js';
import { buildReleaseActionState } from './release_action_state.js';
import { renderActionToolbar, renderAcquireActionButton, renderRemoveFromBeetsButton, renderReplaceButton } from './release_actions.js';
import { renderStatusBadges } from './badges.js';
import { invalidateBrowseArtist } from './browse.js';
import { applyAnalysisToExpansion } from './analysis.js';
import { renderLabelLinks } from './labels.js';
import { renderSearchPlanButton } from './search_plan.js';
import { loadActiveRgs, hasActiveRg, invalidateActiveRgs } from './active_rgs.js';
import {
  renderReleaseRow, renderBeetsTrackRow, renderExpectedTrackRow, toggleExpand,
} from './render_primitives.js';

/**
 * One release-group row: year + title + badges + expansion target.
 * Shared by every section of the unified artist page (issue #575 PR4) —
 * In library, Missing, Appearances, Bootlegs, and the late-appended
 * unpaired work and ungrouped-release sections from the other source.
 *
 * @param {Object} rg - Release-group row from /api/artist or /api/discogs/artist
 *   (or a compare-bucket row for complement sections).
 * @param {{artistName: string, nameLC: string, source?: string}} ctx -
 *   nameLC is the lowercased artist name (credit-note suppression);
 *   source forces loadReleaseGroup onto 'mb'/'discogs' for rows that do
 *   not belong to state.browseSource (compare complement rows).
 * @returns {string}
 */
export function renderRgRow(rg, ctx) {
  const year = rg.first_release_date ? rg.first_release_date.slice(0, 4) : '';
  const creditNote = rg.artist_credit && rg.artist_credit.toLowerCase() !== ctx.nameLC
    ? `<span class="rg-meta"> - ${esc(rg.artist_credit)}</span>` : '';
  const badges = renderStatusBadges(rg) + catalogueProvenanceBadges(rg);
  const isReleaseUnit = rg.identity_kind === 'release';
  // Ungrouped Discogs releases have no child master to expand; the row is
  // the leaf, so it carries data-release-id for search-by-ID ringing.
  const leafAttr = isReleaseUnit ? ` data-release-id="${esc(rg.id)}"` : '';
  // Search-plan inspector button — only when this rg has a pipeline
  // request. RG-level pipeline_id surfaces from the analysis overlay's
  // disambData snapshot via pipelineStore (see release_action_state.js).
  const spBtn = renderSearchPlanButton({
    pipelineId: buildReleaseActionState({
      ...rg,
      artist: ctx.artistName,
      album: rg.title,
    }).pipelineId,
  });
  const optParts = [];
  if (isReleaseUnit) optParts.push('masterless:true');
  if (ctx.source) optParts.push(`source:'${ctx.source}'`);
  const opts = `{${optParts.join(',')}}`;
  return `
    <div class="rg" data-rg-id="${esc(rg.id)}"${leafAttr}>
      <div onclick="event.stopPropagation(); window.loadReleaseGroup(${jsArg(rg.id)}, this, ${opts})">
        <span class="rg-year">${year}</span> <span class="rg-title">${esc(rg.title)}</span>${creditNote}${badges}${spBtn}
      </div>
      <div class="releases" id="rel-${esc(rg.id)}"></div>
    </div>
  `;
}

/**
 * Preserve mixed/exceptional master evidence on the collapsed work row.
 * @param {Object} row
 * @returns {string}
 */
export function catalogueProvenanceBadges(row) {
  const provenance = new Set(row.provenance || []);
  const labels = [];
  if (provenance.has('promo')) labels.push('promo');
  if (provenance.has('unofficial')) labels.push('unofficial');
  return labels.map(label =>
    `<span class="badge badge-nonofficial">${label}</span>`).join('');
}

/**
 * Search-by-ID post-discography-render hook.
 *
 * Reads `state.searchTargetExpandId` / `state.searchTargetId`; if both
 * targets resolve into the just-rendered DOM, auto-expand the parent
 * release-group and (after the inner releases render) apply the ring.
 *
 * Masterless rg rows ARE the leaf — they carry data-release-id directly
 * (web/js/discography.js renderRgRow), so we ring + scroll the rg row
 * itself with no expansion step.
 *
 * Walks ancestors and opens any collapsed `.type-body` sections — without
 * this, a target inside Appearances, Bootleg-only, or any own/EPs/Singles
 * typed section is invisible even after the inner releases load (those
 * wrappers default to display:none until the .open class is added).
 *
 * @param {HTMLElement} rgEl - The artist-page container that just rendered.
 */
export function applySearchTargetAfterDiscography(rgEl) {
  const expandId = state.searchTargetExpandId;
  if (!expandId) return;
  // Source guard: only apply ring when the discography source matches
  // the source the resolver returned. Avoids ringing the wrong row when
  // the user is browsing MB but the resolver returned a Discogs target
  // (or vice versa).
  if (state.searchTargetSource && state.browseSource !== state.searchTargetSource) return;

  // Masterless: the rg row IS the leaf. Ring + scroll directly, no expand.
  const masterlessRow = /** @type {HTMLElement|null} */ (
    rgEl.querySelector(`.rg[data-release-id="${cssEscape(expandId)}"]`));
  if (masterlessRow) {
    openCollapsedAncestors(masterlessRow, rgEl);
    masterlessRow.classList.add('search-target');
    masterlessRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
    return;
  }

  // Non-masterless: find the parent rg row (no data-release-id) and
  // expand it via the same loadReleaseGroup helper that powers manual
  // clicks. The post-render hook in loadReleaseGroup applies the leaf ring.
  const inner = /** @type {HTMLElement|null} */ (rgEl.querySelector(`#rel-${cssEscape(expandId)}`));
  if (!inner) return;
  openCollapsedAncestors(inner, rgEl);
  if (inner.innerHTML) return;  // already expanded (cache re-render); ring will re-apply on next loadReleaseGroup
  loadReleaseGroup(expandId, inner, { targetEl: inner });
}

/**
 * Walk up from an element and add `.open` to every `.type-body` ancestor
 * up to (but not including) `stopEl`. Used by the search-by-ID hook so
 * the user actually sees the ringed target — without this, a target
 * inside Appearances, Bootleg-only, or any non-Albums typed section is
 * present in the DOM but hidden behind a collapsed `display:none`
 * wrapper, leaving the user staring at a discography with nothing
 * visibly highlighted.
 *
 * @param {HTMLElement} el
 * @param {HTMLElement} stopEl
 */
function openCollapsedAncestors(el, stopEl) {
  /** @type {HTMLElement|null} */
  let cursor = el.parentElement;
  while (cursor && cursor !== stopEl) {
    if (cursor.classList.contains('type-body')) {
      cursor.classList.add('open');
    }
    cursor = cursor.parentElement;
  }
}

/**
 * CSS.escape polyfill — vendored to keep util.js framework-free.
 * Used for ID values that may contain hyphens, dots, or other
 * selector-special characters (MB UUIDs, Discogs IDs).
 * @param {string} s
 * @returns {string}
 */
export function cssEscape(s) {
  if (typeof CSS !== 'undefined' && CSS.escape) return CSS.escape(s);
  return String(s).replace(/[^a-zA-Z0-9_-]/g, ch => `\\${ch}`);
}

/**
 * Synthesize the single pressing row for a masterless Discogs release
 * from its /api/discogs/release payload, so the rest of the rendering
 * path is unchanged. MUST forward the pipeline/library overlay fields —
 * dropping them rendered an already-requested masterless release with a
 * green "Add request" button and no badge (Deloris "Feather
 * Figure/Elastic Bones", request 8838).
 * @param {Object} data - /api/discogs/release payload
 * @returns {Object} A release row shaped like /api/discogs/master rows.
 */
export function synthesizeMasterlessRow(data) {
  return {
    id: data.id,
    title: data.title || '',
    date: data.date || '',
    country: data.country || '',
    status: data.status || 'Official',
    track_count: (data.tracks || []).length,
    format: (data.formats || []).map(f => f && f.name).filter(Boolean).join(', ') || '?',
    media_count: (data.formats || []).length,
    labels: data.labels || [],
    release_group_id: data.release_group_id ?? null,
    in_library: data.in_library,
    beets_album_id: data.beets_album_id ?? null,
    pipeline_status: data.pipeline_status ?? null,
    pipeline_id: data.pipeline_id ?? null,
    library_format: data.library_format,
    library_min_bitrate: data.library_min_bitrate,
    library_avg_bitrate: data.library_avg_bitrate,
    library_rank: data.library_rank,
  };
}

/**
 * Split a release group's date-sorted pressing rows into the visible
 * list and the collapsed Bootleg / Promo bucket.
 *
 * Invariant: a pressing the operator owns (``in_library``) or has an
 * ACTIVE pipeline request on is NEVER hidden, whatever its status — the
 * collapsed bucket only ever holds unowned, inactive non-official
 * pressings. The Wrens "The Meadowlands" (request 4228) is the pin: its
 * library copy is a Promotion pressing, and the old status-only split
 * buried it in the collapsed section, making the expansion contradict
 * the row header's "in library · wanted" badges.
 *
 * ``replaced`` does not pin: it's the terminal frozen-audit status (the
 * overlay carries it through unfiltered), renders no badge, and an
 * abandoned request is not a claim on the pressing.
 *
 * @param {Array<Object>} rows - Pressing rows (overlay fields present).
 * @returns {{visible: Object[], hidden: Object[]}}
 */
export function splitPressings(rows) {
  const pinned = (r) => r.in_library === true
    || (!!r.pipeline_status && r.pipeline_status !== 'replaced');
  const official = (r) => r.status === 'Official' || !r.status;
  return {
    visible: rows.filter(r => official(r) || pinned(r)),
    hidden: rows.filter(r => !official(r) && !pinned(r)),
  };
}

/**
 * Provenance chip for non-official pressings ("promo", "bootleg", ...).
 * Rendered on every non-official pressing row so a hoisted row (see
 * splitPressings) carries its provenance into the main list.
 * @param {string|undefined} status - MB release status.
 * @returns {string} '' for Official / missing status.
 */
export function statusChipHtml(status) {
  if (!status || status === 'Official') return '';
  const label = status === 'Promotion' ? 'promo' : status.toLowerCase();
  return `<span class="badge badge-nonofficial">${esc(label)}</span>`;
}

/**
 * Render one child pressing row in a release-group expansion.
 *
 * Child rows omit only the unavailable beets-removal action. Their acquire,
 * Replace, and search-plan actions keep the same state semantics as every
 * other browse surface; an enabled beets removal remains visible.
 *
 * @param {Object} rel - Pressing row with pipeline/library overlay fields.
 * @param {{artistName: string, parentRgId: string|null, canReplace: boolean}} ctx
 * @returns {string}
 */
export function renderPressingRow(rel, ctx) {
  const badges = renderStatusBadges(rel);
  const actionState = buildReleaseActionState({
    ...rel,
    artist: ctx.artistName,
    album: rel.title,
  });
  const toolbar = renderActionToolbar(actionState, {
    size: 'small',
    hideDisabledRemove: true,
  });
  // Search-plan inspector button — Browse-tab only renders when the
  // release has an active pipeline request (see release_action_state.js
  // for the pipelineStore lookup).
  const spBtn = renderSearchPlanButton({ pipelineId: actionState.pipelineId });
  // Replace button — two variants:
  //
  //   - ``isCurrent`` (acquireKind === 'remove_request'): this row
  //     IS the active/imported request. Standard mode — clicking
  //     opens the picker on this request's release group so the
  //     operator can switch to a sibling pressing.
  //
  //   - Otherwise: inverted mode. Clicking asks the operator which
  //     active request in this RG should be replaced with the
  //     clicked row's MBID. Enabled only when an existing
  //     non-replaced row already targets a sibling MBID in the same RG.
  //
  // ``releaseGroupId`` may be null for legacy rows; the picker
  // lazy-resolves it via ``POST /api/pipeline/<id>/resolve-rg``
  // (standard) or ``GET /api/release/<mbid>`` (inverted) before
  // fetching siblings.
  const rgForReplace = rel.release_group_id || ctx.parentRgId || null;
  const isCurrent = actionState.acquireKind === 'remove_request';
  let replaceBtn = '';
  if (isCurrent) {
    if (actionState.pipelineId) {
      replaceBtn = renderReplaceButton({
        mode: 'standard',
        sourceRequestId: actionState.pipelineId,
        releaseGroupId: rgForReplace,
        sourceLabel: `${ctx.artistName} — ${rel.title || ''}`,
      }, {
        className: 'btn',
        style: 'padding:2px 8px;font-size:0.7em;white-space:nowrap;',
        stopPropagation: true,
      });
    }
  } else {
    replaceBtn = renderReplaceButton({
      mode: 'inverted',
      targetMbid: rel.id,
      releaseGroupId: rgForReplace,
      targetLabel: `${ctx.artistName} — ${rel.title || ''}`,
    }, {
      className: 'btn',
      style: 'padding:2px 8px;font-size:0.7em;white-space:nowrap;',
      enabled: ctx.canReplace,
      stopPropagation: true,
    });
  }
  return renderReleaseRow({
    dataReleaseId: rel.id,
    onclick: `event.stopPropagation(); window.toggleReleaseDetail(${jsArg(rel.id)})`,
    titleHtml: `${esc(rel.title)}${statusChipHtml(rel.status)}${badges}`,
    metaLines: [`${rel.country || '?'} ${rel.date || '?'} - ${rel.format} - ${rel.track_count}t - ${rel.status || '?'}`],
    actionsHtml: `${toolbar}${replaceBtn}${spBtn}`,
    detail: { id: `reldet-${rel.id}` },
  });
}

/**
 * Load and display releases for a release group.
 *
 * @param {string} id - MusicBrainz release group ID or Discogs master ID
 * @param {HTMLElement} el - The clicked element (kept for signature compat)
 * @param {Object} [opts]
 * @param {HTMLElement} [opts.targetEl] - Where to render. Defaults to
 *   document.getElementById('rel-' + id) so existing call sites still work.
 *   Compare view passes its own div so its IDs don't collide with the
 *   Discography view's `rel-${id}` ones.
 * @param {string} [opts.source] - 'mb' or 'discogs'. Defaults to
 *   state.browseSource. Compare view passes the explicit source so MB and
 *   Discogs pressings can be loaded independently for the same row.
 * @param {() => boolean} [opts.isStale] - Optional callback returning true
 *   when this load should be discarded. Checked after each await and
 *   before any DOM write. Used by the VA fallback (where the target
 *   element is a stable, never-replaced node so a stale write is visible)
 *   to thread the parent flow's in-flight token down. Artist-view callers
 *   omit it because their target #rel-X is detached on re-render.
 */
export async function loadReleaseGroup(id, el, opts = {}) {
  const relEl = opts.targetEl || document.getElementById('rel-' + id);
  if (!relEl) return;
  if (relEl.innerHTML) { relEl.innerHTML = ''; return; }
  relEl.innerHTML = '<div class="loading">Loading releases...</div>';
  const isStale = opts.isStale || (() => false);
  try {
    const source = opts.source || state.browseSource;
    const isDiscogs = source === 'discogs';
    // Ungrouped Discogs release identities have no upstream master row;
    // their ``id`` is a release ID. Hit the
    // release endpoint directly and synthesise a single-pressing list so
    // the rest of the rendering path is unchanged.
    const masterless = isDiscogs && !!opts.masterless;
    const url = masterless
      ? `${API}/api/discogs/release/${id}`
      : isDiscogs
        ? `${API}/api/discogs/master/${id}`
        : `${API}/api/release-group/${id}`;
    // Warm the active-rg cache in parallel — the Browse-search inverted
    // Replace button per release row consults it. MB releases carry the
    // release-group id in the parent ``id`` here; Discogs masters don't
    // map to MB release-group IDs, so the button stays disabled on the
    // Discogs path (hasActiveRg(rel.release_group_id) — undefined → false).
    const [r] = await Promise.all([fetch(url), loadActiveRgs()]);
    if (isStale()) return;
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    if (isStale()) return;
    if (data.error) throw new Error(data.error);
    /** @type {Array<any>} */
    const releaseRows = masterless
      ? [synthesizeMasterlessRow(data)]
      : (data.releases || []);
    const all = releaseRows.sort((a, b) => (a.date || '').localeCompare(b.date || ''));
    const { visible, hidden } = splitPressings(all);

    // For MB release-group endpoints the parent ``id`` IS the
    // release_group_id. For Discogs masters it is the master id, which
    // doesn't map to an MB release-group — the button is disabled on
    // that path because ``hasActiveRg`` will look up a non-MB id.
    const parentRgId = isDiscogs ? null : id;

    const artistName = state.browseArtist?.name || '';
    const renderRelease = (rel) => {
      const rgForReplace = rel.release_group_id || parentRgId || null;
      return renderPressingRow(rel, {
        artistName,
        parentRgId,
        canReplace: hasActiveRg(rgForReplace),
      });
    };

    let html = visible.map(renderRelease).join('');
    if (hidden.length > 0) {
      html += `
        <div class="type-header" onclick="event.stopPropagation(); window.toggleSection(this)" style="color:#777;margin-top:6px;">
          Bootleg / Promo <span class="type-count">${hidden.length}</span>
        </div>
        <div class="type-body">
          ${hidden.map(renderRelease).join('')}
        </div>
      `;
    }
    if (isStale()) return;
    relEl.innerHTML = html;
    applySearchTargetAfterReleases(relEl);
    // Analysis overlay (unified artist page): when disambiguate data for
    // this release group is loaded, decorate pressing rows with colour
    // dots / exclusive counts and append the recordings breakdown.
    applyAnalysisToExpansion(relEl, id);
  } catch (e) {
    if (isStale()) return;
    relEl.innerHTML = '<div class="loading">Failed to load</div>';
  }
}

/**
 * Search-by-ID post-loadReleaseGroup hook.
 *
 * Now that the master/release-group's child .release rows are in the
 * DOM, find the one matching state.searchTargetId, ring it, and scroll
 * it into view. No-op when the search-by-ID flow isn't active or the
 * target leaf isn't a child of this group (e.g. compare view rendering
 * a different group into its own targetEl).
 *
 * @param {HTMLElement} relEl - The .releases container that just rendered.
 */
function applySearchTargetAfterReleases(relEl) {
  const targetId = state.searchTargetId;
  if (!targetId) return;
  if (state.searchTargetSource && state.browseSource !== state.searchTargetSource) return;
  const row = /** @type {HTMLElement|null} */ (
    relEl.querySelector(`.release[data-release-id="${cssEscape(targetId)}"]`));
  if (!row) return;
  row.classList.add('search-target');
  row.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

/**
 * Add a release to the pipeline.
 * @param {string} mbid - MusicBrainz release ID
 * @param {HTMLButtonElement} btn - The clicked button
 */
export async function addRelease(mbid, btn) {
  const releaseId = normalizeReleaseId(mbid);
  btn.disabled = true;
  btn.textContent = '...';
  try {
    const requestId = releaseId || mbid;
    const idField = detectSource(requestId) === 'discogs' ? 'discogs_release_id' : 'mb_release_id';
    const r = await fetch(`${API}/api/pipeline/add`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[idField]: requestId}),
    });
    const data = await r.json();
    if (data.status === 'added') {
      btn.textContent = 'Added';
      invalidateBrowseArtist();
      // Adding a request may bring a new release-group into the
      // active set — invalidate so the inverted Replace button on
      // sibling pressings flips from disabled to enabled on next
      // render.
      invalidateActiveRgs();
      updatePipelineStatus(requestId, 'wanted', data.id);
      toast(`Added: ${data.artist} - ${data.album} (${data.tracks} tracks)`);
    } else if (data.status === 'exists') {
      if (data.current_status === 'wanted' && data.id) {
        btn.textContent = 'Remove';
        btn.disabled = false;
        btn.style.background = '#5a2a2a';
        btn.style.color = '#f88';
        btn.onclick = (e) => { e.stopPropagation(); window.disambRemove(data.id, btn); };
      } else {
        btn.textContent = data.current_status;
      }
      // R33 / U10: surface the "previously abandoned" warning with a
      // forward-link to the active descendant when the existing row is
      // a frozen audit row from a past Replace.
      if (data.current_status === 'replaced' && data.descendant_request_id) {
        toast(
          `This MBID was previously abandoned via Replace. ` +
          `Active request: #${data.descendant_request_id} ` +
          `(${data.descendant_status || 'unknown'}).`,
          true,
        );
      } else {
        toast(`Already in pipeline (${data.current_status})`);
      }
    } else {
      btn.textContent = 'Error';
      toast(data.error || 'Unknown error', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Request failed', true);
  }
}

/**
 * Render the release-detail body — tracks, label links, external link,
 * and acquire/remove action buttons — into a target element.
 *
 * Pure render: takes a fetched release payload (from /api/release/<mbid>
 * or /api/discogs/release/<id>) plus the canonical release ID, writes
 * innerHTML, returns nothing. Reused by `toggleReleaseDetail` (the
 * artist-view expand path) and by the search-by-ID VA fallback card
 * (`web/js/browse.js` resolveAndNavigate's VA branch — U5).
 *
 * Behavioural equivalence with the prior inline version is the explicit
 * invariant: same input + no opts → same innerHTML.
 *
 * @param {HTMLElement} targetEl
 * @param {string} releaseId - The canonical release ID (already normalized).
 * @param {Object} data - Release payload from the API.
 * @param {Object} [opts]
 * @param {string} [opts.artist] - Explicit artist name override. Used by
 *   the VA fallback to bypass the `state.browseArtist?.name` fallback,
 *   which on the VA path points at whatever the user previously
 *   navigated to (or null) rather than "Various Artists". Artist-view
 *   callers omit it and keep the original fallback chain.
 */
export function renderReleaseDetail(targetEl, releaseId, data, opts = {}) {
  let html = '';

  // Use beets tracks if owned (has bitrate info), otherwise MB tracks
  const hasBeets = data.beets_tracks && data.beets_tracks.length > 0;
  const tracks = hasBeets ? data.beets_tracks : (data.tracks || []);

  if (tracks.length > 0) {
    html += '<div style="margin-bottom:6px;color:#666;font-size:0.8em;">Tracks (' + tracks.length + ')' + (hasBeets ? ' — from library' : '') + '</div>';
    html += tracks.map(t => hasBeets ? renderBeetsTrackRow(t) : renderExpectedTrackRow(t)).join('');
  }

  // Label links (U7) — Discogs releases carry `labels: [{id, name}]`;
  // MB releases don't surface labels through the route layer in v1.
  const labelLinksHtml = renderLabelLinks(data.labels);
  if (labelLinksHtml) {
    html += `<div class="release-labels" style="margin:4px 0;font-size:0.85em;color:#aaa;">`
      + `<span style="color:#666;margin-right:6px;">Label:</span>${labelLinksHtml}</div>`;
  }

  // Links and actions
  html += '<div class="release-links">';
  const externalUrl = externalReleaseUrl(releaseId);
  const label = sourceLabel(releaseId);
  if (externalUrl && label) {
    html += `<a href="${externalUrl}" target="_blank" rel="noopener" style="color:#6af;font-size:0.85em;" onclick="event.stopPropagation()">${label}</a>`;
  }
  const actionState = buildReleaseActionState({
    id: releaseId,
    in_library: data.in_library,
    beets_album_id: data.beets_album_id,
    pipeline_status: data.pipeline_status,
    pipeline_id: data.pipeline_id,
    artist: opts.artist || data.artist_name || state.browseArtist?.name || '',
    album: data.title || '',
    track_count: tracks.length,
  });
  html += renderAcquireActionButton(actionState, {
    addLabel: 'Add to pipeline',
    stopPropagation: true,
    hideDisabled: true,
  });
  html += renderRemoveFromBeetsButton(actionState, {
    stopPropagation: true,
    hideDisabled: true,
  });
  html += '</div>';

  // Deep library detail (path, download history, status / min-bitrate /
  // intent controls) — absorbed from the old Library sub-view (#575 PR4).
  // Lazy: fetches /api/beets/album/<id> on first open.
  if (data.beets_album_id) {
    html += `
      <div class="type-header" style="margin-top:8px;padding:4px 0;" onclick="event.stopPropagation(); window.toggleReleaseLibDetail(${Number(data.beets_album_id)})">
        Library detail <span class="type-count">history · controls</span>
      </div>
      <div class="lib-detail" id="libdet-${Number(data.beets_album_id)}"></div>`;
  }

  targetEl.innerHTML = html;
}

/**
 * Toggle release detail panel (tracks, links, actions).
 * Wraps `renderReleaseDetail` with the fetch + open/close + error
 * handling. The render itself is shared with the search-by-ID VA
 * fallback card (U5).
 *
 * @param {string} mbid - MusicBrainz release ID or Discogs release ID
 */
export async function toggleReleaseDetail(mbid) {
  const releaseId = normalizeReleaseId(mbid) || mbid;
  const el = document.getElementById('reldet-' + mbid);
  await toggleExpand(el, async (target) => {
    const isDiscogs = detectSource(releaseId) === 'discogs';
    const url = isDiscogs ? `${API}/api/discogs/release/${releaseId}` : `${API}/api/release/${releaseId}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    renderReleaseDetail(target, releaseId, data);
  });
}
