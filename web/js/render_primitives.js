// @ts-check
/**
 * Shared render primitives (issue #575 PR3).
 *
 * One place for the row / track / detail-row markup and the
 * expand-on-click mechanics that were previously copy-pasted per view
 * (discography, library, analysis, labels, pipeline). Pure string
 * builders except the two toggles, which touch only the element passed
 * in — both stay Node-testable with a stub element.
 */
import { esc, externalReleaseUrl, sourceLabel } from './util.js';

/**
 * Format a track duration in seconds as m:ss. Rounds total seconds
 * before splitting so 119.7s renders 2:00 (the old inline copies did
 * Math.round(len % 60) and could render 1:60).
 * @param {number|null|undefined} seconds
 * @returns {string} 'm:ss', or '' when missing/zero
 */
export function formatDuration(seconds) {
  if (!seconds) return '';
  const total = Math.round(seconds);
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, '0')}`;
}

/**
 * Format the per-track quality summary from beets fields:
 * 'FLAC 1024kbps 24bit 96.0kHz'. CD-spec depth (≤16bit) and sample rate
 * (≤44.1kHz) are suppressed as noise.
 * @param {{format?: string, bitrate?: number, bitdepth?: number, samplerate?: number}} t
 * @returns {string}
 */
export function formatTrackMeta(t) {
  const br = t.bitrate ? `${Math.round(t.bitrate / 1000)}kbps` : '';
  const depth = t.bitdepth && t.bitdepth > 16 ? `${t.bitdepth}bit` : '';
  const sr = t.samplerate && t.samplerate > 44100 ? `${(t.samplerate / 1000).toFixed(1)}kHz` : '';
  return [t.format, br, depth, sr].filter(Boolean).join(' ');
}

/**
 * One owned (beets) track row: number, title, duration, quality meta.
 * @param {{disc?: number, track?: number, title?: string, length?: number,
 *          format?: string, bitrate?: number, bitdepth?: number, samplerate?: number}} t
 * @returns {string}
 */
export function renderBeetsTrackRow(t) {
  const dur = formatDuration(t.length);
  return `<div class="lib-track">
      <span>${t.disc && t.disc > 1 ? t.disc + '.' : ''}${t.track}. ${esc(t.title)} ${dur ? '<span style="color:#555;">' + dur + '</span>' : ''}</span>
      <span class="lib-track-meta">${formatTrackMeta(t)}</span>
    </div>`;
}

/**
 * One expected (MB/Discogs) track row: number, title, duration. No
 * quality meta — these tracks aren't on disk.
 * @param {{disc_number?: number, track_number?: number, title?: string,
 *          length_seconds?: number}} t
 * @returns {string}
 */
export function renderExpectedTrackRow(t) {
  const dur = formatDuration(t.length_seconds);
  return `<div class="lib-track">
      <span>${t.disc_number && t.disc_number > 1 ? t.disc_number + '.' : ''}${t.track_number}. ${esc(t.title)} ${dur ? '<span style="color:#555;">' + dur + '</span>' : ''}</span>
    </div>`;
}

/**
 * The release/album row skeleton shared by the Browse-family views:
 *
 *   <div class="{rowClass}" [data-release-id] [style] onclick="...">
 *     <div class="release-info">
 *       <div class="release-title">{titleHtml}</div>
 *       <div class="release-meta">…</div>  (one per metaLines entry)
 *     </div>
 *     {actionsHtml}
 *   </div>
 *   <div class="{detail.className}" id="{detail.id}"></div>  (optional)
 *
 * Title, meta, and actions arrive pre-rendered (call sites own badges,
 * toolbars, and escaping of their own text) — the primitive owns the
 * structure, classes, and the trailing detail placeholder.
 *
 * @param {{
 *   rowClass?: string,
 *   dataReleaseId?: string,
 *   style?: string,
 *   onclick: string,
 *   titleHtml: string,
 *   metaLines?: string[],
 *   actionsHtml?: string,
 *   detail?: {id: string, className?: string},
 * }} vm
 * @returns {string}
 */
export function renderReleaseRow(vm) {
  const dataAttr = vm.dataReleaseId ? ` data-release-id="${esc(vm.dataReleaseId)}"` : '';
  const styleAttr = vm.style ? ` style="${vm.style}"` : '';
  const meta = (vm.metaLines || [])
    .map((line) => `<div class="release-meta" style="color:#777;">${line}</div>`)
    .join('');
  const detail = vm.detail
    ? `\n    <div class="${vm.detail.className || 'release-detail'}" id="${esc(vm.detail.id)}"></div>`
    : '';
  return `
    <div class="${vm.rowClass || 'release'}"${dataAttr}${styleAttr} onclick="${vm.onclick}">
      <div class="release-info">
        <div class="release-title">${vm.titleHtml}</div>
        ${meta}
      </div>
      ${vm.actionsHtml || ''}
    </div>${detail}`;
}

/**
 * One label/value line inside a detail panel.
 * @param {string} label - Plain text; escaped here.
 * @param {string} valueHtml - Pre-rendered value HTML.
 * @param {{valueStyle?: string}} [opts]
 * @returns {string}
 */
export function renderDetailRow(label, valueHtml, opts = {}) {
  const styleAttr = opts.valueStyle ? ` style="${opts.valueStyle}"` : '';
  return `<div class="p-detail-row"><span class="p-detail-label">${esc(label)}</span><span class="p-detail-value"${styleAttr}>${valueHtml}</span></div>`;
}

/**
 * External-source detail row: 'MusicBrainz | 9a7c2e1b...' linking out to
 * musicbrainz.org / discogs.com. Empty string when the id maps to
 * neither source.
 * @param {string} releaseId - MB UUID or Discogs numeric id.
 * @returns {string}
 */
export function renderExternalLinkRow(releaseId) {
  const label = sourceLabel(releaseId);
  const url = externalReleaseUrl(releaseId);
  if (!label || !url) return '';
  const link = `<a href="${url}" target="_blank" rel="noopener" style="color:#6af;" onclick="event.stopPropagation()">${esc(releaseId.slice(0, 8))}...</a>`;
  return renderDetailRow(label, link);
}

/**
 * Generic expand/collapse for a lazy-loaded detail panel.
 *
 * Open panel: collapse (loader not called). Closed panel: show a loading
 * placeholder, add `.open`, and call `loader(el)` to fetch + render into
 * el. A loader throw/rejection renders the error placeholder. The loader
 * runs on EVERY open — no caching — matching the previous per-view
 * implementations and required because badge overlays (pipelineStore)
 * change between opens.
 *
 * @param {{classList: {contains: (c: string) => boolean, add: (c: string) => void,
 *          remove: (c: string) => void}, innerHTML: string}|HTMLElement|null} el
 * @param {(el: any) => (Promise<void>|void)} loader
 * @param {{errorText?: string}} [opts]
 * @returns {Promise<void>}
 */
export async function toggleExpand(el, loader, opts = {}) {
  if (!el) return;
  if (el.classList.contains('open')) {
    el.classList.remove('open');
    return;
  }
  el.innerHTML = '<div class="loading" style="padding:8px;">Loading...</div>';
  el.classList.add('open');
  try {
    await loader(el);
  } catch (e) {
    el.innerHTML = `<div class="loading" style="padding:8px;">${esc(opts.errorText || 'Failed to load')}</div>`;
  }
}

/**
 * Section-header click handler: toggle `.open` on the header's next
 * sibling (the section body). Replaces the inline
 * `this.nextElementSibling.classList.toggle('open')` copies; bound as
 * `window.toggleSection` in main.js.
 * @param {{nextElementSibling: {classList: {toggle: (c: string) => void}}|null}|HTMLElement} headerEl
 */
export function toggleSection(headerEl) {
  const body = headerEl.nextElementSibling;
  if (body) body.classList.toggle('open');
}
