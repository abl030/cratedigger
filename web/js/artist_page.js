// @ts-check
/**
 * Unified artist page (issue #575 PR4).
 *
 * One scrolling page replaces the old Discography / Analysis / Library /
 * Compare sub-tabs. The fast pair (source discography + library feed)
 * renders immediately, sectioned into:
 *
 *   In library / In flight / Missing / Appearances / Bootleg-only
 *
 * All release-group rows share renderRgRow (badges, expansion target,
 * data-rg-id), so search-by-ID ringing and the sibling-pressing Replace
 * flow work in every section. Two slow feeds decorate the page after it
 * renders, without re-rendering (expansion state survives):
 *
 *   - /api/artist/compare → an appended "Only on <other source>" section
 *     (the deduped complement bucket; MB-preferred merge).
 *   - /api/artist/<id>/disambiguate → unique-track chips on rows and
 *     colour-dot recordings breakdowns inside expansions (analysis.js).
 *
 * Sectioning invariants (see tests/test_js_artist_page.mjs):
 *   I1 every release-group lands in exactly one of {inLibrary, missing,
 *      appearances, bootlegs}; I2 bootleg precedence; I3 ownership via
 *      the credit rules; I4 in_library === true is the only inLibrary
 *      ticket; I5 inFlight is a lens over the library feed
 *      (downloading/manual only — "wanted" is ambient after the
 *      full-library backfill and stays a badge); I6 no owned album is
 *      invisible — a library-feed row whose release group is absent
 *      from the source discography renders as an orphan album row
 *      inside the In library section.
 */
import { renderTypedSections } from './grouping.js';
import { renderRgRow } from './discography.js';
import { renderLibraryAlbumRow } from './library.js';

/**
 * Split the fast pair into the page's sections.
 * @param {{artistId: string, artistName: string,
 *          releaseGroups: Array<Object>, libraryAlbums: Array<Object>}} input
 * @returns {{inLibrary: Object[], inFlight: Object[], missing: Object[],
 *            appearances: Object[], bootlegs: Object[]}}
 */
export function classifyArtistRows({ artistId, artistName, releaseGroups, libraryAlbums }) {
  const nameLC = (artistName || '').toLowerCase();
  const inLibrary = [], missing = [], appearances = [], bootlegs = [];
  for (const rg of releaseGroups || []) {
    const credit = (rg.artist_credit || '').toLowerCase();
    const isOwn = rg.primary_artist_id === artistId
      || credit === nameLC || credit.startsWith(nameLC + ' /') || credit.startsWith(nameLC + ',') || !credit;
    if (!rg.has_official) {
      bootlegs.push(rg);
    } else if (!isOwn) {
      appearances.push(rg);
    } else if (rg.in_library === true) {
      inLibrary.push(rg);
    } else {
      missing.push(rg);
    }
  }
  const inFlight = (libraryAlbums || []).filter(
    a => a.pipeline_status === 'downloading' || a.pipeline_status === 'manual');
  // I6 — owned albums whose release group never made it into the source
  // discography (MB lacks the release, or the backend's title-fallback
  // match missed). Without this they'd be invisible on the whole page.
  // The title checks approximate the backend fallback so an album whose
  // rg row DID render (under a different rg id) isn't shown twice.
  const rgIds = new Set((releaseGroups || []).map(r => String(r.id)));
  const inLibTitles = new Set(inLibrary.map(r => (r.title || '').toLowerCase()));
  const inLibraryOrphans = (libraryAlbums || []).filter(a =>
    a.in_library !== false
    && !(a.mb_releasegroupid && rgIds.has(String(a.mb_releasegroupid)))
    && !inLibTitles.has((a.album || '').toLowerCase())
    && !inLibTitles.has((a.release_group_title || '').toLowerCase()));
  return { inLibrary, inLibraryOrphans, inFlight, missing, appearances, bootlegs };
}

/**
 * A collapsible page section: type-header (count) + type-body.
 * @param {string} title
 * @param {number} count
 * @param {string} bodyHtml
 * @param {{open?: boolean, color?: string, id?: string}} [opts]
 * @returns {string}
 */
function sectionWrap(title, count, bodyHtml, opts = {}) {
  const style = opts.color ? ` style="color:${opts.color};"` : '';
  const idAttr = opts.id ? ` id="${opts.id}"` : '';
  return `
    <div class="type-section"${idAttr}>
      <div class="type-header section-header" onclick="event.stopPropagation(); window.toggleSection(this)"${style}>
        ${title} <span class="type-count">${count}</span>
      </div>
      <div class="type-body${opts.open ? ' open' : ''}">
        ${bodyHtml}
      </div>
    </div>
  `;
}

/**
 * Render the unified artist page body. Empty sections are omitted.
 * @param {{inLibrary: Object[], inFlight: Object[], missing: Object[],
 *          appearances: Object[], bootlegs: Object[]}} sections
 * @param {{artistId: string, artistName: string}} ctx
 * @returns {string}
 */
export function renderArtistSections(sections, ctx) {
  const nameLC = (ctx.artistName || '').toLowerCase();
  const rgRow = (rg) => renderRgRow(rg, { artistName: ctx.artistName, nameLC });
  const typed = (rgs, defaultOpen) => renderTypedSections(rgs, rgRow,
    { defaultOpen: defaultOpen ? 'Albums' : null });

  let html = '';
  const orphans = sections.inLibraryOrphans || [];
  if (sections.inLibrary.length > 0 || orphans.length > 0) {
    let body = sections.inLibrary.length > 0 ? typed(sections.inLibrary, true) : '';
    if (orphans.length > 0) {
      body += `
        <div class="type-header" style="color:#777;margin-top:6px;cursor:default;" onclick="event.stopPropagation()">
          Library-only editions <span class="type-count">${orphans.length}</span>
        </div>
        ${orphans.map(renderLibraryAlbumRow).join('')}`;
    }
    html += sectionWrap('In library', sections.inLibrary.length + orphans.length,
      body, { open: true });
  }
  if (sections.inFlight.length > 0) {
    html += sectionWrap('In flight', sections.inFlight.length,
      sections.inFlight.map(renderLibraryAlbumRow).join(''), { open: true });
  }
  if (sections.missing.length > 0) {
    html += sectionWrap('Missing', sections.missing.length,
      typed(sections.missing, true), { open: true });
  }
  if (sections.appearances.length > 0) {
    html += sectionWrap('Appearances', sections.appearances.length,
      typed(sections.appearances, false), { color: '#777' });
  }
  if (sections.bootlegs.length > 0) {
    html += sectionWrap('Bootleg-only releases', sections.bootlegs.length,
      typed(sections.bootlegs, false), { color: '#555' });
  }
  return html;
}

/**
 * The late-appended complement section: release groups that exist only
 * on the OTHER metadata source (compare's deduped mb_only/discogs_only
 * bucket). Rows force loadReleaseGroup onto the complement source.
 * @param {Object[]} rows - rg-shaped rows from the compare bucket
 * @param {{artistName: string, source: 'mb'|'discogs'}} ctx
 * @returns {string} '' when the bucket is empty
 */
export function renderOtherSourceSection(rows, ctx) {
  if (!rows || rows.length === 0) return '';
  const nameLC = (ctx.artistName || '').toLowerCase();
  const label = ctx.source === 'discogs' ? 'Discogs' : 'MusicBrainz';
  const rgRow = (rg) => renderRgRow(rg, {
    artistName: ctx.artistName, nameLC, source: ctx.source,
  });
  return sectionWrap(`Only on ${label}`, rows.length,
    renderTypedSections(rows, rgRow, {}),
    { color: '#a96', id: 'only-other-source' });
}
