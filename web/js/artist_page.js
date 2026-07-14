// @ts-check
/** Unified artist-page semantic partitioning and rendering. */
import { classify, renderTypedSections, SECTION_ORDER } from './grouping.js';
import { renderRgRow } from './discography.js';
import { renderLibraryAlbumRow } from './library.js';


/**
 * @param {Object} row
 * @returns {Set<string>}
 */
function provenanceSet(row) {
  return new Set(row.provenance || []);
}


/**
 * Partition work rows by source-authored provenance without title guesses.
 * Appearances remain distinct from both mainline and exceptional own works.
 * Mixed works with positive ordinary evidence remain mainline and carry their
 * exceptional provenance chips on the row.
 * @param {Object[]} rows
 * @returns {{mainline:Object[], appearances:Object[], promoOnly:Object[],
 *            unofficialOnly:Object[], unknown:Object[]}}
 */
export function partitionWorkRows(rows) {
  const result = {
    mainline: [], appearances: [], promoOnly: [], unofficialOnly: [], unknown: [],
  };
  for (const row of rows || []) {
    if (row.is_appearance === true) {
      result.appearances.push(row);
      continue;
    }
    const provenance = provenanceSet(row);
    if (provenance.has('ordinary')) result.mainline.push(row);
    else if (provenance.has('unofficial')) result.unofficialOnly.push(row);
    else if (provenance.has('promo')) result.promoOnly.push(row);
    else result.unknown.push(row);
  }
  return result;
}


/**
 * Split the fast artist/library pair into mutually exclusive work sections.
 * Discogs release units are supplied separately and never enter work buckets.
 * @param {{artistId:string, artistName:string, releaseGroups:Object[],
 *          ungroupedReleases?:Object[], libraryAlbums:Object[]}} input
 * @returns {Object}
 */
export function classifyArtistRows({
  artistId, artistName, releaseGroups, ungroupedReleases = [], libraryAlbums,
}) {
  const nameLC = (artistName || '').toLowerCase();
  const inLibrary = [], missing = [], appearances = [];
  const promoOnly = [], unofficialOnly = [], unknownProvenance = [];
  for (const row of releaseGroups || []) {
    const credit = (row.artist_credit || '').toLowerCase();
    const isOwn = row.primary_artist_id === artistId
      || credit === nameLC || credit.startsWith(nameLC + ' /')
      || credit.startsWith(nameLC + ',') || !credit;
    if (row.is_appearance === true || !isOwn) {
      appearances.push(row);
      continue;
    }
    const provenance = provenanceSet(row);
    if (!provenance.has('ordinary')) {
      if (provenance.has('unofficial')) unofficialOnly.push(row);
      else if (provenance.has('promo')) promoOnly.push(row);
      else unknownProvenance.push(row);
      continue;
    }
    (row.in_library === true ? inLibrary : missing).push(row);
  }

  const inFlight = (libraryAlbums || []).filter(
    album => album.pipeline_status === 'downloading'
      || album.pipeline_status === 'manual');

  // Suppress library editions only through exact identity. Title collisions
  // never hide a curated edition or fabricate cross-source ownership.
  const workIds = new Set((releaseGroups || []).map(row => String(row.id)));
  const releaseIds = new Set((ungroupedReleases || [])
    .filter(row => row.in_library === true)
    .map(row => String(row.id)));
  const inLibraryOrphans = (libraryAlbums || []).filter(album =>
    album.in_library !== false
    && !(album.mb_releasegroupid
      && workIds.has(String(album.mb_releasegroupid)))
    && !(album.mb_albumid && releaseIds.has(String(album.mb_albumid))));

  return {
    inLibrary, inLibraryOrphans, inFlight, missing, appearances,
    promoOnly, unofficialOnly, unknownProvenance,
    ungroupedReleases: ungroupedReleases || [],
  };
}


/** @param {string} title @param {number} count @param {string} bodyHtml
 * @param {{open?:boolean,color?:string,id?:string}} [opts] @returns {string} */
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


/** @param {Object[]} rows @returns {string[]} */
export function ownedTypeSections(rows) {
  const owned = new Set((rows || [])
    .filter(row => row.in_library === true)
    .map(classify));
  return SECTION_ORDER.filter(section => owned.has(section));
}


/**
 * @param {string} title @param {Object[]} rows
 * @param {(row:Object)=>string} rowRenderer @param {string} color
 */
function renderCollapsedWorkSection(title, rows, rowRenderer, color) {
  if (!rows.length) return '';
  const ownedTypes = ownedTypeSections(rows);
  return sectionWrap(title, rows.length, renderTypedSections(rows, rowRenderer, {
    defaultOpen: null,
    openSections: ownedTypes,
  }), { color, open: ownedTypes.length > 0 });
}


/**
 * @param {Object[]} rows @param {(row:Object)=>string} rowRenderer
 * @param {string} label
 */
function renderProvenanceSections(rows, rowRenderer, label) {
  const split = partitionWorkRows(rows);
  let html = '';
  if (split.mainline.length) {
    const owned = ownedTypeSections(split.mainline);
    html += renderTypedSections(split.mainline, rowRenderer, {
      defaultOpen: null, openSections: owned,
    });
  }
  html += renderCollapsedWorkSection(
    `Appears on${label}`, split.appearances, rowRenderer, '#777');
  html += renderCollapsedWorkSection(
    `Promo-only${label}`, split.promoOnly, rowRenderer, '#8a7040');
  html += renderCollapsedWorkSection(
    `Unofficial-only${label}`, split.unofficialOnly, rowRenderer, '#777');
  html += renderCollapsedWorkSection(
    `Unknown provenance${label}`, split.unknown, rowRenderer, '#777');
  return html;
}


/** Render the fast primary-source artist page. @param {Object} sections
 * @param {{artistId:string,artistName:string}} ctx @returns {string} */
export function renderArtistSections(sections, ctx) {
  const nameLC = (ctx.artistName || '').toLowerCase();
  const rowRenderer = row => renderRgRow(row, {
    artistName: ctx.artistName, nameLC,
  });
  const typed = (rows, defaultOpen, openSections) => renderTypedSections(
    rows, rowRenderer, {
      defaultOpen: defaultOpen ? 'Albums' : null,
      ...(openSections ? { openSections } : {}),
    });

  let html = '';
  const orphans = sections.inLibraryOrphans || [];
  if (sections.inLibrary.length || orphans.length) {
    let body = sections.inLibrary.length ? typed(sections.inLibrary, true) : '';
    if (orphans.length) {
      body += `
        <div class="type-header" style="color:#777;margin-top:6px;cursor:default;" onclick="event.stopPropagation()">
          Library-only editions <span class="type-count">${orphans.length}</span>
        </div>
        ${orphans.map(renderLibraryAlbumRow).join('')}`;
    }
    html += sectionWrap('In library', sections.inLibrary.length + orphans.length,
      body, { open: true });
  }
  if (sections.inFlight.length) {
    html += sectionWrap('In flight', sections.inFlight.length,
      sections.inFlight.map(renderLibraryAlbumRow).join(''), { open: true });
  }
  if (sections.missing.length) {
    html += sectionWrap('Missing', sections.missing.length,
      typed(sections.missing, true), { open: true });
  }
  html += renderCollapsedWorkSection(
    'Appearances', sections.appearances, rowRenderer, '#777');
  html += renderCollapsedWorkSection(
    'Promo-only works', sections.promoOnly, rowRenderer, '#8a7040');
  html += renderCollapsedWorkSection(
    'Unofficial-only works', sections.unofficialOnly, rowRenderer, '#555');
  html += renderCollapsedWorkSection(
    'Unknown-provenance works', sections.unknownProvenance, rowRenderer, '#777');

  const ungrouped = sections.ungroupedReleases || [];
  if (ungrouped.length) {
    const ownedTypes = ownedTypeSections(ungrouped);
    html += sectionWrap(
      'Ungrouped Discogs releases',
      ungrouped.length,
      renderProvenanceSections(ungrouped, rowRenderer, ' releases'),
      {
        color: '#777',
        id: 'ungrouped-discogs-releases',
        open: ownedTypes.length > 0,
      },
    );
  }
  return html;
}


/**
 * Render the other source's unpaired work units and, when Discogs is the
 * other source, its separately conserved ungrouped release units.
 * @param {Object[]} rows @param {Object[]} ungroupedRows
 * @param {{artistName:string,source:'mb'|'discogs'}} ctx @returns {string}
 */
export function renderUnpairedSourceSections(rows, ungroupedRows, ctx) {
  const nameLC = (ctx.artistName || '').toLowerCase();
  const label = ctx.source === 'discogs' ? 'Discogs' : 'MusicBrainz';
  const rowRenderer = row => renderRgRow(row, {
    artistName: ctx.artistName, nameLC, source: ctx.source,
  });
  let html = '';
  if (rows && rows.length) {
    const owned = ownedTypeSections(rows);
    html += sectionWrap(
      `Unpaired ${label} works`, rows.length,
      renderProvenanceSections(rows, rowRenderer, ' works'),
      {
        color: '#a96', id: 'unpaired-other-source', open: owned.length > 0,
      },
    );
  }
  if (ungroupedRows && ungroupedRows.length) {
    const owned = ownedTypeSections(ungroupedRows);
    html += sectionWrap(
      'Ungrouped Discogs releases', ungroupedRows.length,
      renderProvenanceSections(ungroupedRows, rowRenderer, ' releases'),
      {
        color: '#777', id: 'ungrouped-discogs-releases',
        open: owned.length > 0,
      },
    );
  }
  return html;
}
