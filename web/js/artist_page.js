// @ts-check
/** Unified artist-page semantic catalogue and simple #601 presentation. */
import { renderTypedSections } from './grouping.js';
import { renderRgRow } from './discography.js';
import { renderLibraryAlbumRow } from './library.js';


/** @param {Object} row @returns {Set<string>} */
function displayProvenance(row) {
  return new Set(row.display_provenance || row.provenance || []);
}


/**
 * Project one diagnostic pair onto the operator-selected source identity.
 * The counterpart contributes display classification and an honest presence
 * hint only; it never overwrites the selected row's exact ownership, request,
 * source, identity kind, or id.
 * @param {Object} selected
 * @param {Object} counterpart
 * @returns {Object}
 */
function projectPair(selected, counterpart) {
  return {
    ...selected,
    display_provenance: Array.from(new Set([
      ...(selected.provenance || []),
      ...(counterpart.provenance || []),
    ])),
    counterpart: { ...counterpart },
  };
}


/**
 * Turn compare diagnostics into one deduped musical catalogue.
 *
 * A paired work renders once using the selected source identity. Unpaired rows
 * from both sources remain visible as ordinary catalogue rows; their pairing
 * or master topology never becomes a page section. Exact source/kind/id is
 * preserved on every returned row.
 * @param {Object} compare
 * @param {'mb'|'discogs'} source
 * @returns {Object[]}
 */
export function composeCompareCatalogue(compare, source) {
  /** @type {Object[]} */
  const result = [];
  const seen = new Set();
  const add = (row) => {
    if (!row) return;
    const key = `${row.source}:${row.identity_kind}:${row.id}`;
    if (seen.has(key)) return;
    seen.add(key);
    result.push(row);
  };

  for (const pair of compare.both || []) {
    const selected = source === 'mb' ? pair.mb : pair.discogs;
    const counterpart = source === 'mb' ? pair.discogs : pair.mb;
    add(projectPair(selected, counterpart));
  }

  if (source === 'mb') {
    for (const row of compare.mb_unpaired || []) add(row);
    for (const row of compare.discogs_unpaired || []) add(row);
    for (const row of compare.discogs_ungrouped_releases || []) add(row);
  } else {
    for (const row of compare.discogs_unpaired || []) add(row);
    for (const row of compare.discogs_ungrouped_releases || []) add(row);
    for (const row of compare.mb_unpaired || []) add(row);
  }
  return result;
}


/**
 * Split one semantic catalogue into the original simple page model.
 * Ordinary own rows go to exact In library or Missing. Everything
 * exceptional, unknown, or appearance-shaped goes to one collapsed Other
 * releases area. Masterless Discogs releases are ordinary catalogue rows.
 * @param {{artistId:string, artistName:string, releaseGroups:Object[],
 *          ungroupedReleases?:Object[], libraryAlbums:Object[]}} input
 * @returns {Object}
 */
export function classifyArtistRows({
  artistId, artistName, releaseGroups, ungroupedReleases = [], libraryAlbums,
}) {
  const rows = [...(releaseGroups || []), ...(ungroupedReleases || [])];
  const nameLC = (artistName || '').toLowerCase();
  const inLibrary = [], missing = [], otherReleases = [];
  for (const row of rows) {
    const credit = (row.artist_credit || '').toLowerCase();
    const isOwn = row.primary_artist_id === artistId
      || credit === nameLC || credit.startsWith(nameLC + ' /')
      || credit.startsWith(nameLC + ',') || !credit;
    const provenance = displayProvenance(row);
    if (row.is_appearance === true || !isOwn || !provenance.has('ordinary')) {
      otherReleases.push(row);
    } else {
      (row.in_library === true ? inLibrary : missing).push(row);
    }
  }

  const inFlight = (libraryAlbums || []).filter(
    album => album.pipeline_status === 'downloading'
      || album.pipeline_status === 'manual');

  // Suppress the library-feed fallback only through exact identities from
  // the displayed row or its associated counterpart. A counterpart can
  // explain the owned edition without claiming that the selected pressing is
  // owned. Titles never participate.
  const identities = rows.flatMap(row => [row, row.counterpart].filter(Boolean));
  const mbWorkIds = new Set(identities
    .filter(row => row.source === 'mb' && row.identity_kind === 'work')
    .map(row => String(row.id)));
  const discogsReleaseIds = new Set(identities
    .filter(row => row.source === 'discogs' && row.identity_kind === 'release')
    .map(row => String(row.id)));
  const inLibraryOrphans = (libraryAlbums || []).filter(album =>
    album.in_library !== false
    && !(album.mb_releasegroupid
      && mbWorkIds.has(String(album.mb_releasegroupid)))
    && !(album.mb_albumid
      && discogsReleaseIds.has(String(album.mb_albumid))));

  return {
    inLibrary, inLibraryOrphans, inFlight, missing, otherReleases,
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


/** Render the simple four-area artist page. @param {Object} sections
 * @param {{artistId:string,artistName:string}} ctx @returns {string} */
export function renderArtistSections(sections, ctx) {
  const nameLC = (ctx.artistName || '').toLowerCase();
  const rowRenderer = row => renderRgRow(row, {
    artistName: ctx.artistName, nameLC,
  });
  const typed = (rows, defaultOpen) => renderTypedSections(
    rows, rowRenderer, { defaultOpen },
  );

  let html = '';
  const orphans = sections.inLibraryOrphans || [];
  if (sections.inLibrary.length || orphans.length) {
    let body = sections.inLibrary.length
      ? typed(sections.inLibrary, 'Albums') : '';
    if (orphans.length) {
      body += `
        <div class="type-header" style="color:#777;margin-top:6px;cursor:default;" onclick="event.stopPropagation()">
          Library-only editions <span class="type-count">${orphans.length}</span>
        </div>
        ${orphans.map(renderLibraryAlbumRow).join('')}`;
    }
    html += sectionWrap(
      'In library', sections.inLibrary.length + orphans.length, body,
      { open: true, id: 'catalogue-in-library' },
    );
  }
  if (sections.inFlight.length) {
    html += sectionWrap(
      'In flight', sections.inFlight.length,
      sections.inFlight.map(renderLibraryAlbumRow).join(''),
      { open: true, id: 'catalogue-in-flight' },
    );
  }
  if (sections.missing.length) {
    html += sectionWrap(
      'Missing', sections.missing.length, typed(sections.missing, 'Albums'),
      { open: true, id: 'catalogue-missing' },
    );
  }
  if (sections.otherReleases.length) {
    html += sectionWrap(
      'Other releases', sections.otherReleases.length,
      typed(sections.otherReleases, null),
      { color: '#777', id: 'catalogue-other-releases' },
    );
  }
  return html;
}
