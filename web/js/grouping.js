// @ts-check

/**
 * Shared release-grouping helpers used by every browse-artist sub-tab
 * (Discography / Library / Analysis / Compare). Centralises the
 * Albums/EPs/Singles/Compilations/Live/Remixes/DJ Mixes/Demos/Other
 * sectioning so every view sorts the same way.
 *
 * Tolerates row shapes from different sources:
 *   - Discography RGs    : `type` + `secondary_types[]`
 *   - Analysis RGs       : `primary_type`
 *   - Library albums     : `type` (lowercase strings from beets albumtype)
 *   - Compare pairs      : `{mb, discogs}` — caller passes a custom
 *                          classifier that picks one side.
 */

/** @type {string[]} */
export const SECTION_ORDER = [
  'Albums', 'EPs', 'Singles', 'Compilations', 'Live',
  'Remixes', 'DJ Mixes', 'Demos', 'Other',
];

/**
 * Classify a release-group / album row into its display section.
 * @param {Object} row
 * @returns {string}
 */
export function classify(row) {
  const st = row.secondary_types || [];
  if (st.includes('Compilation')) return 'Compilations';
  if (st.includes('Live')) return 'Live';
  if (st.includes('Remix')) return 'Remixes';
  if (st.includes('DJ-mix')) return 'DJ Mixes';
  if (st.includes('Demo')) return 'Demos';
  if (st.length > 0) return 'Other';
  const t = String(row.primary_type || row.type || '').toLowerCase();
  if (t === 'album') return 'Albums';
  if (t === 'ep') return 'EPs';
  if (t === 'single') return 'Singles';
  if (t === 'compilation' || t === 'soundtrack') return 'Compilations';
  if (t === 'live') return 'Live';
  return 'Other';
}

/**
 * Render rows grouped into typed collapsible sections, sorted by date
 * within each section. Section order matches SECTION_ORDER.
 *
 * @param {Object[]} rows
 * @param {(row: Object) => string} renderRow - HTML for one row
 * @param {Object} [opts]
 * @param {(row: Object) => string} [opts.classify] - Override classifier
 *   (defaults to classify()). Use when row is a wrapper like compare's
 *   {mb, discogs}.
 * @param {(row: Object) => string} [opts.dateOf] - Date-extractor for
 *   intra-section sorting. Defaults to row.first_release_date.
 * @param {string|null} [opts.defaultOpen] - Section name to open by
 *   default. Pass null for none. Default 'Albums'.
 * @param {string} [opts.headerStyle] - Inline style on the type-header
 *   div. Useful for muting a "bootleg" group.
 * @returns {string}
 */
export function renderTypedSections(rows, renderRow, opts = {}) {
  const classifier = opts.classify || classify;
  const dateOf = opts.dateOf || ((r) => String(r.first_release_date || ''));
  const defaultOpen = opts.defaultOpen === undefined ? 'Albums' : opts.defaultOpen;
  const headerStyle = opts.headerStyle || '';

  /** @type {Object<string, Object[]>} */
  const sections = {};
  for (const row of rows) {
    const sec = classifier(row);
    if (!sections[sec]) sections[sec] = [];
    sections[sec].push(row);
  }
  for (const sec of Object.values(sections)) {
    sec.sort((a, b) => dateOf(a).localeCompare(dateOf(b)));
  }
  return SECTION_ORDER
    .filter((s) => sections[s])
    .map((s) => {
      const items = sections[s];
      const isOpen = s === defaultOpen;
      const hStyle = headerStyle ? ` style="${headerStyle}"` : '';
      return `
        <div class="type-section">
          <div class="type-header" onclick="event.stopPropagation(); this.nextElementSibling.classList.toggle('open')"${hStyle}>
            ${s} <span class="type-count">${items.length}</span>
          </div>
          <div class="type-body${isOpen ? ' open' : ''}">
            ${items.map(renderRow).join('')}
          </div>
        </div>`;
    })
    .join('');
}
