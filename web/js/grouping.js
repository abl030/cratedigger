// @ts-check

/**
 * Shared release-grouping helpers used by the unified artist page and
 * the label view. Centralises the
 * Albums/EPs/Singles/Compilations/Live/Remixes/DJ Mixes/Demos/Other
 * sectioning so every view sorts the same way.
 *
 * Tolerates row shapes from different sources:
 *   - Normalized artist rows: `primary_types[]` + source qualifiers
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
  const secondaryTypes = row.display_secondary_types ?? row.secondary_types ?? [];
  const formatQualifiers = row.display_format_qualifiers ?? row.format_qualifiers ?? [];
  const qualifiers = [
    ...secondaryTypes,
    ...formatQualifiers,
  ];
  if (qualifiers.includes('Compilation')) return 'Compilations';
  if (qualifiers.includes('Live')) return 'Live';
  if (qualifiers.includes('Remix')) return 'Remixes';
  if (qualifiers.includes('DJ-mix')) return 'DJ Mixes';
  if (qualifiers.includes('Demo')) return 'Demos';
  if (row.display_primary_types !== undefined || row.primary_types !== undefined) {
    const structural = row.display_primary_types ?? row.primary_types ?? [];
    if (structural.includes('Album')) return 'Albums';
    if (structural.includes('EP')) return 'EPs';
    if (structural.includes('Single')) return 'Singles';
    return 'Other';
  }
  if (qualifiers.length > 0) return 'Other';
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
 * @param {string[]} [opts.openSections] - Exact sections to open. When
 *   supplied, this takes precedence over defaultOpen and may name more than
 *   one section.
 * @param {string} [opts.headerStyle] - Inline style on the type-header
 *   div. Useful for muting a "bootleg" group.
 * @returns {string}
 */
export function renderTypedSections(rows, renderRow, opts = {}) {
  const classifier = opts.classify || classify;
  const dateOf = opts.dateOf || ((r) => String(r.first_release_date || ''));
  const defaultOpen = opts.defaultOpen === undefined ? 'Albums' : opts.defaultOpen;
  const openSections = opts.openSections
    ? new Set(opts.openSections)
    : null;
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
      const isOpen = openSections ? openSections.has(s) : s === defaultOpen;
      const hStyle = headerStyle ? ` style="${headerStyle}"` : '';
      return `
        <div class="type-section">
          <div class="type-header" onclick="event.stopPropagation(); window.toggleSection(this)"${hStyle}>
            ${s} <span class="type-count">${items.length}</span>
          </div>
          <div class="type-body${isOpen ? ' open' : ''}">
            ${items.map(renderRow).join('')}
          </div>
        </div>`;
    })
    .join('');
}
