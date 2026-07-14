/** Unit tests for the unified artist-page semantic catalogue. */
import {
  classifyArtistRows,
  ownedTypeSections,
  partitionWorkRows,
  renderArtistSections,
  renderUnpairedSourceSections,
} from '../web/js/artist_page.js';
import { renderRgRow } from '../web/js/discography.js';

let passed = 0;
let failed = 0;

function assertEqual(actual, expected, msg) {
  if (actual === expected) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${msg} - expected '${expected}', got '${actual}'`);
  }
}
function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) passed++;
  else { failed++; console.error(`  FAIL: ${msg} - '${needle}' missing`); }
}
function assertExcludes(haystack, needle, msg) {
  if (!haystack.includes(needle)) passed++;
  else { failed++; console.error(`  FAIL: ${msg} - '${needle}' present`); }
}
function bodyIsOpenAfter(html, marker) {
  const start = html.indexOf(marker);
  if (start < 0) return false;
  const body = html.slice(start).match(/<div class="type-body([^"]*)">/);
  return Boolean(body && body[1].split(/\s+/).includes('open'));
}

const ARTIST_ID = 'aaaaaaaa-1111-2222-3333-444444444444';
const ARTIST_NAME = 'The Lucksmiths';

function work(id, overrides = {}) {
  return {
    id, title: `Work ${id}`, type: 'Album', source: 'mb',
    identity_kind: 'work', primary_types: ['Album'], secondary_types: [],
    format_qualifiers: [], provenance: ['ordinary'],
    first_release_date: '2001-05-01', artist_credit: ARTIST_NAME,
    primary_artist_id: ARTIST_ID, is_appearance: false, in_library: false,
    ...overrides,
  };
}
function release(id, overrides = {}) {
  return work(id, {
    source: 'discogs', identity_kind: 'release', discogs_release_id: id,
    ...overrides,
  });
}
function library(overrides = {}) {
  return {
    id: 1, album: 'Album', artist: ARTIST_NAME, year: 2001,
    mb_albumid: 'release-id', mb_releasegroupid: null,
    release_group_title: null, added: 0, formats: 'Opus',
    min_bitrate: 112292, avg_bitrate: 122563, type: 'album', label: '',
    country: 'AU', source: 'musicbrainz', in_library: true,
    beets_album_id: 1, pipeline_status: null, pipeline_id: null,
    upgrade_queued: false, library_rank: 'good', ...overrides,
  };
}
function classify(groups, albums = [], ungrouped = []) {
  return classifyArtistRows({
    artistId: ARTIST_ID, artistName: ARTIST_NAME,
    releaseGroups: groups, ungroupedReleases: ungrouped,
    libraryAlbums: albums,
  });
}

console.log('work partition is total, exclusive, and provenance-backed');
{
  const world = [
    work('owned', { in_library: true }),
    work('missing'),
    work('appearance', { is_appearance: true }),
    work('foreign', { primary_artist_id: 'other', artist_credit: 'Else' }),
    work('promo', { provenance: ['promo'] }),
    work('unofficial', { provenance: ['unofficial'] }),
    work('unknown', { provenance: [] }),
    work('mixed', { provenance: ['ordinary', 'unofficial'] }),
  ];
  const sections = classify(world);
  const buckets = [
    sections.inLibrary, sections.missing, sections.appearances,
    sections.promoOnly, sections.unofficialOnly, sections.unknownProvenance,
  ];
  assertEqual(
    buckets.flat().map(row => row.id).sort().join(','),
    world.map(row => row.id).sort().join(','),
    'every work appears exactly once',
  );
  assertEqual(sections.inLibrary.map(row => row.id).join(','), 'owned',
    'ordinary exact-owned work is in library');
  assertEqual(sections.missing.map(row => row.id).join(','), 'missing,mixed',
    'mixed work with ordinary evidence remains mainline');
  assertEqual(sections.appearances.map(row => row.id).join(','),
    'appearance,foreign', 'appearances remain separate');
  assertEqual(sections.promoOnly[0].id, 'promo', 'promo-only bucket');
  assertEqual(sections.unofficialOnly[0].id, 'unofficial',
    'unofficial-only bucket');
  assertEqual(sections.unknownProvenance[0].id, 'unknown',
    'unknown provenance remains reachable');
}

console.log('release units stay outside work sections');
{
  const ungrouped = [release('r1'), release('r2', { provenance: ['promo'] })];
  const sections = classify([work('w1')], [], ungrouped);
  assertEqual(sections.missing.map(row => row.id).join(','), 'w1',
    'work bucket excludes releases');
  assertEqual(sections.ungroupedReleases.map(row => row.id).join(','), 'r1,r2',
    'release units conserved explicitly');
}

console.log('library-only suppression uses exact identity, never title');
{
  const sameTitle = work('wrong-rg', { title: 'The Rolling Stones' });
  const album = library({
    album: 'The Rolling Stones', mb_releasegroupid: 'actual-rg',
    mb_albumid: 'actual-release',
  });
  const titleCollision = classify([sameTitle], [album]);
  assertEqual(titleCollision.inLibraryOrphans.length, 1,
    'same title does not hide a different edition');

  const exactGroup = classify([
    work('actual-rg', { title: 'Different typography', in_library: true }),
  ], [album]);
  assertEqual(exactGroup.inLibraryOrphans.length, 0,
    'exact MB release-group identity suppresses duplicate library row');

  const exactLeaf = classify([], [album], [
    release('actual-release', { title: 'Different typography', in_library: true }),
  ]);
  assertEqual(exactLeaf.inLibraryOrphans.length, 0,
    'exact Discogs release identity suppresses duplicate library row');
}

console.log('unpaired wording, structural grouping, and release navigation');
{
  const unpaired = [
    work('d-work', {
      source: 'discogs', type: 'Album', primary_types: [],
      format_qualifiers: ['Compilation'], title: 'Compilation work',
    }),
    work('d-promo', {
      source: 'discogs', provenance: ['promo'], title: 'Promo work',
    }),
  ];
  const ungrouped = [release('999222', {
    type: 'Album', primary_types: [], title: 'Representative scalar trap',
  })];
  const html = renderUnpairedSourceSections(unpaired, ungrouped, {
    artistName: ARTIST_NAME, source: 'discogs',
  });
  assertContains(html, 'Unpaired Discogs works <span class="type-count">2</span>',
    'honest unpaired heading');
  assertExcludes(html, 'Only on Discogs', 'false exclusivity wording removed');
  assertContains(html, 'Ungrouped Discogs releases <span class="type-count">1</span>',
    'masterless rows have their own section');
  assertContains(html, 'Compilations <span class="type-count">1</span>',
    'compilation qualifier groups as Compilation');
  assertContains(html, 'Other <span class="type-count">1</span>',
    'legacy scalar Album cannot authorize Albums');
  assertContains(html, "{masterless:true,source:'discogs'}",
    'release unit keeps exact release expansion');
  assertContains(html, 'data-release-id="999222"',
    'release unit remains ringable by exact id');
  assertContains(html, 'Promo-only works <span class="type-count">1</span>',
    'exceptional works remain explicit');
}

console.log('Rolling Stones title collision cannot auto-open exceptional types');
{
  const rows = [
    work('bootleg-comp', {
      title: 'The Rolling Stones', provenance: ['unofficial'],
      secondary_types: ['Compilation'], in_library: false,
    }),
    work('bootleg-live', {
      title: 'The Rolling Stones', provenance: ['unofficial'],
      secondary_types: ['Live'], in_library: false,
    }),
  ];
  const sections = classify(rows, [library({
    album: 'The Rolling Stones', mb_releasegroupid: 'official-1964',
    mb_albumid: '088fe5c7-d58f-4868-b1a9-548e590a5a35',
  })]);
  const html = renderArtistSections(sections, {
    artistId: ARTIST_ID, artistName: 'The Rolling Stones',
  });
  assertEqual(bodyIsOpenAfter(html, 'Unofficial-only works'), false,
    'title-only ownership does not open outer exceptional section');
  assertEqual(bodyIsOpenAfter(html, 'Compilations <span'), false,
    'Compilation stays collapsed');
  assertEqual(bodyIsOpenAfter(html, 'Live <span'), false,
    'Live stays collapsed');
  const collisionStart = html.indexOf('data-rg-id="bootleg-comp"');
  const collisionHeader = html.slice(
    collisionStart, html.indexOf('</div>', collisionStart),
  );
  assertExcludes(collisionHeader, 'in library',
    'title-colliding bootleg has no inherited library badge');
}

console.log('only exact-owned exceptional types auto-expand');
{
  const rows = [
    work('owned', {
      provenance: ['unofficial'], secondary_types: ['Live'], in_library: true,
    }),
    work('queued', {
      provenance: ['unofficial'], primary_types: ['Album'],
      pipeline_status: 'wanted', in_library: false,
    }),
  ];
  const sections = classify(rows);
  assertEqual(ownedTypeSections(sections.unofficialOnly).join(','), 'Live',
    'exact ownership selects one type');
  const html = renderArtistSections(sections, {
    artistId: ARTIST_ID, artistName: ARTIST_NAME,
  });
  assertEqual(bodyIsOpenAfter(html, 'Unofficial-only works'), true,
    'owned exceptional work opens outer section');
  assertEqual(bodyIsOpenAfter(html, 'Live <span'), true,
    'owned type opens');
  assertEqual(bodyIsOpenAfter(html, 'Albums <span'), false,
    'pipeline-only type remains closed');
}

console.log('mixed provenance is visible on its work row');
{
  const html = renderRgRow(work('mixed', {
    provenance: ['ordinary', 'promo', 'unofficial'],
  }), { artistName: ARTIST_NAME, nameLC: ARTIST_NAME.toLowerCase() });
  assertContains(html, '>promo</span>', 'mixed promo evidence chip');
  assertContains(html, '>unofficial</span>', 'mixed unofficial evidence chip');
}

console.log('appearance partition preserves native provenance');
{
  const rows = [work('main'), work('app', { is_appearance: true })];
  const provenance = partitionWorkRows(rows);
  assertEqual(provenance.mainline[0].id, 'main', 'mainline stays mainline');
  assertEqual(provenance.appearances[0].id, 'app',
    'appearance stays separate');
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
