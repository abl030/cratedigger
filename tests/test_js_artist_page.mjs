/** Unit tests for the unified artist-page semantic catalogue. */
import {
  classifyArtistRows,
  composeCompareCatalogue,
  renderArtistSections,
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
function countOccurrences(haystack, needle) {
  return haystack.split(needle).length - 1;
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
    primary_artist_id: '361476',
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

console.log('simple catalogue partition is total and provenance-backed');
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
    sections.inLibrary, sections.missing, sections.otherReleases,
  ];
  assertEqual(
    buckets.flat().map(row => row.id).sort().join(','),
    world.map(row => row.id).sort().join(','),
    'every catalogue row appears exactly once',
  );
  assertEqual(sections.inLibrary.map(row => row.id).join(','), 'owned',
    'ordinary exact-owned row is in library');
  assertEqual(sections.missing.map(row => row.id).join(','), 'missing,mixed',
    'ordinary and mixed ordinary rows remain mainline');
  assertEqual(
    sections.otherReleases.map(row => row.id).join(','),
    'appearance,foreign,promo,unofficial,unknown',
    'appearances and exceptional or unknown rows share one Other area',
  );
}

console.log('masterless rows enter musical buckets with exact release navigation');
{
  const ordinary = release('3938744', { title: 'Fraulein' });
  const promo = release('19254925', {
    title: 'Loup Garou', provenance: ['promo'], primary_types: ['Single'],
  });
  const sections = classify([], [], [ordinary, promo]);
  assertEqual(sections.missing.map(row => row.id).join(','), '3938744',
    'ordinary masterless album is a normal Missing row');
  assertEqual(sections.otherReleases.map(row => row.id).join(','), '19254925',
    'promo masterless release is in Other releases');
  const html = renderArtistSections(sections, {
    artistId: ARTIST_ID, artistName: ARTIST_NAME,
  });
  assertExcludes(html, 'Ungrouped', 'storage topology is not a heading');
  assertContains(html, "{masterless:true,source:'discogs',identityKind:'release'}",
    'masterless row keeps exact release expansion');
  assertContains(html, 'data-release-id="3938744"',
    'masterless row remains ringable by exact release id');
}

console.log('library-only suppression uses exact source/kind identity, including pairs');
{
  const sameTitle = work('wrong-rg', { title: 'The Rolling Stones' });
  const album = library({
    album: 'The Rolling Stones', mb_releasegroupid: 'actual-rg',
    mb_albumid: 'actual-release',
  });
  assertEqual(classify([sameTitle], [album]).inLibraryOrphans.length, 1,
    'same title does not hide a different edition');
  assertEqual(classify([
    work('actual-rg', { title: 'Different typography', in_library: true }),
  ], [album]).inLibraryOrphans.length, 0,
  'exact MB work suppresses its duplicate library row');
  assertEqual(classify([], [album], [
    release('actual-release', { title: 'Different typography', in_library: true }),
  ]).inLibraryOrphans.length, 0,
  'exact Discogs release suppresses its duplicate library row');

  const rows = composeCompareCatalogue({
    both: [{
      mb: work('actual-rg', { title: 'Paired work', in_library: true }),
      discogs: release('3938744', { title: 'Paired work', in_library: false }),
    }],
    mb_unpaired: [], discogs_unpaired: [], discogs_ungrouped_releases: [],
  }, 'discogs');
  const paired = classify(rows, [album]);
  assertEqual(paired.inLibraryOrphans.length, 0,
    'exact owned counterpart suppresses a duplicate library orphan');
  assertEqual(paired.missing.map(row => row.id).join(','), '3938744',
    'selected Discogs pressing stays Missing when only MB counterpart is owned');
}

console.log('Deloris Fraulein renders once with selected-source exact identity');
{
  const mbId = '1c9e2970-b221-30ab-93c6-7896b52a240b';
  const compare = {
    both: [{
      mb: work(mbId, {
        title: 'Fraulein', first_release_date: '1998', in_library: true,
        pipeline_status: 'wanted', pipeline_id: 425,
      }),
      discogs: release('3938744', {
        title: 'Fraulein', first_release_date: '1998', in_library: false,
        pipeline_status: 'wanted', pipeline_id: 8840,
      }),
    }],
    mb_unpaired: [], discogs_unpaired: [], discogs_ungrouped_releases: [],
  };
  const album = library({
    album: 'Fraulein', mb_releasegroupid: mbId,
    mb_albumid: 'mb-release-id',
  });

  const mbRows = composeCompareCatalogue(compare, 'mb');
  const mbHtml = renderArtistSections(classify(mbRows, [album]), {
    artistId: ARTIST_ID, artistName: 'Deloris',
  });
  assertEqual(countOccurrences(mbHtml, '<span class="rg-title">Fraulein</span>'), 1,
    'MB primary renders the paired work exactly once');
  assertContains(mbHtml, `data-catalogue-source="mb"`,
    'MB primary keeps MB source');
  assertContains(mbHtml, `data-catalogue-id="${mbId}"`,
    'MB primary keeps exact release-group id');

  const dgRows = composeCompareCatalogue(compare, 'discogs');
  const dgSections = classify(dgRows, [album]);
  const dgHtml = renderArtistSections(dgSections, {
    artistId: '361476', artistName: 'Deloris',
  });
  assertEqual(countOccurrences(dgHtml, '<span class="rg-title">Fraulein</span>'), 1,
    'Discogs primary renders the paired work exactly once');
  assertEqual(dgSections.inLibrary.length, 0,
    'counterpart ownership never claims the selected Discogs release');
  assertEqual(dgSections.inLibraryOrphans.length, 0,
    'owned MB counterpart does not double-render as a library orphan');
  assertContains(dgHtml, 'data-catalogue-source="discogs"',
    'Discogs primary keeps Discogs source');
  assertContains(dgHtml, 'data-identity-kind="release"',
    'Discogs primary keeps release identity kind');
  assertContains(dgHtml, 'data-catalogue-id="3938744"',
    'Discogs primary keeps exact release id');
  assertContains(dgHtml, 'other edition in library',
    'counterpart ownership is expressed without claiming exact ownership');
  assertContains(dgHtml, '>wanted</span>',
    'selected Discogs request status remains its exact action state');
}

console.log('associated positive ordinary evidence classifies without rewriting source provenance');
{
  const mb = work('mb-split', {
    title: 'The Split', provenance: [], in_library: false,
  });
  const dg = release('461708', {
    title: 'The Split', provenance: ['ordinary'], in_library: true,
  });
  const [row] = composeCompareCatalogue({
    both: [{ mb, discogs: dg }],
    mb_unpaired: [], discogs_unpaired: [], discogs_ungrouped_releases: [],
  }, 'mb');
  const sections = classify([row]);
  assertEqual(row.provenance.length, 0,
    'selected MB provenance remains source-authored unknown');
  assertEqual(row.display_provenance.join(','), 'ordinary',
    'display classification sees positive ordinary counterpart evidence');
  assertEqual(sections.missing.map(item => item.id).join(','), 'mb-split',
    'unknown plus ordinary associated row is in the normal album catalogue');
}

console.log('top-level vocabulary and defaults match the original simple model');
{
  const sections = classify([
    work('owned-album', { in_library: true, title: 'Owned Album' }),
    work('owned-ep', {
      in_library: true, title: 'Owned EP', primary_types: ['EP'], type: 'EP',
    }),
    work('missing-album', { title: 'Missing Album' }),
    work('missing-compilation', {
      title: 'Missing Compilation', secondary_types: ['Compilation'],
    }),
    work('other-live', {
      title: 'Unofficial Live', provenance: ['unofficial'],
      secondary_types: ['Live'],
    }),
  ], [library({
    id: 9, album: 'DL Album', in_library: false, beets_album_id: null,
    pipeline_status: 'downloading', pipeline_id: 9,
  })]);
  const html = renderArtistSections(sections, {
    artistId: ARTIST_ID, artistName: ARTIST_NAME,
  });
  for (const id of [
    'catalogue-in-library', 'catalogue-in-flight',
    'catalogue-missing', 'catalogue-other-releases',
  ]) assertContains(html, `id="${id}"`, `${id} top-level section exists`);
  for (const heading of ['Unpaired', 'Ungrouped', 'Appearances', 'Promo-only', 'Unofficial-only']) {
    assertExcludes(html, heading, `${heading} is not page taxonomy`);
  }
  assertEqual(bodyIsOpenAfter(html, 'id="catalogue-in-library"'), true,
    'In library is open');
  assertEqual(bodyIsOpenAfter(html, 'id="catalogue-in-flight"'), true,
    'In flight is open');
  assertEqual(bodyIsOpenAfter(html, 'id="catalogue-missing"'), true,
    'Missing is open');
  assertEqual(bodyIsOpenAfter(html, 'id="catalogue-other-releases"'), false,
    'Other releases is collapsed');
  assertEqual(bodyIsOpenAfter(html, 'Albums <span'), true,
    'Albums is the only default-open musical bucket');
  assertEqual(bodyIsOpenAfter(html, 'EPs <span'), false, 'EPs stay closed');
  assertEqual(bodyIsOpenAfter(html, 'Compilations <span'), false,
    'Compilations stay closed');
  assertEqual(bodyIsOpenAfter(html, 'Live <span'), false, 'Live stays closed');
}

console.log('Rolling Stones title collision never claims ownership or expands exceptions');
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
  assertEqual(bodyIsOpenAfter(html, 'id="catalogue-other-releases"'), false,
    'title-only ownership does not open Other releases');
  assertEqual(bodyIsOpenAfter(html, 'Compilations <span'), false,
    'Compilation stays collapsed');
  assertEqual(bodyIsOpenAfter(html, 'Live <span'), false,
    'Live stays collapsed');
  for (const id of ['bootleg-comp', 'bootleg-live']) {
    const start = html.indexOf(`data-rg-id="${id}"`);
    const header = html.slice(start, html.indexOf('</div>', start));
    assertExcludes(header, 'in library', `${id} has no inherited ownership badge`);
  }
}

console.log('even exact-owned exceptional rows leave Other releases collapsed');
{
  const html = renderArtistSections(classify([
    work('owned-live', {
      provenance: ['unofficial'], secondary_types: ['Live'], in_library: true,
    }),
  ]), { artistId: ARTIST_ID, artistName: ARTIST_NAME });
  assertEqual(bodyIsOpenAfter(html, 'id="catalogue-other-releases"'), false,
    'Other releases always starts collapsed');
  assertEqual(bodyIsOpenAfter(html, 'Live <span'), false,
    'types inside Other releases always start collapsed');
}

console.log('mixed source-authored provenance stays visible on its row');
{
  const html = renderRgRow(work('mixed', {
    provenance: ['ordinary', 'promo', 'unofficial'],
  }), { artistName: ARTIST_NAME, nameLC: ARTIST_NAME.toLowerCase() });
  assertContains(html, '>promo</span>', 'mixed promo evidence chip');
  assertContains(html, '>unofficial</span>', 'mixed unofficial evidence chip');
}

console.log('ownership credit variants preserve the established contract');
{
  const world = [
    work('id-match', { primary_artist_id: ARTIST_ID, artist_credit: 'Different' }),
    work('exact-credit', { primary_artist_id: 'other', artist_credit: 'the lucksmiths' }),
    work('slash-credit', { primary_artist_id: 'other', artist_credit: 'The Lucksmiths / Someone' }),
    work('comma-credit', { primary_artist_id: 'other', artist_credit: 'The Lucksmiths, Someone' }),
    work('empty-credit', { primary_artist_id: 'other', artist_credit: '' }),
    work('foreign', { primary_artist_id: 'other', artist_credit: 'Someone Else' }),
  ];
  const sections = classify(world);
  const own = new Set(sections.missing.map(row => row.id));
  for (const id of ['id-match', 'exact-credit', 'slash-credit', 'comma-credit', 'empty-credit']) {
    assertEqual(own.has(id), true, `${id} remains an own-work credit`);
  }
  assertEqual(sections.otherReleases.map(row => row.id).join(','), 'foreign',
    'foreign credit lands in Other releases');
}

console.log('in-flight lens includes downloading/manual and excludes ambient states');
{
  const albums = [
    library({ id: 1, album: 'DL', pipeline_status: 'downloading', pipeline_id: 11 }),
    library({ id: 2, album: 'Manual', pipeline_status: 'manual', pipeline_id: 12 }),
    library({ id: 3, album: 'Wanted', pipeline_status: 'wanted', pipeline_id: 13 }),
    library({ id: 4, album: 'Imported', pipeline_status: 'imported', pipeline_id: 14 }),
    library({ id: 5, album: 'None', pipeline_status: null }),
    library({
      id: 6, album: 'Pipeline-only DL', in_library: false,
      beets_album_id: null, pipeline_status: 'downloading', pipeline_id: 16,
    }),
  ];
  assertEqual(classify([], albums).inFlight.map(row => row.album).join(','),
    'DL,Manual,Pipeline-only DL',
    'downloading/manual are visible regardless of library ownership');
}

console.log('empty and orphan-only artist worlds remain renderable');
{
  const empty = classify([], []);
  assertEqual([
    empty.inLibrary, empty.inLibraryOrphans, empty.inFlight,
    empty.missing, empty.otherReleases,
  ].flat().length, 0, 'empty world has no synthetic rows');

  const orphanOnly = classify([], [library({
    id: 7, album: 'Only Orphan', mb_releasegroupid: null,
  })]);
  const html = renderArtistSections(orphanOnly, {
    artistId: ARTIST_ID, artistName: ARTIST_NAME,
  });
  assertContains(html, 'In library <span class="type-count">1</span>',
    'orphan-only In library section renders');
  assertContains(html, 'Library-only editions <span class="type-count">1</span>',
    'genuine orphan has its explicit subheader');
  assertContains(html, 'Only Orphan', 'orphan row remains visible');
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
