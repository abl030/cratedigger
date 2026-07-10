/**
 * Unit tests for web/js/artist_page.js — the unified artist page
 * (issue #575 PR4): sectioning decisions + section rendering.
 *
 * Invariants under test (written before the implementation):
 *  I1 Partition totality — every input release-group lands in exactly one
 *     of {inLibrary, missing, appearances, bootlegs}; nothing dropped,
 *     nothing duplicated.
 *  I2 Bootleg precedence — !has_official → bootlegs, regardless of
 *     ownership or library state.
 *  I3 Appearance split — has_official && !own → appearances, where "own"
 *     is the exact port of the old renderArtistDiscography credit logic.
 *  I4 Library split — has_official && own && in_library === true →
 *     inLibrary; anything else → missing.
 *  I5 In-flight lens — inFlight rows come from the library feed with
 *     pipeline_status ∈ {downloading, manual}; a row appearing there
 *     never removes it from the release-group partition ("wanted" is
 *     ambient — every backfilled album has one — so it stays a badge).
 *
 * Run with: node tests/test_js_artist_page.mjs
 */

import { classifyArtistRows, renderArtistSections, renderOtherSourceSection } from '../web/js/artist_page.js';

let passed = 0;
let failed = 0;

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - expected '${expected}', got '${actual}'`);
  }
}

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - '${needle}' not in output`);
  }
}

function assertExcludes(haystack, needle, msg) {
  if (!haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - unexpectedly found '${needle}'`);
  }
}

const ARTIST_ID = 'aaaaaaaa-1111-2222-3333-444444444444';
const ARTIST_NAME = 'The Lucksmiths';

/** Release-group row builder matching /api/artist/<id>?name= rows. */
function rg(id, overrides = {}) {
  return {
    id,
    title: `RG ${id}`,
    type: 'Album',
    secondary_types: [],
    first_release_date: '2001-05-01',
    artist_credit: ARTIST_NAME,
    primary_artist_id: ARTIST_ID,
    has_official: true,
    in_library: false,
    ...overrides,
  };
}

/** LibraryAlbumRow-shaped builder matching /api/library/artist albums. */
function libRow(overrides = {}) {
  return {
    id: 1,
    album: 'An Album',
    artist: ARTIST_NAME,
    year: 2001,
    mb_albumid: 'bbbbbbbb-1111-2222-3333-444444444444',
    track_count: 10,
    mb_releasegroupid: null,
    release_group_title: null,
    added: 0,
    formats: 'MP3',
    min_bitrate: 320000,
    type: 'album',
    label: '',
    country: 'AU',
    source: 'musicbrainz',
    in_library: true,
    beets_album_id: 1,
    pipeline_status: null,
    pipeline_id: null,
    upgrade_queued: false,
    library_rank: 'good',
    ...overrides,
  };
}

function classify(releaseGroups, libraryAlbums = []) {
  return classifyArtistRows({
    artistId: ARTIST_ID,
    artistName: ARTIST_NAME,
    releaseGroups,
    libraryAlbums,
  });
}

console.log('I1 — partition totality: every rg in exactly one section');
{
  // All 8 combos of has_official × own × in_library.
  const world = [
    rg('a', { has_official: true, in_library: true }),
    rg('b', { has_official: true, in_library: false }),
    rg('c', { has_official: true, in_library: true, primary_artist_id: 'other', artist_credit: 'Someone Else' }),
    rg('d', { has_official: true, in_library: false, primary_artist_id: 'other', artist_credit: 'Someone Else' }),
    rg('e', { has_official: false, in_library: true }),
    rg('f', { has_official: false, in_library: false }),
    rg('g', { has_official: false, in_library: true, primary_artist_id: 'other', artist_credit: 'Someone Else' }),
    rg('h', { has_official: false, in_library: false, primary_artist_id: 'other', artist_credit: 'Someone Else' }),
  ];
  const s = classify(world);
  const buckets = [s.inLibrary, s.missing, s.appearances, s.bootlegs];
  const allIds = buckets.flat().map(r => r.id).sort();
  assertEqual(allIds.join(','), 'a,b,c,d,e,f,g,h', 'union covers every input exactly once');
  const seen = new Set();
  let dup = false;
  for (const id of buckets.flat().map(r => r.id)) {
    if (seen.has(id)) dup = true;
    seen.add(id);
  }
  assertEqual(dup, false, 'no rg appears in two sections');

  assertEqual(s.inLibrary.map(r => r.id).join(','), 'a', 'own+official+in_library → inLibrary');
  assertEqual(s.missing.map(r => r.id).join(','), 'b', 'own+official+not-in-library → missing');
  assertEqual(s.appearances.map(r => r.id).join(','), 'c,d', 'official non-own → appearances');
  assertEqual(s.bootlegs.map(r => r.id).join(','), 'e,f,g,h', 'I2: bootleg precedence beats own/library');
}

console.log('I3 — ownership credit logic (port of renderArtistDiscography)');
{
  const world = [
    rg('id-match', { primary_artist_id: ARTIST_ID, artist_credit: 'Totally Different' }),
    rg('exact-credit', { primary_artist_id: 'other', artist_credit: 'the lucksmiths' }),
    rg('slash-credit', { primary_artist_id: 'other', artist_credit: 'The Lucksmiths / Someone' }),
    rg('comma-credit', { primary_artist_id: 'other', artist_credit: 'The Lucksmiths, Someone' }),
    rg('empty-credit', { primary_artist_id: 'other', artist_credit: '' }),
    rg('foreign', { primary_artist_id: 'other', artist_credit: 'Someone Else' }),
  ];
  const s = classify(world);
  const own = new Set(s.missing.map(r => r.id)); // all not-in-library → own goes to missing
  assertEqual(own.has('id-match'), true, 'primary_artist_id match → own');
  assertEqual(own.has('exact-credit'), true, 'case-insensitive credit match → own');
  assertEqual(own.has('slash-credit'), true, 'credit "name /" prefix → own');
  assertEqual(own.has('comma-credit'), true, 'credit "name," prefix → own');
  assertEqual(own.has('empty-credit'), true, 'empty credit → own');
  assertEqual(s.appearances.map(r => r.id).join(','), 'foreign', 'different id+credit → appearance');
}

console.log('I4 — in_library undefined falls to missing');
{
  const row = rg('u');
  delete row.in_library;
  const s = classify([row]);
  assertEqual(s.missing.map(r => r.id).join(','), 'u', 'missing annotation → missing section');
}

console.log('I5 — in-flight lens: downloading/manual only');
{
  const albums = [
    libRow({ id: 1, album: 'DL', pipeline_status: 'downloading', pipeline_id: 11 }),
    libRow({ id: 2, album: 'Manual', pipeline_status: 'manual', pipeline_id: 12 }),
    libRow({ id: 3, album: 'Wanted-ambient', pipeline_status: 'wanted', pipeline_id: 13 }),
    libRow({ id: 4, album: 'Imported', pipeline_status: 'imported', pipeline_id: 14 }),
    libRow({ id: 5, album: 'NoPipeline', pipeline_status: null }),
    libRow({ id: 6, album: 'PipelineOnly-DL', in_library: false, beets_album_id: null, pipeline_status: 'downloading', pipeline_id: 16 }),
  ];
  const s = classify([], albums);
  assertEqual(s.inFlight.map(a => a.album).join(','), 'DL,Manual,PipelineOnly-DL',
    'downloading+manual rows only, regardless of in_library');
}

console.log('classifyArtistRows — empty world');
{
  const s = classify([], []);
  assertEqual(
    s.inLibrary.length + s.inLibraryOrphans.length + s.missing.length
    + s.appearances.length + s.bootlegs.length + s.inFlight.length,
    0, 'all sections empty');
}

console.log('I6 — owned albums absent from the discography become orphans');
{
  const rgs = [
    rg('rg-owned', { in_library: true, title: 'Matched Album' }),
  ];
  const albums = [
    // rg id matches a rendered rg → covered by the rg row, not an orphan.
    libRow({ id: 1, album: 'Matched Album', mb_releasegroupid: 'rg-owned' }),
    // Backend title-fallback case: no rg id, but an in-library rg row
    // with the same title rendered → not an orphan (no double-show).
    libRow({ id: 2, album: 'Matched Album', mb_releasegroupid: null }),
    // Genuinely absent from the source discography → orphan.
    libRow({ id: 3, album: 'Long Tail Demo', mb_releasegroupid: 'rg-not-listed' }),
    // Pipeline-only row (not actually in the library) → never an orphan.
    libRow({ id: 4, album: 'Wanted Ghost', in_library: false, beets_album_id: null, pipeline_status: 'wanted' }),
  ];
  const s = classify(rgs, albums);
  assertEqual(s.inLibraryOrphans.map(a => a.album).join(','), 'Long Tail Demo',
    'only the unmatched owned album is an orphan');

  const html = renderArtistSections(s, { artistId: ARTIST_ID, artistName: ARTIST_NAME });
  assertContains(html, 'In library <span class="type-count">2</span>',
    'section count includes orphans');
  assertContains(html, 'Library-only editions <span class="type-count">1</span>',
    'orphan subheader with count');
  assertContains(html, 'Long Tail Demo', 'orphan album row rendered');
}

console.log('I6 — title-matched RG in ANY bucket suppresses the orphan (live 1998-split bug)');
{
  // The live case: an owned Discogs-tagged split 7" (no MB rg id) whose
  // MB twin has no Official release — the RG lands in bootlegs with the
  // in-library badge, and the owned album must NOT re-emit as an orphan.
  const s = classify(
    [rg('rg-split', { has_official: false, in_library: true, title: 'Split 7 Inch' })],
    [libRow({ id: 11, album: 'Split 7 Inch', mb_releasegroupid: null, mb_albumid: '461708' })]);
  assertEqual(s.bootlegs.length, 1, 'unofficial RG stays in bootlegs');
  assertEqual(s.inLibraryOrphans.length, 0,
    'owned album title-matched to a bootleg-bucket RG is not an orphan');

  // Same shape via the appearances bucket (guest-credit RG in library).
  const s2 = classify(
    [rg('rg-guest', {
      in_library: true, title: 'Guest Comp',
      primary_artist_id: 'other', artist_credit: 'Someone Else',
    })],
    [libRow({ id: 12, album: 'Guest Comp', mb_releasegroupid: null })]);
  assertEqual(s2.appearances.length, 1, 'guest RG stays in appearances');
  assertEqual(s2.inLibraryOrphans.length, 0,
    'owned album title-matched to an appearances-bucket RG is not an orphan');

  // Known-bad self-test: a NOT-in-library bootleg RG with the same title
  // must not suppress a genuine orphan — the dedupe keys on the
  // annotation, not on mere title co-existence.
  const s3 = classify(
    [rg('rg-unowned', { has_official: false, in_library: false, title: 'Same Name' })],
    [libRow({ id: 13, album: 'Same Name', mb_releasegroupid: null })]);
  assertEqual(s3.inLibraryOrphans.length, 1,
    'un-annotated same-title RG does not hide the owned album');
}

console.log('I6 — orphans alone still render the In library section');
{
  const s = classify([], [libRow({ id: 7, album: 'Only Orphan', mb_releasegroupid: null })]);
  const html = renderArtistSections(s, { artistId: ARTIST_ID, artistName: ARTIST_NAME });
  assertContains(html, 'In library <span class="type-count">1</span>',
    'orphan-only In library section renders');
  assertContains(html, 'Only Orphan', 'orphan row rendered');
}

console.log('renderOtherSourceSection — complement rows force the other source');
{
  const rows = [
    { id: '999111', title: 'Discogs Only LP', type: 'Album', first_release_date: '1999-01-01' },
    { id: '999222', title: 'Masterless Single', type: 'Single', first_release_date: '2000-01-01', is_masterless: true },
  ];
  const html = renderOtherSourceSection(rows, { artistName: ARTIST_NAME, source: 'discogs' });
  assertContains(html, 'Only on Discogs <span class="type-count">2</span>', 'header + count');
  assertContains(html, 'id="only-other-source"', 'idempotency marker id');
  assertContains(html, "{source:'discogs'}", 'rows force the complement source');
  assertContains(html, "{masterless:true,source:'discogs'}", 'masterless keeps its flag');
  assertContains(html, 'data-release-id="999222"', 'masterless leaf carries data-release-id');
  assertEqual(renderOtherSourceSection([], { artistName: ARTIST_NAME, source: 'discogs' }), '',
    'empty bucket -> empty string');
  const mbSide = renderOtherSourceSection([rows[0]], { artistName: ARTIST_NAME, source: 'mb' });
  assertContains(mbSide, 'Only on MusicBrainz', 'mb complement label');
}

console.log('renderArtistSections — section headers, counts, defaults');
{
  const sections = classify([
    rg('lib1', { in_library: true, title: 'Owned Album' }),
    rg('miss1', { title: 'Missing Album' }),
    rg('miss2', { title: 'Missing EP', type: 'EP' }),
    rg('app1', { primary_artist_id: 'other', artist_credit: 'Someone Else', title: 'Guest Spot' }),
    rg('boot1', { has_official: false, title: 'Live Tape' }),
  ], [
    libRow({ id: 9, album: 'DL Album', pipeline_status: 'downloading', pipeline_id: 9 }),
  ]);
  const html = renderArtistSections(sections, { artistId: ARTIST_ID, artistName: ARTIST_NAME });

  // The downloading row's rg isn't in the discography, so it also
  // counts as a library-only orphan (in-flight is a lens — I5/I6).
  assertContains(html, 'In library <span class="type-count">2</span>', 'In library header with count');
  assertContains(html, 'In flight <span class="type-count">1</span>', 'In flight header with count');
  assertContains(html, 'Missing <span class="type-count">2</span>', 'Missing header with count');
  assertContains(html, 'Appearances <span class="type-count">1</span>', 'Appearances header with count');
  assertContains(html, 'Bootleg-only releases <span class="type-count">1</span>', 'Bootlegs header with count');

  assertContains(html, 'Owned Album', 'in-library rg row rendered');
  assertContains(html, 'Missing Album', 'missing rg row rendered');
  assertContains(html, 'Missing EP', 'missing EP row rendered');
  assertContains(html, 'Guest Spot', 'appearance row rendered');
  assertContains(html, 'Live Tape', 'bootleg row rendered');
  assertContains(html, 'DL Album', 'in-flight row rendered');

  // Expansion targets exist for rg rows (search-by-ID + pressings flow).
  assertContains(html, 'id="rel-lib1"', 'in-library rg keeps its expansion target');
  assertContains(html, 'id="rel-miss1"', 'missing rg keeps its expansion target');
  // Rows carry data-rg-id for late analysis decoration.
  assertContains(html, 'data-rg-id="miss1"', 'rg rows carry data-rg-id');

  // Section toggles route through the shared primitive.
  assertContains(html, 'window.toggleSection(this)', 'section headers use toggleSection');
}

console.log('renderArtistSections — empty sections are omitted');
{
  const sections = classify([rg('only', { in_library: true })], []);
  const html = renderArtistSections(sections, { artistId: ARTIST_ID, artistName: ARTIST_NAME });
  assertContains(html, 'In library', 'non-empty section rendered');
  assertExcludes(html, 'In flight', 'empty in-flight omitted');
  assertExcludes(html, 'Missing', 'empty missing omitted');
  assertExcludes(html, 'Appearances', 'empty appearances omitted');
  assertExcludes(html, 'Bootleg-only', 'empty bootlegs omitted');
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
