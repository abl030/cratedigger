/**
 * Unit tests for web/js/discography.js pure helpers.
 * Run with: node tests/test_js_discography.mjs
 */

import {
  addRelease,
  catalogueDomId,
  releaseGroupRequestPath,
  renderPressingRow,
  renderRgRow,
  synthesizeMasterlessRow,
  splitPressings,
  statusChipHtml,
} from '../web/js/discography.js';

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
  assertEqual(haystack.includes(needle), true, msg);
}

function assertExcludes(haystack, needle, msg) {
  assertEqual(haystack.includes(needle), false, msg);
}

/** Independent expected encoder: JSON JS literal, then HTML attribute escaping. */
function expectedJsArg(value) {
  return JSON.stringify(String(value))
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/\\/g, '&#92;');
}

/** Independent expected text encoder for caller-owned HTML fragments. */
function expectedEsc(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/\\/g, '&#92;');
}

console.log('synthesizeMasterlessRow() — overlay fields survive the synthesis');
{
  // The live bug (request 8838, Deloris "Feather Figure/Elastic Bones"):
  // the payload carried pipeline_status=wanted but the synthetic pressing
  // row dropped it, rendering a green "Add request" on an
  // already-requested release.
  const row = synthesizeMasterlessRow({
    id: '8317023',
    title: 'Feather Figure/Elastic Bones',
    date: '2005-06-00',
    country: 'Australia',
    status: 'Official',
    formats: [{ name: 'CD' }],
    tracks: new Array(10).fill({ title: 't' }),
    labels: [{ id: 1, name: 'Dot Dash' }],
    release_group_id: null,
    in_library: false,
    beets_album_id: null,
    pipeline_status: 'wanted',
    pipeline_id: 8838,
    processing_owner: null,
    has_captured_history: true,
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  });
  assertEqual(row.pipeline_status, 'wanted', 'pipeline_status forwarded');
  assertEqual(row.pipeline_id, 8838, 'pipeline_id forwarded');
  assertEqual(row.processing_owner, null, 'non-processing owner null forwarded');
  assertEqual(row.has_captured_history, true, 'captured history forwarded');
  assertEqual(row.pipeline_verified_lossless, true, 'verified proof forwarded');
  assertEqual(row.pipeline_provisional, false, 'provisional fact forwarded');
  assertEqual(row.in_library, false, 'in_library forwarded');
  assertEqual(row.beets_album_id, null, 'beets_album_id forwarded');
  assertEqual(row.id, '8317023', 'id kept');
  assertEqual(row.title, 'Feather Figure/Elastic Bones', 'title kept');
  assertEqual(row.format, 'CD', 'formats joined');
  assertEqual(row.track_count, 10, 'track count derived');
  assertEqual(row.status, 'Official', 'status kept');
}

console.log('synthesizeMasterlessRow() — exact processing owner survives synthesis');
{
  const owner = {
    job_id: 8839,
    status: 'queued',
    preview_status: 'running',
  };
  const row = synthesizeMasterlessRow({
    id: '8317024',
    title: 'Processing pressing',
    tracks: [],
    formats: [],
    in_library: false,
    pipeline_status: 'processing',
    pipeline_id: 8840,
    processing_owner: owner,
  });
  assertEqual(row.processing_owner, owner, 'processing_owner object is forwarded unchanged');
  const html = renderPressingRow(row, {
    artistName: 'Deloris',
    parentRgId: null,
    canReplace: true,
  });
  assertContains(html, 'previewing', 'pressing action consumes canonical owner label');
  assertContains(html, '/api/import-jobs/8839/recovery', 'pressing links exact owner recovery detail');
  assertExcludes(html, 'window.disambRemove', 'processing pressing cannot remove request');
}

console.log('addRelease() — processing exists response exposes exact owner recovery');
{
  const oldDocument = globalThis.document;
  const oldFetch = globalThis.fetch;
  const oldWindow = globalThis.window;
  const created = [];
  const mounted = [];
  function element(tag = '') {
    const attributes = new Map();
    const node = {
      attributes,
      children: [],
      className: '',
      dataset: {},
      disabled: false,
      focused: 0,
      id: '',
      isConnected: tag === 'button',
      style: {},
      tag,
      textContent: '',
      setAttribute(name, value) { attributes.set(name, value); },
      removeAttribute(name) { attributes.delete(name); },
      getAttribute(name) { return attributes.get(name) || null; },
      appendChild(child) {
        child.isConnected = true;
        this.children.push(child);
      },
      insertAdjacentElement(_position, child) {
        child.isConnected = true;
        mounted.push(child);
      },
      focus() { this.focused++; },
      remove() { this.isConnected = false; },
    };
    created.push(node);
    return node;
  }
  const button = element('button');
  button.textContent = 'Add request';
  const body = element('body');
  body.isConnected = true;
  const documentElement = element('html');
  documentElement.isConnected = true;
  globalThis.document = {
    activeElement: button,
    body,
    documentElement,
    createElement(tag) { return element(tag); },
    getElementById(id) {
      return created.find(node => node.id === id && node.isConnected) || null;
    },
    querySelectorAll(selector) {
      return selector === '[data-pipeline-request-id="321"]'
        ? [button]
        : [];
    },
  };
  globalThis.window = {
    scrollX: 0,
    scrollY: 0,
    scrollTo() {},
  };
  const calls = [];
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    if (url === '/api/pipeline/add') {
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            status: 'exists',
            id: 321,
            current_status: 'processing',
            processing_owner: {
              job_id: 654,
              status: 'queued',
              preview_status: 'running',
            },
          };
        },
      };
    }
    return {
      ok: false,
      status: 503,
      async json() { return {}; },
    };
  };

  await addRelease(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    button,
  );

  assertEqual(
    button.textContent,
    'previewing',
    'Add exists lock label comes from the exact processing owner',
  );
  assertEqual(
    button.attributes.get('data-pipeline-request-id'),
    '321',
    'Add exists lock binds the authoritative request id',
  );
  assertEqual(
    created.some(
      node => node.href === '/api/import-jobs/654/recovery',
    ),
    true,
    'Add exists lock links exact owner recovery detail',
  );
  assertEqual(
    calls.join(','),
    '/api/pipeline/add,/api/pipeline/321',
    'Add conflict refreshes only the affected request',
  );
  globalThis.document = oldDocument;
  globalThis.fetch = oldFetch;
  globalThis.window = oldWindow;
}

console.log('synthesizeMasterlessRow() — in-library payload keeps quality fields');
{
  const row = synthesizeMasterlessRow({
    id: '999',
    title: 'Owned One',
    tracks: [],
    formats: [],
    in_library: true,
    beets_album_id: 42,
    pipeline_status: 'imported',
    pipeline_id: 7,
    library_format: 'FLAC',
    library_min_bitrate: 900,
    library_avg_bitrate: 1100,
    library_rank: 'lossless',
    has_captured_history: true,
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  });
  assertEqual(row.in_library, true, 'in_library true forwarded');
  assertEqual(row.beets_album_id, 42, 'beets_album_id forwarded');
  assertEqual(row.library_format, 'FLAC', 'library_format forwarded');
  assertEqual(row.library_min_bitrate, 900, 'library_min_bitrate forwarded');
  assertEqual(row.library_avg_bitrate, 1100, 'library_avg_bitrate forwarded');
  assertEqual(row.library_rank, 'lossless', 'library_rank forwarded');
  assertEqual(row.has_captured_history, true, 'has_captured_history forwarded');
  assertEqual(row.pipeline_verified_lossless, true, 'pipeline_verified_lossless forwarded');
  assertEqual(row.pipeline_provisional, false, 'pipeline_provisional forwarded');
  assertEqual(row.format, '?', 'empty formats fall back to ?');

  const html = renderPressingRow(row, {
    artistName: 'Artist',
    parentRgId: null,
    canReplace: false,
  });
  assertContains(html, '>in library</span>', 'masterless row renders current holding');
  assertContains(html, '>F</span>', 'masterless row renders quality independently');
  assertContains(html, '>captured<', 'masterless row renders captured history');
  assertContains(html, '>verified<', 'masterless row renders carried proof');
}

console.log('splitPressings() — owned/in-flight pressings are never hidden (The Meadowlands pin)');
{
  // Live confusion (request 4228, The Wrens "The Meadowlands"): the
  // library copy is the 2002 US Promotion pressing, which the old split
  // buried inside the collapsed Bootleg / Promo section — the expansion
  // contradicted the row's holding, quality, and wanted badges.
  const rows = [
    { id: 'cef6b0f6', status: 'Promotion', in_library: true, pipeline_status: 'wanted' },
    { id: '2aa0ae0e', status: 'Bootleg', in_library: false, pipeline_status: null },
    { id: 'fef45b67', status: 'Official', in_library: false, pipeline_status: 'downloading' },
    { id: 'a0fadcc2', status: 'Official', in_library: false, pipeline_status: null },
  ];
  const { visible, hidden } = splitPressings(rows);
  assertEqual(visible.some(r => r.id === 'cef6b0f6'), true, 'owned promo is visible');
  assertEqual(hidden.some(r => r.id === 'cef6b0f6'), false, 'owned promo not hidden');
  assertEqual(hidden.length, 1, 'only the unowned bootleg is hidden');
  assertEqual(hidden[0].id, '2aa0ae0e', 'unowned bootleg stays in the collapsed bucket');
  assertEqual(visible.length, 3, 'officials + owned promo visible');
}

console.log('splitPressings() — partition + hoist invariants over the status/ownership space');
{
  const statuses = [undefined, '', 'Official', 'Promotion', 'Bootleg', 'Pseudo-Release'];
  const ownerships = [
    { in_library: false, pipeline_status: null },
    { in_library: true, pipeline_status: null },
    { in_library: false, pipeline_status: 'downloading' },
    { in_library: true, pipeline_status: 'wanted' },
    { in_library: false, pipeline_status: 'imported' },
    { in_library: false, pipeline_status: 'unsearchable' },
    { in_library: false, pipeline_status: 'replaced' },
    { in_library: true, pipeline_status: 'replaced' },
  ];
  let n = 0;
  const rows = [];
  for (const status of statuses) {
    for (const own of ownerships) {
      rows.push({ id: `r${n++}`, status, ...own });
    }
  }
  const { visible, hidden } = splitPressings(rows);
  assertEqual(visible.length + hidden.length, rows.length, 'every row lands in exactly one bucket');
  for (const r of rows) {
    const inVisible = visible.includes(r);
    const inHidden = hidden.includes(r);
    assertEqual(inVisible !== inHidden, true, `${r.id} in exactly one bucket`);
    // 'replaced' is the terminal frozen-audit status — an abandoned
    // request is NOT an active claim on the pressing and must not pin it.
    const owned = r.in_library === true
      || (!!r.pipeline_status && r.pipeline_status !== 'replaced');
    const official = r.status === 'Official' || !r.status;
    if (owned || official) {
      assertEqual(inVisible, true, `${r.id} (status=${r.status}, owned=${owned}) must be visible`);
    } else {
      assertEqual(inHidden, true, `${r.id} (status=${r.status}, unowned non-official) must be hidden`);
    }
  }
}

console.log('splitPressings() — a replaced-only pipeline row does not pin (visible frozen audit badge)');
{
  // Replaced is visible frozen audit history, not an active ownership
  // claim. Only in_library or an ACTIVE pipeline status pins.
  const rows = [
    { id: 'abandoned', status: 'Bootleg', in_library: false, pipeline_status: 'replaced' },
    { id: 'owned-abandoned', status: 'Promotion', in_library: true, pipeline_status: 'replaced' },
  ];
  const { visible, hidden } = splitPressings(rows);
  assertEqual(hidden.length, 1, 'unowned replaced bootleg stays collapsed');
  assertEqual(hidden[0].id, 'abandoned', 'the replaced-only row is the hidden one');
  assertEqual(visible.length, 1, 'library ownership still pins a replaced row');
  assertEqual(visible[0].id, 'owned-abandoned', 'in_library wins over replaced');

  const replacedHtml = renderPressingRow(rows[0], {
    artistName: 'Artist',
    parentRgId: null,
    canReplace: false,
  });
  assertContains(replacedHtml, '>replaced<', 'collapsed row explains its frozen request history');
}

console.log('splitPressings() — known-bad self-check: the OLD split violates the hoist invariant');
{
  // Prove the assertion above actually constrains something: the
  // pre-fix split (status-only) hides the owned promo.
  const rows = [{ id: 'x', status: 'Promotion', in_library: true, pipeline_status: null }];
  const oldHidden = rows.filter(r => r.status && r.status !== 'Official');
  assertEqual(oldHidden.length, 1, 'old split hides the owned promo (the bug)');
  assertEqual(splitPressings(rows).hidden.length, 0, 'new split does not');
}

console.log('statusChipHtml() — non-official pressings get a provenance chip');
{
  assertEqual(statusChipHtml('Official'), '', 'Official -> no chip');
  assertEqual(statusChipHtml(''), '', 'empty -> no chip');
  assertEqual(statusChipHtml(undefined), '', 'missing -> no chip');
  assertEqual(statusChipHtml('Promotion').includes('promo'), true, 'Promotion -> promo chip');
  assertEqual(statusChipHtml('Promotion').includes('badge-nonofficial'), true, 'chip uses the nonofficial badge class');
  assertEqual(statusChipHtml('Bootleg').includes('bootleg'), true, 'Bootleg -> bootleg chip');
  assertEqual(statusChipHtml('Pseudo-Release').includes('pseudo-release'), true, 'other statuses lowercased verbatim');
}

console.log('Release-id onclick arguments — adversarial deterministic pin');
{
  const id = "rg'\"\\</div><script>alert(1)</script>";
  const arg = expectedJsArg(id);
  const rgHtml = renderRgRow(
    {
      id, title: 'Adversarial release', first_release_date: '2003',
      identity_kind: 'release',
    },
    { artistName: 'The Wrens', nameLC: 'the wrens', source: 'mb' },
  );
  const pressingHtml = renderPressingRow({
    id,
    title: 'Adversarial pressing',
    status: 'Official',
    in_library: false,
    pipeline_status: null,
    country: 'US',
    date: '2003',
    format: 'CD',
    track_count: 13,
  }, { artistName: 'The Wrens', parentRgId: 'parent', canReplace: false });

  assertContains(rgHtml, `window.loadReleaseGroup(${arg}, this`, 'RG click passes one encoded JS string argument');
  assertExcludes(rgHtml, `window.loadReleaseGroup('${id}'`, 'known-bad raw single-quoted RG interpolation is absent');
  assertContains(pressingHtml, `window.toggleReleaseDetail(${arg})`, 'pressing click passes one encoded JS string argument');
  assertExcludes(pressingHtml, `window.toggleReleaseDetail('${id}')`, 'known-bad raw single-quoted pressing interpolation is absent');
  assertExcludes(pressingHtml, '>Remove from beets</button>', 'unowned pressing omits disabled beets action');
  assertContains(pressingHtml, '>Add request</button>', 'unowned pressing keeps Add request');
  assertContains(pressingHtml, '>Replace</button>', 'unowned pressing keeps Replace');
}

console.log('Release-id onclick arguments — generated critical-character property sweep');
{
  const atoms = ['a', "'", '"', '\\', '<', '>', '&', '\n', '\u2028'];
  const ids = ['plain-id', ...atoms];
  for (const left of atoms) {
    for (const right of atoms) ids.push(`id${left}${right}tail`);
  }
  for (const id of ids) {
    const arg = expectedJsArg(id);
    const rgHtml = renderRgRow(
      { id, title: 'RG', first_release_date: '2000' },
      { artistName: 'Artist', nameLC: 'artist' },
    );
    const pressingHtml = renderPressingRow({
      id,
      title: 'Pressing',
      status: 'Official',
      in_library: true,
      beets_album_id: 42,
      country: 'AU',
      date: '2000',
      format: 'CD',
      track_count: 10,
    }, { artistName: 'Artist', parentRgId: 'parent', canReplace: true });
    assertContains(rgHtml, `window.loadReleaseGroup(${arg}, this`, `RG id round-trips safely: ${JSON.stringify(id)}`);
    assertContains(pressingHtml, `window.toggleReleaseDetail(${arg})`, `pressing id round-trips safely: ${JSON.stringify(id)}`);
    assertContains(pressingHtml, 'window.confirmDeleteBeets(42', `owned removal survives: ${JSON.stringify(id)}`);
  }

  const badId = "break'out";
  const oldHandler = `window.toggleReleaseDetail('${badId}')`;
  let oldCompiles = true;
  try { new Function('window', oldHandler); } catch (_) { oldCompiles = false; }
  assertEqual(oldCompiles, false, 'known-bad raw interpolation checker rejects apostrophe ID');
}

console.log('Pressing metadata — hostile catalogue values stay text at the caller-owned HTML boundary');
{
  const ordinary = renderPressingRow({
    id: 'metadata-ordinary',
    title: 'Safe title',
    country: 'Australia',
    date: '2003-06-00',
    format: 'CD',
    track_count: 13,
    status: 'Official',
    in_library: false,
    pipeline_status: null,
  }, { artistName: 'Artist', parentRgId: 'parent', canReplace: false });
  assertContains(ordinary, 'Australia 2003-06-00 - CD - 13t - Official',
    'ordinary pressing metadata presentation is unchanged');

  const hostile = '<img src=x onerror=alert(1)>';
  const pressingHtml = renderPressingRow({
    id: 'metadata-hostile',
    title: 'Safe title',
    country: hostile,
    date: hostile,
    format: hostile,
    track_count: hostile,
    status: hostile,
    in_library: false,
    pipeline_status: null,
  }, { artistName: 'Artist', parentRgId: 'parent', canReplace: false });
  const escaped = '&lt;img src=x onerror=alert(1)&gt;';
  const metaStart = pressingHtml.indexOf('<div class="release-meta"');
  const metaEnd = pressingHtml.indexOf('</div>', metaStart);
  const metadataHtml = pressingHtml.slice(metaStart, metaEnd);
  assertEqual(metadataHtml.split(escaped).length - 1, 5,
    'country, date, format, track count, and status are each escaped exactly once');
  assertExcludes(pressingHtml, hostile,
    'hostile pressing metadata cannot create an image element');

  const oldMeta = `${hostile} ${hostile} - ${hostile} - ${hostile}t - ${hostile}`;
  assertContains(oldMeta, hostile,
    'known-bad raw metadata composition admits an image element');
}

console.log('Pressing metadata — generated critical-character property sweep');
{
  const atoms = ['plain', '&', '<', '>', '"', "'", '\\'];
  for (const field of ['country', 'date', 'format', 'track_count', 'status']) {
    for (const value of atoms) {
      const metadata = {
        country: 'AU', date: '2000', format: 'CD', track_count: 12, status: 'Official', [field]: value,
      };
      const pressingHtml = renderPressingRow({
        id: 'metadata-sweep',
        title: 'Safe title',
        ...metadata,
        in_library: false,
        pipeline_status: null,
      }, { artistName: 'Artist', parentRgId: 'parent', canReplace: false });
      const expectedMeta = `${expectedEsc(metadata.country)} ${expectedEsc(metadata.date)} - ${expectedEsc(metadata.format)} - ${expectedEsc(metadata.track_count)}t - ${expectedEsc(metadata.status)}`;
      assertContains(pressingHtml, expectedMeta,
        `${field} remains escaped text: ${JSON.stringify(value)}`);
    }
  }
}

console.log('Discogs master/release DOM identities stay distinct at equal numeric IDs');
{
  const masterId = catalogueDomId('discogs', 'work', '122');
  const releaseId = catalogueDomId('discogs', 'release', '122');
  assertEqual(masterId, 'rel-discogs-work-122', 'master target is namespaced as work');
  assertEqual(releaseId, 'rel-discogs-release-122', 'leaf target is namespaced as release');
  assertEqual(masterId === releaseId, false, 'equal numeric IDs cannot collide');
  assertEqual(
    catalogueDomId('mb', 'work', '122') === masterId,
    false,
    'equal IDs from different catalogues cannot collide',
  );
  assertEqual(
    releaseGroupRequestPath('122', 'discogs', 'work'),
    '/api/discogs/master/122',
    'work identity loads the master endpoint',
  );
  assertEqual(
    releaseGroupRequestPath('122', 'discogs', 'release'),
    '/api/discogs/release/122',
    'release identity loads the leaf endpoint',
  );

  const masterHtml = renderRgRow(
    { id: '122', title: 'Master', identity_kind: 'work' },
    { artistName: 'The Rolling Stones', nameLC: 'the rolling stones', source: 'discogs' },
  );
  const releaseHtml = renderRgRow(
    { id: '122', title: 'Release', identity_kind: 'release' },
    { artistName: 'The Rolling Stones', nameLC: 'the rolling stones', source: 'discogs' },
  );
  assertContains(masterHtml, `id="${masterId}"`, 'master renders its own expansion target');
  assertContains(releaseHtml, `id="${releaseId}"`, 'release renders its own expansion target');
  assertContains(masterHtml, 'data-identity-kind="work"', 'master row carries selector identity');
  assertContains(releaseHtml, 'data-identity-kind="release"', 'release row carries selector identity');
  assertContains(masterHtml, "source:'discogs',identityKind:'work'", 'master click preserves endpoint identity');
  assertContains(releaseHtml, "source:'discogs',identityKind:'release'", 'release click preserves endpoint identity');
}

console.log('Discogs DOM identity namespace — generated numeric collision sweep');
{
  for (let id = 1; id <= 1000; id += 37) {
    const master = catalogueDomId('discogs', 'work', id);
    const release = catalogueDomId('discogs', 'release', id);
    assertEqual(master === release, false, `master/release ${id} targets differ`);
    assertEqual(
      releaseGroupRequestPath(id, 'discogs', 'work').includes('/master/'),
      true,
      `master ${id} dispatches to master endpoint`,
    );
    assertEqual(
      releaseGroupRequestPath(id, 'discogs', 'release').includes('/release/'),
      true,
      `release ${id} dispatches to release endpoint`,
    );
  }
  const oldDomId = id => `rel-${id}`;
  assertEqual(oldDomId(122), oldDomId(122), 'known-bad scalar target collides');
  assertEqual(
    catalogueDomId('discogs', 'work', 122) === catalogueDomId('discogs', 'release', 122),
    false,
    'new checker rejects the known-bad collision',
  );
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
