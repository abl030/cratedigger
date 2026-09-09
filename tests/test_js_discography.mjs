/**
 * Unit tests for web/js/discography.js pure helpers.
 *
 * `renderReleaseDetail` is the composed entry `browse.js` calls and this
 * suite drives it. `renderPressingRow` and `statusChipHtml` keep direct
 * calls (issue #1346): neither is a leaf of that entry at all. They hang
 * off `loadReleaseGroup`, which reaches them only after an async fetch,
 * and `statusChipHtml` is a further hop inside `renderPressingRow`.
 * `renderRgRow` belongs to `artist_page.renderArtistSections`, driven in
 * `tests/test_js_artist_page.mjs`.
 *
 * Run with: node tests/test_js_discography.mjs
 */

import {
  addRelease,
  applySearchTargetAfterDiscography,
  catalogueDomId,
  releaseGroupRequestPath,
  renderPressingRow,
  renderReleaseDetail,
  renderRgRow,
  expansionOptsFromRow,
  pairedGroupOf,
  synthesizeMasterlessRow,
  splitPressings,
  statusChipHtml,
} from '../web/js/discography.js';
import { renderYoutubeRescueControl } from '../web/js/youtube_rescue_control.js';
import { invalidateActiveRgs } from '../web/js/active_rgs.js';
import { replaceOfferState } from '../web/js/replace_offer.js';
import { state } from '../web/js/state.js';

import { element, stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

t.section('shared YouTube rescue control');
{
  const html = renderYoutubeRescueControl('release-7', 7, '129bebd8-a7b9-4099-b0bc-545b704e7a95');
  t.contains(html, 'yt-rescue-release-7', 'release detail uses a surface-keyed control id');
  t.contains(html, 'window.checkYoutubeRescue(', 'inline handler uses the shared generic entry point');
  t.excludes(html, '"window.checkYoutubeRescue("release-7"', 'inline handler does not break its HTML attribute quoting');
  t.contains(html, 'Search YouTube', 'control carries an explicit discovery action');
  t.contains(html, 'Check URL', 'control carries an explicit manual URL action');
  t.contains(html, 'video or playlist URL', 'control explains the admitted URL shape');
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

t.section('synthesizeMasterlessRow() — overlay fields survive the synthesis');
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
  t.equal(row.pipeline_status, 'wanted', 'pipeline_status forwarded');
  t.equal(row.pipeline_id, 8838, 'pipeline_id forwarded');
  t.equal(row.processing_owner, null, 'non-processing owner null forwarded');
  t.equal(row.has_captured_history, true, 'captured history forwarded');
  t.equal(row.pipeline_verified_lossless, true, 'verified proof forwarded');
  t.equal(row.pipeline_provisional, false, 'provisional fact forwarded');
  t.equal(row.in_library, false, 'in_library forwarded');
  t.equal(row.beets_album_id, null, 'beets_album_id forwarded');
  t.equal(row.id, '8317023', 'id kept');
  t.equal(row.title, 'Feather Figure/Elastic Bones', 'title kept');
  t.equal(row.format, 'CD', 'formats joined');
  t.equal(row.track_count, 10, 'track count derived');
  t.equal(row.status, 'Official', 'status kept');
}

t.section('synthesizeMasterlessRow() — exact processing owner survives synthesis');
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
  t.equal(row.processing_owner, owner, 'processing_owner object is forwarded unchanged');
  const html = renderPressingRow(row, {
    artistName: 'Deloris',
    rgForReplace: null,
    canReplace: true,
  });
  t.contains(html, 'previewing', 'pressing action consumes canonical owner label');
  t.contains(html, '/api/import-jobs/8839/recovery', 'pressing links exact owner recovery detail');
  t.excludes(html, 'window.disambRemove', 'processing pressing cannot remove request');
}

t.section('Convergence signal — exact pressing badge without duplicate detail action');
{
  const convergence = {
    request_id: 1240,
    latest_qualifying_log_id: 39278,
    cliff_hz: 15000,
    observation_count: 17,
    distinct_peer_count: 17,
    distinct_candidate_snapshot_count: 14,
    distinct_codec_count: 3,
    raw_cliff_min_hz: 14780,
    raw_cliff_max_hz: 15220,
    cliff_spread_hz: 440,
    signal_token: 'a'.repeat(64),
  };
  const row = {
    id: 'exact-release',
    title: 'The Creek Drank the Cradle',
    status: 'Official',
    in_library: true,
    beets_album_id: 11582,
    pipeline_status: 'wanted',
    pipeline_id: 1240,
    pipeline_provisional: true,
    country: 'US',
    date: '2002-09-24',
    format: 'CD',
    track_count: 11,
    convergence,
  };
  const pressingHtml = renderPressingRow(row, {
    artistName: 'Iron & Wine',
    rgForReplace: 'release-group',
    canReplace: true,
  });
  t.contains(pressingHtml, 'search converged', 'exact pressing carries the distinct badge');

  const target = { innerHTML: '' };
  renderReleaseDetail(target, row.id, { ...row, tracks: [] });
  t.excludes(target.innerHTML, 'Search appears converged',
    'Browse release detail does not duplicate the Library/Recents prompt');
  t.excludes(target.innerHTML, 'window.stopConvergedSearch',
    'Browse release detail has no second stop action');
}

t.section('renderPressingRow() carries the Replace key its caller resolved (issue #1355 Batch D residual)');
{
  // ``loadReleaseGroup`` resolves one key per row
  // (``rel.release_group_id || parentRgId``) and hands the SAME value to
  // ``replaceOfferState`` and to this renderer, so the key the offer was
  // decided from is the key the button carries. Recomputing it here from
  // a parent id was the duplication the Batch D residual recorded: one
  // formula spelled twice inside one closure. A future edit to either
  // site alone would split them silently, taking the enabled/disabled
  // decision on one key while the picker opens on another.
  const row = {
    id: '999999',
    title: 'Everything Is Alive',
    status: 'Official',
    country: 'AU',
    date: '2011-05-01',
    format: 'CD',
    track_count: 10,
  };
  const ownOffer = replaceOfferState({
    ownKey: '424242', ownActive: true, lookupFailed: false,
    pairing: 'none', pair: null, pairActive: false, rowSource: 'discogs',
  });
  const html = renderPressingRow(row, {
    artistName: 'Hiatus Kaiyote',
    rgForReplace: '424242',
    offer: ownOffer,
    paired: null,
  });
  t.contains(html, 'releaseGroupId: &quot;424242&quot;',
    'the inverted Replace button opens the picker on the exact key the caller resolved');

  // A masterless Discogs release has no key of its own and reaches an
  // enabled offer only through its pair; the picker then lazy-resolves
  // from an explicit null. Same producer, a world it really emits.
  const paired = { id: 'rg-mb', kind: 'work', source: 'mb', label: 'Choose Your Weapon' };
  const pairedOffer = replaceOfferState({
    ownKey: null, ownActive: false, lookupFailed: false,
    pairing: 'checked', pair: paired, pairActive: true, rowSource: 'discogs',
  });
  t.equal(pairedOffer.enabled, true,
    'the paired world really does produce an enabled offer for a keyless row');
  const keyless = renderPressingRow(row, {
    artistName: 'Hiatus Kaiyote',
    rgForReplace: null,
    offer: pairedOffer,
    paired,
  });
  t.contains(keyless, 'releaseGroupId: null',
    'a row the caller resolved no key for carries an explicit null for the picker to lazy-resolve');

  // The standard-mode branch reads the same key and had no assertion
  // anywhere: nulling it there survived all 29 JS suites (PR #1385
  // review, survivor M8). Its degradation is soft, since the picker
  // lazy-resolves via `POST /api/pipeline/<id>/resolve-rg`, but it costs
  // a round trip and an operator-visible failure mode when that resolve
  // is the one that goes wrong.
  const ownedRow = { ...row, pipeline_status: 'wanted', pipeline_id: 1240 };
  const owned = renderPressingRow(ownedRow, {
    artistName: 'Hiatus Kaiyote',
    rgForReplace: '424242',
    offer: ownOffer,
    paired: null,
  });
  t.contains(owned, 'window.openReplacePicker({sourceRequestId: 1240, releaseGroupId: &quot;424242&quot;',
    'a row that IS the active request opens the standard picker on the same resolved key');
}

t.section('addRelease() — processing exists response exposes exact owner recovery');
{
  const created = [];
  const mounted = [];
  function node(tag = '') {
    const made = element({
      tag,
      isConnected: tag === 'button',
      inserted: mounted,
    });
    created.push(made);
    return made;
  }
  const button = node('button');
  button.textContent = 'Add request';
  const body = node('body');
  body.isConnected = true;
  const documentElement = node('html');
  documentElement.isConnected = true;
  const calls = [];
  const globals = stubGlobals({
    document: {
      activeElement: button,
      body,
      documentElement,
      createElement(tag) { return node(tag); },
      getElementById(id) {
        return created.find(node => node.id === id && node.isConnected) || null;
      },
      querySelectorAll(selector) {
        return selector === '[data-pipeline-request-id="321"]'
          ? [button]
          : [];
      },
    },
    window: {
      scrollX: 0,
      scrollY: 0,
      scrollTo() {},
    },
    fetch: async (url) => {
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
    },
  });

  await addRelease(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    button,
  );

  t.equal(
    button.textContent,
    'previewing',
    'Add exists lock label comes from the exact processing owner',
  );
  t.equal(
    button.getAttribute('data-pipeline-request-id'),
    '321',
    'Add exists lock binds the authoritative request id',
  );
  t.equal(
    created.some(
      node => node.href === '/api/import-jobs/654/recovery',
    ),
    true,
    'Add exists lock links exact owner recovery detail',
  );
  t.equal(
    calls.join(','),
    '/api/pipeline/add,/api/pipeline/321',
    'Add conflict refreshes only the affected request',
  );
  globals.restore();
}

t.section('synthesizeMasterlessRow() — in-library payload keeps quality fields');
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
  t.equal(row.in_library, true, 'in_library true forwarded');
  t.equal(row.beets_album_id, 42, 'beets_album_id forwarded');
  t.equal(row.library_format, 'FLAC', 'library_format forwarded');
  t.equal(row.library_min_bitrate, 900, 'library_min_bitrate forwarded');
  t.equal(row.library_avg_bitrate, 1100, 'library_avg_bitrate forwarded');
  t.equal(row.library_rank, 'lossless', 'library_rank forwarded');
  t.equal(row.has_captured_history, true, 'has_captured_history forwarded');
  t.equal(row.pipeline_verified_lossless, true, 'pipeline_verified_lossless forwarded');
  t.equal(row.pipeline_provisional, false, 'pipeline_provisional forwarded');
  t.equal(row.format, '?', 'empty formats fall back to ?');

  const html = renderPressingRow(row, {
    artistName: 'Artist',
    rgForReplace: null,
    canReplace: false,
  });
  t.contains(html, '>in library</span>', 'masterless row renders current holding');
  t.contains(html, '>F</span>', 'masterless row renders quality independently');
  t.contains(html, '>captured<', 'masterless row renders captured history');
  t.contains(html, '>verified<', 'masterless row renders carried proof');
}

t.section('renderRgRow() — the compare counterpart rides along to loadReleaseGroup as the pair (issue #1366 part 2)');
{
  const row = {
    id: '6f151223-f3a3-3e57-810f-598f7897006c', title: 'Absolution', source: 'mb',
    identity_kind: 'work', first_release_date: '2003-09-15', provenance: ['ordinary'],
    counterpart: {
      id: 11052, title: 'Absolution "Deluxe" <b>', source: 'discogs', identity_kind: 'work',
    },
  };
  const html = renderRgRow(row, { artistName: 'Muse', nameLC: 'muse', pairingChecked: true });
  t.contains(html,
    "window.loadReleaseGroup(&quot;6f151223-f3a3-3e57-810f-598f7897006c&quot;, this, {source:'mb',identityKind:'work',pairing:'checked',paired:{id:&quot;11052&quot;,kind:'work',source:'discogs',label:&quot;Absolution &#92;&quot;Deluxe&#92;&quot; &lt;b&gt;&quot;}})",
    'the onclick carries the pairing state and the pair, free text as escaped JS strings, vocabularies as literals');
  t.contains(html, 'data-paired-id="11052" data-paired-kind="work" data-paired-source="discogs" data-paired-label="Absolution &quot;Deluxe&quot; &lt;b&gt;"',
    'the row also carries the pair as data attributes for the late-compare reload');
  t.contains(html, 'data-pairing="checked"', 'and the pairing state rides on the row');

  const masterlessPair = renderRgRow({
    ...row, counterpart: { id: '3938744', title: 'Fraulein', source: 'discogs', identity_kind: 'release' },
  }, { artistName: 'Deloris', nameLC: 'deloris', pairingChecked: true });
  t.contains(masterlessPair, "paired:{id:&quot;3938744&quot;,kind:'release',source:'discogs',label:&quot;Fraulein&quot;}",
    'a masterless Discogs counterpart is passed as a release, so the expansion looks it up by exact id');

  const unpaired = renderRgRow({ ...row, counterpart: undefined }, { artistName: 'Muse', nameLC: 'muse' });
  t.contains(unpaired, "{source:'mb',identityKind:'work',pairing:'pending'}",
    'no counterpart and no pairingChecked: the expansion is told the pairing is still pending');
  t.excludes(unpaired, 'data-paired-id', 'no pair, no paired attributes');

  t.deepEqual(pairedGroupOf(row), { id: '11052', kind: 'work', source: 'discogs', label: 'Absolution "Deluxe" <b>' },
    'pairedGroupOf normalises the counterpart: string id, validated kind and source, raw label');

  // The row's attributes are what a programmatic reload reads back; they
  // must say exactly what the onclick says.
  const rendered = {
    dataset: {
      catalogueSource: 'mb', identityKind: 'work', catalogueId: row.id, pairing: 'checked',
      pairedId: '11052', pairedKind: 'work', pairedSource: 'discogs', pairedLabel: 'Absolution "Deluxe" <b>',
    },
  };
  t.deepEqual(expansionOptsFromRow(rendered), {
    source: 'mb', identityKind: 'work', masterless: false, pairing: 'checked',
    paired: { id: '11052', kind: 'work', source: 'discogs', label: 'Absolution "Deluxe" <b>' },
  }, 'expansionOptsFromRow reads the pairing state and the pair back off the row');
  t.deepEqual(expansionOptsFromRow({ dataset: { catalogueSource: 'discogs', identityKind: 'release', catalogueId: '3938744', pairing: 'pending' } }), {
    source: 'discogs', identityKind: 'release', masterless: true, pairing: 'pending', paired: null,
  }, 'a pending masterless row reloads as a masterless release with no pair');
  t.deepEqual(expansionOptsFromRow({ dataset: { catalogueSource: 'mb', identityKind: 'work', catalogueId: 'x' } }), {
    source: 'mb', identityKind: 'work', masterless: false, pairing: 'none', paired: null,
  }, 'a row without a pairing attribute is a surface with no compare');
  t.equal(expansionOptsFromRow({ dataset: { pairing: 'bogus', catalogueSource: 'mb' } }).pairing, 'none',
    'an unknown pairing value is never believed to be pending or checked');
  t.deepEqual(expansionOptsFromRow({ dataset: {
    catalogueSource: 'bogus', identityKind: 'bogus', catalogueId: 'x', pairing: 'checked',
    pairedId: '11052', pairedKind: 'bogus', pairedSource: 'bogus', pairedLabel: 'L',
  } }).paired, { id: '11052', kind: 'work', source: 'mb', label: 'L' },
    'unknown pair kind and source fall back to their safe vocabulary values, never interpolated raw');
  t.equal(pairedGroupOf({ counterpart: { source: 'bogus', identity_kind: 'bogus', id: 1, title: 't' } }).kind, 'work',
    'an unknown identity kind is treated as a work');
  t.equal(pairedGroupOf({ counterpart: { source: 'bogus', identity_kind: 'bogus', id: 1, title: 't' } }).source, 'mb',
    'an unknown source falls back to mb, never interpolated raw');
  t.equal(pairedGroupOf({}), null, 'no counterpart, no pair');
}

t.section('splitPressings() — owned/in-flight pressings are never hidden (The Meadowlands pin)');
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
  t.equal(visible.some(r => r.id === 'cef6b0f6'), true, 'owned promo is visible');
  t.equal(hidden.some(r => r.id === 'cef6b0f6'), false, 'owned promo not hidden');
  t.equal(hidden.length, 1, 'only the unowned bootleg is hidden');
  t.equal(hidden[0].id, '2aa0ae0e', 'unowned bootleg stays in the collapsed bucket');
  t.equal(visible.length, 3, 'officials + owned promo visible');
}

t.section('splitPressings() — partition + hoist invariants over the status/ownership space');
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
  t.equal(visible.length + hidden.length, rows.length, 'every row lands in exactly one bucket');
  for (const r of rows) {
    const inVisible = visible.includes(r);
    const inHidden = hidden.includes(r);
    t.equal(inVisible !== inHidden, true, `${r.id} in exactly one bucket`);
    // 'replaced' is the terminal frozen-audit status — an abandoned
    // request is NOT an active claim on the pressing and must not pin it.
    const owned = r.in_library === true
      || (!!r.pipeline_status && r.pipeline_status !== 'replaced');
    const official = r.status === 'Official' || !r.status;
    if (owned || official) {
      t.equal(inVisible, true, `${r.id} (status=${r.status}, owned=${owned}) must be visible`);
    } else {
      t.equal(inHidden, true, `${r.id} (status=${r.status}, unowned non-official) must be hidden`);
    }
  }
}

t.section('splitPressings() — a replaced-only pipeline row does not pin (visible frozen audit badge)');
{
  // Replaced is visible frozen audit history, not an active ownership
  // claim. Only in_library or an ACTIVE pipeline status pins.
  const rows = [
    { id: 'abandoned', status: 'Bootleg', in_library: false, pipeline_status: 'replaced' },
    { id: 'owned-abandoned', status: 'Promotion', in_library: true, pipeline_status: 'replaced' },
  ];
  const { visible, hidden } = splitPressings(rows);
  t.equal(hidden.length, 1, 'unowned replaced bootleg stays collapsed');
  t.equal(hidden[0].id, 'abandoned', 'the replaced-only row is the hidden one');
  t.equal(visible.length, 1, 'library ownership still pins a replaced row');
  t.equal(visible[0].id, 'owned-abandoned', 'in_library wins over replaced');

  const replacedHtml = renderPressingRow(rows[0], {
    artistName: 'Artist',
    rgForReplace: null,
    canReplace: false,
  });
  t.contains(replacedHtml, '>replaced<', 'collapsed row explains its frozen request history');
}

t.section('splitPressings() — known-bad self-check: the OLD split violates the hoist invariant');
{
  // Prove the assertion above actually constrains something: the
  // pre-fix split (status-only) hides the owned promo.
  const rows = [{ id: 'x', status: 'Promotion', in_library: true, pipeline_status: null }];
  const oldHidden = rows.filter(r => r.status && r.status !== 'Official');
  t.equal(oldHidden.length, 1, 'old split hides the owned promo (the bug)');
  t.equal(splitPressings(rows).hidden.length, 0, 'new split does not');
}

t.section('statusChipHtml() — non-official pressings get a provenance chip');
{
  t.equal(statusChipHtml('Official'), '', 'Official -> no chip');
  t.equal(statusChipHtml(''), '', 'empty -> no chip');
  t.equal(statusChipHtml(undefined), '', 'missing -> no chip');
  t.equal(statusChipHtml('Promotion').includes('promo'), true, 'Promotion -> promo chip');
  t.equal(statusChipHtml('Promotion').includes('badge-nonofficial'), true, 'chip uses the nonofficial badge class');
  t.equal(statusChipHtml('Bootleg').includes('bootleg'), true, 'Bootleg -> bootleg chip');
  t.equal(statusChipHtml('Pseudo-Release').includes('pseudo-release'), true, 'other statuses lowercased verbatim');
}

t.section('Release-id onclick arguments — adversarial deterministic pin');
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
  }, { artistName: 'The Wrens', rgForReplace: 'parent', canReplace: false });

  t.contains(rgHtml, `window.loadReleaseGroup(${arg}, this`, 'RG click passes one encoded JS string argument');
  t.excludes(rgHtml, `window.loadReleaseGroup('${id}'`, 'known-bad raw single-quoted RG interpolation is absent');
  t.contains(pressingHtml, `window.toggleReleaseDetail(${arg})`, 'pressing click passes one encoded JS string argument');
  t.excludes(pressingHtml, `window.toggleReleaseDetail('${id}')`, 'known-bad raw single-quoted pressing interpolation is absent');
  t.excludes(pressingHtml, '>Remove from beets</button>', 'unowned pressing omits disabled beets action');
  t.contains(pressingHtml, '>Add request</button>', 'unowned pressing keeps Add request');
  t.contains(pressingHtml, '>Replace</button>', 'unowned pressing keeps Replace');
}

t.section('Release-id onclick arguments — generated critical-character property sweep');
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
    }, { artistName: 'Artist', rgForReplace: 'parent', canReplace: true });
    t.contains(rgHtml, `window.loadReleaseGroup(${arg}, this`, `RG id round-trips safely: ${JSON.stringify(id)}`);
    t.contains(pressingHtml, `window.toggleReleaseDetail(${arg})`, `pressing id round-trips safely: ${JSON.stringify(id)}`);
    t.contains(pressingHtml, 'window.confirmDeleteBeets(42', `owned removal survives: ${JSON.stringify(id)}`);
  }

  const badId = "break'out";
  const oldHandler = `window.toggleReleaseDetail('${badId}')`;
  let oldCompiles = true;
  try { new Function('window', oldHandler); } catch (_) { oldCompiles = false; }
  t.equal(oldCompiles, false, 'known-bad raw interpolation checker rejects apostrophe ID');
}

t.section('Pressing metadata — hostile catalogue values stay text at the caller-owned HTML boundary');
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
  }, { artistName: 'Artist', rgForReplace: 'parent', canReplace: false });
  t.contains(ordinary, 'Australia 2003-06-00 - CD - 13t - Official',
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
  }, { artistName: 'Artist', rgForReplace: 'parent', canReplace: false });
  const escaped = '&lt;img src=x onerror=alert(1)&gt;';
  const metaStart = pressingHtml.indexOf('<div class="release-meta"');
  const metaEnd = pressingHtml.indexOf('</div>', metaStart);
  const metadataHtml = pressingHtml.slice(metaStart, metaEnd);
  t.equal(metadataHtml.split(escaped).length - 1, 5,
    'country, date, format, track count, and status are each escaped exactly once');
  t.excludes(pressingHtml, hostile,
    'hostile pressing metadata cannot create an image element');

  const oldMeta = `${hostile} ${hostile} - ${hostile} - ${hostile}t - ${hostile}`;
  t.contains(oldMeta, hostile,
    'known-bad raw metadata composition admits an image element');
}

t.section('Pressing metadata — generated critical-character property sweep');
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
      }, { artistName: 'Artist', rgForReplace: 'parent', canReplace: false });
      const expectedMeta = `${expectedEsc(metadata.country)} ${expectedEsc(metadata.date)} - ${expectedEsc(metadata.format)} - ${expectedEsc(metadata.track_count)}t - ${expectedEsc(metadata.status)}`;
      t.contains(pressingHtml, expectedMeta,
        `${field} remains escaped text: ${JSON.stringify(value)}`);
    }
  }
}

t.section('Discogs master/release DOM identities stay distinct at equal numeric IDs');
{
  const masterId = catalogueDomId('discogs', 'work', '122');
  const releaseId = catalogueDomId('discogs', 'release', '122');
  t.equal(masterId, 'rel-discogs-work-122', 'master target is namespaced as work');
  t.equal(releaseId, 'rel-discogs-release-122', 'leaf target is namespaced as release');
  t.equal(masterId === releaseId, false, 'equal numeric IDs cannot collide');
  t.equal(
    catalogueDomId('mb', 'work', '122') === masterId,
    false,
    'equal IDs from different catalogues cannot collide',
  );
  t.equal(
    releaseGroupRequestPath('122', 'discogs', 'work'),
    '/api/discogs/master/122',
    'work identity loads the master endpoint',
  );
  t.equal(
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
  t.contains(masterHtml, `id="${masterId}"`, 'master renders its own expansion target');
  t.contains(releaseHtml, `id="${releaseId}"`, 'release renders its own expansion target');
  t.contains(masterHtml, 'data-identity-kind="work"', 'master row carries selector identity');
  t.contains(releaseHtml, 'data-identity-kind="release"', 'release row carries selector identity');
  t.contains(masterHtml, "source:'discogs',identityKind:'work'", 'master click preserves endpoint identity');
  t.contains(releaseHtml, "source:'discogs',identityKind:'release'", 'release click preserves endpoint identity');
}

t.section('Discogs DOM identity namespace — generated numeric collision sweep');
{
  for (let id = 1; id <= 1000; id += 37) {
    const master = catalogueDomId('discogs', 'work', id);
    const release = catalogueDomId('discogs', 'release', id);
    t.equal(master === release, false, `master/release ${id} targets differ`);
    t.equal(
      releaseGroupRequestPath(id, 'discogs', 'work').includes('/master/'),
      true,
      `master ${id} dispatches to master endpoint`,
    );
    t.equal(
      releaseGroupRequestPath(id, 'discogs', 'release').includes('/release/'),
      true,
      `release ${id} dispatches to release endpoint`,
    );
  }
  const oldDomId = id => `rel-${id}`;
  t.equal(oldDomId(122), oldDomId(122), 'known-bad scalar target collides');
  t.equal(
    catalogueDomId('discogs', 'work', 122) === catalogueDomId('discogs', 'release', 122),
    false,
    'new checker rejects the known-bad collision',
  );
}

t.section('applySearchTargetAfterDiscography() — a search-by-ID expansion reads the pairing state and pair off its row (issue #1366 part 2)');
{
  const flush = () => new Promise((resolve) => setTimeout(resolve, 0));
  const RG = '6f151223-f3a3-3e57-810f-598f7897006c';
  const saved = {
    expandId: state.searchTargetExpandId, source: state.searchTargetSource,
    kind: state.searchTargetIdentityKind, browseSource: state.browseSource,
  };
  state.searchTargetExpandId = RG;
  state.searchTargetSource = 'mb';
  state.searchTargetIdentityKind = 'work';
  state.browseSource = 'mb';
  const inner = { innerHTML: '', parentElement: null };
  const row = {
    dataset: {
      catalogueSource: 'mb', identityKind: 'work', catalogueId: RG, pairing: 'checked',
      pairedId: '11052', pairedKind: 'work', pairedSource: 'discogs', pairedLabel: 'Absolution',
    },
    querySelector: (sel) => (sel === '.releases' ? inner : null),
  };
  const rgEl = { querySelectorAll: (sel) => (sel === '.rg' ? [row] : []) };
  invalidateActiveRgs();
  stubGlobals({
    fetch: async (url) => {
      if (String(url).includes('/api/pipeline/active-rgs')) {
        return { ok: true, status: 200, json: async () => ({ release_group_ids: ['11052'], groupless_release_ids: [] }) };
      }
      return { ok: true, status: 200, json: async () => ({ releases: [{
        id: 'fdd45566-5c8b-4beb-8ec3-1b5f93a01319', title: 'Absolution', status: 'Official',
        country: 'JP', date: '2003-09-15', format: 'CD', track_count: 15,
      }] }) };
    },
  });
  applySearchTargetAfterDiscography(rgEl);
  for (let i = 0; i < 4; i++) await flush();
  t.contains(inner.innerHTML, 'pairedGroupId: &quot;11052&quot;',
    'the programmatic expansion carries the row\'s pair to the picker, exactly as a click would');
  t.excludes(inner.innerHTML, 'disabled', 'and the paired master holding a request enables Replace');
  t.excludes(inner.innerHTML, 'could not be checked', 'a checked row is never described as pending');
  state.searchTargetExpandId = saved.expandId;
  state.searchTargetSource = saved.source;
  state.searchTargetIdentityKind = saved.kind;
  state.browseSource = saved.browseSource;
}

t.done();
