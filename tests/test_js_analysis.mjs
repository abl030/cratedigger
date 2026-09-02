/**
 * Unit tests for web/js/analysis.js — the unique-track analysis overlay
 * (issue #575 PR4): chip rendering + recording-dot computation.
 * Run with: node tests/test_js_analysis.mjs
 */

import {
  analysisChipHtml,
  applyAnalysisChips,
  applyAnalysisToExpansion,
  computeRecordingDots,
  disambRemove,
  renderRecordingsBlock,
} from '../web/js/analysis.js';
import { state } from '../web/js/state.js';

import { element, stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

t.section('analysisChipHtml() — coverage precedence');
{
  t.contains(analysisChipHtml({ covered_by: 'Some <Comp>', unique_track_count: 3 }),
    'covered by Some &lt;Comp&gt;', 'covered_by wins over unique count, escaped');
  t.contains(analysisChipHtml({ covered_by: null, unique_track_count: 9 }),
    '9 unique', 'unique count chip');
  t.contains(analysisChipHtml({ covered_by: null, unique_track_count: 0 }),
    '0 unique', 'zero-unique chip');
}

t.section('applyAnalysisChips() — one DOM index for a large catalogue');
{
  let wholeDocumentQueries = 0;
  const inserted = [];
  const row = id => ({
    dataset: { rgId: id },
    querySelector(selector) {
      if (selector === '.disamb-chip') return null;
      if (selector === '.rg-title') return {
        insertAdjacentHTML(_where, html) { inserted.push(`${id}:${html}`); },
      };
      throw new Error(`unexpected row query ${selector}`);
    },
  });
  const rows = Array.from({ length: 300 }, (_unused, index) => row(`rg-${index}`));
  const container = {
    querySelectorAll(selector) {
      t.equal(selector, '.rg[data-rg-id]', 'index selects all release-group rows once');
      wholeDocumentQueries++;
      return rows;
    },
    querySelector() { throw new Error('per-chip document scan regressed'); },
  };
  applyAnalysisChips(container, {
    release_groups: rows.map((entry, index) => ({
      release_group_id: entry.dataset.rgId, covered_by: null, unique_track_count: index,
    })),
  });
  t.equal(wholeDocumentQueries, 1, 'one catalogue-wide DOM query');
  t.equal(inserted.length, 300, 'every indexed group receives its chip');
}

t.section('computeRecordingDots() — membership + exclusives');
{
  // Two pressings: P0 has r1,r2; P1 has r2,r3. r1 exclusive to P0,
  // r3 exclusive to P1, r2 shared.
  const rg = {
    pressings: [
      { release_id: 'p0', recording_ids: ['r1', 'r2'] },
      { release_id: 'p1', recording_ids: ['r2', 'r3'] },
    ],
    tracks: [
      { recording_id: 'r1', title: 'One', unique: true },
      { recording_id: 'r2', title: 'Two', unique: true },
      { recording_id: 'r3', title: 'Three', unique: true },
    ],
  };
  const { trackToPressings, pressingExclusiveCounts, totalPressings } = computeRecordingDots(rg);
  t.equal(totalPressings, 2, 'two pressings');
  t.equal(trackToPressings['r1'].join(','), '0', 'r1 only on P0');
  t.equal(trackToPressings['r2'].join(','), '0,1', 'r2 on both');
  t.equal(trackToPressings['r3'].join(','), '1', 'r3 only on P1');
  t.equal(pressingExclusiveCounts.join(','), '1,1', 'one exclusive each');
}

t.section('renderRecordingsBlock() — markers stay with titles');
{
  const rg = {
    pressings: [
      { release_id: 'p0', recording_ids: ['r1', 'r2'] },
      { release_id: 'p1', recording_ids: ['r2'] },
    ],
    tracks: [
      { recording_id: 'r1', title: 'Only On P0', unique: true },
      { recording_id: 'r2', title: 'Everywhere', unique: true },
      { recording_id: 'r9', title: 'Comp Track', unique: false, also_on: ['Best Of'] },
    ],
  };
  const html = renderRecordingsBlock(rg);
  t.contains(html, 'Recordings', 'heading present');
  t.contains(html, 'type-header', 'heading styled as a section header (separates the block from Bootleg / Promo)');
  // Single-span rows: marker and title inside one <span> (the flex
  // justify-between fix from PR3's screenshot loop).
  t.contains(html, '●</span></span>Only On P0', 'dot adjacent to partial-coverage title');
  t.contains(html, '★</span> Everywhere', 'star adjacent to all-pressings title');
  t.contains(html, 'also on: Best Of', 'non-unique row keeps also-on note');
  t.equal(renderRecordingsBlock({ tracks: [] }), '', 'no tracks -> empty');
}

t.section('disambRemove() — processing conflict locks and refreshes only the acted-on row');
{
  const inserted = [];
  const btn = element({
    textContent: 'Remove request',
    isConnected: true,
    insertAdjacentElement(_position, child) {
      child.isConnected = true;
      inserted.push(child);
    },
  });
  btn.setAttribute('data-pipeline-request-id', '903');
  const live = element();
  const calls = [];
  const globals = stubGlobals({
    confirm: () => true,
    document: {
      activeElement: btn,
      body: element({ isConnected: true }),
      createElement() { return element(); },
      getElementById(id) {
        if (id === 'processing-lock-live-region') return live;
        return inserted.find(node => node.id === id && node.isConnected) || null;
      },
      querySelectorAll() { return [btn]; },
    },
    window: { scrollX: 5, scrollY: 9, scrollTo() {} },
    fetch: async (url) => {
      calls.push(String(url));
      if (url === '/api/pipeline/delete') {
        return {
          status: 409,
          async json() {
            return {
              error: 'processing_locked',
              request_id: 903,
              processing_owner: {
                job_id: 70,
                status: 'queued',
                preview_status: 'waiting',
              },
            };
          },
        };
      }
      if (url === '/api/pipeline/903') {
        return {
          ok: true,
          async json() {
            return {
              request: {
                id: 903,
                status: 'processing',
                mb_release_id: 'analysis-owner',
                processing_owner: {
                  job_id: 70,
                  status: 'running',
                  preview_status: 'evidence_ready',
                },
              },
            };
          },
        };
      }
      throw new Error(`unexpected fetch ${url}`);
    },
  });
  await disambRemove(903, btn);
  t.equal(calls.join(','), '/api/pipeline/delete,/api/pipeline/903',
    'typed conflict refetches only the affected request');
  t.equal(btn.getAttribute('aria-disabled'), 'true', 'remove control becomes aria-disabled');
  t.equal(btn.textContent, 'importing', 'authoritative owner status replaces stale action');
  t.equal(live.textContent.includes('job #70'), true, 'owner change is announced');
  globals.restore();
}

// ---------------------------------------------------------------------------
// The composed entry (issue #1346).
//
// `applyAnalysisToExpansion` is what `discography.js` calls. It reads the
// release group out of `state.disambData`, then walks the pressings by
// INDEX: pressing i gets colour i and the exclusive count at
// `pressingExclusiveCounts[i]`. Nothing in the leaf tests above touches
// that alignment — `computeRecordingDots` returns the counts as an array
// and `renderRecordingsBlock` renders its own dots — so swapping the two
// index reads apart is invisible until an operator sees an orange dot
// labelled with the blue pressing's count.
// ---------------------------------------------------------------------------

/** A release-title node recording what the entry writes into it. */
function titleNode() {
  return element({
    before: '',
    after: '',
    insertAdjacentHTML(position, html) {
      if (position === 'afterbegin') this.before += html;
      else this.after += html;
    },
  });
}

/**
 * The expansion element `discography.js` hands the entry: it resolves a
 * release row by `data-release-id`, and each row resolves its own title.
 */
function expansionFor(titles, { alreadyApplied = false } = {}) {
  const appended = [];
  return {
    appended,
    el: element({
      querySelector(selector) {
        if (selector === '.disamb-recordings') {
          return alreadyApplied ? { marker: 'block already present' } : null;
        }
        // Match the whole selector production spells, not just the id
        // inside it: an earlier version keyed on `data-release-id="…"`
        // alone, so changing `.release[` to anything else in production
        // still resolved the row (PR #1352 reader, F12).
        const match = /^\.release\[data-release-id="([^"]+)"\]$/.exec(selector);
        if (!match) return null;
        const title = titles[match[1]];
        if (!title) return null;
        return {
          // Likewise: the row must be asked for `.release-title`, not for
          // whatever selector happens to arrive.
          querySelector(inner) {
            return inner === '.release-title' ? title : null;
          },
        };
      },
      insertAdjacentHTML(_position, html) { appended.push(html); },
    }),
  };
}

t.section('applyAnalysisToExpansion() pairs each pressing colour with its own exclusive count');
{
  // Two pressings, deliberately lopsided: P0 owns two exclusive recordings
  // and P1 owns one, so a swapped index shows "1 exclusive" in blue.
  const rg = {
    release_group_id: 'rg-analysis',
    pressings: [
      { release_id: 'rel-0', recording_ids: ['r1', 'r2', 'shared'] },
      { release_id: 'rel-1', recording_ids: ['r3', 'shared'] },
    ],
    tracks: [
      { recording_id: 'r1', title: 'Only P0 one' },
      { recording_id: 'r2', title: 'Only P0 two' },
      { recording_id: 'r3', title: 'Only P1' },
      { recording_id: 'shared', title: 'On both' },
    ],
  };
  const titles = { 'rel-0': titleNode(), 'rel-1': titleNode() };
  const expansion = expansionFor(titles);
  const globals = stubGlobals({ document: { querySelector: () => null } });
  state.disambData = { release_groups: [rg] };

  applyAnalysisToExpansion(expansion.el, 'rg-analysis');

  // The counts come from the production helper, not from arithmetic done
  // here, so the pin cannot drift from what the entry actually reads.
  const { pressingExclusiveCounts } = computeRecordingDots(rg);
  t.deepEqual(pressingExclusiveCounts, [2, 1],
    'the fixture gives the two pressings different exclusive counts');

  t.contains(titles['rel-0'].before, '#6af',
    'the first pressing takes the first colour');
  t.contains(titles['rel-1'].before, '#fa6',
    'the second pressing takes the second colour');
  t.contains(titles['rel-0'].after, '2 exclusive',
    'the first pressing is labelled with its OWN exclusive count');
  t.contains(titles['rel-1'].after, '1 exclusive',
    'the second pressing is labelled with its own count');
  t.contains(titles['rel-0'].after, '#6af',
    'the exclusive label is coloured to match its pressing dot');
  t.equal(expansion.appended.length, 1, 'the recordings block is appended once');
  t.contains(expansion.appended[0], 'disamb-recordings',
    'the appended block carries the marker class the re-entry guard reads');

  globals.restore();
  state.disambData = null;
}

t.section('applyAnalysisToExpansion() withholds a zero exclusive count and re-entry');
{
  // Both pressings carry the same single recording, so neither owns
  // anything exclusively and the `exCount > 0` gate must suppress both
  // labels — while the colour dots still render.
  const rg = {
    release_group_id: 'rg-shared',
    pressings: [
      { release_id: 'rel-0', recording_ids: ['shared'] },
      { release_id: 'rel-1', recording_ids: ['shared'] },
    ],
    tracks: [{ recording_id: 'shared', title: 'On both' }],
  };
  const titles = { 'rel-0': titleNode(), 'rel-1': titleNode() };
  const expansion = expansionFor(titles);
  const globals = stubGlobals({ document: { querySelector: () => null } });
  state.disambData = { release_groups: [rg] };

  applyAnalysisToExpansion(expansion.el, 'rg-shared');
  t.contains(titles['rel-0'].before, '●', 'a pressing with no exclusives still gets its dot');
  t.excludes(titles['rel-0'].after, 'exclusive', 'zero exclusives renders no label');
  t.excludes(titles['rel-1'].after, 'exclusive', 'and neither does the other pressing');

  // Re-entry: an expansion already carrying the block is left alone, which
  // is what stops a second dot arriving on every re-render. The fixture is
  // otherwise complete — same titles, same rows — so removing the guard
  // makes these two assertions fail by name rather than crashing on a
  // half-built stub.
  const reTitles = { 'rel-0': titleNode(), 'rel-1': titleNode() };
  const reEntered = expansionFor(reTitles, { alreadyApplied: true });
  applyAnalysisToExpansion(reEntered.el, 'rg-shared');
  t.equal(reTitles['rel-0'].before, '',
    'a second pass writes no second dot into the release title');
  t.equal(reEntered.appended.length, 0,
    'a second pass appends no second recordings block');

  // An unknown release group is a no-op, not a crash.
  applyAnalysisToExpansion(expansion.el, 'rg-does-not-exist');
  t.equal(expansion.appended.length, 1, 'an unknown release group appends nothing');

  globals.restore();
  state.disambData = null;
}

t.done();
