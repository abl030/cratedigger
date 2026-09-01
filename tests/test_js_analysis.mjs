/**
 * Unit tests for web/js/analysis.js — the unique-track analysis overlay
 * (issue #575 PR4): chip rendering + recording-dot computation.
 * Run with: node tests/test_js_analysis.mjs
 */

import {
  analysisChipHtml,
  applyAnalysisChips,
  computeRecordingDots,
  disambRemove,
  renderRecordingsBlock,
} from '../web/js/analysis.js';

import { stubGlobals, suite } from './js_harness.mjs';

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
  const attributes = new Map([['data-pipeline-request-id', '903']]);
  const inserted = [];
  const btn = {
    dataset: {},
    disabled: false,
    textContent: 'Remove request',
    style: {},
    isConnected: true,
    setAttribute(name, value) { attributes.set(name, value); },
    removeAttribute(name) { attributes.delete(name); },
    getAttribute(name) { return attributes.get(name) || null; },
    focus() {},
    insertAdjacentElement(_position, element) {
      element.isConnected = true;
      inserted.push(element);
    },
  };
  const live = { textContent: '', setAttribute() {} };
  const calls = [];
  const globals = stubGlobals({
    confirm: () => true,
    document: {
      activeElement: btn,
      body: { appendChild() {} },
      createElement() {
        return {
          children: [],
          className: '',
          id: '',
          textContent: '',
          isConnected: false,
          setAttribute() {},
          appendChild(child) { this.children.push(child); },
          remove() { this.isConnected = false; },
        };
      },
      getElementById(id) {
        if (id === 'processing-lock-live-region') return live;
        return inserted.find(element => element.id === id && element.isConnected) || null;
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
  t.equal(attributes.get('aria-disabled'), 'true', 'remove control becomes aria-disabled');
  t.equal(btn.textContent, 'importing', 'authoritative owner status replaces stale action');
  t.equal(live.textContent.includes('job #70'), true, 'owner change is announced');
  globals.restore();
}

t.done();
