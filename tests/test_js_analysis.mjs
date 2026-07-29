/**
 * Unit tests for web/js/analysis.js — the unique-track analysis overlay
 * (issue #575 PR4): chip rendering + recording-dot computation.
 * Run with: node tests/test_js_analysis.mjs
 */

import {
  analysisChipHtml,
  computeRecordingDots,
  disambRemove,
  renderRecordingsBlock,
} from '../web/js/analysis.js';

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

console.log('analysisChipHtml() — coverage precedence');
{
  assertContains(analysisChipHtml({ covered_by: 'Some <Comp>', unique_track_count: 3 }),
    'covered by Some &lt;Comp&gt;', 'covered_by wins over unique count, escaped');
  assertContains(analysisChipHtml({ covered_by: null, unique_track_count: 9 }),
    '9 unique', 'unique count chip');
  assertContains(analysisChipHtml({ covered_by: null, unique_track_count: 0 }),
    '0 unique', 'zero-unique chip');
}

console.log('computeRecordingDots() — membership + exclusives');
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
  assertEqual(totalPressings, 2, 'two pressings');
  assertEqual(trackToPressings['r1'].join(','), '0', 'r1 only on P0');
  assertEqual(trackToPressings['r2'].join(','), '0,1', 'r2 on both');
  assertEqual(trackToPressings['r3'].join(','), '1', 'r3 only on P1');
  assertEqual(pressingExclusiveCounts.join(','), '1,1', 'one exclusive each');
}

console.log('renderRecordingsBlock() — markers stay with titles');
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
  assertContains(html, 'Recordings', 'heading present');
  assertContains(html, 'type-header', 'heading styled as a section header (separates the block from Bootleg / Promo)');
  // Single-span rows: marker and title inside one <span> (the flex
  // justify-between fix from PR3's screenshot loop).
  assertContains(html, '●</span></span>Only On P0', 'dot adjacent to partial-coverage title');
  assertContains(html, '★</span> Everywhere', 'star adjacent to all-pressings title');
  assertContains(html, 'also on: Best Of', 'non-unique row keeps also-on note');
  assertEqual(renderRecordingsBlock({ tracks: [] }), '', 'no tracks -> empty');
}

console.log('disambRemove() — processing conflict locks and refreshes only the acted-on row');
{
  const oldConfirm = globalThis.confirm;
  const oldDocument = globalThis.document;
  const oldFetch = globalThis.fetch;
  const oldWindow = globalThis.window;
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
  globalThis.confirm = () => true;
  globalThis.document = {
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
  };
  globalThis.window = { scrollX: 5, scrollY: 9, scrollTo() {} };
  const calls = [];
  globalThis.fetch = async (url) => {
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
  };
  await disambRemove(903, btn);
  assertEqual(calls.join(','), '/api/pipeline/delete,/api/pipeline/903',
    'typed conflict refetches only the affected request');
  assertEqual(attributes.get('aria-disabled'), 'true', 'remove control becomes aria-disabled');
  assertEqual(btn.textContent, 'importing', 'authoritative owner status replaces stale action');
  assertEqual(live.textContent.includes('job #70'), true, 'owner change is announced');
  globalThis.confirm = oldConfirm;
  globalThis.document = oldDocument;
  globalThis.fetch = oldFetch;
  globalThis.window = oldWindow;
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
