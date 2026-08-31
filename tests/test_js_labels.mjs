/**
 * Unit tests for web/js/labels.js label-search render + click wiring.
 * Run with: node tests/test_js_labels.mjs
 */

import { openLabelDetailFromList, renderLabelSearchResults } from '../web/js/labels.js';

let passed = 0;
let failed = 0;

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - '${needle}' not in output`);
  }
}

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
  }
}

console.log('renderLabelSearchResults() wires each row through window.openLabelDetailFromList by index');
{
  const containerEl = { innerHTML: '' };
  renderLabelSearchResults(containerEl, [
    { id: 7, name: 'First Label' },
    { id: 9, name: 'Second Label' },
  ], () => {});
  // Exact handler + argument order (#1110/#1241 argument-inversion class):
  // the row element lookup, then the hit index into the stashed array.
  assertContains(containerEl.innerHTML,
    "onclick=\"window.openLabelDetailFromList(this.closest('.artist'), 0)\"",
    'first row resolves index 0');
  assertContains(containerEl.innerHTML,
    "onclick=\"window.openLabelDetailFromList(this.closest('.artist'), 1)\"",
    'second row resolves index 1');
  assertContains(containerEl.innerHTML, 'Second Label', 'row renders the hit name');
  // Assert the stash in THIS block too — the click-resolution block below
  // reads the same key, but this render contract must hold on its own.
  assert(/** @type {any} */ (containerEl)._labelHits?.length === 2,
    'render stashes the hits array the click resolver reads');
}

console.log('openLabelDetailFromList() resolves the stashed hit and calls the handler as (id, name)');
{
  const containerEl = { innerHTML: '' };
  const calls = [];
  renderLabelSearchResults(containerEl, [
    { id: 7, name: 'First Label' },
    { id: 9, name: 'Second Label' },
  ], (labelId, labelName) => calls.push([labelId, labelName]));
  openLabelDetailFromList({ parentElement: containerEl }, 1);
  assert(calls.length === 1, 'one click resolves exactly one handler call');
  assert(calls[0][0] === '9' && calls[0][1] === 'Second Label',
    'handler receives (String(id), String(name)) for the clicked index, in order');
  // The missing-hit guard must make an out-of-range click a quiet no-op:
  // assert both halves explicitly so removing the guard fails these
  // assertions rather than crashing before they evaluate.
  let threw = false;
  try {
    openLabelDetailFromList({ parentElement: containerEl }, 5);
  } catch (_e) {
    threw = true;
  }
  assert(!threw, 'an out-of-range click never throws');
  assert(calls.length === 1, 'an out-of-range index never reaches the handler');
}

console.log('renderLabelSearchResults() renders the empty state without stashing hits');
{
  const containerEl = { innerHTML: '' };
  renderLabelSearchResults(containerEl, [], () => {});
  assertContains(containerEl.innerHTML, 'No label results', 'empty search renders its empty state');
  assert(/** @type {any} */ (containerEl)._labelHits === undefined,
    'empty search never stashes hits on the container');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
