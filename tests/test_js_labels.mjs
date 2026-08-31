/**
 * Unit tests for web/js/labels.js label-search render + click wiring,
 * and the label detail page composition (renderLabelDetail).
 * Run with: node tests/test_js_labels.mjs
 */

import {
  BIG_LABEL_THRESHOLD,
  openLabelDetailFromList,
  renderLabelDetail,
  renderLabelSearchResults,
} from '../web/js/labels.js';
import { state } from '../web/js/state.js';

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

// --- renderLabelDetail: the detail page composition (#1278 wx3 residual 2) ---

/** Build the container/body pair renderLabelDetail needs. */
function makeDetailContainer() {
  const body = { innerHTML: '' };
  const container = {
    innerHTML: '',
    querySelector: (selector) => (selector === '#browse-label-rows' ? body : null),
  };
  return { container, body };
}

console.log('renderLabelDetail() composes header, filters, rows and pagination with exact window.* wiring');
{
  state.labelFilters = { yearMin: null, yearMax: null, format: '', hideHeld: false };
  const { container, body } = makeDetailContainer();
  renderLabelDetail(container, {
    label: { id: 42, name: 'Sarah Records', release_count: 7, country: 'UK' },
    releases: [
      { id: '101', title: 'Pristine Christine', artist_name: 'The Sea Urchins', date: '1987', format: 'Vinyl', in_library: false },
      { id: '102', title: 'Emma’s House', artist_name: 'The Field Mice', date: '1988', format: 'Vinyl', in_library: false, sub_label_name: 'Sha-la-la' },
    ],
    pagination: { items: 12, pages: 5, page: 2 },
    include_sublabels: true,
  });
  assertContains(container.innerHTML, 'Sarah Records', 'header renders the label name');
  // The P2 #2 header-count fix: pagination.items (12) wins over the
  // entity release_count (7).
  assertContains(container.innerHTML, '12 releases', 'header count comes from pagination.items, not release_count');
  assertContains(container.innerHTML, 'Page 2 of 5', 'multi-page world renders its page-position note');
  // Exact handler wiring (#1110/#1241 argument-inversion class).
  assertContains(container.innerHTML, 'oninput="window.onLabelYearFilterInput()"', 'year inputs wire the debounced year handler');
  assertContains(container.innerHTML, 'onchange="window.onLabelFilterChange()"', 'format/hide-held controls wire the filter handler');
  assertContains(container.innerHTML, 'window.goToLabelPage(1)', 'prev button targets page 1');
  assertContains(container.innerHTML, 'window.goToLabelPage(3)', 'next button targets page 3');
  assert(!container.innerHTML.includes('toggleLabelIncludeSublabels'),
    'a label under BIG_LABEL_THRESHOLD renders no include-sublabels toggle');
  assert(!container.innerHTML.includes('Sub-labels unavailable'),
    'no dropped-sublabels banner when sub_labels_dropped is absent');
  // Rows really render through renderLabelRows into the body slot.
  assertContains(body.innerHTML, 'Pristine Christine', 'release rows render into #browse-label-rows');
  assertContains(body.innerHTML, 'via Sha-la-la', 'sub-label badge renders when any row carries one');
  // The stash contract filter re-renders and goToLabelPage read back.
  assert(container._releases.length === 2, 'container stashes the release list');
  assert(container._totalCount === 12, 'container stashes the pagination total');
  assert(container._labelId === '42' && container._labelName === 'Sarah Records',
    'container stashes String(id) and name');
  assert(container._includeSub === true, 'container stashes include_sublabels');
  assert(container._hasAnySubLabel === true, 'container stashes the sub-label presence flag');
}

console.log('renderLabelDetail() big-label + dropped-sublabels arms');
{
  state.labelFilters = { yearMin: null, yearMax: null, format: '', hideHeld: false };
  const { container } = makeDetailContainer();
  renderLabelDetail(container, {
    label: { id: 9, name: 'Warp' },
    releases: [
      { id: '201', title: 'Artificial Intelligence', date: '1992', format: 'CD', in_library: false },
    ],
    pagination: { items: BIG_LABEL_THRESHOLD + 1, pages: 1, page: 1 },
    include_sublabels: false,
    sub_labels_dropped: true,
  });
  assertContains(container.innerHTML,
    'onchange="window.toggleLabelIncludeSublabels(this.checked)"',
    'big label wires the include-sublabels toggle to its exact handler');
  assert(!container.innerHTML.includes('this.checked)" checked'),
    'include_sublabels=false renders the toggle unchecked');
  assertContains(container.innerHTML, 'Sub-labels unavailable',
    'sub_labels_dropped renders the degraded-catalogue banner');
  assert(!container.innerHTML.includes('Page 1 of 1'),
    'single-page world renders no page-position note');
  assert(container._includeSub === false, 'include_sublabels=false stashes false');
}

console.log('renderLabelDetail() falls back to the entity count when pagination is missing');
{
  state.labelFilters = { yearMin: null, yearMax: null, format: '', hideHeld: false };
  const { container } = makeDetailContainer();
  renderLabelDetail(container, {
    label: { id: 3, name: 'Fallback Label', release_count: 4 },
    releases: [
      { id: '301', title: 'Only Release', date: '1999', format: 'CD', in_library: false },
    ],
  });
  assertContains(container.innerHTML, '4 releases', 'missing pagination falls back to label.release_count');
  assert(container._totalCount === 4, 'stash carries the fallback total');
  assert(!container.innerHTML.includes('goToLabelPage'),
    'no pagination controls without a pagination payload');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
