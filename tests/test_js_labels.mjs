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

import { suite } from './js_harness.mjs';

const t = suite(import.meta.url);

t.section('renderLabelSearchResults() wires each row through window.openLabelDetailFromList by index');
{
  const containerEl = { innerHTML: '' };
  renderLabelSearchResults(containerEl, [
    { id: 7, name: 'First Label' },
    { id: 9, name: 'Second Label' },
  ], () => {});
  // Exact handler + argument order (#1110/#1241 argument-inversion class):
  // the row element lookup, then the hit index into the stashed array.
  t.contains(containerEl.innerHTML,
    "onclick=\"window.openLabelDetailFromList(this.closest('.artist'), 0)\"",
    'first row resolves index 0');
  t.contains(containerEl.innerHTML,
    "onclick=\"window.openLabelDetailFromList(this.closest('.artist'), 1)\"",
    'second row resolves index 1');
  t.contains(containerEl.innerHTML, 'Second Label', 'row renders the hit name');
  // Assert the stash in THIS block too — the click-resolution block below
  // reads the same key, but this render contract must hold on its own.
  t.ok(/** @type {any} */ (containerEl)._labelHits?.length === 2,
    'render stashes the hits array the click resolver reads');
}

t.section('openLabelDetailFromList() resolves the stashed hit and calls the handler as (id, name)');
{
  const containerEl = { innerHTML: '' };
  const calls = [];
  renderLabelSearchResults(containerEl, [
    { id: 7, name: 'First Label' },
    { id: 9, name: 'Second Label' },
  ], (labelId, labelName) => calls.push([labelId, labelName]));
  openLabelDetailFromList({ parentElement: containerEl }, 1);
  t.ok(calls.length === 1, 'one click resolves exactly one handler call');
  t.ok(calls[0][0] === '9' && calls[0][1] === 'Second Label',
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
  t.ok(!threw, 'an out-of-range click never throws');
  t.ok(calls.length === 1, 'an out-of-range index never reaches the handler');
}

t.section('renderLabelSearchResults() renders the empty state without stashing hits');
{
  const containerEl = { innerHTML: '' };
  renderLabelSearchResults(containerEl, [], () => {});
  t.contains(containerEl.innerHTML, 'No label results', 'empty search renders its empty state');
  t.ok(/** @type {any} */ (containerEl)._labelHits === undefined,
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

t.section('renderLabelDetail() composes header, filters, rows and pagination with exact window.* wiring');
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
  t.contains(container.innerHTML, 'Sarah Records', 'header renders the label name');
  // The P2 #2 header-count fix: pagination.items (12) wins over the
  // entity release_count (7).
  t.contains(container.innerHTML, '12 releases', 'header count comes from pagination.items, not release_count');
  // The em-dash "N releases total" suffix distinguishes renderLabelDetail's
  // own page-position note from renderPaginationControls' "Page X of Y"
  // span (reader finding: the bare string matches both producers, so a
  // deleted note survived it).
  t.contains(container.innerHTML, 'Page 2 of 5 — 12 releases total',
    'multi-page world renders its page-position note');
  // Exact handler wiring (#1110/#1241 argument-inversion class).
  t.contains(container.innerHTML, 'oninput="window.onLabelYearFilterInput()"', 'year inputs wire the debounced year handler');
  t.contains(container.innerHTML, 'onchange="window.onLabelFilterChange()"', 'format/hide-held controls wire the filter handler');
  t.contains(container.innerHTML, 'window.goToLabelPage(1)', 'prev button targets page 1');
  t.contains(container.innerHTML, 'window.goToLabelPage(3)', 'next button targets page 3');
  t.excludes(container.innerHTML, 'toggleLabelIncludeSublabels',
    'a label under BIG_LABEL_THRESHOLD renders no include-sublabels toggle');
  t.excludes(container.innerHTML, 'Sub-labels unavailable',
    'no dropped-sublabels banner when sub_labels_dropped is absent');
  // Rows really render through renderLabelRows into the body slot.
  t.contains(body.innerHTML, 'Pristine Christine', 'release rows render into #browse-label-rows');
  t.contains(body.innerHTML, 'via Sha-la-la', 'sub-label badge renders when any row carries one');
  // The stash contract filter re-renders and goToLabelPage read back.
  t.ok(container._releases.length === 2, 'container stashes the release list');
  t.ok(container._totalCount === 12, 'container stashes the pagination total');
  t.ok(container._labelId === '42' && container._labelName === 'Sarah Records',
    'container stashes String(id) and name');
  t.ok(container._includeSub === true, 'container stashes include_sublabels');
  t.ok(container._hasAnySubLabel === true, 'container stashes the sub-label presence flag');
}

t.section('renderLabelDetail() big-label + dropped-sublabels arms');
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
  t.contains(container.innerHTML,
    'onchange="window.toggleLabelIncludeSublabels(this.checked)"',
    'big label wires the include-sublabels toggle to its exact handler');
  // `checked` renders BEFORE the onchange attribute, so pin the substring
  // that actually distinguishes the arms (reader finding: the original
  // pin quoted a string neither arm produces and was inert both ways).
  t.excludes(container.innerHTML, 'label-include-sublabels" checked',
    'include_sublabels=false renders the toggle unchecked');
  t.contains(container.innerHTML, 'Sub-labels unavailable',
    'sub_labels_dropped renders the degraded-catalogue banner');
  t.excludes(container.innerHTML, 'Page 1 of 1',
    'single-page world renders no page-position note');
  t.ok(container._includeSub === false, 'include_sublabels=false stashes false');
  // The positive arm: re-render with include_sublabels on and the same
  // distinguishing substring must appear (kills a hard-coded '' mutant
  // the negative pin alone cannot see).
  const rerender = makeDetailContainer();
  renderLabelDetail(rerender.container, {
    label: { id: 9, name: 'Warp' },
    releases: [
      { id: '201', title: 'Artificial Intelligence', date: '1992', format: 'CD', in_library: false },
    ],
    pagination: { items: BIG_LABEL_THRESHOLD + 1, pages: 1, page: 1 },
    include_sublabels: true,
  });
  t.contains(rerender.container.innerHTML, 'label-include-sublabels" checked',
    'include_sublabels=true renders the toggle checked');
}

t.section('renderLabelDetail() falls back to the entity count when pagination is missing');
{
  state.labelFilters = { yearMin: null, yearMax: null, format: '', hideHeld: false };
  const { container } = makeDetailContainer();
  renderLabelDetail(container, {
    label: { id: 3, name: 'Fallback Label', release_count: 4 },
    releases: [
      { id: '301', title: 'Only Release', date: '1999', format: 'CD', in_library: false },
    ],
  });
  t.contains(container.innerHTML, '4 releases', 'missing pagination falls back to label.release_count');
  t.ok(container._totalCount === 4, 'stash carries the fallback total');
  t.excludes(container.innerHTML, 'goToLabelPage',
    'no pagination controls without a pagination payload');
}

t.done();
