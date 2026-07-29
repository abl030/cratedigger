/**
 * Unit tests for the shared browse action state + pure HTML renderers.
 * Run with: node tests/test_js_release_actions.mjs
 */

import { pipelineStore, updatePipelineStatus } from '../web/js/state.js';
import {
  buildReleaseActionState,
  handleProcessingLockedConflict,
  processingConflictFromResponse,
  processingOwnerPresentation,
} from '../web/js/release_action_state.js';
import {
  renderActionToolbar,
  renderAcquireActionButton,
  renderRemoveFromBeetsButton,
  suppressProcessingAction,
} from '../web/js/release_actions.js';

let passed = 0;
let failed = 0;

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — '${needle}' not in output`);
  }
}

function assertExcludes(haystack, needle, msg) {
  if (!haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — unexpectedly found '${needle}'`);
  }
}

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected '${expected}', got '${actual}'`);
  }
}

function clearStore() {
  pipelineStore.clear();
}

console.log('Acquire button — fresh row → Add request enabled');
clearStore();
{
  const state = buildReleaseActionState({ id: 'rel-1', in_library: false });
  const html = renderActionToolbar(state);
  assertContains(html, '>Add request</button>', 'shows Add request label');
  assertContains(html, 'window.addRelease(&quot;rel-1&quot;', 'Add wired up');
  assertExcludes(html, '>Upgrade</button>', 'no Upgrade in this state');
  assertExcludes(html, '>Remove request</button>', 'no Remove request in this state');
  assertContains(html, '>Remove from beets</button>', 'Remove from beets always rendered');
  assertExcludes(html, 'window.confirmDeleteBeets', 'Remove from beets disabled');
}

console.log('Acquire button — in library, no pipeline → Upgrade enabled');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-2',
    in_library: true,
    beets_album_id: 42,
    artist: 'Bodyjar',
    album: 'Plastic Skies',
    track_count: 12,
  });
  const html = renderActionToolbar(state);
  assertContains(html, '>Upgrade</button>', 'shows Upgrade label');
  assertContains(html, 'window.upgradeAlbum(&quot;rel-2&quot;', 'Upgrade wired up');
  assertExcludes(html, '>Add request</button>', 'no Add request');
  assertExcludes(html, '>Remove request</button>', 'no Remove request');
  assertContains(html, 'window.confirmDeleteBeets(42', 'Remove from beets enabled');
  assertContains(html, ', null, &quot;rel-2&quot;)', 'release id passed to delete confirm');
  assertContains(html, '&quot;Bodyjar&quot;', 'artist passed to delete confirm');
  assertContains(html, '&quot;Plastic Skies&quot;', 'album passed to delete confirm');
}

console.log('Acquire button — in library + wanted → Remove request');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-3',
    in_library: true,
    beets_album_id: 42,
    pipeline_status: 'wanted',
    pipeline_id: 1712,
  });
  const html = renderActionToolbar(state);
  assertContains(html, '>Remove request</button>', 'wanted → Remove request');
  assertContains(html, 'window.disambRemove(1712', 'Remove wired up');
  assertExcludes(html, '>Upgrade</button>', 'no Upgrade — wanted wins');
  assertContains(html, 'window.confirmDeleteBeets(42', 'Remove from beets still enabled');
  assertContains(html, ', 1712, &quot;rel-3&quot;)', 'pipeline context passed to delete confirm');
}

console.log('Acquire button — not in library + wanted → Remove request');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-4',
    in_library: false,
    pipeline_status: 'wanted',
    pipeline_id: 200,
  });
  const html = renderActionToolbar(state);
  assertContains(html, '>Remove request</button>', 'wanted (no library) → Remove request');
  assertContains(html, 'window.disambRemove(200', 'Remove wired up');
  assertExcludes(html, '>Add request</button>', 'no Add request when wanted');
}

console.log('Acquire button — downloading → Remove request enabled');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-5',
    in_library: false,
    pipeline_status: 'downloading',
    pipeline_id: 300,
  });
  const html = renderActionToolbar(state);
  assertContains(html, '>Remove request</button>', 'downloading shows Remove request label');
  assertContains(html, 'window.disambRemove(300', 'Remove request enabled mid-download');
}

console.log('Processing owner presentation — every durable state has one canonical label');
{
  const cases = [
    [{ job_id: 41, status: 'queued', preview_status: 'waiting' }, 'queued for preview'],
    [{ job_id: 42, status: 'queued', preview_status: 'running' }, 'previewing'],
    [{ job_id: 43, status: 'queued', preview_status: 'evidence_ready' }, 'waiting to import'],
    [{ job_id: 44, status: 'running', preview_status: 'evidence_ready' }, 'importing'],
    [{ job_id: 45, status: 'recovery_required', preview_status: 'running' }, 'needs recovery'],
  ];
  for (const [owner, label] of cases) {
    const presentation = processingOwnerPresentation('processing', owner);
    assertEqual(presentation?.label, label, `${owner.status}/${owner.preview_status} label`);
    assertEqual(
      presentation?.recoveryTarget,
      `/api/import-jobs/${owner.job_id}/recovery`,
      `${owner.status}/${owner.preview_status} exact recovery target`,
    );
    assertContains(
      presentation?.lockReason || '',
      `job #${owner.job_id}`,
      `${owner.status}/${owner.preview_status} lock reason names exact owner`,
    );
  }
  assertEqual(
    processingOwnerPresentation('downloading', cases[0][0]),
    null,
    'non-processing rows never receive processing presentation',
  );
}

console.log('Processing owner action state — every mutation control is focusable and inert');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'processing-release',
    in_library: true,
    beets_album_id: 88,
    pipeline_status: 'processing',
    pipeline_id: 901,
    processing_owner: {
      job_id: 71,
      status: 'running',
      preview_status: 'evidence_ready',
    },
  });
  const html = renderActionToolbar(state);
  assertContains(html, 'aria-disabled="true"', 'processing control exposes disabled semantics');
  assertContains(html, 'aria-describedby=', 'processing control names its explanation');
  assertContains(html, 'job #71 is importing', 'visible lock explanation names owner state');
  assertContains(html, '/api/import-jobs/71/recovery', 'exact recovery detail target is linked');
  assertExcludes(html, ' disabled', 'processing control remains keyboard focusable');
  assertExcludes(html, 'window.disambRemove', 'processing request cannot be removed');
  assertExcludes(html, 'window.confirmDeleteBeets', 'processing library files cannot be removed');
}

console.log('Processing owner action suppression — pointer, Enter, and Space never activate');
{
  for (const [type, key, expected] of [
    ['click', '', false],
    ['keydown', 'Enter', false],
    ['keydown', ' ', false],
    ['keydown', 'Escape', true],
  ]) {
    let prevented = 0;
    let stopped = 0;
    const result = suppressProcessingAction({
      type,
      key,
      preventDefault() { prevented++; },
      stopPropagation() { stopped++; },
    });
    assertEqual(result, expected, `${type}/${key || 'pointer'} return value`);
    assertEqual(prevented, expected ? 0 : 1, `${type}/${key || 'pointer'} preventDefault`);
    assertEqual(stopped, expected ? 0 : 1, `${type}/${key || 'pointer'} stopPropagation`);
  }
}

console.log('Processing conflict detector — canonical and temporary transition mappings agree');
{
  const owner = { job_id: 72, status: 'queued', preview_status: 'running' };
  const canonical = processingConflictFromResponse(409, {
    error: 'processing_locked',
    request_id: 902,
    processing_owner: owner,
  });
  assertEqual(canonical?.requestId, 902, 'canonical response request id');
  assertEqual(canonical?.owner.job_id, 72, 'canonical response exact owner');
  const transition = processingConflictFromResponse(409, {
    error: 'transition_conflict',
    reason: 'processing_locked',
    request_id: 902,
    processing_owner: owner,
  });
  assertEqual(transition?.owner.job_id, 72, 'temporary transition mapping uses same detector');
  assertEqual(
    processingConflictFromResponse(400, {
      error: 'processing_locked',
      request_id: 902,
      processing_owner: owner,
    }),
    null,
    'non-409 response is not a processing conflict',
  );
}

console.log('Processing conflict handler — immediate lock, row refetch, focus and scroll survive');
{
  const attributes = new Map([['onclick', 'window.deleteRequest(903)']]);
  let focused = 0;
  const inserted = [];
  const control = {
    dataset: {},
    textContent: 'delete',
    isConnected: true,
    setAttribute(name, value) { attributes.set(name, value); },
    removeAttribute(name) { attributes.delete(name); },
    getAttribute(name) { return attributes.get(name) || null; },
    focus() { focused++; },
    insertAdjacentElement(_position, element) {
      element.isConnected = true;
      inserted.push(element);
    },
  };
  const live = { textContent: '', setAttribute() {} };
  const oldDocument = globalThis.document;
  const oldWindow = globalThis.window;
  globalThis.document = {
    activeElement: control,
    body: { appendChild() {} },
    createElement() {
      return {
        children: [],
        className: '',
        id: '',
        type: '',
        textContent: '',
        hidden: false,
        setAttribute() {},
        appendChild(child) { this.children.push(child); },
        remove() {},
      };
    },
    getElementById(id) {
      if (id === 'processing-lock-live-region') return live;
      return inserted.find(element => element.id === id) || null;
    },
    querySelectorAll() { return [control]; },
  };
  let restored = '';
  globalThis.window = {
    scrollX: 13,
    scrollY: 29,
    scrollTo(x, y) { restored = `${x},${y}`; },
  };
  let refetches = 0;
  const handled = await handleProcessingLockedConflict({
    httpStatus: 409,
    payload: {
      error: 'processing_locked',
      request_id: 903,
      processing_owner: {
        job_id: 73,
        status: 'queued',
        preview_status: 'evidence_ready',
      },
    },
    control,
    releaseId: 'processing-release-903',
    refetch: async () => { refetches++; },
  });
  assertEqual(handled, true, 'canonical conflict is handled');
  assertEqual(attributes.get('aria-disabled'), 'true', 'control locks immediately');
  assertEqual(attributes.has('onclick'), false, 'stale inline mutation is removed');
  assertEqual(control.dataset.processingLocked, 'true', 'typed locked state is retained');
  assertEqual(refetches, 1, 'only affected request is refetched');
  assertEqual(restored, '13,29', 'scroll context restored');
  assertEqual(focused, 1, 'focus restored to acted-on control');
  assertContains(live.textContent, 'job #73', 'aria-live announcement names exact owner');
  assertEqual(inserted.length, 1, 'visible owner explanation stays beside the locked control');
  assertEqual(
    pipelineStore.get('processing-release-903')?.processing_owner?.job_id,
    73,
    'central store retains exact conflict owner',
  );
  globalThis.document = oldDocument;
  globalThis.window = oldWindow;
}

console.log('Processing conflict handler — authoritative owner refresh repaints the lock');
{
  const attributes = new Map([['data-pipeline-request-id', '905']]);
  const inserted = [];
  const control = {
    dataset: {},
    textContent: 'delete',
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
  const oldDocument = globalThis.document;
  const oldWindow = globalThis.window;
  const oldFetch = globalThis.fetch;
  globalThis.document = {
    activeElement: control,
    body: { appendChild() {} },
    createElement() {
      return {
        children: [],
        className: '',
        id: '',
        type: '',
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
    querySelectorAll() { return [control]; },
  };
  globalThis.window = { scrollX: 0, scrollY: 0, scrollTo() {} };
  globalThis.fetch = async (url) => {
    assertEqual(url, '/api/pipeline/905', 'refresh fetches only the affected request');
    return {
      ok: true,
      async json() {
        return {
          request: {
            id: 905,
            status: 'processing',
            mb_release_id: 'fresh-owner-release',
            processing_owner: {
              job_id: 76,
              status: 'running',
              preview_status: 'evidence_ready',
            },
          },
        };
      },
    };
  };
  await handleProcessingLockedConflict({
    httpStatus: 409,
    payload: {
      error: 'processing_locked',
      request_id: 905,
      processing_owner: {
        job_id: 75,
        status: 'queued',
        preview_status: 'waiting',
      },
    },
    control,
  });
  assertEqual(control.textContent, 'importing', 'fresh owner status replaces conflict-time label');
  assertContains(
    attributes.get('aria-describedby') || '',
    'processing-owner-76',
    'fresh owner identity replaces conflict-time description target',
  );
  assertEqual(
    pipelineStore.get('fresh-owner-release')?.processing_owner?.job_id,
    76,
    'fresh owner replaces conflict owner in central store',
  );
  assertEqual(
    inserted.some(element => element.isConnected && element.textContent.includes('job #76')),
    true,
    'visible explanation is repainted from the authoritative owner',
  );
  globalThis.fetch = async () => ({
    ok: true,
    async json() {
      return {
        request: {
          id: 905,
          status: 'imported',
          mb_release_id: 'fresh-owner-release',
          processing_owner: null,
        },
      };
    },
  });
  await handleProcessingLockedConflict({
    httpStatus: 409,
    payload: {
      error: 'processing_locked',
      request_id: 905,
      processing_owner: {
        job_id: 76,
        status: 'running',
        preview_status: 'evidence_ready',
      },
    },
    control,
  });
  assertEqual(control.textContent, 'imported', 'fresh lifecycle status replaces stale processing label');
  assertEqual(
    attributes.get('data-request-refreshed-status'),
    'imported',
    'control records the reconciled non-processing status',
  );
  assertEqual(
    attributes.has('data-processing-locked'),
    false,
    'completed request no longer claims a processing lock',
  );
  assertContains(live.textContent, 'now imported', 'non-processing refresh is announced truthfully');
  globalThis.document = oldDocument;
  globalThis.window = oldWindow;
  globalThis.fetch = oldFetch;
}

console.log('Processing conflict handler — stale select becomes a focusable inert button');
{
  const oldDocument = globalThis.document;
  const oldWindow = globalThis.window;
  const oldSelect = globalThis.HTMLSelectElement;
  const oldButton = globalThis.HTMLButtonElement;
  class FakeSelect {
    constructor() {
      this.attributes = new Map([['data-pipeline-request-id', '906']]);
      this.dataset = {};
      this.disabled = false;
      this.isConnected = true;
      this.options = [{ value: 'default' }, { value: 'lossless' }];
      this.replacement = null;
    }
    set textContent(value) {
      this._textContent = value;
      this.options = [];
    }
    get textContent() { return this._textContent || ''; }
    setAttribute(name, value) { this.attributes.set(name, value); }
    removeAttribute(name) { this.attributes.delete(name); }
    getAttribute(name) { return this.attributes.get(name) || null; }
    replaceWith(replacement) {
      this.replacement = replacement;
      replacement.isConnected = true;
      this.isConnected = false;
    }
    insertAdjacentElement() {}
    focus() {}
  }
  class FakeButton {
    constructor() {
      this.attributes = new Map();
      this.dataset = {};
      this.disabled = false;
      this.isConnected = false;
      this.textContent = '';
      this.focused = 0;
    }
    setAttribute(name, value) { this.attributes.set(name, value); }
    removeAttribute(name) { this.attributes.delete(name); }
    getAttribute(name) { return this.attributes.get(name) || null; }
    insertAdjacentElement(_position, element) { element.isConnected = true; }
    focus() { this.focused++; }
  }
  const select = new FakeSelect();
  const live = { textContent: '', setAttribute() {} };
  globalThis.HTMLSelectElement = FakeSelect;
  globalThis.HTMLButtonElement = FakeButton;
  globalThis.document = {
    activeElement: select,
    body: { appendChild() {} },
    createElement(tag) {
      if (tag === 'button') return new FakeButton();
      return {
        children: [],
        className: '',
        id: '',
        textContent: '',
        isConnected: false,
        setAttribute() {},
        appendChild(child) { this.children.push(child); },
      };
    },
    getElementById(id) {
      return id === 'processing-lock-live-region' ? live : null;
    },
    querySelectorAll() { return [select]; },
  };
  globalThis.window = { scrollX: 0, scrollY: 0, scrollTo() {} };
  await handleProcessingLockedConflict({
    httpStatus: 409,
    payload: {
      error: 'processing_locked',
      request_id: 906,
      processing_owner: {
        job_id: 77,
        status: 'queued',
        preview_status: 'running',
      },
    },
    control: select,
    refetch: async () => {},
  });
  assertEqual(select.options.length, 2, 'locking never destroys select options in place');
  assertEqual(
    select.replacement instanceof FakeButton,
    true,
    'select is replaced by a button',
  );
  assertEqual(select.replacement?.textContent, 'previewing', 'replacement uses shared lock label');
  assertEqual(
    select.replacement?.attributes.get('aria-disabled'),
    'true',
    'replacement exposes disabled semantics',
  );
  assertEqual(select.replacement?.focused, 1, 'focus follows the replaced select');
  globalThis.document = oldDocument;
  globalThis.window = oldWindow;
  globalThis.HTMLSelectElement = oldSelect;
  globalThis.HTMLButtonElement = oldButton;
}

console.log('Processing conflict handler — failed refetch stays locked with working retry');
{
  const attributes = new Map();
  const inserted = [];
  let focused = 0;
  const control = {
    dataset: {},
    textContent: 'replace',
    isConnected: true,
    setAttribute(name, value) { attributes.set(name, value); },
    removeAttribute(name) { attributes.delete(name); },
    getAttribute(name) { return attributes.get(name) || null; },
    focus() { focused++; },
    insertAdjacentElement(_position, element) {
      element.isConnected = true;
      inserted.push(element);
    },
  };
  const live = { textContent: '', setAttribute() {} };
  const oldDocument = globalThis.document;
  const oldWindow = globalThis.window;
  globalThis.document = {
    activeElement: control,
    body: { appendChild() {} },
    createElement() {
      return {
        children: [],
        className: '',
        id: '',
        type: '',
        textContent: '',
        hidden: false,
        setAttribute() {},
        appendChild(child) { this.children.push(child); },
        remove() { this.removed = true; },
      };
    },
    getElementById(id) {
      if (id === 'processing-lock-live-region') return live;
      return inserted.find(element => element.id === id) || null;
    },
    querySelectorAll() { return [control]; },
  };
  globalThis.window = { scrollX: 0, scrollY: 0, scrollTo() {} };
  let attempts = 0;
  await handleProcessingLockedConflict({
    httpStatus: 409,
    payload: {
      error: 'processing_locked',
      request_id: 904,
      processing_owner: {
        job_id: 74,
        status: 'recovery_required',
        preview_status: 'running',
      },
    },
    control,
    refetch: async () => {
      attempts++;
      if (attempts === 1) throw new Error('offline');
    },
  });
  assertEqual(attributes.get('aria-disabled'), 'true', 'failed refresh cannot unlock action');
  assertEqual(inserted.length, 2, 'visible explanation and accessible retry are exposed');
  const retry = inserted[1];
  assertEqual(retry.textContent, 'Retry row refresh', 'retry has explicit action text');
  await retry.onclick();
  assertEqual(attempts, 2, 'retry refetches the affected row');
  assertEqual(retry.removed, true, 'successful retry removes retry affordance');
  assertEqual(focused, 2, 'initial failure and successful retry retain focus context');
  globalThis.document = oldDocument;
  globalThis.window = oldWindow;
}

console.log('Acquire button — pipeline=imported (no library) → Upgrade enabled');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-6',
    in_library: false,
    pipeline_status: 'imported',
    pipeline_id: 400,
  });
  const html = renderActionToolbar(state);
  assertContains(html, '>Upgrade</button>', 'imported → Upgrade');
  assertContains(html, 'window.upgradeAlbum', 'Upgrade wired up');
  assertExcludes(html, '>Remove request</button>', 'no Remove request when imported');
}

console.log('Acquire button — pipelineStore overlay');
clearStore();
pipelineStore.set('rel-7', { status: 'wanted', id: 500 });
{
  const state = buildReleaseActionState({
    id: 'rel-7',
    in_library: false,
    pipeline_status: null,
    pipeline_id: null,
  });
  const html = renderActionToolbar(state);
  assertContains(html, 'window.disambRemove(500', 'pipelineStore overrides backend');
}

console.log('Acquire button — updatePipelineStatus normalizes UUID keys');
clearStore();
updatePipelineStatus(' AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA ', 'wanted', 700);
{
  const state = buildReleaseActionState({
    id: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    in_library: false,
    pipeline_status: null,
    pipeline_id: null,
  });
  const html = renderActionToolbar(state);
  assertContains(html, 'window.disambRemove(700', 'normalized store key overrides backend');
}

console.log('Remove from beets — apostrophes stay JS-safe inside onclick');
clearStore();
{
  const state = buildReleaseActionState({
    id: "rel-10'oops",
    in_library: true,
    beets_album_id: 77,
    artist: 'The 12th Man',
    album: "Some of the 12th Man's Greatest Hits",
    track_count: 14,
  });
  const html = renderActionToolbar(state);
  assertContains(html, '&quot;rel-10&#39;oops&quot;', 'release id encoded as JS string arg');
  assertContains(html, '&quot;Some of the 12th Man&#39;s Greatest Hits&quot;', 'album encoded as JS string arg');
  assertContains(html, 'window.confirmDeleteBeets(77', 'delete handler still rendered');
}

console.log('Remove from beets helper — shared renderer supports detail view styling');
clearStore();
{
  const state = buildReleaseActionState({
    id: "rel-11'oops",
    in_library: true,
    beets_album_id: 88,
    pipeline_id: 900,
    artist: 'Mum & Dad',
    album: "Kid A's <special>",
    track_count: 10,
  });
  const html = renderRemoveFromBeetsButton(state, {
    className: 'p-btn delete-beets',
    label: 'Delete from beets',
    stopPropagation: true,
  });
  assertContains(html, 'class="p-btn delete-beets"', 'custom class supported');
  assertContains(html, 'event.stopPropagation(); window.confirmDeleteBeets(88', 'stopPropagation wiring supported');
  assertContains(html, '&quot;Kid A&#39;s &lt;special&gt;&quot;', 'album encoded safely');
}

console.log('Acquire helper — detail view can override add label');
clearStore();
{
  const state = buildReleaseActionState({ id: 'rel-12', in_library: false });
  const html = renderAcquireActionButton(state, {
    addLabel: 'Add to pipeline',
    stopPropagation: true,
    hideDisabled: true,
  });
  assertContains(html, '>Add to pipeline</button>', 'detail add label override supported');
  assertContains(html, 'event.stopPropagation(); window.addRelease(&quot;rel-12&quot;', 'detail add action uses same state contract');
}

console.log('Acquire helper — detail view can override upgrade label');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-13',
    in_library: true,
    beets_album_id: 13,
  });
  const html = renderAcquireActionButton(state, {
    className: 'p-btn',
    upgradeClassName: 'p-btn upgrade-btn',
    upgradeLabel: 'Upgrade (lowest: 192kbps)',
    stopPropagation: true,
    hideDisabled: true,
  });
  assertContains(html, 'class="p-btn upgrade-btn"', 'detail upgrade class override supported');
  assertContains(html, '>Upgrade (lowest: 192kbps)</button>', 'detail upgrade label override supported');
  assertContains(html, 'window.upgradeAlbum(&quot;rel-13&quot;', 'detail upgrade action uses same state contract');
}

console.log('Acquire helper — library detail shows Remove request for wanted albums');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-13b',
    in_library: true,
    beets_album_id: 130,
    pipeline_status: 'wanted',
    pipeline_id: 913,
  });
  const html = renderAcquireActionButton(state, {
    className: 'p-btn',
    addClassName: 'p-btn upgrade-btn',
    upgradeClassName: 'p-btn upgrade-btn',
    removeClassName: 'p-btn remove-request',
    stopPropagation: true,
    hideDisabled: true,
  });
  assertContains(html, 'class="p-btn remove-request"', 'wanted detail button uses distinct cancel styling');
  assertContains(html, '>Remove request</button>', 'wanted detail button switches away from Upgrade');
  assertContains(html, 'window.disambRemove(913', 'wanted detail button removes queued request');
}

console.log('Acquire helper — disabled states can be hidden in detail views');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-14',
    in_library: false,
    pipeline_status: 'unsearchable',
    pipeline_id: 600,
  });
  const html = renderAcquireActionButton(state, { hideDisabled: true });
  assertEqual(html, '', 'detail disabled action can be omitted');
}

console.log('Remove helper — disabled delete button can be hidden in detail views');
clearStore();
{
  const state = buildReleaseActionState({ id: 'rel-15', in_library: false });
  const html = renderRemoveFromBeetsButton(state, { hideDisabled: true });
  assertEqual(html, '', 'detail disabled delete action can be omitted');
}

console.log('Child pressing toolbar — hides only meaningless disabled beets removal');
clearStore();
{
  // Deterministic pin: a fresh unowned pressing must keep Add + Replace
  // affordances at its call site while dropping the disabled beets action.
  const state = buildReleaseActionState({ id: 'pressing-unowned', in_library: false });
  const oldHtml = renderActionToolbar(state);
  const html = renderActionToolbar(state, { hideDisabledRemove: true });
  assertContains(oldHtml, '<button class="btn"', 'known-bad default toolbar contains disabled beets button');
  assertContains(oldHtml, '>Remove from beets</button>', 'known-bad old child rendering shows meaningless action');
  assertContains(html, '>Add request</button>', 'Add request remains visible');
  assertExcludes(html, '>Remove from beets</button>', 'disabled beets action is omitted');
}

console.log('Child pressing toolbar — state-space sweep preserves acquire action and enabled removal');
clearStore();
{
  const cases = [
    { in_library: false, beets_album_id: null, pipeline_status: null, pipeline_id: null },
    { in_library: false, beets_album_id: null, pipeline_status: 'wanted', pipeline_id: 101 },
    { in_library: false, beets_album_id: null, pipeline_status: 'downloading', pipeline_id: 102 },
    { in_library: false, beets_album_id: null, pipeline_status: 'imported', pipeline_id: 103 },
    { in_library: false, beets_album_id: null, pipeline_status: 'unsearchable', pipeline_id: 104 },
    { in_library: true, beets_album_id: null, pipeline_status: null, pipeline_id: null },
    { in_library: true, beets_album_id: 42, pipeline_status: null, pipeline_id: null },
    { in_library: true, beets_album_id: 43, pipeline_status: 'wanted', pipeline_id: 105 },
  ];
  const acquireLabels = ['Add request', 'Upgrade', 'Remove request'];
  for (const [i, input] of cases.entries()) {
    const state = buildReleaseActionState({ id: `pressing-${i}`, ...input });
    const baseline = renderActionToolbar(state);
    const compact = renderActionToolbar(state, { hideDisabledRemove: true });
    for (const label of acquireLabels) {
      assertEqual(
        compact.includes(`>${label}</button>`),
        baseline.includes(`>${label}</button>`),
        `case ${i}: ${label} visibility is unchanged`,
      );
    }
    assertEqual(
      compact.includes('>Remove from beets</button>'),
      state.canRemoveBeets,
      `case ${i}: beets removal renders exactly when enabled`,
    );
    assertEqual(
      compact.includes('window.confirmDeleteBeets'),
      state.canRemoveBeets,
      `case ${i}: enabled beets handler is retained`,
    );
  }
}

console.log('Acquire button — unsearchable → disabled Add request');
clearStore();
{
  const state = buildReleaseActionState({
    id: 'rel-8',
    in_library: false,
    pipeline_status: 'unsearchable',
    pipeline_id: 600,
  });
  const html = renderActionToolbar(state);
  assertContains(html, '>Add request</button>', 'unsearchable falls through to disabled Add request');
  assertExcludes(html, 'window.addRelease', 'Add disabled in unsearchable state');
  assertExcludes(html, 'window.disambRemove', 'no Remove handler in unsearchable state');
}

console.log('Acquire button — minimal input never crashes');
clearStore();
{
  const state = buildReleaseActionState({ id: 'rel-9' });
  const html = renderActionToolbar(state);
  assertContains(html, 'action-toolbar', 'toolbar wrapper present');
  assertContains(html, '>Add request</button>', 'falls back to Add request');
  assertContains(html, '>Remove from beets</button>', 'Remove from beets always present');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
