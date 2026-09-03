// @ts-check

/**
 * `web/js/main.js::showTab` — the composed entry that selects a tab and
 * dispatches its follow-up render (issue #1355 WE4). `web/js/tabs.js`'s
 * own suite (`tests/test_js_tabs.mjs`) covers the pure registry facts in
 * isolation; this suite drives the real `showTab` through a DOM stub to
 * prove the two are actually wired together — selection by the stable
 * `data-tab-name` attribute (not position), and dispatch to the right
 * tab's follow-up render.
 *
 * `main.js` has top-level side effects (`installSessionGuard`,
 * `document.getElementById('q')` listener wiring, the `window.*`
 * `Object.assign`) that run once at import time, so `document`/`window`
 * must be stubbed BEFORE the dynamic import — the same pattern
 * `tests/test_js_search_plan.mjs`'s F12 section already established.
 * `showTab` is a named export (not read off `window`) so this suite can
 * import it directly.
 */
import { suite, stubGlobals, element, domStub } from './js_harness.mjs';
import { state } from '../web/js/state.js';

const t = suite(import.meta.url);

const TAB_NAMES = ['browse', 'recents', 'pipeline', 'manual'];
const SECTION_IDS = TAB_NAMES.map((name) => `${name}-section`);
const CONTENT_IDS = {
  recents: 'recents-content',
  pipeline: 'pipeline-content',
  manual: 'wrong-matches-content',
};

/**
 * Build a tab-bar + section-set DOM stub matching `web/index.html`'s real
 * markup shape: one `.tab[data-tab-name]` element per tab (Browse starts
 * active, matching the real page), one `<section id="${name}-section">`,
 * plus the three tabs' content containers so their loaders' synchronous
 * pre-`await` `el.innerHTML = ...` write does not throw on a null element.
 *
 * @returns {{document: any, tabs: Record<string, any>, sections: Record<string, any>, content: Record<string, any>}}
 */
function buildAppDocument() {
  /** @type {Record<string, any>} */
  const tabs = {};
  for (const name of TAB_NAMES) {
    const el = element({ className: name === 'browse' ? 'tab active' : 'tab' });
    el.setAttribute('data-tab-name', name);
    tabs[name] = el;
  }
  /** @type {Record<string, any>} */
  const sections = {};
  for (const id of SECTION_IDS) {
    sections[id] = element({ className: id === 'browse-section' ? 'section active' : 'section' });
  }
  /** @type {Record<string, any>} */
  const content = {};
  for (const id of Object.values(CONTENT_IDS)) {
    content[id] = element();
  }
  const byId = { ...sections, ...content };
  const doc = domStub(byId, {
    querySelector(sel) {
      const m = /^\.tab\[data-tab-name="([^"]+)"\]$/.exec(sel);
      if (m) return tabs[m[1]] || null;
      if (sel === '.tab.active') {
        return Object.values(tabs).find((el) => el.classList.contains('active')) || null;
      }
      return null;
    },
    querySelectorAll(sel) {
      if (sel === '.tab') return Object.values(tabs);
      if (sel === '.section') return Object.values(sections);
      return [];
    },
  });
  return { document: doc, tabs, sections, content };
}

/**
 * Install the full global world `showTab`'s dispatched loaders need
 * (document, a no-op `window`, a fetch that always rejects so every
 * loader's own catch swallows it, and `localStorage` for
 * `loadWrongMatches`), and return `{app, restore}`.
 *
 * @returns {{app: ReturnType<typeof buildAppDocument>, restore: () => void}}
 */
function installAppWorld() {
  const app = buildAppDocument();
  const restore = stubGlobals({
    document: app.document,
    window: { setTimeout: () => 0 },
    fetch: () => Promise.reject(new Error('test: no network')),
    localStorage: { getItem: () => null },
  }).restore;
  return { app, restore };
}

let showTab;

t.section('showTab() — import main.js under a stubbed DOM');

{
  const { restore } = installAppWorld();
  try {
    ({ showTab } = await import('../web/js/main.js'));
  } finally {
    restore();
  }
  t.equal(typeof showTab, 'function', 'main.js exports showTab as a real named export');
}

t.section('showTab() selects the active tab by data-tab-name, not position');

{
  const { app, restore } = installAppWorld();
  const prevPipelineView = state.pipelineView;
  const prevRecentsSub = state.recentsSub;
  try {
    state.pipelineView = 'dashboard';
    state.recentsSub = 'history';
    showTab('pipeline');
    t.ok(app.tabs.pipeline.classList.contains('active'),
      "showTab('pipeline') marks the pipeline tab active via its data-tab-name attribute");
    t.ok(!app.tabs.browse.classList.contains('active'),
      "showTab('pipeline') clears 'active' from the previously active browse tab");
    t.ok(app.sections['pipeline-section'].classList.contains('active'),
      "showTab('pipeline') activates pipeline-section");
    t.ok(!app.sections['browse-section'].classList.contains('active'),
      "showTab('pipeline') deactivates browse-section");

    showTab('manual');
    t.ok(app.tabs.manual.classList.contains('active'),
      "showTab('manual') marks the Wrong Matches tab active by its data-tab-name (not the 4th nth-child position)");
    t.ok(!app.tabs.pipeline.classList.contains('active'),
      "showTab('manual') clears 'active' from the previously active pipeline tab");
    t.ok(app.sections['manual-section'].classList.contains('active'),
      "showTab('manual') activates manual-section");
  } finally {
    state.pipelineView = prevPipelineView;
    state.recentsSub = prevRecentsSub;
    restore();
  }
}

t.section("showTab() dispatches each tab's own follow-up render — the same fact tabHasAsyncRender() reports");

{
  // browse has no follow-up render: its content is never touched.
  const { app, restore } = installAppWorld();
  try {
    showTab('browse');
    t.equal(app.tabs.browse.classList.contains('active') ? 'active' : 'inactive', 'active',
      "showTab('browse') still activates the browse tab");
    for (const id of Object.values(CONTENT_IDS)) {
      t.equal(app.content[id].innerHTML, '',
        `showTab('browse') touches no tab content (${id} untouched) — browse has no follow-up render`);
    }
  } finally {
    restore();
  }
}

{
  // recents/pipeline each write their own loading state into their content
  // element SYNCHRONOUSLY, before their first `await`; manual's own first
  // step (`_deriveTriageButtonState()`) is itself `async`, so its write
  // lands one microtask tick later even though nothing in it truly waits
  // on I/O — `await` always yields at least once, resolved or not. One
  // tick covers all three and proves dispatch reached the right loader
  // without needing the (deliberately failing) fetch to settle.
  const cases = [
    ['recents', CONTENT_IDS.recents],
    ['pipeline', CONTENT_IDS.pipeline],
    ['manual', CONTENT_IDS.manual],
  ];
  for (const [tabName, contentId] of cases) {
    const { app, restore } = installAppWorld();
    const prevPipelineView = state.pipelineView;
    const prevRecentsSub = state.recentsSub;
    try {
      state.pipelineView = 'dashboard';
      state.recentsSub = 'history';
      showTab(tabName);
      await Promise.resolve();
      t.ok(app.content[contentId].innerHTML.length > 0,
        `showTab('${tabName}') writes a loading state into #${contentId} — proves dispatch reached its real loader`);
      // Let the deliberately-rejecting fetch settle so nothing here leaks
      // an unhandled rejection into a later section.
      await Promise.resolve();
      await Promise.resolve();
    } finally {
      state.pipelineView = prevPipelineView;
      state.recentsSub = prevRecentsSub;
      restore();
    }
  }
}

t.done();
