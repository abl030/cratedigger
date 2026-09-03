// @ts-check
import { readFileSync } from 'node:fs';
import { suite } from './js_harness.mjs';
import { tabDefs, isTabName, tabLabel, tabHasAsyncRender, dispatchTabShown } from '../web/js/tabs.js';

const t = suite(import.meta.url);

t.section('tabDefs() — the one owner of name/label/order');

{
  const defs = tabDefs();
  t.deepEqual(
    defs.map((d) => d.name),
    ['browse', 'recents', 'pipeline', 'manual'],
    'internal names, in display order — the same order web/index.html renders the tab bar in',
  );
  t.deepEqual(
    defs.map((d) => d.label),
    ['Browse', 'Recents', 'Pipeline', 'Wrong Matches'],
    'visible labels match the text web/index.html renders for each tab',
  );
}

t.section("web/index.html's tab bar matches tabDefs() exactly");

{
  // tabs.js's own module doc claims this test is what keeps the markup
  // and the registry in lockstep -- so it has to actually parse the
  // markup, not compare tabDefs() against a second hand-typed literal
  // (that would just be two guesses agreeing with each other, the exact
  // shape .claude/rules/test-fidelity.md Rule C forbids). Renaming a
  // label or reordering the tab bar in web/index.html without touching
  // tabDefs() must fail here.
  const indexHtml = readFileSync(new URL('../web/index.html', import.meta.url), 'utf8');
  const tabDivRe = /<div class="tab( active)?" data-tab-name="([a-z]+)" onclick="showTab\('\2'\)">([^<]+)<\/div>/g;
  /** @type {Array<[string, string]>} */
  const rendered = [];
  let match = tabDivRe.exec(indexHtml);
  while (match) {
    rendered.push([match[2], match[3]]);
    match = tabDivRe.exec(indexHtml);
  }
  t.ok(rendered.length > 0, 'the tab-bar regex actually matched something in web/index.html (a rewritten markup shape must update this parser, not silently match zero)');
  t.deepEqual(
    rendered,
    tabDefs().map((d) => [d.name, d.label]),
    "web/index.html's rendered [data-tab-name, label] pairs, in DOM order, equal tabDefs()'s [name, label] pairs, in registry order",
  );
}

t.section('isTabName()');

{
  for (const name of ['browse', 'recents', 'pipeline', 'manual']) {
    t.ok(isTabName(name), `isTabName('${name}') is true for a real tab`);
  }
  t.notOk(isTabName('decisions'), "isTabName('decisions') is false — no live tab has ever used this name (dead labelToName entry removed)");
  t.notOk(isTabName('Browse'), "isTabName('Browse') is false — internal names are lowercase, not the visible label");
  t.notOk(isTabName(''), 'isTabName(empty string) is false');
  t.notOk(isTabName('unknown'), 'isTabName of an unregistered name is false');
}

t.section('tabLabel()');

{
  const cases = [
    ['browse', 'Browse'],
    ['recents', 'Recents'],
    ['pipeline', 'Pipeline'],
    ['manual', 'Wrong Matches'],
    ['unknown', null],
  ];
  for (const [name, expected] of cases) {
    t.equal(tabLabel(name), expected, `tabLabel('${name}') === ${JSON.stringify(expected)}`);
  }
}

t.section('tabHasAsyncRender() — the single fact showTab\'s dispatch and closeSearchPlanDetail both read');

{
  const cases = [
    ['browse', false],
    ['recents', true],
    ['pipeline', true],
    ['manual', true],
    ['unknown', false],
  ];
  for (const [name, expected] of cases) {
    t.equal(tabHasAsyncRender(name), expected, `tabHasAsyncRender('${name}') === ${expected}`);
  }
}

t.section("dispatchTabShown() calls exactly the named tab's own onShow, never another tab's");

{
  // A `try { dispatchTabShown(x) } catch { threw = true }` shape looks like
  // it proves "does nothing", but every real onShow is an async loader --
  // a wrongly-dispatched call rejects a promise rather than throwing
  // synchronously, so that shape passes even when the WRONG loader ran
  // (mutant-runner finding on this exact file: a `dispatchTabShown` that
  // ignored `name` and always ran TAB_DEFS[1]'s loader still reported
  // "0 failed" here). `tabDefs()` returns the TAB_DEFS array itself, not a
  // deep clone of its entries, which is what lets this test substitute an
  // entry's `onShow` with a recorder and observe real invocation counts
  // instead of "didn't throw": mutating `def.onShow` here mutates the
  // exact object `findTab` looks up inside `dispatchTabShown`, whether
  // `tabDefs()` hands back that array by reference or a shallow copy of
  // it (verified empirically: a `TAB_DEFS.slice()` return still keeps
  // this section passing, since the array entries are the same objects
  // either way). Per-name real-loader dispatch (the actual production
  // loaders, through a DOM stub) is exercised by tests/test_js_main.mjs.
  const defs = tabDefs();
  const originalOnShow = defs.map((d) => d.onShow);
  /** @type {Record<string, number>} */
  const calls = {};
  for (const def of defs) {
    if (def.onShow !== null) {
      def.onShow = () => { calls[def.name] = (calls[def.name] || 0) + 1; };
    }
  }
  try {
    for (const target of defs) {
      for (const def of defs) calls[def.name] = 0;
      dispatchTabShown(target.name);
      for (const def of defs) {
        const expected = (def.name === target.name && def.onShow !== null) ? 1 : 0;
        const label = def.name === target.name
          ? 'calls its own recorder exactly once'
          : `does not call the '${def.name}' recorder`;
        t.equal(calls[def.name], expected, `dispatchTabShown('${target.name}') ${label}`);
      }
    }
    for (const def of defs) calls[def.name] = 0;
    dispatchTabShown('not-a-real-tab');
    t.deepEqual(Object.values(calls), defs.map(() => 0),
      'dispatchTabShown of an unknown name calls no recorder at all');
  } finally {
    defs.forEach((d, i) => { d.onShow = originalOnShow[i]; });
  }
}

t.done();
