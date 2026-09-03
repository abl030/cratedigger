// @ts-check
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

t.section('dispatchTabShown() — no-op for a tab with no follow-up render, and for an unknown name');

{
  // browse's onShow is null; the real recents/pipeline/manual loaders touch
  // `document`/`fetch` and are exercised through the composed `showTab`
  // entry in tests/test_js_main.mjs instead, which is the real caller.
  let threw = false;
  try {
    dispatchTabShown('browse');
  } catch (err) {
    threw = true;
  }
  t.ok(!threw, "dispatchTabShown('browse') does not throw (browse has no follow-up render)");
}

{
  let threw = false;
  try {
    dispatchTabShown('not-a-real-tab');
  } catch (err) {
    threw = true;
  }
  t.ok(!threw, 'dispatchTabShown of an unknown name is a silent no-op');
}

t.done();
