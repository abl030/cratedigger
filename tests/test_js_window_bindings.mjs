/**
 * Audit inline onclick handlers against main.js's public window bindings.
 *
 * Invariant (issue #603): every application handler emitted by a web/js/*.js
 * string/template or declared inline in web/index.html is exposed by the
 * Object.assign(window, {...}) block in web/js/main.js.
 *
 * Run with: node tests/test_js_window_bindings.mjs
 */

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  assertWindowBindings,
  auditWindowBindings,
  emittedOnclickHandlers,
  exposedWindowBindings,
} from './helpers/js_window_bindings_audit.mjs';

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.dirname(TEST_DIR);
const WEB_JS_DIR = path.join(REPO_ROOT, 'web', 'js');

const jsSources = Object.fromEntries(
  fs.readdirSync(WEB_JS_DIR)
    .filter((name) => name.endsWith('.js'))
    .sort()
    .map((name) => [name, fs.readFileSync(path.join(WEB_JS_DIR, name), 'utf8')]),
);
const indexHtml = fs.readFileSync(path.join(REPO_ROOT, 'web', 'index.html'), 'utf8');
const mainSource = jsSources['main.js'];

// Deterministic pin: the release toolbar emits the public alias, not the local
// implementation name. The binding parser must therefore use the property key
// from `openReplacePicker: openReplacePickerAndHandle`.
const releaseActionHandlers = emittedOnclickHandlers({
  jsSources: { 'release_actions.js': jsSources['release_actions.js'] },
  indexHtml: '',
});
assert(releaseActionHandlers.has('openReplacePicker'));
assert(exposedWindowBindings(mainSource).has('openReplacePicker'));
assert(!exposedWindowBindings(mainSource).has('openReplacePickerAndHandle'));

// Generated/property sweep: the real repository is complete, then removing
// each required public property in turn must make precisely that property
// observable as missing. This exercises every currently emitted handler rather
// than pinning a hand-maintained handler list.
const actualAudit = assertWindowBindings({ jsSources, indexHtml, mainSource });
assert(actualAudit.required.size > 0, 'audit discovered no inline onclick handlers');

for (const handler of [...actualAudit.required].sort()) {
  const withoutHandler = mainSource.replace(
    new RegExp(`^\\s*${handler}(?:\\s*:\\s*[A-Za-z_$][\\w$]*)?\\s*,\\s*$`, 'm'),
    '',
  );
  assert.notEqual(withoutHandler, mainSource, `fixture could not remove binding ${handler}`);
  const fault = auditWindowBindings({ jsSources, indexHtml, mainSource: withoutHandler });
  assert.deepEqual([...fault.missing], [handler], `removing ${handler} did not fail precisely`);
}

// Known-bad self-test: a newly emitted application handler with no public
// binding is rejected. Browser-native calls and non-handler comments/code are
// deliberately present to prove they do not become application requirements.
const knownBadSources = {
  'fixture.js': `
    // onclick="window.onlyInAComment()"
    window.onlyOrdinaryCode();
    const native = '<button onclick="window.open(\"about:blank\")">native</button>';
    const broken = '<button onclick="window.issue603UnboundHandler()">broken</button>';
  `,
};
const knownBad = auditWindowBindings({
  jsSources: knownBadSources,
  indexHtml: '',
  mainSource: 'Object.assign(window, { onlyInAComment, onlyOrdinaryCode });',
});
assert.deepEqual([...knownBad.missing], ['issue603UnboundHandler']);
assert.throws(
  () => assertWindowBindings({
    jsSources: knownBadSources,
    indexHtml: '',
    mainSource: 'Object.assign(window, { onlyInAComment, onlyOrdinaryCode });',
  }),
  /issue603UnboundHandler/,
);

console.log(`JS window-binding audit passed (${actualAudit.required.size} handlers)`);
