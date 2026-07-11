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
  emittedOnclickBodies,
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

// Independent adversarial matrix. These fixtures are intentionally not
// derived from the production scanner's output, so they constrain what an
// onclick surface IS rather than merely mutating whatever the scanner found.
const adversarialSource = `
  const help = 'To debug, call window.unrelatedHelpExample() in the console';
  const direct = '<button onclick="window.windowHandler()">window</button>';
  const bare = '<button onclick="bareHandler()">bare</button>';
  const prefixed = '<button onclick="event.stopPropagation(); window.prefixedHandler()">prefixed</button>';
  const concatenated = '<button onclick="event.preventDefault(); '
    + 'window.concatenatedHandler()">concatenated</button>';
  const templated = \`<button onclick="window.templateArgumentHandler(\${row.id})">templated</button>\`;
  const resolvedBody = 'event.stopPropagation(); window.resolvedBodyHandler()';
  const resolved = \`<button onclick="\${resolvedBody}">resolved</button>\`;
`;
const adversarialMain = `Object.assign(window, {
  windowHandler,
  bareHandler,
  prefixedHandler,
  concatenatedHandler,
  templateArgumentHandler,
  publicAlias: localImplementation,
  resolvedBodyHandler,
});`;
const adversarialBodies = emittedOnclickBodies({
  jsSources: { 'adversarial.js': adversarialSource },
  indexHtml: '<button onclick="publicAlias()">alias</button>',
});
assert(adversarialBodies.some((body) => body.includes('window.windowHandler()')));
assert(adversarialBodies.some((body) => body.includes('bareHandler()')));
assert(adversarialBodies.some((body) => body.includes('window.concatenatedHandler()')));
const adversarialAudit = assertWindowBindings({
  jsSources: { 'adversarial.js': adversarialSource },
  indexHtml: '<button onclick="publicAlias()">alias</button>',
  mainSource: adversarialMain,
});
assert.deepEqual([...adversarialAudit.required].sort(), [
  'bareHandler',
  'concatenatedHandler',
  'prefixedHandler',
  'publicAlias',
  'resolvedBodyHandler',
  'templateArgumentHandler',
  'windowHandler',
]);
assert(!adversarialAudit.required.has('unrelatedHelpExample'));

// Missing bare handlers and dynamic callee names must both fail. A dynamic
// argument is safe because it cannot alter which public function is called.
const missingBare = auditWindowBindings({
  jsSources: { 'fixture.js': `const html = '<button onclick="bareMissing()">x</button>';` },
  indexHtml: '',
  mainSource: 'Object.assign(window, {});',
});
assert.deepEqual([...missingBare.missing], ['bareMissing']);

for (const dynamicSource of [
  'const html = `<button onclick="window.${handlerName}()">x</button>`;',
  'const html = `<button onclick="${handlerBody}">x</button>`;',
  `// const handlerBody = 'window.commentDecoyHandler()';
   const html = \`<button onclick="\${handlerBody}">x</button>\`;`,
  `const html = '<button onclick="window.' + handlerName + '()">x</button>';`,
  `const html = '<button onclick="window[handlerName]()">x</button>';`,
  `const handlerBody = enabled ? 'window.onlyStaticBranch()' : dynamicBody;
   const html = \`<button onclick="\${handlerBody}">x</button>\`;`,
]) {
  const dynamicAudit = auditWindowBindings({
    jsSources: { 'dynamic.js': dynamicSource },
    indexHtml: '',
    mainSource: 'Object.assign(window, {});',
  });
  assert(dynamicAudit.unresolved.length > 0, `dynamic handler must be unresolved: ${dynamicSource}`);
  assert.throws(
    () => assertWindowBindings({
      jsSources: { 'dynamic.js': dynamicSource },
      indexHtml: '',
      mainSource: 'Object.assign(window, {});',
    }),
    /unresolved inline onclick/,
  );
}

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
