/**
 * Conservative audit for statically authored window handlers (issue #603).
 * Run with: node tests/test_js_window_bindings.mjs
 */

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  assertWindowBindings,
  auditWindowBindings,
  emittedWindowHandlers,
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

// Alias pin: the public Object.assign key is the binding, not its local value.
const releaseHandlers = emittedWindowHandlers({
  jsSources: { 'release_actions.js': jsSources['release_actions.js'] },
  indexHtml: '',
});
assert(releaseHandlers.handlers.has('openReplacePicker'));
const realBindings = exposedWindowBindings(mainSource);
assert(realBindings.has('openReplacePicker'));
assert(!realBindings.has('openReplacePickerAndHandle'));

// Top-level-only pin: nested values do not accidentally become public keys.
const nestedBindings = exposedWindowBindings(`Object.assign(window, {
  shorthand,
  publicAlias: localImplementation,
  namespace: { nestedHandler, deeper: { hiddenHandler } },
});`);
assert.deepEqual([...nestedBindings].sort(), ['namespace', 'publicAlias', 'shorthand']);

// Dynamic onclick BODY interpolation is allowed: its statically authored
// handler strings are conservatively found elsewhere in the same sources.
const dynamicBodyHandlers = emittedWindowHandlers({
  jsSources: {
    'pipeline.js': jsSources['pipeline.js'],
    'render_primitives.js': jsSources['render_primitives.js'],
    'discography.js': jsSources['discography.js'],
  },
  indexHtml: '',
});
for (const name of ['loadLongTail', 'loadPipelineDashboard', 'loadPipeline', 'toggleReleaseDetail']) {
  assert(dynamicBodyHandlers.handlers.has(name), `static handler body not found: ${name}`);
}
assert.deepEqual(dynamicBodyHandlers.dynamicCallees, []);

// Generated/property sweep independent of production discovery: deterministic
// synthetic names define both the expected set and each missing-binding world.
const generatedNames = Array.from({ length: 32 }, (_, i) => `generatedHandler${String(i).padStart(2, '0')}`);
const generatedSource = generatedNames
  .map((name) => `const html_${name} = '<button onclick="window.${name}()">x</button>';`)
  .join('\n');
const generatedBindings = (names) => `Object.assign(window, { ${names.join(', ')} });`;
const generatedAudit = assertWindowBindings({
  jsSources: { 'generated.js': generatedSource },
  indexHtml: '',
  mainSource: generatedBindings(generatedNames),
});
assert.deepEqual([...generatedAudit.required].sort(), generatedNames);
for (const missingName of generatedNames) {
  const audit = auditWindowBindings({
    jsSources: { 'generated.js': generatedSource },
    indexHtml: '',
    mainSource: generatedBindings(generatedNames.filter((name) => name !== missingName)),
  });
  assert.deepEqual([...audit.missing], [missingName]);
}

// index.html uses bare calls; comments and ordinary JS code do not count as
// string/template surfaces. Unrelated strings DO count by design: conservative
// false positives are preferable to silently missed dead buttons.
const conservative = emittedWindowHandlers({
  jsSources: {
    'fixture.js': `
      // const ignored = '<button onclick="window.commentOnly()">';
      window.ordinaryCodeOnly();
      const help = 'Debug with window.helpStringHandler()';
      const native = 'window.fetch("/api")';
    `,
  },
  indexHtml: '<button onclick="bareIndexHandler()">x</button>',
});
assert.deepEqual([...conservative.handlers].sort(), ['bareIndexHandler', 'helpStringHandler']);

// Known-bad static handler proves the binding assertion has teeth.
const knownBad = auditWindowBindings({
  jsSources: { 'bad.js': `const html = '<button onclick="window.unboundStaticHandler()">x</button>';` },
  indexHtml: '',
  mainSource: 'Object.assign(window, {});',
});
assert.deepEqual([...knownBad.missing], ['unboundStaticHandler']);
assert.throws(
  () => assertWindowBindings({
    jsSources: { 'bad.js': `const html = '<button onclick="window.unboundStaticHandler()">x</button>';` },
    indexHtml: '',
    mainSource: 'Object.assign(window, {});',
  }),
  /unboundStaticHandler/,
);

// Computed/dynamic window callees fail closed; dynamic handler bodies do not.
for (const source of [
  'const html = `<button onclick="window.${handlerName}()">x</button>`;',
  `const html = '<button onclick="window[handlerName]()">x</button>';`,
]) {
  const audit = auditWindowBindings({
    jsSources: { 'dynamic.js': source },
    indexHtml: '',
    mainSource: 'Object.assign(window, {});',
  });
  assert(audit.dynamicCallees.length > 0);
  assert.throws(
    () => assertWindowBindings({
      jsSources: { 'dynamic.js': source },
      indexHtml: '',
      mainSource: 'Object.assign(window, {});',
    }),
    /dynamic window callee/,
  );
}
assert.doesNotThrow(() => assertWindowBindings({
  jsSources: { 'body.js': 'const html = `<button onclick="${handlerBody}">x</button>`;' },
  indexHtml: '',
  mainSource: 'Object.assign(window, {});',
}));

// Approved native calls are ignored, but exposing an app binding under a
// reserved native name is rejected as shadowing.
assert.doesNotThrow(() => assertWindowBindings({
  jsSources: { 'native.js': `const help = 'window.fetch("/api"); window.open("/")';` },
  indexHtml: '',
  mainSource: 'Object.assign(window, {});',
}));
const nativeShadow = auditWindowBindings({
  jsSources: {},
  indexHtml: '',
  mainSource: 'Object.assign(window, { fetch });',
});
assert.deepEqual([...nativeShadow.nativeCollisions], ['fetch']);
assert.throws(
  () => assertWindowBindings({
    jsSources: {},
    indexHtml: '',
    mainSource: 'Object.assign(window, { fetch });',
  }),
  /reserved native window names: fetch/,
);

const actualAudit = assertWindowBindings({ jsSources, indexHtml, mainSource });
assert(actualAudit.required.size > 0, 'audit discovered no static handlers');
console.log(`JS window-binding audit passed (${actualAudit.required.size} conservative handlers)`);
