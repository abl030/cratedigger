/**
 * Static audit helpers for inline onclick -> window binding coverage.
 *
 * This deliberately inspects JavaScript string/template fragments rather than
 * every `window.foo()` token in source. Application code, comments, JSDoc, and
 * keyboard handlers are not inline onclick surfaces and must not expand the
 * binding contract.
 */

const BROWSER_WINDOW_APIS = new Set([
  'alert',
  'blur',
  'cancelAnimationFrame',
  'clearInterval',
  'clearTimeout',
  'close',
  'confirm',
  'focus',
  'open',
  'print',
  'prompt',
  'requestAnimationFrame',
  'scroll',
  'scrollBy',
  'scrollTo',
  'setInterval',
  'setTimeout',
]);

const WINDOW_CALL_RE = /\bwindow\.([A-Za-z_$][\w$]*)\s*\(/g;
const HTML_ONCLICK_RE = /\bonclick\s*=\s*(["'])([\s\S]*?)\1/gi;
const BARE_HANDLER_CALL_RE = /(?:^|;)\s*([A-Za-z_$][\w$]*)\s*\(/g;

/**
 * Return the literal fragments from JS strings and template literals.
 * Template expressions are scanned recursively, while separate literal
 * fragments remain independent. That is enough for the handler contract:
 * `window.someHandler(` itself cannot straddle an interpolation.
 *
 * @param {string} source
 * @returns {string[]}
 */
function jsStringFragments(source) {
  /** @type {string[]} */
  const fragments = [];
  let i = 0;

  function scanQuoted(quote) {
    i++; // opening quote
    let value = '';
    while (i < source.length) {
      const ch = source[i];
      if (ch === '\\') {
        value += ch;
        i++;
        if (i < source.length) value += source[i++];
      } else if (ch === quote) {
        i++;
        fragments.push(value);
        return;
      } else {
        value += ch;
        i++;
      }
    }
    fragments.push(value);
  }

  function scanTemplate() {
    i++; // opening backtick
    let value = '';
    while (i < source.length) {
      const ch = source[i];
      if (ch === '\\') {
        value += ch;
        i++;
        if (i < source.length) value += source[i++];
      } else if (ch === '`') {
        i++;
        fragments.push(value);
        return;
      } else if (ch === '$' && source[i + 1] === '{') {
        fragments.push(value);
        value = '';
        i += 2;
        scanCode(true);
      } else {
        value += ch;
        i++;
      }
    }
    fragments.push(value);
  }

  function scanRegex() {
    i++; // opening slash
    let inClass = false;
    while (i < source.length) {
      const ch = source[i++];
      if (ch === '\\') {
        i++;
      } else if (ch === '[') {
        inClass = true;
      } else if (ch === ']') {
        inClass = false;
      } else if (ch === '/' && !inClass) {
        while (/[A-Za-z]/.test(source[i] || '')) i++;
        return;
      }
    }
  }

  function slashStartsRegex() {
    let cursor = i - 1;
    while (cursor >= 0 && /\s/.test(source[cursor])) cursor--;
    return cursor < 0 || /[({\[=,:;!?&|+*%~<>]/.test(source[cursor]);
  }

  function scanCode(stopAtTemplateBrace = false) {
    let braceDepth = stopAtTemplateBrace ? 1 : 0;
    while (i < source.length) {
      const ch = source[i];
      if (ch === "'" || ch === '"') {
        scanQuoted(ch);
      } else if (ch === '`') {
        scanTemplate();
      } else if (ch === '/' && source[i + 1] === '/') {
        i += 2;
        while (i < source.length && source[i] !== '\n') i++;
      } else if (ch === '/' && source[i + 1] === '*') {
        i += 2;
        while (i < source.length && !(source[i] === '*' && source[i + 1] === '/')) i++;
        i = Math.min(i + 2, source.length);
      } else if (ch === '/' && slashStartsRegex()) {
        scanRegex();
      } else if (stopAtTemplateBrace && ch === '{') {
        braceDepth++;
        i++;
      } else if (stopAtTemplateBrace && ch === '}') {
        braceDepth--;
        i++;
        if (braceDepth === 0) return;
      } else {
        i++;
      }
    }
  }

  scanCode();
  return fragments;
}

function windowCalls(text) {
  const names = new Set();
  for (const match of text.matchAll(WINDOW_CALL_RE)) {
    if (!BROWSER_WINDOW_APIS.has(match[1])) names.add(match[1]);
  }
  return names;
}

/**
 * @param {{jsSources: Record<string, string>, indexHtml: string}} sources
 * @returns {Set<string>}
 */
export function emittedOnclickHandlers({ jsSources, indexHtml }) {
  const handlers = new Set();

  for (const source of Object.values(jsSources)) {
    for (const fragment of jsStringFragments(source)) {
      for (const name of windowCalls(fragment)) handlers.add(name);
    }
  }

  for (const onclick of indexHtml.matchAll(HTML_ONCLICK_RE)) {
    for (const name of windowCalls(onclick[2])) handlers.add(name);
    for (const call of onclick[2].matchAll(BARE_HANDLER_CALL_RE)) {
      if (!BROWSER_WINDOW_APIS.has(call[1])) handlers.add(call[1]);
    }
  }

  return handlers;
}

function withoutJsComments(source) {
  let output = '';
  let i = 0;
  while (i < source.length) {
    if (source[i] === '/' && source[i + 1] === '/') {
      while (i < source.length && source[i] !== '\n') i++;
    } else if (source[i] === '/' && source[i + 1] === '*') {
      i += 2;
      while (i < source.length && !(source[i] === '*' && source[i + 1] === '/')) i++;
      i = Math.min(i + 2, source.length);
    } else {
      output += source[i++];
    }
  }
  return output;
}

/**
 * Public property keys exposed by main.js's Object.assign(window, {...}).
 * For an alias (`publicName: localName`), only `publicName` is exposed.
 *
 * @param {string} mainSource
 * @returns {Set<string>}
 */
export function exposedWindowBindings(mainSource) {
  const source = withoutJsComments(mainSource);
  const marker = /Object\.assign\s*\(\s*window\s*,\s*\{/g.exec(source);
  if (!marker) throw new Error('main.js has no Object.assign(window, {...}) binding block');

  const objectStart = marker.index + marker[0].lastIndexOf('{');
  let depth = 1;
  let objectEnd = -1;
  for (let cursor = objectStart + 1; cursor < source.length; cursor++) {
    if (source[cursor] === '{') depth++;
    if (source[cursor] === '}') depth--;
    if (depth === 0) {
      objectEnd = cursor;
      break;
    }
  }
  if (objectEnd < 0) throw new Error('unterminated Object.assign(window, {...}) binding block');

  const bindings = new Set();
  const body = source.slice(objectStart + 1, objectEnd);
  for (const entry of body.split(',')) {
    const match = /^\s*([A-Za-z_$][\w$]*)\s*(?::|$)/.exec(entry);
    if (match) bindings.add(match[1]);
  }
  return bindings;
}

/**
 * @param {{jsSources: Record<string, string>, indexHtml: string, mainSource: string}} sources
 */
export function auditWindowBindings({ jsSources, indexHtml, mainSource }) {
  const required = emittedOnclickHandlers({ jsSources, indexHtml });
  const exposed = exposedWindowBindings(mainSource);
  const missing = new Set([...required].filter((name) => !exposed.has(name)).sort());
  return { required, exposed, missing };
}

/**
 * @param {{jsSources: Record<string, string>, indexHtml: string, mainSource: string}} sources
 */
export function assertWindowBindings(sources) {
  const audit = auditWindowBindings(sources);
  if (audit.missing.size > 0) {
    throw new Error(`inline onclick handlers missing from window bindings: ${[...audit.missing].join(', ')}`);
  }
  return audit;
}
