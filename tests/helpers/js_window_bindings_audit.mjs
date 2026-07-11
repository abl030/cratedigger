/**
 * Conservative static audit for inline window handlers (issue #603).
 *
 * Every statically named `window.name(` in a web JS string/template surface
 * counts, even when this scanner cannot prove the string becomes an onclick
 * attribute. That deliberate over-approximation prefers a fixable false
 * positive to a silently dead button. index.html is narrower: only inline
 * onclick bodies are scanned, including their bare handler calls.
 */

const RESERVED_NATIVE_WINDOW_CALLS = new Set([
  'alert',
  'blur',
  'cancelAnimationFrame',
  'clearInterval',
  'clearTimeout',
  'close',
  'confirm',
  'fetch',
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
const NON_HANDLER_BARE_CALLS = new Set([
  ...RESERVED_NATIVE_WINDOW_CALLS,
  'catch',
  'for',
  'if',
  'parseFloat',
  'parseInt',
  'switch',
  'while',
]);

const STATIC_WINDOW_CALL_RE = /\bwindow\.([A-Za-z_$][\w$]*)\s*\(/g;
const COMPUTED_WINDOW_RE = /\bwindow\s*\[/;
const INTERPOLATED_WINDOW_RE = /\bwindow\s*\.\s*\$\{/;
const ONCLICK_RE = /\bonclick\s*=\s*(["'])([\s\S]*?)\1/gi;
const BARE_CALL_RE = /(?<![.\w$])([A-Za-z_$][\w$]*)\s*\(/g;

function slashStartsRegex(source, index) {
  let cursor = index - 1;
  while (cursor >= 0 && /\s/.test(source[cursor])) cursor--;
  return cursor < 0 || /[({\[=,:;!?&|+*%~<>]/.test(source[cursor]);
}

function skipRegex(source, start) {
  let i = start + 1;
  let inClass = false;
  while (i < source.length) {
    const ch = source[i++];
    if (ch === '\\') i++;
    else if (ch === '[') inClass = true;
    else if (ch === ']') inClass = false;
    else if (ch === '/' && !inClass) {
      while (/[A-Za-z]/.test(source[i] || '')) i++;
      break;
    }
  }
  return i;
}

/** Return literal contents while skipping ordinary code and comments. */
function jsLiteralSurfaces(source) {
  const surfaces = [];
  let i = 0;

  function quoted(quote) {
    i++;
    let value = '';
    while (i < source.length) {
      if (source[i] === '\\') {
        value += source[i++];
        if (i < source.length) value += source[i++];
      } else if (source[i] === quote) {
        i++;
        surfaces.push(value);
        return;
      } else value += source[i++];
    }
    surfaces.push(value);
  }

  function template() {
    i++;
    let value = '';
    while (i < source.length) {
      if (source[i] === '\\') {
        value += source[i++];
        if (i < source.length) value += source[i++];
      } else if (source[i] === '`') {
        i++;
        surfaces.push(value);
        return;
      } else if (source[i] === '$' && source[i + 1] === '{') {
        value += '${...}';
        i += 2;
        code(true);
      } else value += source[i++];
    }
    surfaces.push(value);
  }

  function code(stopAtTemplateBrace = false) {
    let braces = stopAtTemplateBrace ? 1 : 0;
    while (i < source.length) {
      const ch = source[i];
      if (ch === "'" || ch === '"') quoted(ch);
      else if (ch === '`') template();
      else if (ch === '/' && source[i + 1] === '/') {
        i += 2;
        while (i < source.length && source[i] !== '\n') i++;
      } else if (ch === '/' && source[i + 1] === '*') {
        i += 2;
        while (i < source.length && !(source[i] === '*' && source[i + 1] === '/')) i++;
        i = Math.min(i + 2, source.length);
      } else if (ch === '/' && slashStartsRegex(source, i)) i = skipRegex(source, i);
      else if (stopAtTemplateBrace && ch === '{') {
        braces++;
        i++;
      } else if (stopAtTemplateBrace && ch === '}') {
        i++;
        if (--braces === 0) return;
      } else i++;
    }
  }

  code();
  return surfaces;
}

function collectSurface(surface, origin, handlers, dynamicCallees, includeBare) {
  if (COMPUTED_WINDOW_RE.test(surface) || INTERPOLATED_WINDOW_RE.test(surface)) {
    dynamicCallees.push(`${origin}: ${surface.trim()}`);
  }
  for (const match of surface.matchAll(STATIC_WINDOW_CALL_RE)) {
    if (!RESERVED_NATIVE_WINDOW_CALLS.has(match[1])) handlers.add(match[1]);
  }
  if (includeBare) {
    for (const match of surface.matchAll(BARE_CALL_RE)) {
      if (!NON_HANDLER_BARE_CALLS.has(match[1])) handlers.add(match[1]);
    }
  }
}

export function emittedWindowHandlers({ jsSources, indexHtml }) {
  const handlers = new Set();
  const dynamicCallees = [];
  for (const [name, source] of Object.entries(jsSources)) {
    for (const surface of jsLiteralSurfaces(source)) {
      collectSurface(surface, name, handlers, dynamicCallees, false);
    }
  }
  for (const match of indexHtml.matchAll(ONCLICK_RE)) {
    collectSurface(match[2], 'web/index.html', handlers, dynamicCallees, true);
  }
  return { handlers, dynamicCallees: [...new Set(dynamicCallees)].sort() };
}

function stripComments(source) {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\/\/[^\n]*/g, '');
}

function topLevelEntries(body) {
  const entries = [];
  let start = 0;
  let depth = 0;
  for (let i = 0; i < body.length; i++) {
    if ('{[('.includes(body[i])) depth++;
    else if ('}])'.includes(body[i])) depth--;
    else if (body[i] === ',' && depth === 0) {
      entries.push(body.slice(start, i));
      start = i + 1;
    }
    if (depth < 0) throw new Error('unsupported Object.assign window binding syntax');
  }
  if (depth !== 0) throw new Error('unsupported Object.assign window binding syntax');
  entries.push(body.slice(start));
  return entries;
}

/** Return top-level public keys from Object.assign(window, {...}). */
export function exposedWindowBindings(mainSource) {
  const source = stripComments(mainSource);
  const marker = /Object\.assign\s*\(\s*window\s*,\s*\{/.exec(source);
  if (!marker) throw new Error('main.js has no Object.assign(window, {...}) binding block');
  const open = marker.index + marker[0].lastIndexOf('{');
  let depth = 1;
  let close = -1;
  for (let i = open + 1; i < source.length; i++) {
    if (source[i] === '{') depth++;
    else if (source[i] === '}' && --depth === 0) {
      close = i;
      break;
    }
  }
  if (close < 0) throw new Error('unterminated Object.assign(window, {...}) binding block');

  const bindings = new Set();
  for (const rawEntry of topLevelEntries(source.slice(open + 1, close))) {
    const entry = rawEntry.trim();
    if (!entry) continue;
    const shorthand = /^([A-Za-z_$][\w$]*)$/.exec(entry);
    const keyed = /^([A-Za-z_$][\w$]*)\s*:/.exec(entry);
    if (!shorthand && !keyed) throw new Error(`unsupported window binding entry: ${entry}`);
    bindings.add((shorthand ?? keyed)[1]);
  }
  return bindings;
}

export function auditWindowBindings({ jsSources, indexHtml, mainSource }) {
  const emitted = emittedWindowHandlers({ jsSources, indexHtml });
  const exposed = exposedWindowBindings(mainSource);
  const missing = new Set([...emitted.handlers].filter((name) => !exposed.has(name)).sort());
  const nativeCollisions = new Set(
    [...exposed].filter((name) => RESERVED_NATIVE_WINDOW_CALLS.has(name)).sort(),
  );
  return {
    required: emitted.handlers,
    exposed,
    missing,
    dynamicCallees: emitted.dynamicCallees,
    nativeCollisions,
  };
}

export function assertWindowBindings(sources) {
  const audit = auditWindowBindings(sources);
  if (audit.dynamicCallees.length) {
    throw new Error(`dynamic window callee forms are unsupported:\n${audit.dynamicCallees.join('\n')}`);
  }
  if (audit.nativeCollisions.size) {
    throw new Error(`app bindings collide with reserved native window names: ${[...audit.nativeCollisions].join(', ')}`);
  }
  if (audit.missing.size) {
    throw new Error(`static window handlers missing from bindings: ${[...audit.missing].join(', ')}`);
  }
  return audit;
}
