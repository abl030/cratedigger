/**
 * Static audit helpers for inline onclick -> window binding coverage.
 *
 * The scanner projects only JavaScript string/template surfaces, reconstructs
 * concatenated literals, extracts actual onclick attributes, and then resolves
 * statically-known handler-body substitutions. Ordinary code, comments, and
 * unrelated strings are outside the contract.
 */

const BROWSER_CALLS = new Set([
  'alert',
  'blur',
  'cancelAnimationFrame',
  'clearInterval',
  'clearTimeout',
  'close',
  'confirm',
  'focus',
  'isFinite',
  'isNaN',
  'Number',
  'open',
  'parseFloat',
  'parseInt',
  'print',
  'prompt',
  'requestAnimationFrame',
  'scroll',
  'scrollBy',
  'scrollTo',
  'setInterval',
  'setTimeout',
  'String',
]);

const JS_KEYWORD_CALLS = new Set(['catch', 'for', 'if', 'switch', 'while', 'with']);
const HTML_ONCLICK_RE = /\bonclick\s*=\s*(["'])([\s\S]*?)\1/gi;
const WINDOW_CALL_RE = /\bwindow\.([A-Za-z_$][\w$]*)\s*\(/g;
const BARE_CALL_RE = /(?<![.\w$])([A-Za-z_$][\w$]*)\s*\(/g;
const PLACEHOLDER_START = '\u{e000}';
const PLACEHOLDER_END = '\u{e001}';
const PLACEHOLDER_RE = /\u{e000}([\s\S]*?)\u{e001}/u;

function decodeEscape(source, index) {
  const escaped = source[index];
  const decoded = { n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', v: '\v' }[escaped];
  return { value: decoded ?? escaped ?? '', next: Math.min(index + 1, source.length) };
}

function readQuoted(source, start) {
  const quote = source[start];
  let value = '';
  let i = start + 1;
  while (i < source.length) {
    if (source[i] === '\\') {
      const escaped = decodeEscape(source, i + 1);
      value += escaped.value;
      i = escaped.next;
    } else if (source[i] === quote) {
      return { value, end: i + 1 };
    } else {
      value += source[i++];
    }
  }
  return { value, end: source.length };
}

function skipLineComment(source, start) {
  let i = start + 2;
  while (i < source.length && source[i] !== '\n') i++;
  return i;
}

function skipBlockComment(source, start) {
  let i = start + 2;
  while (i < source.length && !(source[i] === '*' && source[i + 1] === '/')) i++;
  return Math.min(i + 2, source.length);
}

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
    if (ch === '\\') {
      i++;
    } else if (ch === '[') {
      inClass = true;
    } else if (ch === ']') {
      inClass = false;
    } else if (ch === '/' && !inClass) {
      while (/[A-Za-z]/.test(source[i] || '')) i++;
      return i;
    }
  }
  return i;
}

function readTemplateExpression(source, start) {
  let depth = 1;
  let i = start;
  while (i < source.length) {
    const ch = source[i];
    if (ch === "'" || ch === '"') {
      i = readQuoted(source, i).end;
    } else if (ch === '`') {
      i = readTemplate(source, i).end;
    } else if (ch === '/' && source[i + 1] === '/') {
      i = skipLineComment(source, i);
    } else if (ch === '/' && source[i + 1] === '*') {
      i = skipBlockComment(source, i);
    } else if (ch === '/' && slashStartsRegex(source, i)) {
      i = skipRegex(source, i);
    } else if (ch === '{') {
      depth++;
      i++;
    } else if (ch === '}') {
      depth--;
      if (depth === 0) return { expression: source.slice(start, i), end: i + 1 };
      i++;
    } else {
      i++;
    }
  }
  return { expression: source.slice(start), end: source.length };
}

function readTemplate(source, start) {
  let value = '';
  /** @type {string[]} */
  const expressions = [];
  let i = start + 1;
  while (i < source.length) {
    if (source[i] === '\\') {
      const escaped = decodeEscape(source, i + 1);
      value += escaped.value;
      i = escaped.next;
    } else if (source[i] === '`') {
      return { value, expressions, end: i + 1 };
    } else if (source[i] === '$' && source[i + 1] === '{') {
      const nested = readTemplateExpression(source, i + 2);
      const expression = nested.expression.trim();
      expressions.push(expression);
      value += `${PLACEHOLDER_START}${expression}${PLACEHOLDER_END}`;
      i = nested.end;
    } else {
      value += source[i++];
    }
  }
  return { value, expressions, end: source.length };
}

/**
 * Project string/template values from a JS expression or source region.
 * Adjacent literals connected only by `+` are also emitted as their combined
 * runtime surface, which reconstructs split HTML attributes.
 */
function jsStringSurfaces(source) {
  /** @type {{start: number, end: number, value: string}[]} */
  const tokens = [];
  /** @type {string[]} */
  const nestedSurfaces = [];
  let i = 0;
  while (i < source.length) {
    const ch = source[i];
    if (ch === "'" || ch === '"') {
      const quoted = readQuoted(source, i);
      tokens.push({ start: i, end: quoted.end, value: quoted.value });
      i = quoted.end;
    } else if (ch === '`') {
      const template = readTemplate(source, i);
      tokens.push({ start: i, end: template.end, value: template.value });
      for (const expression of template.expressions) {
        nestedSurfaces.push(...jsStringSurfaces(expression));
      }
      i = template.end;
    } else if (ch === '/' && source[i + 1] === '/') {
      i = skipLineComment(source, i);
    } else if (ch === '/' && source[i + 1] === '*') {
      i = skipBlockComment(source, i);
    } else if (ch === '/' && slashStartsRegex(source, i)) {
      i = skipRegex(source, i);
    } else {
      i++;
    }
  }

  const surfaces = [...tokens.map((token) => token.value), ...nestedSurfaces];
  let run = tokens[0]?.value ?? '';
  let runLength = tokens.length > 0 ? 1 : 0;
  for (let index = 1; index < tokens.length; index++) {
    const between = source.slice(tokens[index - 1].end, tokens[index].start);
    if (between.includes('+') && /^[\s+]*$/.test(between)) {
      run += tokens[index].value;
      runLength++;
    } else {
      const dynamicConcat = /^\s*\+\s*([\s\S]*?)\s*\+\s*$/.exec(between);
      if (dynamicConcat && !dynamicConcat[1].includes(';')) {
        const expression = dynamicConcat[1].trim();
        run += `${PLACEHOLDER_START}${expression}${PLACEHOLDER_END}${tokens[index].value}`;
        runLength++;
        continue;
      }
      if (runLength > 1) surfaces.push(run);
      run = tokens[index].value;
      runLength = 1;
    }
  }
  if (runLength > 1) surfaces.push(run);
  return [...new Set(surfaces)];
}

function findExpressionEnd(source, start, delimiters) {
  const closing = [];
  let i = start;
  while (i < source.length) {
    const ch = source[i];
    if (ch === "'" || ch === '"') {
      i = readQuoted(source, i).end;
    } else if (ch === '`') {
      i = readTemplate(source, i).end;
    } else if (ch === '/' && source[i + 1] === '/') {
      i = skipLineComment(source, i);
    } else if (ch === '/' && source[i + 1] === '*') {
      i = skipBlockComment(source, i);
    } else if (ch === '/' && slashStartsRegex(source, i)) {
      i = skipRegex(source, i);
    } else if (ch === '(' || ch === '[' || ch === '{') {
      closing.push({ '(': ')', '[': ']', '{': '}' }[ch]);
      i++;
    } else if (closing.length > 0 && ch === closing.at(-1)) {
      closing.pop();
      i++;
    } else if (closing.length === 0 && delimiters.has(ch)) {
      return i;
    } else {
      i++;
    }
  }
  return i;
}

function addValues(map, name, values) {
  const existing = map.get(name) ?? [];
  map.set(name, [...new Set([...existing, ...values])]);
}

function splitTopLevelConditional(expression) {
  let question = -1;
  let nestedQuestions = 0;
  const closing = [];
  let i = 0;
  while (i < expression.length) {
    const ch = expression[i];
    if (ch === "'" || ch === '"') {
      i = readQuoted(expression, i).end;
    } else if (ch === '`') {
      i = readTemplate(expression, i).end;
    } else if (ch === '/' && expression[i + 1] === '/') {
      i = skipLineComment(expression, i);
    } else if (ch === '/' && expression[i + 1] === '*') {
      i = skipBlockComment(expression, i);
    } else if (ch === '/' && slashStartsRegex(expression, i)) {
      i = skipRegex(expression, i);
    } else if (ch === '(' || ch === '[' || ch === '{') {
      closing.push({ '(': ')', '[': ']', '{': '}' }[ch]);
      i++;
    } else if (closing.length > 0 && ch === closing.at(-1)) {
      closing.pop();
      i++;
    } else if (closing.length === 0 && ch === '?' && expression[i + 1] !== '.' && expression[i + 1] !== '?') {
      if (question < 0) question = i;
      else nestedQuestions++;
      i++;
    } else if (closing.length === 0 && ch === ':' && question >= 0) {
      if (nestedQuestions > 0) nestedQuestions--;
      else return [expression.slice(question + 1, i), expression.slice(i + 1)];
      i++;
    } else {
      i++;
    }
  }
  return null;
}

function fullyStaticStringValues(expression) {
  const conditional = splitTopLevelConditional(expression);
  if (conditional) {
    const consequent = fullyStaticStringValues(conditional[0]);
    const alternate = fullyStaticStringValues(conditional[1]);
    if (consequent.length === 0 || alternate.length === 0) return [];
    return [...new Set([...consequent, ...alternate])];
  }

  const projected = codeProjection(expression);
  if (!/^[\s+()]*$/.test(projected)) return [];
  const surfaces = jsStringSurfaces(expression);
  if (surfaces.length === 0) return [];
  const maxLength = Math.max(...surfaces.map((surface) => surface.length));
  return surfaces.filter((surface) => surface.length === maxLength);
}

function codeProjection(source) {
  const projected = [...source];
  const mask = (start, end) => {
    for (let cursor = start; cursor < end; cursor++) {
      if (projected[cursor] !== '\n') projected[cursor] = ' ';
    }
  };
  let i = 0;
  while (i < source.length) {
    const ch = source[i];
    let end = i + 1;
    if (ch === "'" || ch === '"') {
      end = readQuoted(source, i).end;
    } else if (ch === '`') {
      end = readTemplate(source, i).end;
    } else if (ch === '/' && source[i + 1] === '/') {
      end = skipLineComment(source, i);
    } else if (ch === '/' && source[i + 1] === '*') {
      end = skipBlockComment(source, i);
    } else if (ch === '/' && slashStartsRegex(source, i)) {
      end = skipRegex(source, i);
    } else {
      i++;
      continue;
    }
    mask(i, end);
    i = end;
  }
  return projected.join('');
}

function staticStringAssignments(source) {
  const assignments = new Map();
  const declaration = /\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=/g;
  for (const match of codeProjection(source).matchAll(declaration)) {
    const start = match.index + match[0].length;
    const end = findExpressionEnd(source, start, new Set([';']));
    addValues(assignments, match[1], fullyStaticStringValues(source.slice(start, end)));
  }
  return assignments;
}

function staticOnclickPropertyValues(jsSources) {
  const values = [];
  const property = /\bonclick\s*:/g;
  for (const source of Object.values(jsSources)) {
    for (const match of codeProjection(source).matchAll(property)) {
      let start = match.index + match[0].length;
      while (/\s/.test(source[start] || '')) start++;
      const end = findExpressionEnd(source, start, new Set([',', '}']));
      values.push(...fullyStaticStringValues(source.slice(start, end)));
    }
  }
  return [...new Set(values)];
}

function onclickBodiesFromSurfaces(surfaces) {
  const bodies = [];
  for (const surface of surfaces) {
    for (const match of surface.matchAll(HTML_ONCLICK_RE)) bodies.push(match[2]);
  }
  return [...new Set(bodies)];
}

function placeholderIsInsideCallArguments(body, placeholderIndex) {
  let depth = 0;
  let quote = '';
  for (let i = 0; i < placeholderIndex; i++) {
    const ch = body[i];
    if (quote) {
      if (ch === '\\') i++;
      else if (ch === quote) quote = '';
    } else if (ch === "'" || ch === '"') {
      quote = ch;
    } else if (ch === '(') {
      depth++;
    } else if (ch === ')') {
      depth = Math.max(0, depth - 1);
    }
  }
  return depth > 0;
}

function resolveExpression(expression, assignments, onclickPropertyValues) {
  const trimmed = expression.trim();
  const identifier = /^([A-Za-z_$][\w$]*)$/.exec(trimmed);
  if (identifier) return assignments.get(identifier[1]) ?? [];
  if (/^[A-Za-z_$][\w$]*\.onclick$/.test(trimmed)) return onclickPropertyValues;
  return [];
}

function expandOnclickBody(body, assignments, onclickPropertyValues, origin) {
  let candidates = [body];
  const unresolved = [];
  for (let pass = 0; pass < 20; pass++) {
    let changed = false;
    const next = [];
    for (const candidate of candidates) {
      const placeholder = PLACEHOLDER_RE.exec(candidate);
      if (!placeholder) {
        next.push(candidate);
        continue;
      }
      changed = true;
      const replacements = resolveExpression(
        placeholder[1], assignments, onclickPropertyValues,
      );
      if (replacements.length > 0) {
        for (const replacement of replacements) {
          next.push(
            candidate.slice(0, placeholder.index)
            + replacement
            + candidate.slice(placeholder.index + placeholder[0].length),
          );
        }
      } else if (placeholderIsInsideCallArguments(candidate, placeholder.index)) {
        next.push(
          candidate.slice(0, placeholder.index)
          + '__DYNAMIC_ARGUMENT__'
          + candidate.slice(placeholder.index + placeholder[0].length),
        );
      } else {
        unresolved.push(`${origin}: unresolved ${placeholder[0]} in onclick="${body}"`);
      }
    }
    candidates = [...new Set(next)];
    if (!changed) break;
  }
  for (const candidate of candidates) {
    if (PLACEHOLDER_RE.test(candidate)) {
      unresolved.push(`${origin}: recursive/unresolved onclick="${body}"`);
    }
    if (/\bwindow\s*\[/.test(candidate)) {
      unresolved.push(`${origin}: computed window handler in onclick="${body}"`);
    }
  }
  return { bodies: candidates, unresolved };
}

function scanEmittedOnclicks({ jsSources, indexHtml }) {
  const onclickPropertyValues = staticOnclickPropertyValues(jsSources);
  const bodies = [];
  const unresolved = [];

  for (const [name, source] of Object.entries(jsSources)) {
    const assignments = staticStringAssignments(source);
    const rawBodies = onclickBodiesFromSurfaces(jsStringSurfaces(source));
    for (const body of rawBodies) {
      const expanded = expandOnclickBody(body, assignments, onclickPropertyValues, name);
      bodies.push(...expanded.bodies);
      unresolved.push(...expanded.unresolved);
    }
  }

  for (const match of indexHtml.matchAll(HTML_ONCLICK_RE)) bodies.push(match[2]);
  return {
    bodies: [...new Set(bodies)],
    unresolved: [...new Set(unresolved)].sort(),
  };
}

/** Actual expanded inline onclick bodies emitted by JS strings or index.html. */
export function emittedOnclickBodies(sources) {
  return scanEmittedOnclicks(sources).bodies;
}

function handlersFromBodies(bodies) {
  const handlers = new Set();
  for (const body of bodies) {
    for (const match of body.matchAll(WINDOW_CALL_RE)) {
      if (!BROWSER_CALLS.has(match[1])) handlers.add(match[1]);
    }
    for (const match of body.matchAll(BARE_CALL_RE)) {
      const name = match[1];
      if (!BROWSER_CALLS.has(name) && !JS_KEYWORD_CALLS.has(name)) handlers.add(name);
    }
  }
  return handlers;
}

/** Application handler names required by actual emitted onclick bodies. */
export function emittedOnclickHandlers(sources) {
  return handlersFromBodies(scanEmittedOnclicks(sources).bodies);
}

function withoutJsComments(source) {
  let output = '';
  let i = 0;
  while (i < source.length) {
    if (source[i] === '/' && source[i + 1] === '/') {
      const end = skipLineComment(source, i);
      output += source.slice(i, end).replace(/[^\n]/g, '');
      i = end;
    } else if (source[i] === '/' && source[i + 1] === '*') {
      const end = skipBlockComment(source, i);
      output += source.slice(i, end).replace(/[^\n]/g, '');
      i = end;
    } else {
      output += source[i++];
    }
  }
  return output;
}

/** Public property keys exposed by main.js's Object.assign(window, {...}). */
export function exposedWindowBindings(mainSource) {
  const source = withoutJsComments(mainSource);
  const marker = /Object\.assign\s*\(\s*window\s*,\s*\{/g.exec(source);
  if (!marker) throw new Error('main.js has no Object.assign(window, {...}) binding block');

  const objectStart = marker.index + marker[0].lastIndexOf('{');
  const objectEnd = findExpressionEnd(source, objectStart + 1, new Set(['}']));
  if (objectEnd >= source.length) {
    throw new Error('unterminated Object.assign(window, {...}) binding block');
  }

  const bindings = new Set();
  const body = source.slice(objectStart + 1, objectEnd);
  for (const entry of body.split(',')) {
    const match = /^\s*([A-Za-z_$][\w$]*)\s*(?::|$)/.exec(entry);
    if (match) bindings.add(match[1]);
  }
  return bindings;
}

export function auditWindowBindings({ jsSources, indexHtml, mainSource }) {
  const emitted = scanEmittedOnclicks({ jsSources, indexHtml });
  const required = handlersFromBodies(emitted.bodies);
  const exposed = exposedWindowBindings(mainSource);
  const missing = new Set([...required].filter((name) => !exposed.has(name)).sort());
  return { required, exposed, missing, unresolved: emitted.unresolved };
}

export function assertWindowBindings(sources) {
  const audit = auditWindowBindings(sources);
  if (audit.unresolved.length > 0) {
    throw new Error(`unresolved inline onclick handlers:\n${audit.unresolved.join('\n')}`);
  }
  if (audit.missing.size > 0) {
    throw new Error(`inline onclick handlers missing from window bindings: ${[...audit.missing].join(', ')}`);
  }
  return audit;
}
