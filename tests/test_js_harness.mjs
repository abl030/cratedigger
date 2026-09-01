/**
 * Tests for the shared JavaScript test harness (issue #1313, candidate 6).
 *
 * Every behaviour that only shows up in a REAL process — the exit code, the
 * `process.on('exit')` guard for a suite that never reaches `done()`, the
 * repo-relative identity in a marker — is driven through an actual `node`
 * child running an actual fixture file, never simulated in-process. Running
 * a second suite inside this one would register a second exit guard and
 * corrupt this file's own exit code, which is the same reason the fixtures
 * are files rather than closures.
 *
 * Test infrastructure, so deterministic only (`.claude/rules/code-quality.md`
 * § "Never property-test the test machinery").
 */

import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  DONE_MARKER,
  FAILURE_MARKER,
  domStub,
  element,
  stubGlobals,
  suite,
} from './js_harness.mjs';

const t = suite(import.meta.url);

const TESTS_DIR = path.dirname(fileURLToPath(import.meta.url));
const HARNESS = path.join(TESTS_DIR, 'js_harness.mjs');

/**
 * Run `body` as a suite in a child process and return its result.
 *
 * `inRepo` places the fixture beside the harness so the repo-relative
 * identity is exercised; otherwise it goes to a scratch directory.
 */
function runFixture(body, { inRepo = false } = {}) {
  const dir = inRepo ? TESTS_DIR : mkdtempSync(path.join(tmpdir(), 'js-harness-'));
  // `_`-prefixed: `run_js_checks.sh` globs `test_js_*.mjs`, so an in-repo
  // fixture must not look like a suite even for the instant it exists.
  const file = path.join(dir, `_js_harness_fixture_${process.pid}_${Math.random().toString(36).slice(2)}.mjs`);
  const source = `import { suite, stubGlobals, domStub, element } from ${JSON.stringify(HARNESS)};\n`
    + `const t = suite(import.meta.url);\n${body}\n`;
  writeFileSync(file, source);
  try {
    const child = spawnSync(process.execPath, [file], { encoding: 'utf8' });
    return {
      status: child.status,
      stdout: child.stdout,
      stderr: child.stderr,
      lines: child.stdout.split('\n'),
      markers: child.stdout.split('\n').filter(line => line.startsWith(`${FAILURE_MARKER}\t`)),
      file: path.relative(path.resolve(TESTS_DIR, '..'), file),
    };
  } finally {
    rmSync(file, { force: true });
    if (!inRepo) rmSync(dir, { force: true, recursive: true });
  }
}

t.section('a green suite exits 0 and reports its tally');
{
  const run = runFixture("t.equal(1, 1, 'one'); t.ok(true, 'two'); t.done();");
  t.equal(run.status, 0, 'a suite with no failures exits 0');
  t.contains(run.stdout, '2 passed, 0 failed', 'the tally counts both assertions');
  t.contains(run.stdout, `${DONE_MARKER}\t`, 'the done marker is emitted');
  t.equal(run.markers.length, 0, 'no failure marker on a green suite');
}

t.section('a failing suite emits one marker per failed assertion');
{
  const run = runFixture(
    "t.section('sec');\n"
    + "t.equal(1, 2, 'first');\n"
    + "t.contains('abc', 'z', 'second');\n"
    + "t.ok(true, 'third');\n"
    + 't.done();',
    { inRepo: true },
  );
  t.equal(run.status, 1, 'any failed assertion makes the process exit 1');
  t.equal(run.markers.length, 2, 'exactly one marker per FAILED assertion, not one per file');
  t.equal(
    run.markers[0].split('\t')[1],
    `${run.file}::sec::first`,
    'identity is <repo-relative file>::<section>::<message>',
  );
  t.equal(run.markers[0].split('\t')[2], 'expected 2, got 1', 'detail carries the diff');
  t.equal(
    run.markers[1].split('\t')[1],
    `${run.file}::sec::second`,
    'the second failure gets its own identity',
  );
  t.contains(run.stdout, `${DONE_MARKER}\t${run.file}\t1\t2`, 'the done marker carries the tally');
}

t.section('every marker is exactly three tab-separated fields on one line');
{
  const run = runFixture(
    "t.equal('a\\tb\\nc', 'x\\ty\\nz', 'msg\\twith\\ttabs\\nand newline');\nt.done();",
  );
  t.equal(run.markers.length, 1, 'the embedded newline did not split the marker into two lines');
  t.equal(run.markers[0].split('\t').length, 3, 'tabs in message and detail are collapsed');
  t.contains(run.markers[0], 'msg with tabs and newline', 'the message survives, flattened');
}

t.section('a long detail is truncated so one marker stays one readable line');
{
  const run = runFixture(
    `t.contains('${'x'.repeat(5000)}', 'needle', 'huge haystack');\nt.done();`,
  );
  const detail = run.markers[0].split('\t')[2];
  t.ok(detail.length <= 400, `detail is capped (was ${detail.length})`);
  t.contains(detail, '…', 'truncation is marked with an ellipsis');
}

t.section('a suite that never reaches done() fails closed');
{
  const run = runFixture("t.equal(1, 1, 'ran'); // no done()", { inRepo: true });
  t.equal(run.status, 1, 'a forgotten done() cannot pass silently');
  t.equal(run.markers.length, 1, 'the exit guard emits exactly one marker');
  t.contains(
    run.markers[0],
    `${run.file}::suite never reached done()`,
    'the marker names the file and the reason',
  );
  t.excludes(run.stdout, DONE_MARKER, 'no done marker, so run_js_checks.sh adds its file-level finding');
}

t.section('a suite that throws before done() leaves no done marker');
{
  const run = runFixture("t.equal(1, 1, 'ran');\nthrow new Error('boom');\nt.done();");
  t.equal(run.status, 1, 'the throw propagates as a nonzero exit');
  t.excludes(run.stdout, DONE_MARKER, 'the crash is distinguishable from a clean finish');
  t.contains(run.stderr, 'boom', 'the real error still reaches stderr');
}

t.section('checker vocabulary — the passing side of every method');
{
  const run = runFixture(
    "t.ok(1, 'ok'); t.notOk(0, 'notOk');\n"
    + "t.equal('a', 'a', 'equal'); t.notEqual('a', 'b', 'notEqual');\n"
    + "t.deepEqual({x: [1]}, {x: [1]}, 'deepEqual');\n"
    + "t.contains('abc', 'b', 'contains'); t.excludes('abc', 'z', 'excludes');\n"
    + "t.match('abc', /b/, 'match'); t.notMatch('abc', /z/, 'notMatch');\n"
    + "t.throws(() => { throw new TypeError('x'); }, 'throws', TypeError);\n"
    + "t.pass();\n"
    + "await t.rejects(Promise.reject(new RangeError('x')), 'rejects', RangeError);\n"
    + 't.done();',
  );
  t.equal(run.status, 0, 'every passing-side assertion passes');
  t.contains(run.stdout, '12 passed, 0 failed', 'all twelve counted exactly once');
}

t.section('checker vocabulary — the failing side of every method');
{
  const run = runFixture(
    "t.ok(0, 'ok'); t.notOk(1, 'notOk');\n"
    + "t.equal('a', 'b', 'equal'); t.notEqual('a', 'a', 'notEqual');\n"
    + "t.deepEqual({x: [1]}, {x: [2]}, 'deepEqual');\n"
    + "t.contains('abc', 'z', 'contains'); t.excludes('abc', 'b', 'excludes');\n"
    + "t.match('abc', /z/, 'match'); t.notMatch('abc', /b/, 'notMatch');\n"
    + "t.throws(() => {}, 'throwsNothing');\n"
    + "t.throws(() => { throw new TypeError('x'); }, 'throwsWrongClass', RangeError);\n"
    + "t.fail('explicit');\n"
    + "await t.rejects(Promise.resolve(1), 'rejectsNothing');\n"
    + "await t.rejects(Promise.reject(new TypeError('x')), 'rejectsWrongClass', RangeError);\n"
    + 't.done();',
  );
  t.equal(run.status, 1, 'the failing side exits 1');
  t.equal(run.markers.length, 14, 'each of the fourteen failing assertions reports once');
  t.contains(run.stdout, '0 passed, 14 failed', 'none of them was miscounted as a pass');
  t.contains(run.stdout, 'expected a throw, none happened', 'throws() explains a missing throw');
  t.contains(run.stdout, 'expected a rejection, none happened', 'rejects() explains a missing rejection');
  t.contains(run.stdout, 'expected RangeError, got TypeError', 'the wrong error class is named');
}

t.section('a suite that stubs console cannot swallow its own markers');
{
  // Suites legitimately stub console to assert what production logs;
  // tests/test_js_search_plan.mjs did, and had to hand-capture
  // console.error at module load to keep its own output.
  const run = runFixture(
    "console.log = () => {}; console.error = () => {};\n"
    + "globalThis.console = { log() {}, error() {}, warn() {} };\n"
    + "t.section('stubbed');\n"
    + "t.equal(1, 2, 'still reported');\n"
    + 't.done();',
  );
  t.equal(run.status, 1, 'the failure still fails the process');
  t.equal(run.markers.length, 1, 'the marker survived a fully replaced console');
  t.contains(run.markers[0], 'stubbed::still reported', 'the section header was tracked too');
  t.contains(run.stdout, DONE_MARKER, 'the done marker survived as well');
  t.contains(run.stderr, 'FAIL: still reported', 'the human-readable line survived');
}

t.section('an unlabelled assertion has no section segment');
{
  const run = runFixture("t.equal(1, 2, 'bare');\nt.done();", { inRepo: true });
  t.equal(
    run.markers[0].split('\t')[1],
    `${run.file}::bare`,
    'without section() the identity is <file>::<message>',
  );
}

t.section('stubGlobals restores exactly what was there before');
{
  const sentinel = { marker: 'original' };
  globalThis.__harnessPresent = sentinel;
  t.excludes(Object.keys(globalThis).join(','), '__harnessAbsent', 'the absent key really is absent first');

  const stubs = stubGlobals({ __harnessPresent: 'stub', __harnessAbsent: 'stub' });
  t.equal(globalThis.__harnessPresent, 'stub', 'an existing global is replaced');
  t.equal(globalThis.__harnessAbsent, 'stub', 'a new global is installed');

  stubs.restore();
  t.equal(globalThis.__harnessPresent, sentinel, 'the original value is restored by identity');
  t.equal(
    Object.prototype.hasOwnProperty.call(globalThis, '__harnessAbsent'),
    false,
    'a key that was absent is DELETED, not left as undefined',
  );
  delete globalThis.__harnessPresent;
}

t.section('stubGlobals restore is idempotent');
{
  globalThis.__harnessTwice = 'before';
  const stubs = stubGlobals({ __harnessTwice: 'after' });
  stubs.restore();
  globalThis.__harnessTwice = 'changed since';
  stubs.restore();
  t.equal(globalThis.__harnessTwice, 'changed since', 'a second restore does not re-apply a stale value');
  delete globalThis.__harnessTwice;
}

t.section('domStub resolves seeded ids and nothing else');
{
  const button = element({ textContent: 'Go' });
  const doc = domStub({ 'my-btn': button });
  t.equal(doc.getElementById('my-btn'), button, 'a seeded id resolves to its element');
  t.equal(doc.getElementById('missing'), null, 'an unseeded id is null, not undefined');
  t.equal(doc.getElementById('toString'), null, 'an inherited Object key is not mistaken for an element');
  t.equal(doc.querySelector('.anything'), null, 'querySelector defaults to null');
  t.deepEqual(doc.querySelectorAll('.anything'), [], 'querySelectorAll defaults to empty');
}

t.section('domStub extras override the defaults');
{
  const doc = domStub({}, { querySelector: () => 'overridden', body: 'the body' });
  t.equal(doc.querySelector('x'), 'overridden', 'a bespoke querySelector wins');
  t.equal(doc.body, 'the body', 'an extra field is added');
  t.equal(doc.getElementById('x'), null, 'the id lookup still works');
}

t.section('element() gives the fields render code touches');
{
  const el = element();
  t.equal(el.textContent, '', 'textContent defaults to empty');
  t.equal(el.disabled, false, 'disabled defaults to false');
  t.equal(el.removed, false, 'removed starts false');
  el.remove();
  t.equal(el.removed, true, 'remove() flags the element');
  el.setAttribute('aria-disabled', 'true');
  t.equal(el.getAttribute('aria-disabled'), 'true', 'setAttribute round-trips through getAttribute');
  t.equal(el.getAttribute('never-set'), null, 'an unset attribute reads null');

  const seeded = element({ textContent: 'seed', extra: 1 });
  t.equal(seeded.textContent, 'seed', 'a seeded field overrides the default');
  t.equal(seeded.extra, 1, 'an unknown seeded field is kept');
}

t.done();
