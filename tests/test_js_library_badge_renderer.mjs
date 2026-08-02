/** Deterministic contract for the Library live-corpus badge renderer. */

import { renderLibraryBadgeCorpusRow } from '../scripts/render_library_badges.mjs';

let passed = 0;
let failed = 0;

function assert(condition, message) {
  if (condition) passed += 1;
  else {
    failed += 1;
    console.error(`  FAIL: ${message}`);
  }
}

console.log('renderLibraryBadgeCorpusRow() uses the production Library badge path');
{
  const row = {
    _corpus_id: 17,
    id: 41,
    mb_albumid: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    in_library: false,
    has_captured_history: true,
    pipeline_status: 'wanted',
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  };
  const before = JSON.stringify(row);
  const rendered = renderLibraryBadgeCorpusRow(row);

  assert(rendered.id === 17, 'synthetic corpus identity is retained');
  assert(Object.keys(rendered.fields).join(',') === 'row_html',
    'the differential watches the complete production row HTML');
  assert(rendered.fields.row_html.includes('>captured<'),
    'captured history renders through production');
  assert(rendered.fields.row_html.includes('>missing<'),
    'captured current absence renders through production');
  assert(rendered.fields.row_html.includes('>verified<'),
    'carried proof renders through production');
  assert(rendered.fields.row_html.includes('>wanted<'),
    'current acquisition lifecycle renders through production');
  assert(JSON.stringify(row) === before, 'rendering does not mutate the corpus row');
}

console.log('renderLibraryBadgeCorpusRow() fails closed without integer corpus identity');
{
  let rejected = false;
  try {
    renderLibraryBadgeCorpusRow({ _corpus_id: '17' });
  } catch (error) {
    rejected = error instanceof Error
      && error.message.includes('integer _corpus_id');
  }
  assert(rejected, 'known-bad string identity is rejected');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
