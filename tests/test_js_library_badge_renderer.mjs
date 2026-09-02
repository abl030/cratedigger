/** Deterministic contract for the Library live-corpus badge renderer. */

import { renderLibraryBadgeCorpusRow } from '../scripts/render_library_badges.mjs';

import { suite } from './js_harness.mjs';

const t = suite(import.meta.url);

t.section('renderLibraryBadgeCorpusRow() uses the production Library badge path');
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

  t.ok(rendered.id === 17, 'synthetic corpus identity is retained');
  t.ok(Object.keys(rendered.fields).join(',') === 'row_html',
    'the differential watches the complete production row HTML');
  t.contains(rendered.fields.row_html, '>captured<',
    'captured history renders through production');
  t.contains(rendered.fields.row_html, '>missing<',
    'captured current absence renders through production');
  t.contains(rendered.fields.row_html, '>verified<',
    'carried proof renders through production');
  t.contains(rendered.fields.row_html, '>wanted<',
    'current acquisition lifecycle renders through production');
  t.ok(JSON.stringify(row) === before, 'rendering does not mutate the corpus row');
}

t.section('renderLibraryBadgeCorpusRow() fails closed without integer corpus identity');
{
  let rejected = false;
  try {
    renderLibraryBadgeCorpusRow({ _corpus_id: '17' });
  } catch (error) {
    rejected = error instanceof Error
      && error.message.includes('integer _corpus_id');
  }
  t.ok(rejected, 'known-bad string identity is rejected');
}

t.done();
