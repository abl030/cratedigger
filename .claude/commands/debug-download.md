# Debug Download

Query full audit trail for a download_log entry. Pass the download_log ID or an album name.

## Usage

`/debug-download 265` or `/debug-download "Mono Masters"`

## Steps

1. Find the row:
```bash
ssh doc2 "psql -h 192.168.100.11 -U cratedigger cratedigger -c \"
SELECT dl.id, ar.artist_name, ar.album_title, dl.outcome, dl.beets_scenario,
       dl.import_result IS NOT NULL as has_ir,
       dl.validation_result IS NOT NULL as has_vr,
       dl.created_at
FROM download_log dl
JOIN album_requests ar ON dl.request_id = ar.id
WHERE dl.id = <ID> OR ar.album_title ILIKE '%<NAME>%'
ORDER BY dl.id DESC LIMIT 5;
\""
```

2. Import decision (if has_ir):
```bash
ssh doc2 "psql -h 192.168.100.11 -U cratedigger cratedigger -c \"
SELECT import_result->>'decision' as decision,
       import_result->'quality'->>'new_min_bitrate' as new_br,
       import_result->'quality'->>'prev_min_bitrate' as prev_br,
       import_result->'quality'->>'post_conversion_min_bitrate' as post_conv,
       import_result->'quality'->>'is_transcode' as transcode,
       import_result->'quality'->>'will_be_verified_lossless' as verified,
       import_result->'spectral'->>'grade' as spectral,
       import_result->'spectral'->>'suspect_pct' as suspect_pct,
       import_result->'conversion'->>'converted' as converted,
       import_result->'postflight'->>'imported_path' as path
FROM download_log WHERE id = <ID>;
\""
```

3. Per-track spectral (if has per_track):
```bash
ssh doc2 "psql -h 192.168.100.11 -U cratedigger cratedigger -c \"
SELECT t->>'grade' as grade, t->>'hf_deficit_db' as hf_deficit,
       t->>'cliff_detected' as cliff, t->>'estimated_bitrate_kbps' as est_br
FROM download_log, jsonb_array_elements(import_result->'spectral'->'per_track') AS t
WHERE id = <ID>;
\""
```

4. Validation detail (if has_vr):
```bash
ssh doc2 "psql -h 192.168.100.11 -U cratedigger cratedigger -c \"
SELECT validation_result->>'scenario' as scenario,
       validation_result->>'distance' as distance,
       validation_result->>'recommendation' as rec,
       validation_result->>'failed_path' as failed_path,
       validation_result->'denylisted_users' as banned,
       validation_result->'candidates'->0->'distance_breakdown' as breakdown,
       validation_result->'candidates'->0->>'media' as media,
       validation_result->'candidates'->0->>'albumdisambig' as disambig
FROM download_log WHERE id = <ID>;
\""
```

5. Track mapping (if has_vr with mapping):
```bash
ssh doc2 "psql -h 192.168.100.11 -U cratedigger cratedigger -c \"
SELECT m->'item'->>'title' as local_title,
       m->'track'->>'title' as mb_title,
       m->'item'->>'path' as local_file
FROM download_log, jsonb_array_elements(validation_result->'candidates'->0->'mapping') AS m
WHERE id = <ID>;
\""
```
