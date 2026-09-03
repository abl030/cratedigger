# User Cooldowns (issue #39)

Global, temporary cooldowns for Soulseek users who consistently fail to deliver downloads. Separate from the per-request quality denylist (`source_denylist`) — cooldowns are **global** (not per-album) and time-bounded.

## How it works

After every timeout or beets rejection, `check_and_apply_cooldown(username)` queries the user's last 5 download outcomes globally (across all albums). If all 5 are failures (timeout/failed/rejected), a 3-day cooldown is inserted into `user_cooldowns`. During enqueue, cooled-down users are skipped with a distinct "on cooldown" log message.

## Tunables (`CooldownConfig` in `lib/quality/download_state.py`)

| Field | Default | Purpose |
|-------|---------|---------|
| `failure_threshold` | 5 | Consecutive failures before cooldown |
| `cooldown_days` | 3 | Cooldown duration |
| `failure_outcomes` | timeout, failed, rejected | Which outcomes count as failures |
| `lookback_window` | 5 | How many recent outcomes to check |

## Table: `user_cooldowns`

```sql
CREATE TABLE user_cooldowns (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    cooldown_until TIMESTAMPTZ NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- `UNIQUE(username)` — one active cooldown per user, upsert extends it.
- No `request_id` — this is global across all albums.
- Expired rows are harmless (filtered by `cooldown_until > NOW()`).

## Data flow

1. **Trigger**: `_timeout_album()` (download.py) calls `db.check_and_apply_cooldown(username)` directly after logging the timeout outcome. `reject_and_requeue()` (`album_source.py`) and the installed-HAVE abort (`_record_have_analysis_error` in `lib/dispatch/outcome_actions.py`) instead reach the SAME streak evaluator (`_cooldown_streak_verdict`, shared by `check_and_apply_cooldown` and the terminal-outcome writer): on their JOB-LESS branch, through `PipelineDB.persist_request_rejection_outcome` (issue #1355 item 3), which commits the cooldown decision atomically alongside the request transition, `download_log` audit, and denylist writes, rather than as a separate autocommit call after them — `_record_have_analysis_error` used to call `check_and_apply_cooldown` directly after its own commit on this branch, which this change folded into the same transaction; on their job-backed branch, through the existing `PipelineDB.persist_import_terminal_outcome`.

   Inclusion rule for the list below: every reason the vocabulary can emit is
   named, and the ones that are unreachable by construction are marked — the
   vocabulary is deliberately total over (containment, missing, open, read,
   write, unclassified) × (source file, shared share, private tree) so a new
   raise site cannot land in another subject's noun.

   - Source file: `event_path_never_stamped`, `event_path_gone_from_disk`,
     `unsafe_source_path`, `source_open_failed_<ERRNO>`,
     `source_read_failed_<ERRNO>`, `source_write_failed_<ERRNO>` (unreachable:
     nothing writes the share), `source_preflight_refused` (unreachable: every
     code the preflight can hit is classified).
   - Shared slskd share: `slskd_root_unsafe`, `slskd_root_missing`,
     `slskd_root_open_failed_<ERRNO>`, `slskd_root_read_failed_<ERRNO>`
     (unreachable: the share is opened, never read as a directory),
     `slskd_root_write_failed_<ERRNO>` (unreachable), `slskd_root_refused`.
   - Our private processing tree: `processing_authority_unsafe`,
     `processing_path_missing`, `processing_open_failed_<ERRNO>`,
     `processing_read_failed_<ERRNO>`, `processing_write_failed_<ERRNO>`,
     `materialize_authority_failed`, `private_materialize_failed`.
   - Staged-path readiness: `staged_path_missing`,
     `staged_path_missing_tracked_files`, `empty_manifest`,
     `duplicate_final_basename`. Historical audit rows may still carry
     `abandoned_interrupted_auto_import` from the retired job-less recovery
     path.

   The cooldown is applied identically whichever reason it was — the reason is
   evidence, never a lifecycle input. `lib/failure_presentation.py` turns each
   of them into operator copy at render time; the raw token stays in the column.

2. **Decision**: `check_and_apply_cooldown()` queries `download_log` for last N outcomes, delegates to `should_cooldown()` pure function.
3. **Storage**: If triggered, upserts `user_cooldowns` with `cooldown_until = NOW() + 3 days`.
4. **Cache**: `ctx.cooled_down_users` populated at cycle start by the registered Phase-0 step `lib/user_cooldowns.py::load_user_cooldowns`, shared with Phase 1 thread. Updated in real-time when new cooldowns are applied mid-cycle.
5. **Enforcement**: `try_enqueue()` and `try_multi_enqueue()` in `lib/enqueue.py` skip users in `ctx.cooled_down_users` before checking the per-request denylist.

Cooldowns are user-level and remain authoritative before Redis peer-cache lookups. A cooled-down user is skipped before any per-directory positive or negative cache check, so persistent `peer_dir_neg:{user}:{dir}` entries never override the global cooldown decision.

## Re-cooldown behavior

After the 3-day cooldown expires, the user gets one chance. If they succeed, the success breaks their failure streak. If they fail, `check_and_apply_cooldown` sees 4 old failures + 1 new = 5 failures → immediate re-cooldown.

## Diagnostics

```bash
# View active cooldowns
pipeline-cli query - <<'SQL'
SELECT username, cooldown_until, reason
FROM user_cooldowns
WHERE cooldown_until > NOW();
SQL

# View all cooldowns (including expired)
pipeline-cli query - <<'SQL'
SELECT * FROM user_cooldowns ORDER BY cooldown_until DESC;
SQL

# Top timeout offenders
pipeline-cli query - <<'SQL'
SELECT soulseek_username, COUNT(*)
FROM download_log
WHERE outcome = 'timeout'
GROUP BY soulseek_username
ORDER BY count DESC
LIMIT 10;
SQL

# Manually seed cooldowns for all users with 5+ consecutive failures
pipeline-cli query --write --confirm WRITE - <<'SQL'
INSERT INTO user_cooldowns (username, cooldown_until, reason)
SELECT username, NOW() + INTERVAL '3 days', 'operator one-off seed'
FROM ...;
SQL
```
