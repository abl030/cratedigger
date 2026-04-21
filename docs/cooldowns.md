# User Cooldowns (issue #39)

Global, temporary cooldowns for Soulseek users who consistently fail to deliver downloads. Separate from the per-request quality denylist (`source_denylist`) — cooldowns are **global** (not per-album) and time-bounded.

## How it works

After every timeout or beets rejection, `check_and_apply_cooldown(username)` queries the user's last 5 download outcomes globally (across all albums). If all 5 are failures (timeout/failed/rejected), a 3-day cooldown is inserted into `user_cooldowns`. During enqueue, cooled-down users are skipped with a distinct "on cooldown" log message.

## Tunables (`CooldownConfig` in `lib/quality.py`)

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

1. **Trigger**: `_timeout_album()` (download.py) and `reject_and_requeue()` (album_source.py) call `db.check_and_apply_cooldown(username)` after logging the outcome.
2. **Decision**: `check_and_apply_cooldown()` queries `download_log` for last N outcomes, delegates to `should_cooldown()` pure function.
3. **Storage**: If triggered, upserts `user_cooldowns` with `cooldown_until = NOW() + 3 days`.
4. **Cache**: `ctx.cooled_down_users` populated at cycle start in `cratedigger.py main()`, shared with Phase 1 thread. Updated in real-time when new cooldowns are applied mid-cycle.
5. **Enforcement**: `try_enqueue()` and `try_multi_enqueue()` in `lib/enqueue.py` skip users in `ctx.cooled_down_users` before checking the per-request denylist.

## Re-cooldown behavior

After the 3-day cooldown expires, the user gets one chance. If they succeed, the success breaks their failure streak. If they fail, `check_and_apply_cooldown` sees 4 old failures + 1 new = 5 failures → immediate re-cooldown.

## Diagnostics

```bash
# View active cooldowns
pipeline-cli query "SELECT username, cooldown_until, reason FROM user_cooldowns WHERE cooldown_until > NOW()"

# View all cooldowns (including expired)
pipeline-cli query "SELECT * FROM user_cooldowns ORDER BY cooldown_until DESC"

# Top timeout offenders
pipeline-cli query "SELECT soulseek_username, COUNT(*) FROM download_log WHERE outcome = 'timeout' GROUP BY soulseek_username ORDER BY count DESC LIMIT 10"

# Manually seed cooldowns for all users with 5+ consecutive failures
psql -h 192.168.100.11 -U cratedigger cratedigger -c "INSERT INTO user_cooldowns ..."
```
