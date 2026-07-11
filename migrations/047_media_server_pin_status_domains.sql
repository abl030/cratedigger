-- Close the Plex and Jellyfin Recently Added pin status domains. A value
-- outside these domains is neither selected as pending nor recognized by the
-- terminal-row pruner, so it would strand bookkeeping forever.
--
-- Refuse dirty deployments explicitly instead of silently rewriting history.
-- Each preflight names the table/domain and reports every observed bad value
-- with its count before the corresponding ALTER TABLE is attempted.

-- Block INSERT/UPDATE/DELETE across both preflight-to-constraint windows.
-- SHARE conflicts with writers' ROW EXCLUSIVE locks while preserving reads.
-- Acquire in a fixed Plex-to-Jellyfin order to avoid migration deadlocks.
LOCK TABLE plex_added_at_pins IN SHARE MODE;
LOCK TABLE jellyfin_date_created_pins IN SHARE MODE;

DO $$
DECLARE
    invalid_count  BIGINT;
    invalid_values TEXT;
BEGIN
    SELECT COALESCE(SUM(value_count), 0),
           COALESCE(
               string_agg(
                   format('%L (%s row%s)', status, value_count,
                          CASE WHEN value_count = 1 THEN '' ELSE 's' END),
                   ', ' ORDER BY status
               ),
               '(none)'
           )
    INTO invalid_count, invalid_values
    FROM (
        SELECT status, COUNT(*) AS value_count
        FROM plex_added_at_pins
        WHERE status NOT IN ('pending', 'done', 'skipped')
        GROUP BY status
    ) AS invalid;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'cannot constrain plex_added_at_pins.status domain [pending, done, skipped]: found % invalid row(s); observed bad values/counts: %',
            invalid_count, invalid_values
            USING ERRCODE = 'check_violation';
    END IF;
END $$;

ALTER TABLE plex_added_at_pins
    ADD CONSTRAINT plex_added_at_pins_status_check
    CHECK (status IN ('pending', 'done', 'skipped'));

DO $$
DECLARE
    invalid_count  BIGINT;
    invalid_values TEXT;
BEGIN
    SELECT COALESCE(SUM(value_count), 0),
           COALESCE(
               string_agg(
                   format('%L (%s row%s)', status, value_count,
                          CASE WHEN value_count = 1 THEN '' ELSE 's' END),
                   ', ' ORDER BY status
               ),
               '(none)'
           )
    INTO invalid_count, invalid_values
    FROM (
        SELECT status, COUNT(*) AS value_count
        FROM jellyfin_date_created_pins
        WHERE status NOT IN ('pending', 'done', 'skipped', 'expired')
        GROUP BY status
    ) AS invalid;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'cannot constrain jellyfin_date_created_pins.status domain [pending, done, skipped, expired]: found % invalid row(s); observed bad values/counts: %',
            invalid_count, invalid_values
            USING ERRCODE = 'check_violation';
    END IF;
END $$;

ALTER TABLE jellyfin_date_created_pins
    ADD CONSTRAINT jellyfin_date_created_pins_status_check
    CHECK (status IN ('pending', 'done', 'skipped', 'expired'));
