-- 066_processing_automation_owner.sql
--
-- One exact active automation_import job owns a request throughout the
-- processor lifecycle. Historical unattached jobs remain ordinary audit rows;
-- this migration adds constraints and empty authority tables without adopting
-- or rewriting any existing row.

LOCK TABLE album_requests, import_jobs IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE album_requests
    ADD COLUMN active_automation_import_job_id INTEGER;

ALTER TABLE import_jobs
    ADD COLUMN execution_invocation_id TEXT,
    ADD COLUMN execution_host_boot_id TEXT,
    ADD COLUMN execution_systemd_unit TEXT,
    ADD COLUMN execution_worker_pid INTEGER,
    ADD COLUMN execution_worker_start_ticks BIGINT,
    ADD COLUMN execution_beets_pid INTEGER,
    ADD COLUMN execution_beets_start_ticks BIGINT;

ALTER TABLE album_requests
    DROP CONSTRAINT album_requests_status_check;

ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_status_check
    CHECK(status IN (
        'initializing', 'wanted', 'downloading', 'processing', 'imported',
        'unsearchable', 'replaced'
    )),
    ADD CONSTRAINT album_requests_processing_owner_equivalent
    CHECK (
        (status = 'processing')
        = (active_automation_import_job_id IS NOT NULL)
    ),
    ADD CONSTRAINT album_requests_active_automation_owner_unique
    UNIQUE (active_automation_import_job_id)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE import_jobs
    ADD CONSTRAINT import_jobs_id_request_id_unique
    UNIQUE (id, request_id),
    ADD CONSTRAINT import_jobs_execution_lease_complete
    CHECK (
        (
            execution_invocation_id IS NULL
            AND execution_host_boot_id IS NULL
            AND execution_systemd_unit IS NULL
            AND execution_worker_pid IS NULL
            AND execution_worker_start_ticks IS NULL
        )
        OR
        (
            execution_invocation_id IS NOT NULL
            AND execution_invocation_id !~ '^[[:space:]]*$'
            AND execution_host_boot_id IS NOT NULL
            AND execution_host_boot_id !~ '^[[:space:]]*$'
            AND execution_systemd_unit IS NOT NULL
            AND execution_systemd_unit !~ '^[[:space:]]*$'
            AND execution_worker_pid > 0
            AND execution_worker_start_ticks >= 0
        )
    ),
    ADD CONSTRAINT import_jobs_execution_beets_identity_complete
    CHECK (
        (
            execution_beets_pid IS NULL
            AND execution_beets_start_ticks IS NULL
        )
        OR
        (
            execution_beets_pid > 0
            AND execution_beets_start_ticks >= 0
            AND execution_invocation_id IS NOT NULL
        )
    );

ALTER TABLE album_requests
    ADD CONSTRAINT album_requests_active_automation_owner_fk
    FOREIGN KEY (active_automation_import_job_id, id)
    REFERENCES import_jobs (id, request_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX one_active_automation_import_per_request
    ON import_jobs (request_id)
    WHERE job_type = 'automation_import'
      AND status IN ('queued', 'running', 'recovery_required');

CREATE FUNCTION enforce_processing_request_integrity(
    affected_request_ids INTEGER[]
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    affected_request_id INTEGER;
BEGIN
    FOR affected_request_id IN
        SELECT DISTINCT request_id
        FROM unnest(affected_request_ids) AS request_id
        WHERE request_id IS NOT NULL
        ORDER BY request_id
    LOOP
        -- Every validator takes the same request -> job -> journal lock order.
        -- Conflicting transactions therefore serialize or one is deadlock-
        -- aborted; a deferred validator never approves from an unlocked stale
        -- snapshot while a peer concurrently breaks the ownership bundle.
        PERFORM request.id
        FROM album_requests AS request
        WHERE request.id = affected_request_id
        FOR UPDATE;

        PERFORM job.id
        FROM import_jobs AS job
        WHERE job.request_id = affected_request_id
        ORDER BY job.id
        FOR UPDATE;

        PERFORM journal.job_id
        FROM processing_cleanup_journal AS journal
        WHERE journal.request_id = affected_request_id
        ORDER BY journal.job_id
        FOR UPDATE;

        IF EXISTS (
            SELECT 1
            FROM album_requests AS request
            LEFT JOIN import_jobs AS job
              ON job.id = request.active_automation_import_job_id
             AND job.request_id = request.id
            WHERE request.id = affected_request_id
              AND request.active_automation_import_job_id IS NOT NULL
              AND (
                  request.status <> 'processing'
                  OR job.id IS NULL
                  OR job.job_type <> 'automation_import'
                  OR job.status NOT IN (
                      'queued', 'running', 'recovery_required'
                  )
              )
        ) THEN
            RAISE EXCEPTION
                'processing owner must be the request''s active automation job'
                USING
                    ERRCODE = '23514',
                    CONSTRAINT = 'complete_processing_owner';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM processing_cleanup_journal AS journal
            LEFT JOIN album_requests AS request
              ON request.id = journal.request_id
            LEFT JOIN import_jobs AS job
              ON job.id = journal.job_id
             AND job.request_id = journal.request_id
            WHERE journal.request_id = affected_request_id
              AND (
                  request.id IS NULL
                  OR request.status <> 'processing'
                  OR request.active_automation_import_job_id <> journal.job_id
                  OR job.id IS NULL
                  OR job.job_type <> 'automation_import'
                  OR job.status NOT IN (
                      'queued', 'running', 'recovery_required'
                  )
              )
        ) THEN
            RAISE EXCEPTION
                'cleanup journal must belong to the exact active processing owner'
                USING
                    ERRCODE = '23514',
                    CONSTRAINT = 'processing_cleanup_journal_exact_owner';
        END IF;
    END LOOP;
END;
$$;

CREATE FUNCTION enforce_complete_processing_owner()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    affected_request_ids INTEGER[] := ARRAY[]::INTEGER[];
    affected_job_ids INTEGER[] := ARRAY[]::INTEGER[];
    affected_job_id INTEGER;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        affected_request_ids := array_append(
            affected_request_ids,
            CASE
                WHEN TG_TABLE_NAME = 'album_requests'
                THEN (to_jsonb(OLD)->>'id')::INTEGER
                ELSE (to_jsonb(OLD)->>'request_id')::INTEGER
            END
        );
    END IF;
    IF TG_OP <> 'DELETE' THEN
        affected_request_ids := array_append(
            affected_request_ids,
            CASE
                WHEN TG_TABLE_NAME = 'album_requests'
                THEN (to_jsonb(NEW)->>'id')::INTEGER
                ELSE (to_jsonb(NEW)->>'request_id')::INTEGER
            END
        );
    END IF;
    PERFORM enforce_processing_request_integrity(affected_request_ids);
    IF TG_TABLE_NAME = 'import_jobs' THEN
        IF TG_OP <> 'INSERT' THEN
            affected_job_ids := array_append(
                affected_job_ids,
                (to_jsonb(OLD)->>'id')::INTEGER
            );
        END IF;
        IF TG_OP <> 'DELETE' THEN
            affected_job_ids := array_append(
                affected_job_ids,
                (to_jsonb(NEW)->>'id')::INTEGER
            );
        END IF;
    ELSE
        IF TG_OP <> 'INSERT' THEN
            affected_job_ids := array_append(
                affected_job_ids,
                (to_jsonb(OLD)->>'active_automation_import_job_id')::INTEGER
            );
        END IF;
        IF TG_OP <> 'DELETE' THEN
            affected_job_ids := array_append(
                affected_job_ids,
                (to_jsonb(NEW)->>'active_automation_import_job_id')::INTEGER
            );
        END IF;
    END IF;

    FOR affected_job_id IN
        SELECT DISTINCT job_id
        FROM unnest(affected_job_ids) AS job_id
        WHERE job_id IS NOT NULL
        ORDER BY job_id
    LOOP
        -- The request-scoped validator above locks request -> jobs. Re-read
        -- every affected owner afterwards so clearing the pointer, or an
        -- FK-driven request_id = NULL, cannot leave an active automation job
        -- detached.
        PERFORM job.id
        FROM import_jobs AS job
        WHERE job.id = affected_job_id
        FOR UPDATE;

        IF EXISTS (
            SELECT 1
            FROM import_jobs AS job
            LEFT JOIN album_requests AS request
              ON request.id = job.request_id
            WHERE job.id = affected_job_id
              AND job.job_type = 'automation_import'
              AND job.status IN ('queued', 'running', 'recovery_required')
              AND (
                  job.request_id IS NULL
                  OR request.id IS NULL
                  OR request.status <> 'processing'
                  OR request.active_automation_import_job_id
                     IS DISTINCT FROM job.id
              )
        ) THEN
            RAISE EXCEPTION
                'active automation job must own its exact processing request'
                USING
                    ERRCODE = '23514',
                    CONSTRAINT = 'complete_processing_owner';
        END IF;
    END LOOP;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER album_requests_complete_processing_owner
    AFTER INSERT OR UPDATE OR DELETE ON album_requests
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_complete_processing_owner();

CREATE CONSTRAINT TRIGGER import_jobs_complete_processing_owner
    AFTER INSERT OR UPDATE OR DELETE ON import_jobs
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_complete_processing_owner();

CREATE TABLE processing_cleanup_journal (
    job_id INTEGER NOT NULL,
    request_id INTEGER NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    action TEXT NOT NULL CHECK (btrim(action) <> ''),
    source_path TEXT NOT NULL CHECK (btrim(source_path) <> ''),
    source_manifest JSONB NOT NULL
        CHECK (jsonb_typeof(source_manifest) = 'array'),
    source_manifest_hash TEXT NOT NULL
        CHECK (btrim(source_manifest_hash) <> ''),
    destination_path TEXT,
    destination_manifest JSONB,
    destination_manifest_hash TEXT,
    selected_destination_path TEXT,
    step_progress JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(step_progress) = 'object'),
    declared_result_status TEXT
        CHECK (declared_result_status IN ('wanted', 'imported')),
    declared_reason TEXT,
    evidence_revision TEXT,
    completed_receipt JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (job_id, request_id),
    CONSTRAINT processing_cleanup_journal_job_request_fk
        FOREIGN KEY (job_id, request_id)
        REFERENCES import_jobs (id, request_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT processing_cleanup_journal_destination_complete
        CHECK (
            (
                destination_path IS NULL
                AND destination_manifest IS NULL
                AND destination_manifest_hash IS NULL
                AND selected_destination_path IS NULL
            )
            OR
            (
                destination_path IS NOT NULL
                AND destination_path !~ '^[[:space:]]*$'
                AND destination_manifest IS NOT NULL
                AND jsonb_typeof(destination_manifest) = 'array'
                AND destination_manifest_hash IS NOT NULL
                AND btrim(destination_manifest_hash) <> ''
                AND selected_destination_path IS NOT NULL
                AND selected_destination_path !~ '^[[:space:]]*$'
            )
        ),
    CONSTRAINT processing_cleanup_journal_completion_complete
        CHECK (
            (completed_receipt IS NULL AND completed_at IS NULL)
            OR
            (completed_receipt IS NOT NULL AND completed_at IS NOT NULL)
        ),
    CONSTRAINT processing_cleanup_journal_declaration_complete
        CHECK (
            (
                declared_result_status IS NULL
                AND declared_reason IS NULL
                AND evidence_revision IS NULL
            )
            OR
            (
                declared_result_status IS NOT NULL
                AND declared_reason IS NOT NULL
                AND declared_reason !~ '^[[:space:]]*$'
                AND evidence_revision IS NOT NULL
                AND evidence_revision !~ '^[[:space:]]*$'
            )
        )
);

CREATE FUNCTION enforce_processing_cleanup_journal_owner()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    affected_request_ids INTEGER[] := ARRAY[]::INTEGER[];
BEGIN
    IF TG_OP <> 'INSERT' THEN
        affected_request_ids := array_append(
            affected_request_ids,
            CASE
                WHEN TG_TABLE_NAME = 'album_requests'
                THEN (to_jsonb(OLD)->>'id')::INTEGER
                ELSE (to_jsonb(OLD)->>'request_id')::INTEGER
            END
        );
    END IF;
    IF TG_OP <> 'DELETE' THEN
        affected_request_ids := array_append(
            affected_request_ids,
            CASE
                WHEN TG_TABLE_NAME = 'album_requests'
                THEN (to_jsonb(NEW)->>'id')::INTEGER
                ELSE (to_jsonb(NEW)->>'request_id')::INTEGER
            END
        );
    END IF;
    PERFORM enforce_processing_request_integrity(affected_request_ids);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER processing_cleanup_journal_exact_owner
    AFTER INSERT OR UPDATE OR DELETE ON processing_cleanup_journal
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_processing_cleanup_journal_owner();

CREATE CONSTRAINT TRIGGER album_requests_cleanup_journal_exact_owner
    AFTER INSERT OR UPDATE OR DELETE ON album_requests
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_processing_cleanup_journal_owner();

CREATE CONSTRAINT TRIGGER import_jobs_cleanup_journal_exact_owner
    AFTER INSERT OR UPDATE OR DELETE ON import_jobs
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION enforce_processing_cleanup_journal_owner();
