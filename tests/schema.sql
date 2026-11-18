-- ROLLBACK leaves the development database unchanged.
BEGIN;
DO $$
DECLARE
    job jobs%ROWTYPE;
BEGIN
    INSERT INTO jobs (kind, payload, idempotency_key)
        VALUES ('test', '{}', gen_random_uuid()::text) RETURNING * INTO job;
    IF job.state <> 'queued' OR job.attempt_count <> 0 THEN
        RAISE EXCEPTION 'unexpected submission defaults';
    END IF;
    BEGIN
        INSERT INTO jobs (kind, payload, idempotency_key) VALUES ('test', '{}', job.idempotency_key);
        RAISE EXCEPTION 'duplicate submission key accepted';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;
    BEGIN
        UPDATE jobs SET state = 'running', attempt_count = 1 WHERE id = job.id;
        RAISE EXCEPTION 'running job without lease accepted';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    INSERT INTO job_attempts (job_id, attempt_number, worker_id, lock_token)
        VALUES (job.id, 1, 'test', gen_random_uuid());
    BEGIN
        INSERT INTO job_attempts (job_id, attempt_number, worker_id, lock_token)
            VALUES (job.id, 2, 'test', gen_random_uuid());
        RAISE EXCEPTION 'overlapping open attempts accepted';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;
END $$;
ROLLBACK;
