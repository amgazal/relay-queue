CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind text NOT NULL CHECK (btrim(kind) <> ''),
    payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'succeeded', 'dead', 'cancelled')),
    idempotency_key text UNIQUE CHECK (btrim(idempotency_key) <> ''),
    available_at timestamptz NOT NULL DEFAULT now(),
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    lease_owner text CHECK (btrim(lease_owner) <> ''),
    lock_token uuid,
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (attempt_count BETWEEN 0 AND max_attempts),
    CHECK (state <> 'queued' OR attempt_count < max_attempts),
    CHECK (state <> 'running' OR attempt_count > 0),
    CHECK ((state = 'running') = (lease_owner IS NOT NULL)),
    CHECK ((state = 'running') = (lock_token IS NOT NULL)),
    CHECK ((state = 'running') = (lease_expires_at IS NOT NULL))
);

CREATE INDEX jobs_ready_idx ON jobs (available_at, id) WHERE state = 'queued';
CREATE INDEX jobs_expiring_idx ON jobs (lease_expires_at, id) WHERE state = 'running';

CREATE TABLE job_attempts (
    job_id uuid NOT NULL REFERENCES jobs(id),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    worker_id text NOT NULL CHECK (btrim(worker_id) <> ''),
    lock_token uuid NOT NULL UNIQUE,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz CHECK (finished_at >= started_at),
    outcome text NOT NULL DEFAULT 'running'
        CHECK (outcome IN ('running', 'succeeded', 'failed', 'expired', 'cancelled')),
    error_message text,
    PRIMARY KEY (job_id, attempt_number),
    CHECK ((outcome = 'running') = (finished_at IS NULL))
);

CREATE UNIQUE INDEX job_attempts_one_running_idx ON job_attempts (job_id)
    WHERE outcome = 'running';
