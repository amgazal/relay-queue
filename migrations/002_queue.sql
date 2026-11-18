ALTER TABLE jobs
    ADD COLUMN priority integer NOT NULL DEFAULT 0,
    ADD COLUMN requested_at timestamptz,
    ADD COLUMN completed_at timestamptz;

-- Phase 0 only retained the current eligibility time.
UPDATE jobs SET requested_at = available_at WHERE available_at <> created_at;
UPDATE jobs SET completed_at = updated_at WHERE state = 'succeeded';

CREATE INDEX jobs_priority_idx ON jobs (priority DESC, available_at, id)
    WHERE state = 'queued';
