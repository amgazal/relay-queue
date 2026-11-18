CREATE TABLE schedules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL CHECK (btrim(name) <> '' AND octet_length(name)<=128),
    cron text NOT NULL CHECK (btrim(cron) <> '' AND octet_length(cron)<=256),
    kind text NOT NULL CHECK (btrim(kind) <> '' AND octet_length(kind)<=128),
    payload jsonb NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    next_run timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX schedules_due_idx ON schedules (next_run,id) WHERE enabled;
CREATE INDEX schedules_page_idx ON schedules (created_at DESC,id DESC);
ALTER TABLE jobs ADD COLUMN schedule_id uuid REFERENCES schedules(id),
    ADD COLUMN occurrence_at timestamptz,
    ADD CHECK ((schedule_id IS NULL) = (occurrence_at IS NULL));
CREATE UNIQUE INDEX jobs_occurrence_idx ON jobs(schedule_id,occurrence_at) WHERE schedule_id IS NOT NULL;
