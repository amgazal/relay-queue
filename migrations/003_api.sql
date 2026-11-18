ALTER TABLE jobs ADD COLUMN delay_seconds integer NOT NULL DEFAULT 0 CHECK (delay_seconds >= 0),
    ADD COLUMN original_max_attempts integer CHECK (original_max_attempts > 0);
CREATE INDEX jobs_page_idx ON jobs (created_at DESC, id DESC);
CREATE INDEX jobs_active_idx ON jobs (state) WHERE state IN ('queued','running');
