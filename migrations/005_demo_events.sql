-- Appending one inbox entry is the demo receiver's business effect.
CREATE TABLE demo_received_events (
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL,
    data jsonb NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT now()
);
