CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.{{outbox}} (
    id uuid PRIMARY KEY CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    lane integer CHECK (lane >= 0),
    ordering_key text,
    transport text NOT NULL,
    destination text NOT NULL,
    message_type text NOT NULL,
    headers bytea NOT NULL,
    body bytea NOT NULL,
    created_at timestamptz NOT NULL,
    deliver_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text,
    tenant_id text,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'parked')),
    claim_token uuid CHECK (claim_token <> '00000000-0000-0000-0000-000000000000'),
    claim_expires_at timestamptz,
    CHECK ((claim_token IS NULL) = (claim_expires_at IS NULL))
);

CREATE INDEX IF NOT EXISTS {{outbox_pending_idx}} ON {{schema}}.{{outbox}} (lane, id)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS {{outbox_parked_idx}} ON {{schema}}.{{outbox}} (lane, ordering_key, id)
    WHERE status = 'parked';
CREATE INDEX IF NOT EXISTS {{outbox_scheduled_idx}} ON {{schema}}.{{outbox}} (deliver_at)
    WHERE deliver_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS {{schema}}.{{outbox_dead}} (
    id uuid PRIMARY KEY CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    lane integer CHECK (lane >= 0),
    ordering_key text,
    transport text NOT NULL,
    destination text NOT NULL,
    message_type text NOT NULL,
    headers bytea NOT NULL,
    body bytea NOT NULL,
    created_at timestamptz NOT NULL,
    deliver_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text,
    tenant_id text,
    failed_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS {{schema}}.{{inbox}} (
    consumer text NOT NULL,
    message_id uuid NOT NULL CHECK (message_id <> '00000000-0000-0000-0000-000000000000'),
    processed_at timestamptz NOT NULL,
    PRIMARY KEY (consumer, message_id)
);
CREATE INDEX IF NOT EXISTS {{inbox_processed_idx}} ON {{schema}}.{{inbox}} (processed_at);

CREATE TABLE IF NOT EXISTS {{schema}}.{{leases}} (
    name text PRIMARY KEY,
    owner text NOT NULL,
    expires_at timestamptz NOT NULL
);
