-- Run once, in one transaction, with application writers and dispatchers stopped.
-- Preserve the old UUID order for existing rows; historical insert order is unavailable.
LOCK TABLE {{schema}}.{{outbox}} IN ACCESS EXCLUSIVE MODE;
ALTER TABLE {{schema}}.{{outbox}} ADD COLUMN seq bigint;
WITH numbered AS (SELECT id, row_number() OVER (ORDER BY id) AS position FROM {{schema}}.{{outbox}})
UPDATE {{schema}}.{{outbox}} AS target SET seq = numbered.position FROM numbered WHERE target.id = numbered.id;
ALTER TABLE {{schema}}.{{outbox}} ALTER COLUMN seq SET NOT NULL;
ALTER TABLE {{schema}}.{{outbox}} ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY;
SELECT setval(pg_get_serial_sequence({{outbox_literal}}, 'seq'), COALESCE(max(seq), 1), count(*) > 0)
FROM {{schema}}.{{outbox}};
DROP INDEX {{schema}}.{{outbox_pending_idx}};
DROP INDEX {{schema}}.{{outbox_parked_idx}};
CREATE INDEX {{outbox_pending_idx}} ON {{schema}}.{{outbox}} (lane, seq) WHERE status = 'pending' AND lane IS NOT NULL;
CREATE INDEX {{outbox_unordered_idx}} ON {{schema}}.{{outbox}} (id) WHERE status = 'pending' AND lane IS NULL;
CREATE INDEX {{outbox_parked_idx}} ON {{schema}}.{{outbox}} (lane, ordering_key, seq) WHERE status = 'parked';
