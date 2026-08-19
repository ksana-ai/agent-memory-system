ALTER TABLE agent_memory.memory_candidates
    ADD COLUMN expires_at timestamptz;

ALTER TABLE agent_memory.memory_cards
    ADD COLUMN expires_at timestamptz;

CREATE INDEX memory_cards_scope_serviceable_expiry_idx
    ON agent_memory.memory_cards (tenant_id, user_id, expires_at, created_at, id)
    WHERE status = 'active';

---- create above / drop below ----

DROP INDEX IF EXISTS agent_memory.memory_cards_scope_serviceable_expiry_idx;

ALTER TABLE agent_memory.memory_cards
    DROP COLUMN IF EXISTS expires_at;

ALTER TABLE agent_memory.memory_candidates
    DROP COLUMN IF EXISTS expires_at;
