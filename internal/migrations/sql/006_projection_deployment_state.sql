CREATE TABLE agent_memory.embedding_projection_deployment (
    singleton boolean PRIMARY KEY DEFAULT true,
    generation bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT embedding_projection_deployment_singleton_true
        CHECK (singleton),
    CONSTRAINT embedding_projection_deployment_generation_nonnegative
        CHECK (generation >= 0),
    CONSTRAINT embedding_projection_deployment_timestamps_valid
        CHECK (updated_at >= created_at)
);

INSERT INTO agent_memory.embedding_projection_deployment DEFAULT VALUES;

CREATE INDEX memory_cards_active_projection_scan_idx
    ON agent_memory.memory_cards (
        (tenant_id COLLATE "C"),
        (user_id COLLATE "C"),
        (id COLLATE "C")
    )
    INCLUDE (version, expires_at)
    WHERE status = 'active';

CREATE INDEX embedding_projection_jobs_space_scope_memory_idx
    ON agent_memory.embedding_projection_jobs (
        embedding_space, tenant_id, user_id, memory_id
    );

CREATE INDEX memory_embeddings_space_scope_memory_idx
    ON agent_memory.memory_embeddings (
        embedding_space, tenant_id, user_id, memory_id
    );

---- create above / drop below ----

DROP INDEX IF EXISTS agent_memory.memory_embeddings_space_scope_memory_idx;

DROP INDEX IF EXISTS agent_memory.embedding_projection_jobs_space_scope_memory_idx;

DROP INDEX IF EXISTS agent_memory.memory_cards_active_projection_scan_idx;

DROP TABLE IF EXISTS agent_memory.embedding_projection_deployment;
