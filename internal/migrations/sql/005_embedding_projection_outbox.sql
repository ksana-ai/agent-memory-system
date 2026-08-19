CREATE TABLE agent_memory.embedding_projection_targets (
    embedding_space text COLLATE "C" PRIMARY KEY,
    state text COLLATE "C" NOT NULL,
    enqueue_new boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT embedding_projection_targets_space_fk
        FOREIGN KEY (embedding_space)
        REFERENCES agent_memory.embedding_spaces (id)
        ON DELETE RESTRICT,
    CONSTRAINT embedding_projection_targets_space_not_blank
        CHECK (btrim(embedding_space) <> ''),
    CONSTRAINT embedding_projection_targets_state_valid
        CHECK (state IN ('shadow', 'serving', 'blocked', 'retired')),
    CONSTRAINT embedding_projection_targets_enqueue_valid
        CHECK (NOT enqueue_new OR state IN ('shadow', 'serving')),
    CONSTRAINT embedding_projection_targets_timestamps_valid
        CHECK (updated_at >= created_at)
);

CREATE UNIQUE INDEX embedding_projection_targets_one_serving_idx
    ON agent_memory.embedding_projection_targets (state)
    WHERE state = 'serving';

CREATE INDEX embedding_projection_targets_enqueue_new_idx
    ON agent_memory.embedding_projection_targets (embedding_space)
    WHERE enqueue_new;

CREATE TABLE agent_memory.embedding_projection_jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id text COLLATE "C" NOT NULL,
    user_id text COLLATE "C" NOT NULL,
    memory_id text COLLATE "C" NOT NULL,
    embedding_space text COLLATE "C" NOT NULL,
    expected_memory_version integer NOT NULL,
    state text COLLATE "C" NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text COLLATE "C",
    lease_version bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    last_error_code text COLLATE "C",
    last_error_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CONSTRAINT embedding_projection_jobs_scope_memory_space_unique
        UNIQUE (tenant_id, user_id, memory_id, embedding_space),
    CONSTRAINT embedding_projection_jobs_card_fk
        FOREIGN KEY (tenant_id, user_id, memory_id)
        REFERENCES agent_memory.memory_cards (tenant_id, user_id, id)
        ON DELETE CASCADE,
    CONSTRAINT embedding_projection_jobs_space_fk
        FOREIGN KEY (embedding_space)
        REFERENCES agent_memory.embedding_spaces (id)
        ON DELETE RESTRICT,
    CONSTRAINT embedding_projection_jobs_tenant_id_not_blank
        CHECK (btrim(tenant_id) <> ''),
    CONSTRAINT embedding_projection_jobs_user_id_not_blank
        CHECK (btrim(user_id) <> ''),
    CONSTRAINT embedding_projection_jobs_memory_id_not_blank
        CHECK (btrim(memory_id) <> ''),
    CONSTRAINT embedding_projection_jobs_space_not_blank
        CHECK (btrim(embedding_space) <> ''),
    CONSTRAINT embedding_projection_jobs_expected_memory_version_positive
        CHECK (expected_memory_version > 0),
    CONSTRAINT embedding_projection_jobs_state_valid
        CHECK (state IN ('pending', 'leased', 'retry', 'succeeded', 'dead', 'cancelled')),
    CONSTRAINT embedding_projection_jobs_attempt_count_nonnegative
        CHECK (attempt_count >= 0),
    CONSTRAINT embedding_projection_jobs_lease_version_nonnegative
        CHECK (lease_version >= 0),
    CONSTRAINT embedding_projection_jobs_lease_valid CHECK (
        (
            state = 'leased'
            AND lease_owner IS NOT NULL
            AND btrim(lease_owner) <> ''
            AND lease_version > 0
            AND lease_until IS NOT NULL
            AND attempt_count > 0
        )
        OR (
            state <> 'leased'
            AND lease_owner IS NULL
            AND lease_until IS NULL
        )
    ),
    CONSTRAINT embedding_projection_jobs_completion_valid CHECK (
        (
            state IN ('succeeded', 'dead', 'cancelled')
            AND completed_at IS NOT NULL
        )
        OR (
            state IN ('pending', 'leased', 'retry')
            AND completed_at IS NULL
        )
    ),
    CONSTRAINT embedding_projection_jobs_error_pair_valid CHECK (
        (last_error_code IS NULL AND last_error_at IS NULL)
        OR (
            last_error_code IS NOT NULL
            AND last_error_at IS NOT NULL
            AND char_length(last_error_code) BETWEEN 1 AND 64
            AND last_error_code ~ '^[a-z][a-z0-9_]*$'
            AND last_error_at >= created_at
        )
    ),
    CONSTRAINT embedding_projection_jobs_dead_error_required
        CHECK (state <> 'dead' OR last_error_code IS NOT NULL),
    CONSTRAINT embedding_projection_jobs_attempt_state_valid CHECK (
        state IN ('pending', 'cancelled')
        OR attempt_count > 0
    ),
    CONSTRAINT embedding_projection_jobs_timestamps_valid CHECK (
        updated_at >= created_at
        AND (completed_at IS NULL OR completed_at >= created_at)
    )
);

CREATE INDEX embedding_projection_jobs_claim_idx
    ON agent_memory.embedding_projection_jobs (available_at, created_at, id)
    WHERE state IN ('pending', 'retry');

CREATE INDEX embedding_projection_jobs_lease_until_idx
    ON agent_memory.embedding_projection_jobs (lease_until, id)
    WHERE state = 'leased';

---- create above / drop below ----

DROP TABLE IF EXISTS agent_memory.embedding_projection_jobs;

DROP TABLE IF EXISTS agent_memory.embedding_projection_targets;
